问题一：
6.4并不考虑从rowid 生成bitmap，因为此时projection还没有感知到rowid。即6.4直接复用 join_key_executor 的输出当 rowid。
同样probe侧也没有ref列


问题二：DuckDB 多线程模型下，BHJ build 怎么做才正确 + 最快
先对齐你说的并行模型：DuckDB sink 是 N 个线程各自 Sink(local_state) 并行 → Combine 串行汇入 global → Finalize。BHJ 要套进这个模型，但有一个关键区别必须想清楚：

bitmap 和 payload 的并发策略不一样，不能用同一种处理方式。

1. bitmap：thread-local + Combine OR（§7 的 B1，选对了）
bitmap 一个 64-bit word 覆盖 64 个 rowid。多个线程处理不同 rowid 但可能落在同一个 word → read-modify-write race。所以：

每个线程在 LocalSinkState 持有一份 thread-local ValidityMask(N)，Sink 里 SetValidUnsafe(rowid) 无锁写。
Combine 时按 word 做 global_word |= local_word（加 global 锁，或对每个 word 原子 OR）。开销 O(N/64)，极小（SF5 customer 188KB → 约 23K 个 word）。
不要用 B2（全局 atomic-OR 直接 set bit）：bitmap 小，cache line 竞争严重。
2. payload：全局唯一 N-slot，Sink 阶段直接 scatter 写全局（不要 thread-local）
这是我要重点提醒的、和设计文档 §6.4.2 有张力的地方：

文档把 rhs_payload_columns 放在 BitmapJoinBuildState 里，又写了 CombineBitmap(other) 做 payload merge。如果 executor / build_state 是 thread-local 的，payload 就会被切成多份，Combine 还要搬运，既费内存又费带宽。

但 payload 的本质是：每个 rowid 全局只会被 build 侧写入一次（build 侧 unique 是 BHJ 的硬前提）。所以正确做法是：

global executor 预分配一份全局 payload_cols[k] = Vector(N)，所有线程在 Sink 阶段按 payload_cols[k][rowid] = value 直接 scatter 写全局，无锁、无冲突、无需 merge。

Combine 对 payload 就是 no-op。这样比「thread-local payload + Combine 搬运」省掉一整轮内存拷贝。

唯一要注意的是不同线程写相邻 slot 的 false sharing，但 rowid 在不同线程间通常是分散的，影响可忽略；真要优化可让每线程按 morsel 处理连续 rowid 区间。

3. 所有权划分建议（直接影响 6.4 的类结构）
对象	放哪	并发策略
全局 bitmap（最终结果）	global BitmapJoinExecutor（挂 HashJoinGlobalSinkState）	Combine 时 word-OR 合并
thread-local bitmap	HashJoinLocalSinkState	Sink 无锁 SetValidUnsafe
全局 payload_cols[k]（N slot）	global BitmapJoinExecutor	Sink 阶段各线程直接 scatter 写，无需 Combine
也就是说，把 CombineBitmap 的语义收窄成「只 OR bitmap」，payload 这块去掉 merge。这会让 6.4 实现更简单也更快。

4. 其它性能要点
预分配：bitmap_size = pk->row_count 在 build 开始前就已知（来自 registry），一次性分配 bitmap + payload，杜绝动态扩容/rehash（这正是相对 JoinHashTable 省下的最大成本）。
只物化 rhs_output_columns：payload 只缓存 join 真正要输出的 RHS 列，不是全表列。
dense 快路径（§7.2 的 rhs_payload_dense）：MVP 先一律走 scatter；后续若检测到 build 侧无 filter 且 scan 顺序 == rowid，可改顺序 append，cache 更友好。这是 M4 优化，不进 MVP。
probe 端零拷贝：inner join 全命中时 result.Reference(lhs_out)；RHS 列用 Dictionary(payload_cols[k], build_sel_vec) 引用而非 gather copy（§6.4.5 已设计好）。
保留 build→probe 的 pipeline dependency（§4 第 3 点）：bitmap 工作量虽小，probe 仍必须等 build 全部 Finalize 完成，这条 AddDependency 不能删。
