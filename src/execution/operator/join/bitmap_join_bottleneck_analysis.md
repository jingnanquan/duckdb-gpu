# BitmapJoin 与 PerfectHashJoin 瓶颈定位分析

> 适用范围：`duckdb-cuda` 的 `BitmapJoinExecutor`（`bitmap_hash_join_executor.cpp`）
> 与 `PerfectHashJoinExecutor`（`perfect_hash_join_executor.cpp`），结合
> `physical_hash_join.cpp` 的 build/probe 主流程。
>
> 记号约定：
> - `N` = build 侧总行数（≈ PK 基数）
> - `B` = `bitmap_size` = PK row 数量（BitmapJoin 全局/线程局部 bitmap 尺寸）
> - `C` = RHS 输出列数（`C_fixed` 定长、`C_var` 变长 VARCHAR/ARRAY）
> - `T` = 线程数
> - `M` = `build_range + 1`（PerfectHash 表尺寸，上限 `MAX_BUILD_SIZE = 1048576`）
> - `R` = probe 总行数
> - `W = B/64` = bitmap 的 64-bit word 数（`validity_t = uint64_t`）

---

## 0. 两种实现的定位差异

| 维度 | BitmapJoin (BHJ) | PerfectHashJoin (PHJ) |
|---|---|---|
| 适用前提 | build 侧为 PK（唯一）侧、INNER、单列等值、整型 key | 单列整型、无嵌套列、range≤1M、无重复 key、INNER |
| bitmap/表 尺寸 | 与 PK 行数 `B` 同量级（可很大） | 与 key 值域 `M`（≤~1M）同量级（有界） |
| 是否仍需常规 HT | 否（全局 sink 仍构造空 HT，但 build/probe 不触碰，零成本） | **是**：先完整建常规 HT 并 finalize，再做一次全扫描+Gather |
| build 并行度 | 多线程（每线程一份 local bitmap + scatter） | 基本单线程（注释明确标 `TODO: parallel finalize`） |
| probe 并行度 | 多线程 | 多线程 |
| 回表方式 | dictionary `Slice`（零拷贝） | dictionary `Dictionary`（零拷贝） |

---

## 1. 构建 bitmap / combine bitmap 的时间复杂度

### 1.1 BitmapJoin

**构建（每个 build chunk / 每个线程）— `SinkBitmap → ExtractBuildRowids`**
- 遍历 `count`（≈2048）行，对每有效行：`rowid = value - offset`，`local_bitmap.SetValidUnsafe(rowid)`（单次置位 O(1)），并回填 `valid_rows`/`rowids` 两个索引。
- 复杂度：每 chunk `O(count)`，全量 `O(N)`。热路径**无锁**。
- 注意：每个线程持有的 `local_bitmap` 都是**全尺寸 `B`**，因此线程局部 bitmap 内存 = `O(T·B/8)` 字节。当 `B` 很大（如 1e9）且 `T` 较多时，仅 bitmap 就可能占用数 GB。

**合并（Combine 阶段）— `CombineBitmap`**
- 将 `local_data[w]` 用 `std::atomic<validity_t>::fetch_or(..., relaxed)` OR 进全局 bitmap。
- 稀疏优化：仅遍历本线程**非零 word**（`if (local_data[w] != 0)`），跳过全 0 word。
- 复杂度：
  - 稀疏：≈ `O(本线程非零 word 数)`，全线程合计 ≈ `O(实际被置位的 word 数)`。
  - 稠密：每线程约遍历 `W` 个 word，全线程合计 `O(T·W)` 次原子 OR。
- **锁冲突**：无 mutex（用 relaxed atomic，比 mutex 快几个数量级）。但 `fetch_or` 共享同一 word 的多个线程会争用**同一 cache line（64 个 rowid 共享一行）**，产生 MESI 失效流量——这是稠密 + 多线程下的主要争用点。由于 build 行按 chunk 连续分配、chunk 派发给线程，相邻 rowid（同 word）通常落在同一线程，故实际 word 跨线程共享度取决于数据分布。

**dense 判定 — `FinalizeBitmap`**
- `is_build_dense = (total_valid_count == B)`，其中 `total_valid_count` 在 Combine 阶段由各线程 `fetch_add` 原子累加。
- 复杂度 `O(1)`，**不再做全量 `CountValid` 扫描**（已被优化掉）。

### 1.2 PerfectHashJoin

**构建期 bitmap — `bitmap_build_idx`**
- 在 `FullScanHashTable → TemplatedFillSelectionVectorBuild` 中对每个 key 置位：`bitmap_build_idx.SetValidUnsafe(idx)`，`O(N)`。
- 之后每列 `col_mask.Combine(bitmap_build_idx, build_size)`：`O(M/64)` 的 validity OR（把 NULL 槽位标为无效）。单线程。

**`CanDoPerfectHashJoin` 的开销（重点）**
- 调用时机：① `GetGlobalSinkState` 用统计 min/max 预判断；② `Finalize` 用实测 min/max 再判断。即**全查询仅调用约 2 次**，非逐行。
- 内部：静态检查（`conditions`、`children[1].GetTypes()` 嵌套类型遍历，均为列元数据 `O(C)`）+ `ExtractNumericValue`（O(1) cast）+ `TrySubtractOperator` 求 range（O(1)）+ `ht.Count() > build_range` 比较（O(1)）+ 与 `MAX_BUILD_SIZE=1048576` 比较。
- **结论：`CanDo` 开销为常数级（≈O(C)），对性能可忽略。** 真正进入 PHJ 后，build 还要先完整建常规 HT，这部分成本远高于 canDo 判断。

---

## 2. payload 空间创建 & 填入右表 chunk

### 2.1 BitmapJoin

**创建 payload 空间 — 构造函数中**
- 为每列 `make_uniq<Vector>(col_type, alloc_size)`，`alloc_size = max(B,1)`；并 `FlatVector::Validity(*col).Initialize(alloc_size)`。
- 复杂度：`O(C·B)` 内存分配，**串行**（构造期一次性，单线程）。`B` 很大时这是一笔可观的 upfront 内存/延迟，且后续不可释放。

**填入 payload — `SinkBitmap → ScatterColumn`（构建期，随 chunk 进行）**
- 定长列（`fixed_payload_indices`）：`tdata[rowid] = sdata[sidx]`，按 rowid 散写。
  - 并行：**多线程并行、lock-free**。正确性来自 PK 唯一性 → 不同线程写不同 rowid，无数据竞争。
  - 拷贝性质：**非零拷贝**，是 scatter 值拷贝（源 chunk 与目标 payload vector 内存布局不同，必须逐值写）。
- 变长列（`var_payload_indices`，VARCHAR/ARRAY）：同样 scatter，但 `StringVector::AddStringOrBlob(target, ...)` 需操作 target 的 StringHeap（有状态分配器，不能并发）。
  - 并行：**被 `lock_guard<mutex> build_lock` 串行化**。所有线程的所有变长列共享**同一把锁** → 变长 payload 填充是完全串行的，是 varchar 占比高时的关键瓶颈。
- 额外：`payload_source_columns` 在构造期完成「输出列 → build chunk 源列」映射（O(C)），无逐行开销。

### 2.2 PerfectHashJoin

**创建 payload 空间 — `BuildPerfectHashTable`**
- 每列 `DictionaryVector::CreateReusableDictionary(type, build_size)`，`build_size = M ≤ 1048577`。
- 复杂度 `O(C·M)`，串行、有界（M 上限 ~1M）。`bitmap_build_idx` 同样 `O(M)`。

**填入 payload — `FullScanHashTable`（单线程）**
- `ht.ScanKeyColumn` 扫描整张常规 HT 抽出 key：`O(N)`。
- `FillSelectionVectorSwitchBuild`：逐 key 计算 `idx = value - min`，置位去重：`O(N)`。
- 逐列 `data_collection.Gather(tuples_addresses, sel_tuples, key_count, ..., vector, sel_build, ...)`：从 HT 数据收集散写进 perfect_hash_table：`O(C·N)` 的总 scatter 拷贝。
- 列 validity `Combine`：`O(M/64)`。
- 拷贝性质：**非零拷贝**（Gather 是 scatter 拷贝）。并行度：**单线程**（代码注释明确标注未做并行 finalize）。

---

## 3. probe 后右表 payload 回表

### 3.1 BitmapJoin — `ProbeBitmap → FillProbeSelection + Slice`
- 选择向量：对每 probe 行计算 `rowid`，稠密 fast-path 直接取 rowid（**无位测试**），否则 `bitmap.RowIsValidUnsafe(rowid)` 单次位测试。`O(R)` 每查询。
- 右表回表：`result.data[lhs_count+i].Slice(*payload_columns[i], build_sel_vec, result_count)`。
  - 生成 **dictionary vector**，仅记录「基向量指针 + 选择向量 + 行数」，**不移动任何 payload 数据**。
  - **零拷贝**。复杂度 `O(R)`（构建选择向量），无锁、并行。
- 左表输出：`ReferenceColumns`（全命中）或 `Slice`（零拷贝引用 probe 输入）。

### 3.2 PerfectHashJoin — `ProbePerfectHashTable → FillSelectionVectorSwitchProbe + Dictionary`
- 选择向量：逐行 `idx = value - min`，`bitmap_build_idx.RowIsValid(idx)` 单次位测试（range 检查 + 位测试）。`O(R)`。
- 右表回表：`result_vector.Dictionary(perfect_hash_table[i], build_sel_vec)` → **dictionary 引用，零拷贝**，`O(R)`。
- 左表：同上，零拷贝。

**结论：两种实现的回表都是零拷贝的 dictionary 引用，复杂度 `O(R)`，通常不是瓶颈。**

---

## 4. 瓶颈定位总结

### BitmapJoin 潜在瓶颈（按优先级）
1. **变长列 payload 填充的单锁串行化**（`var_payload_indices` 下 `build_lock`）：多 VARCHAR 列 + 多线程时，scatter 被完全串行。→ 用 `perf`/火焰图看 `build_lock` 占用率。
2. **稠密 + 多线程下 `CombineBitmap` 的 cache-line 争用**：relaxed atomic 虽无锁，但 `T·W` 次共享 word 的 `fetch_or` 带来 MESI 失效流量。稀疏场景因跳过 0 word 影响很小。→ 看 combine 阶段 CPU 的 cache-miss / 远程访问。
3. **构造函数一次性 `O(C·B)` 大块 payload 分配**：`B` 极大时 upfront 内存与延迟、以及 `O(T·B)` 的线程局部 bitmap 内存压力。→ 监控 BHJ 构造期分配与峰值内存。
4. **probe 回表/选择**：零拷贝 `O(R)`，**一般不是瓶颈**（除非 build 极稠密但 probe 命中率极低，纯位测试开销）。

### PerfectHashJoin 潜在瓶颈
1. **双倍 build 成本**：必须先完整构建并 finalize 常规 HT，再做 `ScanKeyColumn + Gather` 全扫描。→ 看 build 期常规 HT finalize + `FullScanHashTable` 两段耗时。
2. **build 全程单线程**：`FullScanHashTable`/`Gather` 不可并行。→ 大 build 时 build 期是串行瓶颈。
3. **`M` 远超 `N` 的内存浪费**：稀疏值域下 perfect_hash_table 按 `M`（≤1M）分配，可能远大于实际 `N`；且 `col_mask.Combine` 扫描 `M/64` word。
4. **`CanDoPerfectHashJoin` 开销可忽略**（常数级、仅 2 次调用），不应作为优化对象。
5. probe 回表零拷贝 `O(R)`，**不是瓶颈**。

### 建议
- 若热点在 **build 期且右表多 VARCHAR**：优先优化 BHJ 变长列锁（如按列分锁 / 批量加锁 / 单线程预聚合 StringHeap）。
- 若在稀疏场景下，会造成分配payload大量浪费，bitmapjoinexecutor能否提前识别这种情况。并且采用二次hash的方式来减少payload浪费，并且使得线程local的chunk不必scatter到payload上，而是直接零拷贝append。中间新增一层bitmapidx到payloadrow的映射。