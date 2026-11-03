# DuckDB hash join vs. unchained：本质区别与在 DuckDB 1.5 上的复现方案

> 依据：Birler et al., *Simple, Efficient, and Robust Hash Tables for Join Processing* (DaMoN'24) 原文 §3.1–3.5、Fig.4/6/9/10/13；
> DuckDB main（1.5+）`src/execution/join_hashtable.cpp`、`src/execution/operator/join/physical_hash_join.cpp`。
> 本文修正上一版笔记 `DuckDB-hashjoin-vs-unchained论文-对比与优化建议.md` 中两处不准确表述。

---

## 0. 先纠正两处说法

| 旧说法 | 实际情况 |
|---|---|
| "论文的 slot 里装的是同一个 hash 的 tuple" | **错**。slot 是**hash 前缀等价类**：`slot = hash >> shift`，只有 top-K 位相等。区间内可以有多个不同 hash、不同 key，且**顺序任意**（按拷贝顺序），并未按后缀排序 |
| "论文需要对分区排序" | 不准确。是**桶排序**（counting sort）只按前缀分桶：count → exclusive prefix sum → copy（Fig.10）。桶内**不做任何排序**，所以也不能二分，只能线性扫 |

这两点连起来才能解释论文的两个设计：为什么负载率压到 65%（每 slot 平均 0.65 个 tuple，线性扫通常 1–2 次比较就结束）、为什么必须配 4-bit Bloom（FPR 1/169）。

你说的第三条判断——**DuckDB 的一个 slot 里链上的 key 全都相同**——是对的，代码依据见 §1.2。

---

## 1. 本质区别：点映射 vs. 区间映射

### 1.1 一句话

| | DuckDB | 论文 unchained |
|---|---|---|
| slot 的语义 | **一个 key 的"家"**（home bucket） | **hash 空间的一段"切片"**（prefix class） |
| 冲突解决 | collision **resolution**：异 key 争同一 slot → 向外探测 | collision **containment**：同前缀一律塞进同一段连续区间 |
| 目录项指向 | 一个 row（链头） | 一个**区间** `[dir[i-1], dir[i])` |
| 结构类比 | `key → intrusive linked list` | `radix-bucket-sorted array` 的 run index |

### 1.2 不变量（这是全部差异的根）

**DuckDB 的不变量 I1**：若 `entry` 被占用，则 `entry` 所指的行链上**所有行的 key 相等**。
依据 `join_hashtable.cpp: InsertHashesLoop` / `InsertMatchesAndIncrementMisses`：

```cpp
// 探测循环：只有 salt 撞了才停下来比 key
while (true) {
    entry = atomic_entry.load();
    if (!entry.IsOccupied()) break;          // 空位 → 插入
    if (entry.GetSalt() == salt) break;      // salt 相同 → 比 key
    IncrementAndWrap(ht_offset, capacity_mask);  // salt 不同 → 线性探测下一个 slot
}

// 比完 key 之后：
//   key 相等 → InsertRowToEntry<PARALLEL, false>()，头插进该行的链
//   key 不等 → 该行 ht_offset 前进一格，继续探测（salt 位保留）
```

⇒ 同 key 的复数（skew）走**行内链**（`pointer_offset` 处 8 字节 next 指针，头插，2 次 CAS）；
异构 key 撞上同一 slot（16-bit salt 冲突，概率 ≈ 1/65536）走**开放寻址**搬去别的 slot。
两条路径彻底分离——这正是你说的"链专门存 skew"。

**论文的不变量 I2**：目录 slot `i` 对应 tuple 数组中 `[dir[i-1]>>16, dir[i]>>16)` 这一段，
段内所有 tuple 的 hash 前 K 位都等于 `i`，**key 可以互不相同、也可以重复，顺序任意**。
依据 Fig.4 与 Fig.10：

```cpp
// 查询：先查 Bloom，再扫区间
u64 slot = hash >> shift;
u64 entry = directory[slot];
if (!couldContain((u16)entry, hash)) return;      // andn，5 条指令
T* start = directory[slot - 1] >> 16;
T* end   = entry >> 16;
for (T* cur = start; cur != end; ++cur)
    if (cur->key == key) produce(cur);            // 逐个比 key，无链、无跳转
```

```cpp
// 构建：count → prefix sum → copy（桶排序，不排桶内）
for (tuple : partitionTuples[p]) directory[slot] += sizeof(T) << 16, directory[slot] |= tag;
for (i = start; i < end; ++i) { val = directory[i] >> 16; directory[i] = (cur << 16) | (u16)directory[i]; cur += val; }
for (tuple : partitionTuples[p]) { target = directory[slot] >> 16; *target = tuple; directory[slot] += sizeof(T) << 16; }
```

⇒ 没有"占位竞争"，也就没有 overflow 到邻居 slot 的概念；代价是**每次命中都要线性扫区间**，
并且**必须物化一份按前缀排好序的 tuple 副本**。

### 1.3 由此推出的六个可观察差异

| 维度 | DuckDB | 论文 | 谁占优 |
|---|---|---|---|
| 无匹配路径 | 1 次目录 load + 16-bit salt 精确比较（FPR 1/65536） | 1 次目录 load + 16-bit Bloom（FPR 1/169） | **DuckDB** |
| 多匹配路径 | 链式 N 次随机访问 + N 轮 `Next()` | 区间顺序扫，`for` 循环 | **论文**（graph 上 20×） |
| 构建 | 增量插入，1–2 次 CAS/行，无排序无拷贝 | 全量重建，2 趟扫描 + 1 次散写/行 + 整行拷贝 | **DuckDB**（小 build 快 30%） |
| 并行 build | CAS 原子插入，靠硬件 | 分区后零同步，8 线程近峰值 | **论文** |
| 空间 | dir 8B × next_pow2(2n)（25–50% 负载）+ 行内 8B 链指针 | dir 8B × ≈1.54n（65% 负载）+ 16B/行（我们的适配版） | 大致持平，见 §3.6 |
| 增量/外部 | 支持（spill + 多轮） | 不支持（必须全量已知） | **DuckDB** |

### 1.4 成本模型：你的判断在什么条件下成立

DuckDB 现状每行 build 成本 ≈ 1 次目录随机 RMW（CAS）。
论文每行 build 成本 ≈ 1 次直方图 RMW + 1 次拷贝写（+ 我们的变体再加 1 次 16B ptr 写）。
DuckDB 每次 probe 命中成本 ≈ 1 dir load + 1 row load + 链跳；论文 ≈ 1 dir load + 1–2 次连续 row load。

于是净收益的粗略临界条件：

```
收益 ≈ probe_rows × P(hit) × [1 次随机读 + 链跳的 cycles]
成本 ≈ build_rows × [1–2 次随机写 + 拷贝的 cycles]
回本条件 ≈ probe_rows / build_rows > (build 每行多花 cycles) / (probe 每行省下 cycles) ≈ 2~4 倍
```

论文自己的数据就是这个结论的实证（Fig.13）：
- 绝大多数查询**持平**；
- 极小 build：chained 快 **30%**（分区 + 拷贝纯亏）；
- 大 build 且单分区装不进 L1/L2：**回退**（论文承认是分区数启发式失准）；
- 只有 >100ms 的 graph 查询（大量重复 key）：**最高 20×**。

⇒ 论文的赢面在 **skew + 强不对称（probe ≫ build）**，而不是"更快的哈希"。
你上一轮的质疑——"排序/拷贝开销在很多情况下大于 probe 的随机访问开销"——**论文自己承认并测出来了**。

---

## 2. 复现方案总览

前提：**不修改 DuckDB 原有 `JoinHashTable`，新写一个同层实现**，由 setting 切换，external 路径回退旧实现。
核心取舍：**不复制 tuple，复制 `(hash, row_ptr)` 16 字节记录**。

理由：DuckDB 的行在 `TupleDataCollection` 里是变长（validity bitmap + 变长 heap），物理重排会破坏 block 结构、
且 string 类型必须搬 heap 指针，代价远高于论文里"整行 ≤ 2× cache line 直接 vector copy"的前提。
把"按前缀排序"降级为"按前缀排序的指针数组"，得到的是**逻辑 adjacency array**：
区间扫描变成顺序读 16B 记录，再逐条 deref 到行——和 DuckDB 现状的 `entry → row` 间接次数相同，不多花钱。

**副作用（正面）**：不需要论文 §3.4 的"大 tuple 退化为链"回退，因为从来没拷贝过行。

---

## 3. 分阶段实现

### 3.1 数据结构

```cpp
// 目录项：与论文 Fig.2 同位布局（指针在高 48 位，tag 在低 16 位）
// 注意与 DuckDB ht_entry_t 相反（DuckDB: salt 高 16 / ptr 低 48）
struct unchained_entry_t {
    uint64_t offset_and_tag;   // [47:16] = adjacency 数组下标，[15:0] = 16-bit register-blocked Bloom
    uint64_t GetOffset() const { return offset_and_tag >> 16; }
    uint16_t GetTag()    const { return static_cast<uint16_t>(offset_and_tag); }
};

// 邻接数组元素：只存 hash + 行指针，行本身不动
struct adjacency_t {
    hash_t hash;        // 用于 slot 归属校验 / 可选二次比较
    data_ptr_t row;     // 指向 TupleDataCollection 中的行
};                      // 16 字节，可整块 vector 拷贝

class UnchainedHashTable {
public:
    // build 阶段：由 sink 收集，等价于论文 §3.3.1
    void Sink(DataChunk &build_chunk, SelectionVector *sel, idx_t count);
    void Combine(UnchainedHashTable &other);

    // finalize：count → prefix sum → copy，等价于论文 §3.3.2/3.3.3
    void Finalize();                       // 单线程入口，内部按分区并行
    void FinalizePartition(idx_t p);       // 每个 task 处理一个 partition

    // probe：区间扫描
    idx_t Probe(const Vector &hashes, const SelectionVector &sel, idx_t count,
                SelectionVector &match_sel, Vector &row_pointers);

private:
    BufferHandle directory;     // 8B × m
    BufferHandle adjacency;     // 16B × n，按 slot 有序
    idx_t slot_bits = 0;        // K
    idx_t radix_bits = 4;       // R，需满足 K >= R
    idx_t slot_mask = 0;
    idx_t shift = 0;            // 64 - K
};
```

### 3.2 三阶段与 DuckDB pipeline 的映射

| 论文阶段 | DuckDB 落点 | 说明 |
|---|---|---|
| §3.3.1 tuple collection + radix partition | `RadixPartitionedTupleData`（`sink_collection`，`initial_radix_bits=4`）+ `HashJoinLocalSinkState` | **已存在，直接复用** |
| §3.3.2 count | `HashJoinFinalizeEvent::GetTasks` 派发的 per-partition task | 新增：读行内 hash（`pointer_offset`）→ 直方图 |
| §3.3.2 prefix sum | partition 级 + partition 内 slot 级，两级 | 见 3.3 |
| §3.3.3 copy | 同一 task，写 `adjacency[base_p + ...]` | 只写 16B，不写行 |
| §3.1 probe | `ScanStructure` 的同层替代 | 见 3.4 |

**一个 DuckDB 独有的便宜**：`PrepareFinalize` 时 `Count()` 已精确已知（不像论文只能靠基数估计）。
所以论文 §4 承认的最大缺陷（"partition count 只能靠启发式，过大过小都会退化"）在这里可以直接消除：
按精确 n 选 `R` 与 `K`，一次算对。这算是对论文的一点"白捡的改进"。

### 3.3 槽位与分区的对齐（最容易写错的地方）

必须满足：`partition = f(slot)` 是**单调的区间映射**，否则 prefix sum 的区间会跨分区。

```cpp
// K = slot bits，R = radix bits，K >= R
shift   = 64 - K;
slot    = hash >> shift;            // 高 K 位
part    = slot >> (K - R);          // 再取高 R 位 → partition
```

这与 DuckDB 现有的 `bucket_shift` 完全同构（`AllocatePointerTable`）：

```cpp
bucket_shift = use_radix_layout && radix_bits > 0 && capacity_bits >= radix_bits
             ? RadixPartitioning::Shift(capacity_bits) : 0;
// 查询：slot = (hash >> bucket_shift) & bitmask
```

所以：**partition p 独占 slot 区间 `[p << (K-R), (p+1) << (K-R))`**，每个 task 只需对自己那段做 count/prefix/copy，
目录不存在跨分区写竞争。这是整个并行方案能零同步的原因。

`K` 的取法（对齐论文的 m = 2^⌈log2(1.125n)⌉，即 65% 负载）：

```cpp
K = max(radix_bits, ceil(log2(n * 1.125)));   // 上限受 48-bit 偏移限制
R = clamp(ceil(log2(n * tuple_width / L2_TARGET)), radix_bits_min, 8);
```

### 3.4 Probe：输出协议是成败关键

DuckDB 的 `ScanStructure` 是**多轮语义**：一次 `Next()` 只走一轮链，`AdvancePointers()` 推进，
`chains_longer_than_one` 决定是否还有下一轮。新结构**没有轮次概念**——一个 probe key 的全部匹配在同一段区间里。

```cpp
idx_t UnchainedHashTable::Probe(const Vector &hashes, const SelectionVector &sel, idx_t count,
                                SelectionVector &match_sel, Vector &row_pointers) {
    idx_t out = 0;
    auto h = FlatVector::GetData<hash_t>(hashes);
    auto ptrs = FlatVector::GetData<data_ptr_t>(row_pointers);

    for (idx_t i = 0; i < count && out < STANDARD_VECTOR_SIZE; i++) {
        const auto row_idx = sel.get_index(i);
        const auto hash = h[row_idx];
        const auto slot = hash >> shift;

        const auto entry = directory_ptr[slot];
        if (!CouldContain(entry.GetTag(), hash)) continue;   // 与论文 Fig.6 等价

        const auto begin = directory_ptr[slot - 1].GetOffset();   // 依赖 dir[-1] 哨兵
        const auto end   = entry.GetOffset();
        for (auto j = begin; j < end && out < STANDARD_VECTOR_SIZE; j++) {
            match_sel.set_index(out, row_idx);   // LHS 索引允许重复
            ptrs[out] = adjacency_ptr[j].row;    // RHS 指针顺序追加
            out++;
        }
        // 若 out 打满：把 (row_idx, j) 存进 state 的剩余游标，下次 Next() 从这里续
    }
    return out;
}
```

要点：

1. **区间内不校验 key** 是错的——必须校验。上例省略了比较，实现时应把 `[begin,end)` 的候选
   框成 `(sel, pointers)` 批量喂给现有 `RowMatcher`（`ht.row_matcher_build.Match(...)`），
   由它统一处理所有类型（含 varlen、嵌套）。这样可以零改动复用 DuckDB 的类型系统。
2. **LHS 索引重复 + RHS 顺序指针**：正好是 PR #14956 的 `rhs_pointers` + `UpdateCompactionBuffer` 形态，
   已有 `GatherResult(Vector&, idx_t, idx_t)` 重载可以直接用，一次 gather 物化。
3. **截断恢复**：只需保存每个未完成 key 的 `(probe_idx, next_j)`，比 DuckDB 现在用 `last_sel_vector` 重放一整轮链要便宜得多。
4. **join 类型语义**：
   - `SEMI/ANTI/MARK`（`insert_duplicate_keys == false`）：区间内首命中即停，天然比链快；
   - `LEFT/SINGLE`：靠 `vfound` 标记，未命中仍要产出 NULL 行，与现有逻辑同构；
   - build 侧 NULL key：沿用 `PropagatesBuildSide` / `NullValuesAreEqual` 的过滤规则，不要自己另写一套。

### 3.5 与现有代码的边界

```
PhysicalHashJoin
├── Sink/Combine            ← 复用（RadixPartitionedTupleData + 本地表）
├── PrepareFinalize         ← 复用（内存预留、external 判定）
├── Finalize
│   ├── in-memory + 开关开  → UnchainedHashTable::Finalize()      [新]
│   └── external / 开关关   → 原 JoinHashTable 路径               [不动]
└── Probe
    ├── 开关开  → UnchainedScanStructure（区间扫描 + 剩余游标）     [新]
    └── 开关关  → 原 ScanStructure                                [不动]
```

不建议做的事：
- 不要改 `ht_entry_t`、`DataChunk`、`TupleDataCollection` —— 全在核心数据结构上，回归风险不可控；
- 不要动 `PhysicalHashJoin::Finalize` 的 external 分支 —— 新结构天然不支持增量 build，硬塞会引入难以复现的 bug。

### 3.6 内存核算

设 build 行数 n、行宽 T（不含链指针）、slot 数 m：

| 项 | DuckDB 现状 | unchained 适配版 |
|---|---|---|
| 目录 | 8B × next_pow2(2n) ≈ 16–32B/行 | 8B × next_pow2(1.125n) ≈ 12–24B/行 |
| 行内链指针 | 8B/行（`pointer_offset`） | **可省**（复用同一 8B 存 hash） |
| 邻接数组 | — | 16B/行 |
| 合计（n=10M，T=40B） | ≈ 268MB + 80MB = 348MB | ≈ 134MB + 160MB = 294MB |

⇒ **总量大致持平甚至略低**，因为负载率从 25–50% 提到 65%，且省掉了行内链指针。
但峰值内存的**形态**变了：邻接数组是一次性 16n 字节的连续块（`AllocateHuge`），
external 判定的 `total_size` 估算必须相应更新，否则会在边界处误判。

### 3.7 削减构建开销的四个手段

1. **分区压到 L2**：论文的拷贝是"散写到自己的分区"，只要单分区（tuple 副本）装得进 L2，散写就是 L2 命中。
   用精确 n 选 `R`，让 `n/P × 16B` 落在 256KB–1MB。
2. **计数用 u32 数组而非 16-bit 打包**：省掉位域运算，直方图阶段本来就是 build 的瓶颈。
3. **两趟合一的可能性**：DuckDB 的行 hash 已存在 `pointer_offset`，count 阶段不需要重算 hash，
   只需一次非连续读；copy 阶段已是顺序读 + 顺序写。这是比论文更有利的起点。
4. **不做全序排序**：只要按前缀分桶（桶内无序），就避免了比较排序；论文原本就是这样，别画蛇添足去桶内排序。

### 3.8 可选快路径：key 内联

若 build 侧 key 全为定长且 ≤ 16B（`int32/int64/date/decimal(p≤18)` 等），可把 key 直接写进 `adjacency_t`
的 16B 里（hash 用 4B 去掉，行指针 8B，key ≤ 8B），probe 时 **完全不 deref 行**，
用 SSE/AVX2 一次比较 4–8 个候选。这是论文原样结构在 DuckDB 里的可达形态，但需要
`IsFixedSize && !HasNull` 判定 + 与 `RowMatcher` 二选一，建议放到最后一个里程碑。

### 3.9 里程碑与验证

| 里程碑 | 内容 | 验收 |
|---|---|---|
| M0 | 独立 microbench：复现论文 Fig.3（multiplicity）/ Fig.5（selectivity） | 曲线形状与论文一致，量级不差 2× |
| M1 | `UnchainedHashTable` 单体：build from `TupleDataCollection` + probe API | 随机数据 join 结果与 `std::unordered_multimap` 完全一致 |
| M2 | 接入 `PhysicalHashJoin`，setting `hash_join_algorithm=unchained` | `sqllogictest` 全绿；TPC-H SF1 结果一致 |
| M3 | probe 侧剩余游标 + 2048 截断 + 所有 join 类型 | LEFT/SEMI/ANTI/MARK/SINGLE 全部通过 |
| M4 | 精确分区选择 + 去掉行内链 | build 时间相对 M3 再降 |
| M5 | key 内联 SIMD / crc32 私有 hash | 仅在定长 key 上生效，需单独开关 |

性能对比口径（务必分开计时，否则会得到论文 Fig.13 那种"持平"的糊结论）：
- build 阶段：`Sink + Combine + Finalize` 各自计时（`EXPLAIN ANALYZE` 的 operator timing 不够细，建议在 `Finalize` 内埋点）；
- probe 阶段：总时间 / probe 行数 = ns per probe，直接对齐论文 Fig.3/Fig.5 的坐标；
- 三组负载：TPC-H（1:n 无 skew）、JOB（高选择性）、自造 skew（Zipf 分布 key，multiplicity 1–1000）。

---

## 4. 与论文的四点必然偏离（要提前说清）

| 论文 | 本方案 | 原因 |
|---|---|---|
| 拷贝整行到连续存储 | 拷贝 `(hash, row_ptr)` | DuckDB 行为变长 row format，整行拷贝会破坏 block 结构 |
| 大 tuple 退化为链（Fig.11） | 不需要 | 从未拷贝行 |
| 分区数靠基数估计 | 用精确 `Count()` | DuckDB `PrepareFinalize` 时行数已知 |
| 构建可完全增量 | 必须全量 | 计数排序要求先知道分布；不支持 external 多轮 |

代价一句话：**每个候选多一次 deref**（邻接项 → 行），换来的是"链跳 + 多轮 Next"彻底消失。
DuckDB 现状本来就有这次 deref（`entry → row`），所以 probe 侧不多花钱；真正的成本全在 build。

---

## 5. 与你在做的 GPU join 的关联

这个结构对 GPU 的适配性远好于 DuckDB 现状：

- **区间映射 = 天然合并访存**：一个 slot 的候选是一段连续内存，warp 内 32 个 lane 可以按区间切片并行；
  DuckDB 的链是 pointer chasing，在 GPU 上是最坏形态（每次依赖加载 ~500 cycle 且无法合并）。
- **build 阶段可以整体下沉**：count → prefix sum（GPU 上就是 scan）→ scatter，三步都是 GPU 的强项，
  且 `adjacency_t` 是定长 16B，非常规整。
- 但注意：你的 `PerfectHashJoinExecutor` 走的是**完美哈希**（无冲突、直接下标），
  和 unchained 是两条路：前者适用于小整数域、零冲突；后者适用于任意 key、有冲突但要吃 skew。
  如果要往 GPU 上搬，先确认 key 分布——如果 key 域小且密集，完美哈希仍然更优，不需要引入区间扫描。

---

## 附：一句话总结三者的关系

| 设计 | 冲突本质 | 多匹配代价 | 构建代价 |
|---|---|---|---|
| DuckDB 1.5+ | 点映射 + 行内链（同 key） + 线性探测（异 key 同 salt） | O(链长) 随机访问 | 最低（增量 + CAS） |
| 论文 unchained | 前缀区间映射，区间内混装 | O(1) 次连续扫描 | 最高（两趟 + 散写 + 拷贝） |
| 本方案 | 前缀区间映射 + 指针邻接数组（不搬行） | O(1) 次连续扫描 + 每候选 1 次 deref | 中（两趟，但只写 16B） |
