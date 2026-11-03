# Unchained Hash Table 复现进展

> 论文: *Simple, Efficient, and Robust Hash Tables for Join Processing* (DaMoN'24)
> 目标: 在 DuckDB 执行框架内复现该论文的 unchained hash table，作为 `JoinHashTable` 的同层替代品，
> 并保证 (1) 与执行器/pipeline 正确集成、(2) 能接入上一轮做的 Join micro-bench。
>
> 本文记录「做到哪一步 / 当前拿到了什么结果 / 还差什么」。

---

## 1. 当前状态速览

| 项目 | 状态 |
|---|---|
| 核心数据结构 (directory + adjacency + tag 表) | ✅ 完成 |
| 构建三步 (Counting → Prefix sum → Copy) | ✅ 完成，零同步并行 |
| 探针 (Bloom andn 判定 + 区间扫描 + 可续跑游标) | ✅ 完成 |
| 与 `JoinHashTable::Probe` / `ScanStructure` 的集成 | ✅ 完成，全部 join 类型可用 |
| 与 `PhysicalHashJoin::Finalize` 的集成 | ✅ 完成，external/BHJ 自动回退 |
| setting 开关 `open_unchained_join` | ✅ 完成（默认 false） |
| 接入 Join micro-bench（第 4 条路径） | ✅ 完成 |
| SQL 层正确性验证（7 类 join，开关开/关结果一致） | ✅ 通过 |
| 微基准 14 个场景全量通过 | ✅ 418 assertions / 14 test cases |
| BHJ 回归 (`[bitmap_join]` 22 用例) | ✅ 通过 |
| 全量 `unittest` 回归 | ⚠️ 未跑完（见 §7） |

---

## 2. 代码清单

### 新增

| 文件 | 内容 |
|---|---|
| `src/include/duckdb/execution/unchained_hash_table.hpp` | `unchained_entry_t` / `unchained_adjacency_t` / `UnchainedHashTable` 声明 + 与论文差异的详细注释 |
| `src/execution/unchained_hash_table.cpp` | tag 表生成、Build 三步、并行、`GetRowPointers`、`AdvancePointers`、重复 hash 检测 |

### 修改

| 文件 | 改动 |
|---|---|
| `src/include/duckdb/main/settings.hpp` | 新增 `OpenUnchainedJoinSetting`（`open_unchained_join`，默认 false，index 97）；`MaxSettingIndex` 97→98 |
| `src/main/config.cpp` | 注册该 setting（插在 `OpenPerfectJoinSetting` 之后，保持字母序） |
| `src/include/duckdb/execution/join_hashtable.hpp` | ① 新增 `struct UnchainedScanState`（每 probe 行的 cursor/end/hash）<br>② `ScanStructure` 增加 `unchained_state` 与 `AdvanceUnchainedPointers()`<br>③ `JoinHashTable` 增加 `unique_ptr<UnchainedHashTable> unchained` |
| `src/execution/join_hashtable.cpp` | `Probe()` 与 `ScanStructure::AdvancePointers()` 两处**分派**：`unchained` 非空时走 unchained，否则走原链式路径 |
| `src/execution/operator/join/physical_hash_join.cpp` | `HashJoinGlobalSinkState::use_unchained_join`；`Finalize()` 在 `Unpartition()` **之前**构建 unchained，并跳过 `ScheduleFinalize()` |
| `src/execution/CMakeLists.txt` | 加入 `unchained_hash_table.cpp` |
| `test/microbench/join_microbench.{hpp,cpp}` | 新增第 4 条路径 `UNCHAINED_JOIN`；新增分布 `UNCHAINED_SLOT_COLLISION`；`MurmurHash64Inverse` 提为公开函数 |
| `test/microbench/test_join_microbench.cpp` | 新增 unchained 断言 + 一个新场景 |

---

## 3. 论文思想 vs 实际实现（含偏差说明）

### 3.1 一致的部分

| 论文 | 实现 |
|---|---|
| `directory` 每项 8 字节：高 48 位 = 该 slot 在 adjacency 的**上界**，低 16 位 = 4-bit 阻塞 Bloom tag | 完全一致 |
| `directory[slot-1]` 给区间下界，需要多分配 1 项使 `directory[-1]` 合法 | 完全一致（分配 `m+1`，`directory = base + 1`，`directory[-1] = 0`） |
| 桶排序式构建：Counting → Prefix sum → Copy | 完全一致 |
| Counting 时 `directory[slot] += 1<<16; directory[slot] \|= tag;` | 完全一致（用 `unchained_entry_t::IncrementOffset` 保证不污染 tag） |
| Prefix sum 按分区做，`start = (p<<K)/P, end = ((p+1)<<K)/P` | 一致（用 `p * (m >> R)` 算区间） |
| 探针 5 条指令 andn：`!(tag & ~entry)` | 一致 |
| tag 表 = C(16,4)=1820 个「16 位里恰好 4 位为 1」的模式，补齐到 2048 | 一致（补齐用循环复用，避免退化成"全放行"） |
| 分区后构建零同步（无 atomic/CAS） | 一致 |
| m = 1.125 n | 一致（`SLOT_LOAD_FACTOR = 1.125`，向上取到 2 的幂，下界 `2^14`） |

### 3.2 与论文**不同**的地方（必须在报告里写明）

**(A) slot 取 hash 的 bit [48-K, 47]，而不是最高 K 位。**

DuckDB 的 `RadixPartitioning::Shift(radix_bits) = 48 - radix_bits`，即分区号 = hash 的 bit [48-R, 47]（最高 16 位被 DuckDB 留作 salt）。把 slot 定义在同一个起点上，就有

```
partition = slot >> (K - R)
```

于是「分区 p 恰好覆盖一段连续的 slot 区间」，可以直接复用 `sink_collection` 的**物理分区**做零同步并行，**不需要自己再拷贝一遍行**。
代价：hash 最高 16 位被浪费。

**(B) Bloom tag 取样位改为 hash 的最低 11 位（bit [0,10]），而不是论文的「低 32 位的 top 11」。**

因为 (A) 之后 slot 占了 bit [48-K, 47]，必须让 tag 位与 slot 位不相交，否则同一 slot 内所有 key 的 tag 相同、过滤器完全失效。
约束：`K < 38`，因此 `MAX_SLOT_BITS = 34`（directory 最大 16G 项 / 128GB）。超出后 m 被夹紧、负载因子退化。

**(C) adjacency 存 `{row_ptr(8B), hash(8B)}` 16 字节，而不是把整个 tuple 拷进来。**

DuckDB 的 build 行已经物化在 `TupleDataCollection` 里，再拷一遍代价太大；存 `row_ptr` 就够 `Gather`。
额外 8B 的 hash 是论文 Bloom 过滤器在「行不连续」场景下的等价替代品：Bloom 判定命中后先比 hash，再决定是否解引用行（随机访存）。

副作用（**实测主要劣势来源**）：
- 内存：`9n (directory) + 16n (adjacency) = 25n`，高于 DuckDB 的 `16n`（capacity=2n 的 8 字节 pointer table）。
- 探针多一跳：DuckDB = `pointer table → row`；unchained = `directory → adjacency → row`。
  directory 与 adjacency 命中时各多一次缓存不命中，这是 §5 里「全命中时 unchained 反而慢 ~20%」的主因。

**(D) 并行度上限 = DuckDB 的 radix 分区数（16）。**

论文自己分区所以 R 随便选；这里刻意**不改** `INITIAL_RADIX_BITS`（=4）。理由：改了会同时改变 sink 阶段的代价，和另外 3 条路径的对比就不公平了。

**(E) 用「64 位 hash 相等」代替论文的 `cur->key == key`。**

为了保住 (C) 的性能收益，默认不做精确 key 比较。构建末尾会做一遍 `DetectDuplicateHashes`：
只要发现某个 slot 里有两条记录 hash 相同（重复 key 必然落在这里），就把 `needs_chain_matcher` 打开、
并补建一个基于 `equality_predicates` 的 `RowMatcher`，退化成精确比较（牺牲性能换 100% 正确）。
只有真正的 64 位 hash 碰撞（概率 ≈ n²/2⁶⁴）才会漏，而检测逻辑对此是保守的。

**(F) 探针的游标是可续跑的（per-probe-row cursor）。**

论文是 `for (cur = start; cur < end; ++cur) if (cur->key == key) produce(cur)`，一口气产出。
DuckDB 的结果 chunk 上限 2048 行，且要走 `HAVE_MORE_OUTPUT` 语义，所以拆成
「每次 `AdvancePointers` 每条 probe 行前进到**下一条 hash 相同**的记录」。

**(G) `chains_longer_than_one` 的语义被重新定义。**

DuckDB 用它判断「链上是否可能有多条记录 → 要不要走 `NextInnerJoin` 的 compaction 慢路径」。
对 unchained，一条 probe 行最多产出多少结果取决于「同一 slot 里有多少条 hash 相同的记录」，
所以这里绑定的是 `has_duplicate_hashes`，**不是** `max_slot_count >= 2`。（第一版绑错了，实测 probe 从 104ms 降到 95ms。）

**(H) unchained 路径下跳过 join filter pushdown。**

`FinalizeFilters` 依赖 `InsertHashes` 顺带填充 pushdown 用的 Bloom filter，而 unchained 不走 `InsertHashes`。
所以 `use_unchained == true` 时不算 `filter_min_max`、不调用 `FinalizeFilters`：正确性没问题（只是 probe 侧少一层过滤），
但对**有 min/max 动态过滤**的查询会不公平，对比时需要注意。

---

## 4. 与执行器 / pipeline 的集成

```
PhysicalHashJoin
├── Sink / Combine / PrepareFinalize        ← 完全不改，仍走 RadixPartitionedTupleData + 本地 HT
├── Finalize (in-memory 分支)
│   ├── Merge(所有 local HT)
│   ├── [新增] 记录每个 radix 分区的起始行号
│   ├── [新增] UnchainedHashTable::Build()   ← 必须在 Unpartition() 之前
│   ├── Unpartition()
│   ├── perfect hash join 判定（unchained 优先，见下）
│   └── 原 ScheduleFinalize()               ← unchained 命中时跳过
└── Probe (ExecuteInternal)
    ├── ht->Probe(...)                      ← 分派到 UnchainedHashTable::GetRowPointers
    └── scan_structure.Next(...)            ← NextInnerJoin/NextLeftJoin/NextSemiJoin/... 全部不改
                                               AdvancePointers 分派到 UnchainedHashTable::AdvancePointers
```

**关键顺序约束**：`UnchainedHashTable::Build()` 必须在 `Unpartition()` **之前**跑，因为它需要 `sink_collection`
的物理分区。而 `TupleDataCollection::Combine` 只是搬移 segment 指针（`AddSegment(std::move(other_seg))`），
所以写入 adjacency 的行地址在随后的 `Unpartition()` 之后依然有效 —— 这一点是整套集成能成立的前提。

**回退条件**（任一成立就走原实现）：`open_unchained_join=false` / `external` / BHJ / build 侧为空。

**优先级**：`perfect hash join > unchained > 普通 hash join`。
之所以让 unchained 优先于 perfect，是因为 `Build()` 必须在 `Unpartition()` 之前跑，无法等 perfect 判定结果。

---

## 5. 当前实测结果

机器：96 核，`build/release` Release 构建。单位 ms，取 `unittest "Join micro-bench: *"` 单轮结果。
`join_only = build + finalize + probe`（不含 scan），横向对比看这一列。

### 5.1 基线：维度表子集（build 500K / PK 域 1M / probe 8M / hit=1.0 / 4+4 线程）

| 算法 | build | finalize | probe | join_only |
|---|---:|---:|---:|---:|
| hash-join | 10.16 | 13.71 | 89.75 | **113.62** |
| perfect-hash-join | 5.94 | 25.54 | 25.52 | **56.99** |
| bitmap-join (BHJ) | 3.85 | 0.00 | 33.79 | **37.64** |
| **unchained-join** | 8.54 | 15.42 | 107.65 | **131.61** |

unchained 附加信息：`slots=1048576  max_slot_len=8  directory=8MB  adjacency=7MB`

### 5.2 命中率扫描（build 500K / probe 4M）——**论文的核心结论在这里**

| hit_ratio | hash probe | unchained probe | BHJ probe | unchained vs hash |
|---:|---:|---:|---:|---|
| 1.0 | 48.02 | 59.95 | 15.06 | **+25%（更慢）** |
| 0.5 | 49.01 | 48.33 | 22.46 | 持平 |
| 0.1 | 40.44 | 24.24 | 12.78 | **-40%（更快）** |
| 0.0 | 34.63 | **10.55** | 7.55 | **-70%（快 3.3×）** |

→ 命中的 probe 行越少，unchained 优势越大。
原因是 Bloom 过滤器能在**不触碰任何行**的情况下整块拒绝（论文 §3.2 的主打场景：选择性 join / anti-join / 未命中的 FK）。
全命中时反而慢，就是 §3.2(C) 说的「adjacency 多一跳」。

### 5.3 线程扩展（build 1M / probe 4M / hit=1.0）

| 线程数 | hash probe | unchained probe |
|---:|---:|---:|
| 1 | 190.78 | 293.09 |
| 8 | 38.72 | 45.92 |

1 线程下 unchained 落后更多（1.54× vs 1.19×），怀疑是额外的 directory/adjacency 访存延迟在单线程下
缺少 memory-level parallelism 掩盖。这一点还没深挖。

### 5.4 两个最坏情况（都符合论文预期）

| 场景 | hash-join probe | unchained probe | 说明 |
|---|---:|---:|---|
| `hash-collision`（10K 个 key 全挤进 DuckDB 同一个桶且 salt 相同） | 1177.90 | **180.49** | DuckDB 插入 360ms / 探测 O(链长×chunk)；unchained 用顺序扫描扛过去，快 6.5× |
| `unchained-slot-collision`（10 万 key 全挤进 unchained 同一个 slot，`max_slot_len=390`） | 24.06 | **174.66** | unchained 的软肋：区间被拉长且 hash 各不相同 → 每 probe 行扫 390 条，慢 7.3× |

### 5.5 SQL 层正确性（C++ API，开关开/关逐条比对）

`INNER / LEFT / semi(IN) / ANTI / RIGHT / 非等值 / DISTINCT 子查询 join` 共 7 条查询，
`open_unchained_join=false` 与 `=true` 结果**逐字节一致**。

### 5.6 回归

- `unittest "[bitmap_join]"`：**22 test cases / 200494 assertions 全通过**
- `unittest "Join micro-bench: *"`：**14 test cases / 418 assertions 全通过**
- `unittest "Join micro-bench: harness self-check"`（非隐藏，含 MurmurHash64 求逆自检）：141 assertions 通过
- `unittest "[settings]"`：26 用例中 3 个失败（`allowed_paths` / `setting_threads` / `test_disabled_file_systems`），
  失败信息都是 `Cannot change ... when enable_external_access is disabled` 一类，**与本次改动无关**（环境沙箱导致）；
  真正会枚举所有 setting 的 `setting_exhaustive.test` 通过。⚠️ 建议用改动前的二进制复核一次确认是既有失败。

---

## 6. 过程中发现并修掉的两个 bug

1. **`MurmurHash64Inverse` 多做了一次乘逆元。** 正向是 `A B A B A`（5 步），逆应是 `A⁻¹B⁻¹A⁻¹B⁻¹A⁻¹`（3 次 undo + 2 次乘），
   第一版写成 3+3。结果「构造重复 hash」的功能**完全没有生效**（round-trip 正确率 0/26）。
   现已修正，并在 micro-bench 自检里加了 `MurmurHash64Inverse(MurmurHash64(v)) == v` 的 REQUIRE 防回归。
   修正后 `hash-collision` 场景才真正出现预期的最坏情况（见 §5.4）。
2. **`max_slot_count` 统计错误**：`local_max` 声明在分区循环外，变成跨分区累加。已改成每分区独立。

---

## 7. 还需要做什么（按优先级）

### P0 —— 结果可信度

- [ ] **跑一次完整 `unittest` 回归**（现在只跑了 3 个 tag）。确认除了上面 3 个环境相关的 settings 用例外没有新失败。
- [ ] **复核那 3 个 settings 失败是既有失败**（用改动前的二进制或 `git stash` 后的构建跑一次）。
- [ ] 微基准目前是**单轮**，波动不小（同一个场景前后两次差 10~20%）。加一个 `repeat` 参数取中位数/最小值。

### P1 —— 性能上还有明显空间

- [ ] **探针循环加软件预取 / 向量化。** 现在是纯标量循环；论文强调的正是「顺序扫描 + 无分支」，
      可以考虑 `__builtin_prefetch(&directory[next_slot])` 之类的批量预取。
- [ ] **消除 adjacency 那一跳。** 把最常用的 1~2 个 payload 列直接内联进 adjacency（回到论文的原始形态），
      probe 时就不用再解引用行。预期能显著改善「全命中」场景（§5.1 里 unchained 唯一的劣势）。
- [ ] 查清 §5.3 里 1 线程下差距变大的原因。

### P2 —— 完整性

- [ ] **external / spill 路径未接入**（目前直接回退）。论文 §3.4 有对应的分区外排方案，要做的话是一大块。
- [ ] **filter pushdown 的 Bloom filter 在 unchained 下被跳过**（§3.2 H）。可以在 Copy 阶段顺手填 `ht.bloom_filter`，
      这样有动态过滤的查询对比才公平。
- [ ] `has_duplicate_hashes` 现在是 slot 内 O(k²) 两两比较、`k > 32` 直接保守置 true。倾斜场景下会误伤性能，
      可以换成「按 hash 排序后判相邻」的 O(k log k)。
- [ ] 并行度上限 16（§3.2 D）。若确需要更高并行度，得自己分区或改 `INITIAL_RADIX_BITS`——但后者会破坏对比公平性，
      要做的话建议只在 unchained 打开时改，并在报告里标明。

### P3 —— 评测

- [ ] 用 `test_bitmap_join_perf.cpp` 那套（TPC-H SF5 / 有 SF5 数据）加一个 `open_unchained_join` 的对比，
      把 BHJ / perfect / unchained / 普通 HJ 四条路在真实查询上放一起比。
- [ ] 写 SQL 层的开关对比测试（类似 §5.5，但固化成 `test/sql` 或 unittest 用例）。
- [ ] 把 §3.2 的 8 条偏差整理进毕设正文的「实现差异」章节（代码注释里已有完整版，可直接引用
      `src/include/duckdb/execution/unchained_hash_table.hpp` 头部）。

---

## 8. 怎么跑

```bash
# 微基准（4 条路径一起跑 + 交叉校验）
./build/release/test/unittest "Join micro-bench: *"
./build/release/test/unittest "Join micro-bench: dense unique PK (hash vs perfect vs bitmap)"

# SQL 层开关
SET open_unchained_join=true;
SELECT ...;

# 回退/对比
SET open_unchained_join=false;
```
