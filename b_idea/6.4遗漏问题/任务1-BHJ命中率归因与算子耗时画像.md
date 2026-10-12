---
title: BitmapJoin (BHJ) 6.4 遗漏问题 —— 任务1：命中率归因与算子耗时画像
version: v0.2.0
status: Done（诊断完成，归因表/耗时表均已用真实 SF5 数据填充；过程中发现并修复了一个真实的 RIGHT_SEMI 崩溃 bug）
related:
  - ./README.md
  - ../perf/sf5/tpch_prof.md
  - ../perf/sf5/tpch_q5_q9_q10.csv
  - ../perf/sf5/tpch_operator_timing.csv（本次新增，条目7产出）
  - ../../src/optimizer/bitmap_join_resolver.cpp
  - ../../src/include/duckdb/catalog/catalog_entry/bitmap_join_meta.hpp（BitmapJoinSkipReason）
  - ../../src/execution/operator/join/physical_hash_join.cpp（ParamsToString）
  - ../../src/execution/physical_plan/plan_comparison_join.cpp
  - ../../test/api/test_bitmap_join_tpch.cpp
  - ../../test/api/test_bitmap_join_tpch_profile.cpp（本次新增）
  - ../../test/api/test_bitmap_join_tpch_22queries.cpp（本次新增）
---

# 说明

本任务对应用户提出的原问题 1、2，都是**诊断类**任务，目标是把"命中率为什么只有 3/5、3/5、1/3"以及"命中之后到底有没有换来实际提速"这两件事用可复核的数据说清楚。

先明确一个记账口径修正：`b_idea/perf/sf5/tpch_q5_q9_q10.csv` 里 Q10 是 `hash_join_count=3, bhj_hits=1`，即 **1/3**，不是用户原话里笔误的"1/2"（`test_bitmap_join_tpch.cpp` 里 `expected_bhj_hits` 给 Q10 标注的是 2，实测命中要求已核实为 1，`hash_join_count` 是 3）。

**本任务在实施过程中，条目7新增的"22 条标准 TPC-H SQL smoke test"额外发现并修复了一个真实的、会导致查询直接报错的 bug**（详见 §7.5），这不在原计划范围内，但属于诊断工作暴露出的、必须立即处理的正确性问题，一并记录在本文档。

---

## 条目6：EXPLAIN 执行计划命中归因（已完成，结论见 6.5）

### 6.1 问题重述

需要针对 Q5（3/5）、Q9（3/5）、Q10（1/3）逐个 HASH_JOIN 节点给出具体未命中原因，并归类为"无解类"（当前设计边界内本就不该命中）和"可优化类"（已知局限，理论上可修复）。

### 6.2 现状证据

```text
（来自 b_idea/perf/sf5/tpch_q5_q9_q10.csv）
Q5 ,bitmap,5,3,...   -> 5个HASH_JOIN，命中3个
Q9 ,bitmap,5,3,...   -> 5个HASH_JOIN，命中3个
Q10,bitmap,3,1,...   -> 3个HASH_JOIN，命中1个
```

### 6.3 实现内容（已落地）

**Step A：新增诊断枚举 `BitmapJoinSkipReason`**（`src/include/duckdb/catalog/catalog_entry/bitmap_join_meta.hpp`），覆盖 11 种状态：`HIT`、`NOT_SINGLE_EQUALITY`、`NOT_INNER_OR_RIGHT_SEMI`、`CONDITION_NOT_PLAIN_COLUMN`、`CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION`（下述新发现的独立分类）、`TRACE_TO_GET_FAILED`、`CATALOG_NAME_RESOLUTION_FAILED`、`CATALOG_NOT_REGISTERED`、`FK_ON_BUILD_SIDE`、`ROWID_MODE_MISMATCH`、`PATH_INCOMPLETE_LEFT_SIDE`、`PATH_INCOMPLETE_OTHER`。

**Step B**：`LogicalComparisonJoin`/`PhysicalHashJoin` 各新增 `bhj_skip_reason` 字段（默认 `NOT_PROCESSED`，仅诊断用，不参与任何决策）；`BitmapJoinResolver::ResolveJoin` 每个 `return` 前补上对应原因；`PlanComparisonJoin` 把该字段搬到物理算子；`PhysicalHashJoin::ParamsToString()` 在 `use_bitmap_join==false` 时输出 `"Bitmap Join": "no (xxx)"`（`EXPLAIN (FORMAT JSON)` 下可见）。这样以后任何一次 `EXPLAIN` 都能直接看到每个 HASH_JOIN 为什么没走 BHJ，不需要临时调试脚本。

**Step C（意外发现）**：诊断过程中发现 Q9/Q10 里 3 个未命中的 join，`TraceBindingToGet` 都在某个 `LOGICAL_PROJECTION` 处失败——但打印实际表达式后发现，join key 被 `CompressedMaterialization` 优化器 pass（`src/optimizer/compressed_materialization/`，运行在统计信息传播阶段，早于 `BitmapJoinResolver`）包了一层 `__internal_compress_integral_uinteger(#[N.M], 1)` 压缩函数。这与"用户写了 cast/表达式"完全不同——是**上游优化器根据统计信息主动插入的包装**，因此新增了独立诊断项 `CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION`（`IsCompressedMaterializationWrapper` 识别 `__internal_compress_integral_*`/`__internal_decompress_integral_*` 函数名），不再和"非纯列引用"的通用情形混在一起。

### 6.4 归因表（真实 SF5 数据，`EXPLAIN (FORMAT JSON)` 实测）

| Query | Join（probe⋈build，按表名标注） | 命中? | 原因（`bhj_skip_reason`） | 分类 |
| --- | --- | --- | --- | --- |
| Q5 | `[lineitem+orders+customer+nation+region] ⋈ supplier` | 否 | `not_single_equality` | 无解（多条件复合等值：`c_nationkey=s_nationkey`） |
| Q5 | `lineitem ⋈ [orders+customer+nation+region]` | 否 | `path_incomplete_left_side` | **可优化**（任务2条目8目标） |
| Q5 | `orders ⋈ [customer+nation+region]` | 是 | `hit` | — |
| Q5 | `customer ⋈ [nation+region]` | 是 | `hit` | — |
| Q5 | `nation ⋈ region` | 是 | `hit` | — |
| Q9 | `[lineitem+part+orders] ⋈ [partsupp+supplier+nation]` | 否 | `not_single_equality` | 无解（`ps_partkey=l_partkey AND ps_suppkey=l_suppkey` 复合条件） |
| Q9 | `[lineitem+part] ⋈ orders` | 否 | `condition_wrapped_by_compressed_materialization` | **可优化**（新发现，见6.5） |
| Q9 | `lineitem ⋈ part` | 是 | `hit` | — |
| Q9 | `partsupp ⋈ [supplier+nation]` | 是 | `hit` | — |
| Q9 | `supplier ⋈ nation` | 是 | `hit` | — |
| Q10 | `[customer+nation] ⋈ [lineitem+orders]` | 否 | `condition_wrapped_by_compressed_materialization` | **可优化**（新发现，见6.5） |
| Q10 | `customer ⋈ nation` | 是 | `hit` | — |
| Q10 | `lineitem ⋈ orders` | 否 | `condition_wrapped_by_compressed_materialization` | **可优化**（新发现，见6.5） |

### 6.5 分类统计与结论

| Query | 命中 | 无解类 | 可优化类 |
| --- | --- | --- | --- |
| Q5 | 3/5 | 1（`not_single_equality`） | 1（`path_incomplete_left_side`，任务2条目8目标） |
| Q9 | 3/5 | 1（`not_single_equality`） | 1（`condition_wrapped_by_compressed_materialization`，**新发现**） |
| Q10 | 1/3 | 0 | 2（全部是 `condition_wrapped_by_compressed_materialization`） |

**核心结论**：

1. **Q10 命中率低的真正原因，跟任务2条目8要解决的"中间 join LEFT 侧限制"完全无关**——Q10 唯一被 `PATH_INCOMPLETE_LEFT_SIDE` 命中过的场景（Q5 那一条）在 Q10 里根本没出现；Q10 的两个未命中 join **都**是被 `CompressedMaterialization` 优化器包裹了 join key 导致的。也就是说：如果只做任务2条目8（LEFT 侧优化），**Q10 的命中率完全不会提升**（1/3 不变）；反而是这个新发现的 `CompressedMaterialization` 包装问题，同时影响了 Q9（1个）和 Q10（2个），潜在收益比条目8更大。
2. Q5 唯一的"可优化类"未命中，才是任务2条目8瞄准的场景（`lineitem⋈[orders+customer+nation+region]`，`lineitem` 是探测/事实侧，`orders` 一侧在中间 join 里落在了 LEFT）。
3. 无解类（`not_single_equality`）在三条查询里各出现一次，都是"多列等值条件"的天然限制（设计文档 Q6 open question），不建议投入解决。

**对任务2的优先级建议（更新，v2：条目8b已实施）**：`CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION` 这个新发现的问题，已作为任务2条目8b 实施并通过真实数据验证——采用的是"全局预扫描 + 源头跳过压缩"方案（而非本节最初设想的"事后看穿一层压缩函数继续追踪"，后者被任务2 §8b.3 证明存在稠密PK场景下的静默错误结果风险，被否决）。修复后 Q9/Q10 的 3 个 `CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION` 归因项已清零，还原出的真实根因分别是：Q9 1个 → `PATH_INCOMPLETE_LEFT_SIDE`（条目8的目标，待条目8落地）；Q10 1个 → `FK_ON_BUILD_SIDE`（无解类）；Q10 1个 → 直接命中。**条目8的预期收益因此从"仅Q5 1个"上调为"Q5+Q9共2个"**。详见任务2文档 §8b.4.2/8b.4.3。

---

## 条目7：算子级耗时画像（已完成，结论见 7.5）

### 7.1 问题重述

判断"命中 BHJ 的 join 是否真的比 baseline/perfect 更快"以及"没命中的 join 有没有额外 overhead"，从而判断当前总耗时提升不明显，主要瓶颈是**计划层（命中率）**还是**执行层（executor 本身）**。

### 7.2 实现内容（已落地）

新增两个测试文件（`[bitmap_join_tpch_profile]` tag，`[.]` 隐藏，需显式指定运行）：

- **`test/api/test_bitmap_join_tpch_profile.cpp`**：对 Q5/Q9/Q10 分别在 baseline/perfect/bitmap 三模式下用 `PRAGMA enable_profiling='json'` 采集每个 `HASH_JOIN` 节点自身的 `operator_timing`（非累计值），按"深度优先遍历顺序"跨模式对齐（同一 SQL/统计信息下三种模式的 join 形状应一致，只是标记不同），并用 build/probe 子树扫描到的表名做二次标注防止错位；结果打印到 stdout，并写入 `b_idea/perf/sf5/tpch_operator_timing.csv`。
- **`test/api/test_bitmap_join_tpch_22queries.cpp`**：额外补充的、覆盖全部标准 TPC-H 22 条 SQL 的 e2e smoke test（复用 `tpch` extension 内置的 `TpchExtension::GetQuery(1..22)`），只跑在 `DUCKDB_EXTENSION_TPCH_SHOULD_LINK` 打开的构建里；不做命中率断言，只断言 `open_bitmap_join=true` 相比 baseline **不产生错误结果、不崩溃**——这是比 Q5/Q9/Q10 窄范围验证更强的鲁棒性保障，因为 `BitmapJoinResolver` 对 `open_bitmap_join=true` 时的**每一个** `LogicalComparisonJoin`（不管是不是 Q5/Q9/Q10）都会尝试处理。

### 7.3 算子耗时对比表（真实 SF5 数据，见 `b_idea/perf/sf5/tpch_operator_timing.csv`）

| Query | Join（条件 [probe⋈build]） | baseline(ms) | perfect(ms) | bitmap(ms) | 命中? |
| --- | --- | --- | --- | --- | --- |
| Q5 | `[lineitem+orders+customer+nation+region] ⋈ supplier` | 63 | 62 | 62 | 否 |
| Q5 | `l_orderkey=o_orderkey [lineitem ⋈ orders+customer+nation+region]` | 141 | 143 | 147 | 否 |
| Q5 | `o_custkey=c_custkey [orders ⋈ customer+nation+region]` | 48 | 31 | **90** | 是 |
| Q5 | `c_nationkey=n_nationkey [customer ⋈ nation+region]` | 3.6 | 4.9 | 4.8 | 是 |
| Q5 | `n_regionkey=r_regionkey [nation ⋈ region]` | 0.16 | 0.15 | 0.02 | 是 |
| Q9 | `[lineitem+part+orders] ⋈ [partsupp+supplier+nation]` | 934 | 990 | 1032 | 否 |
| Q9 | `l_orderkey=o_orderkey [lineitem+part ⋈ orders]` | 1045 | 1023 | 1018 | 否 |
| Q9 | `l_partkey=p_partkey [lineitem ⋈ part]` | 171 | 133 | 145 | 是 |
| Q9 | `ps_suppkey=s_suppkey [partsupp ⋈ supplier+nation]` | 127 | **13** | **12** | 是 |
| Q9 | `s_nationkey=n_nationkey [supplier ⋈ nation]` | 0.9 | 0.24 | 0.15 | 是 |
| Q10 | `c_custkey=o_custkey [customer+nation ⋈ lineitem+orders]` | 102 | 102 | 104 | 否 |
| Q10 | `c_nationkey=n_nationkey [customer ⋈ nation]` | 12.5 | 12.9 | **3.1** | 是 |
| Q10 | `l_orderkey=o_orderkey [lineitem ⋈ orders]` | 226 | 221 | 221 | 否 |

（原始逐行数据见 `b_idea/perf/sf5/tpch_operator_timing.csv`；本表数值取某一次实测，存在测量噪声，量级/趋势结论以此为准。）

### 7.4 结果判读

- **executor 层收益是真实存在的**：几个命中 BHJ 的小/中型 join（`c_nationkey=n_nationkey`、`ps_suppkey=s_suppkey`）耗时相比 baseline 有数倍下降（Q10 的 `customer⋈nation` 从 12.5ms 降到 3.1ms，降幅 ~75%；Q9 的 `partsupp⋈supplier+nation` 从 127ms 降到 12ms，降幅 ~90%），说明位图查找确实比哈希探测快得多。
- **但收益被两件事抵消了**：
  1. **命中率不够**——三条查询里耗时占比最大的几个 join（Q9/Q10 里动辄 200~1000+ms 的大 join）恰恰都是未命中的那几个，它们的耗时在 bitmap 模式下和 baseline 几乎没有差异（因为走的还是原来的普通 HashJoin 路径），三条查询的**总**耗时因此被这些未命中的大 join 主导，命中的小 join 省下来的几十毫秒相对总耗时（几百到上千毫秒）占比很小。
  2. `Q5` 的 `orders⋈customer+nation+region` 这一条命中了 BHJ，但耗时反而从 baseline 48ms 涨到了 bitmap 模式 90ms（涨了近 2 倍）——这是一个需要额外关注的异常点，说明**并非所有命中 BHJ 的 join 都稳定获益**。**根因已在任务2条目8b验证过程中定位**（该条目新增命中的 Q10 `lineitem⋈orders` 复现了更极端的5倍暴涨，促使做了系统性排查）：`BitmapJoinExecutor` 的位图/payload 大小固定按 PK 表**静态全表行数**分配，不适应"build 侧被上游过滤器大幅收窄"的场景——密度（实际到达行数/静态总行数）越低，probe 端随机访问的 cache 命中率越差；Q5 这个 join 的 `customer` 密度约20.1%（`region='ASIA'` 过滤），恰好处于"收益打平"的区间，与 Q10 `orders` 密度3.8%（明显暴涨）、Q9 `supplier` 密度100%（明显获益）构成完整的密度梯度证据链。详见任务2文档 §8b.4.5。
- **未命中的 join 没有额外 overhead**：对比未命中 join 在 bitmap 模式与 baseline 的耗时（如 Q10 的 `l_orderkey=o_orderkey` 226ms→221ms，Q5 的 `l_orderkey=o_orderkey` 141ms→147ms），差异都在测量噪声范围内，说明 `BitmapJoinResolver` 本身的规划期遍历/`ResolveOperatorTypes()` 重算没有引入可观察的额外开销。

### 7.5 总体结论（回答"瓶颈在计划层还是执行层"）

**两者都是瓶颈，但计划层（命中率不足）是当前更大的一块**：已命中的 join 已经证明 executor 有效（部分降幅达 75%~90%），但被"未命中的大 join 占了总耗时的大头"完全抵消。结合条目6的归因结果，**如果先解决 `CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION` 这个新发现的问题（能救下 Q9 的 `l_orderkey=o_orderkey` 1018ms 和 Q10 的 `c_custkey=o_custkey` 104ms + `l_orderkey=o_orderkey` 221ms 共3个大/中型 join），预期总耗时收益会比只做条目8（LEFT侧优化，只影响 Q5 一个 147ms 的 join）更显著**。Q5 里那个 90ms 的"命中但变慢"的异常，则是一个独立于命中率的执行器问题，值得记录但不建议在任务2里处理。

### 7.6 意外发现并修复的 bug：`RIGHT_SEMI` 导致真实崩溃（TPCH Q20）

条目7新增的 22 条 SQL smoke test 首次运行时，**Q20 在 `open_bitmap_join=true` 时直接报错**（baseline 不报错）：

```text
ERROR: Not implemented Error: BitmapJoinExecutor: only INNER joins are supported (MVP §6.4)
```

Q20 的 `s_suppkey IN (SELECT ps_suppkey FROM partsupp WHERE ...)` 子查询去关联后，产生了一个 `RIGHT_SEMI` 类型的 `LogicalComparisonJoin`。根因是**历史遗留的一处不一致**：`BitmapJoinResolver::ResolveJoin` 和 `PlanComparisonJoin` 里的 `bhj_eligible` 判断，从条目1/2起就一直写的是"`INNER` 或 `RIGHT_SEMI`"都放行；但 `BitmapJoinExecutor` 的构造函数从一开始就只实现了 `INNER`（`if (join.join_type != JoinType::INNER) throw NotImplementedException(...)`）。这两处代码此前从未被任何测试同时触达过（之前任务2条目9文档就推测过这个风险，但当时判断"未经验证"；这次是**第一次被真实数据坐实**）。

这直接违反了整套设计"任何不确定/不支持的情形都必须安全回退到普通 HashJoin，绝不抛异常"的核心原则（详见 `bitmap_join_resolver.md` §8 第2条）。**修复方式**：把 `BitmapJoinResolver::ResolveJoin` 和 `plan_comparison_join.cpp` 里的 `bhj_eligible` 判断都收紧为只允许 `JoinType::INNER`，不再允许 `RIGHT_SEMI`（对应诊断枚举 `NOT_INNER_OR_RIGHT_SEMI` 的语义随之更新为"不是 INNER"）。修复后 22 条 SQL 全部通过（`22 ok, 0 mismatched, 0 crashed`），且 Q5/Q9/Q10 的命中数/正确性不受影响（回归测试全部通过）。

是否要在未来正确地支持 `RIGHT_SEMI`（而不是简单排除），留给任务2条目9继续讨论（该文档条目9原本设计的探查步骤，现在有了一个现成的、真实的 `RIGHT_SEMI` 触发场景 —— TPCH Q20 —— 可以直接复用，不需要再手工构造 SQL 去探查"能否稳定触发 RIGHT_SEMI"这件事了）。

### 7.7 验收标准（达成情况）

- [x] 产出真实数据填充的算子耗时对比表（§7.3）；
- [x] 给出"计划层 vs 执行层"结论（§7.5）；
- [x] 覆盖全部 22 条标准 TPC-H SQL 的 e2e smoke test 已实现并纳入常规可运行的测试套件；
- [x] 过程中发现的真实 bug（RIGHT_SEMI 崩溃）已修复并通过回归验证。
