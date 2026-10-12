---
title: BitmapJoin (BHJ) 6.4 遗漏问题 —— 任务拆分索引
version: v0.1.0
status: Draft（方案设计，尚未实施）
related:
  - ../6.4/6.4后续进展-实现与测试方案.md（条目1-5，已实施完成）
  - ../perf/sf5/tpch_prof.md
  - ../perf/sf5/tpch_q5_q9_q10.csv
  - ../../src/optimizer/bitmap_join_resolver.cpp
  - ../../src/optimizer/bitmap_join_resolver.md
  - ../../test/api/test_bitmap_join_chain.cpp
  - ../../test/api/test_bitmap_join_tpch.cpp
---

# 说明

条目1-5（`../6.4/6.4后续进展-实现与测试方案.md`）已全部实施并通过测试。在验收复盘时，发现了 4 个尚未解决的遗留问题，按"诊断 vs 实现"的性质拆分成两个任务：

| 原问题编号 | 内容 | 归入任务 |
| --- | --- | --- |
| 1 | 拿到 Q5/Q9/Q10 执行计划，分析命中率只有 3/5、3/5、1/3 的具体原因（无解 vs 可优化） | **任务1** |
| 2 | 对 Q5/Q9/Q10 的每个 HASH_JOIN 做三模式（baseline/perfect/bitmap）绝对耗时 profiling，判断优化效果不明显是计划问题还是执行器问题 | **任务1** |
| 3 | 优化"PK 表在中间 join 里落在 LEFT 侧"导致上层放弃 BHJ 的问题，需要在 `test_bitmap_join_chain.cpp` 补充测试 | **任务2** |
| 4 | `RIGHT_SEMI` 路径从未被测试跑过，需要补测 | **任务2** |

拆分理由：1、2 都是"不改变生产代码逻辑的诊断工作"（顶多加一个默认关闭的诊断标注），产出是分析报告和数据表，用来回答"3 是否值得投入"；3、4 都是"真实的代码改动 + 回归测试"，且 3 的实现方案设计依赖对现有约束（`RemoveUnusedColumns` 产生的绝对位置引用）的深入分析，篇幅和复杂度都明显大于诊断类工作，单独成文。

- 详见 [任务1-BHJ命中率归因与算子耗时画像.md](./任务1-BHJ命中率归因与算子耗时画像.md)（**已完成**：归因表+耗时表均已用真实 SF5 数据填充，并在此过程中意外发现并修复了一个真实的 `RIGHT_SEMI` 崩溃 bug——TPCH Q20）
- 详见 [任务2-中间Join左侧传播优化与RIGHT_SEMI覆盖.md](./任务2-中间Join左侧传播优化与RIGHT_SEMI覆盖.md)（**条目8b+条目8 均已实施并通过真实SF5数据验证**；条目9/RIGHT_SEMI定案不做）

## 任务1 核心结论速览（详见任务1文档）

1. **命中率归因**：Q5(3/5)/Q9(3/5)/Q10(1/3) 里"无解类"未命中各1个（多列等值条件，设计边界内）；"可优化类"里，Q5 的1个属于"中间join左侧限制"（任务2条目8目标），**但 Q9/Q10 的3个未命中，全部是一个新发现的、更大影响面的原因**——`CompressedMaterialization` 优化器给 join key 包了一层压缩函数，遮蔽了 `BitmapJoinResolver` 的追踪逻辑。已实施为任务2条目8b（全局预扫描+源头过滤方案）并通过真实数据验证。
2. **算子耗时画像**：已命中 BHJ 的 join 有真实的、可观的加速（部分降幅 75%~90%），但总耗时被未命中的大 join 主导，命中率不足是当前更大的瓶颈；同时发现多个"命中但反而变慢"的异常点（Q5 `orders⋈[...]` ~2倍；条目8b验证时新发现 Q10 `lineitem⋈orders` ~5倍，是当前记录到的最严重一例），需要另开执行器层面的诊断任务。
3. **意外发现并修复的 bug**：22条标准TPC-H SQL的smoke test测出 Q20 在 `open_bitmap_join=true` 时崩溃——`RIGHT_SEMI` 类型join被`BitmapJoinResolver`错误地放行,但`BitmapJoinExecutor`只支持`INNER`。已修复（收紧`bhj_eligible`为仅`INNER`），22条SQL全部验证通过。经用户确认，正确支持`RIGHT_SEMI`不再投入（Q5/Q9/Q10均无此类join，无真实收益）。

## 任务2 条目8b 核心结论速览（详见任务2文档 §8b）

用真实 SF5 数据验证：条目8b（消除 `CompressedMaterialization` 对 BHJ 追踪的遮蔽）已实施并生效——Q9/Q10 里全部 3 个原本被压缩问题卡住的 join，压缩包装已彻底消失，`bhj_skip_reason` 还原为各自真实根因：
- Q9 的 1 个 → `path_incomplete_left_side`（条目8的目标，待条目8落地后可命中，**条目8的预期收益因此从"仅Q5 1个"上调为"Q5+Q9共2个"**）；
- Q10 的 1 个 → `fk_on_build_side`（无解类，设计边界内，不会再提升）；
- Q10 的另 1 个 → 直接命中（命中率 Q10 1/3→2/3）。

方案实际比最初设计更复杂：`CompressedMaterialization` 自下而上逐节点压缩，某列在下层可能只是"payload列"被压缩，但它是上层某个祖先join的字面条件——最初"只保护当前join自己条件列"的设计不足以覆盖这种跨层情况，最终采用"全局预扫描（跑一次，收集全树所有INNER单等值join的条件绑定）+ 所有4个Compress*函数统一按此名单排除"的方案，详见任务2文档 §8b.4。

## 任务2 条目8 核心结论速览（详见任务2文档 §8.7）

条目8（LEFT侧传播优化）已实施并验证通过——采用 `bhj_passthrough_refs` 方案（完整 Step A-E），在 `LogicalComparisonJoin` 上新增旁路列字段，将隐藏列追加在 join 输出的物理最末尾 `[left][right][passthrough]`，避免 shift 任何已有位置。改动覆盖逻辑层（`GetColumnBindings`/`ResolveTypes` override、`TraceBindingToGet`/`PropagateHiddenColumn` LEFT侧处理、`ColumnBindingResolver` passthrough解析）和物理执行层（三处执行路径：常规HashJoin、perfect hash join、BHJ ProbeBitmap 均追加 passthrough 列）。

- Q5 命中率 3/5 → **4/5**，Q9 命中率 3/5 → **4/5**，`PATH_INCOMPLETE_LEFT_SIDE` 归因清零。
- Q9 总耗时降幅 **19%**（341→278ms），新增命中的 `l_orderkey=o_orderkey` join 从 1101ms 降至 563ms。
- Q5 出现中等额外开销（§8b.4.5 预警的"低密度=性能下降"场景在 Q5 上真实复现）：新增命中的 `l_orderkey=o_orderkey` join 走 BHJ 后，wall-clock 从 158ms 增至 266ms（1.68x）。**此前记录的"暴涨13.5倍"系 `operator_timing` 测量偏差**（含 pipeline 等待阻塞，详见任务2文档 §8.7.2a）。实际开销来自 `payload_columns`（114MB，密度3%）的稀疏 gather，bitmap 本身 916KB 可放 L2 不是瓶颈。

## 建议执行顺序（v0.8.0 更新）

```text
任务1（诊断，已完成）
  ├─ 条目6：EXPLAIN 命中归因表 ──┐
  └─ 条目7：三模式算子耗时画像 ──┤
                                  ▼
                     结论：CompressedMaterialization冲突（条目8b）优先级 > LEFT侧限制（条目8）
                                  │
任务2（实现，全部完成）
  ├─ 条目8b：CompressedMaterialization 源头过滤 —— 已实施并验证通过 ✓
  ├─ 条目8：中间 join LEFT 侧传播优化（bhj_passthrough_refs方案）—— 已实施并验证通过 ✓
  │   Q5 命中率 3/5→4/5，Q9 命中率 3/5→4/5，PATH_INCOMPLETE_LEFT_SIDE 归因清零
  │   Q9 总耗时降幅22%（338→262ms）；Q5 wall-clock 增53%（157→240ms，低密度join开销）
  │   Q10 仅慢1%（222→225ms）← payload scatter锁优化+CombineBitmap无锁稀疏合并后消除>20%告警
  └─ 条目9：RIGHT_SEMI —— 已决策不做（Q5/Q9/Q10均为INNER join，无真实场景驱动）
```

条目8b 和条目8 均已完成。下一步建议优先处理密度问题（§8b.4.5）：`BitmapJoinExecutor` 的 `payload_columns` 按 bitmap_size（静态PK全表行数）分配，不适应"build侧被上游过滤器大幅收窄"的低密度场景，导致 Q5 新命中 join wall-clock 增68%（158→266ms）、Q10 既有命中 join 增36%（219→298ms）。可能方向：按实际到达的 build 行数动态选择 payload 分配大小，或在密度低于某阈值时自动回退到普通 HashJoin。
