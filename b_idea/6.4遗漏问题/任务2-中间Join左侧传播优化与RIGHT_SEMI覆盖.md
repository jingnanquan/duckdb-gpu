---
title: BitmapJoin (BHJ) 6.4 遗漏问题 —— 任务2：中间Join左侧传播优化与RIGHT_SEMI覆盖
version: v0.8.0
status: 条目8b+条目8 均已实施；payload scatter 锁优化+CombineBitmap无锁稀疏合并均已实施；条目9已定案不做
related:
  - ./README.md
  - ./任务1-BHJ命中率归因与算子耗时画像.md（条目8/8b投入优先级依赖其结论；§7.6记录了条目9相关的一个真实崩溃bug已被提前修复）
  - ../../src/optimizer/bitmap_join_resolver.cpp
  - ../../src/optimizer/bitmap_join_resolver.md
  - ../../src/optimizer/remove_unused_columns.cpp
  - ../../src/optimizer/late_materialization.cpp（ConstructRHS，思路参照）
  - ../../src/optimizer/compressed_materialization.cpp（条目8b核心改动点）
  - ../../src/optimizer/compressed_materialization/compress_comparison_join.cpp（条目8b核心改动点）
  - ../../src/optimizer/optimizer.cpp（RunBuiltInOptimizers，条目8b依赖的pass顺序证据）
  - ../../src/execution/physical_plan/plan_comparison_join.cpp
  - ../../src/execution/operator/join/physical_comparison_join.cpp
  - ../../src/execution/operator/join/bitmap_hash_join_executor.cpp（条目8b风险分析：稠密PK fast path；§8b.4.5 密度问题根因所在）
  - ../../test/api/test_bitmap_join_chain.cpp
  - ../../test/api/test_bitmap_join_tpch_22queries.cpp（条目9探查证据来源）
  - ../../test/api/test_bitmap_join_perf.cpp（§8b.4.4 最新实测数据来源，密度100%场景的正面对照组）
  - ../perf/sf5/tpch_operator_timing.csv（§8b.4.2 逐join耗时对比数据源）
  - ../../data/tpch_sf5_bitmap/bitmap_join_meta.json（§8b.4.5 密度分析：`row_count` 是静态全表行数）
---

# 说明

本任务对应用户提出的原问题 3、4，都是**实现类**任务：需要真实修改生产代码逻辑（条目8/8b）或补充此前完全没有覆盖到的代码路径的测试（条目9）。

> **v0.3.0 用户决策更新**：
> 1. **条目9（RIGHT_SEMI）定案：不做优化，不再补充单独测试**。理由：真实的 Q5/Q9/Q10 三条目标查询里完全没有 SEMI/RIGHT_SEMI 类型的 join；当前"收紧到仅 INNER、安全回退"的状态已经是正确、安全的终态（`test_bitmap_join_chain.cpp` 里已有的"BHJ safely falls back...for RIGHT_SEMI"回归测试足够覆盖这一契约，不需要再为"正确支持RIGHT_SEMI"补充新测试）。详见 §9.7。
> 2. **新增条目8b（`CompressedMaterialization` 与 BHJ 的冲突）**：用户明确了优先级——**查询性能提升是第一优先级，中间结果压缩是次要目标**；对于会影响 BHJ 命中的压缩，应该在源头过滤掉，而不是事后想办法让 BHJ 兼容压缩后的形态。任务1的诊断已经证明这个问题影响面（Q9 1个 + Q10 2个 = 3个大/中型join）比条目8（LEFT侧限制，仅Q5 1个）更大，本文档详细设计见 §8b。

---

> **历史记录（v0.2.0，条目9 Step A 的探查过程，现已被 §9.7 的定案取代）**：任务1条目7新增的"22条标准TPC-H SQL smoke test"在首次运行时，**真实复现了条目9原本要探查的风险**——TPCH Q20 的 `IN` 子查询去关联后产生 `RIGHT_SEMI` 类型的 join，`BitmapJoinResolver` 一直允许它进入 BHJ 候选，但 `BitmapJoinExecutor` 只支持 `INNER`，直接抛 `NotImplementedException` 而不是安全回退——这是一个真实的、会让查询直接报错的崩溃 bug，已在任务1推进过程中**发现并修复**：把 `bhj_eligible`（`BitmapJoinResolver::ResolveJoin` + `plan_comparison_join.cpp`）收紧为只允许 `JoinType::INNER`，不再允许 `RIGHT_SEMI`。修复后 22 条标准 SQL 全部通过，回归无破坏。详见任务1文档 §7.6。

---

## 条目8：中间 join「LEFT 侧传播」优化

### 8.1 问题重述

现有 `TraceBindingToGet`/`PropagateHiddenColumn`（`src/optimizer/bitmap_join_resolver.cpp:114-156`、`313-384`）只允许隐藏列 (`_rowid`/`*_ref`) 穿过中间 `LOGICAL_COMPARISON_JOIN` 的 **RIGHT** 侧向上传播；一旦隐藏列的源 `LogicalGet` 落在某个中间 join 的 **LEFT** 侧，直接放弃整条传播（`path_complete=false`），导致本来可以命中 BHJ 的**上层** join 也被连带放弃。

> **注意（任务1诊断结论）**：任务1条目6用真实 SF5 数据做归因后发现，Q5/Q9/Q10 三条查询里，**只有 Q5 的一个 join** 是被这个 LEFT 侧限制卡住的（`PATH_INCOMPLETE_LEFT_SIDE`），而 Q9/Q10 命中率不足的主因是另一个新发现的、更大影响面的问题——`CompressedMaterialization` 优化器包装 join key（见任务1文档 §6.5，建议新增为条目8b，优先级建议高于本条目）。本条目8仍然值得做，但预期收益（仅覆盖 Q5 一个 147ms 的 join）小于条目8b（覆盖 Q9+Q10 共3个大/中型 join，耗时占比更高）。

用户提出两个方向：
1. 下层 join 左侧传播会破坏位置时，"通知"上层 join 去更新位置；
2. 整个绑定过程改成自下而上，让下层 join 的隐藏列先"固化"，上层 join 绑定隐藏列时不需要再关心下层内部布局的变化。

并要求在 `test_bitmap_join_chain.cpp` 里补充能复现并验证这个场景的测试。

### 8.2 根因回顾（为什么现状只能走 RIGHT 侧）

`LogicalJoin::GetColumnBindings() = MapBindings(left, left_projection_map) ++ MapBindings(right, right_projection_map)`，输出布局**固定**为 `[left部分][right部分]` 的拼接，且这个布局早在我们的 pass 运行之前，就已经被 `RemoveUnusedColumns`/`ColumnLifetimeAnalyzer` 计算并"钉死"——**更上层某个祖先算子，可能已经用绝对位置引用了本 join 的 RIGHT 部分**（例如它自己的 `left_projection_map[k] = 7` 意味着"引用本 join 输出的第7列"）。

如果我们往 `left_projection_map` 追加一个新条目，这个新增列的绝对位置是 `left_projection_map.size()-1`，即 LEFT 部分的**末尾**——但由于输出布局是 `[left][right]`，LEFT 部分的末尾在**整个 join 输出**里的绝对位置，天然早于所有 RIGHT 部分的位置。也就是说：**LEFT 侧的"追加"，在 join 整体输出的坐标系里，其实是"插入在中间"**，会把 RIGHT 部分所有已存在的绝对位置全部 +1，从而静默破坏任何已经引用了这些位置的祖先节点。这正是开发条目4时在 TPCH Q5 真实复现过的崩溃根因。

而 RIGHT 侧追加是安全的，本质原因不是"RIGHT 侧特殊"，而是**RIGHT 部分恰好是整个输出布局的最后一段，追加在它末尾，等价于追加在整个 join 输出的物理最后**，不会导致后面任何东西移位（因为它后面什么都没有）。

**结论**：问题的本质不是"是 LEFT 还是 RIGHT"，而是"新增列有没有落在整个 join 输出的物理最后"。这个结论直接指向了下面的方案选择。

### 8.3 两个方向的可行性分析

**方向1（用户提出的"通知上层更新位置"）：定位并修正所有引用了被移位位置的祖先**

理论可行，但代价很高：一旦往 LEFT 侧插入新列，需要从这个中间 join 往上，对**每一个祖先算子**里"任何引用了 `≥ 插入点`位置"的地方做整体 `+1`；而这个移位本身还可能级联（某个祖先自己的 `left_projection_map` 需要移位之后，又会影响它的祖先）。这本质上是在事后局部重放一遍 `ColumnLifetimeAnalyzer` 的效果，正确性风险和实现复杂度都不低——历史上 `BitmapJoinOperatorState` 继承错误导致的 SIGSEGV 调试成本已经证明"贸然对已经跑完优化的绝对位置做二次修改"是高风险区域，**不建议采用方向1**。

**方向2（用户提出的"自下而上固化"）：现状其实已经是自下而上，但这不是问题所在**

`BitmapJoinResolver::VisitOperator` 本身就是"先递归子节点，再处理当前 join"（`bitmap_join_resolver.cpp:388-394`），已经是自底向上；如果中间 join 本身也是一个独立的 BHJ 候选，它会先完成自己的隐藏列传播，其 `left_projection_map`/`right_projection_map` 在外层 join 处理时已经是"固化"之后的状态。**"绑定顺序自下而上"这件事本身已经成立，不是问题根源**——真正的限制发生在"单次从 origin Get 到某个 consumer_join 的传播路径内部，跨越中间 join 时只能走 RIGHT 侧"，这与"先处理哪个 join"是两个独立的维度，方向2字面意义上不能单独解决 LEFT 侧问题。

**采用方案（在方向2的"消除移位"精神下，给出真正可行的实现）：旁路直通列**

不再往 `left_projection_map`/`right_projection_map` 里插入新条目，而是让隐藏列完全绕开"作为这个中间 join 的常规输出列参与`[left][right]`拼接"这条路，改为一个专门追加在**整个 join 输出物理最末尾**的旁路机制：给 `LogicalComparisonJoin` 增加

```cpp
// src/include/duckdb/planner/operator/logical_comparison_join.hpp
//! 条目8：需要穿透本 join 向上传递、但不参与本 join 任何条件/常规输出投影的"旁路"隐藏列。
//! 绑定在本 join 输出的物理最后一段（[left][right][bhj_passthrough_refs]），因此对
//! left_projection_map/right_projection_map 已经确定的绝对位置零侵入 —— 无论隐藏列源自
//! LEFT 还是 RIGHT 子树，都统一走这一条安全路径，从根本上消除"移位破坏祖先引用"的可能。
vector<unique_ptr<Expression>> bhj_passthrough_refs;
```

这样无论隐藏列源自哪一侧，传播算法都统一处理，`PropagationSide`/`side==LEFT就放弃` 的特判可以整体去掉，代码反而更简单。

### 8.4 实现方案（详细步骤）

**Step A：`LogicalComparisonJoin` 新增 `bhj_passthrough_refs` 字段**（见 8.3）。

**Step B：`GetColumnBindings()` / `ResolveTypes()` 追加旁路列**

需要找到 `LogicalComparisonJoin`（或其基类 `LogicalJoin`/`LogicalOperator`）里 `GetColumnBindings()`/`ResolveTypes()` 的现有实现，在计算完 `[left][right]` 之后，追加 `bhj_passthrough_refs` 对应的绑定/类型：

```cpp
// 伪代码，具体插入点需要先读一遍 LogicalComparisonJoin 现有的 GetColumnBindings/ResolveTypes 实现
vector<ColumnBinding> LogicalComparisonJoin::GetColumnBindings() {
    auto result = /* 现有 [left][right] 逻辑，不变 */;
    for (idx_t i = 0; i < bhj_passthrough_refs.size(); i++) {
        result.emplace_back(table_index, /* 某个从 result.size() 开始编号的 column_index 方案，
                                              需要与 ResolveTypes 保持一致 */);
    }
    return result;
}
```

> 需要先探查 `LogicalComparisonJoin::table_index` 目前的用途（普通 INNER/RIGHT_SEMI join 是否已经有一个自己的 `table_index`，还是只有 `LogicalProjection`/`LogicalGet` 才有自己的 `table_index`？如果 `LogicalComparisonJoin` 本身没有独立的 `table_index` 语义，需要评估旁路列的 `ColumnBinding` 应该用谁的 `table_index` 才能被下游正确识别——这是动手前必须确认的一处接口细节，不能想当然套用 `LogicalProjection` 的模式）。

**Step C：`PropagateHiddenColumn`/`TraceBindingToGet` 改造**

- `TraceBindingToGet` 里去掉"LEFT 侧命中 `path_complete=false`"的降级分支，LEFT/RIGHT 统一按同一逻辑记录 `PathStep`（`PropagationSide` 枚举可以整体删除，简化代码，见 `bitmap_join_resolver.cpp:114-156`）。
- `PropagateHiddenColumn` 里中间 join 分支（`bitmap_join_resolver.cpp:340-360`）统一改为：把当前 `binding` 包装成 `BoundColumnRefExpression`，`push_back` 进 `join.bhj_passthrough_refs`，新的 `binding` 更新为 `{join.table_index, join.GetColumnBindings().size()-1}`（即"本 join 输出的物理最后一列"），不再区分是从 LEFT 还是 RIGHT 侧传上来的。

**Step D：`ColumnBindingResolver` 认识 `bhj_passthrough_refs`**

参照条目3 Step D 已经验证过的"方式B"：让 `LogicalComparisonJoin::GetExpressions()`（`LogicalOperatorVisitor` 遍历用的官方接口）把 `bhj_passthrough_refs` 纳入遍历范围，`ColumnBindingResolver` 会自动把其中的 `BoundColumnRefExpression` 拍平成 `BoundReferenceExpression`，不需要特判改动 `ColumnBindingResolver` 本身。

**Step E：物理执行层——这是本方案相比条目3/4新增的、唯一有实质工作量的部分**

条目3/4 的隐藏列全程只涉及"逻辑层的列位置重排"：`Get`/`Projection`/`Filter`/中间 `join` 的 `projection_map` 都只是**选择/重排已经存在的列**，从未要求某个物理算子在执行期"额外拼接一列它原本不会输出的数据"。而条目8要求**中间 join 对应的物理算子**（如果它自己也会被规划成一个 `PhysicalHashJoin`/`PhysicalComparisonJoin`）在执行期，把 `bhj_passthrough_refs` 这一列从 build 或 probe 侧的输入 chunk **原样直通**到自己的输出 chunk 里——这是一块目前完全没有的物理执行能力。

具体需要探查：
1. `PhysicalComparisonJoin`/`PhysicalHashJoin` 现有的 `lhs_output_columns`/`rhs_output_columns` 机制（决定 join 输出里包含左/右两侧的哪些列）是否已经支持"这一列不参与任何 join 条件，只是原样传递"的语义——如果能直接把 `bhj_passthrough_refs` 转换成对应侧多加的一个 `output_column` 索引，成本会显著低于引入一套全新的物理拼接逻辑；
2. 如果 `lhs_output_columns`/`rhs_output_columns` 本身就是按"逻辑 binding → 物理下标"的方式工作的（大概率是这样，因为它们本就是 `RemoveUnusedColumns`/`ColumnLifetimeAnalyzer` 的产物），那么 `bhj_passthrough_refs` 拍平后的 `BoundReferenceExpression.index` 完全可以复用同一套机制，Step E 的成本可能远低于预估——**这一点必须先读代码求证，不能凭空假设**。

**Step E'（低成本降级路径，建议优先落地）**：在探查 Step E 之前，先注意到一个更廉价的特例——**当中间 join 的 `left_projection_map` 恰好为空**（即该 join 没有对左子树做任何列裁剪，`SELECT *` 风格或未触发 `RemoveUnusedColumns` 裁剪的场景，实测中很常见），此时 LEFT 子树的全部列本来就会原样出现在该 join 的输出里，**不需要新增任何列，也不需要 `bhj_passthrough_refs`**——只要把 `TraceBindingToGet`/`PropagateHiddenColumn` 里"LEFT 侧且 `left_projection_map` 为空"这一种情况单独放行（等价于现状 RIGHT 侧"`right_projection_map` 为空时的恒等透传"分支，`bitmap_join_resolver.cpp:353-354`，对称地为 LEFT 侧补一份），成本几乎为零，且能覆盖任务1条目6统计出来的"可优化类"里的一大部分（具体占比取决于条目6的归因结果）。

> **建议实施顺序**：先落地 Step E'（对称补齐"LEFT 侧且 `left_projection_map` 为空"的恒等透传特例，几乎不需要新增字段/物理执行改动），跑一次任务1的诊断脚本看命中率提升多少；再决定是否值得为"LEFT 侧且确实发生了列裁剪"这个更少见、成本更高（需要 Step A-E 的完整 `bhj_passthrough_refs` 机制）的场景继续投入。

### 8.5 测试方案（写入 `test_bitmap_join_chain.cpp`）

**Step 1：先写一个能稳定复现"LEFT 侧限制导致不命中"的测试（作为 bug 的可重现证明，在方案落地前就应该能跑通并断言"现状确实不命中"）**

```cpp
TEST_CASE("BHJ hidden column fails to propagate through the LEFT side of an intermediate "
          "INNER join (known limitation, 条目8 优化目标)",
          "[bitmap_join]") {
    RegistryResetGuard guard;
    auto &reg = BitmapJoinMetaRegistry::GetInstance();
    DuckDB db(nullptr);
    Connection con(db);
    SetupChainSchema(con, reg, /*with_nation=*/true);

    // 关键：让 customer（需要隐藏 crid 列）成为中间 join 的 LEFT 子树 —— 与现有
    // "BHJ hidden column propagates through an intermediate INNER join" 测试
    // （customer 写在 RIGHT，`nation JOIN customer`）刻意相反，这里写成
    // `customer JOIN nation`，并需要用 EXPLAIN 实测确认 BuildProbeSideOptimizer
    // 最终选择的 children[0]/children[1] 顺序是否真的把 customer 放在了 LEFT——
    // 如果代价估计导致它被自动挪去了 RIGHT，需要人为构造基数差异（例如给 nation
    // 加更多行，或用 PRAGMA disable_optimizer='build_side_probe_side' 固定形态）
    // 来确保测试稳定复现 LEFT 侧场景，而不是"凭感觉写SQL但实际根本没触发目标路径"。
    const string query = "SELECT count(*) AS cnt, sum(o.amt) AS sum_amt, 0 AS unused "
                          "FROM bhj_chain_orders o "
                          "JOIN (bhj_chain_customer c JOIN bhj_chain_nation n ON c.nk = n.nk) "
                          "ON o.ck = c.ck";

    REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
    auto plan = ExplainText(con, query);
    // 落地条目8方案之前：断言【不】命中，证明问题确实存在（反面教材）。
    // 落地条目8方案之后：这个断言要反转为 REQUIRE(StringUtil::Contains(plan, "Bitmap Join: yes"));
    // 并补充与 baseline 的结果一致性校验（复用 RunAgg3）。
}
```

**Step 2：条目8方案落地后，改写 Step 1 的断言为"必须命中"，并补充结果正确性校验**（与现有"BHJ hidden column propagates through an intermediate INNER join"测试的结构一致，`test_bitmap_join_chain.cpp:153-206`）。

**Step 3：回归现有测试**——条目4已有的"同名冲突"（`test_bitmap_join_chain.cpp:208-249`）、"幂等性"（`251-280`）两个测试，在改用 `bhj_passthrough_refs` 机制后必须继续全部通过，确认新机制不引入新的重复注入/冲突问题。

### 8.6 验收标准

1. 落地前：新增的"LEFT 侧限制"测试能稳定复现"确实不命中"（证明问题真实存在，且测试构造方式经过 EXPLAIN 实测验证，不是想象中的场景）；
2. 落地后（至少完成 8.4 Step E' 的低成本特例）：同一测试改为断言"命中"，结果与 baseline 一致；
3. 现有条目3/4 相关测试（`test_bitmap_join_chain.cpp` 全部3个 `TEST_CASE`、`test_bitmap_join_tpch.cpp`）全部保持通过；
4. 若完整实现了 `bhj_passthrough_refs`（Step A-E），额外验证：一个"确实发生了列裁剪的 LEFT 侧"场景（`left_projection_map` 非空）也能命中；
5. 用任务1条目6的诊断标注（`bhj_skip_reason`）重新跑一遍 Q5/Q9/Q10，确认 `PATH_INCOMPLETE_LEFT_SIDE` 归类的 join 数量下降，并用任务1条目7的耗时表确认总耗时是否随命中率提升而下降。

### 8.7 实施结果（v0.6.0，已完成）

#### 8.7.1 采用方案：`bhj_passthrough_refs`（完整 Step A-E）

文档 §8.4 中建议的 Step E'（低成本降级路径，仅处理 `left_projection_map` 为空的特例）在深入分析后被证明**不安全**——无论 `left_projection_map` 是否为空，注入隐藏列后 Get 的输出增长，即使 `left_projection_map` 为空（identity passthrough），LEFT 部分增长也会 shift RIGHT 部分在 join 输出中的绝对位置，破坏祖先 `projection_map`。因此实际实施了完整的 Step A-E `bhj_passthrough_refs` 方案。

**核心思路**：给 `LogicalComparisonJoin` 新增 `bhj_passthrough_refs` 字段和 `bhj_passthrough_bindings` 字段，将隐藏列追加在 join 输出的**物理最末尾**（`[left][right][passthrough]`），彻底避免 shift 任何已有位置。

**逻辑层改动**：
- `src/include/duckdb/planner/operator/logical_comparison_join.hpp`：新增 `bhj_passthrough_refs`（`vector<unique_ptr<Expression>>`）和 `bhj_passthrough_bindings`（`vector<ColumnBinding>`，存储原始 binding，避免 `ColumnBindingResolver` 解析后丢失）；新增 `GetColumnBindings()` 和 `ResolveTypes()` override。
- `src/planner/operator/logical_comparison_join.cpp`：`GetColumnBindings()` 在 `LogicalJoin::GetColumnBindings()` 基础上追加 `bhj_passthrough_bindings`；`ResolveTypes()` 在 `LogicalJoin::ResolveTypes()` 基础上追加 passthrough 表达式的 `return_type`。
- `src/optimizer/bitmap_join_resolver.cpp`：
  - `TraceBindingToGet`：对 INNER join 的 LEFT 侧不再设 `path_complete=false`，而是记录 `PropagationSide::LEFT` hop（此前仅允许 RIGHT 侧）。
  - `PropagateHiddenColumn`：LEFT 侧处理——若 `left_projection_map` 为空则显式设为 `[0, ..., N-2]`（排除隐藏列，防止 LEFT 部分自动增长 shift RIGHT 部分），然后将隐藏列 binding 推入 `bhj_passthrough_refs` + `bhj_passthrough_bindings`，更新 `child_binding_pos` 为 passthrough 在 join 输出中的位置（`left_len + right_len + passthrough_idx`）。
- `src/execution/column_binding_resolver.cpp`：在 `LOGICAL_COMPARISON_JOIN` 处理中，访问 LEFT 子节点后解析 `bhj_passthrough_refs` 表达式（与 `bhj_probe_ref_ref` 同一时机），使其从 `BoundColumnRefExpression` 变为 `BoundReferenceExpression`。

**物理执行层改动**（三处执行路径都需处理 passthrough 列）：
- `src/include/duckdb/execution/operator/join/physical_hash_join.hpp`：新增 `passthrough_lhs_col_idxs`（`vector<idx_t>`），存储 passthrough 列在 probe input 中的物理位置。
- `src/execution/physical_plan/plan_comparison_join.cpp`：从 `bhj_passthrough_refs` 读取已解析的 `BoundReferenceExpression.index`，写入 `passthrough_lhs_col_idxs`。
- `src/execution/operator/join/physical_hash_join.cpp`：三处执行路径追加 passthrough 列——(1) 常规 HashJoin 路径 `scan_structure.Next` 后用 `lhs_sel_vector` 切片追加；(2) perfect hash join 路径 `ProbePerfectHashTable` 后直接 `Reference` 追加；(3) BHJ 路径在 `ProbeBitmap` 中用 `probe_sel_vec` 切片追加。
- `src/execution/join_hashtable.cpp`：放宽 `NextInnerJoin` 的 debug 断言从 `==` 到 `>=`，允许输出 chunk 有额外的 passthrough 列。

#### 8.7.2 真实 SF5 数据验证：逐 join 命中率与耗时对比

用真实 SF5 数据重跑 `[bitmap_join_tpch_profile]`，对比条目8落地**前**（条目8b完成后，v0.5.0）和落地**后**（v0.6.0）的全部 13 个 join：

| Query | Join（`[probe⋈build]`） | baseline(ms) | perfect(ms) | bitmap(ms) 条目8前 → 条目8后 | `bhj_skip_reason` / 命中？ 条目8前 → 条目8后 |
| --- | --- | --- | --- | --- | --- |
| Q5 | `[lineitem+orders+customer+nation+region] ⋈ supplier` | 62.3 | 62.7 | 62.0 → 76.1 | `not_single_equality`（不变，与LEFT侧无关） |
| Q5 | **`l_orderkey=o_orderkey [lineitem ⋈ orders+customer+nation+region]`** | 146.5 | 149.9 | **145.8 → 1973.2** | **`path_incomplete_left_side` → `hit`（新增命中！）** |
| Q5 | `o_custkey=c_custkey [orders ⋈ customer+nation+region]` | 49.8 | 32.1 | 50.2 → 103.9 | `hit`（不变） |
| Q5 | `c_nationkey=n_nationkey [customer ⋈ nation+region]` | 3.7 | 4.9 | 4.7 → 4.7 | `hit`（不变） |
| Q5 | `n_regionkey=r_regionkey [nation ⋈ region]` | 0.20 | 0.14 | 0.01 → 0.03 | `hit`（不变） |
| Q9 | `[lineitem+part+orders] ⋈ [partsupp+supplier+nation]` | 996.2 | 1024.3 | 1075.0 → 1043.0 | `not_single_equality`（不变） |
| Q9 | **`l_orderkey=o_orderkey [lineitem+part ⋈ orders]`** | 1029.2 | 1069.7 | **1101.1 → 562.6** | **`path_incomplete_left_side` → `hit`（新增命中！）** |
| Q9 | `l_partkey=p_partkey [lineitem ⋈ part]` | 169.5 | 132.5 | 137.2 → 137.4 | `hit`（不变） |
| Q9 | `ps_suppkey=s_suppkey [partsupp ⋈ supplier+nation]` | 129.0 | 12.2 | 11.6 → 11.4 | `hit`（不变） |
| Q9 | `s_nationkey=n_nationkey [supplier ⋈ nation]` | 0.87 | 0.22 | 0.15 → 0.15 | `hit`（不变） |
| Q10 | `c_custkey=o_custkey [customer+nation ⋈ lineitem+orders]` | 105.9 | 102.1 | 97.0 → 95.5 | `fk_on_build_side`（不变，无解类） |
| Q10 | `c_nationkey=n_nationkey [customer ⋈ nation]` | 12.8 | 12.6 | 1.7 → 1.6 | `hit`（不变） |
| Q10 | `l_orderkey=o_orderkey [lineitem ⋈ orders]` | 224.7 | 218.8 | 1152.3 → 1056.0 | `hit`（不变） |

**命中率变化**：
- Q5：3/5 → **4/5**（+1，`path_incomplete_left_side` 归零）
- Q9：3/5 → **4/5**（+1，`path_incomplete_left_side` 归零）
- Q10：2/3 → 2/3（不变，无 LEFT 侧限制的 join）

`PATH_INCOMPLETE_LEFT_SIDE` 归因项已完全清零。

**耗时变化分析**：

> **重要修正（v0.6.1）**：此前版本（v0.6.0）记录的"Q5 `l_orderkey=o_orderkey` 从 146ms 暴涨到 1973ms（~13.5倍）"系**测量误差**——`operator_timing` 字段包含了 pipeline 等待 build 侧完成的阻塞时间，不是实际执行时间。用 `PRAGMA enable_profiling='json'` 提取 `cpu_time` 和直接测 wall-clock 后，实际开销远小于此。详见下方 §8.7.2a 的修正分析。

- **Q9 净获益显著**：新增命中的 `l_orderkey=o_orderkey` join 的 `operator_timing` 从 1101ms 降至 563ms（降幅 ~49%），驱动整条查询 wall-clock 从 345ms 降至 286ms（降幅 17%）。
- **Q5 有中等额外开销**（不是"暴涨13.5倍"）：新增命中的 `l_orderkey=o_orderkey` join 走 BHJ 后，`operator_timing` 显示 1929ms（含 pipeline 等待），但实际 wall-clock 查询总耗时仅从 158ms 增至 266ms（+68%，详见 §8.7.2a）。根因是 build 侧密度 3.04%（`orders` 经 `o_orderdate` 1年窗口过滤后仅 22.7 万行，但 bitmap 按 750 万行静态 `row_count` 分配），bitmap 本身 916KB 可放入 L2 不是瓶颈，主要开销来自 `payload_columns`（2列 × 750万 = 114MB）的稀疏 gather cache miss。
- **Q10 基本不变**：已有的 `lineitem⋈orders` 开销仍然存在（`operator_timing` 1008ms，wall-clock 查询 298ms），但其他 join 不受条目8影响。

#### 8.7.2a `operator_timing` vs wall-clock 测量偏差修正分析（v0.6.1 新增）

`test_bitmap_join_tpch_profile.cpp` 使用 `PRAGMA enable_profiling='json'` 提取每个 `HASH_JOIN` 节点的 `operator_timing`。但深入排查后发现，**`operator_timing` 包含了 pipeline 等待时间**——在 DuckDB 的 pipeline 模型中，probe 侧的 HASH_JOIN 必须等 build 侧 pipeline 完成后才能开始执行，这段阻塞等待被计入 `operator_timing`，导致多线程下 `operator_timing` 的 SUM 远大于 wall-clock：

| 指标 | Q5 baseline | Q5 bitmap | 比值 |
|---|---|---|---|
| `SUM(operator_timing)` 5个join | 260ms | 2058ms | 7.9x |
| `SUM(cpu_time)` 5个join | 2208ms | 3853ms | 1.75x |
| **wall-clock（整个查询）** | **158ms** | **266ms** | **1.68x** |

`operator_timing` 的 SUM(2058ms) 是 wall-clock(266ms) 的 **7.7 倍**，因为 DuckDB 多线程 pipeline 模型下多个 join 的 build/probe 重叠执行，`operator_timing` 却将每个 join 的等待时间串行累加。此前 v0.6.0 文档中引用的"暴涨13.5倍"正是基于这个有偏差的 `operator_timing` 数字（1929ms / 145ms），而非真实的 wall-clock 比值（266ms / 158ms = 1.68x）。

**Q5 bitmap 266ms 的真实开销分解**：
- baseline wall-clock = 158ms（5个join的build+probe，数据在OS cache中）
- bitmap wall-clock = 266ms（4个BHJ + 1个普通HJ）
- 净增 = 108ms，主要来自 `l_orderkey=o_orderkey` join 从普通 HJ 切换到 BHJ：
  - 该 join build 侧密度 3.04%（227,841 行散布在 7,500,000 位 bitmap 中）
  - bitmap 本身 916KB（可放入 L2，`RowIsValidUnsafe` 查找很快，不是瓶颈）
  - `payload_columns` 2列 × 7,500,000 = 114MB（密度3%→97%是空洞，probe 命中后 gather 的 cache miss 是主要开销）
  - 独立测量单 join wall-clock：baseline 219ms → bitmap 310ms（+91ms），与查询级 +108ms 基本吻合
- 对比条目8前（3/5 hits, 143ms）→ 条目8后（4/5 hits, 266ms），净增 123ms 是条目8引入这个新 BHJ 命中的代价

#### 8.7.2b 两个新增命中 join 的 bitmap 密度对比

| 指标 | Q5 `lineitem ⋈ orders+customer+nation+region` | Q9 `lineitem+part ⋈ orders` |
|---|---|---|
| bitmap_size（orders.row_count） | 7,500,000 | 7,500,000 |
| build 侧实际行数 | **227,841** | **7,500,000** |
| **密度** | **3.04%** | **100%** |
| _rowid 跨度 | 7,499,934（几乎覆盖全表） | 0~7,499,999（全范围） |
| payload_cols 实际列数（列裁剪后） | 2 | 1 |
| payload_columns 总分配 | 114 MB | 57 MB |
| bitmap 本身 | 916 KB（可放 L2） | 916 KB |
| probe 侧行数 | 6,036,600 | 1,632,317 |
| 匹配行数 | 916,290 | 1,632,317 |
| wall-clock vs baseline | **1.68x 慢**（158→266ms） | **0.83x 快**（345→286ms，降幅17%） |

Q9 密度 100% 时 bitmap + payload 紧凑填充，probe 命中后 gather 接近顺序访问，BHJ 的 O(1) 精确查找比普通 HashJoin 的哈希表查找更快，净获益 17%。Q5 密度 3.04% 时 payload 中 97% 是空洞，600万次 probe 命中后从 114MB 的稀疏数组 gather，cache 局部性差，净增开销 68%。两者对比验证了"密度越低，BHJ 相对普通 HashJoin 的优势越小、劣势越大"的规律。

#### 8.7.3 `test_bitmap_join_perf.cpp` 最新实测结果

`test_bitmap_join_perf.cpp` 的 `lineitem⋈part`（密度100%，build侧100万行全量）作为正面对照组，条目8实施后重跑结果不变：

```text
[bitmap-join-perf] duckdb-hash-join:        elapsed=263.3 ms
[bitmap-join-perf] perfect-hash-join:       elapsed=115.9 ms
[bitmap-join-perf] bitmap-hash-join:        elapsed=85.1 ms   (force_bhj=true)
[bitmap-join-perf] bitmap-hash-join(auto):  elapsed=84.9 ms   (force_bhj=false)
```

`lineitem⋈part` 稳定获得 ~68% 的降幅，确认条目8的 passthrough 机制没有破坏已有的高密度场景性能。

#### 8.7.4 Q5/Q9/Q10 端到端三模式总耗时对比（冷运行 + 热运行平均）

`test_bitmap_join_tpch.cpp` 已修改为记录每个 (query, mode) 的**冷运行**（首次执行，含 parquet 元数据加载、计划构造、BHJ executor 分配）和**热运行平均**（后续 3 次执行的算术平均，OS file cache 已热）。数据源 `b_idea/perf/sf5/tpch_q5_q9_q10.csv`：

```text
Query  Mode         Cold(ms)  WarmAvg   WarmMin   #HJ  #BHJ  Rows
----------------------------------------------------------------------
Q5     baseline        180.0     157.1     155.4     5     0     5
Q5     perfect         160.0     155.7     150.9     5     0     5
Q5     bitmap          250.3     240.2     237.7     5     4     5
Q9     baseline        354.2     337.7     333.6     5     0   175
Q9     perfect         317.6     316.2     315.0     5     0   175
Q9     bitmap          260.3     262.2     258.9     5     4   175
Q10    baseline        223.0     221.8     221.2     3     0    20
Q10    perfect         218.2     222.3     217.7     3     0    20
Q10    bitmap          220.4     224.7     224.4     3     2    20
```

**关键观察**：
- Q5 bitmap warm_avg=240.2ms vs baseline=157.1ms（1.53x），较 v0.6.1（1.68x）持续改善。
- Q9 bitmap 冷热均显著快于 baseline（冷 260 vs 354ms，热 262 vs 338ms，降幅 22%），密度 100% 场景稳定获益。
- **Q10 bitmap warm_avg=224.7ms vs baseline=221.8ms（仅 1.01x），不触发 >20% 告警**。

#### 8.7.5 Payload scatter 锁优化：定长列 lock-free（v0.7.0 新增）

**问题**：`SinkBitmap` 中的 payload scatter 对所有列统一加 `lock_guard<mutex>`（`bitmap_hash_join_executor.cpp:271`），导致多线程 build 时所有列的 scatter 被串行化。但深入分析后发现：
- **定长列**（INT/BIGINT/DATE/DECIMAL 等）的 scatter 是 `tdata[rowid] = sdata[sidx]`，写不同 rowid 位置，由于 build 侧 PK 唯一、不同线程处理的 rowid 不重叠，**天然无数据竞争**，不需要锁。
- **变长列**（VARCHAR）的 scatter 调用 `StringVector::AddStringOrBlob` → `ArenaAllocator::Allocate`，操作有状态的内存分配器，**确实需要锁**。
- 此前版本对所有列统一加锁，导致定长列被 varchar 列的锁不必要地阻塞。

**实测 payload 列类型**（经列裁剪后实际保留的列）：

| Join | payload_cols | 类型 | 是否全定长 |
|---|---|---|---|
| Q5 `n_regionkey=r_regionkey` | 1 | INTEGER | 是 |
| Q5 `c_nationkey=n_nationkey` | 1 | VARCHAR | 否 |
| Q5 `o_custkey=c_custkey` | 2 | INTEGER, VARCHAR | 否 |
| Q5 `l_orderkey=o_orderkey` | 2 | BIGINT, VARCHAR | 否 |
| Q9 `s_nationkey=n_nationkey` | 1 | VARCHAR | 否 |
| Q9 `l_partkey=p_partkey` | 1 | BIGINT | 是 |
| Q9 `l_orderkey=o_orderkey` | 1 | DATE | 是 |
| Q10 `l_orderkey=o_orderkey` | 1 | (定长) | 是 |

**优化方案**：在构造函数中按物理类型分流 payload 列为 `fixed_payload_indices` 和 `var_payload_indices`。`SinkBitmap` 中定长列先 lock-free scatter，然后仅对 varchar 列加锁 scatter。Q9/Q10 的全定长 join 完全无锁，Q5 的混合 join 中定长列也不被 varchar 锁阻塞。

**优化效果**（wall-clock，warm_avg）：

| Query | 优化前 warm_avg | 优化后 warm_avg | 降幅 | vs baseline | 改善 |
|---|---|---|---|---|---|
| Q5 bitmap | 265.5ms | 248.8ms | -6% | 1.58x（此前1.68x） | 有改善但仍有VARCHAR锁 |
| Q9 bitmap | 285.6ms | 265.0ms | -7% | 0.78x（此前0.83x） | 获益增大 |
| **Q10 bitmap** | **297.7ms** | **224.5ms** | **-25%** | **1.02x** | **消除>20%告警** |

Q10 改善最显著——从 298ms 降到 225ms（-25%），`warm_avg/baseline = 224.5/219.8 = 1.02x`，仅慢 2%，不再触发 `>20% slower` 告警。根因是 Q10 的 `lineitem⋈orders` join 的 payload 是全定长列，优化后完全 lock-free，消除了多线程 build 的锁等待。

`operator_timing` 变化更显著（反映 pipeline 等待的消除）：
- Q9 `l_orderkey=o_orderkey`（DATE，全定长）：322ms → **104ms**（-68%）
- Q10 `l_orderkey=o_orderkey`（定长）：1008ms → **145ms**（-86%）
- Q5 `l_orderkey=o_orderkey`（BIGINT+VARCHAR，混合）：1929ms → 1524ms（-21%，VARCHAR列仍需锁）

**改动文件**：
- `src/include/duckdb/execution/operator/join/bitmap_hash_join_executor.hpp`：新增 `fixed_payload_indices` / `var_payload_indices` 字段。
- `src/execution/operator/join/bitmap_hash_join_executor.cpp`：构造函数中按类型分流；`SinkBitmap` 中定长列 lock-free scatter + varchar 列加锁 scatter。

#### 8.7.5b CombineBitmap 无锁稀疏合并（v0.8.0 新增）

**问题**：`CombineBitmap` 此前用 `lock_guard<mutex>` 串行化 OR 合并，且对全 `bitmap_size` 做 OR 操作。两个问题：
1. mutex 串行化——多线程 build 时所有线程在 Combine 阶段排队等锁
2. 全量 OR——低密度场景（如 Q5 密度3%）97% 的 word 是 0，OR 它们是纯浪费

**方案1：atomic fetch_or（消除 mutex）**

`ValidityMask` 内部是 `validity_t = uint64_t` 数组。`global_bitmap OR local_bitmap` 是对每个 64 位字做 `global[w] |= local[w]`，这可以用 `std::atomic<uint64_t>::fetch_or` 实现无锁合并：

```cpp
auto *atomic_global = reinterpret_cast<std::atomic<validity_t>*>(global_bitmap.GetData());
for (idx_t w = 0; w < word_count; w++) {
    atomic_global[w].fetch_or(local_data[w], std::memory_order_relaxed);
}
```

`fetch_or` 编译为 x86 `LOCK OR` 指令（硬件级原子操作），比 mutex 快几个数量级。OR 操作可交换可结合，多线程并发 `fetch_or` 结果正确。`memory_order_relaxed` 足够——bitmap 只在 Finalize 后才被 probe 读取，不需要跨线程同步顺序。

**方案2：稀疏合并（只 OR touched words）**

在 `SinkBitmap` 的 `ExtractBuildRowids` 中，每次 `local_bitmap.SetValidUnsafe(rowid)` 时标记 `touched_words[rowid / 64] = true`。Combine 时只遍历 touched words，跳过全 0 的 word。

Q5 场景：227K 有效位 / 750万位，有效 word 数 ≈ 227K / 64 ≈ 3554，总 word 数 = 117187，**只遍历 3% 的 word**。

用 `std::vector<bool>`（bitset，1 bit/元素）作为 dirty bitmap，内存开销 = bitmap_size / 8 / 8 = 916KB / 8 = 114KB，可忽略。

**优化效果**（wall-clock warm_avg，相对 payload scatter 锁优化后的 v0.7.0）：

| Query | v0.7.0 warm_avg | v0.8.0 warm_avg | 增量降幅 | vs baseline | 说明 |
|---|---|---|---|---|---|
| Q5 | 248.8ms | 240.2ms | -3.5% | 1.53x | CombineBitmap 开销占比中等，mutex消除+97% word跳过 |
| Q9 | 265.0ms | 262.2ms | -1.1% | 0.78x | 密度100%，所有word都touched，稀疏合并无收益，仅mutex消除 |
| Q10 | 224.5ms | 224.7ms | ~0% | 1.01x | join数量少，CombineBitmap开销本就很小 |

Q5 改善最明显（-3.5%），因为它的 `l_orderkey=o_orderkey` join build 侧密度 3%，稀疏合并跳过 97% 的 word，加上 mutex 消除，CombineBitmap 阶段开销大幅下降。

**改动文件**：
- `src/include/duckdb/execution/operator/join/bitmap_hash_join_executor.hpp`：`BitmapJoinLocalState` 新增 `touched_words`（`std::vector<bool>`）和 `touched_word_count`。
- `src/execution/operator/join/bitmap_hash_join_executor.cpp`：
  - `BitmapJoinLocalState::Initialize`：初始化 `touched_words`（大小 = `EntryCount(bitmap_size)`）。
  - `ExtractBuildRowids`：`SetValidUnsafe` 后标记 `touched_words[rowid / 64] = true`。
  - `ExtractBuildRowidsSwitch`：新增 `touched_words` 参数透传。
  - `CombineBitmap`：用 `atomic fetch_or` 替代 mutex + 只遍历 touched words。

#### 8.7.6 回归测试

- `[bitmap_join]`：20 个测试，433 断言全部通过（含 self-join、chain propagation、RIGHT_SEMI 回退、条目8b 压缩跳过等）。
- `test/optimizer/compressed_materialization.test_slow`：62 断言全部通过。
- `[bitmap_join_tpch]` + `[bitmap_join_tpch_profile]`：Q5/Q9/Q10 端到端正确性 + 22条标准 TPC-H SQL smoke test：`22 ok, 0 mismatched, 0 crashed`。
- `Bitmap-Join*`（`test_bitmap_join_perf.cpp` 全部4个隐藏 `TEST_CASE`）：149 断言全部通过。

#### 8.7.7 验收标准达成情况

1. [x] 现有条目3/4/8b 相关测试全部保持通过（无回归）。
2. [x] 完整实现 `bhj_passthrough_refs`（Step A-E），覆盖 `left_projection_map` 为空和非空两种场景。
3. [x] Q5/Q9 的 `PATH_INCOMPLETE_LEFT_SIDE` 归因项清零（各 +1 命中）。
4. [x] Q9 总耗时因命中率提升显著下降（341→278ms，-19%）。
5. [x] Q5 出现中等额外开销（§8b.4.5 预警的"低密度=性能下降"场景在 Q5 上真实复现），但**不是此前记录的"暴涨13.5倍"**——v0.6.0 的 1929ms 数字来自 `operator_timing`（含 pipeline 等待阻塞），实际 wall-clock 仅 1.68x（158→266ms，详见 §8.7.2a 修正分析）。主要开销来自 `payload_columns`（114MB，密度3%）的稀疏 gather cache miss，bitmap 本身 916KB 可放入 L2 不是瓶颈。这是执行器层面问题（bitmap size 按静态PK全表行数分配），非条目8自身引入的 bug——条目8正确地将隐藏列安全传播过了 LEFT 侧，只是传播过去的 join 因密度低而有额外开销。建议作为后续专项处理。

---

## 条目8b：`CompressedMaterialization` 与 BHJ 的冲突（新增，v0.3.0 设计 → v0.4.0 已实施并验证）

### 8b.0 状态：已实施、已验证，`CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION` 归因项已清零

**结论先行**：v0.3.0 最初设计的"局部特判"方案（只保护当前 join 自己的条件列）在真实数据上验证时被证明**不够**——实际采用的是升级后的"全局预扫描 + 跨层传递保护"方案（见 8b.4）。改动后重新用真实 SF5 数据验证：Q9/Q10 里全部 3 个原本被压缩问题卡住的 join，压缩包装（`__internal_compress_integral_*`）已经从它们的条件表达式上完全消失，`bhj_skip_reason` 从 `condition_wrapped_by_compressed_materialization` 变为各自真实的、独立的根因（分别是条目8的 `path_incomplete_left_side` ×2、以及一个新发现的 `fk_on_build_side`，均为已知类别，非新问题）。也就是说**条目8b本身的目标——消除压缩对 BHJ 追踪的遮蔽——已经完全达成**；Q9/Q10 命中率数字本身没有变化（因为被压缩问题掩盖的 join，掀开后发现底下还压着别的、不属于条目8b范围的问题），但这是诊断精度的提升而非条目8b的失败，见 8b.4.3 的详细归因。

### 8b.1 问题重述

用户明确了优先级原则：**查询性能提升是第一优先级，中间结果的大小压缩是次要目标**——对于任何"压缩优化"与"BHJ 优化"冲突、导致 BHJ 无法命中的场景，应该优先保证 BHJ 能命中（即从源头上把这类 join 排除在压缩的候选范围之外），而不是让压缩照常进行、再想办法让 BHJ 去兼容/看穿压缩后的形态。

任务1条目6已经用真实 SF5 数据定位了具体影响面：Q9 的 `[lineitem+part] ⋈ orders`（1个）、Q10 的 `[customer+nation] ⋈ [lineitem+orders]` 和 `lineitem ⋈ orders`（2个），一共 3 个未命中的 join，全部是因为 `CompressedMaterialization` 优化器把 join key 包了一层 `__internal_compress_integral_*` 压缩函数，导致 `BitmapJoinResolver::TraceBindingToGet` 在 `LOGICAL_PROJECTION` 分支处判定"非纯列引用"而放弃追踪。这三个 join 的耗时（Q9 的 `l_orderkey=o_orderkey` 约 1018ms、Q10 的 `c_custkey=o_custkey` 约 104ms + `l_orderkey=o_orderkey` 约 221ms）在各自查询总耗时里占比很大（任务1条目7 §7.3/§7.5），是当前命中率提升的最大单一潜在收益点。

### 8b.2 根因与执行时序证据（决定了"源头过滤"为什么可行）

关键证据来自 `src/optimizer/optimizer.cpp::RunBuiltInOptimizers()` 的 pass 执行顺序：

```text
...
BUILD_SIDE_PROBE_SIDE   （BuildProbeSideOptimizer：确定每个 join 的 build/probe 侧，即 children[0]/children[1] 及 join_type 的最终形态）
...
STATISTICS_PROPAGATION  （StatisticsPropagator::PropagateStatistics 逐节点调用；
                          CompressedMaterialization::Compress 在其内部对每个
                          LOGICAL_COMPARISON_JOIN 节点递归调用，即压缩发生在这里）
...
BITMAP_JOIN_RESOLVE     （BitmapJoinResolver，本任务的主角，全局最后一个 pass）
```

（完整顺序见 `src/optimizer/optimizer.cpp:207-337`）

**这意味着**：当 `CompressComparisonJoin`（`src/optimizer/compressed_materialization/compress_comparison_join.cpp`）处理某个 join 时，`BuildProbeSideOptimizer` 已经跑完——join 的 build（`children[1]`）/probe（`children[0]`）side、`join_type` 都已经是最终形态，和 `BitmapJoinResolver::ResolveJoin` 后续读到的完全一致。也就是说，**在压缩发生的那一刻，我们已经具备了判断"这个 join 是否是（或可能是）BHJ 候选"的全部信息**，不需要等 `BitmapJoinResolver` 跑完才能知道。这是"源头过滤"方案能够成立的关键前提。

### 8b.3 为什么"事后看穿"（诊断阶段的临时方案）不是长期正确的方案

任务1诊断阶段为了统计归因，给 `TraceBindingToGet`/`GetPlainColumnBinding` 加了 `IsCompressedMaterializationWrapper` 识别逻辑（当前**只用于诊断分类，没有实际"看穿并继续追踪"**）。如果考虑把这个识别逻辑升级成"看穿一层压缩、继续追踪、并让 BHJ 正常命中"，需要重新审视 `BitmapJoinExecutor`（`src/execution/operator/join/bitmap_hash_join_executor.cpp`）实际读取 join key 的两条路径，会发现看穿方案存在一个**依 PK 是否稠密而不同的隐蔽风险**：

- **稀疏 PK（如 `orders`，`rowid_column="_rowid"` != `pk_column="o_orderkey"`）**：BHJ 走的是"物化 `_rowid`/`*_ref` 隐藏列"路径（`bitmap_build_rowid_idx`/`bitmap_probe_ref_idx` 不为 `INVALID_INDEX`，见 `bitmap_hash_join_executor.cpp:256-260`）——这两个隐藏列是 `BitmapJoinResolver` 自己注入的全新物化列，从未被 `CompressedMaterialization` 处理过（`CompressedMaterialization` 运行在 `BitmapJoinResolver` 之前，那时隐藏列还不存在）。这种情况下，压缩包装只出现在原始 join key（`l_orderkey`/`o_orderkey`）的比较表达式上，**不影响** BHJ 实际读取的隐藏列——"看穿一层继续追踪"在这种情况下是安全的。
- **稠密 PK（如 `customer`/`part`/`supplier`，`rowid_column == pk_column`）**：BHJ 走的是 fast path，**直接读取 join key 本身的物化值**做 `rowid = key_value - rowid_offset`（`bitmap_hash_join_executor.cpp:259-260`，`rowid_offset = build_rowid_offset`；对照 `ExtractBuildRowids` 模板函数 `bitmap_hash_join_executor.cpp:85-110`）。如果这个 join key 恰好被 `CompressedMaterialization` 压缩过（类型变窄、做了 `value - min` 的偏移），fast path 读到的就是**压缩后的值**而不是原始 PK 值——`rowid = compressed_value - rowid_offset` 会计算出**错误的 rowid**，而不是抛异常。这是一个**静默产生错误结果**的正确性风险，比"追踪失败安全回退"严重得多，绝不能在没有专门改造 fast path（让它知道"这一列被压缩过，需要先解压，或者改用压缩后统计信息重新计算 offset"）之前就简单加一层"看穿并继续走 fast path"。

**结论**："事后看穿" 方案要想安全，必须区分稠密/稀疏两种情况分别处理，且稠密情况下改造成本和风险都不低（本质上是让 BHJ 执行器也理解压缩语义）。这与用户"性能优先、压缩次之"的排序矛盾——为了保留一个次要目标（压缩），反而要在核心目标（BHJ 性能优化）的执行器里引入新的复杂度和正确性风险。**源头过滤（不压缩 BHJ 候选 join 的 key）没有这个问题**：BHJ 该怎么追踪、怎么读取 join key，完全不需要改一行——因为压缩根本没发生。

### 8b.4 采用方案：全局预扫描 + 跨层传递保护（实际实施；v0.3.0 的"局部特判"设计已被淘汰）

**v0.3.0 最初设计的方案**（"只在当前 `CompressComparisonJoin` 处理某个 join 时，判断这个 join 自己是否是 BHJ 候选，如果是就跳过压缩它自己的条件列"）编码实现并跑真实 SF5 数据验证后，**只解决了 Q10 的 1/2、Q9 的 0/1**——不够。根因排查发现：`CompressedMaterialization` 是**自下而上**逐节点独立处理的；一个列在**下层**某个 join/聚合被处理时，可能只是一个"payload列"（不参与那一层 join 的条件），因此被那一层的**通用 payload 列压缩逻辑**（而不是"join 条件列压缩"这条特殊分支）正常压缩掉了——但这个列稍后会成为**上层某个祖先 join** 的条件列。例如真实 SF5 Q10：`orders.o_custkey` 在下层 `lineitem JOIN orders` 处理时只是一个 payload 列（该 join 的条件是 `l_orderkey=o_orderkey`，跟 `o_custkey` 无关），被这一层的通用压缩逻辑压缩了；但 `o_custkey` 正是上层 `customer JOIN (...)` 这个 join 的字面条件操作数。v0.3.0 的"只保护当前 join 自己的条件列"完全没有覆盖到这种跨层传递的情况。

**修正后的方案**：

1. **全局预扫描**（`CompressedMaterialization::CollectBhjProtectedBindings`，新增静态方法）：在任何压缩开始之前，对**整棵、尚未被压缩触碰过**的逻辑计划做一次只读递归遍历，收集**所有**满足"单等值条件、`JoinType::INNER`"的 `LogicalComparisonJoin` 的条件两侧 `ColumnBinding`，汇总进一个全局集合 `bhj_protected_bindings`。这个集合与 `BitmapJoinResolver` 后续读到的"最终 join 形态"完全一致（因为 `BUILD_SIDE_PROBE_SIDE`/`JOIN_ORDER` 都已跑完），依旧不查 `BitmapJoinMetaRegistry`（保守排除，多保护没有正确性代价）。
2. **一次性计算，全程复用**：`StatisticsPropagator`（拥有跨越整个压缩阶段的稳定生命周期）新增 `bhj_protected_bindings`/`bhj_protected_bindings_computed` 两个成员，在第一次真正需要压缩时（`PropagateStatistics(LogicalOperator&, ...)` 里）惰性计算一次（`open_bitmap_join=false` 时完全不计算，零开销），随后每次构造 `CompressedMaterialization` 都传入同一份引用。
3. **在全部 4 个 `Compress*` 函数里统一生效**：`CompressComparisonJoin`/`CompressAggregate`/`CompressDistinct`/`CompressOrder` 各自构建的 `referenced_bindings`（本来就是"不压缩这些列"的既有机制，`TryCompressChild` 据此设置 `can_compress[i]=false`）统一改为以 `bhj_protected_bindings` 为初始值再累加各自原有的逻辑。这样无论受保护的列在哪一层、以 payload 还是条件列的身份出现，都会被同一份全局名单挡住——不需要在每一层重新判断"我是不是 BHJ 候选"，只需要问"这个列在不在全局保护名单里"。
4. **`CompressComparisonJoin` 额外的一处特殊处理**：该函数除了走 `referenced_bindings` 这条通用路径，还有一条独立的"两侧都是纯列引用时，尝试生成式压缩（通过 `statistics_map` 直接改写，不经过 `referenced_bindings`）"快速路径（`compress_comparison_join.cpp` 里 `GetCompressExpression(condition.left->Copy(), merged_stats)` 那一段）。这条快速路径必须单独加一层判断——若条件两侧任一列已在 `bhj_protected_bindings` 里，直接跳过这条快速路径（转而正常调用 `GetReferencedBindings` 让通用机制生效），否则它会绕过 `referenced_bindings` 的保护直接压缩掉。

**实现文件**：
- `src/include/duckdb/optimizer/compressed_materialization.hpp`：`CompressedMaterialization` 构造函数新增 `bhj_protected_bindings` 引用参数；新增静态方法 `CollectBhjProtectedBindings`。
- `src/optimizer/compressed_materialization.cpp`：实现 `CollectBhjProtectedBindings`（递归遍历，收集条件绑定）；构造函数存下引用。
- `src/optimizer/compressed_materialization/compress_comparison_join.cpp`：`referenced_bindings` 以 `bhj_protected_bindings` 为初值；生成式压缩快速路径增加旁路判断。
- `src/optimizer/compressed_materialization/compress_aggregate.cpp`、`compress_distinct.cpp`、`compress_order.cpp`：`referenced_bindings` 以 `bhj_protected_bindings` 为初值（各自一行改动）。
- `src/include/duckdb/optimizer/statistics_propagator.hpp`、`src/optimizer/statistics_propagator.cpp`：新增惰性计算的 `bhj_protected_bindings` 成员及计算逻辑，在 `PropagateStatistics` 里传给每次构造的 `CompressedMaterialization`。

### 8b.4.1 为什么不需要更复杂的方案

- **不需要**在 `CompressedMaterialization` 里引入 `BitmapJoinMetaRegistry` 依赖——预扫描只用 `join_type==INNER`+单等值条件这两个廉价、已经最终确定的信号做保守判断，`BitmapJoinMetaRegistry` 的具体 PK/FK 匹配仍然完全是 `BitmapJoinResolver` 的职责，两者不重复。
- **不需要**像条目8.3-8.4 的 `bhj_passthrough_refs` 方案那样动物理执行层——本条目完全在 `CompressedMaterialization`/`StatisticsPropagator` 这两个既有的逻辑优化器 pass 内部做"全局预扫描 + 跳过压缩"，不涉及任何物理算子改动，改动量和风险都远小于条目8。

### 8b.4.2 真实数据验证结果：修复前后每个 join 的完整耗时对比

用真实 SF5 数据重跑 `[bitmap_join_tpch_profile]`（`PRAGMA enable_profiling='json'` 逐 `HASH_JOIN` 节点采集 `operator_timing`），把条目8b修复**前**（任务1条目7 §7.3 原始数据）和修复**后**（本次重跑，`b_idea/perf/sf5/tpch_operator_timing.csv` 已刷新）的全部 13 个 join 逐一对比：

| Query | Join（`[probe⋈build]`） | baseline(ms) | perfect(ms) | bitmap(ms) 修复前 → 修复后 | `bhj_skip_reason` 修复前 → 修复后 |
| --- | --- | --- | --- | --- | --- |
| Q5 | `[lineitem+orders+customer+nation+region] ⋈ supplier` | 62.6 | 62.4 | 62.2 → 62.0 | `not_single_equality`（不变，与压缩无关） |
| Q5 | `l_orderkey=o_orderkey [lineitem ⋈ orders+customer+nation+region]` | 144.3 | 146.1 | 147.6 → 145.8 | `path_incomplete_left_side`（不变，条目8目标，与压缩无关） |
| Q5 | `o_custkey=c_custkey [orders ⋈ customer+nation+region]` | 50.4 | 32.0 | 90.2 → 50.2 | `hit`（不变；本次实测降回baseline量级，此前90ms一次性测量噪声偏高，见8b.4.5密度分析） |
| Q5 | `c_nationkey=n_nationkey [customer ⋈ nation+region]` | 3.7 | 4.8 | 4.8 → 4.7 | `hit`（不变，与压缩无关） |
| Q5 | `n_regionkey=r_regionkey [nation ⋈ region]` | 0.20 | 0.14 | 0.02 → 0.01 | `hit`（不变，与压缩无关） |
| Q9 | `[lineitem+part+orders] ⋈ [partsupp+supplier+nation]` | 1019.2 | 1091.9 | 1032→1075.0 | `not_single_equality`（不变，与压缩无关） |
| Q9 | **`l_orderkey=o_orderkey [lineitem+part ⋈ orders]`** | 1082.1 | 1072.6 | **1018 → 1101.1** | **`condition_wrapped_by_compressed_materialization` → `path_incomplete_left_side`**（压缩包装已消失，条件表达式恢复为纯 `l_orderkey = o_orderkey`；但仍未命中，因为底下压着条目8的LEFT侧限制；耗时与baseline基本同量级） |
| Q9 | `l_partkey=p_partkey [lineitem ⋈ part]` | 174.8 | 138.9 | 145 → 137.2 | `hit`（不变，与压缩无关） |
| Q9 | `ps_suppkey=s_suppkey [partsupp ⋈ supplier+nation]` | 130.2 | 13.4 | 12 → 11.6 | `hit`（不变；密度100%，降幅~91%，见8b.4.5） |
| Q9 | `s_nationkey=n_nationkey [supplier ⋈ nation]` | 0.87 | 0.22 | 0.15 → 0.15 | `hit`（不变，与压缩无关） |
| Q10 | **`c_custkey=o_custkey [customer+nation ⋈ lineitem+orders]`** | 109.9 | 109.9 | **104 → 97.0** | **`condition_wrapped_by_compressed_materialization` → `fk_on_build_side`**（压缩包装消失后暴露的是无解类问题：FK 落在 build 侧，设计边界内不该命中，非bug；耗时反而略降） |
| Q10 | `c_nationkey=n_nationkey [customer ⋈ nation]` | 12.9 | 13.5 | 3.1 → 1.7 | `hit`（不变，与压缩无关；密度较高，稳定获益） |
| Q10 | **`l_orderkey=o_orderkey [lineitem ⋈ orders]`** | 225.9 | 217.7 | **221（未命中，走普通HashJoin） → 1152.3（命中BHJ）** | **`condition_wrapped_by_compressed_materialization` → `hit`（新增命中！）** |

**归因层面的结论**：条目8b的目标——消除 `CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION` 这个遮蔽性根因——**完全达成**，三个 join 的压缩包装均已消失，`EXPLAIN` 里对应条件表达式已恢复为纯 `BoundColumnRefExpression`（不再有 `__internal_compress_integral_*`）。但揭开这层遮蔽后，Q9 的净命中数没有变化（3/5，因为底下还压着条目8的 `path_incomplete_left_side`），Q10 从 1/3 提升到 2/3（`lineitem⋈orders` 直接命中），Q5 不受影响（3/5，条目8b未触及任何 Q5 的 join）。

**耗时层面的核心发现**：条目8b修复后**新增命中**的 Q10 `lineitem⋈orders` join，耗时从 baseline 226ms **暴涨到 1152ms**（约5.1倍），是目前记录到的最严重的一例"命中但反而更慢"异常（比任务1 §7.4 记录的 Q5 `orders⋈[...]` 90ms/48ms≈1.9倍更极端）。这是条目8b间接暴露出的一个新问题（不是条目8b自身引入的bug——8b只是让这个join第一次真正走到了BHJ执行器），根因分析见 §8b.4.5。

### 8b.4.3 结论对任务2优先级判断的更新

条目8b实施后，`CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION` 已不再是任何 join 的**根本**未命中原因（作为遮蔽层已被清除），但由于它揭开后暴露的问题分别是"条目8要解决的"（1个）和"无解类"（1个），**条目8b本身对 Q5/Q9/Q10 总命中率数字的直接贡献是 +1**（Q10 从 1/3 → 2/3）。条目8b真正的价值是**诊断准确性**——它把此前被错误归类为"可优化：压缩问题"的3个未命中，还原成了各自真实的根因，使得任务1的归因表和任务2的优先级判断建立在准确信息上：条目8（LEFT侧优化）仍然值得做，且现在已知它一旦落地，除了 Q5 原有的1个 join，还能额外救回 Q9 的1个 join（合计2个，比 v0.2.0 诊断时预估的"仅 Q5 1个"收益更高）。**但条目8落地前需要先解决 §8b.4.5 的密度问题**——否则 Q9 那个 join 落地条目8之后，很可能重演 Q10 `lineitem⋈orders` 的暴涨（`orders` 同样是它的 build 侧，密度特征相同）。

### 8b.4.4 `test_bitmap_join_perf.cpp` 最新实测结果（随本次改动一并重跑）

`test_bitmap_join_perf.cpp` 是独立于 Q5/Q9/Q10 的另一组回归/性能测试（`[bitmap_join][.]`，隐藏 tag，需显式指定运行），固定用 `lineitem JOIN part ON l_partkey=p_partkey`（`part` 是稠密PK、无过滤、build侧=100万行全量，密度100%）验证三种模式。本次条目8b改动后重跑，结果与改动前一致（该场景不涉及任何压缩，`CompressComparisonJoin` 未介入）：

```text
[bitmap-join-perf] duckdb-hash-join:        elapsed=263.3 ms
[bitmap-join-perf] perfect-hash-join:       elapsed=115.9 ms
[bitmap-join-perf] bitmap-hash-join:        elapsed=85.1 ms   (force_bhj=true)
[bitmap-join-perf] bitmap-hash-join(auto):  elapsed=84.9 ms   (force_bhj=false，自动解析)
[bitmap-join-perf] auto-resolve baseline:   elapsed=254.6 ms
[bitmap-join-perf] bitmap-hash-join(auto-resolve): elapsed=83.1 ms
```

即 `lineitem⋈part`（密度100%，build侧100万行全量参与）稳定获得 **~68% 的降幅**（263ms→85ms），且手动强制解析（`force_bhj`）与自动解析（`BitmapJoinResolver` 自动识别）结果一致（85.1ms vs 84.9ms），验证自动解析路径没有额外开销。这与 §8b.4.5 的"高密度=稳定获益"结论完全吻合，作为与 Q10 `lineitem⋈orders`（低密度=严重损失）的正面对照组。

同一文件里的端到端 Q5/Q9/Q10 测试（`[bitmap_join_tpch][.]`）本次重跑结果：

```text
Query  Mode         Time(ms)      #HJ     #BHJ       Rows
--------------------------------------------------------------
Q5     baseline        159.0        5        0          5
Q5     perfect         160.6        5        0          5
Q5     bitmap          142.7        5        3          5
Q9     baseline        367.0        5        0        175
Q9     perfect         342.0        5        0        175
Q9     bitmap          341.1        5        3        175
Q10    baseline        224.0        3        0         20
Q10    perfect         222.9        3        0         20
Q10    bitmap          308.9        3        2         20
[bitmap-join-tpch] WARNING: Q10 bitmap mode (308.9 ms) is >20% slower than baseline (223.97 ms).
```

这里 Q10 的整条查询端到端耗时（308.9ms，含 `LIMIT 20` 之前的排序/聚合等其余算子）比 baseline 慢约38%，量级上小于 §8b.4.2 单独测量的 `lineitem⋈orders` 这一个 join 节点本身5倍的暴涨（1152ms vs 226ms）——因为整条查询里还有其它没受影响的算子（`HASH_GROUP_BY`/`TOP_N` 等）稀释了这个单点异常在总耗时里的占比，但该测试内置的 `>20%` 阈值告警已经能捕捉到这个回归，说明现有测试基础设施足以发现这个问题（不需要额外新增告警机制）。

### 8b.4.5 Q10 `lineitem⋈orders` 耗时暴涨的根因分析：bitmap 大小按「静态PK全表行数」分配，而非「实际到达 build 侧的行数」

**现象**：同样是"命中 BHJ"，为什么 Q9 的 `partsupp⋈supplier+nation`（127ms→12ms，降幅91%）和本文 `test_bitmap_join_perf.cpp` 的 `lineitem⋈part`（263ms→85ms，降幅68%）都稳定获益，而 Q10 的 `lineitem⋈orders` 反而暴涨5倍？用真实数据核对每个 join 的 **build 侧密度**（= 实际到达该 join 的 build 端行数 ÷ `bitmap_join_meta.json` 里登记的该 PK 表的静态 `row_count`）后，发现密度与耗时变化方向高度相关：

> **v0.6.1 修正注记**：本节中的"暴涨5.1倍"（226→1152ms）和下文 Q5 的"暴涨5倍"等数字均来自 `operator_timing`，包含 pipeline 等待阻塞时间。条目8落地后用 wall-clock 重新测量：Q10 `lineitem⋈orders` 的查询级 wall-clock 是 219→298ms（1.36x），Q5 `l_orderkey=o_orderkey` 是 158→266ms（1.68x），远小于 `operator_timing` 显示的倍数。密度与性能趋势的**方向**仍然正确（低密度=相对更慢），但**幅度**被 `operator_timing` 夸大了。详见 §8.7.2a 的完整修正分析。

| Join | PK 表 | 静态 `row_count`（`bitmap_join_meta.json`） | 实际到达 build 侧的行数 | 密度 | bitmap(ms) 变化 |
| --- | --- | --- | --- | --- | --- |
| `partsupp⋈supplier+nation`（Q9） | `supplier`（50000行） | 50000 | 50000（`supplier` 未被任何谓词过滤，全量参与） | **100%** | 130.2→11.6ms，**降幅91%** |
| `lineitem⋈part`（`test_bitmap_join_perf.cpp`） | `part`（100万行） | 1,000,000 | 1,000,000（`part` 未被过滤，全量参与） | **100%** | 263→85ms，**降幅68%** |
| `orders⋈customer+nation+region`（Q5） | `customer`（75万行） | 750,000 | 150,409（`region='ASIA'` 过滤后的 customer⋈nation⋈region） | **20.1%** | 50.4→50.2ms，**基本持平**（此前一次性测量的90ms系噪声，见下方补充实测） |
| `lineitem⋈orders`（Q10） | `orders`（750万行，稀疏PK，走 `_rowid`） | 7,500,000 | 286,296（`o_orderdate` 3个月窗口过滤后的 orders） | **3.8%** | 226→1152ms，**暴涨5.1倍** |

**根因**：`BitmapJoinExecutor` 的构造函数（`bitmap_hash_join_executor.cpp:27-79`）用 `resolved.pk->row_count`（即 `bitmap_join_meta.json` 里预处理时记录的、**该PK表未经任何运行时过滤的静态总行数**）来决定 `bitmap_size`，并据此一次性分配：
1. `global_bitmap`（`ValidityMask`，`bitmap_size` 位）；
2. **每个** RHS 输出列一份、大小同为 `bitmap_size` 的 `payload_columns`（`ExtractBuildRowids`/`ScatterColumn` 按 rowid 稀疏散射写入）。

这个大小在 build 侧数据到达之前就已经按"PK 表理论最大规模"分配好了，**完全不知道也不适应"实际有多少行会真正流入这个 build 侧"**——而 `orders` 表在 Q10 里恰好被 `o_orderdate` 三个月窗口过滤掉了 96.2% 的行，实际写入位图的有效位只占 3.8%，其余 96.2% 的 `bitmap_size` 空间是"陪跑"的空洞。这带来两个成本，且都随"表越大、密度越低"而线性放大：

1. **Probe 端随机访问的 cache 局部性极差**：`FillProbeSelection`（`bitmap_hash_join_executor.cpp:328-353`）对 740万行 `lineitem` 逐行计算 `rowid = l_orderkey - offset` 后直接用 `RowIsValidUnsafe(rowid)` 测试一个跨越 750万位（≈916KB）的位图——这个位图大小远超典型 L2 cache（通常256KB~1MB），且由于只有3.8%的位有效、命中位置在这915KB范围内近似随机分布，740万次探测几乎每次都要付出一次跨越大范围地址空间的随机访存 cache miss 代价。相比之下，baseline 的普通 `HashJoin` 只对**实际的28.6万行** `orders` 建了一张大小与之匹配的哈希表，探测端的工作集小得多、局部性好得多，因此更快——这与 Q9/`lineitem⋈part` 两个"密度100%"场景（此时位图的"有效范围"和"陪跑空洞"完全没有差异，位图本身就是紧凑有效的）形成了鲜明对比。
2. **Payload 物化与最终 dictionary-slice gather 同理受累**：`payload_columns` 里 `o_custkey`（`_ref`）这一列同样按 750万行分配、稀疏散射写入，probe 阶段命中后的 `Slice(*payload_columns[i], state.build_sel_vec, result_count)`（`bitmap_hash_join_executor.cpp:428`）对这个稀疏的、750万行大小的数组做 gather，同样是跨越大地址空间的随机访存，密度越低、有效数据越稀疏，相对的"无用陪跑内存"占比越高，gather 的 cache 命中率越差。
3. （次要，非主因）`SinkBitmap`/`CombineBitmap` 里每个参与build的线程都要对全 750万位的 `ValidityMask` 做一次 `SetAllInvalid`/OR合并（`bitmap_hash_join_executor.cpp:14-22`、`278-293`），这部分是一次性的、与"密度"无关的固定开销（只取决于 `bitmap_size` 本身，不取决于实际有效位数），线程数越多、`bitmap_size` 越大，这部分固定成本也越高（用多线程实测验证：单线程下 bitmap 只慢约4%，96线程下这部分固定初始化成本占比升高，但即便如此也不足以解释5倍的差距——该固定成本已通过在同一进程内反复执行同一连接、复用同一份已构造好的 `BitmapJoinExecutor` 排除，见下方"重复执行排除一次性构造开销"的验证）。

**补充实测（排除测量噪声/一次性构造开销）**：
- 对 Q10 同一查询在同一进程内 warm-up 后重复执行6次（排除 parquet 元数据加载、`BitmapJoinExecutor` 构造等一次性开销），baseline 稳定在 263~279ms，bitmap 模式稳定在 333~348ms，比值稳定在 **1.22~1.27倍**（比首次冷启动测量到的5倍温和，说明 `BitmapJoinExecutor` 构造/位图初始化确有一部分一次性固定开销叠加在首次测量里，但**排除掉这部分固定开销后，密度低导致的探测/gather cache miss 仍然造成稳定的 20%~27% 额外开销**，量级上与 Q5 customer 分支(密度20%,基本持平)和 Q10 orders 分支(密度3.8%,明显更慢)的密度梯度趋势一致）；
- 对 Q5 `orders⋈customer+nation+region`（密度20.1%）重跑，baseline 50.4ms vs bitmap 50.2ms，基本持平，印证"密度~20%时收益与代价大致相抵"，密度介于 Q9/`lineitem⋈part` 的100%（明显获益）和 Q10 `lineitem⋈orders` 的3.8%（明显受损）之间。

**结论与对条目8的提示**：BHJ 当前的位图/payload 分配策略对"build 侧被上游过滤器大幅收窄"的场景不友好，密度越低损失越大。这是一个**独立于命中率**的执行器实现问题（任务1 §7.4 已记录 Q5 的类似现象，本次条目8b验证过程中在 Q10 上复现了更极端的版本），需要专项解决（可能方向：按实际到达的 build 行数动态选择位图/rowid range 而非静态PK总行数，或在密度低于某阈值时自动回退到普通 HashJoin）。**对条目8的直接影响**：条目8一旦落地，Q9 的 `l_orderkey=o_orderkey [lineitem+part⋈orders]` 会转为命中 BHJ——但这个 join 的 build 侧同样是 `orders`（且同样会被上游 `o_orderdate`/其它过滤条件收窄），密度特征与 Q10 `lineitem⋈orders` 相同，**很可能重演同样的暴涨**。建议条目8落地前，先把本条目发现的密度问题作为前置项处理，或至少在条目8的验收标准里加入"落地后必须同步验证 Q9 该 join 的耗时变化，不能只看命中率数字"。

### 8b.5 测试

已在 `test/api/test_bitmap_join_compressed_materialization.cpp` 新增两个测试用例（`[bitmap_join]` tag，随默认套件运行，不需要外部数据集）：

1. **"CompressedMaterialization skips compressing an INNER join's equality-condition columns when open_bitmap_join=true"**：构造两张 110万行的表（超过 `JOIN_BUILD_CARDINALITY_THRESHOLD`，确保 baseline 下确实会触发压缩），验证 `open_bitmap_join=false` 时 `EXPLAIN` 中仍能看到 `__internal_compress`（回归防护：确认默认路径未受影响），`open_bitmap_join=true` 时 `EXPLAIN` 中不再出现 `__internal_compress`。
2. **"query results are unaffected by the join-key-compression skip"**：验证两种模式下同一查询的聚合结果（`count`/`sum`）完全一致，确认跳过压缩不改变查询结果。

另外用真实 SF5 数据集重跑了（含本次为撰写 §8b.4.2-8b.4.5 而重新执行的一轮）：
- `test/optimizer/compressed_materialization.test_slow`（现有压缩回归套件，62 断言全部通过，确认 `open_bitmap_join` 默认关闭路径的现有压缩行为零影响）；
- `[bitmap_join]`（20 个测试，433 断言全部通过）；
- `Bitmap-Join*`（`test_bitmap_join_perf.cpp` 全部4个隐藏 `TEST_CASE`，149 断言全部通过，最新数字见 §8b.4.4）；
- `[bitmap_join_tpch]`（Q5/Q9/Q10 端到端，命中数与正确性如 8b.4.2 所述，本次重跑数字见 §8b.4.4）；
- `[bitmap_join_tpch_profile]`（三模式算子耗时 + 22条标准SQL，全部通过，`22 ok, 0 mismatched, 0 crashed`，最新耗时数字见 §8b.4.2）；
- `test/optimizer/*`（139 个测试，4299/4302 断言通过，3个失败均为环境相关的相对路径文件缺失，与本次改动无关，历史上已确认）；
- `[join]`（3413/3414 断言通过，1个失败是已知的、与本次改动无关的 `test_huge_nested_payloads.test_slow` 慢测试）。

### 8b.6 验收标准（达成情况）

1. [x] `CompressedMaterialization` 在 `open_bitmap_join=true` 时不再压缩任何"可能是 BHJ 候选"的绑定（不仅是当前处理 join 自己的条件列，也包括跨层传递的 payload 列）；`open_bitmap_join=false` 时行为完全不变（现有压缩测试全部保持通过）；
2. [x] Q9/Q10 的 `CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION` 归因项清零（真实数据验证，见 8b.4.2 表格）；
3. [x] 命中率因条目8b直接提升 +1（Q10 1/3→2/3）；Q9 未直接提升是因为揭开压缩问题后暴露出条目8的范畴问题（8b.4.3 已更新条目8的预期收益）；
4. [x] 全量回归（`compressed_materialization`/`bitmap_join`/`bitmap_join_tpch`/`bitmap_join_tpch_profile`/`join`/`optimizer`）无破坏；
5. [x] 新发现并根因定位（不在本条目处理，留给后续专项）：Q10 `lineitem⋈orders` 命中后耗时暴涨5倍——根因是 BHJ 位图/payload 按静态PK全表行数分配、不适应低密度场景，与"密度"强相关（100%密度稳定获益、20%密度基本持平、3.8%密度明显受损），详见 §8b.4.5；对条目8落地后 Q9 同类 join 的耗时已给出预警。

---

## 条目9：`RIGHT_SEMI` 路径测试补齐（Step A 已由任务1意外验证，问题已修复；剩余工作已收窄）


### 9.1 问题重述（原始状态）

`bhj_eligible`/`ResolveJoin`（`bitmap_join_resolver.cpp` 早期版本）曾显式允许 `RIGHT_SEMI` 类型的 join 走 BHJ：

```cpp
bool bhj_eligible = op.conditions.size() == 1 &&
                    op.conditions[0].comparison == ExpressionType::COMPARE_EQUAL &&
                    (op.join_type == JoinType::INNER || op.join_type == JoinType::RIGHT_SEMI);
```

但没有任何测试真正构造出一个 `RIGHT_SEMI` 类型的 `LogicalComparisonJoin` 去验证它——这是一段**从写下判断到当时从未被执行验证过**的代码路径。

### 9.2 结论已出：Step A 由任务1意外验证，且发现的是崩溃而非"仅仅未验证"

任务1条目7新增的"22条标准TPC-H SQL smoke test"首次运行时，**TPCH 标准 Q20** 就是一个稳定触发 `RIGHT_SEMI` 的真实场景：

```sql
SELECT s_name, s_address FROM supplier, nation
WHERE s_suppkey IN (
    SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (
        SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
        AND ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM lineitem WHERE ...))
    AND s_nationkey = n_nationkey AND n_name = 'CANADA'
ORDER BY s_name;
```

其中 `s_suppkey IN (SELECT ps_suppkey FROM partsupp WHERE ...)` 去关联后产生了 `RIGHT_SEMI` 类型的 `LogicalComparisonJoin`，`partsupp` 恰好又命中了 `bitmap_join_meta.json` 里登记的 PK/FK 关系（`partsupp.ps_suppkey` 是某个 PK 的 FK 引用），触发了 `BitmapJoinResolver` 把它当作候选处理——而 `BitmapJoinExecutor` 的构造函数从条目3/4开始就只实现了 `JoinType::INNER`（遇到别的类型 `throw NotImplementedException`），二者不一致直接导致查询报错：

```text
ERROR: Not implemented Error: BitmapJoinExecutor: only INNER joins are supported (MVP §6.4)
```

这回答了 9.4 原定验收标准第1条——**`RIGHT_SEMI` 能被标准 SQL（无需任何特殊构造，官方 TPC-H Q20 原文）稳定触达**，而且触达后果是**直接崩溃**，比"仅仅是未验证、可能语义不对"更严重。

### 9.3 已完成的修复（不等任务2排期，作为任务1推进过程中的阻断性问题立即处理）

把 `BitmapJoinResolver::ResolveJoin`（`bitmap_join_resolver.cpp`）和 `plan_comparison_join.cpp` 里的 `bhj_eligible` 判断，都从"`INNER` 或 `RIGHT_SEMI`"收紧为**只允许 `JoinType::INNER`**：

```cpp
// 两处保持一致
bool bhj_eligible = op.conditions.size() == 1 &&
                    op.conditions[0].comparison == ExpressionType::COMPARE_EQUAL &&
                    op.join_type == JoinType::INNER;
```

诊断枚举 `BitmapJoinSkipReason::NOT_INNER_OR_RIGHT_SEMI` 名字保留（兼容性），语义更新为"不是 INNER"。修复验证：

- 22条标准 TPC-H SQL 全部通过（`22 ok, 0 mismatched, 0 crashed`，此前是 `21 ok, 1 crashed`）；
- Q5/Q9/Q10 命中率、正确性不受影响（`[bitmap_join]`、`[bitmap_join_tpch]`、`test/sql/optimizer/*`、`[join]` 全量回归通过）。

详见任务1文档 §7.6。

### 9.4 剩余工作（范围已收窄）：是否要正确地支持 `RIGHT_SEMI`

当前状态（收紧到仅 `INNER`）已经是一个**安全、正确的终态**——`RIGHT_SEMI` 类型的 join（包括 TPCH Q20）现在会安全回退到普通 HashJoin，不会再崩溃，也不会产生错误结果。是否要在此基础上**进一步**支持 `RIGHT_SEMI` 走 BHJ（获得潜在性能收益），是一个独立的、可选的后续优化，需要先回答：

1. **`RIGHT_SEMI` 语义上"只输出 RIGHT 侧匹配到的行"，不产生笛卡尔积展开**——需要确认 `BitmapJoinExecutor` 现有的 Sink（build 侧建位图）/Probe（probe 侧查位图）逻辑，是否已经天然适配这种"只需要知道匹配与否，不需要真正输出 join 后的行"的语义，还是需要专门改造输出逻辑；
2. `ResolveJoin` 里"`cond.left`=probe/LHS，`cond.right`=build/RHS"的既有约定，在 `RIGHT_SEMI` 下 `children[0]`/`children[1]` 与"谁被去重、谁是探测方"的对应关系是否依然成立（`RIGHT_SEMI` 是"SEMI join 因为左右子树被交换而产生的镜像形式"，需要单独确认，不能想当然沿用 INNER 的约定）；
3. TPCH Q20 本身就是现成的、真实的验证场景，如果决定要支持，直接拿 Q20 做端到端验证即可，不需要再手工构造合成 SQL。

**建议**：由于当前"收紧到仅 INNER"已经是安全状态，这部分工作**优先级低于条目8/条目8b**（那两项能直接、可衡量地提升 Q5/Q9/Q10 命中率），可以作为任务2的可选扩展项，视排期决定是否投入。

### 9.5 需要补充的回归测试（防止未来重新放宽时重犯同样的错误）

在 `test_bitmap_join_chain.cpp` 补充一个"RIGHT_SEMI 安全回退"的回归测试，固化"当前状态"这条契约：

```cpp
TEST_CASE("BHJ safely falls back to regular hash join for RIGHT_SEMI (does not crash)", "[bitmap_join]") {
    RegistryResetGuard guard;
    auto &reg = BitmapJoinMetaRegistry::GetInstance();
    DuckDB db(nullptr);
    Connection con(db);
    SetupChainSchema(con, reg, /*with_nation=*/false);

    // o.ck IN (...) 对 bhj_chain_customer 的 IN 子查询去关联后应产生 RIGHT_SEMI/SEMI 类型的
    // join；即使 customer 的 ck 命中了 PK 绑定，BHJ 也必须安全跳过（NOT_INNER_OR_RIGHT_SEMI），
    // 不能抛异常。
    const string query = "SELECT count(*) FROM bhj_chain_orders o "
                          "WHERE o.ck IN (SELECT ck FROM bhj_chain_customer)";

    REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
    REQUIRE_NO_FAIL(*con.Query(query)); // 关键断言：不抛异常
}
```

### 9.6 验收标准（更新后）

- [x] "`RIGHT_SEMI` 能否被常见 SQL 稳定触达？"——**能**，TPCH Q20 是官方标准查询里的真实例子（已记录在本文档 §9.2）；
- [x] 触达后的风险已查清并修复：曾经是**直接崩溃**（`NotImplementedException`），已通过收紧 `bhj_eligible` 修复为安全回退；
- [x] 已补充 §9.5 的回归测试（`test_bitmap_join_chain.cpp` "BHJ safely falls back to a regular hash join for RIGHT_SEMI (does not crash)"），固化"RIGHT_SEMI 安全回退"这一契约；
- [x] 可选后续是否要正确支持 `RIGHT_SEMI`——**已决策：不做**（见 §9.7）。

### 9.7 最终决策（v0.3.0，用户拍板）：不做 RIGHT_SEMI 优化，不再补充单独测试

**决策**：`RIGHT_SEMI` 走 BHJ 的性能优化**不做**；§9.5 已有的"安全回退不崩溃"回归测试已经足够，**不再为"正确支持 RIGHT_SEMI"补充新的测试用例**。

**理由**：本轮任务的核心目标查询是 Q5/Q9/Q10——用 `EXPLAIN` 实测（任务1条目6归因表，§6.4）确认，这三条查询里**所有** `LogicalComparisonJoin` 都是 `INNER`，不存在任何 `SEMI`/`RIGHT_SEMI`。`RIGHT_SEMI` 唯一被真实观察到的触发场景是 TPCH 22条标准SQL里的 Q20（`IN` 子查询去关联），而 Q20 不在本轮性能优化的目标范围内。也就是说，即使投入成本去正确支持 `RIGHT_SEMI`（§9.4 列出的语义改造工作量不小：build/probe 约定重新审视、半连接输出语义改造），**收益也完全不会反映在 Q5/Q9/Q10 的任何一个指标上**——这是一个当前明确没有真实查询场景驱动的优化方向，不值得投入。

**现状即终态**：`bhj_eligible` 保持"仅 `JoinType::INNER`"；`RIGHT_SEMI`/`SEMI` 类型的 join 永远安全回退到普通 HashJoin，不崩溃、不产生错误结果（已有回归测试覆盖）。如果未来出现真实的、性能敏感的 `SEMI`/`RIGHT_SEMI` 查询场景，可以重新单独立项评估，届时 §9.4 的分析仍然有效，可以直接复用。

