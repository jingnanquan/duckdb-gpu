# BitmapJoinResolver 代码说明文档

> 对应源码：`src/optimizer/bitmap_join_resolver.cpp`
> 头文件：`src/include/duckdb/optimizer/bitmap_join_resolver.hpp`
> 元数据：`src/include/duckdb/catalog/catalog_entry/bitmap_join_meta.hpp`

本文档详细说明 DuckDB 中 **Bitmap-Join（位图连接，简称 BHJ）自动解析优化 pass** 的实现原理、数据流、关键数据结构与边界条件。

---

## 1. 背景与定位

Bitmap-Join 是一种面向「维度表（PK）JOIN 事实表（FK）」这类星型/雪花模型查询的优化执行方式：维度表行数有限、主键可被编码成 **稠密 rowid**（0/1/2…）；事实表上预先物化了一个 `*_ref` 列，直接存「指向维度表 rowid 的位置」。在这种前提下，连接可以被改写成「位图过滤」而非逐行哈希探测，从而大幅减少随机内存访问。

`BitmapJoinResolver` 是这套机制在 **逻辑计划优化阶段** 的「自动识别器」：

- 它在 **ColumnBindingResolver 之前** 运行（是最后一个内置 optimizer pass）；
- 它是一个 **纯读取 + 标注（read + annotate）** 的 visitor：只读逻辑计划、查元数据注册表，成功后将结果挂到 `LogicalComparisonJoin::bhj_hint`（以及两个隐藏列引用）上；
- 它 **不重写表达式**，因此默认 `VisitReplace` 保持为 no-op；
- 下游 `PlanComparisonJoin` 消费 `bhj_hint`，据此把 `PhysicalHashJoin::use_bitmap_join` 打开并接线位图执行所需的两个隐藏列索引。

设计目标（来自代码注释中反复提到的「条目 1/2/3/4」「设计文档 b_idea/6.4」）：
- **条目 1/2**：把连接条件里的列「反查」回其底层 `LogicalGet` 的真实表名/列名，用于和 `BitmapJoinMetaRegistry` 匹配；
- **条目 3**：当 PK 列本身不是稠密 rowid 时，需要向构建侧/探测侧各自注入并向上传播一个「隐藏物化列」；
- **条目 4**：处理多连接链（multi-join chain）中，连接条件一侧的列隔着一层中间 INNER JOIN 才到达 `LogicalGet` 的情况。

---

## 2. 整体入口与递归结构

```cpp
void BitmapJoinResolver::VisitOperator(LogicalOperator &op) {
    VisitOperatorChildren(op);                       // 先递归子节点，让嵌套 join 各自解析
    if (op.type == LOGICAL_COMPARISON_JOIN) {
        ResolveJoin(op.Cast<LogicalComparisonJoin>());
    }
}
```

- **自底向上**：先递归孩子，再处理当前算子。这样嵌套的 join 会先让内层拿到自己的 hint。
- 只对 `LOGICAL_COMPARISON_JOIN` 调用 `ResolveJoin`。

`ResolveJoin` 是核心，流程见第 4 节。

---

## 3. 关键数据结构（匿名命名空间内）

### 3.1 `PropagationSide` 枚举
```cpp
enum class PropagationSide { LEFT, RIGHT };
```
表示一条中间 `LOGICAL_COMPARISON_JOIN` 在路径里属于被追溯列的哪一侧。只对 `PathStep` 中 `op` 是 join 时有意义（见条目 4）。

### 3.2 `PathStep`
```cpp
struct PathStep {
    reference<LogicalOperator> op;
    PropagationSide side = PropagationSide::LEFT;
};
```
记录从隐藏列最终来源 `LogicalGet` 一路向上、回溯到「需要消费它的那个 join」所经过的 **每一跳中间算子**。

### 3.3 `TracedBinding`（追溯结果）
```cpp
struct TracedBinding {
    optional_ptr<LogicalGet> get;        // 最终来源的 scan
    idx_t get_column_index;              // 在 get->GetColumnIds() 中的位置
    vector<PathStep> path;               // 中间算子，top-down 顺序（front 靠近 join，back 靠近 Get）
    bool path_complete = true;           // 是否整条链都可安全「再暴露一个新列」
};
```

`path` 的顺序约定很重要：
- `path.front()` 离消费 join 最近，`path.back()` 离 `Get` 最近；
- 后续 `PropagateHiddenColumn` 会从尾到头（reverse）沿 `path` 把隐藏列「泵」回去。

`path_complete` 标志：
- 当向下追溯 `get` 的过程中经过了 **本解析器不知道如何安全重暴露新列** 的算子（如 Aggregate，或不会保证「left ++ right」输出布局的 join 类型）时，置为 `false`；
- `get` / `get_column_index` 始终有效，可用于「条目 1/2」的目录名反查；但 `path_complete == false` 会禁止「条目 3/4」的隐藏列传播，强制回退到普通哈希 join。

---

## 4. `ResolveJoin` 主流程

```cpp
void BitmapJoinResolver::ResolveJoin(LogicalComparisonJoin &op)
```

### 步骤 A：BHJ 资格判定（与 `plan_comparison_join.cpp` 的 `bhj_eligible` 一致）
```cpp
bool bhj_eligible = op.conditions.size() == 1 &&
                    op.conditions[0].comparison == ExpressionType::COMPARE_EQUAL &&
                    (op.join_type == JoinType::INNER || op.join_type == JoinType::RIGHT_SEMI);
```
即：**单条等值连接条件**，且是 **INNER 或 RIGHT_SEMI**。不符合直接返回（走普通哈希 join）。

### 步骤 B：取出连接条件两侧的原始列绑定
- `cond.left` = 探测侧（probe / LHS，事实表侧）；
- `cond.right` = 构建侧（build / RHS，维度表侧）；
- 通过 `GetPlainColumnBinding` 取出 `BoundColumnRefExpression`。若任一侧不是「纯列引用」（例如是 cast、计算表达式），跳过——安全回退到普通哈希 join。

### 步骤 C：向下追溯到源 `LogicalGet`
```cpp
auto probe_trace = TraceBindingToGet(*op.children[0], *probe_binding);
auto build_trace = TraceBindingToGet(*op.children[1], *build_binding);
```
沿着 Filter / Projection / 中间 INNER JOIN 链，把两侧列一路反查到最底层的 `LogicalGet`。若任一侧查不到（被别名子查询、计算表达式或其他不支持的算子挡住），回退普通哈希 join。

### 步骤 D：解析逻辑表名与列名
```cpp
string build_table = ResolveLogicalTableName(*build_trace.get);
string build_col   = ResolveColumnName(*build_trace.get, build_trace.get_column_index);
// 对称地解析 probe_table / probe_col
```
- 真实表：用 `get.GetTable()->name`；
- 读 parquet 风格的表函数：回退到第一个文件路径的 **基名（无目录、无扩展名）**，以匹配 `add_bitmap_columns.py` 写 `bitmap_join_meta.json` 时的命名约定；
- 已知限制：不考虑别名（别名在 binder 阶段之后就不存在了）。

任一为空则跳过。

### 步骤 E：查元数据注册表
```cpp
BitmapJoinResolved resolved;
if (!BitmapJoinMetaRegistry::Get(context).TryResolve(build_table, build_col,
                                                     probe_table, probe_col, resolved)) {
    return;
}
```
若注册表里没有这条 PK↔FK 预绑定，回退普通哈希 join。

### 步骤 F：构建侧必须是 PK（维度）侧
```cpp
if (!resolved.build_is_pk_side) {
    return;   // FK 事实表落在 build 侧 -> BHJ 核心前提（build 侧键唯一）被违反
}
```
BHJ 要求构建侧（维度表）主键唯一。若 FK 表错误地落在了 build 侧，直接回退。

### 步骤 G：非稠密 rowid 时注入/传播隐藏列（条目 3）
```cpp
if (resolved.pk->rowid_column != resolved.pk->pk_column) {
    if (!resolved.fk || resolved.fk->ref_column.empty()) return;
    if (!build_trace.path_complete || !probe_trace.path_complete) return;
    op.bhj_build_rowid_ref = PropagateHiddenColumn(*build_trace.get, build_trace.path, resolved.pk->rowid_column);
    op.bhj_probe_ref_ref   = PropagateHiddenColumn(*probe_trace.get, probe_trace.path, resolved.fk->ref_column);
}
```
- `rowid_column != pk_column` 是 **唯一开关**：决定两侧是否都要改读「物化的隐藏列」而非连接键原始值；
- 两侧必须处于同一模式：若 FK 绑定没有 `ref_column` 可对应，则无法一致计算探测侧 rowid，整体放弃（防止 rowid 方案不匹配）；
- 若追溯链路不完整（`path_complete == false`），也放弃隐藏列传播，回退普通哈希 join；
- 最后把解析结果挂到 `op.bhj_hint = make_uniq<BitmapJoinResolved>(resolved)` 供 `PlanComparisonJoin` 消费。

> 当 `rowid_column == pk_column`（PK 列本身即稠密 rowid，常见快路径）时，`bhj_build_rowid_ref` / `bhj_probe_ref_ref` 保持为空，仅挂 `bhj_hint`。

---

## 5. 向下追溯：`TraceBindingToGet`

```cpp
TracedBinding TraceBindingToGet(LogicalOperator &op, ColumnBinding binding);
```

从 `op` 出发，沿 `binding` 向下走，沿途可「解开」一列 Filter / Projection /（仅 INNER）比较 JOIN，直到找到产生 `binding` 的 `LogicalGet`。

### 各分支行为

| 算子类型 | 行为 |
|---|---|
| `LOGICAL_GET` | 比对 `table_index`，命中则返回 `get` + `get_column_index`。`table_index` 不符返回空。 |
| `LOGICAL_PROJECTION` | 若 `proj.table_index == binding.table_index`：看该投影列是否仍是「纯列引用」（否则视为计算表达式，超出范围，安全放弃 BHJ）。递归子节点，命中后把该 Projection 压入 `path` 头部。 |
| `LOGICAL_FILTER` | Filter 无自身 `table_index`，其 `projection_map` 只做「选哪些子列、按什么顺序」，不重写 `ColumnBinding` 值本身。因此 `binding` 原值对子节点同样有效，直接递归、把 Filter 压入 `path`。 |
| `LOGICAL_COMPARISON_JOIN` | 见「条目 4」详细分析。 |
| 其它（Aggregate / ASOF / DELIM 等） | 通用回退：在孩子们里 DFS，找到 `get` 但 `path_complete = false`（这些算子不在支持的 Filter/Projection/INNER-Join 链范围内）。 |

### 条目 4：中间比较 JOIN 的安全处理（难点）

`LogicalJoin::GetColumnBindings()` = `MapBindings(left, left_projection_map)` `++ MapBindings(right, right_projection_map)`，是「值的选取/重排」，并不改写 `ColumnBinding` 的值。

- 只有 **INNER** join 保证「左侧自己的绑定 + 右侧自己的绑定，不丢不加」的简单布局（这也正是 `RemoveUnusedColumns` 只对 INNER join 改列引用的原因）。
- 即便对 INNER join，**向左（LEFT）侧扩张也不安全**：join 输出布局固定为 `[left 绑定][right 绑定]`。更上层某个祖先可能已用「绝对位置」引用了本 join 的 RIGHT 侧（由更早的 `RemoveUnusedColumns` 计算好的 `left/right_projection_map`），若在 LEFT 侧插入新列，会把所有后续位置整体右移，静默破坏那些无关的祖先引用（开发期 TPCH Q5 的 `orders JOIN lineitem` 链曾因此真实崩溃）。
- **向右（RIGHT）侧扩张始终安全**：本 join 输出布局中 RIGHT 之后没有任何内容跟随。
- 因此：
  - LEFT 侧匹配：仍找到底层 `get`（供条目 1/2 目录名反查），但 `path_complete = false`，永不尝试隐藏列传播；
  - RIGHT 侧匹配：INNER 则把该 join 以 `PropagationSide::RIGHT` 压入 `path`，否则 `path_complete = false`。

---

## 6. 向上传播：`PropagateHiddenColumn`（条目 3）

```cpp
unique_ptr<Expression> PropagateHiddenColumn(LogicalGet &get,
                                             const vector<PathStep> &path,
                                             const string &column_name);
```

把名为 `column_name` 的隐藏物理列 **注入（或复用）** 进 `get` 的扫描输出，再沿 `path`（top-down 顺序）**从尾到头** 向上「泵」过每级 Filter / Projection / 中间 INNER JOIN，最终返回一个从 `path` 顶端（即消费 join 这一侧的孩子处）可见的 `BoundColumnRefExpression`。`ColumnBindingResolver` 之后会把它拍平为带正确物理 chunk 偏移的 `BoundReferenceExpression`。

执行步骤：

1. **注入 Get 侧列**：`EnsureGetColumn` 确保列被扫描并可见，得到 `column_index`、类型，构造初始 `binding(get.table_index, column_index)`；`child_binding_pos = GetOutputPosition(get, column_index)`。
2. **沿 path 倒序遍历**：
   - `LOGICAL_PROJECTION`：`proj.expressions.push_back(...)` 追加引用该列的 `BoundColumnRefExpression`，`binding` 更新为新位置；
   - `LOGICAL_FILTER`：若有 `projection_map`，用 `FindOrAppend` 把 `child_binding_pos` 加进去（无 map 则透传）；
   - `LOGICAL_COMPARISON_JOIN`：必定是 RIGHT 侧（由 `D_ASSERT` 保证）。若有 `right_projection_map` 则 `FindOrAppend`；否则右侧是恒等透传，直接把 `child_binding_pos` 加上 `left_len`（左宽）即可；`binding` 值本身不变（join 不改写 `table_index`）。
3. **修复 `.types` 缓存**：所有被改动的算子都缓存了 `.types`，直接改 list 不会自动同步它。`ColumnBindingResolver` 通用路径会读它，size 不匹配会静默破坏其它列引用。因此调用 `root.ResolveOperatorTypes()` 自底向上一次性重算（只调用 `path.front()`，因为它已递归覆盖 `get`）。

---

## 7. 辅助函数详解

### `GetPlainColumnBinding(expr)`
- 若 `expr` 仍是 `BOUND_COLUMN_REF`，返回其 `binding`；否则返回 `nullptr`（计算表达式场景）。

### `ResolveLogicalTableName(get)`
- 真实表 → `table->name`；
- 否则 `dynamic_cast<MultiFileBindData*>` 取首个文件路径基名；
- 都没有 → 返回空串。

### `ResolveColumnName(get, column_index)`
- `column_index` 是 `GetColumnIds()` 中的位置（**不是底层物理列号**）；
- 越界返回空串，否则 `get.GetColumnName(column_ids[column_index])`。

### `EnsureGetColumn(get, column_name)`
- 在 `get.names` 中找到物理列 id（`physical_id`）；找不到抛 `InternalException`；
- 在 `column_ids` 中找是否已被扫描（idempotency，避免重复注入）；没有则 `AddColumnId`；
- **可见性修复**：普通表扫描 `function.filter_prune == true` 时，`RemoveUnusedColumns` 还会用 `projection_ids` 额外门控「可见输出」。`AddColumnId` 只保证扫描、不保证可见，因此还需把位置推进 `projection_ids`（必要时补 `types`）；若 `projection_ids` 为空，则 `types` 随 `column_ids` 1:1 增长，直接补 `types`。

### `GetOutputPosition(get, column_ids_pos)`
- `projection_ids` 为空 → 直接返回 `column_ids_pos`；
- 否则返回它在 `projection_ids` 中的 **位置**（因为 `GetColumnBindings` 用 `projection_ids` 的「值」作为 `ColumnBinding::column_index`，而 `projection_map` 用的是「位置」）。

### `FindOrAppend(map, value)`
- 在 `projection_map` / `*_projection_map` 中找 `value`，没有则 `push_back`；
- **去重关键**：对 `SELECT *` 类查询，用户可见列可能恰好已在需要的绝对位置，盲目 `push_back` 会复制绑定、破坏后续所有基于位置的列解析。逻辑与 `EnsureGetColumn` 的去重对称。

---

## 8. 数据流与正确性要点总结

1. **纯标注、零表达式重写**：保持 visitor 的 `VisitReplace` 默认 no-op，不影响其它 optimizer。
2. **保守回退**：任何不支持/不确定安全的情形（条件侧非纯列、追溯失败、PK 不在 build 侧、rowid 方案不一致、链路不完整）一律回退普通哈希 join，绝不强撑 BHJ。
3. **「绝对位置」不变量**：所有对中间 INNER JOIN 的扩张只走 RIGHT 侧，避免破坏祖先基于绝对位置的 `projection_map` 引用。
4. **类型缓存一致性**：改动 `expressions` / `projection_map` 后必须 `ResolveOperatorTypes()` 重建 `.types`，否则 `ColumnBindingResolver` 会读错 size。
5. **幂等注入**：`EnsureGetColumn` / `FindOrAppend` 都做了「已存在则复用」的去重，保证同一计划多次经过 resolver 也不会重复读列或复制绑定。
6. **构建侧唯一性前提**：BHJ 依赖构建侧（维度表）键唯一，`build_is_pk_side` 必须为 true。

---

## 9. 与其它模块的关系

| 模块 | 关系 |
|---|---|
| `BitmapJoinMetaRegistry` | 进程级单例，离线由 `add_bitmap_columns.py` 生成的 `bitmap_join_meta.json` 经 parquet 扩展加载钩子注入，或测试里 `RegisterPK/RegisterFK` 注入。`TryResolve` 提供反向匹配。 |
| `LogicalComparisonJoin` | 承载 `bhj_hint` / `bhj_build_rowid_ref` / `bhj_probe_ref_ref` 三个字段，供 `PlanComparisonJoin` 消费。 |
| `ColumnBindingResolver` | 紧随其后运行，把本 pass 注入的 `BoundColumnRefExpression` 拍平成 `BoundReferenceExpression` 并定物理偏移。 |
| `RemoveUnusedColumns` | 决定 join 输出布局 / `projection_map` 的绝对位置，是条目 4「只扩 RIGHT 侧」安全性的根本依据。 |
| `LateMaterialization::ConstructRHS` | `PropagateHiddenColumn` 的向上传播逻辑在思路上与之镜像。 |

---

## 10. 已知限制（Known Limitations）

- 别名（alias）不参与表名解析（别名在 binder 之后消失）；
- 连接条件侧必须是纯列引用（cast/计算表达式即放弃 BHJ）；
- 只支持 INNER / RIGHT_SEMI 单等值连接；
- 隐藏列传播链只支持 Filter / Projection / 中间 INNER JOIN 三种算子，遇到 Aggregate、ASOF/DELIM join 等一律回退；
- `path_complete == false` 时即便能反查出目录名，也不会尝试隐藏列传播。
