# b_idea/scripts —— BHJ（Bitmap HashJoin）配套脚本

本目录是详细设计 [§6.1 / 附录 B](../详细设计.md) 的实施落地。

## 文件清单（M1 阶段）

| 文件 | 模块（详设引用） | 说明 |
| ---- | ---------------- | ---- |
| `tpch_constraints.csv` | §B.1 | TPC-H 主外键约束清单（脚本输入） |
| `add_bitmap_columns.py` | §6.1 模块 M-S | 离线给 parquet 追加 `_rowid` / `*_ref` 列，输出 `bitmap_join_meta.json` |
| `test_add_bitmap_columns.py` | §14（自检） | 不依赖 TPC-H 的最小自检：稠密 PK / 字符串 PK / 幂等性 |

后续里程碑会陆续加入：
- `inject_bitmap_plan.py`（§6.5，M3）
- `run_bitmap_baseline.py`（§6.7 / §12，M0/M4）

## 环境要求

- Python ≥ 3.7
- duckdb python 包（与本仓库代码版本一致）：`pip install duckdb`

## 用法（M1 阶段）

### 0) 先准备 TPC-H parquet 数据

参考 `benchmark/tpch/tpch_load_parquet.benchmark.in`：

```sql
CALL dbgen(sf=10);
EXPORT DATABASE 'data/tpch_sf10/' (FORMAT PARQUET);
```

执行后 `data/tpch_sf10/` 下应有 `customer.parquet`、`orders.parquet` 等 8 张表。

### 1) 离线预处理

```bash
python b_idea/scripts/add_bitmap_columns.py \
    --input  data/tpch_sf10/ \
    --output data/tpch_sf10_bitmap/ \
    --constraints b_idea/scripts/tpch_constraints.csv \
    --meta-out   data/tpch_sf10_bitmap/bitmap_join_meta.json \
    -v
```

可选参数：
- `--force`：即使目标 parquet 已含 rowid/ref 列也重新生成。
- `--dry-run`：只打印计划，不写文件。
- `--threads N`：传给 DuckDB 的并发度。

输出：
- `data/tpch_sf10_bitmap/{customer,orders,...}.parquet`
  与原 parquet 同 schema，**仅追加** `_rowid` / `*_ref` 列。
- `data/tpch_sf10_bitmap/bitmap_join_meta.json`：
  ```json
  {
    "version": 1,
    "rowid_column_default": "_rowid",
    "ref_column_suffix": "_ref",
    "pk_bindings": [
      {"pk_table": "customer", "pk_column": "c_custkey",
       "rowid_column": "c_custkey", "rowid_offset": 1, "row_count": 1500000},
      ...
    ],
    "fk_bindings": [
      {"fk_table": "orders", "fk_column": "o_custkey",
       "ref_column": "o_custkey_ref",
       "pk_table": "customer", "pk_column": "c_custkey"},
      ...
    ]
  }
  ```

### 2) 自检（无需 TPC-H 数据）

```bash
python b_idea/scripts/test_add_bitmap_columns.py
```

预期输出末尾为 `[ALL PASS]`。

## TPC-H 适配要点（详设 §B.3）

- TPC-H 的整数主键大多为 1 起稠密整数（如 `c_custkey`、`o_orderkey`），脚本会自动检测，**不新增列**，仅在 meta 中记录 `rowid_offset = 1`。
  - 加速：FK 表的 ref 列由纯算术 `fk - 1` 生成，不走 join；
  - 节省：主表存储不变。
- `partsupp` 是复合 PK，本期**不**作为 PK 注册，但作为 FK 表会同时绑定 `part` 与 `supplier`。
- `lineitem` 同理，仅作为 FK 表存在。

## 与 DuckDB 引擎侧（§6.2 / §6.3）的对接

启动 DuckDB 时，`extension/parquet` 的 load hook 会调用：

```cpp
BitmapJoinMetaRegistry::Get(ctx).LoadFromJson(
    "data/tpch_sf10_bitmap/bitmap_join_meta.json");
```

之后用户即可：

```sql
PRAGMA enable_bitmap_join = true;
PRAGMA bitmap_join_meta;          -- 查看已加载的绑定
.read benchmark/tpch/queries/q05.sql
```

引擎侧（§6.2 / §6.3 / §6.4）的实现将在后续里程碑提交。
