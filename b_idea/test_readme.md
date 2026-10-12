# BHJ（Bitmap-Join）测试复现指南

本文档给出从零开始（新机器/新 clone）复现本仓库全部 BHJ 相关测试所需的完整命令序列：编译 DuckDB → 生成 TPC-H SF=5 数据集 → 离线预处理（生成 `_rowid`/`*_ref` 列 + `bitmap_join_meta.json`）→ 编译测试 → 运行测试。

> 本文档只覆盖 BHJ（Bitmap Hash Join）相关测试；不涉及项目里其它无关的测试套件。

---

## 0. 环境要求

- Linux x86_64（本仓库开发/验证环境）
- C++ 编译器（gcc/g++，支持 C++17）、CMake ≥ 3.x、GNU Make
- Python ≥ 3.8，且已安装 `duckdb` Python 包（仅用于离线预处理脚本，与 C++ 引擎版本无关）：
  ```bash
  python3 -m pip install duckdb
  ```
- 磁盘空间：TPC-H SF=5 原始 parquet 约 1.6GB，追加 `_rowid`/`*_ref` 列后的 bitmap 版本约 2.0GB，两者都需要保留，共约 **4GB**。
- 编译耗时提示：本仓库默认开启 `ENABLE_SANITIZER`/`ENABLE_UBSAN`（见下文），Debug 构建（`make unittest` 走的 `make debug`）会比 Release 明显慢，首次全量编译在多核机器上大约十几分钟到数十分钟不等。

---

## 1. 编译 DuckDB（含 tpch/tpcds 扩展 + 单测二进制）

### 1.1 确认扩展配置

本仓库通过 `extension/extension_config_local.cmake`（已提交到 git，无需手动创建）默认加载 `tpch`、`tpcds` 扩展：

```cmake
duckdb_extension_load(tpch)
duckdb_extension_load(tpcds)
```

`extension/extension_config.cmake`（基础配置，随 DuckDB 一起提交）额外默认加载 `core_functions`、`parquet`（以及 x86_64 Linux 上的 `jemalloc`）。这两个文件会被 CMake 自动读取，**不需要额外传参**。

`test/api/test_bitmap_join_tpch_22queries.cpp`（22 条标准 TPC-H SQL 的 e2e smoke test）依赖 `tpch` 扩展被链接进 `unittest` 二进制，由 CMake 变量 `DUCKDB_EXTENSION_TPCH_SHOULD_LINK` 控制——只要 `extension_config_local.cmake` 里加载了 `tpch`，这个变量就会被自动置真，无需手动干预。

### 1.2 编译

本仓库大多数 BHJ 测试是通过 `[bitmap_join]`/`[bitmap_join_tpch]` 等 tag 显式调用 `build/debug/test/unittest` 或 `build/release/test/unittest` 跑的，Debug 和 Release 都可以，二者选一即可（Release 明显更快，跑 SF5 数据集相关的慢测试时建议用 Release）：

**方式 A：Release（推荐，跑 SF5 数据集测试更快）**

```bash
cd /home/featurize/workspace/duckdb-cuda
make release -j"$(nproc)"
CMAKE_BUILD_PARALLEL_LEVEL=96 make release
```

产出：
- CLI 二进制：`build/release/duckdb`
- 单测二进制：`build/release/test/unittest`

**方式 B：Debug（`make unittest`/`make unit` 走的默认路径）**

```bash
cd /home/featurize/workspace/duckdb-cuda
make unittest -j"$(nproc)"   # 等价于 make debug 然后自动跑一次全量测试
```

若只想编译不立即跑全量测试，直接：

```bash
make debug -j"$(nproc)"
```

产出：`build/debug/test/unittest`。

> 后文命令统一以 **Release** 路径（`build/release/test/unittest`）为例；如果用 Debug，把路径换成 `build/debug/test/unittest` 即可，用法完全一致。

### 1.3 验证编译产物

```bash
./build/release/duckdb --version
./build/release/test/unittest --list-tags 2>&1 | grep -i bitmap
```

预期能看到 `[bitmap_join]`、`[bitmap_join_tpch]`、`[bitmap_join_tpch_profile]` 等 tag。

---

## 2. 生成 TPC-H SF=5 数据集（原始版本）

用刚编译好的 `duckdb` CLI（必须是已加载 `tpch` 扩展的这份二进制）跑数据生成 SQL：

```bash
cd /home/featurize/workspace/duckdb-cuda
mkdir -p data/tpch_sf5
./build/release/duckdb <<'EOF'
LOAD tpch;
CALL dbgen(sf=5);
EXPORT DATABASE '/home/featurize/workspace/duckdb-cuda/data/tpch_sf5/' (FORMAT PARQUET);
EOF
```

完成后 `data/tpch_sf5/` 下应有 8 张表的 parquet 文件：`customer.parquet`、`orders.parquet`、`lineitem.parquet`、`part.parquet`、`partsupp.parquet`、`supplier.parquet`、`nation.parquet`、`region.parquet`（以及 `schema.sql`/`load.sql`）。

> 如果同时需要 TPC-DS SF=5 数据（本仓库其它测试可能用到，BHJ 测试不需要），可以参考 `a_idea/任务拆分/gen_data_sf5.sql` 里额外的 `CALL dsdgen(sf=5)` 部分，一次性生成两套数据。BHJ 测试**只依赖 TPC-H**，可以跳过 TPC-DS 部分。

---

## 3. 离线预处理：生成 `_rowid`/`*_ref` 隐藏列 + `bitmap_join_meta.json`

用 `b_idea/scripts/add_bitmap_columns.py` 把上一步生成的原始 parquet，转换成 BHJ 需要的"追加隐藏列"版本，输出到独立目录 `data/tpch_sf5_bitmap/`（不修改/覆盖原始数据）：

```bash
cd /home/featurize/workspace/duckdb-cuda
python3 b_idea/scripts/add_bitmap_columns.py \
    --input       data/tpch_sf5/ \
    --output      data/tpch_sf5_bitmap/ \
    --constraints b_idea/scripts/tpch_constraints.csv \
    --meta-out    data/tpch_sf5_bitmap/bitmap_join_meta.json \
    -v
```

完成后 `data/tpch_sf5_bitmap/` 下应有：
- 8 张表的 parquet（与原表同 schema，仅追加 `_rowid`/`*_ref` 列）
- `bitmap_join_meta.json`（记录 PK/FK 绑定关系，供 DuckDB 运行期自动加载）

### 3.1（可选）不依赖真实数据的脚本自检

```bash
python3 b_idea/scripts/test_add_bitmap_columns.py
```

预期输出末尾为 `[ALL PASS]`。这个自检只验证脚本逻辑本身（稠密 PK / 字符串 PK / 幂等性），不需要 TPC-H 数据，可用于快速确认 Python 环境是否正确，无需等生成大数据集。

---

## 4. 运行 BHJ 测试

以下全部基于 `build/release/test/unittest`（Debug 换成 `build/debug/test/unittest`）。

### 4.1 不依赖外部数据集的测试（合成小数据，运行快）

```bash
cd /home/featurize/workspace/duckdb-cuda
./build/release/test/unittest "[bitmap_join]"
```

覆盖：`test_bitmap_join_auto_resolve.cpp`（自动识别正/负例）、`test_bitmap_join_chain.cpp`（隐藏列跨中间 join 传播、RIGHT_SEMI 安全回退回归）、`test_bitmap_join_rowid_ref.cpp`（filter/projection 场景下隐藏列的保留）。这些测试**不需要**上面第 2/3 步生成的 SF=5 数据集。

### 4.2 依赖 SF=5 数据集的测试

以下测试都依赖第 2/3 步生成的 `data/tpch_sf5/` 和 `data/tpch_sf5_bitmap/`；数据集不存在时会打印一行提示并静默跳过（不会 FAIL），因此**必须先完成第 2/3 步**才能真正跑到断言。这些测试都打了 Catch2 的隐藏 tag `[.]`（多 GB 外部数据集 + 相对慢），需要显式指定测试名才会被跑，不在日常 `./unittest "*"` 全量跑批里。

```bash
# Q5/Q9/Q10 端到端正确性 + 命中率/耗时软性打印 + 原始数据交叉验证，可指定具体query查询
./build/release/test/unittest "[bitmap_join_tpch]"
BHJ_QUERIES=1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22 ./build/release/test/unittest "[bitmap_join_tpch]"

BHJ_QUERIES=1 ./build/release/test/unittest "[bitmap_join_tpch]"


# 算子级(HASH_JOIN)耗时画像(baseline/perfect/bitmap三模式对比) + 22条标准TPCH SQL全量smoke test
./build/release/test/unittest "[bitmap_join_tpch_profile]"
BHJ_QUERIES=1 ./build/release/test/unittest "[bitmap_join_tpch_profile]"

# 单条等值连接场景下的 BHJ vs PerfectHashJoin vs 普通HashJoin 性能对比
./build/release/test/unittest "[bitmap_join][.]"
```

> 注：`[bitmap_join_tpch_profile]` 这个 tag 同时覆盖 `test_bitmap_join_tpch_profile.cpp`（三模式算子耗时）和 `test_bitmap_join_tpch_22queries.cpp`（22条SQL），后者额外要求 `tpch` 扩展已链接进当前 `unittest` 二进制（见 §1.1，默认满足）。

### 4.3 一次性跑全部 BHJ 测试（含隐藏 tag）

```bash
./build/release/test/unittest "[bitmap_join],[bitmap_join_tpch],[bitmap_join_tpch_profile]"
```

或者按名字模糊匹配（不依赖 tag，效果等价）：

```bash
./build/release/test/unittest "*bitmap*"
```

---

### 4.4 实验测试

```bash
bash b_idea/perf/tpche2e/build_harness.sh
python3 b_idea/perf/tpche2e/tpche2e_bench_sdk.py
```
---

## 5. 常见问题

**Q: 测试提示 `dataset directory '.../data/tpch_sf5_bitmap' not found/incomplete - skipping test`？**
A: 说明第 2/3 步没有做，或者目标目录下 8 张表没有齐（脚本会检查 `region/nation/customer/orders/part/partsupp/supplier/lineitem` 是否全部存在）。重新执行第 2、3 步即可。

**Q: `test_bitmap_join_tpch_22queries.cpp` 没有被编译进 `unittest`，`--list-tags` 里看不到 `bitmap_join_tpch_profile`？**
A: 检查 `extension/extension_config_local.cmake` 是否存在且包含 `duckdb_extension_load(tpch)`（该文件已提交到 git，clone 后应自带；如被误删可参考 §1.1 内容手动重建），然后重新执行 `cmake .`（在 `build/release` 目录下）或直接重新 `make release`。

**Q: 想确认某次改动是否引入回归，除了 BHJ 自身还要跑哪些？**
A: 建议至少加跑一次 `optimizer` 和 `join` 相关的通用回归，排除对上游逻辑的影响：
```bash
./build/release/test/unittest "test/sql/optimizer/*"
./build/release/test/unittest "[join]"
```

**Q: 数据生成/预处理一步都不想等，只想先验证代码能编译、逻辑没写错？**
A: 只做第 1 步 + `./build/release/test/unittest "[bitmap_join]"`（§4.1）即可，几秒内跑完，不需要任何外部数据集。
