# tpche2e vs tpche2e_origin 对比

两个目录都是 TPC-H 22 查询端到端 (e2e) 性能测试，均通过 C++ harness (`tpch_bench_harness`) 链接 `libduckdb.so` 执行。核心区别是：**tpche2e_origin 是两组对比（官方 vs 项目+bitmap），tpche2e 扩展为三组对比（official_baseline / project_baseline / project_bitmap）**。

## 文件差异总览

| 文件 | tpche2e_origin | tpche2e |
|---|---|---|
| `build_harness.sh` | `SRC_DIR` 指向 `tpche2e_origin` | `SRC_DIR` 指向 `tpche2e` |
| `tpch_bench_harness.cpp` | 单进程/单 DB 实例跑完所有查询 | 每条查询独立 DB 实例；新增 `--set-bitmap-off` |
| `tpche2e_bench_sdk.py` | 两组：official / bitmap | 三组：official_baseline / project_baseline / project_bitmap |
| `tpche2e_bench_sdk_bitmap_only.py` | 只测 bitmap，official 读固定 CSV | 只测 project_bitmap，两个 baseline 读固定 CSV |
| `tpch_official.csv` | 无 | 新增（固定 baseline 数据） |
| `cast_file/` | 无 | 新增（旧版脚本存档：`tpche2e_bench.py`、`tpche2e_bench_bitmap_only.py`） |
| `tpch_queries*.json` | 相同 | 相同 |

## 详细区别

### 1. 测试组别：两组 → 三组

- **origin（2 组）**：
  - A) 官方 DuckDB 1.5.1 `libduckdb.so`（baseline，不开 bitmap）
  - B) 本项目 `libduckdb.so` + `bitmap_join_load` + `open_bitmap_join=true`
- **新版（3 组）**：
  - A) `official_baseline`：官方 1.5.1（不设置 `open_bitmap_join`，因官方 lib 无此配置项）
  - B) `project_baseline`：本项目 `libduckdb.so` + `open_bitmap_join=false`
  - C) `project_bitmap`：本项目 `libduckdb.so` + `open_bitmap_join=true`

新增 B 组的目的：把「项目自身相对官方的改动」与「bitmap join 带来的收益」解耦，可以单独衡量 bitmap 的净收益。

### 2. harness 执行模型

| | origin | 新版 |
|---|---|---|
| DB 实例 | 全程一个进程 / 一个 DB / 一个 connection | 每条查询循环内新建独立 DB 实例，跑完析构释放 buffer pool |
| 新增 CLI 参数 | 无 | `--set-bitmap-off`（项目 baseline 模式） |

新版设计意图：消除跨查询缓存污染与内存淘汰对单条查询计时的干扰（OS page cache 仍共享），同时保留单进程加载一次 `.so` 的优势。warmup 在新模型下是在该查询自己的 buffer pool 内预热。

### 3. Python 驱动脚本

- `run_harness()` 新增 `set_bitmap_off` 参数，对应新增的配置 B。
- CLI 选项变化：
  - origin：`--official-only` / `--bitmap-only`
  - 新版：`--official-only` / **`--project-baseline-only`** / `--bitmap-only`（互斥校验，只能指定一个）
- 新增对 `PROJECT_LIB` 存在性的前置检查。
- CSV 字段重命名：
  - `official_*` → `official_baseline_*`
  - 新增 `project_baseline_*` / `project_bitmap_*`
  - `speedup_official_over_bitmap` → 两组加速比列（vs official_baseline、vs project_baseline）
- 汇总日志输出两组整体加速比（bitmap vs 两个 baseline 各自的整体加速）。
- 画图：耗时柱状图由两组变三组；加速比图由单组变双组柱状（`tpche2e_speedup.png`）。

### 4. bitmap_only 脚本

- origin：只重测 bitmap，official 数据固定读 `tpch_official.csv`。
- 新版：只重测 project_bitmap，`official_baseline` 和 `project_baseline` 均从 CSV 读取；CSV 解析兼容旧列名（`official_*` 作为 legacy prefix）。

### 5. 新增文件

- `tpch_official.csv`：origin 版脚本全量跑出的固定 baseline 数据，供 bitmap_only 脚本复用。
- `cast_file/`：旧版 subprocess 计时脚本的存档（`tpche2e_bench.py`、`tpche2e_bench_bitmap_only.py`），仅作历史保留。

## 结论

tpche2e 是 tpche2e_origin 的演进版：在保留「C++ harness 单进程精确计时」核心思想的基础上，
1. 增加 `project_baseline` 对照组，使 bitmap join 的净收益可被单独度量；
2. 改为每查询独立 DB 实例，减少缓存/内存淘汰对计时的干扰；
3. 输出三组 CSV/图表与双加速比汇总，结果更完整。
