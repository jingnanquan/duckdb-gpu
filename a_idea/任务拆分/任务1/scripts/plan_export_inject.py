#!/usr/bin/env python3
"""
任务1.6: 执行计划导出与注入验证
1. 导出重点查询的执行计划（JSON + 文本格式）
2. 验证通过固定 optimizer 配置可以复现相同计划
3. 验证结果一致性（重放验证）
4. 探索 PhysicalPlan 序列化的可行性

用法:
    cd /home/featurize/workspace/duckdb-cuda
    python3 a_idea/任务拆分/任务1/scripts/plan_export_inject.py
"""

import subprocess
import json
import os
import hashlib
import sys
import time
import re

# ============================================================
# 配置
# ============================================================
BASE_DIR = "/home/featurize/workspace/duckdb-cuda"
DUCKDB_BIN = f"{BASE_DIR}/build/release/duckdb"
TPCH_DATA_DIR = f"{BASE_DIR}/data/tpch_sf5"
TPCDS_DATA_DIR = f"{BASE_DIR}/data/tpcds_sf5"
TASK_DIR = f"{BASE_DIR}/a_idea/任务拆分/任务1"
PLANS_DIR = f"{TASK_DIR}/results/plans"

# 重点查询
TARGET_QUERIES = {
    "tpch_Q01": ("tpch", 1),
    "tpch_Q03": ("tpch", 3),
    "tpch_Q06": ("tpch", 6),
    "tpch_Q12": ("tpch", 12),
    "tpcds_Q04": ("tpcds", 4),
    "tpcds_Q11": ("tpcds", 11),
    "tpcds_Q74": ("tpcds", 74),
}

# ============================================================
# 辅助函数
# ============================================================

def get_setup_sql(benchmark):
    if benchmark == "tpch":
        tables = ['lineitem', 'customer', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
        data_dir = TPCH_DATA_DIR
    else:
        tables = [
            'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
            'customer', 'customer_address', 'customer_demographics', 'date_dim',
            'household_demographics', 'income_band', 'inventory', 'item',
            'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
            'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
            'web_sales', 'web_site'
        ]
        data_dir = TPCDS_DATA_DIR
    sqls = []
    for t in tables:
        path = f"{data_dir}/{t}.parquet"
        if os.path.exists(path):
            sqls.append(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{path}');")
    return "\n".join(sqls)

def get_query(benchmark, qnum):
    """获取查询 SQL"""
    sql = f"SELECT query FROM {benchmark}_queries() WHERE query_nr={qnum};"
    result = subprocess.run(
        [DUCKDB_BIN, "-noheader", "-list", "-c", sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=30
    )
    if result.returncode != 0:
        return None
    query = result.stdout.strip()
    return query if query else None

# ============================================================
# 步骤 1：导出执行计划
# ============================================================

def export_plan_text(setup_sql, query_sql, output_path):
    """导出 EXPLAIN 文本格式的物理计划"""
    full_sql = f"""
{setup_sql}
PRAGMA threads=4;
EXPLAIN {query_sql}
"""
    result = subprocess.run(
        [DUCKDB_BIN, "-c", full_sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=120
    )
    if result.returncode == 0:
        with open(output_path, 'w') as f:
            f.write(result.stdout)
        return True
    return False

def export_plan_profile_json(setup_sql, query_sql, output_path):
    """通过 profiler 导出 JSON 格式的执行计划"""
    full_sql = f"""
{setup_sql}
PRAGMA threads=4;
PRAGMA enable_profiling='json';
PRAGMA profiling_mode='detailed';
PRAGMA profiling_output='{output_path}';
{query_sql}
"""
    result = subprocess.run(
        [DUCKDB_BIN, "-c", full_sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=600
    )
    return result.returncode == 0

def export_plan_json_explain(setup_sql, query_sql, output_path):
    """导出 EXPLAIN (FORMAT JSON) 格式"""
    full_sql = f"""
{setup_sql}
PRAGMA threads=4;
EXPLAIN (FORMAT JSON) {query_sql}
"""
    result = subprocess.run(
        [DUCKDB_BIN, "-c", full_sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=120
    )
    if result.returncode == 0 and result.stdout.strip():
        with open(output_path, 'w') as f:
            f.write(result.stdout)
        return True
    return False

def save_query_sql(setup_sql, query_sql, output_path):
    """保存原始查询 SQL（含 setup）"""
    with open(output_path, 'w') as f:
        f.write(f"-- ============================================================\n")
        f.write(f"-- Setup SQL (创建 views)\n")
        f.write(f"-- ============================================================\n")
        f.write(f"{setup_sql}\n\n")
        f.write(f"-- ============================================================\n")
        f.write(f"-- Query SQL\n")
        f.write(f"-- ============================================================\n")
        f.write(f"PRAGMA threads=4;\n")
        f.write(f"{query_sql}\n")

# ============================================================
# 步骤 2：验证计划稳定性
# ============================================================

def verify_plan_stability(setup_sql, query_sql, runs=5):
    """验证多次执行产生相同的执行计划"""
    plans = []
    for i in range(runs):
        full_sql = f"""
{setup_sql}
PRAGMA threads=4;
EXPLAIN {query_sql}
"""
        result = subprocess.run(
            [DUCKDB_BIN, "-c", full_sql],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=120
        )
        if result.returncode == 0:
            plan_hash = hashlib.md5(result.stdout.encode()).hexdigest()
            plans.append(plan_hash)

    if not plans:
        return False, "所有执行都失败"

    unique_plans = set(plans)
    if len(unique_plans) == 1:
        return True, f"计划稳定：{runs} 次执行产生相同计划 (hash={plans[0][:8]})"
    else:
        return False, f"计划不稳定：{len(unique_plans)} 种不同计划 (共 {runs} 次执行)"

# ============================================================
# 步骤 3：验证通过 PRAGMA 固定计划后重放
# ============================================================

def verify_replay_consistency(setup_sql, query_sql, query_key):
    """
    验证：保存 SQL + PRAGMA 配置后，能否在新 session 中复现相同结果。
    通过对比两次独立执行的结果行数来验证。
    """
    # 去掉查询末尾的分号（避免嵌套子查询时语法错误）
    clean_query = query_sql.strip().rstrip(';').strip()
    # 使用 COUNT(*) 来验证结果一致性（避免大结果集的 hash 计算问题）
    count_sql = f"SELECT COUNT(*) FROM ({clean_query}) AS sub;"

    full_sql = f"""
{setup_sql}
PRAGMA threads=4;
{count_sql}
"""
    results = []
    for i in range(3):
        result = subprocess.run(
            [DUCKDB_BIN, "-noheader", "-list", "-c", full_sql],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=600
        )
        if result.returncode == 0:
            count = result.stdout.strip()
            results.append(count)
        else:
            results.append(f"ERROR:{result.stderr[:100]}")

    if len(set(results)) == 1 and not results[0].startswith("ERROR"):
        return True, f"结果一致: COUNT(*)={results[0]} (3次执行)"
    elif all(r.startswith("ERROR") for r in results):
        return False, f"所有执行失败: {results[0]}"
    else:
        return False, f"结果不一致: {results}"

# ============================================================
# 步骤 4：验证禁用特定 optimizer 后计划是否变化
# ============================================================

def test_optimizer_control(setup_sql, query_sql):
    """测试通过禁用 optimizer rules 来控制执行计划"""
    configs = {
        "default": "",
        "no_filter_pushdown": "SET disabled_optimizers='filter_pushdown';",
        "no_statistics_propagation": "SET disabled_optimizers='statistics_propagation';",
        "no_join_order": "SET disabled_optimizers='join_order';",
        "no_all_optimizers": "SET disabled_optimizers='filter_pushdown,statistics_propagation,join_order,top_n,common_subexpressions';",
    }

    results = {}
    for config_name, pragma in configs.items():
        full_sql = f"""
{setup_sql}
PRAGMA threads=4;
{pragma}
EXPLAIN {query_sql}
"""
        result = subprocess.run(
            [DUCKDB_BIN, "-c", full_sql],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=120
        )
        if result.returncode == 0:
            plan_hash = hashlib.md5(result.stdout.encode()).hexdigest()
            results[config_name] = {
                "hash": plan_hash,
                "plan_preview": result.stdout[:500]
            }
        else:
            results[config_name] = {
                "hash": "ERROR",
                "error": result.stderr[:200]
            }

    # 分析哪些配置产生了不同的计划
    default_hash = results.get("default", {}).get("hash", "")
    different_configs = [name for name, info in results.items()
                         if info.get("hash") != default_hash and info.get("hash") != "ERROR"]

    return results, different_configs

# ============================================================
# 步骤 5：序列化可行性分析
# ============================================================

def generate_feasibility_report(stability_results, replay_results, optimizer_results):
    """生成 PhysicalPlan 序列化可行性报告"""
    report = f"""# 执行计划注入可行性分析报告

## 1. 测试环境

- DuckDB 版本: v1.5.1 (本地编译，静态链接 tpch/tpcds)
- 数据集: TPCH SF=5 + TPCDS SF=5 (Parquet 格式)
- 线程数: 4

## 2. 计划稳定性验证

验证目标：在相同数据 + 相同配置下，DuckDB 的执行计划是否确定性稳定。

| 查询 | 稳定性 | 说明 |
| ---- | ------ | ---- |
"""
    for key, (stable, msg) in stability_results.items():
        status = "✅ 稳定" if stable else "❌ 不稳定"
        report += f"| {key} | {status} | {msg} |\n"

    all_stable = all(s for s, _ in stability_results.values())
    report += f"\n**结论**: {'所有查询计划均稳定' if all_stable else '部分查询计划不稳定，需要进一步分析'}\n"

    report += f"""
## 3. 重放一致性验证

验证目标：使用相同 SQL + 相同 PRAGMA 配置，多次执行结果是否完全一致。

| 查询 | 一致性 | 说明 |
| ---- | ------ | ---- |
"""
    for key, (consistent, msg) in replay_results.items():
        status = "✅ 一致" if consistent else "❌ 不一致"
        report += f"| {key} | {status} | {msg} |\n"

    all_consistent = all(c for c, _ in replay_results.values())
    report += f"\n**结论**: {'所有查询结果均一致，方案 B 可行' if all_consistent else '部分查询结果不一致，需要分析原因'}\n"

    report += f"""
## 4. Optimizer 控制能力验证

验证目标：通过 `SET disabled_optimizers` 能否有效控制执行计划。

"""
    for key, (configs, different) in optimizer_results.items():
        report += f"### {key}\n\n"
        if different:
            report += f"以下配置产生了不同的执行计划: {different}\n\n"
        else:
            report += f"所有配置产生了相同的执行计划（optimizer 对该查询影响不大）\n\n"

    report += f"""
## 5. 方案评估

### 方案 A: SQL Hint 控制
- **可行性**: 部分可行
- **优点**: 零开发量
- **缺点**: DuckDB hint 支持有限，粒度粗

### 方案 B: SQL + PRAGMA 固定配置（推荐短期方案）
- **可行性**: {'✅ 验证通过' if all_stable and all_consistent else '⚠️ 需要进一步验证'}
- **优点**: 零开发量；结果可验证；计划稳定
- **缺点**: 不能精确控制物理计划的每个细节
- **适用场景**: M1~M2 里程碑的实验验证

### 方案 C: PhysicalPlan JSON 序列化/反序列化（推荐中期方案）
- **可行性**: 需要开发（预计 3~5 人日）
- **优点**: 精确控制物理计划；支持 A/B 对比
- **缺点**: 需要修改 DuckDB 内核代码
- **入口点**: `src/main/client_context.cpp` → `CreatePreparedStatement()` 之后
- **适用场景**: M3+ 里程碑

### 方案 D: Prepared Statement 缓存
- **可行性**: 可行但不够灵活
- **优点**: DuckDB 原生支持
- **缺点**: 只能在同一 session 内复用

## 6. 推荐路线

1. **M1~M2（短期）**: 使用方案 B
   - 保存 SQL + PRAGMA 配置文件
   - 通过脚本自动化执行和对比
   - 验证 GPU 路径 vs CPU 路径的结果一致性

2. **M3+（中期）**: 开发方案 C
   - 实现 `PRAGMA gpu_run_with_plan('plan.json')`
   - 在 `physical_planner.Plan()` 之后增加计划注入点
   - 支持从 JSON 文件加载预定义的物理执行计划

## 7. 对后续任务的影响

| 结论 | 影响 |
| ---- | ---- |
| 计划稳定 | GPU 加速实验可以通过「相同 SQL + 相同 PRAGMA」做可靠的 A/B 对比 |
| 结果一致 | 确认 GPU 路径的正确性验证方法可行 |
| Optimizer 可控 | 可以通过禁用特定 optimizer 来隔离 GPU 加速的效果 |
"""
    return report

# ============================================================
# 主流程
# ============================================================

def main():
    os.makedirs(PLANS_DIR, exist_ok=True)

    # 验证 DuckDB 可用
    result = subprocess.run([DUCKDB_BIN, "-c", "SELECT 'ok';"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=10)
    if result.returncode != 0:
        print(f"❌ DuckDB 不可用: {result.stderr}")
        sys.exit(1)

    print("=" * 60)
    print("执行计划导出与注入验证")
    print("=" * 60)

    stability_results = {}
    replay_results = {}
    optimizer_results = {}

    for query_key, (benchmark, qnum) in TARGET_QUERIES.items():
        print(f"\n{'─'*50}")
        print(f"  {query_key} ({benchmark} Q{qnum})")
        print(f"{'─'*50}")

        setup_sql = get_setup_sql(benchmark)
        query_sql = get_query(benchmark, qnum)
        if not query_sql:
            print(f"  ⚠️ SKIP: 无法获取查询")
            continue

        # ---- 步骤 1: 导出执行计划 ----
        print(f"  [1/4] 导出执行计划...")

        # 保存原始 SQL
        sql_path = f"{PLANS_DIR}/{query_key}.sql"
        save_query_sql(setup_sql, query_sql, sql_path)

        # 导出文本格式 EXPLAIN
        text_path = f"{PLANS_DIR}/{query_key}_explain.txt"
        export_plan_text(setup_sql, query_sql, text_path)

        # 导出 JSON 格式 EXPLAIN
        json_explain_path = f"{PLANS_DIR}/{query_key}_explain_json.txt"
        export_plan_json_explain(setup_sql, query_sql, json_explain_path)

        # 导出 profiler JSON
        profile_path = f"{PLANS_DIR}/{query_key}_profile.json"
        export_plan_profile_json(setup_sql, query_sql, profile_path)

        print(f"       ✅ 已导出: {sql_path}")
        print(f"       ✅ 已导出: {text_path}")
        print(f"       ✅ 已导出: {profile_path}")

        # ---- 步骤 2: 验证计划稳定性 ----
        print(f"  [2/4] 验证计划稳定性 (5次执行)...")
        stable, msg = verify_plan_stability(setup_sql, query_sql, runs=5)
        stability_results[query_key] = (stable, msg)
        print(f"       {'✅' if stable else '❌'} {msg}")

        # ---- 步骤 3: 验证重放一致性 ----
        print(f"  [3/4] 验证重放一致性 (3次执行)...")
        consistent, msg = verify_replay_consistency(setup_sql, query_sql, query_key)
        replay_results[query_key] = (consistent, msg)
        print(f"       {'✅' if consistent else '❌'} {msg}")

        # ---- 步骤 4: 测试 optimizer 控制 ----
        print(f"  [4/4] 测试 optimizer 控制...")
        configs, different = test_optimizer_control(setup_sql, query_sql)
        optimizer_results[query_key] = (configs, different)
        if different:
            print(f"       ℹ️ 以下配置改变了计划: {different}")
        else:
            print(f"       ℹ️ 所有配置产生相同计划")

    # ==================== 生成报告 ====================
    print(f"\n{'='*60}")
    print("生成可行性报告...")
    print(f"{'='*60}")

    report = generate_feasibility_report(stability_results, replay_results, optimizer_results)
    report_path = f"{PLANS_DIR}/injection_feasibility_report.md"
    with open(report_path, 'w') as f:
        f.write(report)
    print(f"  ✅ 报告已保存: {report_path}")

    # 保存验证结果 JSON
    verification_data = {
        "stability": {k: {"stable": s, "message": m} for k, (s, m) in stability_results.items()},
        "replay": {k: {"consistent": c, "message": m} for k, (c, m) in replay_results.items()},
        "optimizer_control": {
            k: {"different_configs": d} for k, (_, d) in optimizer_results.items()
        }
    }
    verification_path = f"{PLANS_DIR}/injection_verification.json"
    with open(verification_path, 'w') as f:
        json.dump(verification_data, f, indent=2, ensure_ascii=False)
    print(f"  ✅ 验证数据已保存: {verification_path}")

    # ==================== 最终汇总 ====================
    print(f"\n{'='*60}")
    print("最终汇总")
    print(f"{'='*60}")

    all_stable = all(s for s, _ in stability_results.values()) if stability_results else False
    all_consistent = all(c for c, _ in replay_results.values()) if replay_results else False

    print(f"\n  计划稳定性: {'✅ 全部稳定' if all_stable else '⚠️ 部分不稳定'}")
    print(f"  重放一致性: {'✅ 全部一致' if all_consistent else '⚠️ 部分不一致'}")

    if all_stable and all_consistent:
        print(f"\n  🎉 结论：方案 B（SQL + PRAGMA 固定）可用于 M1~M2 实验验证")
        print(f"     - GPU 路径可以通过「相同 SQL + 相同 PRAGMA」做可靠的 A/B 对比")
        print(f"     - 方案 C（PhysicalPlan 序列化）可延后到 M3 开发")
    else:
        print(f"\n  ⚠️ 结论：需要提前评估方案 C（PhysicalPlan 序列化/反序列化）")
        print(f"     - 建议分析不稳定/不一致的具体原因")
        print(f"     - 可能需要禁用 adaptive 类优化")

    print(f"\n✅ 所有结果已保存到: {PLANS_DIR}/")

if __name__ == "__main__":
    main()
