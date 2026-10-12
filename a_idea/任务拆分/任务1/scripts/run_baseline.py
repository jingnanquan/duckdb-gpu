#!/usr/bin/env python3
"""
任务1.3: 运行 TPCH 22 条 + TPCDS 99 条查询，记录每条查询的执行时间。
产出 CSV 格式的基线报告。

用法:
    cd /home/featurize/workspace/duckdb-cuda
    python3 a_idea/任务拆分/任务1/scripts/run_baseline.py
"""

import subprocess
import csv
import time
import os
import sys
import statistics

# ============================================================
# 配置
# ============================================================
BASE_DIR = "/home/featurize/workspace/duckdb-cuda"
DUCKDB_BIN = f"{BASE_DIR}/build/release/duckdb"
TPCH_DATA_DIR = f"{BASE_DIR}/data/tpch_sf5"
TPCDS_DATA_DIR = f"{BASE_DIR}/data/tpcds_sf5"
TASK_DIR = f"{BASE_DIR}/a_idea/任务拆分/任务1"
OUTPUT_CSV = f"{TASK_DIR}/results/baseline_sf5.csv"
RUNS = 3  # 每条查询跑 3 次取中位数

# TPCH 查询 Q1~Q22
TPCH_QUERIES = list(range(1, 23))
# TPCDS 查询 Q1~Q99
TPCDS_QUERIES = list(range(1, 100))

# ============================================================
# 辅助函数
# ============================================================

def get_tpch_setup_sql():
    """生成 TPCH 的 view 创建 SQL"""
    tables = ['lineitem', 'customer', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
    sqls = []
    for t in tables:
        sqls.append(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{TPCH_DATA_DIR}/{t}.parquet');")
    return "\n".join(sqls)

def get_tpcds_setup_sql():
    """生成 TPCDS 的 view 创建 SQL"""
    tables = [
        'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
        'customer', 'customer_address', 'customer_demographics', 'date_dim',
        'household_demographics', 'income_band', 'inventory', 'item',
        'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
        'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
        'web_sales', 'web_site'
    ]
    sqls = []
    for t in tables:
        path = f"{TPCDS_DATA_DIR}/{t}.parquet"
        if os.path.exists(path):
            sqls.append(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{path}');")
    return "\n".join(sqls)

def get_tpch_query(qnum):
    """通过 DuckDB tpch extension 获取查询 SQL"""
    sql = f"SELECT query FROM tpch_queries() WHERE query_nr={qnum};"
    result = subprocess.run(
        [DUCKDB_BIN, "-noheader", "-list", "-c", sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=30
    )
    if result.returncode != 0:
        return None
    query = result.stdout.strip()
    if not query:
        return None
    return query

def get_tpcds_query(qnum):
    """通过 DuckDB tpcds extension 获取查询 SQL"""
    sql = f"SELECT query FROM tpcds_queries() WHERE query_nr={qnum};"
    result = subprocess.run(
        [DUCKDB_BIN, "-noheader", "-list", "-c", sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=30
    )
    if result.returncode != 0:
        return None
    query = result.stdout.strip()
    if not query:
        return None
    return query

def run_single_query(setup_sql, query_sql, threads=4):
    """执行单条查询，返回执行时间（秒）"""
    full_sql = f"""
{setup_sql}
PRAGMA threads={threads};
PRAGMA enable_progress_bar=false;
{query_sql}
"""
    start = time.time()
    result = subprocess.run(
        [DUCKDB_BIN, "-c", full_sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=600
    )
    elapsed = time.time() - start

    if result.returncode != 0:
        return None, result.stderr[:500]

    return elapsed, None

def run_query_multiple(setup_sql, query_sql, runs=RUNS):
    """多次执行取中位数"""
    times = []
    errors = []
    for i in range(runs):
        t, err = run_single_query(setup_sql, query_sql)
        if t is None:
            errors.append(err)
            continue
        times.append(t)

    if not times:
        return None, errors[0] if errors else "unknown error"

    median_time = statistics.median(times)
    return median_time, None

# ============================================================
# 主流程
# ============================================================

def main():
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    # 验证 DuckDB 可用
    result = subprocess.run([DUCKDB_BIN, "-c", "SELECT 'ok';"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=10)
    if result.returncode != 0:
        print(f"❌ DuckDB 不可用: {result.stderr}")
        sys.exit(1)
    print(f"✅ DuckDB 可用: {DUCKDB_BIN}")

    results = []
    tpch_setup = get_tpch_setup_sql()
    tpcds_setup = get_tpcds_setup_sql()

    # ==================== TPCH ====================
    print("\n" + "=" * 60)
    print("TPCH SF=5 基线测试 (Q1~Q22)")
    print("=" * 60)

    for qnum in TPCH_QUERIES:
        query_sql = get_tpch_query(qnum)
        if not query_sql:
            print(f"  TPCH Q{qnum:02d}: SKIP (无法获取查询)")
            results.append({"benchmark": "tpch", "query": f"Q{qnum:02d}", "time_s": "", "min_s": "", "max_s": "", "status": "skip"})
            continue

        # 预热一次
        run_single_query(tpch_setup, query_sql)

        # 正式测试
        times = []
        for i in range(RUNS):
            t, err = run_single_query(tpch_setup, query_sql)
            if t is not None:
                times.append(t)

        if times:
            median_t = statistics.median(times)
            min_t = min(times)
            max_t = max(times)
            print(f"  TPCH Q{qnum:02d}: median={median_t:.3f}s  min={min_t:.3f}s  max={max_t:.3f}s")
            results.append({
                "benchmark": "tpch", "query": f"Q{qnum:02d}",
                "time_s": f"{median_t:.4f}", "min_s": f"{min_t:.4f}", "max_s": f"{max_t:.4f}",
                "status": "ok"
            })
        else:
            print(f"  TPCH Q{qnum:02d}: ERROR")
            results.append({"benchmark": "tpch", "query": f"Q{qnum:02d}", "time_s": "", "min_s": "", "max_s": "", "status": "error"})

    # ==================== TPCDS ====================
    print("\n" + "=" * 60)
    print("TPCDS SF=5 基线测试 (Q1~Q99)")
    print("=" * 60)

    for qnum in TPCDS_QUERIES:
        query_sql = get_tpcds_query(qnum)
        if not query_sql:
            print(f"  TPCDS Q{qnum:02d}: SKIP (无法获取查询)")
            results.append({"benchmark": "tpcds", "query": f"Q{qnum:02d}", "time_s": "", "min_s": "", "max_s": "", "status": "skip"})
            continue

        # 预热一次
        run_single_query(tpcds_setup, query_sql)

        # 正式测试
        times = []
        for i in range(RUNS):
            t, err = run_single_query(tpcds_setup, query_sql)
            if t is not None:
                times.append(t)

        if times:
            median_t = statistics.median(times)
            min_t = min(times)
            max_t = max(times)
            print(f"  TPCDS Q{qnum:02d}: median={median_t:.3f}s  min={min_t:.3f}s  max={max_t:.3f}s")
            results.append({
                "benchmark": "tpcds", "query": f"Q{qnum:02d}",
                "time_s": f"{median_t:.4f}", "min_s": f"{min_t:.4f}", "max_s": f"{max_t:.4f}",
                "status": "ok"
            })
        else:
            print(f"  TPCDS Q{qnum:02d}: ERROR")
            results.append({"benchmark": "tpcds", "query": f"Q{qnum:02d}", "time_s": "", "min_s": "", "max_s": "", "status": "error"})

    # ==================== 写入 CSV ====================
    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["benchmark", "query", "time_s", "min_s", "max_s", "status"])
        writer.writeheader()
        writer.writerows(results)

    # 打印汇总
    ok_results = [r for r in results if r["status"] == "ok"]
    skip_results = [r for r in results if r["status"] == "skip"]
    error_results = [r for r in results if r["status"] == "error"]

    print("\n" + "=" * 60)
    print("汇总")
    print("=" * 60)
    print(f"  成功: {len(ok_results)} 条")
    print(f"  跳过: {len(skip_results)} 条")
    print(f"  失败: {len(error_results)} 条")

    if ok_results:
        tpch_ok = [r for r in ok_results if r["benchmark"] == "tpch"]
        tpcds_ok = [r for r in ok_results if r["benchmark"] == "tpcds"]
        if tpch_ok:
            tpch_total = sum(float(r["time_s"]) for r in tpch_ok)
            print(f"  TPCH 总耗时: {tpch_total:.2f}s ({len(tpch_ok)} 条)")
        if tpcds_ok:
            tpcds_total = sum(float(r["time_s"]) for r in tpcds_ok)
            print(f"  TPCDS 总耗时: {tpcds_total:.2f}s ({len(tpcds_ok)} 条)")

    print(f"\n✅ 基线结果已保存到: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
