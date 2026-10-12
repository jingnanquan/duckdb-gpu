#!/usr/bin/env python3
"""
任务1.4: 查询分类脚本
根据 EXPLAIN 输出分析每条查询的类型：Scan-bound / Hash join 重叠 / 同表多次扫描

用法:
    cd /home/featurize/workspace/duckdb-cuda
    python3 a_idea/任务拆分/任务1/scripts/classify_queries.py
"""

import subprocess
import json
import re
import os
import sys

# ============================================================
# 配置
# ============================================================
BASE_DIR = "/home/featurize/workspace/duckdb-cuda"
DUCKDB_BIN = f"{BASE_DIR}/build/release/duckdb"
TPCH_DATA_DIR = f"{BASE_DIR}/data/tpch_sf5"
TPCDS_DATA_DIR = f"{BASE_DIR}/data/tpcds_sf5"
TASK_DIR = f"{BASE_DIR}/a_idea/任务拆分/任务1"
OUTPUT_JSON = f"{TASK_DIR}/results/query_classification_sf5.json"

# 大表列表（用于判断 hash join 重叠）
LARGE_TABLES_TPCH = {'lineitem', 'orders', 'partsupp', 'customer', 'part', 'supplier'}
LARGE_TABLES_TPCDS = {'store_sales', 'catalog_sales', 'web_sales', 'store_returns',
                      'catalog_returns', 'web_returns', 'inventory', 'customer',
                      'customer_address', 'customer_demographics', 'item', 'date_dim'}

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

def get_explain_output(setup_sql, query_sql):
    """获取 EXPLAIN 文本输出"""
    full_sql = f"""
{setup_sql}
PRAGMA threads=4;
EXPLAIN {query_sql}
"""
    result = subprocess.run(
        [DUCKDB_BIN, "-c", full_sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=120
    )
    if result.returncode != 0:
        return None
    return result.stdout

def extract_tables_from_sql(query_sql, known_tables):
    """从 SQL 文本中提取涉及的表名（含重复出现次数）"""
    sql_lower = query_sql.lower()
    scan_tables = []

    # 按表名长度降序排列，优先匹配长表名（如 customer_address 优先于 customer）
    sorted_tables = sorted(known_tables, key=len, reverse=True)

    for t in sorted_tables:
        # 匹配 FROM/JOIN 后面的表名，使用单词边界
        # 匹配模式: FROM table / JOIN table / , table（逗号分隔的 FROM 列表）
        pattern = r'(?:from|join|,)\s+' + re.escape(t) + r'(?:\s|,|;|$|\))'
        matches = re.findall(pattern, sql_lower)
        for _ in matches:
            scan_tables.append(t)

    return scan_tables


def classify_query(explain_text, benchmark, query_sql=None):
    """根据 EXPLAIN 文本和查询 SQL 分类查询"""
    if not explain_text:
        return {"categories": ["unknown"], "reason": "explain failed"}

    text_upper = explain_text.upper()

    # 已知表名集合
    all_known_tables_tpch = {'lineitem', 'customer', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier'}
    all_known_tables_tpcds = {
        'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
        'customer', 'customer_address', 'customer_demographics', 'date_dim',
        'household_demographics', 'income_band', 'inventory', 'item',
        'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
        'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
        'web_sales', 'web_site'
    }
    known_tables = all_known_tables_tpch if benchmark == "tpch" else all_known_tables_tpcds

    scan_tables = []

    # 主要方法: 从查询 SQL 中提取表名（最可靠）
    if query_sql:
        scan_tables = extract_tables_from_sql(query_sql, known_tables)

    # 备选方法: 从 EXPLAIN 文本中提取
    if not scan_tables:
        # 方法A: 从 memory.main.XXX.column 格式提取表名
        memory_pattern = r'memory\.main\.([a-z_]+)\.'
        memory_matches = re.findall(memory_pattern, explain_text.lower())
        tables_from_memory = [t for t in set(memory_matches) if t in known_tables]

        if tables_from_memory:
            scan_tables = tables_from_memory
        else:
            # 方法B: 从 EXPLAIN 文本中匹配已知表名
            for t in known_tables:
                if t.upper() in text_upper:
                    scan_tables.append(t)

    # 统计 READ_PARQUET 算子出现次数
    # 在 EXPLAIN 文本中，每个扫描算子框包含 "Function: READ_PARQUET" 一行
    # 所以用 "Function:" + "READ_PARQUET" 的组合来计数更准确
    read_parquet_count = text_upper.count('FUNCTION:')
    if read_parquet_count == 0:
        # 退而求其次：直接数 READ_PARQUET 出现次数，除以2（算子名+Function名各一次）
        raw_count = len(re.findall(r'READ_PARQUET', explain_text))
        read_parquet_count = max(1, raw_count // 2)

    # 检测 join 类型
    has_hash_join = 'HASH_JOIN' in text_upper
    has_nested_loop = 'NESTED_LOOP_JOIN' in text_upper
    has_merge_join = 'MERGE_JOIN' in text_upper or 'PIECEWISE_MERGE_JOIN' in text_upper
    has_any_join = has_hash_join or has_nested_loop or has_merge_join

    # 统计每个表被扫描的次数
    table_counts = {}
    for t in scan_tables:
        table_counts[t] = table_counts.get(t, 0) + 1

    # 同表多次扫描检测
    multi_scan_tables = {k: v for k, v in table_counts.items() if v >= 2}

    # 判断大表参与情况
    large_tables = LARGE_TABLES_TPCH if benchmark == "tpch" else LARGE_TABLES_TPCDS
    large_table_scans = [t for t in scan_tables if t in large_tables]

    # 检测 aggregate / sort
    has_aggregate = 'HASH_GROUP_BY' in text_upper or 'PERFECT_HASH_GROUP_BY' in text_upper or 'UNGROUPED_AGGREGATE' in text_upper
    has_sort = 'ORDER_BY' in text_upper or 'TOP_N' in text_upper
    has_window = 'WINDOW' in text_upper

    # ==================== 分类逻辑 ====================
    categories = []

    # 1. 同表多次扫描
    if multi_scan_tables:
        categories.append("multi_scan")

    # 2. Hash join 重叠（多个大表参与 hash join）
    if has_hash_join and len(large_table_scans) >= 2:
        categories.append("hash_join_overlap")

    # 3. Scan-bound（无 join 或仅小表 join，大表全扫为主）
    if not has_any_join and len(scan_tables) >= 1:
        categories.append("scan_bound")
    elif has_any_join and len(large_table_scans) == 1 and len(scan_tables) <= 3:
        # 只有一个大表 + 小表 join，仍然是 scan-bound
        categories.append("scan_bound")

    # 如果没有匹配任何类别
    if not categories:
        categories.append("mixed")

    return {
        "categories": categories,
        "scan_tables": list(set(scan_tables)),
        "scan_table_counts": table_counts,
        "multi_scan_tables": multi_scan_tables,
        "has_hash_join": has_hash_join,
        "has_nested_loop": has_nested_loop,
        "has_aggregate": has_aggregate,
        "has_sort": has_sort,
        "has_window": has_window,
        "num_total_scans": len(scan_tables),
        "num_large_table_scans": len(large_table_scans),
        "large_tables_involved": list(set(large_table_scans))
    }

# ============================================================
# 主流程
# ============================================================

def main():
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)

    # 验证 DuckDB 可用
    result = subprocess.run([DUCKDB_BIN, "-c", "SELECT 'ok';"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=10)
    if result.returncode != 0:
        print(f"❌ DuckDB 不可用: {result.stderr}")
        sys.exit(1)

    classifications = {}

    # ==================== TPCH ====================
    print("=" * 60)
    print("TPCH 查询分类 (Q1~Q22)")
    print("=" * 60)

    tpch_setup = get_setup_sql("tpch")
    for qnum in range(1, 23):
        key = f"tpch_Q{qnum:02d}"
        query_sql = get_query("tpch", qnum)
        if not query_sql:
            classifications[key] = {"categories": ["skip"], "reason": "query not available"}
            print(f"  {key}: SKIP")
            continue

        explain = get_explain_output(tpch_setup, query_sql)
        cls = classify_query(explain, "tpch", query_sql)
        classifications[key] = cls
        print(f"  {key}: {cls['categories']}  tables={cls.get('scan_tables', [])}")

    # ==================== TPCDS ====================
    print("\n" + "=" * 60)
    print("TPCDS 查询分类 (Q1~Q99)")
    print("=" * 60)

    tpcds_setup = get_setup_sql("tpcds")
    for qnum in range(1, 100):
        key = f"tpcds_Q{qnum:02d}"
        query_sql = get_query("tpcds", qnum)
        if not query_sql:
            classifications[key] = {"categories": ["skip"], "reason": "query not available"}
            print(f"  {key}: SKIP")
            continue

        explain = get_explain_output(tpcds_setup, query_sql)
        cls = classify_query(explain, "tpcds", query_sql)
        classifications[key] = cls
        print(f"  {key}: {cls['categories']}  large_tables={cls.get('large_tables_involved', [])}")

    # ==================== 保存结果 ====================
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(classifications, f, indent=2, ensure_ascii=False)

    # ==================== 打印分类汇总 ====================
    print("\n" + "=" * 60)
    print("分类汇总")
    print("=" * 60)

    for cat in ["scan_bound", "hash_join_overlap", "multi_scan", "mixed"]:
        queries = [k for k, v in classifications.items()
                   if isinstance(v.get("categories"), list) and cat in v["categories"]]
        print(f"\n  [{cat}] ({len(queries)} 条):")
        for q in sorted(queries)[:15]:
            info = classifications[q]
            tables = info.get("large_tables_involved", [])
            multi = info.get("multi_scan_tables", {})
            extra = ""
            if multi:
                extra = f"  multi_scan={multi}"
            elif tables:
                extra = f"  large_tables={tables}"
            print(f"    - {q}{extra}")
        if len(queries) > 15:
            print(f"    ... 共 {len(queries)} 条")

    # ==================== 推荐 MVP 查询 ====================
    print("\n" + "=" * 60)
    print("推荐 MVP 查询（用于后续 GPU 加速验证）")
    print("=" * 60)

    # Scan-bound: 选择只有大表全扫 + filter 的查询
    scan_bound_queries = [k for k, v in classifications.items()
                          if isinstance(v.get("categories"), list) and "scan_bound" in v["categories"]]
    print(f"\n  Scan-bound MVP 候选 ({len(scan_bound_queries)} 条):")
    for q in sorted(scan_bound_queries)[:5]:
        print(f"    - {q}")

    # Hash join overlap: 选择有多个大表参与的查询
    hash_join_queries = [k for k, v in classifications.items()
                         if isinstance(v.get("categories"), list) and "hash_join_overlap" in v["categories"]]
    print(f"\n  Hash join overlap MVP 候选 ({len(hash_join_queries)} 条):")
    for q in sorted(hash_join_queries)[:5]:
        print(f"    - {q}")

    # Multi-scan: 选择同表多次扫描的查询
    multi_scan_queries = [k for k, v in classifications.items()
                          if isinstance(v.get("categories"), list) and "multi_scan" in v["categories"]]
    print(f"\n  Multi-scan MVP 候选 ({len(multi_scan_queries)} 条):")
    for q in sorted(multi_scan_queries)[:5]:
        info = classifications[q]
        print(f"    - {q}  multi_scan_tables={info.get('multi_scan_tables', {})}")

    print(f"\n✅ 分类结果已保存到: {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
