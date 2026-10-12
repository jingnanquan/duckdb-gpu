#!/usr/bin/env python3
"""
任务1.5: Profiling 分析脚本
对重点查询进行详细 profiling，获取算子时间占比。
使用高精度监控器 (2ms 采样间隔) 采集 CPU 和 IO 负载，
并生成 matplotlib 时序图。

用法:
    cd /home/featurize/workspace/duckdb-cuda
    python3 a_idea/任务拆分/任务1/scripts/run_profiling.py

依赖:
    - matplotlib (画图): pip install matplotlib
    - 无需 sysstat，直接从 /proc 文件系统采集
"""

import subprocess
import json
import os
import time
import sys
import signal

# 将当前脚本目录加入 path，以便导入 highres_monitor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from highres_monitor import HighResMonitor, monitor_query_execution

# ============================================================
# 配置
# ============================================================
BASE_DIR = "/home/featurize/workspace/duckdb-cuda"
DUCKDB_BIN = f"{BASE_DIR}/build/release/duckdb"
TPCH_DATA_DIR = f"{BASE_DIR}/data/tpch_sf5"
TPCDS_DATA_DIR = f"{BASE_DIR}/data/tpcds_sf5"
TASK_DIR = f"{BASE_DIR}/a_idea/任务拆分/任务1"
PROFILE_OUTPUT_DIR = f"{TASK_DIR}/results/profiles"
SYSTEM_METRICS_DIR = f"{TASK_DIR}/results/system_metrics"

# 重点查询（覆盖三类：scan-bound / hash join overlap / multi-scan）
# 实际执行时可根据 1.4 分类结果调整
TARGET_QUERIES = {
    # Scan-bound 代表
    "tpch_Q01": ("tpch", 1),
    "tpch_Q06": ("tpch", 6),
    "tpch_Q14": ("tpch", 14),
    # Hash join 重叠代表
    "tpch_Q03": ("tpch", 3),
    "tpch_Q05": ("tpch", 5),
    "tpch_Q09": ("tpch", 9),
    "tpch_Q12": ("tpch", 12),
    # 同表多次扫描代表 (TPCDS)
    "tpcds_Q04": ("tpcds", 4),
    "tpcds_Q11": ("tpcds", 11),
    "tpcds_Q47": ("tpcds", 47),
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

# 高精度监控采样间隔 (毫秒)
# 2ms 可以在 0.2s 的查询中获得 ~100 个采样点
MONITOR_INTERVAL_MS = 2

def run_profiled_query(setup_sql, query_sql, profile_json_path):
    """执行查询并输出 JSON profile"""
    full_sql = f"""
{setup_sql}
PRAGMA threads=4;
PRAGMA enable_profiling='json';
PRAGMA profiling_mode='detailed';
PRAGMA profiling_output='{profile_json_path}';
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

def parse_profile_json(profile_path):
    """解析 DuckDB profile JSON，提取算子时间占比"""
    if not os.path.exists(profile_path):
        return None

    try:
        with open(profile_path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        return None

    operators = []

    def walk(node, depth=0):
        """递归遍历算子树"""
        # DuckDB profile JSON 可能有不同的字段名
        op_name = (node.get("operator_type") or
                   node.get("name") or
                   node.get("type") or
                   "unknown")
        timing = (node.get("operator_timing") or
                  node.get("timing") or
                  node.get("time") or
                  0)
        cardinality = (node.get("operator_cardinality") or
                       node.get("cardinality") or
                       node.get("result_set_size") or
                       0)
        extra_info = node.get("extra_info", "")

        # 确保 timing 是数值
        if isinstance(timing, str):
            try:
                timing = float(timing)
            except ValueError:
                timing = 0

        operators.append({
            "name": op_name,
            "timing_s": timing,
            "cardinality": cardinality,
            "extra_info": str(extra_info)[:200],
            "depth": depth
        })

        # 递归处理子节点
        children = node.get("children", [])
        if isinstance(children, list):
            for child in children:
                if isinstance(child, dict):
                    walk(child, depth + 1)

    # DuckDB profile JSON 结构可能有多种形式
    if isinstance(data, dict):
        if "children" in data:
            for child in data["children"]:
                if isinstance(child, dict):
                    walk(child)
        else:
            walk(data)
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                walk(item)

    # 计算占比
    total_time = sum(op["timing_s"] for op in operators if op["timing_s"] > 0)
    for op in operators:
        op["pct"] = (op["timing_s"] / total_time * 100) if total_time > 0 else 0

    # 按算子类型聚合
    type_summary = {}
    for op in operators:
        name = op["name"]
        if name not in type_summary:
            type_summary[name] = {"total_time_s": 0, "count": 0, "total_cardinality": 0}
        type_summary[name]["total_time_s"] += op["timing_s"]
        type_summary[name]["count"] += 1
        type_summary[name]["total_cardinality"] += op.get("cardinality", 0)

    # 计算类型占比
    for name, info in type_summary.items():
        info["pct"] = (info["total_time_s"] / total_time * 100) if total_time > 0 else 0

    return {
        "operators": operators,
        "type_summary": type_summary,
        "total_time_s": total_time,
        "num_operators": len(operators)
    }

def compute_scan_ratio(profile_data):
    """计算 scan 算子占总时间的比例"""
    if not profile_data or not profile_data.get("type_summary"):
        return None

    scan_keywords = ["SCAN", "PARQUET_SCAN", "SEQ_SCAN", "TABLE_SCAN"]
    total_scan_time = 0
    total_time = profile_data["total_time_s"]

    for name, info in profile_data["type_summary"].items():
        if any(kw in name.upper() for kw in scan_keywords):
            total_scan_time += info["total_time_s"]

    return (total_scan_time / total_time * 100) if total_time > 0 else 0

# ============================================================
# 主流程
# ============================================================

def main():
    os.makedirs(PROFILE_OUTPUT_DIR, exist_ok=True)
    os.makedirs(SYSTEM_METRICS_DIR, exist_ok=True)

    # 验证 DuckDB 可用
    result = subprocess.run([DUCKDB_BIN, "-c", "SELECT 'ok';"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=10)
    if result.returncode != 0:
        print(f"❌ DuckDB 不可用: {result.stderr}")
        sys.exit(1)

    # 检查 matplotlib
    try:
        import matplotlib
        print(f"✅ matplotlib {matplotlib.__version__} 可用")
    except ImportError:
        print("⚠️ matplotlib 未安装，尝试安装...")
        subprocess.run([sys.executable, "-m", "pip", "install", "matplotlib"],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            import matplotlib
            print(f"✅ matplotlib {matplotlib.__version__} 安装成功")
        except ImportError:
            print("⚠️ matplotlib 安装失败，将跳过画图")

    print(f"📊 高精度监控: 采样间隔={MONITOR_INTERVAL_MS}ms (独立进程, /proc 直读)")

    summary = {}

    for query_key, (benchmark, qnum) in TARGET_QUERIES.items():
        print(f"\n{'='*50}")
        print(f"Profiling: {query_key} ({benchmark} Q{qnum})")
        print(f"{'='*50}")

        setup_sql = get_setup_sql(benchmark)
        query_sql = get_query(benchmark, qnum)
        if not query_sql:
            print(f"  ⚠️ SKIP: 无法获取查询")
            continue

        profile_path = f"{PROFILE_OUTPUT_DIR}/{query_key}_profile.json"
        metrics_prefix = f"{SYSTEM_METRICS_DIR}/{query_key}"

        # 使用高精度监控器执行查询
        print(f"  [预热] 执行一次...")
        run_profiled_query(setup_sql, query_sql, "/tmp/warmup_profile.json")

        print(f"  [监控] 启动高精度采集 (interval={MONITOR_INTERVAL_MS}ms)...")
        monitor = HighResMonitor(interval_ms=MONITOR_INTERVAL_MS)
        monitor.start()

        # 等待监控进程稳定
        time.sleep(0.01)

        # 执行 profiled 查询
        print(f"  [执行] 运行查询...")
        elapsed, err = run_profiled_query(setup_sql, query_sql, profile_path)

        # 等待最后几个采样点
        time.sleep(0.01)
        monitor.stop()

        if elapsed is None:
            print(f"  ❌ ERROR: {err}")
            continue

        print(f"  ✅ 执行完成: {elapsed:.3f}s")

        # 保存高精度监控数据
        monitor.save_csv(f"{metrics_prefix}_metrics.csv")
        mon_summary = monitor.get_summary()
        if mon_summary:
            print(f"  📈 系统指标 ({mon_summary['num_samples']} 采样点, "
                  f"实际间隔={mon_summary['actual_interval_ms']:.1f}ms):")
            print(f"     CPU:  avg={mon_summary['cpu_pct']['avg']:.1f}%, "
                  f"max={mon_summary['cpu_pct']['max']:.1f}%, "
                  f"p95={mon_summary['cpu_pct']['p95']:.1f}%")
            print(f"     IOW:  avg={mon_summary['iowait_pct']['avg']:.1f}%, "
                  f"max={mon_summary['iowait_pct']['max']:.1f}%")
            print(f"     Read: avg={mon_summary['disk_read_mbps']['avg']:.1f} MB/s, "
                  f"max={mon_summary['disk_read_mbps']['max']:.1f} MB/s")
            print(f"     Util: avg={mon_summary['disk_util_pct']['avg']:.1f}%, "
                  f"max={mon_summary['disk_util_pct']['max']:.1f}%")

        # 画图
        plot_path = f"{metrics_prefix}_metrics.png"
        if monitor.plot(plot_path, title=f"{query_key} ({benchmark} Q{qnum})"):
            print(f"  🖼️  图表已保存: {plot_path}")

        # 解析 profile
        profile_data = parse_profile_json(profile_path)
        if profile_data:
            scan_ratio = compute_scan_ratio(profile_data)
            print(f"  📊 算子统计:")
            print(f"     总算子数: {profile_data['num_operators']}")
            print(f"     Profile 总时间: {profile_data['total_time_s']:.4f}s")
            print(f"     Scan 占比: {scan_ratio:.1f}%")
            print(f"     算子时间分布 (Top 5):")

            sorted_types = sorted(profile_data["type_summary"].items(),
                                  key=lambda x: x[1]["total_time_s"], reverse=True)
            for name, info in sorted_types[:5]:
                print(f"       {name:35s} {info['total_time_s']:.4f}s ({info['pct']:.1f}%) x{info['count']}")

            summary[query_key] = {
                "wall_time_s": elapsed,
                "profile_total_time_s": profile_data["total_time_s"],
                "scan_ratio_pct": scan_ratio,
                "type_summary": {k: {"time_s": v["total_time_s"], "pct": v["pct"], "count": v["count"]}
                                 for k, v in sorted_types},
                "num_operators": profile_data["num_operators"],
                "profile_file": profile_path,
                "metrics_files": {
                    "csv": f"{metrics_prefix}_metrics.csv",
                    "plot": f"{metrics_prefix}_metrics.png",
                },
                "system_metrics": mon_summary if mon_summary else None
            }
        else:
            print(f"  ⚠️ 无法解析 profile JSON")
            summary[query_key] = {
                "wall_time_s": elapsed,
                "profile_parse_error": True
            }

    # ==================== 保存汇总 ====================
    summary_path = f"{PROFILE_OUTPUT_DIR}/profiling_summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    # ==================== 打印最终汇总 ====================
    print("\n" + "=" * 60)
    print("Profiling 汇总")
    print("=" * 60)
    print(f"\n{'查询':<15} {'Wall Time':<12} {'Scan占比':<10} {'Top算子'}")
    print("-" * 70)

    for query_key, info in summary.items():
        if "profile_parse_error" in info:
            print(f"{query_key:<15} {info['wall_time_s']:.3f}s       (解析失败)")
            continue

        wall = info.get("wall_time_s", 0)
        scan_pct = info.get("scan_ratio_pct", 0)
        top_op = ""
        if info.get("type_summary"):
            top_name = list(info["type_summary"].keys())[0]
            top_pct = info["type_summary"][top_name]["pct"]
            top_op = f"{top_name} ({top_pct:.1f}%)"
        print(f"{query_key:<15} {wall:.3f}s       {scan_pct:.1f}%       {top_op}")

    # ==================== 结论分析 ====================
    print("\n" + "=" * 60)
    print("初步结论")
    print("=" * 60)

    scan_bound_queries = {k: v for k, v in summary.items()
                          if v.get("scan_ratio_pct", 0) > 50}
    join_heavy_queries = {k: v for k, v in summary.items()
                          if v.get("scan_ratio_pct", 0) <= 50 and not v.get("profile_parse_error")}

    if scan_bound_queries:
        print(f"\n  Scan-bound 查询 (scan > 50%):")
        for k, v in scan_bound_queries.items():
            print(f"    - {k}: scan={v.get('scan_ratio_pct', 0):.1f}% → GPU filter/projection 有收益空间")

    if join_heavy_queries:
        print(f"\n  Join-heavy 查询 (scan <= 50%):")
        for k, v in join_heavy_queries.items():
            print(f"    - {k}: scan={v.get('scan_ratio_pct', 0):.1f}% → probe preload 有收益空间")

    print(f"\n✅ Profiling 结果已保存到: {summary_path}")
    print(f"✅ 系统指标已保存到: {SYSTEM_METRICS_DIR}/")

if __name__ == "__main__":
    main()
