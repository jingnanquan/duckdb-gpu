#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tpche2e_bench_sdk_bitmap_only.py — 只重测 project_bitmap, 两个 baseline 从固定 CSV 读取

与 tpche2e_bench_sdk.py 的区别:
  - 不再下载/运行官方 libduckdb.so
  - official_baseline / project_baseline 的 warmup/avg/min/max 从 CSV 读取
  - 只运行本项目 project_bitmap: libduckdb.so + bitmap_join_load + open_bitmap_join=true

产物:
  - tpche2e_results.csv
  - tpche2e_chart.png      三组耗时柱状图
  - tpche2e_speedup.png    project_bitmap 相比 official_baseline / project_baseline 的加速比

用法:
  cd /home/featurize/workspace/duckdb-cuda
  bash b_idea/perf/tpche2e/build_harness.sh
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk_bitmap_only.py
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk_bitmap_only.py --runs 5
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk_bitmap_only.py --queries 1,6,9,18
"""

import argparse
import csv
import json
import os
import subprocess
import sys

# ============================================================
# 路径配置
# ============================================================
BASE_DIR = "/home/featurize/workspace/duckdb-cuda"
DATA_DIR = os.path.join(BASE_DIR, "data", "tpch_sf5_bitmap")
META_PATH = os.path.join(DATA_DIR, "bitmap_join_meta.json")
PROJECT_LIB = os.path.join(BASE_DIR, "build", "release", "src", "libduckdb.so")
PROJECT_CLI = os.path.join(BASE_DIR, "build", "release", "duckdb")

OUT_DIR = os.path.join(BASE_DIR, "b_idea", "perf", "tpche2e")
HARNESS = os.path.join(OUT_DIR, "tpch_bench_harness")
QUERIES_CACHE = os.path.join(OUT_DIR, "tpch_queries.json")
QUERIES_RUN = os.path.join(OUT_DIR, "queries_run.sql")

# 固定 baseline 数据的 CSV (由 tpche2e_bench_sdk.py 全量跑出)
BASELINES_CSV = os.path.join(OUT_DIR, "tpche2e_results.csv")

# 计时参数
DEFAULT_RUNS = 3
DEFAULT_WARMUP = 1
DEFAULT_TIMEOUT = 600

CSV_PATH = os.path.join(OUT_DIR, "tpche2e_results.csv")
CHART_PATH = os.path.join(OUT_DIR, "tpche2e_chart.png")
SPEEDUP_PATH = os.path.join(OUT_DIR, "tpche2e_speedup.png")

TPCH_TABLES = ["region", "nation", "customer", "orders",
               "part", "partsupp", "supplier", "lineitem"]


# ============================================================
# 工具
# ============================================================
def log(msg):
    print(msg, flush=True)


def check_data_dir(data_dir):
    missing = [t for t in TPCH_TABLES
               if not os.path.exists(os.path.join(data_dir, t + ".parquet"))]
    if missing:
        log(f"[ERROR] 数据集目录 {data_dir} 缺少 parquet: {missing}")
        sys.exit(1)


def extract_queries(project_cli, refresh=False):
    if os.path.exists(QUERIES_CACHE) and not refresh:
        with open(QUERIES_CACHE) as f:
            data = json.load(f)
        if len(data) == 22:
            log(f"[queries] 使用缓存: {QUERIES_CACHE}")
            return {int(k): v for k, v in data.items()}
    log("[queries] 通过本项目 CLI 的 tpch_queries() 提取 22 条查询文本 ...")
    sql = "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr;"
    proc = subprocess.run([project_cli, "-json", "-c", sql],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          universal_newlines=True, timeout=120)
    if proc.returncode != 0:
        log(f"[ERROR] 无法提取查询文本: {proc.stderr[:400]}")
        sys.exit(1)
    rows = json.loads(proc.stdout)
    queries = {int(r["query_nr"]): r["query"] for r in rows}
    if len(queries) != 22:
        log(f"[ERROR] tpch_queries() 返回 {len(queries)} 条, 期望 22 条")
        sys.exit(1)
    with open(QUERIES_CACHE, "w") as f:
        json.dump(queries, f, indent=2)
    log(f"[queries] 已缓存 22 条查询到 {QUERIES_CACHE}")
    return queries


def write_queries_file(queries, path):
    with open(path, "w") as f:
        for qnum in sorted(queries.keys()):
            q = queries[qnum].strip()
            if q.endswith(";"):
                q = q[:-1]
            f.write(f"--QUERY Q{qnum:02d}\n")
            f.write(q + "\n")
            f.write("--END\n")
    log(f"[queries] 已写出 harness 查询文件: {path}")


# ============================================================
# 从 CSV 加载固定 baseline 数据
# ============================================================
def value(row, *names):
    for name in names:
        if name in row and row[name]:
            return float(row[name])
    return None


def csv_metric(row, prefix, legacy_prefix=None):
    names = [prefix]
    if legacy_prefix:
        names.append(legacy_prefix)
    warmup = value(row, *(f"{name}_warmup_s" for name in names))
    avg = value(row, *(f"{name}_avg_s" for name in names))
    mn = value(row, *(f"{name}_min_s" for name in names))
    mx = value(row, *(f"{name}_max_s" for name in names))
    runs = [v for v in (mn, mx) if v is not None]
    return {"warmup": warmup, "avg": avg, "runs": runs, "error": None}


def load_baselines_from_csv(csv_path):
    """读取全量结果 CSV, 返回 official_baseline_res / project_baseline_res。"""
    if not os.path.exists(csv_path):
        log(f"[ERROR] baseline CSV 不存在: {csv_path}")
        log("       请先运行 tpche2e_bench_sdk.py 生成 official_baseline / project_baseline。")
        sys.exit(1)
    official_res = {}
    project_baseline_res = {}
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qname = row["query"]
            official_res[qname] = csv_metric(row, "official_baseline", legacy_prefix="official")
            project_baseline_res[qname] = csv_metric(row, "project_baseline")
    log(f"[baseline] 从 {csv_path} 加载了 {len(official_res)} 条 official_baseline 固定数据")
    log(f"[baseline] 从 {csv_path} 加载了 {len(project_baseline_res)} 条 project_baseline 固定数据")
    return official_res, project_baseline_res


# ============================================================
# 运行 harness (bitmap)
# ============================================================
def run_harness(lib_dir, data_dir, meta, bitmap, warmup, runs, queries_file):
    env = dict(os.environ)
    existing = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = lib_dir + (":" + existing if existing else "")

    cmd = [HARNESS,
           "--queries", queries_file,
           "--data-dir", data_dir,
           "--warmup", str(warmup),
           "--runs", str(runs)]
    if bitmap:
        cmd += ["--bitmap", "--meta", meta]

    log(f"[harness] LD_LIBRARY_PATH={lib_dir}  bitmap={bitmap}")
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              universal_newlines=True, timeout=DEFAULT_TIMEOUT,
                              env=env)
    except subprocess.TimeoutExpired as e:
        log(f"[harness ERROR] 超时: {e}")
        sys.exit(1)
    if proc.returncode != 0:
        log(f"[harness ERROR] 退出码 {proc.returncode}\n{proc.stderr}")
        sys.exit(1)
    if proc.stderr.strip():
        for line in proc.stderr.strip().splitlines():
            log("  " + line)
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        log(f"[harness ERROR] 解析 JSON 失败: {e}\nRAW:\n{proc.stdout}")
        sys.exit(1)


# ============================================================
# CSV / 画图
# ============================================================
def metric(res, qname):
    item = res.get(qname)
    if not item:
        return "", "", "", ""
    warmup = item["warmup"] if item.get("warmup") is not None else ""
    avg = item.get("avg") if item.get("avg") is not None else ""
    mn = min(item["runs"]) if item.get("runs") else ""
    mx = max(item["runs"]) if item.get("runs") else ""
    return warmup, avg, mn, mx


def is_number(value):
    return isinstance(value, (int, float))


def fmt_seconds(value):
    return f"{value:.4f}" if is_number(value) else ""


def fmt_speedup(base_avg, bitmap_avg):
    if is_number(base_avg) and is_number(bitmap_avg) and bitmap_avg > 0:
        return f"{base_avg / bitmap_avg:.3f}"
    return ""


def write_csv(official_res, project_baseline_res, project_bitmap_res, queries):
    rows = []
    for qnum in sorted(queries.keys()):
        qname = f"Q{qnum:02d}"
        ob_w, ob_avg, ob_min, ob_max = metric(official_res, qname)
        pb_w, pb_avg, pb_min, pb_max = metric(project_baseline_res, qname)
        pm_w, pm_avg, pm_min, pm_max = metric(project_bitmap_res, qname)
        rows.append({
            "query": qname,
            "official_baseline_warmup_s": fmt_seconds(ob_w),
            "official_baseline_avg_s": fmt_seconds(ob_avg),
            "official_baseline_min_s": fmt_seconds(ob_min),
            "official_baseline_max_s": fmt_seconds(ob_max),
            "project_baseline_warmup_s": fmt_seconds(pb_w),
            "project_baseline_avg_s": fmt_seconds(pb_avg),
            "project_baseline_min_s": fmt_seconds(pb_min),
            "project_baseline_max_s": fmt_seconds(pb_max),
            "project_bitmap_warmup_s": fmt_seconds(pm_w),
            "project_bitmap_avg_s": fmt_seconds(pm_avg),
            "project_bitmap_min_s": fmt_seconds(pm_min),
            "project_bitmap_max_s": fmt_seconds(pm_max),
            "speedup_project_bitmap_over_official_baseline": fmt_speedup(ob_avg, pm_avg),
            "speedup_project_bitmap_over_project_baseline": fmt_speedup(pb_avg, pm_avg),
        })
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    log(f"[csv] 已写出 {CSV_PATH}")
    return rows


def plot(results_rows):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        log("[plot] matplotlib 不可用, 跳过画图 (CSV 已正常生成)")
        return

    labels = []
    official_baseline = []
    project_baseline = []
    project_bitmap = []
    speedup_vs_official = []
    speedup_vs_project = []
    for r in results_rows:
        labels.append(r["query"])
        ob = float(r["official_baseline_avg_s"]) if r["official_baseline_avg_s"] else None
        pb = float(r["project_baseline_avg_s"]) if r["project_baseline_avg_s"] else None
        pm = float(r["project_bitmap_avg_s"]) if r["project_bitmap_avg_s"] else None
        official_baseline.append(ob)
        project_baseline.append(pb)
        project_bitmap.append(pm)
        speedup_vs_official.append(ob / pm if (ob and pm and pm > 0) else 0.0)
        speedup_vs_project.append(pb / pm if (pb and pm and pm > 0) else 0.0)

    x = list(range(len(labels)))
    w = 0.26

    fig, ax = plt.subplots(figsize=(18, 7))
    ax.bar([i - w for i in x],
           [v if v is not None else np.nan for v in official_baseline],
           width=w, label="official_baseline (fixed CSV)", color="#4C72B0")
    ax.bar(x,
           [v if v is not None else np.nan for v in project_baseline],
           width=w, label="project_baseline (fixed CSV)", color="#55A868")
    ax.bar([i + w for i in x],
           [v if v is not None else np.nan for v in project_bitmap],
           width=w, label="project_bitmap (re-measured)", color="#DD8452")
    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Avg execution time (s, log scale)")
    ax.set_title("TPC-H SF=5 — official_baseline vs project_baseline vs project_bitmap")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(CHART_PATH, dpi=120)
    plt.close(fig)
    log(f"[plot] 已保存三柱状图 {CHART_PATH}")

    fig, ax = plt.subplots(figsize=(18, 7))
    sw = 0.38
    bars1 = ax.bar([i - sw / 2 for i in x], speedup_vs_official,
                   width=sw, label="official_baseline / project_bitmap", color="#4C72B0")
    bars2 = ax.bar([i + sw / 2 for i in x], speedup_vs_project,
                   width=sw, label="project_baseline / project_bitmap", color="#55A868")
    ax.axhline(1.0, color="black", linestyle="--", linewidth=1)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Speedup (baseline avg / project_bitmap avg)")
    ax.set_title("project_bitmap speedup over official_baseline and project_baseline")
    for bars in (bars1, bars2):
        for b in bars:
            s = b.get_height()
            if s > 0:
                ax.text(b.get_x() + b.get_width() / 2, s + 0.02, f"{s:.2f}",
                        ha="center", va="bottom", fontsize=6, rotation=90)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(SPEEDUP_PATH, dpi=120)
    plt.close(fig)
    log(f"[plot] 已保存两组加速比图 {SPEEDUP_PATH}")


# ============================================================
# 主流程
# ============================================================
def main():
    global DEFAULT_RUNS, DEFAULT_WARMUP, DEFAULT_TIMEOUT, DATA_DIR, META_PATH
    ap = argparse.ArgumentParser(
        description="TPC-H 22 e2e: baseline=固定CSV, project_bitmap=SDK harness")
    ap.add_argument("--data-dir", default=DATA_DIR)
    ap.add_argument("--project-cli", default=PROJECT_CLI)
    ap.add_argument("--harness", default=HARNESS)
    ap.add_argument("--baselines-csv", default=BASELINES_CSV,
                    help="固定 official_baseline/project_baseline 数据的 CSV 文件路径")
    ap.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    ap.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--queries", default=None, help="只跑指定查询, 如 1,5,9")
    ap.add_argument("--refresh-queries", action="store_true")
    args = ap.parse_args()

    DEFAULT_RUNS = args.runs
    DEFAULT_WARMUP = args.warmup
    DEFAULT_TIMEOUT = args.timeout
    DATA_DIR = args.data_dir
    META_PATH = os.path.join(DATA_DIR, "bitmap_join_meta.json")

    os.makedirs(OUT_DIR, exist_ok=True)
    check_data_dir(DATA_DIR)

    if not os.path.exists(args.harness):
        log(f"[ERROR] harness 不存在: {args.harness}, 请先运行 build_harness.sh")
        sys.exit(1)
    if not os.path.exists(args.project_cli):
        log(f"[ERROR] 项目 CLI 不存在: {args.project_cli}")
        sys.exit(1)
    if not os.path.exists(PROJECT_LIB):
        log(f"[ERROR] 项目 libduckdb.so 不存在: {PROJECT_LIB}")
        sys.exit(1)

    queries_subset = set(range(1, 23))
    if args.queries:
        queries_subset = set(int(x) for x in args.queries.split(",") if x.strip())

    # 1) 查询文本
    all_queries = extract_queries(args.project_cli, refresh=args.refresh_queries)
    queries = {q: all_queries[q] for q in all_queries if q in queries_subset}
    write_queries_file(queries, QUERIES_RUN)

    # 2) 从 CSV 加载固定 baseline 数据
    official_res, project_baseline_res = load_baselines_from_csv(args.baselines_csv)

    # 3) 跑 project_bitmap 测试 (SDK harness)
    log("\n" + "=" * 64)
    log("配置 C: project_bitmap — 本项目 libduckdb.so + open_bitmap_join=true")
    log("=" * 64)
    project_bitmap_res = run_harness(
        lib_dir=os.path.dirname(PROJECT_LIB),
        data_dir=DATA_DIR, meta=META_PATH, bitmap=True,
        warmup=DEFAULT_WARMUP, runs=DEFAULT_RUNS, queries_file=QUERIES_RUN)

    # 4) 汇总 / CSV / 画图
    log("\n" + "=" * 64)
    log("汇总")
    log("=" * 64)
    rows = write_csv(official_res, project_baseline_res, project_bitmap_res, queries)
    plot(rows)

    ob_rows = [r for r in rows if r["official_baseline_avg_s"]]
    pb_rows = [r for r in rows if r["project_baseline_avg_s"]]
    pm_rows = [r for r in rows if r["project_bitmap_avg_s"]]
    common_official = [r for r in rows if r["official_baseline_avg_s"] and r["project_bitmap_avg_s"]]
    common_project = [r for r in rows if r["project_baseline_avg_s"] and r["project_bitmap_avg_s"]]
    ob_total = sum(float(r["official_baseline_avg_s"]) for r in common_official)
    pb_total = sum(float(r["project_baseline_avg_s"]) for r in common_project)
    pm_total_official = sum(float(r["project_bitmap_avg_s"]) for r in common_official)
    pm_total_project = sum(float(r["project_bitmap_avg_s"]) for r in common_project)
    log(f"  已测量/加载: official_baseline {len(ob_rows)} 条, project_baseline {len(pb_rows)} 条, "
        f"project_bitmap {len(pm_rows)} 条 (共 {len(rows)} 条)")
    if ob_total and pm_total_official:
        log(f"  project_bitmap 相比 official_baseline 整体加速比: {ob_total / pm_total_official:.3f}x")
    if pb_total and pm_total_project:
        log(f"  project_bitmap 相比 project_baseline 整体加速比: {pb_total / pm_total_project:.3f}x")

    log("\n✅ 完成。产物:")
    log(f"   CSV : {CSV_PATH}")
    log(f"   图1 : {CHART_PATH}")
    log(f"   图2 : {SPEEDUP_PATH}")


if __name__ == "__main__":
    main()
