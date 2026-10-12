#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tpche2e_bench_sdk_bitmap_only.py — 只测 bitmap (SDK harness), official 用固定 CSV

与 tpche2e_bench_sdk.py 的区别:
  - 不再下载/运行官方 libduckdb.so
  - 官方 (official) 的 warmup/avg/min/max 固定从 tpche2e_official.csv 读取
  - 只运行本项目的 bitmap join 测试, 且用 C++ SDK harness (单进程/单 connection,
    计时紧贴 Query(), 精度远高于旧脚本的 subprocess wall-clock)

产物 (字段与 tpche2e_bench_sdk.py / 旧脚本兼容):
  - tpche2e_results.csv
  - tpche2e_chart.png / tpche2e_speedup.png

用法:
  cd /home/featurize/workspace/duckdb-cuda
  bash b_idea/perf/tpche2e/build_harness.sh          # 先编译 harness
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk_bitmap_only.py
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk_bitmap_only.py --runs 5
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk_bitmap_only.py --queries 1,6,9,18
"""

import argparse
import csv
import json
import os
import statistics
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

# 固定 official 数据的 CSV (由 tpche2e_bench_sdk.py 全量跑出)
OFFICIAL_CSV = os.path.join(OUT_DIR, "tpch_official.csv")

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
# 从 CSV 加载固定 official 数据
# ============================================================
def load_official_from_csv(csv_path):
    """读取 tpche2e_official.csv, 返回 dict: qname(str "Q01") -> {...}。

    与 write_csv 的键 (字符串 qname) 对齐, 避免旧脚本里 int/str 键不一致导致
    official 列全空的问题。
    """
    if not os.path.exists(csv_path):
        log(f"[ERROR] official CSV 不存在: {csv_path}")
        log("       请先运行 tpche2e_bench_sdk.py 生成 (含 official 全量结果)。")
        sys.exit(1)
    official_res = {}
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qname = row["query"]  # "Q01"
            o_w = float(row["official_warmup_s"]) if row["official_warmup_s"] else None
            o_avg = float(row["official_avg_s"]) if row["official_avg_s"] else None
            o_min = float(row["official_min_s"]) if row["official_min_s"] else None
            o_max = float(row["official_max_s"]) if row["official_max_s"] else None
            runs = [o_min, o_max] if (o_min is not None and o_max is not None) else []
            official_res[qname] = {
                "warmup": o_w, "avg": o_avg, "runs": runs, "error": None,
            }
    log(f"[official] 从 {csv_path} 加载了 {len(official_res)} 条固定数据")
    return official_res


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
def write_csv(official_res, bitmap_res, queries):
    rows = []
    for qnum in sorted(queries.keys()):
        qname = f"Q{qnum:02d}"
        o = official_res.get(qname)
        b = bitmap_res.get(qname)
        o_w = o["warmup"] if o and o.get("warmup") is not None else ""
        o_avg = o.get("avg") if o and o.get("avg") is not None else ""
        o_min = min(o["runs"]) if o and o.get("runs") else ""
        o_max = max(o["runs"]) if o and o.get("runs") else ""
        b_w = b["warmup"] if b and b.get("warmup") is not None else ""
        b_avg = b.get("avg") if b and b.get("avg") is not None else ""
        b_min = min(b["runs"]) if b and b.get("runs") else ""
        b_max = max(b["runs"]) if b and b.get("runs") else ""
        speedup = ""
        if isinstance(o_avg, float) and isinstance(b_avg, float) and b_avg > 0:
            speedup = f"{o_avg / b_avg:.3f}"
        rows.append({
            "query": qname,
            "official_warmup_s": f"{o_w:.4f}" if isinstance(o_w, float) else "",
            "official_avg_s": f"{o_avg:.4f}" if isinstance(o_avg, float) else "",
            "official_min_s": f"{o_min:.4f}" if isinstance(o_min, float) else "",
            "official_max_s": f"{o_max:.4f}" if isinstance(o_max, float) else "",
            "bitmap_warmup_s": f"{b_w:.4f}" if isinstance(b_w, float) else "",
            "bitmap_avg_s": f"{b_avg:.4f}" if isinstance(b_avg, float) else "",
            "bitmap_min_s": f"{b_min:.4f}" if isinstance(b_min, float) else "",
            "bitmap_max_s": f"{b_max:.4f}" if isinstance(b_max, float) else "",
            "speedup_official_over_bitmap": speedup,
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

    labels, official, bitmap, speedup = [], [], [], []
    for r in results_rows:
        labels.append(r["query"])
        oa = float(r["official_avg_s"]) if r["official_avg_s"] else None
        ba = float(r["bitmap_avg_s"]) if r["bitmap_avg_s"] else None
        official.append(oa)
        bitmap.append(ba)
        speedup.append(oa / ba if (oa and ba and ba > 0) else 0.0)

    x = list(range(len(labels)))
    w = 0.4

    fig, ax = plt.subplots(figsize=(16, 7))
    ax.bar([i - w / 2 for i in x],
           [v if v is not None else np.nan for v in official],
           width=w, label="Official DuckDB 1.5.1 (fixed CSV)", color="#4C72B0")
    ax.bar([i + w / 2 for i in x],
           [v if v is not None else np.nan for v in bitmap],
           width=w, label="Project + open_bitmap_join", color="#DD8452")
    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Avg execution time (s, log scale)")
    ax.set_title("TPC-H SF=5 — 22 queries e2e [bitmap=SDK harness, official=fixed CSV]")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(CHART_PATH, dpi=120)
    plt.close(fig)
    log(f"[plot] 已保存柱状图 {CHART_PATH}")

    fig, ax = plt.subplots(figsize=(16, 7))
    colors = ["#55A868" if s >= 1.0 else "#C44E52" for s in speedup]
    bars = ax.bar(x, speedup, color=colors)
    ax.axhline(1.0, color="black", linestyle="--", linewidth=1)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Speedup = official_avg / bitmap_avg")
    ax.set_title("Bitmap-Join speedup per query (>=1 means project+bitmap is faster)")
    for b, s in zip(bars, speedup):
        if s > 0:
            ax.text(b.get_x() + b.get_width() / 2, s + 0.02, f"{s:.2f}",
                    ha="center", va="bottom", fontsize=7)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(SPEEDUP_PATH, dpi=120)
    plt.close(fig)
    log(f"[plot] 已保存加速比图 {SPEEDUP_PATH}")


# ============================================================
# 主流程
# ============================================================
def main():
    global DEFAULT_RUNS, DEFAULT_WARMUP, DEFAULT_TIMEOUT, DATA_DIR, META_PATH
    ap = argparse.ArgumentParser(
        description="TPC-H 22 e2e: official=固定CSV, bitmap=SDK harness")
    ap.add_argument("--data-dir", default=DATA_DIR)
    ap.add_argument("--project-cli", default=PROJECT_CLI)
    ap.add_argument("--harness", default=HARNESS)
    ap.add_argument("--official-csv", default=OFFICIAL_CSV,
                    help="固定 official 数据的 CSV 文件路径")
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

    queries_subset = set(range(1, 23))
    if args.queries:
        queries_subset = set(int(x) for x in args.queries.split(",") if x.strip())

    # 1) 查询文本
    all_queries = extract_queries(args.project_cli, refresh=args.refresh_queries)
    queries = {q: all_queries[q] for q in all_queries if q in queries_subset}
    write_queries_file(queries, QUERIES_RUN)

    # 2) 从 CSV 加载固定 official 数据
    official_res = load_official_from_csv(args.official_csv)

    # 3) 跑 bitmap 测试 (SDK harness)
    log("\n" + "=" * 64)
    log("Bitmap 测试: 本项目 libduckdb.so + open_bitmap_join=true (SDK harness)")
    log("=" * 64)
    bitmap_res = run_harness(
        lib_dir=os.path.dirname(PROJECT_LIB),
        data_dir=DATA_DIR, meta=META_PATH, bitmap=True,
        warmup=DEFAULT_WARMUP, runs=DEFAULT_RUNS, queries_file=QUERIES_RUN)

    # 4) 汇总 / CSV / 画图
    log("\n" + "=" * 64)
    log("汇总")
    log("=" * 64)
    rows = write_csv(official_res, bitmap_res, queries)
    plot(rows)

    o_rows = [r for r in rows if r["official_avg_s"]]
    b_rows = [r for r in rows if r["bitmap_avg_s"]]
    common = [r for r in rows if r["official_avg_s"] and r["bitmap_avg_s"]]
    o_total = sum(float(r["official_avg_s"]) for r in common)
    b_total = sum(float(r["bitmap_avg_s"]) for r in common)
    log(f"  已测量: official (固定CSV) {len(o_rows)} 条, bitmap {len(b_rows)} 条 "
        f"(共 {len(rows)} 条)")
    if o_total and b_total:
        log(f"  两端都有 {len(common)} 条: official avg 总耗时 {o_total:.2f}s, "
            f"bitmap avg 总耗时 {b_total:.2f}s")
        log(f"  整体加速比 (官方/bitmap): {o_total / b_total:.3f}x")

    log("\n✅ 完成。产物:")
    log(f"   CSV : {CSV_PATH}")
    log(f"   图1 : {CHART_PATH}")
    log(f"   图2 : {SPEEDUP_PATH}")


if __name__ == "__main__":
    main()
