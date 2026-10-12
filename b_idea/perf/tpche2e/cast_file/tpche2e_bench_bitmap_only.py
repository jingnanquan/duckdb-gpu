#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tpche2e_bench_bitmap_only.py — 只测试 bitmap, official 数据固定采用 tpche2e_official.csv

与 tpche2e_bench.py 的区别:
  - 不再下载/运行官方 DuckDB CLI
  - 官方 (official) 的 warmup/avg/min/max 数据固定从 tpche2e_official.csv 读取
  - 只运行本项目的 bitmap join 测试
  - 对比图表 (柱状图 + 加速比) 和结果 CSV 照常生成

输出:
  - tpche2e_results.csv          每条查询的对比 (官方固定 + bitmap 实测)
  - tpche2e_chart.png            分组柱状图
  - tpche2e_speedup.png          加速比图

用法示例:
  cd /home/featurize/workspace/duckdb-cuda
  python3 b_idea/perf/tpche2e/tpche2e_bench_bitmap_only.py
  python3 b_idea/perf/tpche2e/tpche2e_bench_bitmap_only.py --runs 5
  python3 b_idea/perf/tpche2e/tpche2e_bench_bitmap_only.py --queries 1,6,9,18
"""

import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import time

# ============================================================
# 路径 / 版本配置
# ============================================================
BASE_DIR = "/home/featurize/workspace/duckdb-cuda"
DATA_DIR = os.path.join(BASE_DIR, "data", "tpch_sf5_bitmap")
META_PATH = os.path.join(DATA_DIR, "bitmap_join_meta.json")
PROJECT_CLI = os.path.join(BASE_DIR, "build", "release", "duckdb")

OUT_DIR = os.path.join(BASE_DIR, "b_idea", "perf", "tpche2e")
QUERIES_CACHE = os.path.join(OUT_DIR, "tpch_queries.json")

# 固定 official 数据的 CSV
OFFICIAL_CSV = os.path.join(OUT_DIR, "tpche2e_official.csv")

# 输出产物 (与原始脚本保持一致)
CSV_PATH = os.path.join(OUT_DIR, "tpche2e_results.csv")
CHART_PATH = os.path.join(OUT_DIR, "tpche2e_chart.png")
SPEEDUP_PATH = os.path.join(OUT_DIR, "tpche2e_speedup.png")

TPCH_TABLES = ["region", "nation", "customer", "orders",
               "part", "partsupp", "supplier", "lineitem"]

# 计时参数
DEFAULT_RUNS = 3
DEFAULT_WARMUP = 1
DEFAULT_TIMEOUT = 600


# ============================================================
# 工具函数
# ============================================================
def log(msg):
    print(msg, flush=True)


def check_data_dir(data_dir):
    missing = []
    for t in TPCH_TABLES:
        p = os.path.join(data_dir, t + ".parquet")
        if not os.path.exists(p):
            missing.append(t)
    if missing:
        log(f"[ERROR] 数据集目录 {data_dir} 缺少 parquet 文件: {missing}")
        sys.exit(1)
    if not os.path.exists(META_PATH if data_dir == DATA_DIR else
                          os.path.join(data_dir, "bitmap_join_meta.json")):
        log(f"[WARN] 未找到 bitmap_join_meta.json, bitmap 配置将无法加载元数据。")


def build_setup_sql(data_dir):
    lines = []
    for t in TPCH_TABLES:
        lines.append(
            f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{data_dir}/{t}.parquet');"
        )
    return "\n".join(lines)


def build_run_sql(data_dir, query_sql, use_bitmap):
    setup = build_setup_sql(data_dir)
    flags = ""
    if use_bitmap:
        flags = (
            f"PRAGMA bitmap_join_load('{META_PATH}');\n"
            "SET open_bitmap_join=true;\n"
        )
    q = query_sql.strip()
    if q.endswith(";"):
        q = q[:-1]
    return (
        "SET autoinstall_known_extensions=false;\n"
        "SET autoload_known_extensions=false;\n"
        f"{setup}\n"
        f"{flags}"
        f"{q};\n"
    )


def run_single(cli, sql, timeout):
    start = time.perf_counter()
    try:
        proc = subprocess.run(
            [cli, "-c", sql],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return None, f"timeout>{timeout}s"
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        return None, (proc.stderr or "").strip().splitlines()[-1][:300] if proc.stderr else "unknown error"
    return elapsed, None


# ============================================================
# 查询文本提取
# ============================================================
def extract_queries(project_cli, refresh=False):
    if os.path.exists(QUERIES_CACHE) and not refresh:
        with open(QUERIES_CACHE) as f:
            data = json.load(f)
        if len(data) == 22:
            log(f"[queries] 使用缓存: {QUERIES_CACHE}")
            return {int(k): v for k, v in data.items()}
    log("[queries] 通过本项目 CLI 的 tpch_queries() 提取 22 条查询文本 ...")
    sql = "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr;"
    proc = subprocess.run(
        [project_cli, "-json", "-c", sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, timeout=120,
    )
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


# ============================================================
# 从 CSV 加载固定 official 数据
# ============================================================
def load_official_from_csv(csv_path):
    """
    读取 tpche2e_official.csv, 返回 dict:
      qnum (int) -> {
        "warmup": float|None,
        "avg": float|None,
        "min": float|None,
        "max": float|None,
        "runs": [min, max]  (保持与 run_config 输出格式兼容, 用于写 CSV 时取 min/max)
      }
    """
    if not os.path.exists(csv_path):
        log(f"[ERROR] official CSV 不存在: {csv_path}")
        sys.exit(1)

    official_res = {}
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qname = row["query"]  # Q01, Q02, ...
            qnum = int(qname.lstrip("Q"))
            o_w = float(row["official_warmup_s"]) if row["official_warmup_s"] else None
            o_avg = float(row["official_avg_s"]) if row["official_avg_s"] else None
            o_min = float(row["official_min_s"]) if row["official_min_s"] else None
            o_max = float(row["official_max_s"]) if row["official_max_s"] else None
            # runs 用 [min, max] 近似 (仅用于 CSV 输出 min/max 列)
            runs = [o_min, o_max] if o_min is not None and o_max is not None else []
            official_res[qnum] = {
                "warmup": o_w,
                "avg": o_avg,
                "runs": runs,
                "error": None,
            }

    log(f"[official] 从 {csv_path} 加载了 {len(official_res)} 条固定数据")
    return official_res


# ============================================================
# 单配置跑批 (bitmap)
# ============================================================
def run_config(cli, queries, use_bitmap, label, queries_subset, timeout):
    results = {}
    for qnum in sorted(queries.keys()):
        if qnum not in queries_subset:
            continue
        qname = f"Q{qnum:02d}"
        sql = build_run_sql(DATA_DIR, queries[qnum], use_bitmap)

        # warmup
        w_t, w_err = None, None
        for _ in range(DEFAULT_WARMUP):
            w_t, w_err = run_single(cli, sql, timeout)
        if w_err:
            log(f"  {label} {qname}: WARMUP ERROR -> {w_err}")
            results[qnum] = {"warmup": None, "runs": [], "error": w_err}
            continue

        # measured runs
        runs = []
        err = None
        for i in range(DEFAULT_RUNS):
            t, e = run_single(cli, sql, timeout)
            if e:
                err = e
                break
            runs.append(t)
        if err:
            log(f"  {label} {qname}: RUN ERROR -> {err}")
            results[qnum] = {"warmup": w_t, "runs": [], "error": err}
            continue

        avg = statistics.fmean(runs)
        log(f"  {label} {qname}: warmup={w_t:.3f}s  avg={avg:.3f}s "
            f"(min={min(runs):.3f} max={max(runs):.3f})")
        results[qnum] = {"warmup": w_t, "runs": runs, "avg": avg, "error": None}
    return results


# ============================================================
# CSV / 画图
# ============================================================
def write_csv(official_res, bitmap_res, queries):
    rows = []
    for qnum in sorted(queries.keys()):
        qname = f"Q{qnum:02d}"
        o = official_res.get(qnum)
        b = bitmap_res.get(qnum)
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
        log("[plot] matplotlib 不可用, 尝试 pip 安装 ...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "matplotlib"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            import numpy as np
        except ImportError:
            log("[plot] 仍无法导入 matplotlib, 跳过画图 (CSV 已正常生成)")
            return

    labels, official, bitmap, speedup = [], [], [], []
    for r in results_rows:
        labels.append(r["query"])
        oa = float(r["official_avg_s"]) if r["official_avg_s"] else None
        ba = float(r["bitmap_avg_s"]) if r["bitmap_avg_s"] else None
        official.append(oa)
        bitmap.append(ba)
        if oa and ba and ba > 0:
            speedup.append(oa / ba)
        else:
            speedup.append(0.0)

    x = list(range(len(labels)))
    w = 0.4

    # ---- 图1: 分组柱状图 ----
    fig, ax = plt.subplots(figsize=(16, 7))
    ax.bar([i - w / 2 for i in x],
           [v if v is not None else np.nan for v in official],
           width=w, label="Official DuckDB 1.5.x (fixed CSV)", color="#4C72B0")
    ax.bar([i + w / 2 for i in x],
           [v if v is not None else np.nan for v in bitmap],
           width=w, label="Project + open_bitmap_join", color="#DD8452")
    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Avg execution time (s, log scale)")
    ax.set_title("TPC-H SF=5 — 22 queries e2e (official=fixed CSV, bitmap=measured)")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(CHART_PATH, dpi=120)
    plt.close(fig)
    log(f"[plot] 已保存柱状图 {CHART_PATH}")

    # ---- 图2: 加速比 ----
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
        description="TPC-H 22 e2e 性能对比: official 数据固定 (来自CSV), 仅测 bitmap"
    )
    ap.add_argument("--data-dir", default=DATA_DIR, help="SF5 parquet 目录 (含 8 张表)")
    ap.add_argument("--project-cli", default=PROJECT_CLI, help="本项目构建的 duckdb CLI 路径")
    ap.add_argument("--official-csv", default=OFFICIAL_CSV,
                    help="固定 official 数据的 CSV 文件路径")
    ap.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    ap.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--queries", default=None, help="只跑指定查询, 逗号分隔, 如 1,5,9")
    ap.add_argument("--refresh-queries", action="store_true",
                    help="强制重新从 tpch_queries() 提取查询文本")
    args = ap.parse_args()

    DEFAULT_RUNS = args.runs  # pyright: ignore[reportConstantRedefinition]
    DEFAULT_WARMUP = args.warmup  # pyright: ignore[reportConstantRedefinition]
    DEFAULT_TIMEOUT = args.timeout  # pyright: ignore[reportConstantRedefinition]
    DATA_DIR = args.data_dir  # pyright: ignore[reportConstantRedefinition]
    META_PATH = os.path.join(DATA_DIR, "bitmap_join_meta.json")  # pyright: ignore[reportConstantRedefinition]

    os.makedirs(OUT_DIR, exist_ok=True)
    check_data_dir(DATA_DIR)

    queries_subset = set(range(1, 23))
    if args.queries:
        queries_subset = set(int(x) for x in args.queries.split(",") if x.strip())

    # 1) 查询文本
    if not os.path.exists(args.project_cli):
        log(f"[ERROR] 项目 CLI 不存在: {args.project_cli}")
        sys.exit(1)
    queries = extract_queries(args.project_cli, refresh=args.refresh_queries)

    # 2) 从 CSV 加载固定 official 数据
    official_res = load_official_from_csv(args.official_csv)

    # 3) 跑 bitmap 测试
    log("\n" + "=" * 64)
    log("Bitmap 测试: 本项目 DuckDB + open_bitmap_join=true")
    log("=" * 64)
    bitmap_res = run_config(args.project_cli, queries, use_bitmap=True,
                            label="bitmap", queries_subset=queries_subset,
                            timeout=DEFAULT_TIMEOUT)

    # 4) 汇总 / CSV / 画图
    log("\n" + "=" * 64)
    log("汇总")
    log("=" * 64)
    rows = write_csv(official_res, bitmap_res, queries)
    plot(rows)

    # 打印总览
    o_rows = [r for r in rows if r["official_avg_s"]]
    b_rows = [r for r in rows if r["bitmap_avg_s"]]
    common = [r for r in rows if r["official_avg_s"] and r["bitmap_avg_s"]]
    o_total = sum(float(r["official_avg_s"]) for r in common)
    b_total = sum(float(r["bitmap_avg_s"]) for r in common)
    log(f"  已测量: official (固定CSV) {len(o_rows)} 条, bitmap {len(b_rows)} 条 (共 {len(rows)} 条查询)")
    if o_total and b_total:
        log(f"  两端都成功 {len(common)} 条: official avg 总耗时 {o_total:.2f}s, "
            f"bitmap avg 总耗时 {b_total:.2f}s")
        log(f"  整体加速比 (official/bitmap, 仅共同成功项): {o_total / b_total:.3f}x")

    log("\n✅ 完成。产物:")
    log(f"   CSV : {CSV_PATH}")
    log(f"   图1 : {CHART_PATH}")
    log(f"   图2 : {SPEEDUP_PATH}")


if __name__ == "__main__":
    main()
