#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tpche2e_bench_sdk.py — 基于 DuckDB C SDK 的 TPC-H 22 查询端到端 (e2e) 性能对比

对比对象 (都在 *同一份* SF5 parquet 数据集上运行标准 22 条 TPC-H 查询):
  (A) 官方 DuckDB 1.5.1 libduckdb.so   (baseline, 不开启 bitmap join)
  (B) 本项目 build/release/src/libduckdb.so + bitmap_join_load + open_bitmap_join=true

与旧脚本 (tpche2e_bench.py) 的关键区别 —— 解决你指出的问题:
  1) 旧脚本每次 run_single 都 fork 一个 subprocess 跑 CLI, 计时是 Python 侧
     perf_counter() 包住整个进程 (含 CLI 启动/库加载/退出), 且每条查询冷启动,
     avg 毫无意义、波动巨大。
  2) 新方案用 C++ harness (tpch_bench_harness) 链接 libduckdb.so, 在 *单个进程 /
     单个 connection* 内把 22 条查询各跑 warmup+measured 多轮 —— 没有 per-query
     冷启动, 计时用 std::chrono::steady_clock 紧贴 Query()+Materialize(),
     只统计查询引擎本身开销, 精度高、avg 有意义。
  3) 官方版与 bitmap 版用 *同一个* harness 二进制, 仅通过 LD_LIBRARY_PATH 切换
     .so、以及 --bitmap 开关决定 bitmap 配置。两遍在同一机器、同一进程模型下测量,
     消除了旧脚本 "bitmap 脚本用一份固定 CSV 对比、与当前环境不符" 导致的巨大偏差。

每条查询: DEFAULT_WARMUP 次预热 (只暖 OS/内部缓存, 不计入) + DEFAULT_RUNS 次取均值。
产物 (与旧脚本字段兼容, 可直接延续使用):
  - tpche2e_results.csv
  - tpche2e_chart.png / tpche2e_speedup.png

用法:
  cd /home/featurize/workspace/duckdb-cuda
  bash b_idea/perf/tpche2e/build_harness.sh          # 先编译 harness
  python3 b_idea/perf/tpche2e_origin/tpche2e_bench_sdk.py
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk.py --official-only
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk.py --bitmap-only --runs 5
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk.py --queries 1,6,9,18
"""

import argparse
import csv
import json
import os
import shutil
import statistics
import subprocess
import sys
import zipfile

# ============================================================
# 路径配置
# ============================================================
BASE_DIR = "/home/featurize/workspace/duckdb-cuda"
DATA_DIR = os.path.join(BASE_DIR, "data", "tpch_sf5_bitmap")
META_PATH = os.path.join(DATA_DIR, "bitmap_join_meta.json")
PROJECT_LIB = os.path.join(BASE_DIR, "build", "release", "src", "libduckdb.so")
PROJECT_CLI = os.path.join(BASE_DIR, "build", "release", "duckdb")

OUT_DIR = os.path.join(BASE_DIR, "b_idea", "perf", "tpche2e_origin")
HARNESS = os.path.join(OUT_DIR, "tpch_bench_harness")
QUERIES_CACHE = os.path.join(OUT_DIR, "tpch_queries.json")
QUERIES_RUN = os.path.join(OUT_DIR, "queries_run.sql")   # 喂给 harness 的查询文件

# 官方 baseline: 官方 libduckdb.so (1.5.1)
OFFICIAL_VERSION = "1.5.1"
OFFICIAL_URL = ("https://github.com/duckdb/duckdb/releases/download/"
                f"v{OFFICIAL_VERSION}/libduckdb-linux-amd64.zip")
OFFICIAL_LIB_DIR = os.path.join(OUT_DIR, "bin", "official")
OFFICIAL_LIB = os.path.join(OFFICIAL_LIB_DIR, "libduckdb.so")

# 计时参数
DEFAULT_RUNS = 3
DEFAULT_WARMUP = 1
DEFAULT_TIMEOUT = 600  # 单条查询超时 (秒), harness 内部不设超时, 这里仅保护 subprocess

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


# ============================================================
# 提取 22 条查询文本 (复用项目 CLI 的 tpch_queries())
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
    """写成 harness 可解析的格式: 每个查询前一行 '--QUERY Qnn', 之后为 SQL。"""
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
# 官方 libduckdb.so 获取
# ============================================================
def ensure_official_lib(force=False):
    if os.path.exists(OFFICIAL_LIB) and not force:
        log(f"[official] 已存在, 跳过下载: {OFFICIAL_LIB}")
        return OFFICIAL_LIB
    os.makedirs(OFFICIAL_LIB_DIR, exist_ok=True)
    zip_path = os.path.join(OFFICIAL_LIB_DIR, "libduckdb-linux-amd64.zip")
    log(f"[official] 下载 {OFFICIAL_URL}")
    try:
        import urllib.request
        def _hook(block_num, block_size, total):
            if total > 0 and block_num % 40 == 0:
                done = block_num * block_size
                log(f"           {done/1e6:.1f}/{total/1e6:.1f} MB")
        urllib.request.urlretrieve(OFFICIAL_URL, zip_path, _hook)
    except Exception as e:  # noqa: BLE001
        log(f"[ERROR] 下载官方 libduckdb 失败: {e}")
        sys.exit(1)
    log(f"[official] 解压 libduckdb.so -> {OFFICIAL_LIB_DIR}")
    with zipfile.ZipFile(zip_path) as z:
        # 资源里 libduckdb.so 通常在根, 也可能在 include/ 等; 只取 .so
        so_names = [n for n in z.namelist() if n.endswith("libduckdb.so")]
        if not so_names:
            log(f"[ERROR] zip 中未找到 libduckdb.so, 含: {z.namelist()[:10]}")
            sys.exit(1)
        with z.open(so_names[0]) as src, open(OFFICIAL_LIB, "wb") as dst:
            shutil.copyfileobj(src, dst)
    os.chmod(OFFICIAL_LIB, 0o755)
    os.remove(zip_path)
    log(f"[official] 就绪: {OFFICIAL_LIB}")
    return OFFICIAL_LIB


# ============================================================
# 运行 harness (单进程, 返回解析后的结果 dict)
# ============================================================
def run_harness(lib_dir, data_dir, meta, bitmap, warmup, runs, queries_file):
    env = dict(os.environ)
    # 让 LD_LIBRARY_PATH 优先于二进制内建的 RUNPATH, 从而切换到指定 .so
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
    # stderr 是日志, 原样打印
    if proc.stderr.strip():
        for line in proc.stderr.strip().splitlines():
            log("  " + line)
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        log(f"[harness ERROR] 解析 JSON 失败: {e}\nRAW:\n{proc.stdout}")
        sys.exit(1)


# ============================================================
# CSV / 画图 (字段与旧脚本兼容)
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
           width=w, label="Official DuckDB 1.5.1", color="#4C72B0")
    ax.bar([i + w / 2 for i in x],
           [v if v is not None else np.nan for v in bitmap],
           width=w, label="Project + open_bitmap_join", color="#DD8452")
    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Avg execution time (s, log scale)")
    ax.set_title("TPC-H SF=5 — 22 queries e2e [C++ SDK harness, single-process]")
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
        description="TPC-H 22 e2e 对比 (C++ SDK harness): 官方 1.5.1 vs 项目 + bitmap")
    ap.add_argument("--data-dir", default=DATA_DIR)
    ap.add_argument("--project-cli", default=PROJECT_CLI)
    ap.add_argument("--official-lib", default=OFFICIAL_LIB,
                    help="官方 libduckdb.so 路径 (缺省自动下载)")
    ap.add_argument("--official-version", default=OFFICIAL_VERSION)
    ap.add_argument("--harness", default=HARNESS)
    ap.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    ap.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--queries", default=None, help="只跑指定查询, 如 1,5,9")
    ap.add_argument("--official-only", action="store_true")
    ap.add_argument("--bitmap-only", action="store_true")
    ap.add_argument("--skip-download", action="store_true",
                    help="若官方 lib 不存在则直接报错, 不下载")
    ap.add_argument("--force-download", action="store_true")
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

    queries_subset = set(range(1, 23))
    if args.queries:
        queries_subset = set(int(x) for x in args.queries.split(",") if x.strip())

    # 1) 查询文本 (项目 CLI)
    if not os.path.exists(args.project_cli):
        log(f"[ERROR] 项目 CLI 不存在: {args.project_cli}")
        sys.exit(1)
    all_queries = extract_queries(args.project_cli, refresh=args.refresh_queries)
    queries = {q: all_queries[q] for q in all_queries if q in queries_subset}
    write_queries_file(queries, QUERIES_RUN)

    # 2) 官方 libduckdb.so
    official_lib = None
    if not args.bitmap_only:
        if os.path.exists(args.official_lib):
            official_lib = args.official_lib
            log(f"[official] 使用已有 lib: {official_lib}")
        elif args.skip_download:
            log("[ERROR] 官方 lib 不存在且 --skip-download, 无法继续")
            sys.exit(1)
        else:
            official_lib = ensure_official_lib(force=args.force_download)

    # 3) 跑批 (同一 harness, 切换 .so)
    official_res, bitmap_res = {}, {}
    if official_lib:
        log("\n" + "=" * 64)
        log(f"配置 A: 官方 DuckDB {args.official_version} (libduckdb.so, 无 bitmap)")
        log("=" * 64)
        official_res = run_harness(
            lib_dir=os.path.dirname(official_lib),
            data_dir=DATA_DIR, meta=META_PATH, bitmap=False,
            warmup=DEFAULT_WARMUP, runs=DEFAULT_RUNS, queries_file=QUERIES_RUN)
    if not args.official_only:
        log("\n" + "=" * 64)
        log("配置 B: 本项目 libduckdb.so + open_bitmap_join=true")
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
    log(f"  已测量: 官方 {len(o_rows)} 条, bitmap {len(b_rows)} 条 (共 {len(rows)} 条)")
    if o_total and b_total:
        log(f"  两端都成功 {len(common)} 条: 官方 avg 总耗时 {o_total:.2f}s, "
            f"bitmap avg 总耗时 {b_total:.2f}s")
        log(f"  整体加速比 (官方/bitmap): {o_total / b_total:.3f}x")

    log("\n✅ 完成。产物:")
    log(f"   CSV : {CSV_PATH}")
    log(f"   图1 : {CHART_PATH}")
    log(f"   图2 : {SPEEDUP_PATH}")


if __name__ == "__main__":
    main()
