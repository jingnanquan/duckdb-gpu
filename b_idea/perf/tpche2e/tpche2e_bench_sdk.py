#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tpche2e_bench_sdk.py — 基于 DuckDB C SDK 的 TPC-H 22 查询端到端 (e2e) 性能对比

对比对象 (都在 *同一份* SF5 parquet 数据集上运行标准 22 条 TPC-H 查询):
  (A) official_baseline: 官方 DuckDB 1.5.1 libduckdb.so (无 open_bitmap_join 配置项)
  (B) project_baseline : 本项目 build/release/src/libduckdb.so + open_bitmap_join=false
  (C) project_bitmap   : 本项目 build/release/src/libduckdb.so + bitmap_join_load + open_bitmap_join=true

每条查询: DEFAULT_WARMUP 次预热 (只暖 OS/内部缓存, 不计入) + DEFAULT_RUNS 次取均值。
产物:
  - tpche2e_results.csv
  - tpche2e_chart.png      三组耗时柱状图
  - tpche2e_speedup.png    project_bitmap 相比 official_baseline / project_baseline 的加速比

用法:
  cd /home/featurize/workspace/duckdb-cuda
  bash b_idea/perf/tpche2e/build_harness.sh
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk.py
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk.py --queries 1,6,9,18
"""

import argparse
import csv
import json
import os
import shutil
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

OUT_DIR = os.path.join(BASE_DIR, "b_idea", "perf", "tpche2e")
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
def run_harness(lib_dir, data_dir, meta, bitmap, set_bitmap_off, warmup, runs, queries_file):
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
    elif set_bitmap_off:
        cmd += ["--set-bitmap-off"]

    log(f"[harness] LD_LIBRARY_PATH={lib_dir}  bitmap={bitmap}  set_bitmap_off={set_bitmap_off}")
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
           width=w, label="official_baseline (DuckDB 1.5.1)", color="#4C72B0")
    ax.bar(x,
           [v if v is not None else np.nan for v in project_baseline],
           width=w, label="project_baseline (open_bitmap_join=false)", color="#55A868")
    ax.bar([i + w for i in x],
           [v if v is not None else np.nan for v in project_bitmap],
           width=w, label="project_bitmap (open_bitmap_join=true)", color="#DD8452")
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
        description="TPC-H 22 e2e 对比: official_baseline / project_baseline / project_bitmap")
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
    ap.add_argument("--official-only", action="store_true",
                    help="只跑 official_baseline")
    ap.add_argument("--project-baseline-only", action="store_true",
                    help="只跑 project_baseline")
    ap.add_argument("--bitmap-only", action="store_true",
                    help="只跑 project_bitmap")
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
    if not os.path.exists(PROJECT_LIB):
        log(f"[ERROR] 项目 libduckdb.so 不存在: {PROJECT_LIB}")
        sys.exit(1)

    only_flags = [args.official_only, args.project_baseline_only, args.bitmap_only]
    if sum(1 for flag in only_flags if flag) > 1:
        log("[ERROR] --official-only / --project-baseline-only / --bitmap-only 只能指定一个")
        sys.exit(1)
    run_official = not any(only_flags) or args.official_only
    run_project_baseline = not any(only_flags) or args.project_baseline_only
    run_project_bitmap = not any(only_flags) or args.bitmap_only

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
    if run_official:
        if os.path.exists(args.official_lib):
            official_lib = args.official_lib
            log(f"[official] 使用已有 lib: {official_lib}")
        elif args.skip_download:
            log("[ERROR] 官方 lib 不存在且 --skip-download, 无法继续")
            sys.exit(1)
        else:
            official_lib = ensure_official_lib(force=args.force_download)

    # 3) 跑批 (同一 harness, 切换 .so / --bitmap)
    official_res, project_baseline_res, project_bitmap_res = {}, {}, {}
    if run_official:
        log("\n" + "=" * 64)
        log(f"配置 A: official_baseline — 官方 DuckDB {args.official_version} (无项目 bitmap 配置项)")
        log("=" * 64)
        official_res = run_harness(
            lib_dir=os.path.dirname(official_lib),
            data_dir=DATA_DIR, meta=META_PATH, bitmap=False, set_bitmap_off=False,
            warmup=DEFAULT_WARMUP, runs=DEFAULT_RUNS, queries_file=QUERIES_RUN)
    if run_project_baseline:
        log("\n" + "=" * 64)
        log("配置 B: project_baseline — 本项目 libduckdb.so + open_bitmap_join=false")
        log("=" * 64)
        project_baseline_res = run_harness(
            lib_dir=os.path.dirname(PROJECT_LIB),
            data_dir=DATA_DIR, meta=META_PATH, bitmap=False, set_bitmap_off=True,
            warmup=DEFAULT_WARMUP, runs=DEFAULT_RUNS, queries_file=QUERIES_RUN)
    if run_project_bitmap:
        log("\n" + "=" * 64)
        log("配置 C: project_bitmap — 本项目 libduckdb.so + open_bitmap_join=true")
        log("=" * 64)
        project_bitmap_res = run_harness(
            lib_dir=os.path.dirname(PROJECT_LIB),
            data_dir=DATA_DIR, meta=META_PATH, bitmap=True, set_bitmap_off=False,
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
    log(f"  已测量: official_baseline {len(ob_rows)} 条, project_baseline {len(pb_rows)} 条, "
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
