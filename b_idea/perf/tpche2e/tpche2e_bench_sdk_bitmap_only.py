#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tpche2e_bench_sdk_bitmap_only.py — 只重测 project_bitmap, 两个 baseline 从固定 CSV 读取

与 tpche2e_bench_sdk.py 的区别:
  - 不再下载/运行官方 libduckdb.so
  - official_baseline / project_baseline 的 warmup/avg/min/max 从 CSV 读取
  - 只运行本项目 project_bitmap: libduckdb.so + bitmap_join_load + open_bitmap_join=true

产物 (mem 模式下文件名带 _mem 后缀, 与 tpche2e_bench_sdk.py 保持一致):
  - tpche2e_results[_mem].csv
  - tpche2e_chart[_mem].png      三组耗时柱状图
  - tpche2e_speedup[_mem].png    project_bitmap 相比 official_baseline / project_baseline 的加速比

注意: --load-mode 必须与生成 baseline CSV 时使用的模式一致, 否则会把 view 模式的
baseline 与 mem 模式的 bitmap 结果混在一起。

用法:
  cd <base_dir>        # gpu_paths.conf 里的 base_dir (python3 gpu_paths.py 可查看)
  bash b_idea/perf/tpche2e/build_harness.sh
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk_bitmap_only.py
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk_bitmap_only.py --load-mode mem
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
# 仓库根目录: 由仓库根目录 gpu_paths.conf 的 base_dir 决定 (换机器只改那一行)
_gp_root = os.path.dirname(os.path.abspath(__file__))
while not os.path.isfile(os.path.join(_gp_root, "gpu_paths.py")) and os.path.dirname(_gp_root) != _gp_root:
    _gp_root = os.path.dirname(_gp_root)
sys.path.insert(0, _gp_root)
from gpu_paths import BASE_DIR  # noqa: E402
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

LOAD_MODES = ("view", "mem")
DB_MODES = ("auto", "per-query", "reuse")

# 产物路径: view -> 原名, 其它 load_mode -> 带 _<mode> 后缀, 避免互相覆盖
CSV_PATH = os.path.join(OUT_DIR, "tpche2e_results.csv")
CHART_PATH = os.path.join(OUT_DIR, "tpche2e_chart.png")
SPEEDUP_PATH = os.path.join(OUT_DIR, "tpche2e_speedup.png")

TPCH_TABLES = ["region", "nation", "customer", "orders",
               "part", "partsupp", "supplier", "lineitem"]


def set_output_paths(load_mode):
    """按 load_mode 重设产物路径 (view 用原名, mem 用 _mem 后缀)。"""
    global CSV_PATH, CHART_PATH, SPEEDUP_PATH
    suffix = "" if load_mode == "view" else "_" + load_mode
    CSV_PATH = os.path.join(OUT_DIR, f"tpche2e_results{suffix}.csv")
    CHART_PATH = os.path.join(OUT_DIR, f"tpche2e_chart{suffix}.png")
    SPEEDUP_PATH = os.path.join(OUT_DIR, f"tpche2e_speedup{suffix}.png")


def resolve_db_mode(load_mode, db_mode):
    """auto -> mem 用 reuse (物化只做一次), view 用 per-query。与 harness 一致。"""
    if not db_mode or db_mode == "auto":
        return "reuse" if load_mode == "mem" else "per-query"
    return db_mode


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
    load = value(row, *(f"{name}_load_ms" for name in names))
    runs = [v for v in (mn, mx) if v is not None]
    return {"warmup": warmup, "avg": avg, "runs": runs, "load": load, "error": None}


def load_baselines_from_csv(csv_path):
    """读取全量结果 CSV, 返回 official_baseline_res / project_baseline_res。

    返回结构与 harness 结果一致: {"queries": {...}, "load_total": None,
    "load_by_query": {qname: load_ms}} —— CSV 里每行自带 load_ms, 因此都放进
    load_by_query。
    """
    if not os.path.exists(csv_path):
        log(f"[ERROR] baseline CSV 不存在: {csv_path}")
        log("       请先运行 tpche2e_bench_sdk.py 生成 official_baseline / project_baseline。")
        sys.exit(1)
    official_res = {"queries": {}, "load_total": None, "load_by_query": {}}
    project_baseline_res = {"queries": {}, "load_total": None, "load_by_query": {}}
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qname = row["query"]
            for res, prefix, legacy in ((official_res, "official_baseline", "official"),
                                        (project_baseline_res, "project_baseline", None)):
                item = csv_metric(row, prefix, legacy_prefix=legacy)
                load = item.pop("load")
                res["queries"][qname] = item
                if load is not None:
                    res["load_by_query"][qname] = load
    log(f"[baseline] 从 {csv_path} 加载了 {len(official_res['queries'])} 条 "
        f"official_baseline 固定数据")
    log(f"[baseline] 从 {csv_path} 加载了 {len(project_baseline_res['queries'])} 条 "
        f"project_baseline 固定数据")
    return official_res, project_baseline_res


# ============================================================
# 运行 harness (bitmap)
# ============================================================
def run_harness(lib_dir, data_dir, meta, bitmap, warmup, runs, queries_file,
                load_mode="view", db_mode="auto", memory_limit=""):
    env = dict(os.environ)
    existing = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = lib_dir + (":" + existing if existing else "")

    cmd = [HARNESS,
           "--queries", queries_file,
           "--data-dir", data_dir,
           "--warmup", str(warmup),
           "--runs", str(runs),
           "--load-mode", load_mode,
           "--db-mode", db_mode]
    if memory_limit:
        cmd += ["--memory-limit", memory_limit]
    if bitmap:
        cmd += ["--bitmap", "--meta", meta]

    log(f"[harness] LD_LIBRARY_PATH={lib_dir}  bitmap={bitmap}  "
        f"load_mode={load_mode}  db_mode={db_mode}")
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
        parsed = json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        log(f"[harness ERROR] 解析 JSON 失败: {e}\nRAW:\n{proc.stdout}")
        sys.exit(1)
    # 统一成 {"queries": {...}, "load_total": x|None, "load_by_query": {...}}
    load = parsed.get("load") or {}
    return {
        "queries": parsed.get("queries") or {},
        "load_total": load.get("total_ms"),
        "load_by_query": load.get("by_query") or {},
    }


# ============================================================
# CSV / 画图
# ============================================================
def metric(res, qname):
    item = (res or {}).get("queries", {}).get(qname)
    if not item:
        return "", "", "", ""
    warmup = item["warmup"] if item.get("warmup") is not None else ""
    avg = item.get("avg") if item.get("avg") is not None else ""
    mn = min(item["runs"]) if item.get("runs") else ""
    mx = max(item["runs"]) if item.get("runs") else ""
    return warmup, avg, mn, mx


def load_of(res, qname):
    """该组在该查询上的建表/物化耗时 (ms)。"""
    if not res:
        return None
    per_query = (res.get("load_by_query") or {}).get(qname)
    if per_query is not None:
        return per_query
    return res.get("load_total")


def is_number(value):
    return isinstance(value, (int, float))


def fmt_seconds(value):
    return f"{value:.4f}" if is_number(value) else ""


def fmt_ms(value):
    return f"{value:.1f}" if is_number(value) else ""


def fmt_speedup(base_avg, bitmap_avg):
    if is_number(base_avg) and is_number(bitmap_avg) and bitmap_avg > 0:
        return f"{base_avg / bitmap_avg:.3f}"
    return ""


def write_csv(official_res, project_baseline_res, project_bitmap_res, queries,
              load_mode="view", db_mode="auto"):
    rows = []
    for qnum in sorted(queries.keys()):
        qname = f"Q{qnum:02d}"
        ob_w, ob_avg, ob_min, ob_max = metric(official_res, qname)
        pb_w, pb_avg, pb_min, pb_max = metric(project_baseline_res, qname)
        pm_w, pm_avg, pm_min, pm_max = metric(project_bitmap_res, qname)
        rows.append({
            "query": qname,
            "load_mode": load_mode,
            "db_mode": db_mode,
            "official_baseline_load_ms": fmt_ms(load_of(official_res, qname)),
            "official_baseline_warmup_s": fmt_seconds(ob_w),
            "official_baseline_avg_s": fmt_seconds(ob_avg),
            "official_baseline_min_s": fmt_seconds(ob_min),
            "official_baseline_max_s": fmt_seconds(ob_max),
            "project_baseline_load_ms": fmt_ms(load_of(project_baseline_res, qname)),
            "project_baseline_warmup_s": fmt_seconds(pb_w),
            "project_baseline_avg_s": fmt_seconds(pb_avg),
            "project_baseline_min_s": fmt_seconds(pb_min),
            "project_baseline_max_s": fmt_seconds(pb_max),
            "project_bitmap_load_ms": fmt_ms(load_of(project_bitmap_res, qname)),
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


def _import_plot_libs():
    """把 plt / np 提升为模块级, 供下面的辅助函数使用。"""
    global plt, np
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np


def _fmt_bar(value):
    """柱顶数字: 按量级自适应, 尽量短以免柱子间互相压字。"""
    if value >= 100:
        return f"{value:.0f}"
    if value >= 10:
        return f"{value:.1f}"
    if value >= 1:
        return f"{value:.2f}"
    return f"{value:.3f}"


def _grouped_bars(ax, x, series, colors, labels, group_width, gap_ratio,
                  log_scale, fontsize=5):
    """把 n 组柱子均匀排在每个 x 的 group_width 内, 组间留 gap, 并在柱顶标数字。"""
    n = len(series)
    slot = group_width / n
    width = slot * (1.0 - gap_ratio)
    left = -group_width / 2.0 + slot / 2.0
    peak = max(series_flat(series))   # 线性轴下标签留白用
    all_bars = []
    for j, (values, color, label) in enumerate(zip(series, colors, labels)):
        xs = [xi + left + j * slot for xi in x]
        bars = ax.bar(xs, [v if v is not None else np.nan for v in values],
                      width=width, label=label, color=color)
        for b, v in zip(bars, values):
            if v is None or not np.isfinite(v) or v <= 0:
                continue
            # log 轴用乘法留白, 线性轴用加法
            y = v * 1.08 if log_scale else v + peak * 0.012
            ax.text(b.get_x() + b.get_width() / 2.0, y, _fmt_bar(v),
                    ha="center", va="bottom", fontsize=fontsize, rotation=90)
        all_bars.append(bars)
    return all_bars


def series_flat(series):
    return [v for values in series for v in values
            if v is not None and np.isfinite(v) and v > 0] or [1.0]


def _headroom(ax, values, log_scale):
    """给柱顶数字留出空间, 否则旋转 90 度的文字会被顶裁掉。"""
    vs = [v for v in values if v is not None and np.isfinite(v) and v > 0]
    if not vs:
        return
    lo, _ = ax.get_ylim()
    if log_scale:
        ax.set_ylim(bottom=lo, top=max(vs) * 2.6)
    else:
        ax.set_ylim(bottom=0, top=max(vs) * 1.25)


def plot(results_rows):
    try:
        _import_plot_libs()
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

    # ---- 图 1: 三组耗时 (log 轴) ----
    fig, ax = plt.subplots(figsize=(20, 7.5))
    series = [official_baseline, project_baseline, project_bitmap]
    _grouped_bars(
        ax, x, series,
        colors=["#4C72B0", "#55A868", "#DD8452"],
        labels=["official_baseline (fixed CSV)",
                "project_baseline (fixed CSV)",
                "project_bitmap (re-measured)"],
        group_width=0.8, gap_ratio=0.16, log_scale=True)
    ax.set_yscale("log")
    _headroom(ax, series_flat(series), log_scale=True)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Avg execution time (s, log scale)")
    ax.set_title("TPC-H SF=5 — official_baseline vs project_baseline vs project_bitmap")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(CHART_PATH, dpi=130)
    plt.close(fig)
    log(f"[plot] 已保存三柱状图 {CHART_PATH}")

    # ---- 图 2: 两组加速比 (线性轴) ----
    fig, ax = plt.subplots(figsize=(20, 7.5))
    series = [speedup_vs_official, speedup_vs_project]
    _grouped_bars(
        ax, x, series,
        colors=["#4C72B0", "#55A868"],
        labels=["official_baseline / project_bitmap",
                "project_baseline / project_bitmap"],
        group_width=0.74, gap_ratio=0.14, log_scale=False, fontsize=6)
    ax.axhline(1.0, color="black", linestyle="--", linewidth=1)
    _headroom(ax, series_flat(series), log_scale=False)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Speedup (baseline avg / project_bitmap avg)")
    ax.set_title("project_bitmap speedup over official_baseline and project_baseline")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(SPEEDUP_PATH, dpi=130)
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
    ap.add_argument("--baselines-csv", default=None,
                    help="固定 official_baseline/project_baseline 数据的 CSV 文件路径 "
                         "(缺省按 --load-mode 自动选择 tpche2e_results[_mem].csv)")
    ap.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    ap.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--queries", default=None, help="只跑指定查询, 如 1,5,9")
    ap.add_argument("--load-mode", default="view", choices=list(LOAD_MODES),
                    help="view=parquet 映射(每次解码); mem=CTAS 物化进内存列存(只建表一次)")
    ap.add_argument("--db-mode", default="auto", choices=list(DB_MODES),
                    help="DB 生命周期: auto(view->per-query, mem->reuse); "
                         "per-query=每条查询独立实例; reuse=全程一个实例")
    ap.add_argument("--memory-limit", default="",
                    help="可选, 如 24GB; 为空则沿用 DuckDB 默认")
    ap.add_argument("--refresh-queries", action="store_true")
    args = ap.parse_args()

    DEFAULT_RUNS = args.runs
    DEFAULT_WARMUP = args.warmup
    DEFAULT_TIMEOUT = args.timeout
    DATA_DIR = args.data_dir
    META_PATH = os.path.join(DATA_DIR, "bitmap_join_meta.json")
    set_output_paths(args.load_mode)
    baselines_csv = args.baselines_csv or CSV_PATH
    db_mode = resolve_db_mode(args.load_mode, args.db_mode)

    log(f"[config] load_mode={args.load_mode}  db_mode={args.db_mode} -> {db_mode}"
        + (f"  memory_limit={args.memory_limit}" if args.memory_limit else ""))

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
    official_res, project_baseline_res = load_baselines_from_csv(baselines_csv)

    # 3) 跑 project_bitmap 测试 (SDK harness)
    log("\n" + "=" * 64)
    log("配置 C: project_bitmap — 本项目 libduckdb.so + open_bitmap_join=true")
    log("=" * 64)
    project_bitmap_res = run_harness(
        lib_dir=os.path.dirname(PROJECT_LIB),
        data_dir=DATA_DIR, meta=META_PATH, bitmap=True,
        warmup=DEFAULT_WARMUP, runs=DEFAULT_RUNS, queries_file=QUERIES_RUN,
        load_mode=args.load_mode, db_mode=db_mode,
        memory_limit=args.memory_limit)

    # 4) 汇总 / CSV / 画图
    log("\n" + "=" * 64)
    log("汇总")
    log("=" * 64)
    rows = write_csv(official_res, project_baseline_res, project_bitmap_res, queries,
                     load_mode=args.load_mode, db_mode=db_mode)
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
