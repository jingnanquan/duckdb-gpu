#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tpche2e_bench_sdk.py — 基于 DuckDB C SDK 的 TPC-H 22 查询端到端 (e2e) 性能对比

对比对象 (都在 *同一份* SF5 parquet 数据集上运行标准 22 条 TPC-H 查询):
  (A) official_baseline: 官方 DuckDB 1.5.1 libduckdb.so (无 open_bitmap_join 配置项)
  (B) project_baseline : 本项目 build/release/src/libduckdb.so + open_bitmap_join=false
  (C) project_bitmap   : 本项目 build/release/src/libduckdb.so + bitmap_join_load + open_bitmap_join=true

每条查询: DEFAULT_WARMUP 次预热 (只暖 OS/内部缓存, 不计入) + DEFAULT_RUNS 次取均值。

加载模式 (--load-mode, 用于与 "数据已驻留内存" 的执行引擎对齐口径):
  view (默认): CREATE OR REPLACE VIEW ... AS SELECT * FROM read_parquet(...)
      只是 parquet 的映射, 每条查询都要重新解码 parquet。
  mem        : CREATE OR REPLACE TABLE ... AS SELECT * FROM read_parquet(...)
      把 8 张表 CTAS 物化进 DuckDB 自身列存 (纯内存库), 之后查询不再触碰 parquet。
两种模式下建表/物化的耗时都单独统计为 <group>_load_ms, 不计入查询时间。
DB 生命周期 (--db-mode): auto (view->per-query, mem->reuse) / per-query / reuse。

产物 (mem 模式下文件名带 _mem 后缀, 避免覆盖 view 模式结果):
  - tpche2e_results[_mem].csv
  - tpche2e_chart[_mem].png      三组耗时柱状图
  - tpche2e_speedup[_mem].png    project_bitmap 相比 official_baseline / project_baseline 的加速比

用法:
  cd <base_dir>        # gpu_paths.conf 里的 base_dir (python3 gpu_paths.py 可查看)
  bash b_idea/perf/tpche2e/build_harness.sh
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk.py
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk.py --load-mode mem
  python3 b_idea/perf/tpche2e/tpche2e_bench_sdk.py --queries 1,6,9,18

官方 libduckdb.so: 首次运行会下载到 bin/official/, 之后复用。
GitHub Releases 直连在国内常被限速/卡死, 脚本会自动回退到镜像; 如需手动指定:
  --official-url <url>   或   环境变量 DUCKDB_OFFICIAL_LIB_URL=<url>
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile

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
QUERIES_RUN = os.path.join(OUT_DIR, "queries_run.sql")   # 喂给 harness 的查询文件

# 官方 baseline: 官方 libduckdb.so (1.5.1)
OFFICIAL_VERSION = "1.5.1"
OFFICIAL_URL = ("https://github.com/duckdb/duckdb/releases/download/"
                f"v{OFFICIAL_VERSION}/libduckdb-linux-amd64.zip")
OFFICIAL_LIB_DIR = os.path.join(OUT_DIR, "bin", "official")
OFFICIAL_LIB = os.path.join(OFFICIAL_LIB_DIR, "libduckdb.so")

# GitHub Releases 直连常常被限速甚至卡死 (表现为进度停在几 MB 不动)。
# 这里准备若干镜像前缀, 下载失败/卡住时按顺序自动回退。
# 优先级: --official-url > 环境变量 DUCKDB_OFFICIAL_LIB_URL > 直连 > 镜像。
GITHUB_MIRRORS = ["https://ghfast.top/", "https://gh-proxy.com/"]
DOWNLOAD_TIMEOUT = 20       # 单次 urlopen 超时 (秒); 用于检测"卡住完全无数据"
SLOW_GRACE_SECS = 20        # 连接建立后的宽限期, 期间不判慢
MIN_SPEED_BPS = 200 * 1024  # 宽限期后平均速度低于它就放弃该源, 换下一个
MIN_ZIP_BYTES = 5 * 1024 * 1024   # 小于它认为不是完整的 libduckdb zip

# 计时参数
DEFAULT_RUNS = 3
DEFAULT_WARMUP = 1
DEFAULT_TIMEOUT = 600  # 单条查询超时 (秒), harness 内部不设超时, 这里仅保护 subprocess

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
def official_url_candidates():
    """按优先级返回待尝试的下载 URL 列表 (去重保序)。"""
    cands = []
    env_url = os.environ.get("DUCKDB_OFFICIAL_LIB_URL", "").strip()
    if env_url:
        cands.append(env_url)
    cands.append(OFFICIAL_URL)
    cands += [m + OFFICIAL_URL for m in GITHUB_MIRRORS]
    seen, out = set(), []
    for u in cands:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def download_file(url, dst, timeout):
    """分块下载并打印进度。

    用 urlopen(timeout=...) 而不是 urlretrieve: 后者没有超时参数, 一旦
    GitHub 直连被限速就会永久卡住 (实测停在 1MB 不动且不报错)。
    """
    req = urllib.request.Request(url, headers={"User-Agent": "curl/8.0"})
    t0 = time.time()
    last_report = 0.0
    with urllib.request.urlopen(req, timeout=timeout) as resp, open(dst, "wb") as out:
        total = int(resp.headers.get("Content-Length") or 0)
        done = 0
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            out.write(chunk)
            done += len(chunk)
            now = time.time()
            elapsed = now - t0
            # 速度守卫: 光靠 socket timeout 只能防"完全卡死", 防不住"龟速"
            # (实测 GitHub 直连有时能通但只有 60KB/s, 40MB 要 11 分钟)。
            if elapsed > SLOW_GRACE_SECS and done / elapsed < MIN_SPEED_BPS:
                raise RuntimeError(
                    f"速度过低 ({done / elapsed / 1024:.0f} KB/s < "
                    f"{MIN_SPEED_BPS / 1024:.0f} KB/s), 放弃该源")
            if now - last_report >= 5:
                last_report = now
                speed = done / max(elapsed, 1e-9) / 1e6
                size = f"{done/1e6:.1f}/{total/1e6:.1f} MB" if total else f"{done/1e6:.1f} MB"
                log(f"           {size}  ({speed:.2f} MB/s)")
    return done


def ensure_official_lib(force=False, url=None):
    if os.path.exists(OFFICIAL_LIB) and not force:
        log(f"[official] 已存在, 跳过下载: {OFFICIAL_LIB}")
        return OFFICIAL_LIB
    os.makedirs(OFFICIAL_LIB_DIR, exist_ok=True)
    zip_path = os.path.join(OFFICIAL_LIB_DIR, "libduckdb-linux-amd64.zip")

    cands = official_url_candidates()
    if url:
        cands = [url] + [c for c in cands if c != url]

    last_err = None
    for i, u in enumerate(cands, 1):
        log(f"[official] 下载源 {i}/{len(cands)}: {u}")
        try:
            download_file(u, zip_path, DOWNLOAD_TIMEOUT)
        except Exception as e:  # noqa: BLE001
            last_err = e
            log(f"[official]   失败: {e} —— 换下一个源")
            continue
        if os.path.getsize(zip_path) < MIN_ZIP_BYTES:
            last_err = f"只下载了 {os.path.getsize(zip_path)} 字节, 不是完整 zip"
            log(f"[official]   {last_err} —— 换下一个源")
            continue
        log(f"[official]   下载完成: {os.path.getsize(zip_path)/1e6:.1f} MB")
        break
    else:
        log(f"[official ERROR] 所有下载源均失败: {last_err}")
        log(f"       可手动下载后放到: {OFFICIAL_LIB}")
        log("       或用 --official-url <url> / 环境变量 DUCKDB_OFFICIAL_LIB_URL 指定可用源")
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
def run_harness(lib_dir, data_dir, meta, bitmap, set_bitmap_off, warmup, runs,
                queries_file, load_mode="view", db_mode="auto", memory_limit=""):
    env = dict(os.environ)
    # 让 LD_LIBRARY_PATH 优先于二进制内建的 RUNPATH, 从而切换到指定 .so
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
    elif set_bitmap_off:
        cmd += ["--set-bitmap-off"]

    log(f"[harness] LD_LIBRARY_PATH={lib_dir}  bitmap={bitmap}  "
        f"set_bitmap_off={set_bitmap_off}  load_mode={load_mode}  db_mode={db_mode}")
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
    """该组在该查询上的建表/物化耗时 (ms)。

    per-query 模式下每条查询各自建表 -> 用 load_by_query[qname];
    reuse 模式下只建一次 -> 用 load_total。
    """
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
                  log_scale, fontsize=6):
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
        labels=["official_baseline (DuckDB 1.5.1)",
                "project_baseline (open_bitmap_join=false)",
                "project_bitmap (open_bitmap_join=true)"],
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
        group_width=0.74, gap_ratio=0.14, log_scale=False, fontsize=7)
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
        description="TPC-H 22 e2e 对比: official_baseline / project_baseline / project_bitmap")
    ap.add_argument("--data-dir", default=DATA_DIR)
    ap.add_argument("--project-cli", default=PROJECT_CLI)
    ap.add_argument("--official-lib", default=OFFICIAL_LIB,
                    help="官方 libduckdb.so 路径 (缺省自动下载)")
    ap.add_argument("--official-version", default=OFFICIAL_VERSION)
    ap.add_argument("--official-url", default=None,
                    help="指定官方 libduckdb zip 的完整 URL (优先于内置源与镜像); "
                         "也可用环境变量 DUCKDB_OFFICIAL_LIB_URL")
    ap.add_argument("--harness", default=HARNESS)
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
    set_output_paths(args.load_mode)
    db_mode = resolve_db_mode(args.load_mode, args.db_mode)

    log(f"[config] load_mode={args.load_mode}  db_mode={args.db_mode} -> {db_mode}"
        + (f"  memory_limit={args.memory_limit}" if args.memory_limit else ""))

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
            official_lib = ensure_official_lib(force=args.force_download,
                                               url=args.official_url)

    # 3) 跑批 (同一 harness, 切换 .so / --bitmap)
    official_res, project_baseline_res, project_bitmap_res = {}, {}, {}
    if run_official:
        log("\n" + "=" * 64)
        log(f"配置 A: official_baseline — 官方 DuckDB {args.official_version} (无项目 bitmap 配置项)")
        log("=" * 64)
        official_res = run_harness(
            lib_dir=os.path.dirname(official_lib),
            data_dir=DATA_DIR, meta=META_PATH, bitmap=False, set_bitmap_off=False,
            warmup=DEFAULT_WARMUP, runs=DEFAULT_RUNS, queries_file=QUERIES_RUN,
            load_mode=args.load_mode, db_mode=db_mode,
            memory_limit=args.memory_limit)
    if run_project_baseline:
        log("\n" + "=" * 64)
        log("配置 B: project_baseline — 本项目 libduckdb.so + open_bitmap_join=false")
        log("=" * 64)
        project_baseline_res = run_harness(
            lib_dir=os.path.dirname(PROJECT_LIB),
            data_dir=DATA_DIR, meta=META_PATH, bitmap=False, set_bitmap_off=True,
            warmup=DEFAULT_WARMUP, runs=DEFAULT_RUNS, queries_file=QUERIES_RUN,
            load_mode=args.load_mode, db_mode=db_mode,
            memory_limit=args.memory_limit)
    if run_project_bitmap:
        log("\n" + "=" * 64)
        log("配置 C: project_bitmap — 本项目 libduckdb.so + open_bitmap_join=true")
        log("=" * 64)
        project_bitmap_res = run_harness(
            lib_dir=os.path.dirname(PROJECT_LIB),
            data_dir=DATA_DIR, meta=META_PATH, bitmap=True, set_bitmap_off=False,
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
    log(f"  已测量: official_baseline {len(ob_rows)} 条, project_baseline {len(pb_rows)} 条, "
        f"project_bitmap {len(pm_rows)} 条 (共 {len(rows)} 条)")
    for name, res in (("official_baseline", official_res),
                      ("project_baseline", project_baseline_res),
                      ("project_bitmap", project_bitmap_res)):
        if not res:
            continue
        per_query = res.get("load_by_query") or {}
        total = res.get("load_total")
        if per_query:
            vals = [v for v in per_query.values() if v is not None]
            if vals:
                log(f"  {name} 建表耗时: 每条查询各自建表, "
                    f"avg={sum(vals) / len(vals):.1f} ms, 合计={sum(vals):.1f} ms")
        elif total is not None:
            log(f"  {name} 建表耗时: 全程一次, {total:.1f} ms")
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
