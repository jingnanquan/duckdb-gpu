#!/usr/bin/env bash
# ============================================================
# gpu_paths.sh — duckdb-gpu 统一路径配置加载器 (Shell 侧)
# ============================================================
# 所有脚本里的绝对路径都由仓库根目录的 gpu_paths.conf 决定:
#
#     base_dir=/home/yx/code/bishe/duckdb-gpu
#
# 换机器时只改 gpu_paths.conf 那一行即可。
#
# 优先级:
#   1. 环境变量 DUCKDB_GPU_BASE_DIR
#   2. gpu_paths.conf 里的 base_dir
#   3. 本文件所在目录 (即仓库根目录)
#
# 用法 (仓库任意子目录下的脚本里 source 本文件):
#
#     _gp_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#     while [ ! -f "$_gp_root/gpu_paths.sh" ] && [ "$_gp_root" != "/" ]; do
#         _gp_root="$(dirname "$_gp_root")"
#     done
#     . "$_gp_root/gpu_paths.sh"     # 之后即可使用 $BASE_DIR
#
# 也可单独执行查看: bash gpu_paths.sh
# ============================================================

_gpu_paths_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GPU_PATHS_CONF="${GPU_PATHS_CONF:-$_gpu_paths_dir/gpu_paths.conf}"

GPU_BASE_DIR="${DUCKDB_GPU_BASE_DIR:-}"
if [ -z "${GPU_BASE_DIR}" ] && [ -f "${GPU_PATHS_CONF}" ]; then
    GPU_BASE_DIR="$(sed -n 's/^[[:space:]]*base_dir[[:space:]]*=[[:space:]]*//p' \
                    "${GPU_PATHS_CONF}" | tail -n 1)"
    # 去掉可选的引号
    GPU_BASE_DIR="${GPU_BASE_DIR%\"}"
    GPU_BASE_DIR="${GPU_BASE_DIR#\"}"
    GPU_BASE_DIR="${GPU_BASE_DIR%\'}"
    GPU_BASE_DIR="${GPU_BASE_DIR#\'}"
fi
GPU_BASE_DIR="${GPU_BASE_DIR:-$_gpu_paths_dir}"
GPU_BASE_DIR="${GPU_BASE_DIR%/}"

# 对外暴露: $BASE_DIR (各脚本沿用这个名字)
BASE_DIR="${GPU_BASE_DIR}"
DUCKDB_GPU_BASE_DIR="${GPU_BASE_DIR}"
export DUCKDB_GPU_BASE_DIR

unset _gpu_paths_dir

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    echo "${BASE_DIR}"
fi
