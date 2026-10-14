#!/usr/bin/env bash
# 编译 tpch_bench_harness.cpp, 动态链接 libduckdb.so。
#
# 用 --enable-new-dtags (RUNPATH) 写入默认 rpath 指向项目 libduckdb.so,
# 但 RUNPATH 优先级低于 LD_LIBRARY_PATH, 因此运行时可用
#   LD_LIBRARY_PATH=<官方lib目录> ./tpch_bench_harness ...
# 来切换到官方 libduckdb.so (同一二进制, 两种 baseline)。
set -euo pipefail

# 仓库根目录: 由仓库根目录 gpu_paths.conf 的 base_dir 决定 (换机器只改那一行)
_gp_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
while [ ! -f "$_gp_root/gpu_paths.sh" ] && [ "$_gp_root" != "/" ]; do
    _gp_root="$(dirname "$_gp_root")"
done
. "$_gp_root/gpu_paths.sh"     # 提供 $BASE_DIR

SRC_DIR="$BASE_DIR/b_idea/perf/tpche2e"
INC_DIR="$BASE_DIR/src/include"
LIB_DIR="$BASE_DIR/build/release/src"   # 默认链接项目 (bitmap) libduckdb.so
OUT="$SRC_DIR/tpch_bench_harness"

if [ ! -f "$LIB_DIR/libduckdb.so" ]; then
    echo "[build] 找不到 $LIB_DIR/libduckdb.so, 请先构建项目" >&2
    exit 1
fi

g++ -std=c++11 -O2 \
    -I"$INC_DIR" \
    "$SRC_DIR/tpch_bench_harness.cpp" \
    -L"$LIB_DIR" -lduckdb \
    -Wl,--enable-new-dtags -Wl,-rpath,"$LIB_DIR" \
    -o "$OUT"

echo "[build] 已生成: $OUT"
echo "[build] 默认 rpath 指向项目 libduckdb.so ($LIB_DIR)"
echo "[build] 运行官方版时请设置 LD_LIBRARY_PATH=<官方 lib 目录>"
