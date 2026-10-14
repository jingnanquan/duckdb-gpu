#!/usr/bin/env bash
# 生成 TPC-H SF=5 原始数据集 (b_idea/test_readme.md 第 2 节)
set -euo pipefail

BASE_DIR="$(python3 /home/yx/code/bishe/duckdb-gpu/gpu_paths.py)"
cd "$BASE_DIR"

echo "[gen] base_dir = $BASE_DIR"
echo "[gen] 开始 $(date '+%F %T')"

mkdir -p data/tpch_sf5

./build/release/duckdb <<'EOF'
LOAD tpch;
CALL dbgen(sf=5);
EXPORT DATABASE 'data/tpch_sf5/' (FORMAT PARQUET);
EOF

echo "[gen] 结束 $(date '+%F %T')"
ls -lh data/tpch_sf5/
