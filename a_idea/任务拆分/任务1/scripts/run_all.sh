#!/bin/bash
# ============================================================
# 任务1 总控执行脚本
# 按顺序执行 1.3 ~ 1.6 的所有子任务
#
# 用法:
#   cd /home/featurize/workspace/duckdb-cuda
#   bash a_idea/任务拆分/任务1/scripts/run_all.sh
#
# 或者分步执行:
#   bash a_idea/任务拆分/任务1/scripts/run_all.sh 1.3
#   bash a_idea/任务拆分/任务1/scripts/run_all.sh 1.4
#   bash a_idea/任务拆分/任务1/scripts/run_all.sh 1.5
#   bash a_idea/任务拆分/任务1/scripts/run_all.sh 1.6
# ============================================================

set -e

BASE_DIR="/home/featurize/workspace/duckdb-cuda"
TASK_DIR="${BASE_DIR}/a_idea/任务拆分/任务1"
SCRIPTS_DIR="${TASK_DIR}/scripts"
RESULTS_DIR="${TASK_DIR}/results"
DUCKDB_BIN="${BASE_DIR}/build/release/duckdb"

# venv 路径
VENV_DIR="${TASK_DIR}/.venv"
PYTHON_BIN=""

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ============================================================
# 创建并激活 Python venv
# ============================================================
setup_venv() {
    log_info "设置 Python 虚拟环境..."

    if [ ! -d "${VENV_DIR}" ]; then
        log_info "  创建 venv: ${VENV_DIR}"
        python3 -m venv "${VENV_DIR}"
    else
        log_info "  venv 已存在: ${VENV_DIR}"
    fi

    # 使用 venv 中的 python
    PYTHON_BIN="${VENV_DIR}/bin/python3"

    if [ ! -x "${PYTHON_BIN}" ]; then
        log_error "venv python 不可用: ${PYTHON_BIN}"
        exit 1
    fi

    # 升级 pip 并安装依赖
    "${VENV_DIR}/bin/pip" install --upgrade pip -q 2>/dev/null || true
    # 安装 matplotlib（高精度监控画图用）
    if ! "${PYTHON_BIN}" -c "import matplotlib" 2>/dev/null; then
        log_info "  安装 matplotlib..."
        "${VENV_DIR}/bin/pip" install matplotlib -q 2>/dev/null || log_warn "matplotlib 安装失败，将跳过画图"
    fi

    log_info "  ✅ Python venv: ${PYTHON_BIN} ($(${PYTHON_BIN} --version 2>&1))"
    echo ""
}

# ============================================================
# 前置检查
# ============================================================
check_prerequisites() {
    log_info "检查前置条件..."

    # 检查 DuckDB 二进制
    if [ ! -x "${DUCKDB_BIN}" ]; then
        log_error "DuckDB 二进制不存在或不可执行: ${DUCKDB_BIN}"
        log_error "请先编译 DuckDB (任务 1.1)"
        exit 1
    fi
    log_info "  ✅ DuckDB 二进制: ${DUCKDB_BIN}"

    # 检查数据集
    TPCH_COUNT=$(ls ${BASE_DIR}/data/tpch_sf5/*.parquet 2>/dev/null | wc -l)
    TPCDS_COUNT=$(ls ${BASE_DIR}/data/tpcds_sf5/*.parquet 2>/dev/null | wc -l)

    if [ "${TPCH_COUNT}" -lt 8 ]; then
        log_error "TPCH 数据集不完整 (${TPCH_COUNT}/8 个文件)"
        exit 1
    fi
    log_info "  ✅ TPCH SF=5 数据集: ${TPCH_COUNT} 个 parquet 文件"

    if [ "${TPCDS_COUNT}" -lt 24 ]; then
        log_error "TPCDS 数据集不完整 (${TPCDS_COUNT}/24 个文件)"
        exit 1
    fi
    log_info "  ✅ TPCDS SF=5 数据集: ${TPCDS_COUNT} 个 parquet 文件"

    # 检查 Python3
    if ! command -v python3 &> /dev/null; then
        log_error "python3 未安装"
        exit 1
    fi
    log_info "  ✅ Python3: $(python3 --version)"

    # 创建结果目录
    mkdir -p "${RESULTS_DIR}/profiles" "${RESULTS_DIR}/system_metrics" "${RESULTS_DIR}/plans"
    log_info "  ✅ 结果目录已创建"

    echo ""
}

# ============================================================
# 任务 1.3: 基线测试
# ============================================================
run_task_1_3() {
    log_info "=========================================="
    log_info "任务 1.3: 运行全部查询获取基线时间"
    log_info "=========================================="
    echo ""

    cd "${BASE_DIR}"
    ${PYTHON_BIN} "${SCRIPTS_DIR}/run_baseline.py"

    echo ""
    if [ -f "${RESULTS_DIR}/baseline_sf5.csv" ]; then
        log_info "✅ 任务 1.3 完成"
        log_info "   产出: ${RESULTS_DIR}/baseline_sf5.csv"
        log_info "   行数: $(wc -l < ${RESULTS_DIR}/baseline_sf5.csv)"
    else
        log_error "❌ 任务 1.3 失败: 未生成 baseline_sf5.csv"
        exit 1
    fi
    echo ""
}

# ============================================================
# 任务 1.4: 查询分类
# ============================================================
run_task_1_4() {
    log_info "=========================================="
    log_info "任务 1.4: 查询分类"
    log_info "=========================================="
    echo ""

    cd "${BASE_DIR}"
    ${PYTHON_BIN} "${SCRIPTS_DIR}/classify_queries.py"

    echo ""
    if [ -f "${RESULTS_DIR}/query_classification_sf5.json" ]; then
        log_info "✅ 任务 1.4 完成"
        log_info "   产出: ${RESULTS_DIR}/query_classification_sf5.json"
    else
        log_error "❌ 任务 1.4 失败: 未生成分类文件"
        exit 1
    fi
    echo ""
}

# ============================================================
# 任务 1.5: Profiling 分析
# ============================================================
run_task_1_5() {
    log_info "=========================================="
    log_info "任务 1.5: Profiling 分析"
    log_info "=========================================="
    echo ""

    cd "${BASE_DIR}"
    ${PYTHON_BIN} "${SCRIPTS_DIR}/run_profiling.py"

    echo ""
    if [ -f "${RESULTS_DIR}/profiles/profiling_summary.json" ]; then
        log_info "✅ 任务 1.5 完成"
        log_info "   产出: ${RESULTS_DIR}/profiles/profiling_summary.json"
        log_info "   系统指标: ${RESULTS_DIR}/system_metrics/"
    else
        log_error "❌ 任务 1.5 失败: 未生成 profiling 汇总"
        exit 1
    fi
    echo ""
}

# ============================================================
# 任务 1.6: 执行计划导出与注入验证
# ============================================================
run_task_1_6() {
    log_info "=========================================="
    log_info "任务 1.6: 执行计划导出与注入验证"
    log_info "=========================================="
    echo ""

    cd "${BASE_DIR}"
    ${PYTHON_BIN} "${SCRIPTS_DIR}/plan_export_inject.py"

    echo ""
    if [ -f "${RESULTS_DIR}/plans/injection_verification.json" ]; then
        log_info "✅ 任务 1.6 完成"
        log_info "   产出: ${RESULTS_DIR}/plans/"
        log_info "   报告: ${RESULTS_DIR}/plans/injection_feasibility_report.md"
    else
        log_error "❌ 任务 1.6 失败: 未生成验证结果"
        exit 1
    fi
    echo ""
}

# ============================================================
# 最终汇总
# ============================================================
print_summary() {
    log_info "=========================================="
    log_info "任务 1 执行完成 - 产出物汇总"
    log_info "=========================================="
    echo ""
    echo "  结果目录: ${RESULTS_DIR}/"
    echo ""
    echo "  📄 基线报告:     ${RESULTS_DIR}/baseline_sf5.csv"
    echo "  📄 查询分类:     ${RESULTS_DIR}/query_classification_sf5.json"
    echo "  📁 Profiling:    ${RESULTS_DIR}/profiles/"
    echo "  📁 系统指标:     ${RESULTS_DIR}/system_metrics/"
    echo "  📁 执行计划:     ${RESULTS_DIR}/plans/"
    echo "  📄 可行性报告:   ${RESULTS_DIR}/plans/injection_feasibility_report.md"
    echo ""

    # 列出所有产出文件
    log_info "所有产出文件:"
    find "${RESULTS_DIR}" -type f | sort | while read f; do
        size=$(ls -lh "$f" 2>/dev/null | awk '{print $5}')
        echo "    ${f} (${size})"
    done
    echo ""
}

# ============================================================
# 主入口
# ============================================================
main() {
    echo ""
    log_info "============================================"
    log_info "  任务拆分 1: 基线测试 + Profiling + 计划注入验证"
    log_info "============================================"
    echo ""

    check_prerequisites
    setup_venv

    STEP="${1:-all}"

    case "${STEP}" in
        "1.3")
            run_task_1_3
            ;;
        "1.4")
            run_task_1_4
            ;;
        "1.5")
            run_task_1_5
            ;;
        "1.6")
            run_task_1_6
            ;;
        "all")
            START_TIME=$(date +%s)

            run_task_1_3
            run_task_1_4
            run_task_1_5
            run_task_1_6

            END_TIME=$(date +%s)
            ELAPSED=$((END_TIME - START_TIME))

            print_summary
            log_info "总耗时: ${ELAPSED}s ($(( ELAPSED / 60 ))m $(( ELAPSED % 60 ))s)"
            ;;
        *)
            echo "用法: $0 [1.3|1.4|1.5|1.6|all]"
            echo ""
            echo "  1.3  - 运行基线测试"
            echo "  1.4  - 查询分类"
            echo "  1.5  - Profiling 分析"
            echo "  1.6  - 执行计划导出与注入验证"
            echo "  all  - 执行全部 (默认)"
            exit 1
            ;;
    esac
}

main "$@"
