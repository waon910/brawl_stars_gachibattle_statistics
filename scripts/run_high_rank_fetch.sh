#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# 設定
# =============================================================================
SCRIPT_NAME="brawl_stars_high_rank_fetch"
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
LOG_DIR="${BASE_DIR}/data/logs"
PID_FILE="${BASE_DIR}/.${SCRIPT_NAME}.pid"
PIPELINE_PID_FILE="${BASE_DIR}/.brawl_stars_pipeline.pid"
ENV_FILE="${BASE_DIR}/config/settings.env"
LOCAL_ENV_FILE="${BASE_DIR}/.env.local"
PYTHON_BIN="${BASE_DIR}/venv/bin/python"
LOG_FILE="${LOG_DIR}/$(date '+%Y%m%d%H%M')_high_rank.log"

# 2時間おきに上位帯のみ取得する設定
FETCH_ACQ_CYCLE_HOURS=1
MIN_CURRENT_RANK=18
MIN_HIGHEST_RANK=21

START_TIME=0

# =============================================================================
# 初期設定
# =============================================================================
load_env_files() {
    if [[ -f "$ENV_FILE" ]]; then
        # shellcheck disable=SC1090
        source "$ENV_FILE"
    fi
    if [[ -f "$LOCAL_ENV_FILE" ]]; then
        # shellcheck disable=SC1090
        source "$LOCAL_ENV_FILE"
    fi
}

setup() {
    mkdir -p "$LOG_DIR"
    exec > >(tee -a "$LOG_FILE") 2>&1
}

# =============================================================================
# ログ関数
# =============================================================================
log() {
    local level="$1"; shift
    local timestamp
    timestamp=$(TZ=Asia/Tokyo date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $*"
}

log_info() { log "INFO" "$@"; }
log_warn() { log "WARN" "$@"; }
log_error() { log "ERROR" "$@"; }

# =============================================================================
# 重複実行チェック
# =============================================================================
check_duplicate() {
    if [[ -f "$PID_FILE" ]]; then
        local old_pid
        old_pid=$(cat "$PID_FILE" 2>/dev/null || echo "")
        if [[ -n "$old_pid" ]] && ps -p "$old_pid" > /dev/null 2>&1; then
            log_warn "スクリプトが既に実行中です (PID: $old_pid)"
            trap - EXIT
            exit 1
        else
            log_info "古いPIDファイルを削除しています (PID: $old_pid は既に終了済み)"
            rm -f "$PID_FILE"
        fi
    fi

    echo $$ > "$PID_FILE"
}

# =============================================================================
# 他ジョブの確認
# =============================================================================
check_pipeline_running() {
    if [[ -f "$PIPELINE_PID_FILE" ]]; then
        local pipeline_pid
        pipeline_pid=$(cat "$PIPELINE_PID_FILE" 2>/dev/null || echo "")
        if [[ -n "$pipeline_pid" ]] && ps -p "$pipeline_pid" > /dev/null 2>&1; then
            log_warn "メインパイプラインが実行中のためスキップします (PID: $pipeline_pid)"
            rm -f "$PID_FILE"
            trap - EXIT
            exit 0
        fi
    fi
}

# =============================================================================
# クリーンアップ
# =============================================================================
cleanup() {
    local exit_code=$?
    rm -f "$PID_FILE"

    if [[ $exit_code -eq 0 ]]; then
        log_info "上位プレイヤーのバトルログ取得が正常に完了しました"
    else
        log_error "上位プレイヤーのバトルログ取得が失敗しました (終了コード: $exit_code)"
    fi

    if (( START_TIME > 0 )); then
        local end_time elapsed hours minutes seconds elapsed_str
        end_time=$(date +%s)
        elapsed=$((end_time - START_TIME))
        hours=$((elapsed / 3600))
        minutes=$(((elapsed % 3600) / 60))
        seconds=$((elapsed % 60))
        printf -v elapsed_str '%02d時間%02d分%02d秒' "$hours" "$minutes" "$seconds"
        log_info "総処理時間: ${elapsed_str} (${elapsed}秒)"
    fi

    exit $exit_code
}
trap cleanup EXIT

# =============================================================================
# エラーハンドリング
# =============================================================================
handle_error() {
    local line_number=$1
    local command=$2
    local exit_code=$3

    log_error "行 $line_number でエラーが発生しました: コマンド '$command' が終了コード $exit_code で失敗"
}
trap 'handle_error ${LINENO} "$BASH_COMMAND" $?' ERR

# =============================================================================
# メイン処理
# =============================================================================
main() {
    START_TIME=$(date +%s)

    load_env_files
    setup

    log_info "上位プレイヤー限定のバトルログ取得を開始します"
    log_info "対象条件: current_rank >= ${MIN_CURRENT_RANK} または highest_rank >= ${MIN_HIGHEST_RANK}"
    log_info "再取得間隔: ${FETCH_ACQ_CYCLE_HOURS}時間"

    check_duplicate
    check_pipeline_running

    cd "$BASE_DIR" || {
        log_error "ベースディレクトリへの移動に失敗しました: $BASE_DIR"
        exit 1
    }

    if ! "$PYTHON_BIN" -m src.fetch_battlelog \
        --acq-cycle-hours "$FETCH_ACQ_CYCLE_HOURS" \
        --min-current-rank "$MIN_CURRENT_RANK" \
        --min-highest-rank "$MIN_HIGHEST_RANK"; then
        log_error "バトルログの取得に失敗しました"
        exit 1
    fi
}

# =============================================================================
# 実行
# =============================================================================
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
