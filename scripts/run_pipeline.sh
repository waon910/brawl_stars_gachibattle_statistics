#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# 設定
# =============================================================================
SCRIPT_NAME="brawl_stars_pipeline"
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
OUTPUT_DIR="${BASE_DIR}/data/output"
LOG_DIR="${BASE_DIR}/data/logs"
APP_DIR="/Users/shunsukeiwao/develop/brawl_stars_gachibattle_app"
COPY_PATH="/data/map-meta/"
COPY_PATH2="/public/"
WIN_RATE_FILE_NAME="win_rates.json"
STAR_RATE_FILE_NAME="star_rates.json"
RANK_MATCH_COUNT_FILE_NAME="rank_match_counts.json"
PAIR_STATS_DIR_NAME="pair_stats"
TRIO_STATS_DIR_NAME="trio_stats"
THREE_VS_THREE_STATS_DIR_NAME="three_vs_three_stats"
PID_FILE="${BASE_DIR}/.${SCRIPT_NAME}.pid"
ENV_FILE="${BASE_DIR}/config/settings.env"
LOCAL_ENV_FILE="${BASE_DIR}/.env.local"
# 設定ファイルが読み込めなかった場合のフォールバック値
DEFAULT_RETENTION_DAYS=30
RETENTION_DAYS="$DEFAULT_RETENTION_DAYS"
START_TIME=0

# ログ設定
LOG_FILE="${LOG_DIR}/$(date '+%Y%m%d%H%M').log"

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
    RETENTION_DAYS="${DATA_RETENTION_DAYS:-$DEFAULT_RETENTION_DAYS}"
}

setup() {
    mkdir -p "$OUTPUT_DIR" "$LOG_DIR"
    find "$LOG_DIR" -name "*.log" -mtime +"$RETENTION_DAYS" -delete 2>/dev/null || true
    exec > >(tee -a "$LOG_FILE") 2>&1
}

# =============================================================================
# ログ関数
# =============================================================================
log() {
    local level="$1"; shift
    local timestamp=$(TZ=Asia/Tokyo date '+%Y-%m-%d %H:%M:%S')
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
        local old_pid=$(cat "$PID_FILE" 2>/dev/null || echo "")
        if [[ -n "$old_pid" ]] && ps -p "$old_pid" > /dev/null 2>&1; then
            log_warn "スクリプトが既に実行中です (PID: $old_pid)"
            # 重複実行の場合はcleanupトラップを無効化してからexit
            trap - EXIT
            exit 1
        else
            # 古いPIDファイルが残っている場合は削除
            log_info "古いPIDファイルを削除しています (PID: $old_pid は既に終了済み)"
            rm -f "$PID_FILE"
        fi
    fi
    
    # 新しいPIDを記録
    echo $$ > "$PID_FILE"
}

# =============================================================================
# クリーンアップ
# =============================================================================
cleanup() {
    local exit_code=$?

    # PIDファイルを削除
    rm -f "$PID_FILE"

    if [[ $exit_code -eq 0 ]]; then
        log_info "パイプラインが正常に完了しました"
    else
        log_error "パイプラインが失敗しました (終了コード: $exit_code)"
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
# Git操作の関数
# =============================================================================
git_operations() {
    local win_rates_path="$1"
    local star_rates_path="$2"
    local pair_stats_path="$3"
    local trio_stats_path="$4"
    local end_date="$5"
    
    log_info "Git操作を開始しています"
    
    cd "$APP_DIR" || {
        log_error "ディレクトリの変更に失敗しました: $APP_DIR"
        return 1
    }
    
    # Gitステータスをチェック
    if ! git status >/dev/null 2>&1; then
        log_error "Gitリポジトリではないか、Gitが利用できません"
        return 1
    fi
    
    # ファイルの変更をチェック
    if git diff --quiet ".${COPY_PATH}"; then
        log_info "win_rates.jsonに変更がないため、Git操作をスキップします"
        return 0
    fi
    
    # Git操作の実行
    git add ".${COPY_PATH}" ".${COPY_PATH2}" || {
        log_error "ファイルのGit追加に失敗しました"
        return 1
    }
    
    git commit -m "データ更新 ${end_date}" || {
        log_error "コミットに失敗しました"
        return 1
    }
    
    git push || {
        log_error "プッシュに失敗しました"
        return 1
    }
    
    log_info "Git操作が正常に完了しました"
}

# =============================================================================
# メイン処理
# =============================================================================
main() {
    START_TIME=$(date +%s)

    load_env_files

    # 初期設定
    setup

    log_info "Brawl Starsデータパイプラインを開始します"
    log_info "データ保持期間（日数）: ${RETENTION_DAYS}"
    
    # 重複実行チェック
    check_duplicate
    
    # 作業ディレクトリに移動
    cd "$BASE_DIR" || {
        log_error "ベースディレクトリへの移動に失敗しました: $BASE_DIR"
        exit 1
    }
    
    # 日付範囲の計算（JST）
    local start_date=$(TZ=Asia/Tokyo date -v-"${RETENTION_DAYS}"d +%Y%m%d 2>/dev/null || TZ=Asia/Tokyo date -d "${RETENTION_DAYS} days ago" +%Y%m%d)
    local end_date=$(TZ=Asia/Tokyo date +%Y%m%d%H%M)
    local output_file="${OUTPUT_DIR}/${WIN_RATE_FILE_NAME}"
    local star_output_file="${OUTPUT_DIR}/${STAR_RATE_FILE_NAME}"
    local rank_match_output_file="${OUTPUT_DIR}/${RANK_MATCH_COUNT_FILE_NAME}"
    local pair_output_dir="${OUTPUT_DIR}/${PAIR_STATS_DIR_NAME}"
    local trio_output_dir="${OUTPUT_DIR}/${TRIO_STATS_DIR_NAME}"
    local three_vs_three_output_dir="${OUTPUT_DIR}/${THREE_VS_THREE_STATS_DIR_NAME}"
    
    log_info "対象期間: ${start_date} から ${end_date}"
    
    # バトルログを取得
    log_info "バトルログを取得しています"
    if ! /Users/shunsukeiwao/develop/brawl_stars_gachibattle_statistics/venv/bin/python -m src.fetch_battlelog; then
        log_error "バトルログの取得に失敗しました"
        exit 1
    fi

    # 勝率データを出力
    log_info "統計データをまとめてエクスポートしています"
    if ! /Users/shunsukeiwao/develop/brawl_stars_gachibattle_statistics/venv/bin/python \
        -m src.export_all_stats \
        --output-root "$OUTPUT_DIR" \
        --win-rate-filename "$WIN_RATE_FILE_NAME" \
        --star-rate-filename "$STAR_RATE_FILE_NAME" \
        --rank-match-count-filename "$RANK_MATCH_COUNT_FILE_NAME" \
        --pair-dir-name "$PAIR_STATS_DIR_NAME" \
        --trio-dir-name "$TRIO_STATS_DIR_NAME" \
        --three-vs-three-dir-name "$THREE_VS_THREE_STATS_DIR_NAME"; then
        log_error "統計データのエクスポートに失敗しました"
        exit 1
    fi

    if [[ ! -f "$output_file" ]]; then
        log_error "出力ファイルが見つかりません: $output_file"
        exit 1
    fi

    log_info "出力ファイルを生成しました: $output_file"

    if [[ ! -s "$output_file" ]]; then
        log_warn "出力ファイルが空です: $output_file"
    fi

    if [[ ! -f "$star_output_file" ]]; then
        log_error "出力ファイルが見つかりません: $star_output_file"
        exit 1
    fi

    log_info "出力ファイルを生成しました: $star_output_file"

    if [[ ! -s "$star_output_file" ]]; then
        log_warn "出力ファイルが空です: $star_output_file"
    fi

    if [[ ! -f "$rank_match_output_file" ]]; then
        log_error "出力ファイルが見つかりません: $rank_match_output_file"
        exit 1
    fi

    log_info "出力ファイルを生成しました: $rank_match_output_file"

    if [[ ! -s "$rank_match_output_file" ]]; then
        log_warn "出力ファイルが空です: $rank_match_output_file"
    fi

    if [[ ! -d "$pair_output_dir" ]]; then
        log_error "出力ディレクトリが見つかりません: $pair_output_dir"
        exit 1
    fi

    log_info "出力ディレクトリを生成しました: $pair_output_dir"

    if [[ -z $(find "$pair_output_dir" -type f -name '*.json') ]]; then
        log_warn "出力ディクトリが空です: $pair_output_dir"
    fi

    if [[ ! -d "$trio_output_dir" ]]; then
        log_error "出力ディレクトリが見つかりません: $trio_output_dir"
        exit 1
    fi

    log_info "出力ディレクトリを生成しました: $trio_output_dir"

    if [[ -z $(find "$trio_output_dir" -type f -name '*.json') ]]; then
        log_warn "出力ディレクトリが空です: $trio_output_dir"
    fi

    if [[ ! -d "$three_vs_three_output_dir" ]]; then
        log_error "出力ディレクトリが見つかりません: $three_vs_three_output_dir"
        exit 1
    fi

    log_info "出力ディレクトリを生成しました: $three_vs_three_output_dir"

    if [[ -z $(find "$three_vs_three_output_dir" -type f -name '*.json') ]]; then
        log_warn "出力ディレクトリが空です: $three_vs_three_output_dir"
    fi
    # アプリディレクトリへのコピー
    local destination_win_rate="${APP_DIR}${COPY_PATH}${WIN_RATE_FILE_NAME}"
    local destination_star_rate="${APP_DIR}${COPY_PATH}${STAR_RATE_FILE_NAME}"
    local destination_rank_match="${APP_DIR}${COPY_PATH}${RANK_MATCH_COUNT_FILE_NAME}"
    local destination_pair_stats="${APP_DIR}${COPY_PATH2}${PAIR_STATS_DIR_NAME}"
    local destination_trio_stats="${APP_DIR}${COPY_PATH}${TRIO_STATS_DIR_NAME}"
    local destination_three_vs_three_stats="${APP_DIR}${COPY_PATH}${THREE_VS_THREE_STATS_DIR_NAME}"
    log_info "ファイルをアプリケーションディレクトリにコピーしています"

    if ! cp "$output_file" "$destination_win_rate"; then
        log_error "win_rateファイルのコピーに失敗しました"
        exit 1
    fi

    if ! cp "$star_output_file" "$destination_star_rate"; then
        log_error "star_rateファイルのコピーに失敗しました"
        exit 1
    fi

    if ! cp "$rank_match_output_file" "$destination_rank_match"; then
        log_error "rank_matchファイルのコピーに失敗しました"
        exit 1
    fi

    if ! rsync -a --delete "$pair_output_dir/" "$destination_pair_stats/"; then
        log_error "pair_statsフォルダのコピーに失敗しました"
        exit 1
    fi

    if ! rsync -a --delete "$trio_output_dir/" "$destination_trio_stats/"; then
        log_error "trio_statsフォルダのコピーに失敗しました"
        exit 1
    fi

    if ! rsync -a --delete "$three_vs_three_output_dir/" "$destination_three_vs_three_stats/"; then
        log_error "three_vs_three_statsフォルダのコピーに失敗しました"
        exit 1
    fi

    # Git操作
    git_operations "$destination_win_rate" "$destination_star_rate" "$destination_pair_stats" "$destination_trio_stats" "$end_date"
    
    log_info "アプリケーションの更新が正常に完了しました"
}

# =============================================================================
# 実行
# =============================================================================
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
