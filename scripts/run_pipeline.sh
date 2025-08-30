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
COPY_PATH="/lib/map-meta/win_rates.json"
PID_FILE="${BASE_DIR}/.${SCRIPT_NAME}.pid"

# ログ設定
LOG_FILE="${LOG_DIR}/$(date '+%Y%m%d%H%M').log"

# =============================================================================
# 初期設定
# =============================================================================
setup() {
    mkdir -p "$OUTPUT_DIR" "$LOG_DIR"
    find "$LOG_DIR" -name "*.log" -mtime +30 -delete 2>/dev/null || true
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
            exit 1
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
    local file_path="$1"
    local end_date="$2"
    
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
    git add ".${COPY_PATH}" || {
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
    # 初期設定
    setup

    log_info "Brawl Starsデータパイプラインを開始します"
    
    # 重複実行チェック
    check_duplicate
    
    # 作業ディレクトリに移動
    cd "$BASE_DIR" || {
        log_error "ベースディレクトリへの移動に失敗しました: $BASE_DIR"
        exit 1
    }
    
    # 日付範囲の計算（JST）
    local start_date=$(TZ=Asia/Tokyo date -v-30d +%Y%m%d 2>/dev/null || TZ=Asia/Tokyo date -d '30 days ago' +%Y%m%d)
    local end_date=$(TZ=Asia/Tokyo date +%Y%m%d%H%M)
    local output_file="${OUTPUT_DIR}/win_rates_${start_date}-${end_date}.json"
    
    log_info "対象期間: ${start_date} から ${end_date}"
    
    # バトルログを取得
    log_info "バトルログを取得しています"
    if ! python -m src.fetch_battlelog; then
        log_error "バトルログの取得に失敗しました"
        exit 1
    fi

    # 勝率データを出力
    log_info "勝率データをエクスポートしています"
    if ! python -m src.export_win_rates --output "$output_file"; then
        log_error "勝率データのエクスポートに失敗しました"
        exit 1
    fi
    
    # 出力ファイルの存在確認
    if [[ ! -f "$output_file" ]]; then
        log_error "出力ファイルが見つかりません: $output_file"
        exit 1
    fi
    
    log_info "出力ファイルを生成しました: $output_file"
    
    # ファイルサイズをチェック（空ファイルかどうか）
    if [[ ! -s "$output_file" ]]; then
        log_warn "出力ファイルが空です: $output_file"
    fi
    
    # アプリディレクトリへのコピー
    local destination="${APP_DIR}${COPY_PATH}"
    log_info "ファイルをアプリケーションディレクトリにコピーしています: $destination"
    
    if ! cp "$output_file" "$destination"; then
        log_error "ファイルのコピーに失敗しました"
        exit 1
    fi
    
    # Git操作
    git_operations "$destination" "$end_date"
    
    # 古い出力ファイルのクリーンアップ（30日以上古いファイルを削除）
    find "$OUTPUT_DIR" -name "win_rates_*.json" -mtime +30 -delete 2>/dev/null || true
    
    log_info "アプリケーションの更新が正常に完了しました"
}

# =============================================================================
# 実行
# =============================================================================
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
