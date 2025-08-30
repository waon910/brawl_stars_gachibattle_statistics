#!/usr/bin/env bash
# setup_cron.sh - Cron設定用スクリプト

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PIPELINE_SCRIPT="${SCRIPT_DIR}/run_pipeline.sh"

# =============================================================================
# cron設定の追加
# =============================================================================
setup_cron() {
    echo "12時間間隔実行のためのcron設定を開始します..."
    
    # スクリプトファイルの存在確認
    if [[ ! -f "$PIPELINE_SCRIPT" ]]; then
        echo "エラー: パイプラインスクリプトが見つかりません: $PIPELINE_SCRIPT"
        exit 1
    fi
    
    # 実行権限の付与
    chmod +x "$PIPELINE_SCRIPT"
    
    # 現在のcrontabをバックアップ
    crontab -l > /tmp/crontab_backup 2>/dev/null || echo "# 新しいcrontab" > /tmp/crontab_backup
    
    # 既存の設定があるかチェック
    if grep -q "run_pipeline.sh" /tmp/crontab_backup; then
        echo "既存のcron設定を発見しました。古い設定を削除します..."
        grep -v "run_pipeline.sh" /tmp/crontab_backup > /tmp/new_crontab
    else
        cp /tmp/crontab_backup /tmp/new_crontab
    fi
    
    # 新しいcron設定を追加
    cat >> /tmp/new_crontab << EOF

# Brawl Stars データパイプライン - 12時間間隔実行 (JST 0:00, 12:00)
0 15,3 * * * cd "$SCRIPT_DIR" && TZ=Asia/Tokyo "$PIPELINE_SCRIPT" >/dev/null 2>&1
EOF
    
    # crontabに設定
    crontab /tmp/new_crontab
    
    # 一時ファイルの削除
    rm -f /tmp/crontab_backup /tmp/new_crontab
    
    echo "cron設定が完了しました！"
    echo "パイプラインは以下の時刻で12時間間隔で実行されます："
    echo "  - 00:00 JST (15:00 UTC)"
    echo "  - 12:00 JST (03:00 UTC)"
    echo ""
    echo "現在のcrontab設定："
    crontab -l
}

# =============================================================================
# ログローテーション設定
# =============================================================================
setup_logrotate() {
    local logrotate_config="/etc/logrotate.d/brawl-stars-pipeline"
    
    echo "ログローテーション設定を開始します..."
    
    # logrotate設定ファイルの作成（root権限が必要）
    sudo tee "$logrotate_config" > /dev/null << EOF
${BASE_DIR}/data/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    copytruncate
}
EOF
    
    echo "ログローテーション設定が完了しました！"
}

# =============================================================================
# システム要件チェック
# =============================================================================
check_requirements() {
    echo "システム要件をチェックしています..."
    
    # Python3の確認
    if ! command -v python3 &> /dev/null; then
        echo "エラー: python3が必要ですがインストールされていません"
        exit 1
    else
        echo "✓ Python3が利用可能です: $(python3 --version)"
    fi
    
    # Gitの確認
    if ! command -v git &> /dev/null; then
        echo "エラー: gitが必要ですがインストールされていません"
        exit 1
    else
        echo "✓ Gitが利用可能です: $(git --version)"
    fi
    
    # アプリディレクトリの確認
    local app_dir="/Users/shunsukeiwao/develop/brawl_stars_gachibattle_app"
    if [[ ! -d "$app_dir" ]]; then
        echo "警告: アプリケーションディレクトリが見つかりません: $app_dir"
        echo "run_pipeline.sh内のAPP_DIR変数を更新してください"
    else
        echo "✓ アプリケーションディレクトリが存在します"
    fi
    
    # Python スクリプトの確認
    local missing_scripts=()
    for script in "fetch_battlelog.py" "export_win_rates.py"; do
        if [[ ! -f "${BASE_DIR}/src/$script" ]]; then
            missing_scripts+=("$script")
        fi
    done
    
    if [[ ${#missing_scripts[@]} -gt 0 ]]; then
        echo "エラー: 以下の必要なPythonスクリプトが見つかりません："
        printf '  - %s\n' "${missing_scripts[@]}"
        exit 1
    else
        echo "✓ 必要なPythonスクリプトが全て存在します"
    fi
    
    # cronサービスの確認
    if command -v crontab &> /dev/null; then
        echo "✓ cronサービスが利用可能です"
    else
        echo "エラー: cronサービスが利用できません"
        exit 1
    fi
    
    echo "全ての要件が満たされています！"
}

# =============================================================================
# ディレクトリ作成
# =============================================================================
create_directories() {
    echo "必要なディレクトリを作成しています..."
    
    local dirs=("${BASE_DIR}/data/output" "${BASE_DIR}/data/logs")
    
    for dir in "${dirs[@]}"; do
        if [[ ! -d "$dir" ]]; then
            mkdir -p "$dir"
            echo "✓ ディレクトリを作成しました: $dir"
        else
            echo "✓ ディレクトリは既に存在します: $dir"
        fi
    done
}

# =============================================================================
# メイン処理
# =============================================================================
main() {
    echo "=========================================="
    echo "  Brawl Stars パイプライン セットアップ"
    echo "=========================================="
    echo ""
    
    # システム要件チェック
    check_requirements
    echo ""
    
    # ディレクトリ作成
    create_directories
    echo ""
    
    # cron設定
    setup_cron
    echo ""
    
    # logrotateの設定（オプション）
    if [[ "${1:-}" == "--with-logrotate" ]]; then
        setup_logrotate
        echo ""
    fi
    
    echo "=========================================="
    echo "  セットアップが正常に完了しました！"
    echo "=========================================="
    echo ""
    echo "次のステップ:"
    echo "1. パイプラインを手動でテスト実行してください:"
    echo "   $PIPELINE_SCRIPT"
    echo ""
    echo "2. ログを確認してください:"
    echo "   tail -f ${BASE_DIR}/data/logs/\$(date +%Y%m%d).log"
    echo ""
    echo "3. cron実行を監視してください:"
    echo "   - 次回実行: 今日または明日の00:00または12:00 JST"
    echo "   - ログ確認: tail -f ${BASE_DIR}/data/logs/\$(date +%Y%m%d).log"
    echo ""
    echo "4. 問題が発生した場合:"
    echo "   - エラーログ: tail -f ${BASE_DIR}/data/logs/error.log"
    echo "   - cron設定確認: crontab -l"
    echo ""
}

# =============================================================================
# 使用方法
# =============================================================================
usage() {
    echo "使用方法: $0 [オプション]"
    echo ""
    echo "オプション:"
    echo "  --with-logrotate    ログローテーション設定も実行 (sudo権限必要)"
    echo "  --help             このヘルプメッセージを表示"
    echo ""
    echo "例:"
    echo "  $0                    # 基本セットアップ"
    echo "  $0 --with-logrotate   # ログローテーション込みセットアップ"
}

# =============================================================================
# 実行
# =============================================================================
if [[ "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

main "$@"
