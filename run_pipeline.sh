#!/usr/bin/env bash
set -euo pipefail

# 出力フォルダ
OUTPUT_DIR="output"
mkdir -p "$OUTPUT_DIR"

# 30日前から今日までの日付範囲を計算（JST）
START_DATE=$(TZ=Asia/Tokyo date -d '30 days ago' +%Y%m%d)
END_DATE=$(TZ=Asia/Tokyo date +%Y%m%d)

# バトルログを取得
python3 fetch_battlelog.py

# 勝率データを出力
python3 export_win_rates.py --output "${OUTPUT_DIR}/win_rates_${START_DATE}-${END_DATE}.json"

echo "出力ファイル: ${OUTPUT_DIR}/win_rates_${START_DATE}-${END_DATE}.json"
