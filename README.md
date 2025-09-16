# brawl_stars_gachibattle_statistics

Brawl Stars のバトルログを収集・分析するための簡易ツールです。

## マスターデータの挿入

スキーマ作成後、`sql/insert_master.sql` を実行するとモード・マップ・キャラクターのマスターデータを一括で登録できます。データベースはMySQLを使用します。

```bash
# データベース作成（必要に応じてユーザー名・パスワードを変更）
mysql -u root -p -e "CREATE DATABASE brawl_stats CHARACTER SET utf8mb4;"
# スキーマ作成
mysql -u root -p brawl_stats < sql/schema.sql
# マスターデータ挿入
mysql -u root -p brawl_stats < sql/insert_master.sql
```

## データ取得と勝率出力

バトルログの取得から勝率データの JSON 出力までを一括で行うシェルスクリプトを用意しています。実行前に Brawl Stars API キーを環境変数 `BRAWL_STARS_API_KEY` と MySQL接続情報 (`MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DB`) に設定してください。

```bash
./scripts/run_pipeline.sh
```

出力されたファイルは `data/output` フォルダに保存され、ファイル名には取得した日付範囲が含まれます。ログは `data/logs` に保存され、`config/logging.yaml` で設定できます。

## 共通設定

データの保存期間などの共通設定値は `config/settings.env` で管理しています。デフォルトでは `DATA_RETENTION_DAYS=30` として30日分のデータを扱います。保持期間を変更したい場合はこの値を編集するだけで、Pythonスクリプトとシェルスクリプトの両方に反映されます。

## 対キャラ・協力勝率の出力

`src/export_pair_stats.py` を実行すると、対キャラ勝率(`matchup`)と味方同士の相性(`synergy`)をマップごとに分割した JSON として出力できます。

```bash
python -m src.export_pair_stats --output-dir pair_stats_output
```

`--output-dir` で指定したディレクトリ配下に `matchup/<map_id>.json` と `synergy/<map_id>.json` が生成されます。

## GUIダッシュボード

`streamlit` を用いてデータベースの統計情報をリアルタイムに表示するダッシュボードを提供します。

```bash
# 依存パッケージのインストール
pip install streamlit pandas streamlit-autorefresh

# ダッシュボードの起動
streamlit run src/dashboard.py
```

シーズン、モード、マップ、ランクを任意に選択して、キャラ使用率・勝率および対キャラ勝率を視覚的に確認できます。各項目で「全体」を選択すると全データを対象とした集計結果を表示します。シーズンは毎月第1木曜日を開始日として計算されます。
