# brawl_stars_gachibattle_statistics

Brawl Stars のバトルログを収集・分析するための簡易ツールです。

## マスターデータの挿入

スキーマ作成後、`insert_master.sql` を実行するとモード・マップ・キャラクターのマスターデータを一括で登録できます。

```bash
# データベース初期化
sqlite3 brawl_stats.db < schema.sql
# マスターデータ挿入
sqlite3 brawl_stats.db < insert_master.sql
```

## データ取得と勝率出力

バトルログの取得から勝率データの JSON 出力までを一括で行うシェルスクリプトを用意しています。実行前に Brawl Stars API キーを環境変数 `BRAWL_STARS_API_KEY` に設定してください。

```bash
./run_pipeline.sh
```

出力されたファイルは `output` フォルダに保存され、ファイル名には取得した日付範囲が含まれます。

## GUIダッシュボード

`streamlit` を用いてデータベースの統計情報をリアルタイムに表示するダッシュボードを提供します。

```bash
# 依存パッケージのインストール
pip install streamlit pandas streamlit-autorefresh

# ダッシュボードの起動
streamlit run dashboard.py
```

シーズン、モード、マップ、ランクを任意に選択して、キャラ使用率・勝率および対キャラ勝率を視覚的に確認できます。各項目で「全体」を選択すると全データを対象とした集計結果を表示します。シーズンは毎月第1木曜日を開始日として計算されます。
