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

## GUIダッシュボード

`streamlit` を用いてデータベースの統計情報をリアルタイムに表示するダッシュボードを提供します。

```bash
# 依存パッケージのインストール
pip install streamlit pandas streamlit-autorefresh

# ダッシュボードの起動
streamlit run dashboard.py
```

シーズン → モード → マップ → ランクを選択すると、キャラ使用率・勝率および対キャラ勝率を視覚的に確認できます。シーズンは毎月第1木曜日を開始日として計算されます。
