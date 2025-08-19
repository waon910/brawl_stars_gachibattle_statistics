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