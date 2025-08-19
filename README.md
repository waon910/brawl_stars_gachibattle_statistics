# brawl_stars_gachibattle_statistics

Brawl Stars のバトルログを収集・分析するための簡易ツールです。

## 使い方

1. `BRAWL_STARS_API_KEY` 環境変数に公式 API キーを設定します。
2. 収集したいプレイヤーのタグ（例: `PQLOJ9RQG`）を指定して `BattleLogCollector` を呼び出します。
3. 保存されたデータに対して `analyze_usage_and_winrate` を実行すると、モード・マップ・ランクごとのキャラクター使用率と勝率、キャラクター同士の対戦結果を取得できます。

## データベーススキーマ

SQLite を用いたシンプルなスキーマです。`players` テーブルでプレイヤーごとの最終取得時刻を管理し、`battle_logs`
テーブルでは `battle_time` と `star_player_tag` の複合キーでバトルを一意に識別します。`modes`、`maps`、`brawlers`
といったテーブルを別途設け、モード・マップ・キャラクター情報を正規化しています。

テスト実行:

```bash
pytest
```
