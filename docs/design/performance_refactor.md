# 集計バッチ高速化リファクタリング設計

## 目的
- `docs/performance_analysis.md` で指摘されたボトルネックを解消し、DB I/O と Python 側集計の負荷を大幅に削減する。
- 既存データを保持したまま段階的に移行できるよう、スキーマ変更に伴うデータ移行手順を提供する。

## 対応方針
1. **DB スキーマ改善**
   - 新たに `battle_participants` テーブルを追加し、試合ごとの参加キャラクターと所属サイドを正規化して保持する。
   - `rank_logs` と `battle_logs` に複合インデックスを追加し、期間・ランク条件でのスキャン範囲を絞り込む。
   - 既存データを `win_lose_logs` から `battle_participants` へ移し替える移行 SQL を提供する。

## `battle_participants` テーブルの役割
- **データ投入時の利用**: `src/fetch_battlelog.py` でバトルログを取り込む際、勝者・敗者双方の編成を `battle_participants` に同時保存することで、チーム構成を都度 `win_lose_logs` から復元する処理を排除する。【F:src/fetch_battlelog.py†L41-L87】
- **集計時の利用**: `src/stats_loader.py` では参加者テーブルをベースにストリーミング読み込みを行い、各試合のメンバーを即座に取得できるようにすることで、従来の二重 JOIN や Python 側での再構成を不要にしている。【F:src/stats_loader.py†L69-L192】
- **ダッシュボード/トリオ統計での利用**: `src/dashboard.py` や `src/trio_stats.py` のクエリは `battle_participants` を起点にチーム別の勝率・使用率を集計し、`GROUP BY` だけで集計可能な形に整理している。これにより `win_lose_logs` を N² 展開する処理が解消され、クエリ計画が単純化されている。【F:src/dashboard.py†L30-L126】【F:src/trio_stats.py†L15-L94】
- **インデックス設計**: テーブルには `(battle_log_id, team_side)` や `(map_id, team_side, brawler_id)` の複合インデックスを張ることで、試合単位・マップ単位の集計をカバリングし、レコード取得時のランダムアクセスを削減する。【F:sql/schema.sql†L72-L101】
- **移行時の役割**: 既存の勝敗ログから参加者情報を抽出して投入する `sql/migrate_battle_participants.sql` を提供しており、稼働中のデータを保持したまま新スキーマへ移行できるようにしている。【F:sql/migrate_battle_participants.sql†L1-L11】

2. **データ投入フローの更新**
   - バトルログ取得時に勝者・敗者の参加者レコードを同時に `battle_participants` へ登録し、削除処理時にも整合性を保つ。

3. **データ読み込みの効率化**
   - `load_recent_ranked_battles` でサーバーサイドカーソルによるストリーミング取得へ切り替え、`fetchall()` による全件読み込みを廃止する。
   - 参加者情報は `battle_participants` から直接読み込み、勝敗ログからの再構成を不要にする。新テーブルが存在しない場合は後方互換として従来のロジックへフォールバックする。

4. **分析用クエリの最適化**
   - ダッシュボードおよびトリオ統計の SQL を `battle_participants` 基準に組み直し、`win_lose_logs` を二重結合する処理を削減する。
   - 勝率集計は CASE 式による単一クエリで wins / losses を同時計上する。

## 想定移行手順
1. アプリケーション停止後、`sql/schema.sql` を適用してスキーマを更新する。
2. `sql/migrate_battle_participants.sql` を実行し、既存の勝敗ログから参加者テーブルへデータを移行する。
3. アプリケーションを再起動し、新スキーマに対応したコードを利用する。

## 影響範囲
- スキーマ変更に伴い DB バージョンが上がる。移行完了前でも動作できるよう、読み込み処理は新旧スキーマの両方をサポートする。
- `battle_participants` 追加によりディスク使用量は増えるが、集計クエリの複雑度と Python 側処理時間が大幅に減少する見込み。

