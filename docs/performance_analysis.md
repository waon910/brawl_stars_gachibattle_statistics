# 集計バッチの性能ボトルネック分析

## 現状フローの整理
- `src/export_all_stats.py` は統計出力開始時に MySQL へ接続し、`load_recent_ranked_battles` で直近30日分のランク戦データを一括取得した後、勝率・スター率・ペア/トリオ統計などを並列スレッドで計算しています。【F:src/export_all_stats.py†L37-L137】
- データセットの構築では `rank_logs`、`battle_logs`、`win_lose_logs`、`rank_star_logs` の全件を Python 側へ読み込み、`StatsDataset` に格納したのち、各統計計算から同じオブジェクトを共有利用しています。【F:src/stats_loader.py†L69-L191】

ログから、データセット構築だけで約2時間30分を要し、その後の統計計算も合計で約1時間45分かかっているため、DB I/O と Python 集計の双方に大きなボトルネックが存在します。

## ボトルネック1: DB からの全件読み込み
- `load_recent_ranked_battles` は対象期間の試合に紐づく勝敗ログ全件（例: 約1,800万件の win/lose レコード）を `cursor.fetchall()` で一括取得し、Python の辞書に構築し直しています。レコード数に比例してメモリ消費と処理時間が増大し、MySQL→Python 間の転送だけでボトルネックになっています。【F:src/stats_loader.py†L114-L188】
- 現行スキーマでは `win_lose_logs` に (battle_log_id, win_brawler_id, lose_brawler_id) が保存されており、単一インデックス `idx_win_lose_logs_battle_log_id` のみです。【F:sql/schema.sql†L41-L55】対象期間で絞るには `rank_logs` を経由して `id >= since` を評価する必要があり、`rank_logs` 側にも `(rank_id, id)` の複合インデックスが存在しないため、MySQL が広範囲を走査しがちです。【F:sql/schema.sql†L1-L40】

### 抜本的改善案
1. **参照系テーブルの正規化見直しと集約テーブルの導入**
   - 試合参加者を保持する `battle_participants`（`battle_log_id`, `team_side`, `brawler_id`）のようなテーブルを新設し、勝敗ログから都度復元する処理を排除します。これによりチーム単位の集計クエリを `GROUP BY` だけで表現でき、`win_lose_logs` を N² で展開する必要がなくなります。
   - さらに、日次・マップ・ランク別の集約結果を保持するマテリアライズド・ビュー（例: `daily_map_brawler_stats`）をバッチ投入時に更新すれば、統計出力時には必要な粒度のデータのみを参照するだけで済みます。

2. **カバリングインデックスの整備**
   - `rank_logs` に `(rank_id, id)`、`(id)` の複合/単独インデックスを追加し、期間条件とランク条件を同時に満たす最小範囲だけを走査できるようにします。
   - `battle_logs` には既存の `idx_battle_logs_rank_log_id` を活かしつつ、`(rank_log_id, id)` の複合インデックスを追加して結合時のルックアップを削減します。
   - 新設する `battle_participants` には `(battle_log_id, team_side)`、`(map_id, team_side, brawler_id)` 等のインデックスを張ることで後続の集計クエリをカバリングさせられます。

3. **段階的なロード & ストリーミング処理**
   - Python 側で全件メモリ常駐させる代わりに、MySQL サーバーサイドカーソル（`cursor = conn.cursor(dictionary=True, buffered=False)` 等）でチャンク単位にストリーミングし、計算処理をしながら破棄する構造へ変更します。集約を DB へ寄せるとさらに効果的です。

## ボトルネック2: Python 側の N²〜N³ 集計
- ペア勝率集計は、勝者チーム×敗者チームの全組み合わせを Python の二重ループで生成しています。3vs3 の場合、1試合につき最大 9 組の対面が発生し、試合数 1,000万件では 9,000万レコード相当の更新を Python で行う計算量になります。【F:src/export_pair_stats.py†L27-L43】
- シナジー集計でもチーム内組み合わせを `itertools.combinations` 相当の二重ループで生成しており、1試合あたり 3 組の更新が発生します。【F:src/export_pair_stats.py†L47-L63】
- 3対3 編成やトリオ統計も、Python 側で勝者・敗者チームのソートとタプル生成を行い、逆向きの戦績を辞書から再参照する処理が挿入されています。【F:src/export_3v3_win_rates.py†L21-L70】【F:src/trio_stats.py†L16-L78】

### 抜本的改善案
1. **DB 内での組み合わせ展開**
   - `battle_participants` を前提に、MySQL 側で `GROUP_CONCAT` + 自作の組み合わせ生成 UDF もしくは数字テーブルを使ってペア・トリオを算出し、そのまま `GROUP BY` で集計する形に移行します。SQL だけで勝率・試合数まで集計できれば Python 側は Beta-Binomial の後処理だけで済みます。
   - 例: 勝者チームの 3 メンバーを `WITH` 句で展開 → `JOIN` → `GROUP BY map_id, winner_a, winner_b` のように書くことで、1試合あたりの処理を SQL 実行計画に委譲可能です。

2. **差分更新方式への転換**
   - 日次バッチで新規試合のみを対象に組み合わせ集計し、結果を集約テーブルへ `INSERT ... ON DUPLICATE KEY UPDATE` で反映します。過去30日分を毎回フルスキャンする方式から、新規データ分だけを処理する方式へ変更することで、処理時間を大幅に削減できます。

3. **Python 側では行指向→列指向のベクトル化**
   - どうしても Python で組み合わせ展開が必要な部分は、`numpy`/`pandas` を活用した配列演算や `numba` JIT を検討します。ただし根本的には DB 側での集計/差分更新の方が効果が大きいです。

## ボトルネック3: 並列化の限界
- `ThreadPoolExecutor` で 5 タスクを同時に実行していますが、各タスクは CPU バウンドな Python 集計で GIL の影響を受けます。【F:src/export_all_stats.py†L139-L165】CPU コアを活かしきれておらず、スレッド間で同じ `StatsDataset` を共有するためメモリ帯域も競合します。

### 抜本的改善案
1. Python 側の処理を最小化し、DB 内集計や差分更新で計算済みデータを読むだけにする。
2. どうしても Python 集計を残す場合は、`multiprocessing` や `joblib` によるプロセス並列化へ切り替え、共有メモリのオーバーヘッドを避ける。または Cython/Numba によるネイティブコード化で GIL ボトルネックを解消する。

## まとめ
- もっとも効果的なのは「DB 設計を集計用途に最適化し、統計出力時には集約済みデータのみを取得する」アーキテクチャ転換です。
- 具体的には、(1) 参加者テーブルとマテリアライズド集計テーブルの追加、(2) 適切な複合インデックス整備、(3) 差分更新や SQL ベースの組み合わせ展開による Python 側処理の劇的削減、を同時に進めることで、数時間かかっているバッチを数分〜数十分程度まで短縮できる見込みです。
