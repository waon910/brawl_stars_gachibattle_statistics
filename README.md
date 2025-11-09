# brawl_stars_gachibattle_statistics

Brawl Stars のランク戦バトルログを継続的に収集し、MySQL データベースに蓄積したうえで多様な統計データを出力するためのツール群です。勝率・相性・スター取得率といったメタ分析に必要な情報を自動生成し、外部アプリケーションやダッシュボードに取り込める JSON 形式で提供します。また、特定プレイヤーのパフォーマンスを追跡するための監視フラグ管理 CLI も同梱しています。

## 機能一覧

- **バトルログ収集**: `src/fetch_battlelog.py` が Brawl Stars 公式 API からランク戦のバトルログを取得し、プレイヤー情報を含めて MySQL に格納します。過去ログの自動整理（保持期間超過分の削除）にも対応しています。
- **統計データの一括出力**: `src/export_all_stats.py` が勝率・スター取得率・対キャラ/味方相性・トリオ構成・3対3構成・監視対象プレイヤーの実績・ランクマッチ数をまとめて JSON 出力します。Beta-Binomial に基づく信頼区間計算を採用しています。
- **個別統計スクリプト**: `src/export_win_rates.py` や `src/export_pair_stats.py` など、特定の分析指標のみを抽出する専用スクリプトを提供します。
- **監視対象プレイヤー管理**: `scripts/player_monitoring.py` により、特定プレイヤーをデータ収集の優先対象に設定・解除したり、現在の監視リストを確認できます。
- **バッチ実行支援**: `scripts/run_pipeline.sh` が収集〜統計出力〜成果物コピー〜Git 操作までを一貫して実行し、ログ出力や重複起動の抑止、実行時間の計測を行います。`scripts/setup_cron.sh` を使えば cron ジョブのひな型を作成できます。
- **設定/ログ管理**: `config/settings.env` と `.env.local` による環境変数管理、`config/logging.yaml` による Python ログ設定を用意しています。

## 前提条件

- Python 3.10 以上（`requests`、`python-dateutil`、`mysql-connector-python`、`SQLAlchemy`、`python-dotenv` などのライブラリを使用します）
- MySQL 8.x 互換のデータベース
- Brawl Stars API キー（[公式 API ポータル](https://developer.brawlstars.com/) で取得）

プロジェクトで利用する Python パッケージは仮想環境を作成したうえで `pip install requests python-dateutil mysql-connector-python SQLAlchemy python-dotenv` などを実行して整えてください。必要に応じて追加ライブラリをインポートしてください。

## 環境設定

1. `config/settings.env` を編集し、データ保持期間や集計対象ランクなどの共通設定を指定します。サンプル値は以下のとおりです。
   - `DATA_RETENTION_DAYS=30`: 30 日分のログのみを保持・集計します。
   - `MIN_RANK_ID=4`: ダイヤモンドランク相当以上の対戦を集計対象にします。
   - `CONFIDENCE_LEVEL=0.95`: 勝率の信頼区間を 95% で算出します。
2. `.env.local`（任意）を作成すると、個人環境に固有の設定を上書きできます。リポジトリには含めず、必要な値のみ記載してください。
3. データベース接続情報と API キーを環境変数で設定します。
   ```bash
   export MYSQL_HOST="localhost"
   export MYSQL_USER="root"
   export MYSQL_PASSWORD="your_password"
   export MYSQL_DB="brawl_stats"
   export BRAWL_STARS_API_KEY="your_api_key"
   ```
   これらの値は Python スクリプトおよびシェルスクリプトから共通して参照されます。

## データベース初期化とマスター投入

スキーマ作成後に `sql/insert_master.sql` を実行すると、モード・マップ・キャラクター・ランクのマスターデータが登録されます。MySQL への接続ユーザー名・パスワードは環境に合わせて読み替えてください。

```bash
# データベース作成
mysql -u root -p -e "CREATE DATABASE brawl_stats CHARACTER SET utf8mb4;"
# スキーマ作成
mysql -u root -p brawl_stats < sql/schema.sql
# マスターデータ挿入
mysql -u root -p brawl_stats < sql/insert_master.sql
```

## データパイプラインの実行

`scripts/run_pipeline.sh` は以下の処理を順番に実行します。

1. `.env.local` を含む各種設定ファイルを読み込み、保持期間や並列実行数を決定。
2. 多重起動を防ぐ PID ファイルを作成し、ログディレクトリを初期化。
3. `python -m src.fetch_battlelog` を呼び出して Brawl Stars API から最新のバトルログを収集し、不要な古いレコードを削除。
4. `python -m src.export_all_stats` を実行し、勝率 (`win_rates.json`)、スター取得率 (`star_rates.json`)、対キャラ/味方相性 (`pair_stats/`)、トリオ統計 (`trio_stats/`)、3 対 3 構成 (`three_vs_three_stats/`)、監視対象プレイヤー統計 (`monitored_player_stats.json`)、ランクマッチ数 (`rank_match_counts.json`) を生成。監視対象プレイヤー統計では `is_monitored = 1` の対象と現在のランクが22のプレイヤーを両方含めるため、最新のランク22プレイヤーも出力されます。
5. 生成された JSON/ディレクトリを外部アプリケーション用パスへコピーし、必要であれば Git へのコミット・プッシュを実行。
6. 実行終了時に総処理時間をログ出力し、PID ファイルを削除。

スクリプト内の仮想環境パスやコピー先 (`APP_DIR` など) は利用環境に合わせて編集してから使用してください。実行前に `BRAWL_STARS_API_KEY` と MySQL 接続情報を環境変数に設定しておく必要があります。

```bash
./scripts/run_pipeline.sh
```

ログは `data/logs` 配下にタイムスタンプ付きで出力されます。保持期間は `DATA_RETENTION_DAYS` に従って自動的に管理されます。

## 個別の統計出力スクリプト

一括出力に加えて、以下のスクリプトを個別に実行することもできます。`--output-dir` やファイル名は任意に指定してください。

### 対キャラ・協力勝率

```bash
python -m src.export_pair_stats --output-dir pair_stats_output
```

`pair_stats_output/matchup/<map_id>.json` と `pair_stats_output/synergy/<map_id>.json` が生成され、ランク別の勝敗数・勝率・信頼区間を含みます。

### トリオ勝率

```bash
python -m src.export_trio_stats --output-dir trio_stats_output
```

`<map_id>/<rank_id>.json` 形式で、各トリオ編成の勝敗数・勝率・Beta-Binomial LCB を出力します。

### 3 対 3 構成別勝率

```bash
python -m src.export_3v3_win_rates --output-dir three_vs_three_output
```

勝利チームと敗北チームの組み合わせごとに勝率と信頼区間を算出し、`<map_id>.json` に保存します。

### 監視対象プレイヤー統計

```bash
python -m src.export_monitored_player_stats --output monitored_player_stats.json
```

監視対象に設定されているプレイヤーの勝敗をマップ別/キャラクター別に集計します。監視対象が存在しない場合は空データを出力します。

### ランクマッチ数とスター取得率

```bash
python -m src.export_rank_match_counts --output rank_match_counts.json
python -m src.export_star_rates --output star_rates.json
```

ランク別・マップ別の試合数やスター獲得傾向をそれぞれ JSON として取得できます。

## 監視対象プレイヤー管理 CLI (`scripts/player_monitoring.py`)

監視対象に設定されたプレイヤーは `src.fetch_battlelog` が優先的にバトルログを取得し、`export_monitored_player_stats.py` が専用の統計を出力します。`src.export_monitored_player_stats` は監視対象に加えて現在のランクが22のプレイヤーも統計対象に含め、両者が重複するプレイヤーは1件として集計されるので、ランク22プレイヤーを CLI で追加しなくても最新のデータを追跡できます。CLI を使う前に `players` テーブルに対象タグが存在している必要があります（バトルログを一度取得すると自動で登録されます）。

### 事前準備

- MySQL 接続情報を環境変数 (`MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DB`) で設定してください。
- 仮想環境を有効化し、必要な Python ライブラリをインストールしておきます。

### 利用方法

```bash
# 監視対象に追加
python scripts/player_monitoring.py monitor #ABCDEFG #HIJKLMN

# 監視対象から解除
python scripts/player_monitoring.py unmonitor #ABCDEFG

# 監視対象の一覧を表示
python scripts/player_monitoring.py list

# 現在のランク22プレイヤーを監視対象へ
python scripts/player_monitoring.py monitor-rank22
```

主な挙動:

- 入力したタグは前後の空白を除去し、大文字に変換したうえで先頭に `#` が無い場合は補完します。重複タグは 1 度だけ処理されます。
- 指定タグが `players` テーブルに存在しない場合は警告を表示し、既存タグのみ処理します。
- `monitor` コマンドは対象プレイヤーの `is_monitored` フラグを `1` に設定し、初回設定時には `monitoring_started_at` に UTC 現在時刻を記録します。
- `unmonitor` コマンドはフラグを `0` に戻し、`monitoring_started_at` を `NULL` に戻します。
- `list` コマンドは監視対象プレイヤーを監視開始日時順に表示します。対象が存在しない場合はメッセージを出力して終了します。
- `monitor-rank22` コマンドは `current_rank = 22` かつ `is_monitored = 0` のプレイヤーを一括で監視対象に設定します。
- 各コマンドは処理件数を標準出力へ表示し、未入力や存在しないタグのみの場合は終了コード `1` で終了します。

## ログ設定

Python スクリプトは `config/logging.yaml` に定義された設定でロガーを初期化します。必要に応じてログレベルやフォーマット、ハンドラの出力先を編集してください。シェルスクリプトのログは `data/logs` にファイル出力されます。

## ライセンス

このリポジトリの利用条件は `LICENSE`（存在する場合）を参照してください。
