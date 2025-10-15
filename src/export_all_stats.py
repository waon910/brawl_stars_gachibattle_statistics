from __future__ import annotations

import argparse
import json
import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict

import mysql.connector

from .db import get_connection
from .export_3v3_win_rates import (
    compute_matchup_scores as compute_three_vs_three_scores,
    export_matchup_json,
    fetch_matchup_rows as fetch_three_vs_three_rows,
)
from .export_highest_rank_players import fetch_highest_rank_players
from .export_pair_stats import (
    compute_pair_rates,
    fetch_matchup_stats as fetch_pair_matchup_stats,
    fetch_synergy_stats as fetch_pair_synergy_stats,
)
from .export_star_rates import compute_star_rates, fetch_star_rows
from .export_trio_stats import export_trio_json
from .export_win_rates import compute_win_rates, fetch_stats as fetch_win_rate_rows
from .export_rank_match_counts import fetch_rank_match_counts
from .logging_config import setup_logging
from .settings import CONFIDENCE_LEVEL, DATA_RETENTION_DAYS
from .memory_utils import log_memory_usage
from .stats_loader import load_recent_ranked_battles
from .trio_stats import compute_trio_scores, fetch_trio_rows

setup_logging()
JST = timezone(timedelta(hours=9))


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)


def _export_win_rates(dataset, output_path: Path) -> None:
    rows = fetch_win_rate_rows(dataset)
    stats = compute_win_rates(rows, confidence=CONFIDENCE_LEVEL)
    _write_json(output_path, stats)


def _export_star_rates(dataset, output_path: Path) -> None:
    rows = fetch_star_rows(dataset)
    stats = compute_star_rates(rows)
    _write_json(output_path, stats)


def _export_pair_stats(dataset, output_dir: Path) -> None:
    matchup_rows = fetch_pair_matchup_stats(dataset)
    synergy_rows = fetch_pair_synergy_stats(dataset)
    logging.info(
        "ペア統計の入力件数: matchup=%d, synergy=%d",
        len(matchup_rows),
        len(synergy_rows),
    )
    log_memory_usage("pair_stats 集計前")
    matchup_result = compute_pair_rates(
        matchup_rows, symmetrical=False, confidence=CONFIDENCE_LEVEL
    )
    synergy_result = compute_pair_rates(
        synergy_rows, symmetrical=True, confidence=CONFIDENCE_LEVEL
    )
    result = {"matchup": matchup_result, "synergy": synergy_result}

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for kind, maps in result.items():
        kind_dir = output_dir / kind
        kind_dir.mkdir(parents=True, exist_ok=True)
        for map_id, data in maps.items():
            out_file = kind_dir / f"{map_id}.json"
            _write_json(out_file, data)


def _export_trio_stats(dataset, output_dir: Path, since: str) -> None:
    rows = fetch_trio_rows(dataset=dataset, since=since)
    results = compute_trio_scores(
        rows,
        group_by_rank=False,
        confidence=CONFIDENCE_LEVEL,
    )
    export_trio_json(results, output_dir)


def _export_three_vs_three_stats(
    dataset, output_dir: Path, min_games: int
) -> None:
    rows = fetch_three_vs_three_rows(dataset)
    results = compute_three_vs_three_scores(
        rows,
        min_games=min_games,
        confidence=CONFIDENCE_LEVEL,
    )
    export_matchup_json(results, output_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="統計出力をまとめて実行")
    parser.add_argument(
        "--output-root",
        default="data/output",
        help="統計ファイルを配置するルートディレクトリ",
    )
    parser.add_argument(
        "--win-rate-filename",
        default="win_rates.json",
        help="勝率統計のファイル名",
    )
    parser.add_argument(
        "--star-rate-filename",
        default="star_rates.json",
        help="スター取得率統計のファイル名",
    )
    parser.add_argument(
        "--rank-match-count-filename",
        default="rank_match_counts.json",
        help="ランクマッチ数統計のファイル名",
    )
    parser.add_argument(
        "--highest-rank-player-filename",
        default="highest_rank_players.json",
        help="最高ランク達成プレイヤー一覧のファイル名",
    )
    parser.add_argument(
        "--highest-rank-target",
        type=int,
        default=22,
        help="最高ランク達成プレイヤーとして抽出するランク",
    )
    parser.add_argument(
        "--pair-dir-name",
        default="pair_stats",
        help="ペア統計のディレクトリ名",
    )
    parser.add_argument(
        "--trio-dir-name",
        default="trio_stats",
        help="トリオ統計のディレクトリ名",
    )
    parser.add_argument(
        "--three-vs-three-dir-name",
        default="three_vs_three_stats",
        help="3対3統計のディレクトリ名",
    )
    parser.add_argument(
        "--three-vs-three-min-games",
        type=int,
        default=4,
        help="3対3統計で採用する最低試合数",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    jst_now = datetime.now(JST)
    since = (jst_now - timedelta(days=DATA_RETENTION_DAYS)).strftime("%Y%m%d")
    logging.info("統計対象期間（日数）: %d", DATA_RETENTION_DAYS)

    logging.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as exc:  # pragma: no cover - DB接続エラー
        raise SystemExit(f"データベースに接続できません: {exc}")

    try:
        logging.info("共通データセットを読み込んでいます...")
        dataset = load_recent_ranked_battles(conn, since)
        logging.info("ランクマッチ数を取得しています...")
        rank_match_counts = fetch_rank_match_counts(conn)
        highest_rank_players = fetch_highest_rank_players(
            conn, rank=args.highest_rank_target
        )
    except mysql.connector.Error as exc:  # pragma: no cover - クエリエラー
        raise SystemExit(f"クエリの実行に失敗しました: {exc}")
    finally:
        conn.close()

    win_rate_path = output_root / args.win_rate_filename
    star_rate_path = output_root / args.star_rate_filename
    rank_match_path = output_root / args.rank_match_count_filename
    highest_rank_player_path = output_root / args.highest_rank_player_filename
    pair_dir = output_root / args.pair_dir_name
    trio_dir = output_root / args.trio_dir_name
    three_vs_three_dir = output_root / args.three_vs_three_dir_name

    logging.info(
        "共通データセット読み込み完了: rank_logs=%d, battles=%d, star_logs=%d",
        len(dataset.rank_logs),
        len(dataset.battles),
        len(dataset.star_logs),
    )
    log_memory_usage("共通データセット読み込み直後")
    logging.info("ランクマッチ数レコード件数: %d", len(rank_match_counts))
    logging.info("最高ランク達成プレイヤー件数: %d", len(highest_rank_players))

    tasks: Dict[str, Callable[[], None]] = {
        "win_rates": lambda: _export_win_rates(dataset, win_rate_path),
        "star_rates": lambda: _export_star_rates(dataset, star_rate_path),
        "pair_stats": lambda: _export_pair_stats(dataset, pair_dir),
        "trio_stats": lambda: _export_trio_stats(dataset, trio_dir, since),
        "three_vs_three": lambda: _export_three_vs_three_stats(
            dataset, three_vs_three_dir, args.three_vs_three_min_games
        ),
    }

    def _wrap_task(name: str, func: Callable[[], None]) -> Callable[[], None]:
        def _runner() -> None:
            logging.info("%s: 出力処理を開始します", name)
            log_memory_usage(f"{name} 開始時")
            start_time = time.perf_counter()
            try:
                func()
            finally:
                elapsed = time.perf_counter() - start_time
                logging.info("%s: 出力処理が完了しました (経過時間: %.2f秒)", name, elapsed)
                log_memory_usage(f"{name} 完了時")

        return _runner

    wrapped_tasks: Dict[str, Callable[[], None]] = {
        name: _wrap_task(name, task) for name, task in tasks.items()
    }

    logging.info("統計出力を実行しています")
    with ThreadPoolExecutor(max_workers=len(wrapped_tasks)) as executor:
        future_to_name = {
            executor.submit(func): name for name, func in wrapped_tasks.items()
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                future.result()
                logging.info("%s の出力が完了しました", name)
            except Exception:  # pragma: no cover - 実行時エラーはそのまま伝搬
                logging.exception("%s の出力中にエラーが発生しました", name)
                raise

    logging.info("ランクマッチ数を出力しています: %s", rank_match_path)
    _write_json(rank_match_path, rank_match_counts)
    logging.info(
        "最高ランク %d のプレイヤー名を出力しています: %s",
        args.highest_rank_target,
        highest_rank_player_path,
    )
    _write_json(highest_rank_player_path, highest_rank_players)
    logging.info("すべての統計出力が完了しました")


if __name__ == "__main__":
    main()

