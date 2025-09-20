"""スター取得率をマップ・キャラ別に集計してJSON出力するスクリプト."""

import argparse
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import mysql.connector

from .db import get_connection
from .logging_config import setup_logging
from .settings import DATA_RETENTION_DAYS
from .stats_loader import StatsDataset, load_recent_ranked_battles

setup_logging()


def fetch_star_rows(dataset: StatsDataset) -> List[Tuple[int, int, int, int, int]]:
    """共通データセットからスター取得集計を生成する."""

    totals: Dict[int, int] = defaultdict(int)
    for rank_log in dataset.rank_logs.values():
        totals[rank_log.map_id] += 1

    participants = dataset.participants_by_rank_log()
    usage: Dict[Tuple[int, int], int] = defaultdict(int)
    for rank_log_id, brawlers in participants.items():
        rank_entry = dataset.rank_logs.get(rank_log_id)
        if rank_entry is None:
            continue
        for brawler_id in brawlers:
            usage[(rank_entry.map_id, brawler_id)] += 1

    star_counts: Dict[Tuple[int, int], int] = defaultdict(int)
    for rank_log_id, star_brawler_id in dataset.star_logs:
        rank_entry = dataset.rank_logs.get(rank_log_id)
        if rank_entry is None:
            continue
        star_counts[(rank_entry.map_id, star_brawler_id)] += 1

    rows: List[Tuple[int, int, int, int, int]] = []
    for (map_id, brawler_id), rank_logs in usage.items():
        rows.append(
            (
                int(map_id),
                int(brawler_id),
                int(rank_logs),
                int(star_counts.get((map_id, brawler_id), 0)),
                int(totals.get(map_id, 0)),
            )
        )
    return rows


def compute_star_rates(
    rows: List[Tuple[int, int, int, int, int]]
) -> Dict[int, Dict[int, Dict[str, float]]]:
    results: Dict[int, Dict[int, Dict[str, float]]] = {}
    for map_id, brawler_id, rank_logs, star_count, total_rank_logs in rows:
        map_stats = results.setdefault(map_id, {})
        if rank_logs <= 0:
            star_rate = 0.0
        else:
            star_rate = star_count / rank_logs
        usage_rate = rank_logs / total_rank_logs if total_rank_logs else 0.0
        map_stats[brawler_id] = {
            "rank_logs": int(rank_logs),
            "star_rate": star_rate,
            "usage_rate": usage_rate,
        }
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="スター取得率統計データをJSONとして出力")
    parser.add_argument("--output", default="star_rates.json", help="出力先JSONファイル")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    jst_now = datetime.now(timezone(timedelta(hours=9)))
    since = (jst_now - timedelta(days=DATA_RETENTION_DAYS)).strftime("%Y%m%d")
    logging.info("統計対象期間（日数）: %d", DATA_RETENTION_DAYS)

    try:
        conn = get_connection()
    except mysql.connector.Error as exc:
        raise SystemExit(f"データベースに接続できません: {exc}")

    try:
        logging.info("スター取得データを取得しています...")
        dataset = load_recent_ranked_battles(conn, since)
        rows = fetch_star_rows(dataset)
        logging.info("%d 行のデータを取得しました", len(rows))
    except mysql.connector.Error as exc:
        raise SystemExit(f"クエリの実行に失敗しました: {exc}")
    finally:
        conn.close()

    stats = compute_star_rates(rows)

    logging.info("JSONファイルに書き込んでいます: %s", args.output)
    with open(args.output, "w", encoding="utf-8") as fp:
        json.dump(stats, fp, ensure_ascii=False, indent=2)
    logging.info("JSON出力が完了しました")


if __name__ == "__main__":
    main()
