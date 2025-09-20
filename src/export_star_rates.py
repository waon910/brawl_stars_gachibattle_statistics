"""スター取得率をマップ・キャラ別に集計してJSON出力するスクリプト."""

import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import mysql.connector

from .db import get_connection
from .logging_config import setup_logging
from .settings import DATA_RETENTION_DAYS, MIN_RANK_ID

setup_logging()


def fetch_star_rows(conn, since: str) -> List[Tuple[int, int, int, int, int]]:
    cursor = conn.cursor()
    sql = """
    WITH recent_ranks AS (
        SELECT rl.id, rl.map_id
        FROM rank_logs rl
        WHERE rl.rank_id >= %s AND SUBSTRING(rl.id, 1, 8) >= %s
    ), totals AS (
        SELECT map_id, COUNT(*) AS total_rank_logs
        FROM recent_ranks
        GROUP BY map_id
    ), participants AS (
        SELECT DISTINCT rr.id AS rank_log_id, wl.win_brawler_id AS brawler_id
        FROM win_lose_logs wl
        JOIN battle_logs bl ON wl.battle_log_id = bl.id
        JOIN recent_ranks rr ON bl.rank_log_id = rr.id
        UNION
        SELECT DISTINCT rr.id AS rank_log_id, wl.lose_brawler_id AS brawler_id
        FROM win_lose_logs wl
        JOIN battle_logs bl ON wl.battle_log_id = bl.id
        JOIN recent_ranks rr ON bl.rank_log_id = rr.id
    ), usage AS (
        SELECT rr.map_id,
               p.brawler_id,
               COUNT(DISTINCT p.rank_log_id) AS rank_logs
        FROM participants p
        JOIN recent_ranks rr ON p.rank_log_id = rr.id
        GROUP BY rr.map_id, p.brawler_id
    ), star_counts AS (
        SELECT rr.map_id, rsl.star_brawler_id AS brawler_id, COUNT(*) AS star_count
        FROM rank_star_logs rsl
        JOIN recent_ranks rr ON rsl.rank_log_id = rr.id
        GROUP BY rr.map_id, rsl.star_brawler_id
    )
    SELECT u.map_id,
           u.brawler_id,
           u.rank_logs,
           COALESCE(sc.star_count, 0) AS star_count,
           t.total_rank_logs
    FROM usage u
    JOIN totals t ON u.map_id = t.map_id
    LEFT JOIN star_counts sc
        ON u.map_id = sc.map_id AND u.brawler_id = sc.brawler_id
    """
    cursor.execute(sql, (MIN_RANK_ID, since))
    return cursor.fetchall()


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
        rows = fetch_star_rows(conn, since)
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
