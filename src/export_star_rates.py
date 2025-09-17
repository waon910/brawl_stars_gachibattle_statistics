"""スター取得率をマップ・ランク・キャラ別に集計してJSON出力するスクリプト."""

import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict

import mysql.connector

from .db import get_connection
from .logging_config import setup_logging
from .settings import DATA_RETENTION_DAYS

setup_logging()


def fetch_star_rows(conn, since: str) -> list[tuple[int, int, int, int, int]]:
    cursor = conn.cursor()
    sql = """
    WITH recent_ranks AS (
        SELECT rl.id, rl.map_id, rl.rank_id
        FROM rank_logs rl
        WHERE SUBSTRING(rl.id, 1, 8) >= %s
    ), totals AS (
        SELECT map_id, rank_id, COUNT(*) AS total_matches
        FROM recent_ranks
        GROUP BY map_id, rank_id
    )
    SELECT rr.map_id, rr.rank_id, rsl.star_brawler_id, COUNT(*) AS star_count, t.total_matches
    FROM recent_ranks rr
    JOIN rank_star_logs rsl ON rr.id = rsl.rank_log_id
    JOIN totals t ON rr.map_id = t.map_id AND rr.rank_id = t.rank_id
    GROUP BY rr.map_id, rr.rank_id, rsl.star_brawler_id, t.total_matches
    """
    cursor.execute(sql, (since,))
    return cursor.fetchall()


def build_star_stats(rows: list[tuple[int, int, int, int, int]]) -> Dict[int, Dict[int, Dict[int, Dict[str, float]]]]:
    results: Dict[int, Dict[int, Dict[int, Dict[str, float]]]] = {}
    for map_id, rank_id, brawler_id, star_count, total_matches in rows:
        map_stats = results.setdefault(map_id, {})
        rank_stats = map_stats.setdefault(rank_id, {})
        rate = star_count / total_matches if total_matches else 0.0
        rank_stats[brawler_id] = {
            "star_count": star_count,
            "total_matches": total_matches,
            "star_rate": rate,
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

    stats = build_star_stats(rows)

    logging.info("JSONファイルに書き込んでいます: %s", args.output)
    with open(args.output, "w", encoding="utf-8") as fp:
        json.dump(stats, fp, ensure_ascii=False, indent=2)
    logging.info("JSON出力が完了しました")


if __name__ == "__main__":
    main()
