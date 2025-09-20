"""指定ランク以上のランクマッチ数をJSONとして出力するスクリプト."""

import argparse
import json
import logging
from typing import List, TypedDict

import mysql.connector

from .db import get_connection
from .logging_config import setup_logging
from .settings import MIN_RANK_ID


class RankMatchCount(TypedDict):
    """ランク別のランクマッチ集計結果."""

    rank_id: int
    name: str
    name_ja: str
    rank_log_count: int


setup_logging()


def fetch_rank_match_counts(conn) -> List[RankMatchCount]:
    """設定された最低ランク以上のランクごとのランクマッチ数を取得する."""

    query = """
        SELECT
            r.id AS rank_id,
            r.name,
            r.name_ja,
            COUNT(rl.id) AS rank_log_count
        FROM _ranks r
        LEFT JOIN rank_logs rl ON r.id = rl.rank_id
        WHERE r.id >= %s
        GROUP BY r.id, r.name, r.name_ja
        ORDER BY r.id
    """

    cursor = conn.cursor()
    cursor.execute(query, (MIN_RANK_ID,))
    results: List[RankMatchCount] = []
    for rank_id, name, name_ja, rank_log_count in cursor.fetchall():
        results.append(
            RankMatchCount(
                rank_id=int(rank_id),
                name=str(name),
                name_ja=str(name_ja),
                rank_log_count=int(rank_log_count),
            )
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="設定ランク以上のランクマッチ数をJSONとして出力"
    )
    parser.add_argument(
        "--output",
        default="rank_match_counts.json",
        help="出力先JSONファイル",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as exc:
        raise SystemExit(f"データベースに接続できません: {exc}")

    try:
        logging.info("ランクマッチ数を取得しています...")
        results = fetch_rank_match_counts(conn)
        logging.info("%d 件のランク情報を取得しました", len(results))
    except mysql.connector.Error as exc:
        raise SystemExit(f"クエリの実行に失敗しました: {exc}")
    finally:
        conn.close()

    logging.info("JSONファイルに書き込んでいます: %s", args.output)
    with open(args.output, "w", encoding="utf-8") as fp:
        json.dump(results, fp, ensure_ascii=False, indent=2)
    logging.info("JSON出力が完了しました")


if __name__ == "__main__":
    main()
