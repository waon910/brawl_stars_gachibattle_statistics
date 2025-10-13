"""指定の最高ランクを達成したプレイヤー名をJSONとして出力するスクリプト."""

from __future__ import annotations

import argparse
import json
import logging
from typing import List

import mysql.connector

from .db import get_connection
from .logging_config import setup_logging


setup_logging()


def fetch_highest_rank_players(conn, rank: int) -> List[str]:
    """指定された最高ランクを持つプレイヤー名の一覧を取得する."""

    query = """
        SELECT name
        FROM players
        WHERE highest_rank = %s
          AND name IS NOT NULL
          AND name <> ''
        ORDER BY name
    """

    cursor = conn.cursor()
    cursor.execute(query, (rank,))
    return [str(row[0]) for row in cursor.fetchall()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="指定の最高ランクを達成したプレイヤー名をJSONとして出力"
    )
    parser.add_argument(
        "--rank",
        type=int,
        default=22,
        help="対象とする最高ランク",
    )
    parser.add_argument(
        "--output",
        default="highest_rank_players.json",
        help="出力先JSONファイル",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as exc:  # pragma: no cover - DB接続エラー
        raise SystemExit(f"データベースに接続できません: {exc}")

    try:
        logging.info("最高ランク %d のプレイヤーを取得しています...", args.rank)
        player_names = fetch_highest_rank_players(conn, args.rank)
        logging.info("%d 件のプレイヤー名を取得しました", len(player_names))
    except mysql.connector.Error as exc:  # pragma: no cover - クエリエラー
        raise SystemExit(f"クエリの実行に失敗しました: {exc}")
    finally:
        conn.close()

    logging.info("JSONファイルに書き込んでいます: %s", args.output)
    with open(args.output, "w", encoding="utf-8") as fp:
        json.dump(player_names, fp, ensure_ascii=False, indent=2)
    logging.info("JSON出力が完了しました")


if __name__ == "__main__":
    main()

