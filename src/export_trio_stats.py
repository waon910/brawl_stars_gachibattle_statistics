"""ダイヤモンド以上のトリオ勝率指標をJSONとして出力するスクリプト."""
from __future__ import annotations

import argparse
import json
import logging
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import mysql.connector

from .db import get_connection
from .logging_config import setup_logging
from .settings import DATA_RETENTION_DAYS
from .trio_stats import compute_trio_scores, fetch_trio_rows

setup_logging()
JST = timezone(timedelta(hours=9))


def export_trio_json(
    results: Dict[int, Dict[Optional[int], List[Dict[str, object]]]],
    output_dir: Path,
) -> None:
    """計算結果をマップIDごとのJSONとして出力する."""
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for map_id, ranks in results.items():
        combos = ranks.get(None, [])
        simplified = [
            {
                "brawlers": combo["brawlers"],
                "win_rate_lcb": combo["win_rate_lcb"],
            }
            for combo in combos
        ]
        out_file = output_dir / f"{map_id}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(simplified, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="トリオ勝率統計をJSONとして出力")
    parser.add_argument(
        "--output-dir",
        default="trio_stats",
        help="出力先ディレクトリ",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    since = (datetime.now(JST) - timedelta(days=DATA_RETENTION_DAYS)).strftime("%Y%m%d")
    logging.info("統計対象期間（日数）: %d", DATA_RETENTION_DAYS)
    logging.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as exc:  # pragma: no cover - 接続エラーは即時終了
        raise SystemExit(f"データベースに接続できません: {exc}")

    try:
        logging.info("トリオ編成データを取得しています...")
        rows = fetch_trio_rows(conn, since=since)
        logging.info("%d 行のトリオデータを取得", len(rows))
    except mysql.connector.Error as exc:  # pragma: no cover - クエリエラー
        raise SystemExit(f"クエリの実行に失敗しました: {exc}")
    finally:
        conn.close()

    logging.info("勝率指標を計算しています")
    results = compute_trio_scores(rows, group_by_rank=False)
    output_dir = Path(args.output_dir)
    logging.info("JSONを出力しています: %s", output_dir)
    export_trio_json(results, output_dir)
    logging.info("トリオ統計の出力が完了しました")


if __name__ == "__main__":
    main()
