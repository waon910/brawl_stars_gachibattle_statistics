"""設定ランク以上のトリオ勝率指標をJSONとして出力するスクリプト."""
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
from .settings import CONFIDENCE_LEVEL, DATA_RETENTION_DAYS
from .stats_loader import load_recent_ranked_battles
from .trio_stats import compute_trio_scores, fetch_trio_rows

logger = logging.getLogger(__name__)
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
                "games": combo["games"],
                "win_rate_lcb": combo["win_rate_lcb"],
            }
            for combo in combos
        ]
        out_file = output_dir / f"{map_id}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(simplified, f, ensure_ascii=False, indent=2)


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="トリオ勝率統計をJSONとして出力")
    parser.add_argument(
        "--output-dir",
        default="trio_stats",
        help="出力先ディレクトリ",
    )
    args = parser.parse_args()

    since = (datetime.now(JST) - timedelta(days=DATA_RETENTION_DAYS)).strftime("%Y%m%d")
    logger.info("統計対象期間（日数）: %d", DATA_RETENTION_DAYS)
    logger.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as exc:  # pragma: no cover - 接続エラーは即時終了
        raise SystemExit(f"データベースに接続できません: {exc}")

    try:
        logger.info("トリオ編成データを取得しています...")
        dataset = load_recent_ranked_battles(conn, since)
        rows = fetch_trio_rows(dataset=dataset, since=since)
        logger.info("%d 行のトリオデータを取得", len(rows))
    except mysql.connector.Error as exc:  # pragma: no cover - クエリエラー
        raise SystemExit(f"クエリの実行に失敗しました: {exc}")
    finally:
        conn.close()

    logger.info("勝率指標を計算しています")
    results = compute_trio_scores(
        rows,
        group_by_rank=False,
        confidence=CONFIDENCE_LEVEL,
    )
    output_dir = Path(args.output_dir)
    logger.info("JSONを出力しています: %s", output_dir)
    export_trio_json(results, output_dir)
    logger.info("トリオ統計の出力が完了しました")


if __name__ == "__main__":
    main()
