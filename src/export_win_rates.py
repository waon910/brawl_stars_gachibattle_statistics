"""マップIDごとのキャラクター勝率指標をJSON形式で出力するスクリプト.

設定された日数の範囲で行われた設定ランク以上の試合を対象とし、
各キャラクターのバトルログ数（=試合数）とBeta-Binomial による下側信頼限界(LCB)
勝率を算出して出力する。
"""

import argparse
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import mysql.connector
from scipy.stats import beta

from .db import get_connection
from .logging_config import setup_logging
from .settings import CONFIDENCE_LEVEL, DATA_RETENTION_DAYS, MIN_RANK_ID

setup_logging()


def beta_lcb(alpha: float, beta_param: float, confidence: float = CONFIDENCE_LEVEL) -> float:
    """Beta分布の下側信頼限界を求める"""
    return float(beta.ppf(1 - confidence, alpha, beta_param))


def fetch_stats(conn, since: str) -> List[tuple]:
    cur = conn.cursor()
    sql = """
    WITH recent_battles AS (
        SELECT bl.id AS battle_log_id, rl.map_id
        FROM battle_logs bl
        JOIN rank_logs rl ON bl.rank_log_id = rl.id
        WHERE rl.rank_id >= %s AND SUBSTRING(rl.id,1,8) >= %s
    ), win_counts AS (
        SELECT rb.map_id,
               wl.win_brawler_id AS brawler_id,
               COUNT(DISTINCT wl.battle_log_id) AS wins
        FROM win_lose_logs wl
        JOIN recent_battles rb ON wl.battle_log_id = rb.battle_log_id
        GROUP BY rb.map_id, wl.win_brawler_id
    ), lose_counts AS (
        SELECT rb.map_id,
               wl.lose_brawler_id AS brawler_id,
               COUNT(DISTINCT wl.battle_log_id) AS losses
        FROM win_lose_logs wl
        JOIN recent_battles rb ON wl.battle_log_id = rb.battle_log_id
        GROUP BY rb.map_id, wl.lose_brawler_id
    ), combined AS (
        SELECT map_id, brawler_id, wins, 0 AS losses FROM win_counts
        UNION ALL
        SELECT map_id, brawler_id, 0 AS wins, losses FROM lose_counts
    )
    SELECT map_id, brawler_id,
           SUM(wins) AS wins,
           SUM(losses) AS losses
    FROM combined
    GROUP BY map_id, brawler_id
    """
    cur.execute(sql, (MIN_RANK_ID, since))
    return cur.fetchall()


def compute_win_rates(
    rows: List[tuple], *, confidence: float = CONFIDENCE_LEVEL
) -> Dict[int, Dict[int, Dict[str, float]]]:
    logging.info("データを集計しています...")
    # map_id -> brawler_tag -> {wins, games}
    stats: Dict[int, Dict[int, Dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: {"wins": 0.0, "games": 0.0})
    )
    for map_id, brawler_id, wins, losses in rows:
        # MySQLコネクタは SUM 関数の結果を decimal.Decimal で返すため
        # float と混在すると演算で TypeError が発生する。
        # ここではすべての値を明示的に float に変換して集計する。
        wins_f = float(wins)
        losses_f = float(losses)
        stats[map_id][brawler_id]["wins"] += wins_f
        stats[map_id][brawler_id]["games"] += wins_f + losses_f

    logging.info("勝率を計算しています...")
    results: Dict[int, Dict[int, Dict[str, float]]] = {}
    total_maps = len(stats)
    for idx, (map_id, brawlers) in enumerate(stats.items(), 1):
        logging.info("%d/%d %s を処理中", idx, total_maps, map_id)
        total_wins = sum(v["wins"] for v in brawlers.values())
        total_games = sum(v["games"] for v in brawlers.values())
        if total_games == 0 or len(brawlers) == 0:
            results[map_id] = {}
            continue
        mean = total_wins / total_games
        strength = total_games / len(brawlers)
        alpha_prior = mean * strength
        beta_prior = (1 - mean) * strength

        map_result: Dict[int, Dict[str, float]] = {}
        for brawler_id, val in brawlers.items():
            alpha_post = alpha_prior + val["wins"]
            beta_post = beta_prior + val["games"] - val["wins"]
            lcb = beta_lcb(alpha_post, beta_post, confidence=confidence)
            map_result[brawler_id] = {
                "games": int(round(val["games"])),
                "win_rate_lcb": lcb,
            }
        results[map_id] = map_result
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="勝率統計データをJSONとして出力")
    parser.add_argument("--output", default="win_rates.json", help="出力先JSONファイル")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    jst_now = datetime.now(timezone(timedelta(hours=9)))
    since = (jst_now - timedelta(days=DATA_RETENTION_DAYS)).strftime("%Y%m%d")
    logging.info("統計対象期間（日数）: %d", DATA_RETENTION_DAYS)
    logging.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as e:
        raise SystemExit(f"データベースに接続できません: {e}")
    try:
        logging.info("統計情報を取得しています...")
        rows = fetch_stats(conn, since)
        logging.info("%d 行のデータを取得しました", len(rows))
    except mysql.connector.Error as e:
        raise SystemExit(f"クエリの実行に失敗しました: {e}")
    finally:
        conn.close()

    result = compute_win_rates(rows, confidence=CONFIDENCE_LEVEL)
    logging.info("JSONファイルに書き込んでいます: %s", args.output)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logging.info("JSON出力が完了しました")


if __name__ == "__main__":
    main()
