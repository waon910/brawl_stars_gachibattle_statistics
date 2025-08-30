"""マップIDごとのキャラクタータグ勝率をJSON形式で出力するスクリプト.

過去30日間に行われたダイヤモンドランク以上の試合を対象とし、
勝率はEmpirical Bayes(Beta-Binomial)による縮約と95%下側信頼区間(LCB)で算出する。
"""

import argparse
import json
import logging
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import mysql.connector

from .db import get_connection
from .logging_config import setup_logging

# Monte Carloサンプリング数
SAMPLE_SIZE = 10000
random.seed(0)
setup_logging()


def beta_lcb(alpha: float, beta: float, confidence: float = 0.95) -> float:
    """Beta分布の下側信頼限界をモンテカルロ法で近似する"""
    samples = [random.betavariate(alpha, beta) for _ in range(SAMPLE_SIZE)]
    samples.sort()
    index = int((1 - confidence) * len(samples))
    return samples[index]


def fetch_stats(conn, since: str) -> List[tuple]:
    cur = conn.cursor()
    sql = """
    WITH recent_battles AS (
        SELECT bl.id AS battle_log_id, rl.map_id
        FROM battle_logs bl
        JOIN rank_logs rl ON bl.rank_log_id = rl.id
        WHERE rl.rank_id >= 4 AND SUBSTRING(rl.id,1,8) >= %s
    ), pair_counts AS (
        SELECT wl.battle_log_id,
               COUNT(DISTINCT wl.win_brawler_id) AS win_cnt,
               COUNT(DISTINCT wl.lose_brawler_id) AS lose_cnt
        FROM win_lose_logs wl
        JOIN recent_battles rb ON wl.battle_log_id = rb.battle_log_id
        GROUP BY wl.battle_log_id
    ), win_results AS (
        SELECT rb.map_id, wl.win_brawler_id AS brawler_id,
               SUM(1.0 / pc.lose_cnt) AS wins, 0.0 AS losses
        FROM win_lose_logs wl
        JOIN recent_battles rb ON wl.battle_log_id = rb.battle_log_id
        JOIN pair_counts pc ON wl.battle_log_id = pc.battle_log_id
        GROUP BY rb.map_id, wl.win_brawler_id
    ), lose_results AS (
        SELECT rb.map_id, wl.lose_brawler_id AS brawler_id,
               0.0 AS wins, SUM(1.0 / pc.win_cnt) AS losses
        FROM win_lose_logs wl
        JOIN recent_battles rb ON wl.battle_log_id = rb.battle_log_id
        JOIN pair_counts pc ON wl.battle_log_id = pc.battle_log_id
        GROUP BY rb.map_id, wl.lose_brawler_id
    ), weighted_results AS (
        SELECT * FROM win_results
        UNION ALL
        SELECT * FROM lose_results
    )
    SELECT wr.map_id, wr.brawler_id,
           SUM(wr.wins) AS wins, SUM(wr.losses) AS losses
    FROM weighted_results wr
    GROUP BY wr.map_id, wr.brawler_id
    """
    cur.execute(sql, (since,))
    return cur.fetchall()


def compute_win_rates(rows: List[tuple]) -> Dict[int, Dict[int, float]]:
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
    results: Dict[int, Dict[int, float]] = {}
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

        map_result: Dict[int, float] = {}
        for brawler_id, val in brawlers.items():
            alpha_post = alpha_prior + val["wins"]
            beta_post = beta_prior + val["games"] - val["wins"]
            lcb = beta_lcb(alpha_post, beta_post)
            map_result[brawler_id] = lcb
        results[map_id] = map_result
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="勝率統計データをJSONとして出力")
    parser.add_argument("--output", default="win_rates.json", help="出力先JSONファイル")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    jst_now = datetime.now(timezone(timedelta(hours=9)))
    since = (jst_now - timedelta(days=30)).strftime("%Y%m%d")
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

    result = compute_win_rates(rows)
    logging.info("JSONファイルに書き込んでいます: %s", args.output)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logging.info("JSON出力が完了しました")


if __name__ == "__main__":
    main()
