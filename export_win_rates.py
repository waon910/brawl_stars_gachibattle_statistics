"""Mapごとのキャラクター勝率をJSON形式で出力するスクリプト.

過去30日間に行われたダイヤモンドランク以上の試合を対象とし、
勝率はEmpirical Bayes(Beta-Binomial)による縮約と95%下側信頼区間(LCB)で算出する。
"""

import argparse
import json
import random
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List

# Monte Carloサンプリング数
SAMPLE_SIZE = 10000
random.seed(0)


def beta_lcb(alpha: float, beta: float, confidence: float = 0.95) -> float:
    """Beta分布の下側信頼限界をモンテカルロ法で近似する"""
    samples = [random.betavariate(alpha, beta) for _ in range(SAMPLE_SIZE)]
    samples.sort()
    index = int((1 - confidence) * len(samples))
    return samples[index]


def fetch_stats(conn: sqlite3.Connection, since: str) -> List[tuple]:
    cur = conn.cursor()
    sql = """
    WITH pair_counts AS (
        SELECT battle_log_id,
               COUNT(DISTINCT win_brawler_id) AS win_cnt,
               COUNT(DISTINCT lose_brawler_id) AS lose_cnt
        FROM win_lose_logs
        GROUP BY battle_log_id
    ), recent_battles AS (
        SELECT bl.id AS battle_log_id, rl.map_id, substr(rl.id,1,8) AS battle_date
        FROM battle_logs bl
        JOIN rank_logs rl ON bl.rank_log_id = rl.id
        WHERE rl.rank_id >= 4 AND substr(rl.id,1,8) >= ?
    ), weighted_results AS (
        SELECT rb.map_id, wl.win_brawler_id AS brawler_id, 1.0/pc.lose_cnt AS win, 0.0 AS loss
        FROM win_lose_logs wl
        JOIN recent_battles rb ON wl.battle_log_id = rb.battle_log_id
        JOIN pair_counts pc ON wl.battle_log_id = pc.battle_log_id
        UNION ALL
        SELECT rb.map_id, wl.lose_brawler_id AS brawler_id, 0.0 AS win, 1.0/pc.win_cnt AS loss
        FROM win_lose_logs wl
        JOIN recent_battles rb ON wl.battle_log_id = rb.battle_log_id
        JOIN pair_counts pc ON wl.battle_log_id = pc.battle_log_id
    )
    SELECT m.name_ja AS map_name, b.name_ja AS brawler_name,
           SUM(win) AS wins, SUM(loss) AS losses
    FROM weighted_results wr
    JOIN _maps m ON m.id = wr.map_id
    JOIN _brawlers b ON b.id = wr.brawler_id
    GROUP BY wr.map_id, wr.brawler_id
    """
    cur.execute(sql, (since,))
    return cur.fetchall()


def compute_win_rates(rows: List[tuple]) -> Dict[str, Dict[str, float]]:
    # map -> brawler -> {wins, games}
    stats: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(lambda: defaultdict(lambda: {"wins": 0.0, "games": 0.0}))
    for map_name, brawler_name, wins, losses in rows:
        stats[map_name][brawler_name]["wins"] += wins
        stats[map_name][brawler_name]["games"] += wins + losses

    results: Dict[str, Dict[str, float]] = {}
    for map_name, brawlers in stats.items():
        total_wins = sum(v["wins"] for v in brawlers.values())
        total_games = sum(v["games"] for v in brawlers.values())
        if total_games == 0 or len(brawlers) == 0:
            results[map_name] = {}
            continue
        mean = total_wins / total_games
        strength = total_games / len(brawlers)
        alpha_prior = mean * strength
        beta_prior = (1 - mean) * strength

        map_result: Dict[str, float] = {}
        for brawler_name, val in brawlers.items():
            alpha_post = alpha_prior + val["wins"]
            beta_post = beta_prior + val["games"] - val["wins"]
            lcb = beta_lcb(alpha_post, beta_post)
            map_result[brawler_name] = lcb
        results[map_name] = map_result
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="勝率統計データをJSONとして出力")
    parser.add_argument("--db", default="brawl_stats.db", help="SQLiteデータベースファイル")
    parser.add_argument("--output", default="win_rates.json", help="出力先JSONファイル")
    args = parser.parse_args()

    since = (datetime.utcnow() - timedelta(days=30)).strftime("%Y%m%d")
    try:
        conn = sqlite3.connect(args.db)
    except sqlite3.Error as e:
        raise SystemExit(f"データベースに接続できません: {e}")
    try:
        rows = fetch_stats(conn, since)
    except sqlite3.Error as e:
        raise SystemExit(f"クエリの実行に失敗しました: {e}")
    finally:
        conn.close()

    result = compute_win_rates(rows)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
