"""マップごとのキャラクター対キャラクターの強さと仲間としての相性をJSONで出力するスクリプト."""

import argparse
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple
from pathlib import Path

import mysql.connector
from scipy.stats import beta

from .db import get_connection
from .logging_config import setup_logging
setup_logging()
JST = timezone(timedelta(hours=9))


def beta_lcb(alpha: float, beta_param: float, confidence: float = 0.95) -> float:
    """Beta分布の下側信頼限界を計算する"""
    return beta.ppf(1 - confidence, alpha, beta_param)


def fetch_matchup_stats(conn, since: str) -> List[Tuple[int, int, int, float, float]]:
    """対キャラ勝率用の集計データを取得"""
    cur = conn.cursor()
    sql = """
    WITH recent_battles AS (
        SELECT bl.id AS battle_log_id, rl.map_id
        FROM battle_logs bl
        JOIN rank_logs rl ON bl.rank_log_id = rl.id
        WHERE rl.rank_id >= 4 AND SUBSTRING(rl.id,1,8) >= %s
    ), pair_results AS (
        SELECT rb.map_id, wl.win_brawler_id, wl.lose_brawler_id, COUNT(*) AS win_cnt
        FROM win_lose_logs wl
        JOIN recent_battles rb ON wl.battle_log_id = rb.battle_log_id
        GROUP BY rb.map_id, wl.win_brawler_id, wl.lose_brawler_id
    ), combined AS (
        SELECT map_id, win_brawler_id AS brawler_a, lose_brawler_id AS brawler_b,
               win_cnt AS wins, 0 AS losses
        FROM pair_results
        UNION ALL
        SELECT map_id, lose_brawler_id AS brawler_a, win_brawler_id AS brawler_b,
               0 AS wins, win_cnt AS losses
        FROM pair_results
    )
    SELECT map_id, brawler_a, brawler_b, SUM(wins) AS wins, SUM(losses) AS losses
    FROM combined
    GROUP BY map_id, brawler_a, brawler_b
    """
    cur.execute(sql, (since,))
    return cur.fetchall()


def fetch_synergy_stats(conn, since: str) -> List[Tuple[int, int, int, float, float]]:
    """味方キャラ同士の勝率用の集計データを取得"""
    cur = conn.cursor()
    sql = """
    WITH recent_battles AS (
        SELECT bl.id AS battle_log_id, rl.map_id
        FROM battle_logs bl
        JOIN rank_logs rl ON bl.rank_log_id = rl.id
        WHERE rl.rank_id >= 4 AND SUBSTRING(rl.id,1,8) >= %s
    ), win_pairs AS (
        SELECT rb.map_id, wl1.win_brawler_id AS brawler_a, wl2.win_brawler_id AS brawler_b
        FROM win_lose_logs wl1
        JOIN win_lose_logs wl2 ON wl1.battle_log_id = wl2.battle_log_id
                                AND wl1.win_brawler_id < wl2.win_brawler_id
        JOIN recent_battles rb ON wl1.battle_log_id = rb.battle_log_id
    ), lose_pairs AS (
        SELECT rb.map_id, wl1.lose_brawler_id AS brawler_a, wl2.lose_brawler_id AS brawler_b
        FROM win_lose_logs wl1
        JOIN win_lose_logs wl2 ON wl1.battle_log_id = wl2.battle_log_id
                                AND wl1.lose_brawler_id < wl2.lose_brawler_id
        JOIN recent_battles rb ON wl1.battle_log_id = rb.battle_log_id
    ), win_counts AS (
        SELECT map_id, brawler_a, brawler_b, COUNT(*) AS wins
        FROM win_pairs
        GROUP BY map_id, brawler_a, brawler_b
    ), lose_counts AS (
        SELECT map_id, brawler_a, brawler_b, COUNT(*) AS losses
        FROM lose_pairs
        GROUP BY map_id, brawler_a, brawler_b
    ), combined AS (
        SELECT map_id, brawler_a, brawler_b, wins, 0 AS losses FROM win_counts
        UNION ALL
        SELECT map_id, brawler_a, brawler_b, 0 AS wins, losses FROM lose_counts
    )
    SELECT map_id, brawler_a, brawler_b, SUM(wins) AS wins, SUM(losses) AS losses
    FROM combined
    GROUP BY map_id, brawler_a, brawler_b
    """
    cur.execute(sql, (since,))
    return cur.fetchall()


def compute_pair_rates(rows: List[Tuple[int, int, int, float, float]], symmetrical: bool) -> Dict[int, Dict[int, Dict[int, float]]]:
    """Beta-Binomialに基づきLCBを算出"""
    stats: Dict[int, Dict[Tuple[int, int], Dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: {"wins": 0.0, "games": 0.0})
    )
    for map_id, b1, b2, wins, losses in rows:
        wins_f = float(wins)
        losses_f = float(losses)
        stats[map_id][(b1, b2)]["wins"] += wins_f
        stats[map_id][(b1, b2)]["games"] += wins_f + losses_f

    results: Dict[int, Dict[int, Dict[int, float]]] = {}
    for map_id, pairs in stats.items():
        total_wins = sum(v["wins"] for v in pairs.values())
        total_games = sum(v["games"] for v in pairs.values())
        if total_games == 0 or len(pairs) == 0:
            results[map_id] = {}
            continue
        mean = total_wins / total_games
        strength = total_games / len(pairs)
        alpha_prior = mean * strength
        beta_prior = (1 - mean) * strength

        map_result: Dict[int, Dict[int, float]] = defaultdict(dict)
        for (b1, b2), val in pairs.items():
            alpha_post = alpha_prior + val["wins"]
            beta_post = beta_prior + val["games"] - val["wins"]
            lcb = beta_lcb(alpha_post, beta_post)
            map_result[b1][b2] = lcb
            if symmetrical:
                map_result[b2][b1] = lcb
        results[map_id] = map_result
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="対キャラ・協力勝率をJSONとして出力")
    parser.add_argument(
        "--output-dir",
        default="pair_stats",
        help="出力先ディレクトリ",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    since = (datetime.now(JST) - timedelta(days=30)).strftime("%Y%m%d")

    logging.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as e:
        raise SystemExit(f"データベースに接続できません: {e}")

    try:
        logging.info("対キャラデータを取得しています...")
        matchup_rows = fetch_matchup_stats(conn, since)
        logging.info("%d 行の対キャラデータを取得", len(matchup_rows))
        logging.info("協力データを取得しています...")
        synergy_rows = fetch_synergy_stats(conn, since)
        logging.info("%d 行の協力データを取得", len(synergy_rows))
    except mysql.connector.Error as e:
        raise SystemExit(f"クエリの実行に失敗しました: {e}")
    finally:
        conn.close()

    matchup_result = compute_pair_rates(matchup_rows, symmetrical=False)
    synergy_result = compute_pair_rates(synergy_rows, symmetrical=True)
    result = {"matchup": matchup_result, "synergy": synergy_result}

    base_dir = Path(args.output_dir)
    logging.info("ディレクトリに分割して書き込んでいます: %s", base_dir)
    for kind, maps in result.items():
        kind_dir = base_dir / kind
        kind_dir.mkdir(parents=True, exist_ok=True)
        for map_id, data in maps.items():
            out_file = kind_dir / f"{map_id}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info("JSON出力が完了しました")


if __name__ == "__main__":
    main()
