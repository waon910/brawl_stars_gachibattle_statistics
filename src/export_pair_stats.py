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
from .settings import CONFIDENCE_LEVEL, DATA_RETENTION_DAYS
from .stats_loader import StatsDataset, load_recent_ranked_battles
setup_logging()
JST = timezone(timedelta(hours=9))


def beta_lcb(alpha: float, beta_param: float, confidence: float = CONFIDENCE_LEVEL) -> float:
    """Beta分布の下側信頼限界を計算する"""
    return beta.ppf(1 - confidence, alpha, beta_param)


def fetch_matchup_stats(dataset: StatsDataset) -> List[Tuple[int, int, int, float, float]]:
    """共通データセットから対キャラ勝敗集計を生成する."""

    stats: Dict[Tuple[int, int, int], Dict[str, float]] = defaultdict(
        lambda: {"wins": 0.0, "losses": 0.0}
    )
    for battle in dataset.iter_ranked_battles():
        if not battle.win_brawlers or not battle.lose_brawlers:
            continue
        for winner in battle.win_brawlers:
            for loser in battle.lose_brawlers:
                stats[(battle.map_id, winner, loser)]["wins"] += 1.0
                stats[(battle.map_id, loser, winner)]["losses"] += 1.0

    rows: List[Tuple[int, int, int, float, float]] = []
    for (map_id, brawler_a, brawler_b), record in stats.items():
        rows.append((map_id, brawler_a, brawler_b, record["wins"], record["losses"]))
    return rows


def fetch_synergy_stats(dataset: StatsDataset) -> List[Tuple[int, int, int, float, float]]:
    """共通データセットから味方同士の勝敗集計を生成する."""

    stats: Dict[Tuple[int, int, int], Dict[str, float]] = defaultdict(
        lambda: {"wins": 0.0, "losses": 0.0}
    )
    for battle in dataset.iter_ranked_battles():
        if battle.win_brawlers and len(battle.win_brawlers) >= 2:
            winners = sorted(battle.win_brawlers)
            for i, brawler_a in enumerate(winners):
                for brawler_b in winners[i + 1 :]:
                    stats[(battle.map_id, brawler_a, brawler_b)]["wins"] += 1.0
        if battle.lose_brawlers and len(battle.lose_brawlers) >= 2:
            losers = sorted(battle.lose_brawlers)
            for i, brawler_a in enumerate(losers):
                for brawler_b in losers[i + 1 :]:
                    stats[(battle.map_id, brawler_a, brawler_b)]["losses"] += 1.0

    rows: List[Tuple[int, int, int, float, float]] = []
    for (map_id, brawler_a, brawler_b), record in stats.items():
        rows.append((map_id, brawler_a, brawler_b, record["wins"], record["losses"]))
    return rows


def compute_pair_rates(
    rows: List[Tuple[int, int, int, float, float]],
    symmetrical: bool,
    *,
    confidence: float = CONFIDENCE_LEVEL,
) -> Dict[int, Dict[int, Dict[int, Dict[str, float]]]]:
    """Beta-Binomialに基づきLCBを算出"""
    stats: Dict[int, Dict[Tuple[int, int], Dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: {"wins": 0.0, "games": 0.0})
    )
    for map_id, b1, b2, wins, losses in rows:
        wins_f = float(wins)
        losses_f = float(losses)
        stats[map_id][(b1, b2)]["wins"] += wins_f
        stats[map_id][(b1, b2)]["games"] += wins_f + losses_f

    results: Dict[int, Dict[int, Dict[int, Dict[str, float]]]] = {}
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

        map_result: Dict[int, Dict[int, Dict[str, float]]] = defaultdict(dict)
        for (b1, b2), val in pairs.items():
            alpha_post = alpha_prior + val["wins"]
            beta_post = beta_prior + val["games"] - val["wins"]
            lcb = beta_lcb(alpha_post, beta_post, confidence=confidence)
            record = {
                "games": int(round(val["games"])),
                "win_rate_lcb": lcb,
            }
            map_result[b1][b2] = record
            if symmetrical:
                map_result[b2][b1] = record
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
    since = (datetime.now(JST) - timedelta(days=DATA_RETENTION_DAYS)).strftime("%Y%m%d")
    logging.info("統計対象期間（日数）: %d", DATA_RETENTION_DAYS)

    logging.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as e:
        raise SystemExit(f"データベースに接続できません: {e}")

    try:
        logging.info("対キャラデータを取得しています...")
        dataset = load_recent_ranked_battles(conn, since)
        matchup_rows = fetch_matchup_stats(dataset)
        logging.info("%d 行の対キャラデータを取得", len(matchup_rows))
        logging.info("協力データを取得しています...")
        synergy_rows = fetch_synergy_stats(dataset)
        logging.info("%d 行の協力データを取得", len(synergy_rows))
    except mysql.connector.Error as e:
        raise SystemExit(f"クエリの実行に失敗しました: {e}")
    finally:
        conn.close()

    matchup_result = compute_pair_rates(matchup_rows, symmetrical=False, confidence=CONFIDENCE_LEVEL)
    synergy_result = compute_pair_rates(synergy_rows, symmetrical=True, confidence=CONFIDENCE_LEVEL)
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
