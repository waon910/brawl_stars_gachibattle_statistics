"""3対3編成ごとの勝率指標をJSONとして出力するスクリプト."""

from __future__ import annotations

import argparse
import json
import logging
import shutil
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Tuple

import mysql.connector
from scipy.stats import beta

from .db import get_connection
from .logging_config import setup_logging
from .settings import CONFIDENCE_LEVEL, DATA_RETENTION_DAYS, MIN_RANK_ID

setup_logging()
JST = timezone(timedelta(hours=9))

MatchupRow = Tuple[int, int, int, int, int, int, int, float]


def beta_lcb(alpha: float, beta_param: float, confidence: float = CONFIDENCE_LEVEL) -> float:
    """Beta分布の下側信頼限界を計算する."""

    return float(beta.ppf(1 - confidence, alpha, beta_param))


def fetch_matchup_rows(conn, since: str) -> List[MatchupRow]:
    """3対3の編成ごとの勝敗集計を取得する."""

    cursor = conn.cursor()
    query = """
        WITH recent_battles AS (
            SELECT bl.id AS battle_log_id,
                   rl.map_id
            FROM battle_logs bl
            JOIN rank_logs rl ON bl.rank_log_id = rl.id
            WHERE rl.rank_id >= %s AND SUBSTRING(rl.id,1,8) >= %s
        ),
        win_brawlers AS (
            SELECT DISTINCT wl.battle_log_id, wl.win_brawler_id AS brawler_id
            FROM win_lose_logs wl
            JOIN recent_battles rb ON wl.battle_log_id = rb.battle_log_id
        ),
        lose_brawlers AS (
            SELECT DISTINCT wl.battle_log_id, wl.lose_brawler_id AS brawler_id
            FROM win_lose_logs wl
            JOIN recent_battles rb ON wl.battle_log_id = rb.battle_log_id
        ),
        win_trios AS (
            SELECT rb.map_id,
                   wb1.battle_log_id,
                   wb1.brawler_id AS brawler_a,
                   wb2.brawler_id AS brawler_b,
                   wb3.brawler_id AS brawler_c
            FROM win_brawlers wb1
            JOIN win_brawlers wb2
                ON wb1.battle_log_id = wb2.battle_log_id
               AND wb1.brawler_id < wb2.brawler_id
            JOIN win_brawlers wb3
                ON wb1.battle_log_id = wb3.battle_log_id
               AND wb2.brawler_id < wb3.brawler_id
            JOIN recent_battles rb ON wb1.battle_log_id = rb.battle_log_id
        ),
        lose_trios AS (
            SELECT rb.map_id,
                   lb1.battle_log_id,
                   lb1.brawler_id AS brawler_a,
                   lb2.brawler_id AS brawler_b,
                   lb3.brawler_id AS brawler_c
            FROM lose_brawlers lb1
            JOIN lose_brawlers lb2
                ON lb1.battle_log_id = lb2.battle_log_id
               AND lb1.brawler_id < lb2.brawler_id
            JOIN lose_brawlers lb3
                ON lb1.battle_log_id = lb3.battle_log_id
               AND lb2.brawler_id < lb3.brawler_id
            JOIN recent_battles rb ON lb1.battle_log_id = rb.battle_log_id
        )
        SELECT wt.map_id,
               wt.brawler_a AS win_a,
               wt.brawler_b AS win_b,
               wt.brawler_c AS win_c,
               lt.brawler_a AS lose_a,
               lt.brawler_b AS lose_b,
               lt.brawler_c AS lose_c,
               COUNT(*) AS wins
        FROM win_trios wt
        JOIN lose_trios lt ON wt.battle_log_id = lt.battle_log_id
        GROUP BY wt.map_id,
                 win_a,
                 win_b,
                 win_c,
                 lose_a,
                 lose_b,
                 lose_c
    """
    cursor.execute(query, (MIN_RANK_ID, since))
    rows: List[MatchupRow] = [
        (
            int(map_id),
            int(win_a),
            int(win_b),
            int(win_c),
            int(lose_a),
            int(lose_b),
            int(lose_c),
            float(wins),
        )
        for map_id, win_a, win_b, win_c, lose_a, lose_b, lose_c, wins in cursor.fetchall()
    ]
    cursor.close()
    return rows


def compute_matchup_scores(
    rows: Iterable[MatchupRow],
    *,
    confidence: float = CONFIDENCE_LEVEL,
    min_games: int = 0,
) -> Dict[int, List[Dict[str, object]]]:
    """3対3編成ごとの勝率と信頼度を計算する."""

    stats: DefaultDict[
        int, DefaultDict[Tuple[Tuple[int, int, int], Tuple[int, int, int]], float]
    ] = defaultdict(lambda: defaultdict(float))

    for map_id, win_a, win_b, win_c, lose_a, lose_b, lose_c, wins in rows:
        win_team = tuple(sorted((int(win_a), int(win_b), int(win_c))))
        lose_team = tuple(sorted((int(lose_a), int(lose_b), int(lose_c))))
        stats[map_id][(win_team, lose_team)] += float(wins)

    results: Dict[int, List[Dict[str, object]]] = {}
    min_games_threshold = max(min_games, 4)
    for map_id, combos in stats.items():
        orientation_stats: List[Tuple[Tuple[int, int, int], Tuple[int, int, int], float, float]] = []
        for (win_team, lose_team), wins_val in combos.items():
            reverse_wins = combos.get((lose_team, win_team), 0.0)
            games = wins_val + reverse_wins
            if games < min_games_threshold:
                continue
            orientation_stats.append((win_team, lose_team, wins_val, games))

        if not orientation_stats:
            results[map_id] = []
            continue

        total_wins = sum(item[2] for item in orientation_stats)
        total_games = sum(item[3] for item in orientation_stats)
        if total_games <= 0:
            results[map_id] = []
            continue

        mean = total_wins / total_games
        strength = total_games / len(orientation_stats)
        alpha_prior = mean * strength
        beta_prior = (1 - mean) * strength

        records: List[Tuple[Dict[str, object], float]] = []
        for win_team, lose_team, wins_val, games in orientation_stats:
            losses_val = games - wins_val
            alpha_post = alpha_prior + wins_val
            beta_post = beta_prior + losses_val
            lcb = beta_lcb(alpha_post, beta_post, confidence)
            record = {
                "win_brawlers": list(win_team),
                "lose_brawlers": list(lose_team),
                "win_rate": wins_val / games if games > 0 else 0.0,
                "win_rate_lcb": lcb,
            }
            records.append((record, games))

        records.sort(key=lambda item: (item[0]["win_rate_lcb"], item[1]), reverse=True)
        results[map_id] = [record for record, _ in records]

    return results


def export_matchup_json(results: Dict[int, List[Dict[str, object]]], output_dir: Path) -> None:
    """計算結果をマップIDごとのJSONに書き出す."""

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for map_id, records in results.items():
        out_file = output_dir / f"{map_id}.json"
        with open(out_file, "w", encoding="utf-8") as fp:
            json.dump(records, fp, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="3対3編成勝率をJSONとして出力")
    parser.add_argument(
        "--output-dir",
        default="three_vs_three_stats",
        help="出力先ディレクトリ",
    )
    parser.add_argument(
        "--min-games",
        type=int,
        default=4,
        help="統計対象とする最低試合数",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    since = (datetime.now(JST) - timedelta(days=DATA_RETENTION_DAYS)).strftime("%Y%m%d")
    logging.info("統計対象期間（日数）: %d", DATA_RETENTION_DAYS)

    logging.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as exc:
        raise SystemExit(f"データベースに接続できません: {exc}")

    try:
        logging.info("3対3編成データを取得しています...")
        rows = fetch_matchup_rows(conn, since)
        logging.info("%d 行の3対3データを取得", len(rows))
    except mysql.connector.Error as exc:
        raise SystemExit(f"クエリの実行に失敗しました: {exc}")
    finally:
        conn.close()

    logging.info("勝率指標を計算しています")
    results = compute_matchup_scores(
        rows,
        min_games=args.min_games,
        confidence=CONFIDENCE_LEVEL,
    )

    output_dir = Path(args.output_dir)
    logging.info("JSONを出力しています: %s", output_dir)
    export_matchup_json(results, output_dir)

    logging.info("3対3編成統計の出力が完了しました")


if __name__ == "__main__":
    main()

