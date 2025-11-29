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
from .stats_loader import StatsDataset, load_recent_ranked_battles
from .settings import CONFIDENCE_LEVEL, DATA_RETENTION_DAYS

logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))

MatchupRow = Tuple[int, int, int, int, int, int, int, float]

MIN_BETA_PARAMETER = 1e-6


def beta_lcb(alpha: float, beta_param: float, confidence: float = CONFIDENCE_LEVEL) -> float:
    """Beta分布の下側信頼限界を計算する."""

    return float(beta.ppf(1 - confidence, alpha, beta_param))


def fetch_matchup_rows(dataset: StatsDataset) -> List[MatchupRow]:
    """3対3の編成ごとの勝敗集計を共通データから生成する."""

    stats: Dict[Tuple[int, Tuple[int, int, int], Tuple[int, int, int]], float] = defaultdict(float)
    for battle in dataset.iter_ranked_battles():
        if len(battle.win_brawlers) != 3 or len(battle.lose_brawlers) != 3:
            continue
        win_team = tuple(sorted(battle.win_brawlers))
        lose_team = tuple(sorted(battle.lose_brawlers))
        stats[(battle.map_id, win_team, lose_team)] += 1.0

    rows: List[MatchupRow] = []
    for (map_id, win_team, lose_team), wins in stats.items():
        win_a, win_b, win_c = win_team
        lose_a, lose_b, lose_c = lose_team
        rows.append(
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
        )
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
        alpha_prior = max(mean * strength, MIN_BETA_PARAMETER)
        beta_prior = max((1 - mean) * strength, MIN_BETA_PARAMETER)

        records: List[Tuple[Dict[str, object], float]] = []
        for win_team, lose_team, wins_val, games in orientation_stats:
            losses_val = games - wins_val
            alpha_post = max(alpha_prior + wins_val, MIN_BETA_PARAMETER)
            beta_post = max(beta_prior + losses_val, MIN_BETA_PARAMETER)
            lcb = beta_lcb(alpha_post, beta_post, confidence)
            record = {
                "win_brawlers": list(win_team),
                "lose_brawlers": list(lose_team),
                "games": int(round(games)),
                "win_rate": wins_val / games if games > 0 else 0.0,
                "win_rate_lcb": lcb,
            }
            records.append(record)

        records.sort(
            key=lambda item: (item["win_rate_lcb"], item["games"]),
            reverse=True,
        )
        results[map_id] = records

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
    setup_logging()

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

    since = (datetime.now(JST) - timedelta(days=DATA_RETENTION_DAYS)).strftime("%Y%m%d")
    logger.info("統計対象期間（日数）: %d", DATA_RETENTION_DAYS)

    logger.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as exc:
        raise SystemExit(f"データベースに接続できません: {exc}")

    try:
        logger.info("3対3編成データを取得しています...")
        dataset = load_recent_ranked_battles(conn, since)
        rows = fetch_matchup_rows(dataset)
        logger.info("%d 行の3対3データを取得", len(rows))
    except mysql.connector.Error as exc:
        raise SystemExit(f"クエリの実行に失敗しました: {exc}")
    finally:
        conn.close()

    logger.info("勝率指標を計算しています")
    results = compute_matchup_scores(
        rows,
        min_games=args.min_games,
        confidence=CONFIDENCE_LEVEL,
    )

    output_dir = Path(args.output_dir)
    logger.info("JSONを出力しています: %s", output_dir)
    export_matchup_json(results, output_dir)

    logger.info("3対3編成統計の出力が完了しました")


if __name__ == "__main__":
    main()

