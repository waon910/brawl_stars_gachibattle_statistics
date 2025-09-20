"""トリオ編成の勝率集計に関するユーティリティ関数群."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

from scipy.stats import beta

from .settings import MIN_RANK_ID

TrioRow = Tuple[int, int, int, int, int, int, float, float]


def beta_lcb(alpha: float, beta_param: float, confidence: float = 0.95) -> float:
    """Beta分布に基づく下側信頼限界を計算する."""
    return float(beta.ppf(1 - confidence, alpha, beta_param))


def fetch_trio_rows(
    conn,
    *,
    since: Optional[str] = None,
    until: Optional[str] = None,
    rank_id: Optional[int] = None,
    mode_id: Optional[int] = None,
    map_id: Optional[int] = None,
) -> List[TrioRow]:
    """指定した条件でトリオの勝敗集計を取得する."""
    cur = conn.cursor()
    conditions: List[str] = []
    params: List[object] = [MIN_RANK_ID]

    if since is not None:
        conditions.append("SUBSTRING(rl.id,1,8) >= %s")
        params.append(since)
    if until is not None:
        conditions.append("SUBSTRING(rl.id,1,8) < %s")
        params.append(until)
    if rank_id is not None:
        conditions.append("rl.rank_id = %s")
        params.append(rank_id)
    if mode_id is not None:
        conditions.append("m.mode_id = %s")
        params.append(mode_id)
    if map_id is not None:
        conditions.append("rl.map_id = %s")
        params.append(map_id)

    condition_sql = ""
    if conditions:
        condition_sql = " AND " + " AND ".join(conditions)

    sql = f"""
        WITH recent_battles AS (
            SELECT bl.id AS battle_log_id,
                   rl.map_id,
                   rl.rank_id,
                   m.mode_id
            FROM battle_logs bl
            JOIN rank_logs rl ON bl.rank_log_id = rl.id
            JOIN _maps m ON rl.map_id = m.id
            WHERE rl.rank_id >= %s{condition_sql}
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
                   rb.rank_id,
                   rb.mode_id,
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
                   rb.rank_id,
                   rb.mode_id,
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
        ),
        win_counts AS (
            SELECT map_id, rank_id, mode_id, brawler_a, brawler_b, brawler_c, COUNT(*) AS wins
            FROM win_trios
            GROUP BY map_id, rank_id, mode_id, brawler_a, brawler_b, brawler_c
        ),
        lose_counts AS (
            SELECT map_id, rank_id, mode_id, brawler_a, brawler_b, brawler_c, COUNT(*) AS losses
            FROM lose_trios
            GROUP BY map_id, rank_id, mode_id, brawler_a, brawler_b, brawler_c
        ),
        combined AS (
            SELECT map_id, rank_id, mode_id, brawler_a, brawler_b, brawler_c, wins, 0 AS losses FROM win_counts
            UNION ALL
            SELECT map_id, rank_id, mode_id, brawler_a, brawler_b, brawler_c, 0 AS wins, losses FROM lose_counts
        )
        SELECT map_id,
               rank_id,
               mode_id,
               brawler_a,
               brawler_b,
               brawler_c,
               SUM(wins) AS wins,
               SUM(losses) AS losses
        FROM combined
        GROUP BY map_id, rank_id, mode_id, brawler_a, brawler_b, brawler_c
    """

    cur.execute(sql, tuple(params))
    rows: List[TrioRow] = cur.fetchall()
    cur.close()
    return rows


def compute_trio_scores(
    rows: Iterable[TrioRow],
    *,
    group_by_rank: bool = True,
    min_games: int = 0,
    confidence: float = 0.95,
) -> Dict[int, Dict[Optional[int], List[Dict[str, object]]]]:
    """トリオ勝率の指標値を計算する."""

    stats: Dict[int, Dict[Optional[int], Dict[Tuple[int, int, int], Dict[str, float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: {"wins": 0.0, "games": 0.0}))
    )

    for map_id, rank_id, _mode_id, b1, b2, b3, wins, losses in rows:
        rank_key = rank_id if group_by_rank else None
        trio = tuple(sorted((int(b1), int(b2), int(b3))))
        wins_f = float(wins)
        losses_f = float(losses)
        stats[map_id][rank_key][trio]["wins"] += wins_f
        stats[map_id][rank_key][trio]["games"] += wins_f + losses_f

    results: Dict[int, Dict[Optional[int], List[Dict[str, object]]]] = {}
    for map_id, rank_dict in stats.items():
        map_result: Dict[Optional[int], List[Dict[str, object]]] = {}
        for rank_key, combos in rank_dict.items():
            total_wins = sum(v["wins"] for v in combos.values())
            total_games = sum(v["games"] for v in combos.values())
            if total_games <= 0 or not combos:
                map_result[rank_key] = []
                continue
            mean = total_wins / total_games
            strength = total_games / len(combos)
            alpha_prior = mean * strength
            beta_prior = (1 - mean) * strength

            trio_list: List[Dict[str, object]] = []
            for trio, val in combos.items():
                games = val["games"]
                wins_val = val["wins"]
                if games <= 0:
                    continue
                losses_val = games - wins_val
                alpha_post = alpha_prior + wins_val
                beta_post = beta_prior + losses_val
                lcb = beta_lcb(alpha_post, beta_post, confidence=confidence)
                record = {
                    "brawlers": list(trio),
                    "wins": int(round(wins_val)),
                    "losses": int(round(losses_val)),
                    "games": int(round(games)),
                    "win_rate": wins_val / games if games > 0 else 0.0,
                    "win_rate_lcb": lcb,
                }
                if record["games"] >= min_games:
                    trio_list.append(record)

            trio_list.sort(key=lambda x: (x["win_rate_lcb"], x["games"]), reverse=True)
            map_result[rank_key] = trio_list
        results[map_id] = map_result

    return results
