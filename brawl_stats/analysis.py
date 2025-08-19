import json
from typing import Dict, Tuple

from .collector import BattleLogDB

RANK_TIERS = [
    "Bronze",
    "Silver",
    "Gold",
    "Diamond",
    "Elite",
    "Legend",
    "Master",
    "Pro",
]


def rank_from_trophies(trophies: int) -> Tuple[str, int]:
    """Convert trophy number (1-22) to (tier, grade)."""
    index = trophies - 1
    for tier in RANK_TIERS:
        grades = 3 if tier != "Pro" else 1
        if index < grades:
            grade = index + 1
            return tier, grade
        index -= grades
    return "Unknown", 0


def analyze_usage_and_winrate(db: BattleLogDB) -> Tuple[Dict, Dict]:
    """Return usage/winrate stats and matchup stats.

    Returns
    -------
    stats: dict
        key: (mode, map, rank, brawler_id) -> {'games': int, 'wins': int}
    matchups: dict
        key: (mode, map, brawler_id, opponent_id) -> {'games': int, 'wins': int}
    """
    cur = db.conn.cursor()
    cur.execute("SELECT mode, map, winning_team, data FROM battle_logs")
    stats: Dict[Tuple[str, str, str, int], Dict[str, int]] = {}
    matchups: Dict[Tuple[str, str, int, int], Dict[str, int]] = {}
    for mode, map_name, winning_team, data_json in cur.fetchall():
        data = json.loads(data_json)
        battle = data.get("battle", {})
        teams = battle.get("teams", [])
        for idx, team in enumerate(teams):
            win = winning_team == idx
            for player in team:
                brawler = player.get("brawler", {})
                b_id = brawler.get("id")
                trophies = brawler.get("trophies", 0)
                rank = rank_from_trophies(trophies)[0]
                key = (mode, map_name, rank, b_id)
                if key not in stats:
                    stats[key] = {"games": 0, "wins": 0}
                stats[key]["games"] += 1
                if win:
                    stats[key]["wins"] += 1
        if len(teams) >= 2:
            team_a, team_b = teams[0], teams[1]
            for p1 in team_a:
                b1 = p1.get("brawler", {}).get("id")
                for p2 in team_b:
                    b2 = p2.get("brawler", {}).get("id")
                    key_ab = (mode, map_name, b1, b2)
                    if key_ab not in matchups:
                        matchups[key_ab] = {"games": 0, "wins": 0}
                    matchups[key_ab]["games"] += 1
                    if winning_team == 0:
                        matchups[key_ab]["wins"] += 1
                    key_ba = (mode, map_name, b2, b1)
                    if key_ba not in matchups:
                        matchups[key_ba] = {"games": 0, "wins": 0}
                    matchups[key_ba]["games"] += 1
                    if winning_team == 1:
                        matchups[key_ba]["wins"] += 1
    return stats, matchups
