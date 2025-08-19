import json
from typing import Dict, Tuple, Optional

from .collector import BattleLogDB, BrawlStarsAPI, BattleLogCollector

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


def fetch_and_store_from_api(
    db: BattleLogDB, player_tag: str, api_key: Optional[str] = None
) -> int:
    """公式 API からバトルログを取得しデータベースへ保存する。

    Parameters
    ----------
    db: BattleLogDB
        データを保存するデータベースインスタンス。
    player_tag: str
        バトルログを取得したいプレイヤーのタグ。
    api_key: str, optional
        Brawl Stars API の API キー。省略時は環境変数 ``BRAWL_STARS_API_KEY`` を使用。

    Returns
    -------
    int
        新規に保存されたバトルログ件数。
    """
    api = BrawlStarsAPI(api_key)
    collector = BattleLogCollector(api, db)
    items = api.fetch_battle_log(player_tag)
    stored = 0
    for item in items:
        battle = item.get("battle", {})
        event = item.get("event", {})
        star_player = battle.get("starPlayer") or {}
        winning_team = collector._winning_team_index(battle, player_tag)
        added = db.add_battle(
            item.get("battleTime"),
            star_player.get("tag", ""),
            event.get("mode", ""),
            event.get("map", ""),
            star_player.get("brawler"),
            winning_team,
            item,
        )
        if not added:
            continue
        stored += 1
        for team in battle.get("teams", []):
            for player in team:
                tag = player.get("tag")
                if tag:
                    db.add_player(tag)
    db.update_player_fetched(player_tag)
    return stored


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
    cur.execute(
        """
        SELECT modes.name, maps.name, battle_logs.winning_team, battle_logs.data
        FROM battle_logs
        JOIN modes ON battle_logs.mode_id = modes.id
        JOIN maps ON battle_logs.map_id = maps.id
        """
    )
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
