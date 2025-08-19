import json
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from brawl_stats.collector import BattleLogDB
from brawl_stats.analysis import analyze_usage_and_winrate, rank_from_trophies


def sample_db() -> BattleLogDB:
    db = BattleLogDB(":memory:")
    sample = {
        "battleTime": "20250819T094716.000Z",
        "event": {"mode": "heist", "map": "Kaboom Canyon"},
        "battle": {
            "mode": "heist",
            "type": "soloRanked",
            "result": "victory",
            "starPlayer": {
                "tag": "#AAA",
                "name": "sp",
                "brawler": {"id": 1, "name": "BULL", "power": 11, "trophies": 15},
            },
            "teams": [
                [
                    {
                        "tag": "#AAA",
                        "name": "sp",
                        "brawler": {"id": 1, "name": "BULL", "power": 11, "trophies": 15},
                    }
                ],
                [
                    {
                        "tag": "#BBB",
                        "name": "enemy",
                        "brawler": {"id": 2, "name": "OTIS", "power": 11, "trophies": 15},
                    }
                ],
            ],
        },
    }
    db.add_battle(
        battle_time=sample["battleTime"],
        star_player_tag=sample["battle"]["starPlayer"]["tag"],
        mode=sample["event"]["mode"],
        map_name=sample["event"]["map"],
        star_player_brawler=sample["battle"]["starPlayer"].get("brawler"),
        winning_team=0,
        data=sample,
    )
    return db


def test_analyze_usage_and_winrate():
    db = sample_db()
    stats, matchups = analyze_usage_and_winrate(db)
    rank = rank_from_trophies(15)[0]
    key = ("heist", "Kaboom Canyon", rank, 1)
    assert stats[key]["games"] == 1
    assert stats[key]["wins"] == 1
    matchup_key = ("heist", "Kaboom Canyon", 1, 2)
    assert matchups[matchup_key]["games"] == 1
    assert matchups[matchup_key]["wins"] == 1
