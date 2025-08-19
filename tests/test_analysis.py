import json
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from brawl_stats.collector import BattleLogDB, BrawlStarsAPI
from brawl_stats.analysis import (
    analyze_usage_and_winrate,
    rank_from_trophies,
    fetch_and_store_from_api,
)


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


def test_fetch_and_store_from_api(monkeypatch):
    sample_json = """
    {
        "items": [
            {
                "battleTime": "20250819T113050.000Z",
                "event": {
                    "id": 15000007,
                    "mode": "gemGrab",
                    "map": "Hard Rock Mine"
                },
                "battle": {
                    "mode": "gemGrab",
                    "type": "soloRanked",
                    "result": "defeat",
                    "duration": 169,
                    "starPlayer": {
                        "tag": "#PGCCLCPQ9",
                        "name": "くろすぎ",
                        "brawler": {
                            "id": 16000025,
                            "name": "CARL",
                            "power": 11,
                            "trophies": 15
                        }
                    },
                    "teams": [
                        [
                            {
                                "tag": "#PQL0J9RQG",
                                "name": "わおん",
                                "brawler": {
                                    "id": 16000044,
                                    "name": "RUFFS",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#GRPPQRU08",
                                "name": "🄴🅂🄿🄴🅁🄰🄽",
                                "brawler": {
                                    "id": 16000014,
                                    "name": "BO",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#8VC0PRP0Y",
                                "name": "CR|Temusai",
                                "brawler": {
                                    "id": 16000080,
                                    "name": "DRACO",
                                    "power": 11,
                                    "trophies": 15
                                }
                            }
                        ],
                        [
                            {
                                "tag": "#L0VPYUVLU",
                                "name": "니가 잘해서 넣은",
                                "brawler": {
                                    "id": 16000091,
                                    "name": "LUMI",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#PGCCLCPQ9",
                                "name": "くろすぎ",
                                "brawler": {
                                    "id": 16000025,
                                    "name": "CARL",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#890VJUY2R",
                                "name": "メンマ",
                                "brawler": {
                                    "id": 16000021,
                                    "name": "GENE",
                                    "power": 11,
                                    "trophies": 15
                                }
                            }
                        ]
                    ]
                }
            },
            {
                "battleTime": "20250819T112742.000Z",
                "event": {
                    "id": 15000007,
                    "mode": "gemGrab",
                    "map": "Hard Rock Mine"
                },
                "battle": {
                    "mode": "gemGrab",
                    "type": "soloRanked",
                    "result": "defeat",
                    "duration": 96,
                    "starPlayer": null,
                    "teams": [
                        [
                            {
                                "tag": "#PQL0J9RQG",
                                "name": "わおん",
                                "brawler": {
                                    "id": 16000044,
                                    "name": "RUFFS",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#GRPPQRU08",
                                "name": "🄴🅂🄿🄴🅁🄰🄽",
                                "brawler": {
                                    "id": 16000014,
                                    "name": "BO",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#8VC0PRP0Y",
                                "name": "CR|Temusai",
                                "brawler": {
                                    "id": 16000080,
                                    "name": "DRACO",
                                    "power": 11,
                                    "trophies": 15
                                }
                            }
                        ],
                        [
                            {
                                "tag": "#L0VPYUVLU",
                                "name": "니가 잘해서 넣은",
                                "brawler": {
                                    "id": 16000091,
                                    "name": "LUMI",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#PGCCLCPQ9",
                                "name": "くろすぎ",
                                "brawler": {
                                    "id": 16000025,
                                    "name": "CARL",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#890VJUY2R",
                                "name": "メンマ",
                                "brawler": {
                                    "id": 16000021,
                                    "name": "GENE",
                                    "power": 11,
                                    "trophies": 15
                                }
                            }
                        ]
                    ]
                }
            },
            {
                "battleTime": "20250819T112548.000Z",
                "event": {
                    "id": 15000007,
                    "mode": "gemGrab",
                    "map": "Hard Rock Mine"
                },
                "battle": {
                    "mode": "gemGrab",
                    "type": "soloRanked",
                    "result": "victory",
                    "duration": 116,
                    "starPlayer": null,
                    "teams": [
                        [
                            {
                                "tag": "#PQL0J9RQG",
                                "name": "わおん",
                                "brawler": {
                                    "id": 16000044,
                                    "name": "RUFFS",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#GRPPQRU08",
                                "name": "🄴🅂🄿🄴🅁🄰🄽",
                                "brawler": {
                                    "id": 16000014,
                                    "name": "BO",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#8VC0PRP0Y",
                                "name": "CR|Temusai",
                                "brawler": {
                                    "id": 16000080,
                                    "name": "DRACO",
                                    "power": 11,
                                    "trophies": 15
                                }
                            }
                        ],
                        [
                            {
                                "tag": "#L0VPYUVLU",
                                "name": "니가 잘해서 넣은",
                                "brawler": {
                                    "id": 16000091,
                                    "name": "LUMI",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#PGCCLCPQ9",
                                "name": "くろすぎ",
                                "brawler": {
                                    "id": 16000025,
                                    "name": "CARL",
                                    "power": 11,
                                    "trophies": 15
                                }
                            },
                            {
                                "tag": "#890VJUY2R",
                                "name": "メンマ",
                                "brawler": {
                                    "id": 16000021,
                                    "name": "GENE",
                                    "power": 11,
                                    "trophies": 15
                                }
                            }
                        ]
                    ]
                }
            }
        ]
    }
    """
    items = json.loads(sample_json)["items"]

    def fake_fetch(self, tag):
        return items

    monkeypatch.setattr(BrawlStarsAPI, "fetch_battle_log", fake_fetch)
    db = BattleLogDB(":memory:")
    stored = fetch_and_store_from_api(db, "#PGCCLCPQ9", api_key="dummy")
    assert stored == len(items)
    cur = db.conn.cursor()
    cur.execute("SELECT COUNT(*) FROM battle_logs")
    assert cur.fetchone()[0] == len(items)
