import os
import time
import json
import sqlite3
from typing import Iterable, Optional

API_BASE_URL = "https://api.brawlstars.com/v1"


def normalize_tag(tag: str) -> str:
    """Remove leading # and convert to upper case."""
    return tag.strip().lstrip("#").upper()


class BrawlStarsAPI:
    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or os.environ.get("BRAWL_STARS_API_KEY")
        if not self.api_key:
            raise ValueError("API key must be provided via argument or BRAWL_STARS_API_KEY")

    def fetch_battle_log(self, player_tag: str) -> list:
        import requests

        tag = normalize_tag(player_tag)
        url = f"{API_BASE_URL}/players/%23{tag}/battlelog"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("items", [])


class BattleLogDB:
    def __init__(self, path: str = "brawlstats.db") -> None:
        self.conn = sqlite3.connect(path)
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS players(
                tag TEXT PRIMARY KEY,
                last_fetched INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS battle_logs(
                battle_time TEXT,
                star_player_tag TEXT,
                mode TEXT,
                map TEXT,
                winning_team INTEGER,
                data TEXT,
                PRIMARY KEY (battle_time, star_player_tag)
            )
            """
        )
        self.conn.commit()

    def add_player(self, tag: str) -> None:
        tag = normalize_tag(tag)
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO players(tag, last_fetched) VALUES(?, ?)",
            (tag, 0),
        )
        self.conn.commit()

    def add_battle(
        self,
        battle_time: str,
        star_player_tag: str,
        mode: str,
        map_name: str,
        winning_team: Optional[int],
        data: dict,
    ) -> bool:
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO battle_logs(battle_time, star_player_tag, mode, map, winning_team, data)
                VALUES(?,?,?,?,?,?)
                """,
                (
                    battle_time,
                    normalize_tag(star_player_tag),
                    mode,
                    map_name,
                    winning_team if winning_team is not None else -1,
                    json.dumps(data),
                ),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def update_player_fetched(self, tag: str) -> None:
        now = int(time.time())
        cur = self.conn.cursor()
        cur.execute("UPDATE players SET last_fetched=? WHERE tag=?", (now, normalize_tag(tag)))
        self.conn.commit()

    def players_to_fetch(self, interval: int) -> Iterable[str]:
        now = int(time.time())
        cur = self.conn.cursor()
        cur.execute(
            "SELECT tag FROM players WHERE last_fetched IS NULL OR last_fetched < ?",
            (now - interval,),
        )
        for (tag,) in cur.fetchall():
            yield tag


class BattleLogCollector:
    def __init__(self, api: BrawlStarsAPI, db: BattleLogDB) -> None:
        self.api = api
        self.db = db

    def _winning_team_index(self, battle: dict, player_tag: str) -> Optional[int]:
        teams = battle.get("teams", [])
        player_tag_norm = normalize_tag(player_tag)
        player_team = None
        for idx, team in enumerate(teams):
            tags = {normalize_tag(p.get("tag", "")) for p in team}
            if player_tag_norm in tags:
                player_team = idx
                break
        if player_team is None:
            return None
        result = battle.get("result")
        if result == "victory":
            return player_team
        if result == "defeat":
            return 1 - player_team
        return None

    def collect_from_player(self, player_tag: str) -> int:
        items = self.api.fetch_battle_log(player_tag)
        stored = 0
        for item in items:
            battle = item.get("battle", {})
            event = item.get("event", {})
            star_player = battle.get("starPlayer", {})
            winning_team = self._winning_team_index(battle, player_tag)
            added = self.db.add_battle(
                item.get("battleTime"),
                star_player.get("tag", ""),
                event.get("mode", ""),
                event.get("map", ""),
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
                        self.db.add_player(tag)
        self.db.update_player_fetched(player_tag)
        return stored
