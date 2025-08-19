import os
from celery import Celery

from .collector import BrawlStarsAPI, BattleLogDB, BattleLogCollector

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
FETCH_INTERVAL = int(os.environ.get("FETCH_INTERVAL", 3600))

app = Celery("brawl_tasks", broker=REDIS_URL, backend=REDIS_URL)


def _bootstrap_players(db: BattleLogDB) -> None:
    tags = os.environ.get("INITIAL_PLAYER_TAGS", "")
    for tag in tags.split(","):
        tag = tag.strip()
        if tag:
            db.add_player(tag)


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(FETCH_INTERVAL, collect_battle_logs.s(), name="collect battle logs")


@app.task
def collect_battle_logs() -> int:
    db = BattleLogDB()
    _bootstrap_players(db)
    api = BrawlStarsAPI()
    collector = BattleLogCollector(api, db)
    count = 0
    for tag in db.players_to_fetch(FETCH_INTERVAL):
        count += collector.collect_from_player(tag)
    return count
