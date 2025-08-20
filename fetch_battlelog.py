import os
import json
import requests
from dotenv import load_dotenv
from urllib.parse import quote
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
import sqlite3
from map import MAP_NAME_TO_ID
from rank import RANK_TO_ID

# 逆結果マップ
OPPOSITE = {"victory": "defeat", "defeat": "victory"}


@dataclass
class ResultLog:
    result: str = "不明"
    brawlers: list[str] = field(default_factory=list)


def fetch_battle_logs(player_tag: str, api_key: str, conn: sqlite3.Connection) -> set[str]:
    """指定したプレイヤーのバトルログを取得してDBへ保存し、発見したプレイヤータグを返す"""

    tag_enc = quote(player_tag, safe="")
    url = f"https://api.brawlstars.com/v1/players/{tag_enc}/battlelog"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    print(f"GET {url}")
    resp = requests.get(url, headers=headers, timeout=15)
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        print(f"HTTP {resp.status_code}")
        print(resp.text)
        raise

    data = resp.json()

    out_dir = Path("out")
    out_dir.mkdir(parents=True, exist_ok=True)
    tag_sanitized = player_tag.replace("#", "")
    output_path = out_dir / f"battlelog_{tag_sanitized}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"保存しました: {output_path.resolve()}")

    battle_logs = data.get("items", [])
    print(f"取得したバトルログの数: {len(battle_logs)}")

    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO players(tag, last_fetched) VALUES (?, CURRENT_TIMESTAMP)",
        (player_tag,),
    )

    discovered_tags: set[str] = set()

    for battle in battle_logs:
        rank = 0
        battle_detail = battle.get("battle", {})
        if battle_detail.get("type") not in ["soloRanked"]:
            print(f"ランクマッチではないためスキップ: {battle_detail.get('type', '不明')}")
            continue

        battle_map = battle.get("event", {}).get("map", "不明")
        battle_time = battle.get("battleTime", "不明")
        battle_duration = battle_detail.get("duration", "不明")
        battle_log_id = f"{battle_time}_{battle_duration}_{battle_map}"
        star_player = battle_detail.get("starPlayer") or {}
        star_tag = star_player.get("tag")
        if not star_tag:
            continue
        rank_log_id = f"{battle_time}_{star_tag}"
        cur.execute("SELECT id FROM rank_logs WHERE id=?", (rank_log_id,))
        if cur.fetchone():
            print(f"既に記録済みのランクマッチ: {rank_log_id}")
            continue
        print(f"新しいランクマッチ: {rank_log_id}")

        result = battle_detail.get("result", "不明")
        teams = battle_detail.get("teams", [])
        resultInfo: list[ResultLog] = []
        my_side_idx = None

        for side_idx, team in enumerate(teams):
            resultLog = ResultLog()
            for player in team:
                resultLog.brawlers.append(player.get("brawler", {}).get("id", "不明"))
                p_tag = player.get("tag")
                if p_tag:
                    discovered_tags.add(p_tag)
                    cur.execute("INSERT OR IGNORE INTO players(tag) VALUES (?)", (p_tag,))
                if p_tag == player_tag:
                    my_side_idx = side_idx
                    resultLog.result = result
                trophies = player.get("brawler", {}).get("trophies", 0)
                if rank < trophies:
                    rank = trophies
            resultInfo.append(resultLog)

        if my_side_idx is not None and len(resultInfo) == 2 and result in OPPOSITE:
            other = 1 - my_side_idx
            if getattr(resultInfo[other], "result", "不明") in (None, "", "不明"):
                resultInfo[other].result = OPPOSITE[result]

        print(f"ランク: {rank} 結果：{resultInfo}")

        cur.execute(
            "INSERT INTO rank_logs(id, map_id, rank_id) VALUES (?, ?, ?)",
            (rank_log_id, MAP_NAME_TO_ID.get(battle_map), RANK_TO_ID.get(rank)),
        )
        for rlog in resultInfo:
            for brawler_id in rlog.brawlers:
                cur.execute(
                    "SELECT count FROM brawler_used_ranks WHERE brawler_id=? AND map_id=? AND rank_id=?",
                    (brawler_id, MAP_NAME_TO_ID.get(battle_map), RANK_TO_ID.get(rank)),
                )
                if cur.fetchone():
                    cur.execute(
                        "UPDATE brawler_used_ranks SET count = count + 1 WHERE brawler_id=? AND map_id=? AND rank_id=?",
                        (brawler_id, MAP_NAME_TO_ID.get(battle_map), RANK_TO_ID.get(rank)),
                    )
                else:
                    cur.execute(
                        "INSERT INTO brawler_used_ranks(brawler_id, map_id, rank_id, count) VALUES (?, ?, ?, 1)",
                        (brawler_id, MAP_NAME_TO_ID.get(battle_map), RANK_TO_ID.get(rank)),
                    )

        try:
            cur.execute(
                "INSERT INTO battle_logs(id, rank_log_id) VALUES (?, ?)",
                (battle_log_id, rank_log_id),
            )
        except sqlite3.IntegrityError:
            print("既に記録済みのバトルのためスキップ")
            continue

        winners = [b for r in resultInfo if r.result == "victory" for b in r.brawlers]
        losers = [b for r in resultInfo if r.result == "defeat" for b in r.brawlers]
        for w in winners:
            for l in losers:
                cur.execute(
                    "INSERT OR IGNORE INTO win_lose_logs(win_brawler_id, lose_brawler_id, battle_log_id) VALUES (?, ?, ?)",
                    (w, l, battle_log_id),
                )

        conn.commit()

    discovered_tags.discard(player_tag)
    return discovered_tags


def main() -> None:
    load_dotenv()

    api_key = os.getenv("BRAWL_STARS_API_KEY")
    seed_tag = os.getenv("PLAYER_TAG")
    if not api_key:
        raise RuntimeError("環境変数 BRAWL_STARS_API_KEY が設定されていません。")
    if not seed_tag:
        raise RuntimeError("PLAYER_TAG を .env に設定してください。")

    conn = sqlite3.connect("brawl_stats.db")
    try:
        to_fetch = {seed_tag}
        fetched: set[str] = set()
        while to_fetch:
            current = to_fetch.pop()
            if current in fetched:
                continue
            print(f"プレイヤータグ: {current}")
            new_tags = fetch_battle_logs(current, api_key, conn)
            fetched.add(current)
            to_fetch.update(new_tags - fetched)
    finally:
        conn.close()
    print("バトルログの取得が完了しました。")


if __name__ == "__main__":
    main()

