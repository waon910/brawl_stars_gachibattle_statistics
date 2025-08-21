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
import time
import threading

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

    # print(f"GET {url}")
    time.sleep(1)  # 1秒待機
    resp = requests.get(url, headers=headers, timeout=15)

    # エラーハンドリング（内容も表示）
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        print(f"HTTP {resp.status_code}")
        print(resp.text)
        raise

    data = resp.json()

    battle_logs = data.get("items", [])
    if len(battle_logs) < 1 :
        print("バトルログが見つかりませんでした。")
        return

    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO players(tag, last_fetched) VALUES (?, CURRENT_TIMESTAMP)",
        (player_tag,),
    )

    discovered_tags: set[str] = set()

    rank=0
    new_rank_flag = False            
    new_rank_brawlers_flag = False   
    rank_log_id = None   

    for battle in battle_logs:       
        battle_detail = battle.get("battle", {})
        if battle_detail.get("type") not in ["soloRanked"]:
            continue
        battle_map = battle.get("event", {}).get("map", "不明")
        battle_time = battle.get("battleTime", "不明")
        star_player = battle_detail.get("starPlayer") or {}
        star_tag = star_player.get("tag")
        if star_tag:
            new_rank_flag = True
            rank_log_id = f"{battle_time}_{star_tag}"
            # ここですでに存在しているランクマッチを確認
            cur.execute(
                "SELECT id FROM rank_logs WHERE id=?",
                (rank_log_id,),
            )
            row = cur.fetchone()
            if row:
                # print(f"既に記録済みのランクマッチ: {rank_log_id}")
                new_rank_flag = False
                continue
            else:
                new_rank_brawlers_flag = True
        elif not new_rank_flag:
            continue
            
        result = battle_detail.get("result", "不明")
        teams = battle_detail.get("teams", [])
        resultInfo: list[ResultLog] = []

        my_side_idx = None  # 自分がいるチーム(0/1)

        for side_idx,team in enumerate(teams):
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
                if rank < trophies <= 22:
                    rank = trophies
            resultInfo.append(resultLog)

        if my_side_idx is not None and len(resultInfo) == 2 and result in OPPOSITE:
            other = 1 - my_side_idx
            # まだ埋まっていない場合のみ上書き
            if getattr(resultInfo[other], "result", "不明") in (None, "", "不明"):
                resultInfo[other].result = OPPOSITE[result]

        if new_rank_brawlers_flag:
            #新規ランクマッチ登録
            try:
                cur.execute(
                    "INSERT INTO rank_logs(id, map_id, rank_id) VALUES (?, ?, ?)",
                    (rank_log_id, MAP_NAME_TO_ID.get(battle_map), RANK_TO_ID.get(rank)),
                )
            except sqlite3.IntegrityError:
                print(f"修正が必要 マップ：{battle_map} ランク：{rank}")
                print(battle)
                SystemError
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
            new_rank_brawlers_flag = False

        #新規バトル登録
        battle_log_id = f"{battle_time}_{p_tag}_battle"
        try:
            cur.execute(
                "INSERT INTO battle_logs(id, rank_log_id) VALUES (?, ?)",
                (battle_log_id, rank_log_id),
            )
        except sqlite3.IntegrityError:
            print("既に記録済みのバトルのためスキップ")
            print(f"バトルログID：{battle_log_id}　ランクログID：{rank_log_id}")
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

    start_time = time.time()

    try:
        to_fetch = {seed_tag}
        fetched: set[str] = set()
        # current = to_fetch.pop()
        # new_tags = fetch_battle_logs(current, api_key, conn)
        while to_fetch:
            current = to_fetch.pop()
            if current in fetched:
                continue
            new_tags = fetch_battle_logs(current, api_key, conn)
            fetched.add(current)
            to_fetch.update(new_tags - fetched)
            print(f"集計対象プレイヤー数：{len(to_fetch)}")
    finally:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM players")
        players = cur.fetchone()[0]
        print(f"集計プレイヤー:{players-to_fetch}")
        print(f"プレイヤー総数:{players}")

        cur.execute("SELECT COUNT(*) FROM rank_logs")
        rank_logs = cur.fetchone()[0]
        print(f"集計済みランクマッチ:{rank_logs}")

        cur.execute("SELECT COUNT(*) FROM battle_logs")
        battles = cur.fetchone()[0]
        print(f"集計済みバトル:{battles}")
        conn.close()
        
        total_time = time.time() - start_time
        print(f"\n処理時間: {format_time(total_time)}")
        
    print("バトルログの取得が完了しました。")

def format_time(seconds):
    """秒を時:分:秒の形式に変換"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    
    if hours > 0:
        return f"{hours}時間{minutes:02d}分{secs:05.2f}秒"
    elif minutes > 0:
        return f"{minutes}分{secs:05.2f}秒"
    else:
        return f"{secs:.2f}秒"

if __name__ == "__main__":
    main()
