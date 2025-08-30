import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import mysql.connector
import requests
from dateutil.parser import parse
from dotenv import load_dotenv
from urllib.parse import quote

from .country_code import COUNTRY_CODE
from .db import get_connection
from .map import MAP_NAME_TO_ID
from .rank import RANK_TO_ID
from .logging_config import setup_logging

# リクエスト間隔（秒）
REQUEST_INTERVAL = 0.05
# 最大リトライ回数
MAX_RETRIES = 3
# 集計開始日
COL_BEFORE_DATE = 30
# 取得サイクル時間
ACQ_CYCLE_TIME = 18
# トロフィー境界
TROPHIE_BORDER = 90000

# 逆結果マップ
OPPOSITE = {"victory": "defeat", "defeat": "victory"}

JST = timezone(timedelta(hours=9))

setup_logging()
logger = logging.getLogger(__name__)

@dataclass
class ResultLog:
    result: str = "不明"
    brawlers: list[str] = field(default_factory=list)


def request_with_retry(
    url: str,
    headers: Optional[dict[str, str]] = None,
    method: str = "GET",
    timeout: int = 15,
    max_retries: int = MAX_RETRIES,
    request_interval: float = REQUEST_INTERVAL,
) -> Optional[requests.Response]:
    """API にリクエストを送り、失敗した場合はリトライを行う汎用関数"""

    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(request_interval)
            resp = requests.request(method, url, headers=headers, timeout=timeout)
            if resp.status_code == 404:
                logger.warning("Resource not found: %s", url)
                return None
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == max_retries:
                logger.error("Request failed: %s", e)
                return None
            wait = 3 * attempt
            logger.warning(
                "Request failed (%d/%d): %s. Retrying in %d seconds.",
                attempt,
                max_retries,
                e,
                wait,
            )
            time.sleep(wait)


def cleanup_old_logs(conn) -> int:
    """30日前より前のログデータを削除"""
    cur = conn.cursor()
    threshold = (datetime.now(JST) - timedelta(days=COL_BEFORE_DATE)).strftime("%Y%m%d")
    cur.execute("SELECT id FROM rank_logs WHERE SUBSTRING(id, 1, 8) < %s", (threshold,))
    old_rank_ids = [row[0] for row in cur.fetchall()]
    if not old_rank_ids:
        return 0
    placeholders = ",".join("%s" for _ in old_rank_ids)
    cur.execute(
        f"DELETE FROM win_lose_logs WHERE battle_log_id IN (SELECT id FROM battle_logs WHERE rank_log_id IN ({placeholders}))",
        old_rank_ids,
    )
    cur.execute(
        f"DELETE FROM battle_logs WHERE rank_log_id IN ({placeholders})",
        old_rank_ids,
    )
    cur.execute(
        f"DELETE FROM rank_logs WHERE id IN ({placeholders})",
        old_rank_ids,
    )
    conn.commit()
    return len(old_rank_ids)

def fetch_rank_player(api_key: str, conn) -> set[str]:
    """ランク上位プレイヤーを取得してDBへ保存"""
    cur = conn.cursor()

    for code in COUNTRY_CODE:
        url = f"https://api.brawlstars.com/v1/rankings/{code}/players"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }

        resp = request_with_retry(url, headers=headers)
        if resp is None:
            logger.error("国コード:%s エラー:ランキングを取得できませんでした。", code)
            continue

        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            logger.error("JSON の解析に失敗しました: %s", e)
            continue
        
        rank_players = data.get("items", [])

        count=0
        for player in rank_players:
            p_t=player.get("trophies", 0)
            if TROPHIE_BORDER < p_t or p_t == 1:
                count+=1
                p_tag = player.get("tag")
                if p_tag:
                    cur.execute("INSERT IGNORE INTO players(tag) VALUES (%s)", (p_tag,))
        
        logger.info("国コード:%s 取得プレイヤー数 %d", code, count)
        conn.commit()


def fetch_battle_logs(player_tag: str, api_key: str, conn) -> set[str]:
    """指定したプレイヤーのバトルログを取得してDBへ保存"""
    cur = conn.cursor()
    tag_enc = quote(player_tag, safe="")

    url = f"https://api.brawlstars.com/v1/players/{tag_enc}/battlelog"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    resp = request_with_retry(url, headers=headers)
    if resp is None:
        cur.execute(
            "DELETE FROM players WHERE tag=%s",
            (player_tag,),
        )
        conn.commit()
        logger.warning("プレイヤーが見つかりません: %s", player_tag)
        return set()

    try:
        data = resp.json()
    except json.JSONDecodeError as e:
        logger.error("JSON の解析に失敗しました: %s", e)
        return set()

    battle_logs = data.get("items", [])
    if len(battle_logs) < 1:
        logger.info("バトルログが見つかりませんでした。")
        return set()

    cur.execute(
        "UPDATE players SET last_fetched=%s WHERE tag=%s",
        (datetime.now(JST), player_tag),
    )

    rank=0
    new_rank_flag = False            
    new_rank_brawlers_flag = False   
    rank_log_id = None   

    for battle in battle_logs:       
        battle_detail = battle.get("battle", {})
        if battle_detail.get("type") not in ["soloRanked"]:
            continue
        battle_map_id = battle.get("event", {}).get("id", "不明")
        battle_mode = battle.get("event", {}).get("mode", "不明")
        battle_map = battle.get("event", {}).get("map", "不明")
        battle_time = battle.get("battleTime", "不明")
        battle_datetime = parse(battle_time).astimezone(JST)
        col_start_date = datetime.now(JST) - timedelta(days=COL_BEFORE_DATE)
        if battle_datetime < col_start_date:
            continue
        star_player = battle_detail.get("starPlayer") or {}
        star_tag = star_player.get("tag")
        if star_tag:
            new_rank_flag = True
            rank_log_id = f"{battle_time}_{star_tag}"
            # ここですでに存在しているランクマッチを確認
            cur.execute(
                "SELECT id FROM rank_logs WHERE id=%s",
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
                trophies = player.get("brawler", {}).get("trophies", 0)
                if p_tag == player_tag:
                    my_side_idx = side_idx
                    resultLog.result = result
                    if trophies < 7:
                        cur.execute("DELETE FROM players WHERE tag=%s", (player_tag,))
                        if cur.rowcount == 1:  # 削除されたら1、既に存在しなかったら0
                            logger.info("プレイヤー削除:%s", player_tag)
                if 18 < trophies <= 22:
                    cur.execute("INSERT IGNORE INTO players(tag) VALUES (%s)", (p_tag,))
                    if cur.rowcount == 1:  # 挿入されたら1、既存で無視されたら0
                        logger.info("マスターランク発見:%s", p_tag)
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
                    "INSERT INTO rank_logs(id, map_id, rank_id) VALUES (%s, %s, %s)",
                    (rank_log_id, MAP_NAME_TO_ID.get(battle_map), RANK_TO_ID.get(rank)),
                )
            except mysql.connector.IntegrityError:
                logger.warning(
                    "未登録のマップを検出: マップ=%s マップID=%s ランク=%s",
                    battle_map,
                    battle_map_id,
                    rank,
                )
                logger.debug("Battle detail: %s", battle)
                mode_id = cur.execute("SELECT id FROM _modes WHERE name=%s", (battle_mode,)).fetchone()[0]
                cur.execute(
                    "REPLACE INTO _maps(id, name, mode_id) VALUES (%s, %s, %s)",
                    (battle_map_id, battle_map, mode_id),
                )
                MAP_NAME_TO_ID[battle_map] = battle_map_id
                cur.execute(
                    "INSERT INTO rank_logs(id, map_id, rank_id) VALUES (%s, %s, %s)",
                    (rank_log_id, MAP_NAME_TO_ID.get(battle_map), RANK_TO_ID.get(rank)),
                )
            for rlog in resultInfo:
                for brawler_id in rlog.brawlers:
                    cur.execute(
                        "SELECT count FROM brawler_used_ranks WHERE brawler_id=%s AND map_id=%s AND rank_id=%s",
                        (brawler_id, MAP_NAME_TO_ID.get(battle_map), RANK_TO_ID.get(rank)),
                    )
                    if cur.fetchone():
                        cur.execute(
                            "UPDATE brawler_used_ranks SET count = count + 1 WHERE brawler_id=%s AND map_id=%s AND rank_id=%s",
                            (brawler_id, MAP_NAME_TO_ID.get(battle_map), RANK_TO_ID.get(rank)),
                        )
                    else:
                        cur.execute(
                            "INSERT INTO brawler_used_ranks(brawler_id, map_id, rank_id, count) VALUES (%s, %s, %s, 1)",
                            (brawler_id, MAP_NAME_TO_ID.get(battle_map), RANK_TO_ID.get(rank)),
                        )
            new_rank_brawlers_flag = False

        #新規バトル登録
        battle_log_id = f"{battle_time}_{p_tag}_battle"
        try:
            cur.execute(
                "INSERT INTO battle_logs(id, rank_log_id) VALUES (%s, %s)",
                (battle_log_id, rank_log_id),
            )
        except mysql.connector.IntegrityError:
            logger.debug(
                "既に記録済みのバトルのためスキップ battle_log_id=%s rank_log_id=%s",
                battle_log_id,
                rank_log_id,
            )
            continue

        winners = [b for r in resultInfo if r.result == "victory" for b in r.brawlers]
        losers = [b for r in resultInfo if r.result == "defeat" for b in r.brawlers]
        for w in winners:
            for l in losers:
                cur.execute(
                    "INSERT IGNORE INTO win_lose_logs(win_brawler_id, lose_brawler_id, battle_log_id) VALUES (%s, %s, %s)",
                    (w, l, battle_log_id),
                )
                
    conn.commit()
            

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    load_dotenv(dotenv_path=".env.local")

    api_key = os.getenv("BRAWL_STARS_API_KEY")
    if not api_key:
        raise RuntimeError("環境変数 BRAWL_STARS_API_KEY が設定されていません。")
    try:
        with get_connection() as conn:
    
            deleted = cleanup_old_logs(conn)
            logger.info("削除したランクマッチ数:%d", deleted)

            start_time = time.time()

            fetch_rank_player(api_key, conn)
            rest = 0

            try:
                while 1:
                    cur = conn.cursor()
                    seventy_two_hours_ago = datetime.now(JST) - timedelta(hours=ACQ_CYCLE_TIME)
                    
                    cur.execute(
                        "SELECT tag FROM players WHERE last_fetched < %s ORDER BY last_fetched ASC LIMIT 1",
                        (seventy_two_hours_ago,),
                    )
                    row = cur.fetchone()
                    current_tag = row[0] if row else None

                    if not current_tag:
                        logger.info("対象プレイヤーがいません")
                        break

                    fetch_battle_logs(current_tag, api_key, conn)

                    cur.execute(
                        "SELECT COUNT(*) FROM players WHERE last_fetched < %s",
                        (seventy_two_hours_ago,),
                    )
                    rest = cur.fetchone()[0]
                    if rest == 0:
                        logger.info("全てのプレイヤーを集計しました")
                        break
                    logger.info("残り集計対象プレイヤー数:%d", rest)

            finally:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM players")
                players = cur.fetchone()[0]
                logger.info("集計プレイヤー:%d", players - rest)
                logger.info("プレイヤー総数:%d", players)

                cur.execute("SELECT COUNT(*) FROM rank_logs")
                rank_logs = cur.fetchone()[0]
                logger.info("集計済みランクマッチ:%d", rank_logs)

                cur.execute("SELECT COUNT(*) FROM battle_logs")
                battles = cur.fetchone()[0]
                logger.info("集計済みバトル:%d", battles)

                total_time = time.time() - start_time
                logger.info("②処理時間:%s", format_time(total_time))

    except mysql.connector.Error as e:
        logger.error("データベース接続エラー: %s", e)
        return
    logger.info("バトルログの取得が完了しました。")

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
