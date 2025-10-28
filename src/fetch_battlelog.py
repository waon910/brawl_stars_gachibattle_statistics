import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence, Tuple
from mysql.connector import IntegrityError, errorcode

import mysql.connector
import requests
from requests.adapters import HTTPAdapter
from dateutil.parser import parse
from urllib.parse import quote

from .country_code import COUNTRY_CODE
from .db import get_connection
from .map import MAP_NAME_TO_ID
from .rank import RANK_TO_ID
from .logging_config import setup_logging
from .settings import DATA_RETENTION_DAYS, load_environment

# リクエスト間隔（秒）
REQUEST_INTERVAL = 0.01
# 最大リトライ回数
MAX_RETRIES = 3
# 取得サイクル時間
ACQ_CYCLE_TIME = 6
# トロフィー境界
TROPHIE_BORDER = 5000
# 一度に取得するプレイヤー数
FETCH_BATCH_SIZE = 10
# 並列取得時の最大ワーカー数
MAX_WORKERS = 10
# API リクエストのタイムアウト (接続タイムアウト, 読み取りタイムアウト)
REQUEST_TIMEOUT = (5, 30)


def _create_http_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS * 2,
        pool_maxsize=MAX_WORKERS * 2,
        max_retries=0,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.trust_env = False
    return session


SESSION = _create_http_session()

# 逆結果マップ
OPPOSITE = {"victory": "defeat", "defeat": "victory"}

JST = timezone(timedelta(hours=9))

logger = logging.getLogger(__name__)

@dataclass
class ResultLog:
    result: str = "不明"
    brawlers: list[tuple[Optional[int], Optional[str]]] = field(default_factory=list)


def _update_player_profile_from_latest_battle(
    cur,
    battle_logs: Sequence[dict],
    player_tag: str,
    player_name_in_db: Optional[str],
    player_highest_rank: int,
    player_current_rank: int,
) -> tuple[Optional[str], int, int]:
    """最新のバトルログからプレイヤー情報を更新する。

    API から返却されるバトルログは新しい順で並んでいるため、先頭から
    処理して最初に自身のプレイヤー情報を含むランク戦ログを探す。
    ランク戦ログが既に保存済みであっても、ここで名前や最高ランク、
    現在ランクの更新を行うことで情報が最新に保たれる。
    """

    for battle in battle_logs:
        battle_detail = battle.get("battle", {})
        if battle_detail.get("type") not in ["soloRanked"]:
            continue
        teams = battle_detail.get("teams", [])
        for team in teams:
            for player in team:
                if player.get("tag") != player_tag:
                    continue
                update_fields: list[str] = []
                update_values: list[object] = []
                player_name = player.get("name")
                trophies = player.get("brawler", {}).get("trophies", 0)
                if player_name and not player_name_in_db:
                    update_fields.append("name=%s")
                    update_values.append(player_name)
                    player_name_in_db = player_name
                if trophies is not None:
                    if trophies != player_current_rank:
                        update_fields.append("current_rank=%s")
                        update_values.append(trophies)
                        player_current_rank = trophies
                if trophies is not None and trophies > player_highest_rank:
                    update_fields.append("highest_rank=%s")
                    update_values.append(trophies)
                    player_highest_rank = trophies
                if update_fields:
                    update_values.append(player_tag)
                    cur.execute(
                        f"UPDATE players SET {', '.join(update_fields)} WHERE tag=%s",
                        update_values,
                    )
                return player_name_in_db, player_highest_rank, player_current_rank
    return player_name_in_db, player_highest_rank, player_current_rank


def request_with_retry(
    url: str,
    headers: Optional[dict[str, str]] = None,
    method: str = "GET",
    timeout: Optional[Sequence[float] | float] = None,
    max_retries: int = MAX_RETRIES,
    request_interval: float = REQUEST_INTERVAL,
) -> Tuple[Optional[requests.Response], Optional[int]]:
    """API にリクエストを送り、失敗した場合はリトライを行う汎用関数

    Returns:
        Tuple[Optional[requests.Response], Optional[int]]: レスポンスと、エラー発生時の
            HTTP ステータスコード。成功した場合は (response, None)、404 の場合は
            (None, 404)、その他のエラーの場合は (None, status_code) を返す。
    """

    if timeout is None:
        timeout_values: Tuple[float, float] = REQUEST_TIMEOUT
    elif isinstance(timeout, (int, float)):
        timeout_values = (float(timeout), float(timeout))
    else:
        timeout_seq = tuple(timeout)
        if len(timeout_seq) != 2:
            raise ValueError("timeout は (connect, read) の2要素で指定してください。")
        timeout_values = (float(timeout_seq[0]), float(timeout_seq[1]))

    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(request_interval)
            resp = SESSION.request(
                method,
                url,
                headers=headers,
                timeout=timeout_values,
            )
            if resp.status_code == 404:
                logger.warning("Resource not found: %s", url)
                return None, 404
            resp.raise_for_status()
            return resp, None
        except requests.Timeout as e:
            logger.warning(
                "Timeout while requesting %s (attempt %d/%d): %s",
                url,
                attempt,
                max_retries,
                e,
            )
        except requests.RequestException as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code == 404:
                logger.warning("Resource not found: %s", url)
                return None, 404
            if attempt == max_retries:
                logger.error("Request failed: %s", e)
                return None, status_code
            wait = 3 * attempt
            logger.warning(
                "Request failed (%d/%d): %s. Retrying in %d seconds.",
                attempt,
                max_retries,
                e,
                wait,
            )
            time.sleep(wait)
        except KeyboardInterrupt:
            raise

    return None, None


def cleanup_old_logs(conn) -> int:
    """設定された日数より前のログデータを削除"""
    cur = conn.cursor()
    threshold = (datetime.now(JST) - timedelta(days=DATA_RETENTION_DAYS)).strftime("%Y%m%d")
    cur.execute(
        """
        SELECT rl.id
        FROM rank_logs rl
        WHERE SUBSTRING(rl.id, 1, 8) < %s
          AND NOT EXISTS (
              SELECT 1
              FROM battle_logs bl
              JOIN win_lose_logs wll ON wll.battle_log_id = bl.id
              LEFT JOIN players wp ON wp.tag = wll.win_player_tag AND wp.is_monitored = 1
              LEFT JOIN players lp ON lp.tag = wll.lose_player_tag AND lp.is_monitored = 1
              WHERE bl.rank_log_id = rl.id
                AND (wp.tag IS NOT NULL OR lp.tag IS NOT NULL)
          )
        """,
        (threshold,),
    )
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

def fetch_rank_player(api_key: str, conn) -> int:
    """ランク上位プレイヤーを取得してDBへ保存"""
    cur = conn.cursor()
    new_players = 0

    for code in COUNTRY_CODE:
        url = f"https://api.brawlstars.com/v1/rankings/{code}/players"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }

        resp, status_code = request_with_retry(url, headers=headers)
        if resp is None:
            if status_code == 404:
                logger.error("国コード:%s エラー:ランキングを取得できませんでした。(404)", code)
            else:
                logger.error(
                    "国コード:%s エラー:ランキングを取得できませんでした。 status=%s",
                    code,
                    status_code,
                )
            continue

        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            logger.error("JSON の解析に失敗しました: %s", e)
            continue
        
        rank_players = data.get("items", [])

        count = 0
        tags_to_insert: list[tuple[str]] = []
        for player in rank_players:
            p_t = player.get("trophies", 0)
            if TROPHIE_BORDER < p_t or p_t == 1:
                count += 1
                p_tag = player.get("tag")
                if p_tag:
                    tags_to_insert.append((p_tag,))
        if tags_to_insert:
            cur.executemany(
                "INSERT IGNORE INTO players(tag) VALUES (%s)",
                tags_to_insert,
            )
            if cur.rowcount > 0:
                new_players += cur.rowcount

        logger.info("国コード:%s 取得プレイヤー数 %d", code, count)
        conn.commit()

    return new_players


def fetch_battle_logs(player_tag: str, api_key: str) -> tuple[int, int, int]:
    """指定したプレイヤーのバトルログを取得してDBへ保存"""
    new_players = 0
    new_rank_logs = 0
    new_battle_logs = 0
    with get_connection() as conn:
        cur = conn.cursor()
        tag_enc = quote(player_tag, safe="")
        url = f"https://api.brawlstars.com/v1/players/{tag_enc}/battlelog"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }

        resp, status_code = request_with_retry(url, headers=headers)
        if resp is None:
            if status_code == 404:
                cur.execute(
                    "DELETE FROM players WHERE tag=%s",
                    (player_tag,),
                )
                logger.warning("プレイヤーが見つかりません: %s", player_tag)
            else:
                logger.warning(
                    "プレイヤーのバトルログ取得に失敗しました。tag=%s status=%s",
                    player_tag,
                    status_code,
                )
            return (new_players, new_rank_logs, new_battle_logs)

        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            logger.error("JSON の解析に失敗しました: %s", e)
            return (new_players, new_rank_logs, new_battle_logs)

        battle_logs = data.get("items", [])
        if len(battle_logs) < 1:
            logger.info("バトルログが見つかりませんでした。")
            return (new_players, new_rank_logs, new_battle_logs)

        cur.execute(
            "SELECT name, highest_rank, current_rank FROM players WHERE tag=%s",
            (player_tag,),
        )
        row = cur.fetchone()
        player_name_in_db = (row[0] if row and row[0] else None)
        player_highest_rank = row[1] if row and row[1] is not None else 0
        player_current_rank = row[2] if row and row[2] is not None else 0

        (
            player_name_in_db,
            player_highest_rank,
            player_current_rank,
        ) = _update_player_profile_from_latest_battle(
            cur,
            battle_logs,
            player_tag,
            player_name_in_db,
            player_highest_rank,
            player_current_rank,
        )

        cur.execute(
            "UPDATE players SET last_fetched=%s WHERE tag=%s",
            (datetime.now(JST), player_tag),
        )

        rank = 0
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
            col_start_date = datetime.now(JST) - timedelta(days=DATA_RETENTION_DAYS)
            if battle_datetime < col_start_date:
                continue
            star_player = battle_detail.get("starPlayer") or {}
            star_player_tag = star_player.get("tag")
            star_brawler_id = (
                star_player.get("brawler", {}).get("id")
                if isinstance(star_player.get("brawler"), dict)
                else None
            )
            if star_player_tag:
                new_rank_flag = True
                rank_log_id = f"{battle_time}_{star_player_tag}"
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
                    brawler = player.get("brawler") or {}
                    brawler_id = brawler.get("id")
                    p_tag = player.get("tag")
                    resultLog.brawlers.append((brawler_id, p_tag))
                    player_name = player.get("name")
                    trophies = player.get("brawler", {}).get("trophies", 0)
                    if p_tag == player_tag:
                        my_side_idx = side_idx
                        resultLog.result = result
                        # if trophies < 7:
                        #     cur.execute("DELETE FROM players WHERE tag=%s", (player_tag,))
                        #     if cur.rowcount == 1:  # 削除されたら1、既に存在しなかったら0
                        #         logger.info("プレイヤー削除:%s", player_tag)
                    if p_tag and 15 < trophies <= 22:
                        cur.execute("INSERT IGNORE INTO players(tag) VALUES (%s)", (p_tag,))
                        if cur.rowcount == 1:  # 挿入されたら1、既存で無視されたら0
                            new_players += 1
                            if trophies == 22:
                                logger.info("プロランク発見:%s", p_tag)
                            elif trophies > 18:
                                logger.info("マスターランク発見:%s", p_tag)
                            elif trophies > 15:
                                logger.info("レジェンドランク発見:%s", p_tag)
                            elif trophies > 12:
                                logger.info("エピック発見:%s", p_tag)
                        if player_name:
                            cur.execute(
                                "UPDATE players SET name=%s WHERE tag=%s AND (name IS NULL OR name='')",
                                (player_name, p_tag),
                            )
                        if trophies is not None:
                            cur.execute(
                                "UPDATE players SET current_rank=%s, highest_rank=GREATEST(highest_rank, %s) WHERE tag=%s",
                                (trophies, trophies, p_tag),
                            )
                    if rank < trophies <= 22:
                        rank = trophies
                resultInfo.append(resultLog)
            if my_side_idx is not None and len(resultInfo) == 2 and result in OPPOSITE:
                other = 1 - my_side_idx
                # まだ埋まっていない場合のみ上書き
                if getattr(resultInfo[other], "result", "不明") in (None, "", "不明"):
                    resultInfo[other].result = OPPOSITE[result]

            if new_rank_brawlers_flag:
                map_id = MAP_NAME_TO_ID.get(battle_map)
                rank_id = RANK_TO_ID.get(rank)
                rank_log_id = f"{battle_time}_{star_player_tag}"
                #新規ランクマッチ登録
                inserted_rank_log = False
                try:
                    cur.execute(
                        "INSERT INTO rank_logs(id, map_id, rank_id) VALUES (%s, %s, %s)",
                        (rank_log_id, map_id, rank_id),
                    )
                    if cur.rowcount > 0:
                        new_rank_logs += cur.rowcount
                        inserted_rank_log = True
                except IntegrityError as e:
                    if e.errno == errorcode.ER_DUP_ENTRY:  # 1062: Duplicate entry
                        logger.info("重複レコードなのでスキップ")
                        new_rank_flag = False
                        new_rank_brawlers_flag = False
                        continue
                    logger.warning(
                        "未登録のマップを検出: マップ=%s マップID=%s ランク=%s",
                        battle_map,
                        battle_map_id,
                        rank,
                    )
                    logger.warning("Battle detail: %s error: %s", battle, e)
                    mode_id = cur.execute("SELECT id FROM _modes WHERE name=%s", (battle_mode,)).fetchone()[0]
                    cur.execute(
                        "REPLACE INTO _maps(id, name, mode_id) VALUES (%s, %s, %s)",
                        (battle_map_id, battle_map, mode_id),
                    )
                    MAP_NAME_TO_ID[battle_map] = battle_map_id
                    cur.execute(
                        "INSERT INTO rank_logs(id, map_id, rank_id) VALUES (%s, %s, %s)",
                        (rank_log_id, map_id, rank_id),
                    )
                    if cur.rowcount > 0:
                        new_rank_logs += cur.rowcount
                        inserted_rank_log = True
                if inserted_rank_log and star_brawler_id:
                    cur.execute(
                        "INSERT INTO rank_star_logs(rank_log_id, star_brawler_id, star_player_tag)"
                        " VALUES (%s, %s, %s) "
                        "ON DUPLICATE KEY UPDATE "
                        "star_brawler_id=VALUES(star_brawler_id), "
                        "star_player_tag=VALUES(star_player_tag)",
                        (rank_log_id, star_brawler_id, star_player_tag),
                    )
                new_rank_brawlers_flag = False

            #新規バトル登録
            battle_log_id = f"{battle_time}_{p_tag}_battle"
            try:
                cur.execute(
                    "INSERT INTO battle_logs(id, rank_log_id) VALUES (%s, %s)",
                    (battle_log_id, rank_log_id),
                )
                if cur.rowcount > 0:
                    new_battle_logs += cur.rowcount
            except IntegrityError:
                logger.debug(
                    "既に記録済みのバトルのためスキップ battle_log_id=%s rank_log_id=%s",
                    battle_log_id,
                    rank_log_id,
                )
                continue

            winners = [b for r in resultInfo if r.result == "victory" for b in r.brawlers]
            losers = [b for r in resultInfo if r.result == "defeat" for b in r.brawlers]
            pairs = {
                (w_id, w_tag, l_id, l_tag, battle_log_id)
                for w_id, w_tag in winners
                for l_id, l_tag in losers
                if isinstance(w_id, int)
                and isinstance(l_id, int)
                and isinstance(w_tag, str)
                and isinstance(l_tag, str)
                and w_tag
                and l_tag
            }
            if pairs:
                cur.executemany(
                    "INSERT IGNORE INTO win_lose_logs(win_brawler_id, win_player_tag, lose_brawler_id, lose_player_tag, battle_log_id) VALUES (%s, %s, %s, %s, %s)",
                    list(pairs),
                )

        conn.commit()
        return (new_players, new_rank_logs, new_battle_logs)
            

def main() -> None:
    setup_logging()

    load_environment()

    api_key = os.getenv("BRAWL_STARS_API_KEY")
    if not api_key:
        raise RuntimeError("環境変数 BRAWL_STARS_API_KEY が設定されていません。")
    logger.info("データ保持期間（日数）: %d", DATA_RETENTION_DAYS)
    try:
        with get_connection() as conn:
    
            deleted = cleanup_old_logs(conn)
            logger.info("削除したランクマッチ数:%d", deleted)

            start_time = time.time()

            new_players_total = 0
            new_rank_logs_total = 0
            new_battle_logs_total = 0

            # new_players_total += fetch_rank_player(api_key, conn)
            rest = 0

            try:
                while 1:
                    cur = conn.cursor()
                    seventy_two_hours_ago = datetime.now(JST) - timedelta(hours=ACQ_CYCLE_TIME)
                    
                    cur.execute(
                        """
                        SELECT tag FROM players
                        WHERE last_fetched < %s
                        ORDER BY is_monitored DESC, last_fetched ASC
                        LIMIT %s
                        """,
                        (seventy_two_hours_ago, FETCH_BATCH_SIZE),
                    )
                    rows = cur.fetchall()
                    tags = [r[0] for r in rows]

                    if not tags:
                        logger.info("対象プレイヤーがいません")
                        break

                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        results = executor.map(lambda t: fetch_battle_logs(t, api_key), tags)
                        for players_added, rank_added, battles_added in results:
                            new_players_total += players_added
                            new_rank_logs_total += rank_added
                            new_battle_logs_total += battles_added

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
                logger.info("処理時間:%s", format_time(total_time))

    except mysql.connector.Error as e:
        logger.error("データベース接続エラー: %s", e)
        return
    logger.info("バトルログの取得が完了しました。")
    logger.info("新規登録プレイヤー:%d", new_players_total)
    logger.info("新規登録ランクマッチ:%d", new_rank_logs_total)
    logger.info("新規登録バトル:%d", new_battle_logs_total)

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
