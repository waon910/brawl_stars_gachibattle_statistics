from __future__ import annotations

"""監視対象プレイヤーごとの統計情報を出力するモジュール."""

import argparse
import json
import logging

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Mapping

import mysql.connector

from .db import get_connection
from .logging_config import setup_logging
from .settings import DATA_RETENTION_DAYS, MIN_RANK_ID

logger = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))


@dataclass(frozen=True, slots=True)
class MonitoredPlayer:
    """監視対象のプレイヤー情報."""

    tag: str
    name: str | None
    highest_rank_id: int | None
    current_rank_id: int | None


@dataclass(frozen=True, slots=True)
class PlayerBattleRecord:
    """プレイヤー単位のバトル結果."""

    player_tag: str
    map_id: int
    brawler_id: int
    is_win: bool
    is_star: bool


@dataclass(frozen=True, slots=True)
class MonitoredPlayerDataset:
    """監視対象プレイヤーの統計計算用データセット."""

    players: Mapping[str, MonitoredPlayer]
    battles: List[PlayerBattleRecord]


def _empty_counter() -> Dict[str, int]:
    return {"wins": 0, "losses": 0, "mvp": 0}


def fetch_monitored_player_dataset(conn, since: str) -> MonitoredPlayerDataset:
    """監視対象プレイヤーと対象期間のバトル情報を読み込む."""

    cursor = conn.cursor()
    cursor.execute(
        "SELECT tag, name, highest_rank, current_rank FROM players WHERE is_monitored = 1"
    )
    players: Dict[str, MonitoredPlayer] = {}
    for tag, name, highest_rank, current_rank in cursor.fetchall():
        players[str(tag)] = MonitoredPlayer(
            tag=str(tag),
            name=str(name) if name else None,
            highest_rank_id=int(highest_rank) if highest_rank is not None else None,
            current_rank_id=int(current_rank) if current_rank is not None else None,
        )

    if not players:
        cursor.close()
        logger.info("監視対象プレイヤーが存在しないため、統計処理をスキップします")
        return MonitoredPlayerDataset(players={}, battles=[])

    logger.info("監視対象プレイヤー数: %d", len(players))

    query = """
        WITH monitored_players AS (
            SELECT tag FROM players WHERE is_monitored = 1
        ),
        player_battles AS (
            SELECT
                raw.player_tag,
                raw.rank_log_id,
                raw.map_id,
                raw.brawler_id,
                MAX(raw.is_win) AS is_win
            FROM (
                SELECT
                    wll.win_player_tag AS player_tag,
                    bl.rank_log_id,
                    rl.map_id,
                    wll.win_brawler_id AS brawler_id,
                    1 AS is_win
                FROM win_lose_logs wll
                JOIN battle_logs bl ON bl.id = wll.battle_log_id
                JOIN rank_logs rl ON rl.id = bl.rank_log_id
                JOIN monitored_players mp ON mp.tag = wll.win_player_tag
                WHERE rl.rank_id >= %s AND rl.id >= %s
                UNION ALL
                SELECT
                    wll.lose_player_tag AS player_tag,
                    bl.rank_log_id,
                    rl.map_id,
                    wll.lose_brawler_id AS brawler_id,
                    0 AS is_win
                FROM win_lose_logs wll
                JOIN battle_logs bl ON bl.id = wll.battle_log_id
                JOIN rank_logs rl ON rl.id = bl.rank_log_id
                JOIN monitored_players mp ON mp.tag = wll.lose_player_tag
                WHERE rl.rank_id >= %s AND rl.id >= %s
            ) AS raw
            GROUP BY raw.player_tag, raw.rank_log_id, raw.map_id, raw.brawler_id
        )
        SELECT
            pb.player_tag,
            pb.map_id,
            pb.brawler_id,
            pb.is_win,
            CASE WHEN rsl.star_brawler_id = pb.brawler_id THEN 1 ELSE 0 END AS is_star
        FROM player_battles pb
        LEFT JOIN rank_star_logs rsl ON rsl.rank_log_id = pb.rank_log_id
        ORDER BY pb.player_tag, pb.rank_log_id
    """

    cursor.execute(query, (MIN_RANK_ID, since, MIN_RANK_ID, since))
    battles: List[PlayerBattleRecord] = []
    fetched_rows = 0
    for player_tag, map_id, brawler_id, is_win, is_star in cursor.fetchall():
        fetched_rows += 1
        battles.append(
            PlayerBattleRecord(
                player_tag=str(player_tag),
                map_id=int(map_id),
                brawler_id=int(brawler_id),
                is_win=bool(is_win),
                is_star=bool(is_star),
            )
        )
    cursor.close()

    logger.info("監視対象プレイヤーのバトル件数: %d", fetched_rows)

    return MonitoredPlayerDataset(players=players, battles=battles)


def compute_monitored_player_stats(dataset: MonitoredPlayerDataset) -> Dict[str, object]:
    """監視対象プレイヤーごとの統計情報を集計する."""

    results: Dict[str, Dict[str, object]] = {}

    for player_tag, player in dataset.players.items():
        results[player_tag] = {
            "name": player.name,
            "highest_rank_id": player.highest_rank_id,
            "current_rank_id": player.current_rank_id,
            "per_map_per_brawler": {},
            "per_map_loss_ranking": {},
            "per_map_overall": {},
            "overall": _convert_counter(_empty_counter()),
        }

    per_player_map_brawler: Dict[str, Dict[int, Dict[int, Dict[str, int]]]] = {}
    per_player_map_totals: Dict[str, Dict[int, Dict[str, int]]] = {}
    per_player_totals: Dict[str, Dict[str, int]] = {tag: _empty_counter() for tag in dataset.players}

    for record in dataset.battles:
        if record.player_tag not in dataset.players:
            continue
        map_stats = per_player_map_brawler.setdefault(record.player_tag, {})
        brawler_stats = map_stats.setdefault(record.map_id, {})
        counter = brawler_stats.setdefault(record.brawler_id, _empty_counter())
        total_counter = per_player_map_totals.setdefault(record.player_tag, {}).setdefault(
            record.map_id, _empty_counter()
        )
        overall_counter = per_player_totals.setdefault(record.player_tag, _empty_counter())

        if record.is_win:
            counter["wins"] += 1
            total_counter["wins"] += 1
            overall_counter["wins"] += 1
        else:
            counter["losses"] += 1
            total_counter["losses"] += 1
            overall_counter["losses"] += 1
        if record.is_star:
            counter["mvp"] += 1
            total_counter["mvp"] += 1
            overall_counter["mvp"] += 1

    for player_tag, player_result in results.items():
        map_brawlers = per_player_map_brawler.get(player_tag, {})
        map_totals = per_player_map_totals.get(player_tag, {})
        totals_counter = per_player_totals.get(player_tag, _empty_counter())

        per_map_per_brawler: Dict[str, Dict[str, object]] = {}
        per_map_loss_ranking: Dict[str, List[Dict[str, int]]] = {}
        per_map_overall: Dict[str, Dict[str, object]] = {}

        for map_id, brawler_stats in map_brawlers.items():
            map_key = str(map_id)
            per_brawler: Dict[str, object] = {}
            loss_ranking: List[Dict[str, int]] = []
            for brawler_id, counter in brawler_stats.items():
                per_brawler[str(brawler_id)] = _convert_counter(counter)
                losses = counter["losses"]
                if losses > 0:
                    loss_ranking.append({"brawler_id": brawler_id, "losses": losses})
            loss_ranking.sort(key=lambda item: (-item["losses"], item["brawler_id"]))
            per_map_per_brawler[map_key] = per_brawler
            per_map_loss_ranking[map_key] = loss_ranking

        for map_id, counter in map_totals.items():
            per_map_overall[str(map_id)] = _convert_counter(counter)

        player_result["per_map_per_brawler"] = per_map_per_brawler
        player_result["per_map_loss_ranking"] = per_map_loss_ranking
        player_result["per_map_overall"] = per_map_overall
        player_result["overall"] = _convert_counter(totals_counter)

    return {"players": results}


def _convert_counter(counter: Mapping[str, int]) -> Dict[str, object]:
    wins = int(counter.get("wins", 0))
    losses = int(counter.get("losses", 0))
    mvp = int(counter.get("mvp", 0))
    games = wins + losses
    win_rate = round((wins / games) * 100, 2) if games else 0.0
    mvp_rate = round((mvp / games) * 100, 2) if games else 0.0
    return {
        "wins": wins,
        "losses": losses,
        "games": games,
        "win_rate": win_rate,
        "mvp_count": mvp,
        "mvp_rate": mvp_rate,
    }


def export_monitored_player_stats(dataset: MonitoredPlayerDataset, output: Path) -> None:
    """監視対象プレイヤー統計をJSONに出力する."""

    stats = compute_monitored_player_stats(dataset)
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as fp:
        json.dump(stats, fp, ensure_ascii=False, indent=2)
    logger.info("監視対象プレイヤー統計を出力しました: %s", output)


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="監視対象プレイヤー統計をJSONとして出力")
    parser.add_argument(
        "--output",
        default="monitored_player_stats.json",
        help="出力先JSONファイル",
    )
    args = parser.parse_args()

    jst_now = datetime.now(JST)
    since = (jst_now - timedelta(days=DATA_RETENTION_DAYS)).strftime("%Y%m%d")
    logger.info("統計対象期間（日数）: %d", DATA_RETENTION_DAYS)

    logger.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as exc:
        raise SystemExit(f"データベースに接続できません: {exc}")

    try:
        dataset = fetch_monitored_player_dataset(conn, since)
    except mysql.connector.Error as exc:
        raise SystemExit(f"クエリの実行に失敗しました: {exc}")
    finally:
        conn.close()

    export_monitored_player_stats(dataset, Path(args.output))


if __name__ == "__main__":
    main()
