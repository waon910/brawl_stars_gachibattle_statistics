from __future__ import annotations

"""監視対象プレイヤーごとの統計情報を出力するモジュール."""

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Mapping

import mysql.connector

from .db import get_connection
from .logging_config import setup_logging

logger = logging.getLogger(__name__)


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
    battle_log_id: str
    rank_log_id: str
    map_id: int
    brawler_id: int
    is_win: bool


@dataclass(slots=True)
class BattleStats:
    """勝敗数を保持するシンプルなカウンタ."""

    wins: int = 0
    losses: int = 0

    def register_result(self, is_win: bool) -> None:
        if is_win:
            self.wins += 1
        else:
            self.losses += 1

    def to_dict(self, rank_games: int | None = None) -> Dict[str, object]:
        games = self.wins + self.losses
        rank_games_value = rank_games if rank_games is not None else games
        win_rate = round((self.wins / games) * 100, 2) if games else 0.0
        return {
            "wins": self.wins,
            "losses": self.losses,
            "games": games,
            "rank_games": rank_games_value,
            "win_rate": win_rate,
        }


@dataclass(frozen=True, slots=True)
class MonitoredPlayerDataset:
    """監視対象プレイヤーの統計計算用データセット."""

    players: Mapping[str, MonitoredPlayer]
    battles: List[PlayerBattleRecord]


@dataclass(slots=True)
class AggregatedStats:
    """勝敗集計と紐づくランクログID集合を保持するヘルパー."""

    stats: BattleStats
    rank_log_ids: set[str]

    def __init__(self) -> None:
        self.stats = BattleStats()
        self.rank_log_ids = set()

    def register(self, is_win: bool, rank_log_id: str) -> None:
        self.stats.register_result(is_win)
        self.rank_log_ids.add(rank_log_id)

    def to_dict(self) -> Dict[str, object]:
        return self.stats.to_dict(len(self.rank_log_ids))


@dataclass(slots=True)
class PlayerAggregation:
    """監視対象プレイヤー単位の集計データ."""

    per_map_per_brawler: Dict[int, Dict[int, AggregatedStats]]
    per_map_totals: Dict[int, AggregatedStats]
    overall: AggregatedStats

    def __init__(self) -> None:
        self.per_map_per_brawler = {}
        self.per_map_totals = {}
        self.overall = AggregatedStats()

    def register_battle(
        self, map_id: int, brawler_id: int, is_win: bool, rank_log_id: str
    ) -> None:
        map_brawlers = self.per_map_per_brawler.setdefault(map_id, {})
        brawler_aggregation = map_brawlers.setdefault(brawler_id, AggregatedStats())
        brawler_aggregation.register(is_win, rank_log_id)

        map_total_aggregation = self.per_map_totals.setdefault(map_id, AggregatedStats())
        map_total_aggregation.register(is_win, rank_log_id)

        self.overall.register(is_win, rank_log_id)


def fetch_monitored_player_dataset(conn) -> MonitoredPlayerDataset:
    """監視対象プレイヤーのバトル情報を全件読み込む."""

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
                wll.win_player_tag AS player_tag,
                wll.battle_log_id,
                wll.win_brawler_id AS brawler_id,
                1 AS is_win
            FROM win_lose_logs wll
            JOIN monitored_players mp ON mp.tag = wll.win_player_tag
            UNION ALL
            SELECT
                wll.lose_player_tag AS player_tag,
                wll.battle_log_id,
                wll.lose_brawler_id AS brawler_id,
                0 AS is_win
            FROM win_lose_logs wll
            JOIN monitored_players mp ON mp.tag = wll.lose_player_tag
        )
        SELECT
            pb.player_tag,
            pb.battle_log_id,
            bl.rank_log_id,
            rl.map_id,
            pb.brawler_id,
            pb.is_win
        FROM player_battles pb
        JOIN battle_logs bl ON bl.id = pb.battle_log_id
        JOIN rank_logs rl ON rl.id = bl.rank_log_id
    """

    cursor.execute(query)
    raw_rows = cursor.fetchall()
    cursor.close()

    deduped_battles: Dict[tuple[str, str], PlayerBattleRecord] = {}
    duplicate_count = 0

    for player_tag, battle_log_id, rank_log_id, map_id, brawler_id, is_win in raw_rows:
        record = PlayerBattleRecord(
            player_tag=str(player_tag),
            battle_log_id=str(battle_log_id),
            rank_log_id=str(rank_log_id),
            map_id=int(map_id),
            brawler_id=int(brawler_id),
            is_win=bool(is_win),
        )

        key = (record.player_tag, record.battle_log_id)
        existing = deduped_battles.get(key)
        if existing is None:
            deduped_battles[key] = record
            continue

        if (
            existing.rank_log_id != record.rank_log_id
            or existing.map_id != record.map_id
            or existing.brawler_id != record.brawler_id
            or existing.is_win != record.is_win
        ):
            logger.warning(
                "同一バトルに複数の不整合レコードが存在します: player=%s, battle=%s",
                record.player_tag,
                record.battle_log_id,
            )
        duplicate_count += 1

    battles = list(deduped_battles.values())

    logger.info("監視対象プレイヤーのバトル件数(重複除外前): %d", len(raw_rows))
    logger.info("重複除外済みバトル件数: %d", len(battles))
    if duplicate_count:
        logger.info("除外した重複バトル件数: %d", duplicate_count)

    return MonitoredPlayerDataset(players=players, battles=battles)


def compute_monitored_player_stats(dataset: MonitoredPlayerDataset) -> Dict[str, object]:
    """監視対象プレイヤーごとの統計情報を集計する."""

    monitored_players = dataset.players
    per_player_aggregations: Dict[str, PlayerAggregation] = {}

    for record in dataset.battles:
        if record.player_tag not in monitored_players:
            continue

        aggregation = per_player_aggregations.setdefault(
            record.player_tag, PlayerAggregation()
        )
        aggregation.register_battle(
            record.map_id, record.brawler_id, record.is_win, record.rank_log_id
        )

    results: Dict[str, Dict[str, object]] = {}

    for player_tag, player in monitored_players.items():
        aggregation = per_player_aggregations.get(player_tag)

        per_map_per_brawler: Dict[str, Dict[str, object]] = {}
        per_map_overall: Dict[str, Dict[str, object]] = {}

        if aggregation:
            for map_id, brawlers in aggregation.per_map_per_brawler.items():
                per_map_per_brawler[str(map_id)] = {
                    str(brawler_id): brawler_stats.to_dict()
                    for brawler_id, brawler_stats in brawlers.items()
                }

            for map_id, map_stats in aggregation.per_map_totals.items():
                per_map_overall[str(map_id)] = map_stats.to_dict()

            overall_dict = aggregation.overall.to_dict()
        else:
            overall_dict = BattleStats().to_dict(0)

        results[player_tag] = {
            "name": player.name,
            "highest_rank_id": player.highest_rank_id,
            "current_rank_id": player.current_rank_id,
            "per_map_per_brawler": per_map_per_brawler,
            "per_map_overall": per_map_overall,
            "overall": overall_dict,
        }

    return {"players": results}


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

    logger.info("データベースに接続しています")
    try:
        conn = get_connection()
    except mysql.connector.Error as exc:
        raise SystemExit(f"データベースに接続できません: {exc}")

    try:
        dataset = fetch_monitored_player_dataset(conn)
    except mysql.connector.Error as exc:
        raise SystemExit(f"クエリの実行に失敗しました: {exc}")
    finally:
        conn.close()

    logger.info("監視対象プレイヤーの全期間データを集計します")
    export_monitored_player_stats(dataset, Path(args.output))


if __name__ == "__main__":
    main()
