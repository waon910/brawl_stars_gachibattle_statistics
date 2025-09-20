"""統計処理で共通利用するランクマッチデータの読み込みユーティリティ."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .settings import MIN_RANK_ID

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RankLogEntry:
    """ランクログ1件分のメタデータ."""

    id: int
    map_id: int
    rank_id: int
    mode_id: Optional[int]
    date_key: str


@dataclass(frozen=True)
class RankedBattle:
    """1試合分の勝敗情報."""

    battle_log_id: int
    rank_log_id: int
    map_id: int
    rank_id: int
    mode_id: Optional[int]
    win_brawlers: Tuple[int, ...]
    lose_brawlers: Tuple[int, ...]


@dataclass
class StatsDataset:
    """統計エクスポートで使い回すデータセット."""

    rank_logs: Dict[int, RankLogEntry]
    battles: List[RankedBattle]
    star_logs: List[Tuple[int, int]]
    _participants_cache: Optional[Dict[int, Set[int]]] = field(
        default=None, init=False, repr=False
    )

    def iter_ranked_battles(self) -> Iterable[RankedBattle]:
        """読みやすい名前のイテレータを提供."""

        return iter(self.battles)

    def participants_by_rank_log(self) -> Dict[int, Set[int]]:
        """ランクログIDごとの参加キャラクター集合を取得する."""

        if self._participants_cache is None:
            participants: Dict[int, Set[int]] = defaultdict(set)
            for battle in self.battles:
                if battle.win_brawlers:
                    participants[battle.rank_log_id].update(battle.win_brawlers)
                if battle.lose_brawlers:
                    participants[battle.rank_log_id].update(battle.lose_brawlers)
            self._participants_cache = {k: set(v) for k, v in participants.items()}
        return self._participants_cache


def load_recent_ranked_battles(conn, since: str) -> StatsDataset:
    """直近期間のランクマッチ関連データをまとめて読み込む."""

    cursor = conn.cursor()

    logger.info("ランクログ情報を読み込んでいます")
    rank_logs: Dict[int, RankLogEntry] = {}
    cursor.execute(
        """
        SELECT rl.id, rl.map_id, rl.rank_id, m.mode_id
        FROM rank_logs rl
        LEFT JOIN _maps m ON rl.map_id = m.id
        WHERE rl.rank_id >= %s AND SUBSTRING(rl.id, 1, 8) >= %s
        """,
        (MIN_RANK_ID, since),
    )
    for rl_id, map_id, rank_id, mode_id in cursor.fetchall():
        rl_id_int = int(rl_id)
        rank_logs[rl_id_int] = RankLogEntry(
            id=rl_id_int,
            map_id=int(map_id),
            rank_id=int(rank_id),
            mode_id=int(mode_id) if mode_id is not None else None,
            date_key=str(rl_id)[:8],
        )

    logger.info("バトルログ情報を読み込んでいます")
    cursor.execute(
        """
        SELECT bl.id, bl.rank_log_id
        FROM battle_logs bl
        JOIN rank_logs rl ON bl.rank_log_id = rl.id
        WHERE rl.rank_id >= %s AND SUBSTRING(rl.id, 1, 8) >= %s
        """,
        (MIN_RANK_ID, since),
    )
    battle_rank_map: Dict[int, int] = {}
    for battle_log_id, rank_log_id in cursor.fetchall():
        battle_rank_map[int(battle_log_id)] = int(rank_log_id)

    logger.info("勝敗ログを読み込んでいます")
    cursor.execute(
        """
        SELECT wl.battle_log_id, wl.win_brawler_id, wl.lose_brawler_id
        FROM win_lose_logs wl
        JOIN battle_logs bl ON wl.battle_log_id = bl.id
        JOIN rank_logs rl ON bl.rank_log_id = rl.id
        WHERE rl.rank_id >= %s AND SUBSTRING(rl.id, 1, 8) >= %s
        """,
        (MIN_RANK_ID, since),
    )

    def _team_factory() -> Dict[str, Set[int]]:
        return {"win": set(), "lose": set()}

    battle_teams: Dict[int, Dict[str, Set[int]]] = defaultdict(_team_factory)
    for battle_log_id, win_brawler_id, lose_brawler_id in cursor.fetchall():
        battle_id = int(battle_log_id)
        if battle_id not in battle_rank_map:
            continue
        battle_teams[battle_id]["win"].add(int(win_brawler_id))
        battle_teams[battle_id]["lose"].add(int(lose_brawler_id))

    logger.info("スター獲得ログを読み込んでいます")
    cursor.execute(
        """
        SELECT rsl.rank_log_id, rsl.star_brawler_id
        FROM rank_star_logs rsl
        JOIN rank_logs rl ON rsl.rank_log_id = rl.id
        WHERE rl.rank_id >= %s AND SUBSTRING(rl.id, 1, 8) >= %s
        """,
        (MIN_RANK_ID, since),
    )
    star_logs: List[Tuple[int, int]] = []
    for rank_log_id, star_brawler_id in cursor.fetchall():
        rl_id_int = int(rank_log_id)
        if rl_id_int not in rank_logs:
            continue
        star_logs.append((rl_id_int, int(star_brawler_id)))

    cursor.close()

    battles: List[RankedBattle] = []
    for battle_log_id, rank_log_id in battle_rank_map.items():
        rank_entry = rank_logs.get(rank_log_id)
        if rank_entry is None:
            continue
        teams = battle_teams.get(battle_log_id)
        win_team: Tuple[int, ...] = ()
        lose_team: Tuple[int, ...] = ()
        if teams:
            if teams["win"]:
                win_team = tuple(sorted(int(b) for b in teams["win"]))
            if teams["lose"]:
                lose_team = tuple(sorted(int(b) for b in teams["lose"]))
        battles.append(
            RankedBattle(
                battle_log_id=battle_log_id,
                rank_log_id=rank_log_id,
                map_id=rank_entry.map_id,
                rank_id=rank_entry.rank_id,
                mode_id=rank_entry.mode_id,
                win_brawlers=win_team,
                lose_brawlers=lose_team,
            )
        )

    logger.info("ランクログ: %d件, バトル: %d件を読み込みました", len(rank_logs), len(battles))
    return StatsDataset(rank_logs=rank_logs, battles=battles, star_logs=star_logs)

