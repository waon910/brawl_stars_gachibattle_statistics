"""統計処理で共通利用するランクマッチデータの読み込みユーティリティ."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

from .memory_utils import log_memory_usage
from .settings import MIN_RANK_ID

logger = logging.getLogger(__name__)


FETCH_BATCH_SIZE = 100_000


def _iter_cursor(cursor, batch_size: int) -> Iterator[Sequence[Tuple[Any, ...]]]:
    """指定したバッチサイズでカーソルを順次読み出す."""

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows


@dataclass(frozen=True, slots=True)
class RankLogEntry:
    """ランクログ1件分のメタデータ."""

    id: str
    map_id: int
    rank_id: int
    mode_id: Optional[int]
    date_key: str


@dataclass(frozen=True, slots=True)
class RankedBattle:
    """1試合分の勝敗情報."""

    battle_log_id: str
    rank_log_id: str
    map_id: int
    rank_id: int
    mode_id: Optional[int]
    win_brawlers: Tuple[int, ...]
    lose_brawlers: Tuple[int, ...]


@dataclass(slots=True)
class StatsDataset:
    """統計エクスポートで使い回すデータセット."""

    rank_logs: Dict[str, RankLogEntry]
    battles: List[RankedBattle]
    star_logs: List[Tuple[str, int]]
    _participants_cache: Optional[Dict[str, Set[int]]] = field(
        default=None, init=False, repr=False
    )

    def iter_ranked_battles(self) -> Iterable[RankedBattle]:
        """読みやすい名前のイテレータを提供."""

        return iter(self.battles)

    def participants_by_rank_log(self) -> Dict[str, Set[int]]:
        """ランクログIDごとの参加キャラクター集合を取得する."""

        if self._participants_cache is None:
            participants: Dict[str, Set[int]] = defaultdict(set)
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

    total_start = perf_counter()

    logger.info("ランクログ情報を読み込んでいます")
    rank_logs: Dict[str, RankLogEntry] = {}
    # NOTE: rank_logs.id は日付8桁+連番の文字列であり、プレフィックス順に
    # 並ぶため、単純な文字列比較で対象期間以降を効率よく抽出できる。
    # SUBSTRING を使うとインデックスが効かず巨大テーブルの全走査が発生し
    # ていたため、ここでは下限値の文字列比較に置き換えている。
    rank_log_id_lower_bound = since
    query_start = perf_counter()
    cursor.execute(
        """
        SELECT rl.id, rl.map_id, rl.rank_id, m.mode_id
        FROM rank_logs rl
        LEFT JOIN _maps m ON rl.map_id = m.id
        WHERE rl.rank_id >= %s AND rl.id >= %s
        """,
        (MIN_RANK_ID, rank_log_id_lower_bound),
    )
    log_memory_usage("rank_logs 取得開始")
    processed_rank_logs = 0
    for rows in _iter_cursor(cursor, FETCH_BATCH_SIZE):
        for rl_id, map_id, rank_id, mode_id in rows:
            rl_id_str = str(rl_id)
            rank_logs[rl_id_str] = RankLogEntry(
                id=rl_id_str,
                map_id=int(map_id),
                rank_id=int(rank_id),
                mode_id=int(mode_id) if mode_id is not None else None,
                date_key=rl_id_str[:8],
            )
        processed_rank_logs += len(rows)
    logger.info(
        "ランクログ取得・加工完了: %d件 (%.2f秒)",
        processed_rank_logs,
        perf_counter() - query_start,
    )
    log_memory_usage("rank_logs 加工後")

    logger.info("バトルログ情報を読み込んでいます")
    query_start = perf_counter()
    cursor.execute(
        """
        SELECT bl.id, bl.rank_log_id
        FROM battle_logs bl
        JOIN rank_logs rl ON bl.rank_log_id = rl.id
        WHERE rl.rank_id >= %s AND rl.id >= %s
        """,
        (MIN_RANK_ID, rank_log_id_lower_bound),
    )
    battle_rank_map: Dict[str, str] = {}
    log_memory_usage("battle_logs 取得開始")
    processed_battles = 0
    for rows in _iter_cursor(cursor, FETCH_BATCH_SIZE):
        for battle_log_id, rank_log_id in rows:
            battle_rank_map[str(battle_log_id)] = str(rank_log_id)
        processed_battles += len(rows)
    logger.info(
        "バトルログ取得・加工完了: %d件 (%.2f秒)",
        processed_battles,
        perf_counter() - query_start,
    )
    log_memory_usage("battle_logs 加工後")

    logger.info("勝敗ログを読み込んでいます")
    query_start = perf_counter()
    cursor.execute(
        """
        SELECT wl.battle_log_id, wl.win_brawler_id, wl.lose_brawler_id
        FROM win_lose_logs wl
        JOIN battle_logs bl ON wl.battle_log_id = bl.id
        JOIN rank_logs rl ON bl.rank_log_id = rl.id
        WHERE rl.rank_id >= %s AND rl.id >= %s
        """,
        (MIN_RANK_ID, rank_log_id_lower_bound),
    )

    def _team_factory() -> Dict[str, Set[int]]:
        return {"win": set(), "lose": set()}

    battle_teams: Dict[str, Dict[str, Set[int]]] = defaultdict(_team_factory)
    log_memory_usage("win_lose_logs 取得開始")
    processed_win_lose_rows = 0
    for rows in _iter_cursor(cursor, FETCH_BATCH_SIZE):
        for battle_log_id, win_brawler_id, lose_brawler_id in rows:
            battle_id = str(battle_log_id)
            if battle_id not in battle_rank_map:
                continue
            battle_teams[battle_id]["win"].add(int(win_brawler_id))
            battle_teams[battle_id]["lose"].add(int(lose_brawler_id))
        processed_win_lose_rows += len(rows)
    logger.info(
        "勝敗ログ取得・加工完了: %d件 (%.2f秒)",
        processed_win_lose_rows,
        perf_counter() - query_start,
    )
    log_memory_usage("win_lose_logs 加工後")

    logger.info("スター獲得ログを読み込んでいます")
    query_start = perf_counter()
    cursor.execute(
        """
        SELECT rsl.rank_log_id, rsl.star_brawler_id
        FROM rank_star_logs rsl
        JOIN rank_logs rl ON rsl.rank_log_id = rl.id
        WHERE rl.rank_id >= %s AND rl.id >= %s
        """,
        (MIN_RANK_ID, rank_log_id_lower_bound),
    )
    star_logs: List[Tuple[str, int]] = []
    log_memory_usage("star_logs 取得開始")
    processed_star_logs = 0
    for rows in _iter_cursor(cursor, FETCH_BATCH_SIZE):
        for rank_log_id, star_brawler_id in rows:
            rl_id_str = str(rank_log_id)
            if rl_id_str not in rank_logs:
                continue
            star_logs.append((rl_id_str, int(star_brawler_id)))
        processed_star_logs += len(rows)
    logger.info(
        "スター獲得ログ取得・加工完了: %d件 (%.2f秒)",
        processed_star_logs,
        perf_counter() - query_start,
    )
    log_memory_usage("star_logs 加工後")

    cursor.close()

    battles: List[RankedBattle] = []
    participants: Dict[str, Set[int]] = defaultdict(set)
    build_start = perf_counter()
    for battle_log_id, rank_log_id in battle_rank_map.items():
        rank_entry = rank_logs.get(rank_log_id)
        if rank_entry is None:
            continue
        teams = battle_teams.get(battle_log_id)
        win_team: Tuple[int, ...] = ()
        lose_team: Tuple[int, ...] = ()
        if teams:
            if teams["win"]:
                win_members = {int(b) for b in teams["win"]}
                win_team = tuple(sorted(win_members))
                participants[rank_log_id].update(win_members)
            if teams["lose"]:
                lose_members = {int(b) for b in teams["lose"]}
                lose_team = tuple(sorted(lose_members))
                participants[rank_log_id].update(lose_members)
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

    dataset = StatsDataset(rank_logs=rank_logs, battles=battles, star_logs=star_logs)
    logger.info(
        "RankedBattle生成完了: %d件 (%.2f秒)",
        len(battles),
        perf_counter() - build_start,
    )
    log_memory_usage("RankedBattle 生成後")
    if participants:
        dataset._participants_cache = {k: set(v) for k, v in participants.items()}

    logger.info(
        "ランクログ: %d件, バトル: %d件を読み込みました (総処理時間: %.2f秒)",
        len(rank_logs),
        len(battles),
        perf_counter() - total_start,
    )
    return dataset

