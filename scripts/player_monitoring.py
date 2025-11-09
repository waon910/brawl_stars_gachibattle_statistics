"""プレイヤーの監視対象フラグを管理する CLI スクリプト。"""
from __future__ import annotations

import argparse
import os
<<<<<<< ours

=======
>>>>>>> theirs
import sys
from datetime import datetime
from typing import Iterable, Sequence

# src パッケージを import できるようにパスを調整
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.db import get_connection  # noqa: E402


def _normalize_tag(tag: str) -> str:
    normalized = tag.strip().upper()
    if not normalized:
        raise ValueError("プレイヤータグが空です。")
    if not normalized.startswith("#"):
        normalized = "#" + normalized
    return normalized


def _normalize_tags(tags: Iterable[str]) -> list[str]:
    normalized_tags: list[str] = []
    seen = set()
    for raw_tag in tags:
        tag = _normalize_tag(raw_tag)
        if tag not in seen:
            seen.add(tag)
            normalized_tags.append(tag)
    return normalized_tags


def _split_existing_and_missing(cur, tags: Sequence[str]) -> tuple[list[str], list[str]]:
    if not tags:
        return [], []
    placeholders = ",".join(["%s"] * len(tags))
    cur.execute(
        f"SELECT tag FROM players WHERE tag IN ({placeholders})",
        list(tags),
    )
    existing = {row[0] for row in cur.fetchall()}
    existing_ordered = [tag for tag in tags if tag in existing]
    missing = [tag for tag in tags if tag not in existing]
    return existing_ordered, missing


def _monitor_players(tags: Sequence[str]) -> int:
    normalized = _normalize_tags(tags)
    if not normalized:
        print("タグが指定されていません。", file=sys.stderr)
        return 1
    with get_connection() as conn:
        cur = conn.cursor()
        existing, missing = _split_existing_and_missing(cur, normalized)
        if missing:
            print("存在しないプレイヤータグ: " + ", ".join(missing), file=sys.stderr)
        if not existing:
            print("監視対象に設定できるプレイヤーがありませんでした。")
            return 1
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        placeholders = ",".join(["%s"] * len(existing))
        cur.execute(
            f"""
            UPDATE players
            SET is_monitored = 1,
                monitoring_started_at = CASE
                    WHEN monitoring_started_at IS NULL THEN %s
                    ELSE monitoring_started_at
                END
            WHERE tag IN ({placeholders})
            """,
            [now, *existing],
        )
        print(f"{cur.rowcount} 件のプレイヤーを監視対象に設定しました。")
    return 0


def _unmonitor_players(tags: Sequence[str]) -> int:
    normalized = _normalize_tags(tags)
    if not normalized:
        print("タグが指定されていません。", file=sys.stderr)
        return 1
    with get_connection() as conn:
        cur = conn.cursor()
        existing, missing = _split_existing_and_missing(cur, normalized)
        if missing:
            print("存在しないプレイヤータグ: " + ", ".join(missing), file=sys.stderr)
        if not existing:
            print("監視対象から解除できるプレイヤーがありませんでした。")
            return 1
        placeholders = ",".join(["%s"] * len(existing))
        cur.execute(
            f"""
            UPDATE players
            SET is_monitored = 0,
                monitoring_started_at = NULL
            WHERE tag IN ({placeholders})
            """,
            list(existing),
        )
        print(f"{cur.rowcount} 件のプレイヤーを監視対象から解除しました。")
    return 0


def _list_monitored_players() -> int:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT tag, name, monitoring_started_at
            FROM players
            WHERE is_monitored = 1
            ORDER BY monitoring_started_at IS NULL, monitoring_started_at ASC, tag ASC
            """
        )
        rows = cur.fetchall()
    if not rows:
        print("監視対象のプレイヤーはいません。")
        return 0
    print("監視対象プレイヤー一覧:")
    for tag, name, started_at in rows:
        name_display = name or "(未設定)"
        started_display = (
            started_at.strftime("%Y-%m-%d %H:%M:%S")
            if hasattr(started_at, "strftime") and started_at is not None
            else "-"
        )
        print(f"  {tag}: {name_display} (監視開始: {started_display})")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="プレイヤーの監視対象フラグを管理します。",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    monitor_parser = subparsers.add_parser(
        "monitor",
        help="指定したタグのプレイヤーを監視対象に設定します。",
    )
    monitor_parser.add_argument("tags", nargs="+", help="プレイヤータグ (例: #ABCDEFG)")

    unmonitor_parser = subparsers.add_parser(
        "unmonitor",
        help="指定したタグのプレイヤーを監視対象から外します。",
    )
    unmonitor_parser.add_argument("tags", nargs="+", help="プレイヤータグ (例: #ABCDEFG)")

    subparsers.add_parser(
        "list",
        help="監視対象のプレイヤーを一覧表示します。",
    )

<<<<<<< ours
=======
    subparsers.add_parser(
        "monitor-rank22",
        help="現在のランク22のプレイヤーをすべて監視対象に設定します。",
    )

>>>>>>> theirs
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "monitor":
        return _monitor_players(args.tags)
    if args.command == "unmonitor":
        return _unmonitor_players(args.tags)
<<<<<<< ours
=======
    if args.command == "monitor-rank22":
        return _monitor_rank22_players()
>>>>>>> theirs
    if args.command == "list":
        return _list_monitored_players()
    parser.error("不明なコマンドです。")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
