"""PostgreSQL の ``login_histories`` テーブルから ``tag`` カラムを取得するユーティリティ。

``.env.local`` に定義された ``DATABASE_URL`` を用いてデータベースへ接続し、
取得したタグを昇順で返す高水準 API を提供する。
"""

from __future__ import annotations

import os
from typing import List

import psycopg

from .settings import load_environment


def _get_database_url() -> str:
    """環境変数 ``DATABASE_URL`` を取得する。

    Returns:
        str: PostgreSQL への接続に利用する DSN。

    Raises:
        RuntimeError: ``DATABASE_URL`` が未設定の場合。
    """

    load_environment()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "環境変数 DATABASE_URL が設定されていません。.env.local を確認してください。"
        )
    return database_url


def fetch_login_history_tags() -> List[str]:
    """``login_histories`` テーブルの ``tag`` カラムを全件取得する。

    Returns:
        List[str]: ``tag`` カラムの値を昇順に並べたリスト。
    """

    database_url = _get_database_url()
    query = "SELECT tag FROM login_histories ORDER BY tag ASC"

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    return [row[0] for row in rows if row[0] is not None]


if __name__ == "__main__":  # pragma: no cover - CLI からの利用を想定
    for tag in fetch_login_history_tags():
        print(tag)
