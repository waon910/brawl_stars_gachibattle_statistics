"""PostgreSQL の ``login_histories`` テーブルから ``tag`` カラムを取得するユーティリティ。

``.env.local`` に定義された ``DATABASE_URL`` を用いてデータベースへ接続し、
取得したタグを昇順で返す高水準 API を提供する。
"""

from __future__ import annotations

import logging
import os
from typing import Any, List

try:  # pragma: no cover - 環境依存のためテストが困難
    import psycopg  # type: ignore[import]
except Exception as exc:  # pragma: no cover - psycopg の import 失敗時
    psycopg = None  # type: ignore[assignment]
    _PSYCOPG_IMPORT_ERROR = exc
else:  # pragma: no cover - import 成功時
    _PSYCOPG_IMPORT_ERROR = None

try:
    # Allow using the module as part of a package (relative import) and as a standalone script (absolute import).
    from .settings import load_environment
except ImportError:
    from settings import load_environment


logger = logging.getLogger(__name__)


def _describe_psycopg_import_issue(exc: BaseException | None) -> str:
    """psycopg の import 失敗理由を利用者向けに整形する。"""

    guidance = [
        "psycopg が利用できません。libpq をシステムにインストールするか、",
        "pip install \"psycopg[binary]\" を実行してバイナリディストリビューションを導入してください。",
    ]

    if exc is None:
        return "".join(guidance)

    message = str(exc)
    lowered = message.lower()

    if "libpq" in lowered or "pq wrapper" in lowered:
        guidance.insert(
            0,
            "libpq の共有ライブラリが見つからないため psycopg の初期化に失敗しました。",
        )
    else:
        guidance.insert(
            0,
            "psycopg の読み込み時に予期しない例外が発生しました。",
        )

    guidance.append(f" 原因となった例外メッセージ: {message}")
    return "".join(guidance)


def _ensure_psycopg_available() -> Any:
    """psycopg の import 状態を検証し、利用できない場合は詳細なメッセージ付きで失敗させる。"""

    if psycopg is None:
        if _PSYCOPG_IMPORT_ERROR is not None:
            logger.debug(
                "psycopg の import 時に例外が発生しました", exc_info=_PSYCOPG_IMPORT_ERROR
            )
        message = _describe_psycopg_import_issue(_PSYCOPG_IMPORT_ERROR)
        logger.debug(
            "psycopg の import 失敗時環境: LD_LIBRARY_PATH=%s, DYLD_LIBRARY_PATH=%s",
            os.getenv("LD_LIBRARY_PATH"),
            os.getenv("DYLD_LIBRARY_PATH"),
        )
        raise RuntimeError(message) from _PSYCOPG_IMPORT_ERROR
    return psycopg


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

    psycopg_module = _ensure_psycopg_available()

    try:
        with psycopg_module.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
    except Exception as exc:
        raise RuntimeError("PostgreSQL から login_histories テーブルの取得に失敗しました。") from exc

    return [row[0] for row in rows if row[0] is not None]


if __name__ == "__main__":  # pragma: no cover - CLI からの利用を想定
    for tag in fetch_login_history_tags():
        print(tag)
