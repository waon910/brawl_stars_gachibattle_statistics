"""アプリケーション共通の設定値と環境変数の読み込みを管理するモジュール。"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_ENV_FILE = BASE_DIR / "config" / "settings.env"
LOCAL_ENV_FILE = BASE_DIR / ".env.local"
_DEFAULT_RETENTION_DAYS = 30
_DEFAULT_MIN_RANK_ID = 4


@lru_cache(maxsize=1)
def load_environment() -> None:
    """設定ファイルとローカル環境変数ファイルを読み込む。"""
    load_dotenv(DEFAULT_ENV_FILE)
    load_dotenv(LOCAL_ENV_FILE, override=True)


def _get_int_env(name: str, default: int) -> int:
    """環境変数を整数として取得する。"""
    load_environment()
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except ValueError as exc:  # pragma: no cover - エラーハンドリングのための保険
        raise ValueError(
            f"環境変数 {name} は整数値である必要があります (現在の値: {value})"
        ) from exc


DATA_RETENTION_DAYS = _get_int_env("DATA_RETENTION_DAYS", _DEFAULT_RETENTION_DAYS)
MIN_RANK_ID = _get_int_env("MIN_RANK_ID", _DEFAULT_MIN_RANK_ID)
