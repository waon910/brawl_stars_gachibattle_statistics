"""メモリ使用量の取得・ログ出力ユーティリティ."""

from __future__ import annotations

import logging
import sys
from typing import Final

try:
    import resource
except ImportError:  # pragma: no cover - Windows等では利用不可
    resource = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

__all__: Final = ("get_memory_usage_bytes", "format_memory_usage", "log_memory_usage")


def get_memory_usage_bytes() -> int:
    """現在のプロセスの最大常駐メモリ使用量（バイト）を取得する."""

    if resource is None:
        return -1
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return int(usage)
    return int(usage) * 1024


def format_memory_usage() -> str:
    """メモリ使用量を可読形式に整形する."""

    bytes_used = get_memory_usage_bytes()
    if bytes_used < 0:
        return "N/A"

    units = ("B", "KB", "MB", "GB", "TB")
    value = float(bytes_used)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.2f}{unit}"
        value /= 1024
    return f"{value:.2f}TB"


def log_memory_usage(context: str) -> None:
    """現在のメモリ使用量をINFOログに出力する."""

    logger.info("[%s] 現在の最大常駐メモリ: %s", context, format_memory_usage())
