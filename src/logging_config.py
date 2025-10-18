import logging
import logging.config
from pathlib import Path
from threading import Lock

import yaml

_CONFIGURED = False
_LOCK = Lock()


def setup_logging(config_path: Path | None = None, *, force: bool = False) -> None:
    """YAMLファイルからロギング設定を読み込み、必要に応じて初期化する."""

    global _CONFIGURED

    if not force and _CONFIGURED:
        return

    with _LOCK:
        if not force and _CONFIGURED:
            return

        if config_path is None:
            config_path = (
                Path(__file__).resolve().parent.parent / "config" / "logging.yaml"
            )

        with config_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        config.setdefault("disable_existing_loggers", False)
        logging.config.dictConfig(config)
        _CONFIGURED = True
