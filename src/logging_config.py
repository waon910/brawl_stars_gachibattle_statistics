import logging.config
from pathlib import Path

import yaml


def setup_logging(config_path: Path | None = None) -> None:
    """YAMLファイルからロギング設定を読み込む"""
    if config_path is None:
        config_path = Path(__file__).resolve().parent.parent / "config" / "logging.yaml"
    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    logging.config.dictConfig(config)
