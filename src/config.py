from __future__ import annotations

import yaml
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"

@dataclass
class Exchange:
    name: str
    websocket_url: str

@dataclass
class Settings:
    exchanges: Dict[str, Exchange]
    stock: Dict[str, List[str]]


def load_config(path: Path = CONFIG_PATH) -> Settings:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    exchanges = {e["name"]: Exchange(**e) for e in data.get("exchanges", [])}
    stock = data.get("stock", {})
