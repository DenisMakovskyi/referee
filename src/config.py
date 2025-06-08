from __future__ import annotations

import yaml

from typing import List
from pathlib import Path
from dataclasses import dataclass

__CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"
__CONFIG_KEY_STOCK = "stock"

@dataclass
class Stock:
    name: str
    websocket: str

@dataclass
class Settings:
    stocks: List[Stock]

def load_config(path: Path = __CONFIG_PATH) -> Settings:
    with path.open(mode="r", encoding="utf-8") as stream:
        data = yaml.safe_load(stream)
    return Settings(stocks=[Stock(**s) for s in data.get(__CONFIG_KEY_STOCK, [])])