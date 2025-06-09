from __future__ import annotations

import yaml

from typing import List
from pathlib import Path
from dataclasses import dataclass

__CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"
__CONFIG_KEY_EXCHANGES = "exchanges"
__CONFIG_KEY_BUBBLE_FILTER = "bubbles_filter"

@dataclass
class Settings:
    filter: BubblesFilter
    exchanges: List[Exchange]

@dataclass
class Exchange:
    name: str
    websocket: str

@dataclass
class BubblesFilter:
    market_cap_min: float
    market_cap_max: float
    listed_exchanges: int
    performance_per_day: float

def load_config(path: Path = __CONFIG_PATH) -> Settings:
    with path.open(mode="r", encoding="utf-8") as stream:
        data = yaml.safe_load(stream)
    return Settings(
        filter=BubblesFilter(**data.get(__CONFIG_KEY_BUBBLE_FILTER, {})),
        exchanges=[Exchange(**s) for s in data.get(__CONFIG_KEY_EXCHANGES, [])],
    )