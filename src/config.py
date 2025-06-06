from __future__ import annotations

import yaml

from typing import Dict, List
from pathlib import Path
from dataclasses import dataclass

__CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"
__CONFIG_KEY_STOCK = "stock"
__CONFIG_KEY_COINS = "coins"
__CONFIG_KEY_STOCK_NAME = "name"

@dataclass
class Stock:
    name: str
    websocket: str

@dataclass
class Settings:
    stocks: Dict[str, Stock]
    observables: Dict[str, List[str]]

def load_config(path: Path = __CONFIG_PATH) -> Settings:
    with path.open(mode="r", encoding="utf-8") as stream:
        data = yaml.safe_load(stream)

    stocks_raw = data.get(__CONFIG_KEY_STOCK, [])
    stocks_dict = {
        item[__CONFIG_KEY_STOCK_NAME]: Stock(**item)
        for item in stocks_raw
    }
    observables_dict = data.get(__CONFIG_KEY_COINS, {})
    return Settings(
        stocks=stocks_dict,
        observables=observables_dict,
    )
