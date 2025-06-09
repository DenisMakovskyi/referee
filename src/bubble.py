import requests

from typing import List, Dict
from dataclasses import dataclass

from src.config import BubblesFilter
from src.exchanges.bybit import ALIAS as ALIAS_BYBIT
from src.exchanges.binance import ALIAS as ALIAS_BINANCE
from src.exchanges.coinbase import ALIAS as ALIAS_COINBASE

__API_URL = "https://cryptobubbles.net/backend/data/bubbles1000.usd"
__RESPONSE_TIMEOUT = 10

__KEY_SYMBOL = "symbol"
__KEY_MARKET_CAP = "marketcap"
__KEY_PRICES_EXC = "exchangePrices"
__KEY_PERFORMANCE = "performance"
__KEY_PERFORMANCE_DAY = "day"

__supported_exchanges = [ALIAS_BYBIT, ALIAS_BINANCE, ALIAS_COINBASE]

@dataclass
class Bubble:
    symbol: str
    market_cap: float
    listed_exc: List[str]
    performance: Dict[str, float]

def bubbles(filter: BubblesFilter) -> List[Bubble]:
    raw = _fetch_bubbles()
    bubbles = _parse_bubbles(raw)
    return _filter_bubbles(filter=filter, bubbles=bubbles)

def bubbles_filter(bubbles: List[Bubble], exchange: str) -> List[Bubble]:
    return [b for b in bubbles if exchange in b.listed_exc]

def _fetch_bubbles() -> List[Dict]:
    try:
        response = requests.get(url=__API_URL, timeout=__RESPONSE_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError):
        return []

def _parse_bubbles(items: List[Dict]) -> List[Bubble]:
    bubbles = []
    for item in items:
        symbol = item.get(__KEY_SYMBOL).lower()
        market_cap = float(item.get(__KEY_MARKET_CAP, 0))
        listed_exc = [e.lower() for e in list(item.get(__KEY_PRICES_EXC, {}).keys())]
        performance = item.get(__KEY_PERFORMANCE, {})
        bubble = Bubble(
            symbol=symbol,
            market_cap=market_cap,
            listed_exc=listed_exc,
            performance=performance,
        )
        bubbles.append(bubble)
    return bubbles

def _filter_bubbles(filter: BubblesFilter, bubbles: List[Bubble]) -> List[Bubble]:
    filtered = []
    for b in bubbles:
        if b.market_cap < filter.market_cap_min:
            continue
        if b.market_cap > filter.market_cap_max:
            continue
        if len(set(b.listed_exc) & set(__supported_exchanges)) < filter.listed_exchanges:
            continue
        if abs(b.performance.get(__KEY_PERFORMANCE_DAY, 0.0)) < filter.performance_per_day:
            continue
        filtered.append(b)
    filtered.sort(key=lambda x: x.market_cap, reverse=True)
    return filtered