import requests

from typing import List, Dict
from dataclasses import dataclass

from src.stocks.bybit import ALIAS as ALIAS_BYBIT
from src.stocks.binance import ALIAS as ALIAS_BINANCE
from src.stocks.coinbase import ALIAS as ALIAS_COINBASE

__API_URL = "https://cryptobubbles.net/backend/data/bubbles1000.usd"

__MARKET_CAP_MIN = 1_000_000
__MARKET_CAP_MAX = 23_000_000_000
__LISTED_STOCKS_COUNT = 3
__PERFORMANCE_DAY_CHANGE = 5.0

__KEY_SYMBOL = "symbol"
__KEY_MARKET_CAP = "marketcap"
__KEY_PRICES_EXC = "exchangePrices"
__KEY_PERFORMANCE = "performance"
__KEY_PERFORMANCE_DAY = "day"

__supported_stocks = [ALIAS_BYBIT, ALIAS_BINANCE, ALIAS_COINBASE]

@dataclass
class Bubble:
    symbol: str
    market_cap: float
    listed_exc: List[str]
    performance: Dict[str, float]

def _fetch_bubbles() -> List[Dict]:
    try:
        response = requests.get(url=__API_URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError):
        return []

def _parse_bubbles(items: List[Dict]) -> List[Bubble]:
    bubbles = []
    for item in items:
        symbol = item.get(__KEY_SYMBOL).lower()
        market_cap = float(item.get(__KEY_MARKET_CAP, 0))
        listed_exc = list(item.get(__KEY_PRICES_EXC, {}).keys())
        listed_exc = [e.lower() for e in listed_exc]
        performance = item.get(__KEY_PERFORMANCE, {})
        bubble = Bubble(
            symbol=symbol,
            market_cap=market_cap,
            listed_exc=listed_exc,
            performance=performance,
        )
        bubbles.append(bubble)
    return bubbles

def _filter_bubbles(bubbles: List[Bubble]) -> List[Bubble]:
    filtered = []
    for b in bubbles:
        if b.market_cap < __MARKET_CAP_MIN:
            continue
        if b.market_cap > __MARKET_CAP_MAX:
            continue
        if len(set(b.listed_exc) & set(__supported_stocks)) < __LISTED_STOCKS_COUNT:
            continue
        if abs(b.performance.get(__KEY_PERFORMANCE_DAY, 0.0)) < __PERFORMANCE_DAY_CHANGE:
            continue
        filtered.append(b)
    filtered.sort(key=lambda x: x.market_cap, reverse=True)
    return filtered

def bubbles_watchlist() -> List[Bubble]:
    raw = _fetch_bubbles()
    bubbles = _parse_bubbles(raw)
    return _filter_bubbles(bubbles)
