import requests

from typing import List, Dict

from src.basics import matches_number
from src.config import BubblesFilter

from src.interests.bubble import Bubble
from src.interests.coingecko import usdt_exchanges

from src.exchanges.mexc.mexc import ALIAS as ALIAS_MEXC
from src.exchanges.bybit import ALIAS as ALIAS_BYBIT
from src.exchanges.binance import ALIAS as ALIAS_BINANCE
from src.exchanges.coinbase import ALIAS as ALIAS_COINBASE

__API_URL = "https://cryptobubbles.net/backend/data/bubbles1000.usd"
__RESPONSE_TIMEOUT = 10

__KEY_ID = "cg_id"
__KEY_SYMBOL = "symbol"
__KEY_MARKET_CAP = "marketcap"
__KEY_PRICES_EXC = "exchangePrices"
__KEY_PERFORMANCE = "performance"
__KEY_PERFORMANCE_DAY = "day"
__KEY_PERFORMANCE_HOUR_4 = "hour4"
__KEY_PERFORMANCE_HOUR_1 = "hour"
__KEY_PERFORMANCE_MINUTES_15 = "min15"

__supported_exchanges = [ALIAS_MEXC, ALIAS_BYBIT, ALIAS_BINANCE, ALIAS_COINBASE]

async def bubbles_fetch(filter: BubblesFilter) -> List[Bubble]:
    items = _fetch_bubbles()
    bubbles = _parse_bubbles(items)
    bubbles = _filter_bubbles_cap(filter=filter, bubbles=bubbles)
    bubbles = _filter_bubbles_perf(filter=filter, bubbles=bubbles)
    await usdt_exchanges(bubbles)
    return _filter_bubbles_exch(filter=filter, bubbles=bubbles)

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
        bubble = Bubble(
            id=item[__KEY_ID],
            symbol=item.get(__KEY_SYMBOL).lower(),
            market_cap=float(item.get(__KEY_MARKET_CAP, 0)),
            listed_exc=[e.lower() for e in list(item.get(__KEY_PRICES_EXC, {}).keys())],
            performance=item.get(__KEY_PERFORMANCE, {}),
        )
        bubbles.append(bubble)
    return bubbles

def _filter_bubbles_cap(filter: BubblesFilter, bubbles: List[Bubble]) -> List[Bubble]:
    filtered = []
    for b in bubbles:
        if b.id is None:
            continue
        if b.market_cap < filter.market_cap_min:
            continue
        if b.market_cap > filter.market_cap_max:
            continue
        filtered.append(b)
    if filtered:
        filtered.sort(key=lambda x: x.market_cap, reverse=True)
    return filtered

def _filter_bubbles_perf(filter: BubblesFilter, bubbles: List[Bubble]) -> List[Bubble]:
    filtered = []
    for b in bubbles:
        perf_day = b.performance.get(__KEY_PERFORMANCE_DAY, 0.0) or 0.0
        perf_hour_4 = b.performance.get(__KEY_PERFORMANCE_HOUR_4, 0.0) or 0.0
        perf_hour_1 = b.performance.get(__KEY_PERFORMANCE_HOUR_1, 0.0) or 0.0
        perf_minutes_15 = b.performance.get(__KEY_PERFORMANCE_MINUTES_15, 0.0) or 0.0
        performance_slice = [perf_day, perf_hour_4, perf_hour_1, perf_minutes_15]
        if not (all(v > 0 for v in performance_slice) or all(v < 0 for v in performance_slice)):
            continue
        if abs(perf_day) < filter.performance_per_day:
            continue
        if abs(perf_hour_4) < filter.performance_per_hour_4:
            continue
        if abs(perf_hour_1) < filter.performance_per_hour_1:
            continue
        if abs(perf_minutes_15) < filter.performance_per_minutes_15:
            continue
        filtered.append(b)
    if filtered:
        filtered.sort(key=lambda x: x.market_cap, reverse=True)
    return filtered

def _filter_bubbles_exch(filter: BubblesFilter, bubbles: List[Bubble]) -> List[Bubble]:
    filtered = []
    for b in bubbles:
        if matches_number(b.listed_exc, __supported_exchanges) < filter.listed_exchanges:
            continue
        filtered.append(b)
    if filtered:
        filtered.sort(key=lambda x: x.market_cap, reverse=True)
    return filtered