import asyncio

from typing import List, Dict
from aiohttp import ClientSession, ClientTimeout, ClientError

from src.interests.bubble import Bubble

from src.exchanges.mexc.mexc import ALIAS as ALIAS_MEXC
from src.exchanges.bybit import ALIAS as ALIAS_BYBIT
from src.exchanges.binance import ALIAS as ALIAS_BINANCE
from src.exchanges.coinbase import ALIAS as ALIAS_COINBASE

__API_URL = "https://api.coingecko.com/api/v3/coins/{coin_id}/tickers"

__RESPONSE_TIMEOUT = 10
__BUBBLES_BATCH_SIZE = 1
__BUBBLES_BATCH_DELAY_SECONDS = 10.0

__USDT_ID = "tether"

__KEY_ITEMS = "tickers"
__KEY_MARKET = "market"
__KEY_MARKET_ID = "identifier"
__KEY_TARGET_ID = "target_coin_id"

__id_to_alias = {
    "mxc": ALIAS_MEXC,
    "bybit_spot": ALIAS_BYBIT,
    "binance": ALIAS_BINANCE,
    "gdax": ALIAS_COINBASE,
}

async def usdt_exchanges(bubbles: List[Bubble]):
    timeout = ClientTimeout(total=__RESPONSE_TIMEOUT)
    async with ClientSession(timeout=timeout) as session:
        for i in range(0, len(bubbles), __BUBBLES_BATCH_SIZE):
            batch = bubbles[i:i + __BUBBLES_BATCH_SIZE]
            tasks = [__usdt_exchanges(b.id, session) for b in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for bubble, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    bubble.listed_exc = [] or bubble.listed_exc
                else:
                    bubble.listed_exc = result or bubble.listed_exc

            await asyncio.sleep(__BUBBLES_BATCH_DELAY_SECONDS)

async def __usdt_exchanges(coin_id: str, session: ClientSession) -> List[str]:
    data = await __fetch_coingecko(coin_id=coin_id, session=session)
    return __filter_usdt_exchanges(data.get(__KEY_ITEMS, []))

async def __fetch_coingecko(coin_id: str, session: ClientSession) -> Dict:
    try:
        async with session.get(__API_URL.format(coin_id=coin_id)) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(coin_id, response)
                return {}
    except ClientError as e:
        print(coin_id, e)
        return {}

def __filter_usdt_exchanges(items: List[Dict]) -> List[str]:
    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get(__KEY_TARGET_ID) == __USDT_ID:
            market = item.get(__KEY_MARKET)
            if isinstance(market, dict):
                market_id = market.get(__KEY_MARKET_ID)
                if market_id:
                    result.append(
                        __id_to_alias.get(market_id, market_id) or market_id,
                    )
    return result