from src.config import load_config

def test_load_config():
    settings = load_config()

    assert settings.filter.market_cap_min == 1_000_000
    assert settings.filter.listed_exchanges > 1

    binance = next(e for e in settings.exchanges if e.name == "binance")
    assert binance is not None
    assert binance.name == "binance"
    assert binance.websocket == "wss://stream.binance.com:9443/ws"