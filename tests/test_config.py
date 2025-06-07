from src.config import load_config

def test_load_config():
    settings = load_config()

    binance = next(s for s in settings.stocks if s.name == "binance")
    assert binance is not None
    assert binance.name == "binance"
    assert binance.websocket == "wss://stream.binance.com:9443/ws"

    assert "btc" in settings.observables
    assert "eth" in settings.observables