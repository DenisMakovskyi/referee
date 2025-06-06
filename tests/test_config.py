from src.config import load_config

def test_load_config():
    settings = load_config()

    assert 'binance' in settings.stocks

    binance_stock = settings.stocks['binance']
    assert binance_stock.name == 'binance'
    assert binance_stock.websocket == 'wss://stream.binance.com:9443/ws'

    assert 'binance' in settings.observables
    assert settings.observables['binance'] == ['btc', 'etc']