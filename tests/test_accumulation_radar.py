import importlib.util
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
MODULE_PATH = SCRIPTS_DIR / 'accumulation_radar.py'


def load_module():
    sys.path.insert(0, str(SCRIPTS_DIR))
    try:
        spec = importlib.util.spec_from_file_location('accumulation_radar', MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        if sys.path and sys.path[0] == str(SCRIPTS_DIR):
            sys.path.pop(0)


def make_kline(price=10.0, quote_volume=1_000_000.0):
    return [0, str(price), str(price * 1.04), str(price * 0.96), str(price), '1000', 0, str(quote_volume)]


def test_analyze_accumulation_detects_low_volume_sideways_window():
    mod = load_module()
    klines = [make_kline(10 + (idx % 3) * 0.05, 750_000) for idx in range(90)]

    result = mod.analyze_accumulation('TESTUSDT', klines)

    assert result is not None
    assert result['symbol'] == 'TESTUSDT'
    assert result['sideways_days'] >= 45
    assert result['range_pct'] < 10
    assert result['status'] == 'accumulating'
    assert result['accumulation_score'] > 70


def test_build_external_rows_adds_oi_and_funding_reasons():
    mod = load_module()
    rows = mod.build_external_rows(
        [{
            'symbol': 'TESTUSDT',
            'sideways_days': 75,
            'range_pct': 18.2,
            'avg_vol': 900_000,
            'status': 'accumulating',
            'accumulation_score': 78.0,
        }],
        oi_map={'TESTUSDT': {'oi_6h_pct': 6.0, 'oi_usd': 5_000_000}},
        ticker_map={'TESTUSDT': {'price_change_pct': 1.2}},
        funding_map={'TESTUSDT': -0.0004},
    )

    assert rows[0]['symbol'] == 'TESTUSDT'
    assert rows[0]['external_signal_tier'] == 'critical'
    assert rows[0]['external_signal_score'] > 90
    assert 'dark_flow_oi_up_price_flat' in rows[0]['external_reasons']
    assert rows[0]['portfolio_narrative_bucket'] == 'accumulation'


def test_scan_external_signals_uses_existing_writer_payload_shape():
    mod = load_module()

    class StubClient:
        def get(self, path, params=None, timeout=15):
            if path == '/fapi/v1/exchangeInfo':
                return {
                    'symbols': [{
                        'symbol': 'TESTUSDT',
                        'quoteAsset': 'USDT',
                        'contractType': 'PERPETUAL',
                        'status': 'TRADING',
                    }]
                }
            if path == '/fapi/v1/klines':
                return [make_kline(10 + (idx % 2) * 0.02, 700_000) for idx in range(90)]
            if path == '/fapi/v1/ticker/24hr':
                return [{'symbol': 'TESTUSDT', 'priceChangePercent': '1.0', 'quoteVolume': '5000000', 'lastPrice': '10'}]
            if path == '/fapi/v1/premiumIndex':
                return [{'symbol': 'TESTUSDT', 'lastFundingRate': '-0.0003'}]
            if path == '/futures/data/openInterestHist':
                return [
                    {'sumOpenInterestValue': '4000000'},
                    {'sumOpenInterestValue': '4100000'},
                    {'sumOpenInterestValue': '4200000'},
                    {'sumOpenInterestValue': '4300000'},
                    {'sumOpenInterestValue': '4400000'},
                    {'sumOpenInterestValue': '4500000'},
                ]
            raise AssertionError(path)

    payload = mod.scan_external_signals(StubClient(), top=5)

    assert payload['engine'] == 'accumulation_radar'
    assert payload['symbols'] == ['TESTUSDT']
    assert payload['signal_map']['TESTUSDT']['external_signal_score'] > 0
    assert payload['signal_map']['TESTUSDT']['portfolio_narrative_bucket'] == 'accumulation'


def test_public_client_retries_transient_timeout(monkeypatch):
    mod = load_module()
    monkeypatch.setattr(mod.time, 'sleep', lambda _seconds: None)

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {'ok': True}

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, url, params=None, timeout=15):
            self.calls += 1
            if self.calls == 1:
                raise mod.requests.ReadTimeout('slow')
            return Response()

    session = Session()
    client = mod.BinancePublicClient('https://example.test', session=session, max_get_retries=2)

    assert client.get('/fapi/v1/ticker/24hr') == {'ok': True}
    assert session.calls == 2
