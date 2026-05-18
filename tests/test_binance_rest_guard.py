import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import binance_futures_momentum_long as m


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.text = str(self._payload)

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}
        self.calls = 0

    def get(self, *args, **kwargs):
        self.calls += 1
        return self.responses.pop(0)

    def post(self, *args, **kwargs):
        self.calls += 1
        return self.responses.pop(0)


def reset_guard():
    with m._BINANCE_REST_GUARD_LOCK:
        m._BINANCE_REST_GUARD_STATE.update({
            'window_started_at': 0.0,
            'request_count': 0,
            'circuit_open_until_ms': 0,
            'last_used_weight_1m': 0,
        })


def test_public_get_418_opens_rest_guard_and_stops_retrying():
    reset_guard()
    session = FakeSession([
        FakeResponse(418, {'code': -1003, 'msg': 'Way too many requests; IP banned until 9999999999999'}),
        FakeResponse(200, {'ok': True}),
    ])
    client = m.BinanceFuturesClient('https://example.test', session=session, max_get_retries=3, get_retry_sleep_sec=0.001)
    with pytest.raises(m.BinanceAPIError):
        client.get('/fapi/v1/ticker/24hr')
    assert session.calls == 1
    with pytest.raises(m.BinanceAPIError, match='circuit open'):
        client.get('/fapi/v1/ticker/24hr')
    assert session.calls == 1


def test_used_weight_header_blocks_scanner_rest_after_threshold():
    reset_guard()
    session = FakeSession([FakeResponse(200, {'ok': True}, {'X-MBX-USED-WEIGHT-1M': '1501'})])
    client = m.BinanceFuturesClient('https://example.test', session=session, max_get_retries=1)
    assert client.get('/fapi/v1/time') == {'ok': True}
    with pytest.raises(m.BinanceAPIError, match='circuit open'):
        client.get('/fapi/v1/time')


def test_rest_guard_caps_synchronous_client_to_five_requests_per_second():
    reset_guard()
    session = FakeSession([FakeResponse(200, {'ok': True}) for _ in range(6)])
    client = m.BinanceFuturesClient('https://example.test', session=session, max_get_retries=1)
    started = time.monotonic()
    for _ in range(6):
        client.get('/fapi/v1/time')
    assert time.monotonic() - started >= 0.8
