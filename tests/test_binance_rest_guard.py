import argparse
import json
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
        self.last_kwargs = None
        self.last_url = None

    def get(self, url, **kwargs):
        self.calls += 1
        self.last_url = url
        self.last_kwargs = kwargs
        return self.responses.pop(0)

    def post(self, url, **kwargs):
        self.calls += 1
        self.last_url = url
        self.last_kwargs = kwargs
        return self.responses.pop(0)


@pytest.fixture(autouse=True)
def isolated_guard(tmp_path):
    old_path = m._BINANCE_REST_GUARD_STATE_PATH
    m.configure_binance_rest_guard_store(tmp_path)
    m._BINANCE_REST_WEIGHT_BY_PURPOSE.clear()
    with m._BINANCE_REST_GUARD_LOCK:
        m._BINANCE_REST_GUARD_STATE.clear()
        m._BINANCE_REST_GUARD_STATE.update(m._normalize_rest_guard_state({}))
    yield tmp_path
    m.configure_binance_rest_guard_store(old_path.parent)


def test_public_get_418_opens_rest_guard_and_stops_retrying():
    session = FakeSession([
        FakeResponse(418, {'code': -1003, 'msg': 'Way too many requests; IP banned until 9999999999999'}),
        FakeResponse(200, {'ok': True}),
    ])
    client = m.BinanceFuturesClient('https://example.test', session=session, max_get_retries=3, get_retry_sleep_sec=0.001)
    with pytest.raises(m.BinanceAPIError):
        client.get('/fapi/v1/ticker/24hr')
    assert session.calls == 1
    with pytest.raises(m.BinanceAPIError, match='binance_rest_circuit_open'):
        client.get('/fapi/v1/ticker/24hr')
    assert session.calls == 1


def test_used_weight_header_blocks_scanner_rest_after_threshold():
    session = FakeSession([FakeResponse(200, {'ok': True}, {'X-MBX-USED-WEIGHT-1M': '1501'})])
    client = m.BinanceFuturesClient('https://example.test', session=session, max_get_retries=1)
    assert client.get('/fapi/v1/time') == {'ok': True}
    with pytest.raises(m.BinanceAPIError, match='scanner_degraded_wait=true'):
        client.get('/fapi/v1/time')


def test_rest_guard_caps_synchronous_client_to_two_requests_per_second():
    session = FakeSession([FakeResponse(200, {'ok': True}) for _ in range(3)])
    client = m.BinanceFuturesClient('https://example.test', session=session, max_get_retries=1)
    started = time.monotonic()
    for _ in range(3):
        client.get('/fapi/v1/time')
    assert time.monotonic() - started >= 0.8


def test_rest_guard_state_is_persisted_across_process_clients(isolated_guard):
    client = m.BinanceFuturesClient('https://example.test', session=FakeSession([FakeResponse(200, {'ok': True}, {'X-MBX-USED-WEIGHT-1M': '1501'})]), max_get_retries=1)
    assert client.get('/fapi/v1/time') == {'ok': True}
    state = json.loads((isolated_guard / 'binance_rest_guard.json').read_text())
    assert state['rest_circuit_state'] == 'DEGRADED'
    with m._BINANCE_REST_GUARD_LOCK:
        m._BINANCE_REST_GUARD_STATE.clear()
        m._BINANCE_REST_GUARD_STATE.update(m._normalize_rest_guard_state({}))
    client2 = m.BinanceFuturesClient('https://example.test', session=FakeSession([FakeResponse(200, {'ok': True})]), max_get_retries=1)
    with pytest.raises(m.BinanceAPIError, match='scanner_degraded_wait=true'):
        client2.get('/fapi/v1/time')


def test_recovering_guard_does_not_extend_recovery_window_on_each_healthy_response(monkeypatch):
    now_ms = 1_000_000
    monkeypatch.setattr(m, '_rest_now_ms', lambda: now_ms)
    m._set_binance_rest_circuit_state('RECOVERING', reason='rest_weight_recovered')
    first_until = m._binance_rest_guard_snapshot()['recovering_until_ms']

    monkeypatch.setattr(m, '_rest_now_ms', lambda: now_ms + 60_000)
    response = FakeResponse(200, {'ok': True}, {'X-MBX-USED-WEIGHT-1M': '5'})
    m._binance_rest_guard_after_response(response, purpose='scanner', path='/fapi/v1/time')
    assert m._binance_rest_guard_snapshot()['recovering_until_ms'] == first_until

    monkeypatch.setattr(m, '_rest_now_ms', lambda: first_until + 1)
    snapshot = m._binance_rest_guard_snapshot()
    assert snapshot['rest_circuit_state'] == 'CLOSED'
    assert snapshot['recovering_until_ms'] == 0


def test_signed_get_classifies_history_backfill_and_allows_execution_under_core_only_weight():
    session = FakeSession([FakeResponse(200, {'orders': []}, {'X-MBX-USED-WEIGHT-1M': '1801'})])
    client = m.BinanceFuturesClient('https://example.test', api_secret='secret', session=session, max_get_retries=1, allow_heavy_history_rest=True)
    client._server_time_offset_ms = 0
    assert client.signed_get('/fapi/v1/allOrders', {'symbol': 'BTCUSDT'}) == {'orders': []}
    assert m._BINANCE_REST_WEIGHT_BY_PURPOSE['watchdog'] == 1801
    exec_session = FakeSession([FakeResponse(200, {'orderId': 1})])
    exec_client = m.BinanceFuturesClient('https://example.test', api_secret='secret', session=exec_session, max_get_retries=1)
    exec_client._server_time_offset_ms = 0
    assert exec_client.signed_post('/fapi/v1/order', {'symbol': 'BTCUSDT'}) == {'orderId': 1}


def test_scanner_proxy_only_applies_to_public_gets():
    session = FakeSession([FakeResponse(200, {'ok': True}), FakeResponse(200, {'orderId': 1})])
    client = m.BinanceFuturesClient('https://example.test', api_secret='secret', session=session, max_get_retries=1, scanner_proxy_urls=['127.0.0.1:1080'])
    client._server_time_offset_ms = 0
    assert client.get('/fapi/v1/time') == {'ok': True}
    assert 'proxies' in session.last_kwargs
    assert client.signed_post('/fapi/v1/order', {'symbol': 'BTCUSDT'}) == {'orderId': 1}
    assert 'proxies' not in session.last_kwargs


def test_binance_ban_until_parser_accepts_cst_date():
    assert m.extract_binance_ip_ban_until_ms('Way too many requests; IP banned until 2026-05-18 18:30:00 CST.') == 1779100200000


def test_public_get_accepts_purpose_for_guard_and_log_bucket():
    session = FakeSession([FakeResponse(200, {'ok': True}, {'X-MBX-USED-WEIGHT-1M': '123'})])
    client = m.BinanceFuturesClient('https://example.test', session=session, max_get_retries=1)
    assert client.get('/fapi/v1/exchangeInfo', purpose='metadata') == {'ok': True}
    assert m._BINANCE_REST_WEIGHT_BY_PURPOSE['metadata'] == 123


def test_sync_server_time_uses_guarded_metadata_get():
    session = FakeSession([FakeResponse(200, {'serverTime': int(time.time() * 1000)}, {'X-MBX-USED-WEIGHT-1M': '7'})])
    client = m.BinanceFuturesClient('https://example.test', session=session, max_get_retries=1)
    client.sync_server_time(force=True)
    assert session.calls == 1
    assert session.last_url.endswith('/fapi/v1/time')
    assert m._BINANCE_REST_WEIGHT_BY_PURPOSE['metadata'] == 7


def test_signed_open_orders_is_order_status_not_history_backfill():
    session = FakeSession([FakeResponse(200, [], {'X-MBX-USED-WEIGHT-1M': '88'})])
    client = m.BinanceFuturesClient('https://example.test', api_secret='secret', session=session, max_get_retries=1)
    client._server_time_offset_ms = 0
    assert client.signed_get('/fapi/v1/openOrders', {'symbol': 'BTCUSDT'}) == []
    assert m._BINANCE_REST_WEIGHT_BY_PURPOSE['reconcile'] == 88
    assert 'watchdog' not in m._BINANCE_REST_WEIGHT_BY_PURPOSE


@pytest.mark.parametrize('path', ['/fapi/v1/allOrders', '/fapi/v1/userTrades', '/fapi/v1/income'])
def test_heavy_history_signed_get_disabled_by_default(path):
    session = FakeSession([FakeResponse(200, [])])
    client = m.BinanceFuturesClient('https://example.test', api_secret='secret', session=session, max_get_retries=1)
    client._server_time_offset_ms = 0
    with pytest.raises(m.BinanceAPIError, match='history_backfill_rest_disabled'):
        client.signed_get(path, {'symbol': 'BTCUSDT'})
    assert session.calls == 0


class GuardStore:
    def __init__(self, guard=None, cursor=None):
        self.guard = guard or {'rest_circuit_state': 'CLOSED', 'rest_used_weight_1m': 0}
        self.cursor = cursor or {}
        self.saved = []
    def load_json(self, name, default=None):
        if name == 'binance_rest_guard':
            return self.guard
        if name == 'scanner_rest_fallback_cursor':
            return self.cursor
        if name in {'kline_cache', 'order_book_cache'}:
            return {}
        return default
    def save_json(self, name, payload):
        self.saved.append((name, payload))
        if name == 'scanner_rest_fallback_cursor':
            self.cursor = payload
        return payload


class CountingClient:
    def __init__(self):
        self.calls = []
    def get(self, path, params=None, timeout=15, *, purpose='market_data'):
        self.calls.append((path, params, purpose))
        if path.endswith('/exchangeInfo'):
            return {'symbols': [{'symbol': 'BTCUSDT', 'quoteAsset': 'USDT', 'status': 'TRADING', 'contractType': 'PERPETUAL', 'filters': []}]}
        if path.endswith('/klines'):
            return [[1,2,3]]
        if path.endswith('/depth'):
            return {'bids': [], 'asks': []}
        return {}


def test_kline_cache_missing_without_explicit_fallback_does_not_rest():
    args = argparse.Namespace(scanner_rest_fallback=True, scanner_kline_rest_fallback=False)
    client = CountingClient()
    rows, diag = m.resolve_scan_klines(client, GuardStore(), args, 'BTCUSDT', '5m', 10, return_diagnostics=True)
    assert rows == []
    assert client.calls == []
    assert diag['scanner_kline_rest_fallback_skipped_reason'] == 'scanner_kline_rest_fallback_disabled'


def test_depth_cache_missing_without_explicit_fallback_does_not_rest():
    args = argparse.Namespace(scanner_rest_fallback=True, scanner_depth_rest_fallback=False)
    client = CountingClient()
    book, diag = m.resolve_scan_order_book(client, GuardStore(), args, 'BTCUSDT', 100, return_diagnostics=True)
    assert book == {}
    assert client.calls == []
    assert diag['scanner_depth_rest_fallback_skipped_reason'] == 'scanner_depth_rest_fallback_disabled'


def test_market_data_rest_weight_blocks_kline_and_depth_fallback():
    store = GuardStore({'rest_circuit_state': 'CLOSED', 'rest_used_weight_1m': 700})
    args = argparse.Namespace(scanner_rest_fallback=True, scanner_kline_rest_fallback=True, scanner_depth_rest_fallback=True, scanner_market_data_rest_fallback_max_used_weight_1m=700)
    client = CountingClient()
    rows, kdiag = m.resolve_scan_klines(client, store, args, 'BTCUSDT', '5m', 10, return_diagnostics=True)
    book, ddiag = m.resolve_scan_order_book(client, store, args, 'BTCUSDT', 20, return_diagnostics=True)
    assert rows == [] and book == {}
    assert client.calls == []
    assert kdiag['scanner_kline_rest_fallback_skipped_reason'] == 'rest_used_weight_1m_exceeds_limit'
    assert ddiag['scanner_depth_rest_fallback_skipped_reason'] == 'rest_used_weight_1m_exceeds_limit'


def test_fetch_public_exchange_symbol_set_uses_guarded_client(monkeypatch):
    m._EXCHANGE_SYMBOL_SET_CACHE.update({'symbols': set(), 'updated_at_ms': 0})
    client = CountingClient()
    symbols = m.fetch_public_exchange_symbol_set(client=client, ttl_seconds=1)
    assert symbols == {'BTCUSDT'}
    assert client.calls == [('/fapi/v1/exchangeInfo', None, 'metadata')]


def test_scanner_ticker_rest_fallback_decision_always_reads_store_guard():
    store = GuardStore({'rest_circuit_state': 'CLOSED', 'rest_used_weight_1m': 901})
    args = argparse.Namespace(scanner_rest_fallback=True, scanner_market_data_rest_fallback_max_used_weight_1m=700)
    decision = m.scanner_ticker_rest_fallback_decision(store, args, {'fresh': False, 'reason': 'cache_empty'})
    assert decision['allowed'] is False
    assert decision['reason'] == 'rest_used_weight_1m_exceeds_limit'
