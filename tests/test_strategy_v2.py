import argparse
import importlib.util
import json
import pathlib
import statistics
import sys
import tempfile

MODULE_PATH = pathlib.Path(__file__).resolve().parents[1] / 'scripts' / 'binance_futures_momentum_long.py'
spec = importlib.util.spec_from_file_location('strategy_mod', MODULE_PATH)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def make_kline(open_price, high_price, low_price, close_price, volume=1000, quote_volume=None):
    quote_volume = quote_volume if quote_volume is not None else volume * close_price
    return [0, str(open_price), str(high_price), str(low_price), str(close_price), str(volume), 0, str(quote_volume), 0, 0, 0, 0]


def make_meta(symbol='TESTUSDT'):
    return mod.SymbolMeta(
        symbol=symbol,
        price_precision=2,
        quantity_precision=1,
        tick_size=0.01,
        step_size=0.1,
        min_qty=0.1,
        quote_asset='USDT',
        status='TRADING',
        contract_type='PERPETUAL',
    )


def test_compute_expected_slippage_r_and_execution_grade():
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        liquidity_grade='A',
        expected_slippage_pct=0.12,
        book_depth_fill_ratio=0.82,
    )

    alert = mod.build_standardized_alert(candidate, {'label': 'neutral', 'score_multiplier': 1.0, 'reasons': []})

    assert alert['expected_slippage_r'] == 0.06
    assert alert['execution_liquidity_grade'] == 'A'


def test_build_standardized_alert_includes_side_risk_multiplier_in_position_sizing():
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=88.0,
        reasons=['seed'],
        state='launch',
        liquidity_grade='A',
        expected_slippage_pct=0.12,
        book_depth_fill_ratio=0.82,
        alert_tier='high',
        regime_label='risk_on',
        regime_multiplier=0.8,
        side='LONG',
        position_size_pct=2.76,
        side_risk_multiplier=1.15,
    )

    alert = mod.build_standardized_alert(candidate, {'label': 'risk_on', 'score_multiplier': 0.8, 'reasons': []})

    assert alert['side_risk_multiplier'] == 1.15
    assert alert['market_regime_multiplier'] == 0.8
    assert alert['base_position_size_pct'] == 2.76
    assert alert['position_size_pct'] == 2.76


def test_derive_side_risk_multiplier_respects_regime_bias():
    assert mod.derive_side_risk_multiplier('LONG', 'risk_on') == 1.15
    assert mod.derive_side_risk_multiplier('SHORT', 'risk_on') == 0.85
    assert mod.derive_side_risk_multiplier('LONG', 'risk_off') == 0.85
    assert mod.derive_side_risk_multiplier('SHORT', 'risk_off') == 1.15
    assert mod.derive_side_risk_multiplier('LONG', 'caution') == 0.9
    assert mod.derive_side_risk_multiplier('SHORT', 'neutral') == 1.0


def test_recommended_position_size_pct_multiplies_regime_and_side_bias():
    assert mod.recommended_position_size_pct('high', regime_multiplier=0.8, side_multiplier=1.15) == 2.76
    assert mod.recommended_position_size_pct(91.0, 'critical', regime_multiplier=0.9, side_multiplier=0.9) == 2.43


def test_fetch_order_book_hits_binance_depth_endpoint_with_limit():
    calls = []

    class StubClient:
        def get(self, path, params=None, timeout=15):
            calls.append({'path': path, 'params': params, 'timeout': timeout})
            return {'lastUpdateId': 7, 'bids': [['100.0', '12']], 'asks': [['100.1', '9']]}

    payload = mod.fetch_order_book(StubClient(), 'TESTUSDT', limit=20)

    assert payload['lastUpdateId'] == 7
    assert calls == [{
        'path': '/fapi/v1/depth',
        'params': {'symbol': 'TESTUSDT', 'limit': 20},
        'timeout': 15,
    }]


def test_collect_book_ticker_samples_prefers_fresh_runtime_cache(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    now = mod._isoformat_utc(mod._utc_now())
    store.save_json('book_ticker_cache', {
        'TESTUSDT': {
            'updated_at': now,
            'samples': [
                {'bidPrice': '100.0', 'askPrice': '100.1', 'bidQty': '10', 'askQty': '8'},
                {'bidPrice': '100.0', 'askPrice': '100.2', 'bidQty': '9', 'askQty': '7'},
                {'bidPrice': '100.1', 'askPrice': '100.2', 'bidQty': '8', 'askQty': '6'},
            ],
        },
    })

    class StubClient:
        def get(self, path, params=None, timeout=15):
            raise AssertionError('fresh cache should avoid REST polling')

    samples = mod.collect_book_ticker_samples(StubClient(), 'TESTUSDT', sample_count=2, store=store, cache_max_age_seconds=5.0)

    assert samples == [
        {'bidPrice': '100.0', 'askPrice': '100.2', 'bidQty': '9', 'askQty': '7'},
        {'bidPrice': '100.1', 'askPrice': '100.2', 'bidQty': '8', 'askQty': '6'},
    ]
    events = store.read_events(limit=10)
    assert events[-1]['event_type'] == 'book_ticker_cache_hit'
    assert events[-1]['symbol'] == 'TESTUSDT'


def test_append_book_ticker_cache_sample_keeps_recent_ring_buffer(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))

    first = mod.append_book_ticker_cache_sample(
        store,
        'TESTUSDT',
        {'b': '100.0', 'a': '100.1', 'B': '10', 'A': '8', 'E': 1710000000000},
        max_samples=2,
    )
    second = mod.append_book_ticker_cache_sample(
        store,
        'TESTUSDT',
        {'bidPrice': '100.0', 'askPrice': '100.2', 'bidQty': '9', 'askQty': '7'},
        max_samples=2,
    )
    third = mod.append_book_ticker_cache_sample(
        store,
        'TESTUSDT',
        {'bidPrice': '100.1', 'askPrice': '100.2', 'bidQty': '8', 'askQty': '6'},
        max_samples=2,
    )

    cache_state = store.load_json('book_ticker_cache', {})
    symbol_state = cache_state['TESTUSDT']
    assert symbol_state['source'] == 'websocket'
    assert symbol_state['event_count'] == 3
    assert symbol_state['last_bid'] == '100.1'
    assert symbol_state['last_ask'] == '100.2'
    assert symbol_state['samples'] == [
        {'bidPrice': '100.0', 'askPrice': '100.2', 'bidQty': '9', 'askQty': '7'},
        {'bidPrice': '100.1', 'askPrice': '100.2', 'bidQty': '8', 'askQty': '6'},
    ]
    assert first['samples_cached'] == 1
    assert second['samples_cached'] == 2
    assert third['samples_cached'] == 2
    events = store.read_events(limit=10)
    assert events[-1]['event_type'] == 'book_ticker_ws_sample_written'
    assert events[-1]['symbol'] == 'TESTUSDT'


def test_process_book_ticker_stream_message_updates_runtime_cache(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    payload = {
        'stream': 'btcusdt@bookTicker',
        'data': {'s': 'BTCUSDT', 'b': '65000.1', 'a': '65000.2', 'B': '12.3', 'A': '7.8', 'E': 1710000000123},
    }

    result = mod.process_book_ticker_stream_message(store, payload, max_samples=3)

    assert result['symbol'] == 'BTCUSDT'
    assert result['samples_cached'] == 1
    cache_state = store.load_json('book_ticker_cache', {})
    assert cache_state['BTCUSDT']['samples'] == [
        {'bidPrice': '65000.1', 'askPrice': '65000.2', 'bidQty': '12.3', 'askQty': '7.8'}
    ]


def test_process_book_ticker_stream_message_ignores_invalid_payload(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))

    assert mod.process_book_ticker_stream_message(store, {'stream': 'btcusdt@aggTrade', 'data': {'s': 'BTCUSDT'}}, max_samples=3) is None
    assert mod.process_book_ticker_stream_message(store, {'data': {'a': '1'}}, max_samples=3) is None
    assert store.load_json('book_ticker_cache', {}) == {}


def test_run_book_ticker_cache_monitor_cycle_processes_socket_messages(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))

    class StubSocket:
        def __init__(self, messages):
            self.messages = list(messages)
            self.timeout_values = []
            self.closed = False

        def settimeout(self, value):
            self.timeout_values.append(value)

        def recv(self):
            if not self.messages:
                raise AssertionError('recv called after exhaustion')
            message = self.messages.pop(0)
            if isinstance(message, Exception):
                raise message
            return message

        def close(self):
            self.closed = True

    class TimeoutError(Exception):
        pass

    class StubWSModule:
        WebSocketTimeoutException = TimeoutError
        WebSocketException = RuntimeError

    socket = StubSocket([
        {'data': {'e': 'bookTicker', 's': 'BTCUSDT', 'b': '65000.1', 'a': '65000.2', 'B': '12.3', 'A': '7.8'}},
        TimeoutError('timed out'),
    ])

    result = mod.run_book_ticker_cache_monitor_cycle(
        store,
        socket,
        ws_module=StubWSModule,
        max_messages=5,
        max_samples=4,
        recv_timeout_seconds=1.5,
    )

    assert result['status'] == 'healthy'
    assert result['messages_processed'] == 1
    assert result['samples_written'] == 1
    assert socket.timeout_values == [1.5]
    cache_state = store.load_json('book_ticker_cache', {})
    assert cache_state['BTCUSDT']['event_count'] == 1
    events = store.read_events(limit=20)
    assert any(row['event_type'] == 'book_ticker_ws_connected' for row in events)


def test_run_book_ticker_cache_monitor_cycle_records_disconnect_and_closes_socket(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))

    class TimeoutError(Exception):
        pass

    class StubSocket:
        def __init__(self):
            self.closed = False

        def settimeout(self, value):
            self.value = value

        def recv(self):
            raise RuntimeError('socket broken')

        def close(self):
            self.closed = True

    class StubWSModule:
        WebSocketTimeoutException = TimeoutError
        WebSocketException = RuntimeError

    socket = StubSocket()
    result = mod.run_book_ticker_cache_monitor_cycle(
        store,
        socket,
        ws_module=StubWSModule,
        max_messages=2,
        max_samples=4,
        recv_timeout_seconds=1.0,
    )

    assert result['status'] == 'disconnected'
    assert result['messages_processed'] == 0
    assert socket.closed is True
    events = store.read_events(limit=20)
    assert events[-1]['event_type'] == 'book_ticker_ws_disconnected'


def test_run_book_ticker_cache_monitor_cycle_records_disconnect_and_closes_socket(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))

    class TimeoutError(Exception):
        pass

    class StubSocket:
        def __init__(self):
            self.closed = False

        def settimeout(self, value):
            self.value = value

        def recv(self):
            raise RuntimeError('socket broken')

        def close(self):
            self.closed = True

    class StubWSModule:
        WebSocketTimeoutException = TimeoutError
        WebSocketException = RuntimeError

    socket = StubSocket()
    result = mod.run_book_ticker_cache_monitor_cycle(
        store,
        socket,
        ws_module=StubWSModule,
        max_messages=2,
        max_samples=4,
        recv_timeout_seconds=1.0,
    )

    assert result['status'] == 'disconnected'
    assert result['messages_processed'] == 0
    assert socket.closed is True
    events = store.read_events(limit=20)
    assert events[-1]['event_type'] == 'book_ticker_ws_disconnected'


def test_build_book_ticker_stream_names_deduplicates_and_normalizes_symbols():
    streams = mod.build_book_ticker_stream_names(['btcusdt', 'ETHUSDT', 'BTCUSDT', '', None])

    assert streams == ['btcusdt@bookTicker', 'ethusdt@bookTicker']


def test_open_book_ticker_websocket_uses_multiplex_stream_url():
    class StubConnection:
        def __init__(self):
            self.calls = []

        def create_connection(self, url, timeout=None, sslopt=None):
            self.calls.append({'url': url, 'timeout': timeout, 'sslopt': sslopt})
            return {'url': url}

    connector = StubConnection()
    socket = mod.open_book_ticker_websocket(
        ['BTCUSDT', 'ethusdt'],
        ws_module=connector,
        base_ws_url='wss://fstream.binance.com/stream',
        connect_timeout_seconds=7.5,
    )

    assert socket == {'url': 'wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker'}
    assert connector.calls == [{
        'url': 'wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker',
        'timeout': 7.5,
        'sslopt': None,
    }]


def test_update_book_ticker_ws_health_state_persists_runtime_status(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))

    saved = mod.update_book_ticker_ws_health_state(
        store,
        status='healthy',
        symbols=['BTCUSDT', 'ethusdt', 'BTCUSDT'],
        reconnect_count=2,
        subscription_version=4,
        messages_processed=11,
        samples_written=9,
        active_streams=['btcusdt@bookTicker', 'ethusdt@bookTicker'],
        last_error='boom',
    )

    assert saved['status'] == 'healthy'
    assert saved['symbols'] == ['BTCUSDT', 'ETHUSDT']
    assert saved['symbol_count'] == 2
    assert saved['reconnect_count'] == 2
    assert saved['subscription_version'] == 4
    assert saved['messages_processed'] == 11
    assert saved['samples_written'] == 9
    persisted = store.load_json('book_ticker_ws_status', {})
    assert persisted['status'] == 'healthy'
    assert persisted['last_error'] == 'boom'
    assert persisted['active_streams'] == ['btcusdt@bookTicker', 'ethusdt@bookTicker']


def test_refresh_book_ticker_websocket_subscription_reopens_when_symbols_change(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))

    class StubSocket:
        def __init__(self, name):
            self.name = name
            self.closed = False

        def close(self):
            self.closed = True

    calls = []

    def fake_open(symbols, ws_module, base_ws_url, connect_timeout_seconds, sslopt=None):
        calls.append({
            'symbols': list(symbols),
            'base_ws_url': base_ws_url,
            'timeout': connect_timeout_seconds,
            'sslopt': sslopt,
        })
        return StubSocket(f'socket-{len(calls)}')

    state = {
        'symbols': ['BTCUSDT'],
        'ws': StubSocket('original'),
        'reconnect_count': 1,
        'subscription_version': 2,
    }

    refreshed = mod.refresh_book_ticker_websocket_subscription(
        store,
        state,
        requested_symbols=['ETHUSDT', 'BTCUSDT'],
        ws_module=object(),
        open_websocket_fn=fake_open,
        base_ws_url='wss://fstream.binance.com/stream',
        connect_timeout_seconds=5.0,
    )

    assert refreshed['reopened'] is True
    assert state['symbols'] == ['BTCUSDT', 'ETHUSDT']
    assert state['subscription_version'] == 3
    assert state['reconnect_count'] == 2
    assert state['ws'].name == 'socket-1'
    assert calls == [{
        'symbols': ['BTCUSDT', 'ETHUSDT'],
        'base_ws_url': 'wss://fstream.binance.com/stream',
        'timeout': 5.0,
        'sslopt': None,
    }]
    events = store.read_events(limit=20)
    assert events[-1]['event_type'] == 'book_ticker_ws_subscription_refreshed'


def test_book_ticker_websocket_supervisor_reconnects_and_refreshes_symbols(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))

    class TimeoutError(Exception):
        pass

    class StubWSModule:
        WebSocketTimeoutException = TimeoutError
        WebSocketException = RuntimeError

    class StubSocket:
        def __init__(self, name):
            self.name = name
            self.closed = False

        def settimeout(self, value):
            self.timeout = value

        def recv(self):
            raise AssertionError('supervisor test injects monitor results directly')

        def close(self):
            self.closed = True

    opened = []

    def fake_open(symbols, ws_module, base_ws_url, connect_timeout_seconds, sslopt=None):
        socket = StubSocket(f'ws-{len(opened) + 1}')
        opened.append({'symbols': list(symbols), 'socket': socket})
        return socket

    monitor_results = iter([
        {'status': 'disconnected', 'messages_processed': 1, 'samples_written': 1, 'error': 'socket broken'},
        {'status': 'healthy', 'messages_processed': 2, 'samples_written': 3},
    ])

    def fake_monitor(store_obj, ws, ws_module, max_messages, max_samples, recv_timeout_seconds):
        return next(monitor_results)

    requested_symbols = iter([
        ['BTCUSDT', 'ETHUSDT'],
        ['BTCUSDT', 'ETHUSDT'],
    ])

    def fake_symbol_provider():
        try:
            return next(requested_symbols)
        except StopIteration:
            return ['BTCUSDT', 'ETHUSDT']

    sleep_calls = []

    summary = mod.run_book_ticker_websocket_supervisor(
        store,
        initial_symbols=['BTCUSDT'],
        symbol_provider=fake_symbol_provider,
        ws_module=StubWSModule,
        open_websocket_fn=fake_open,
        monitor_cycle_fn=fake_monitor,
        sleep_fn=lambda seconds: sleep_calls.append(seconds),
        max_supervisor_cycles=2,
        base_ws_url='wss://fstream.binance.com/stream',
        connect_timeout_seconds=5.0,
        recv_timeout_seconds=1.0,
        max_messages_per_cycle=10,
        max_samples=4,
        reconnect_backoff_seconds=2.0,
        reconnect_backoff_multiplier=2.0,
        reconnect_backoff_cap_seconds=8.0,
    )

    assert summary['cycles_completed'] == 2
    assert summary['reconnect_count'] == 1
    assert summary['messages_processed_total'] == 3
    assert summary['samples_written_total'] == 4
    assert summary['symbols'] == ['BTCUSDT', 'ETHUSDT']
    assert len(opened) == 2
    assert opened[0]['symbols'] == ['BTCUSDT', 'ETHUSDT']
    assert opened[1]['symbols'] == ['BTCUSDT', 'ETHUSDT']
    assert sleep_calls == [2.0]
    health = store.load_json('book_ticker_ws_status', {})
    assert health['status'] == 'healthy'
    assert health['reconnect_count'] == 1
    assert health['symbols'] == ['BTCUSDT', 'ETHUSDT']


def test_collect_book_ticker_samples_falls_back_to_rest_when_cache_stale(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    stale_time = mod._isoformat_utc(mod._utc_now() - mod.datetime.timedelta(seconds=10))
    store.save_json('book_ticker_cache', {
        'TESTUSDT': {
            'updated_at': stale_time,
            'samples': [
                {'bidPrice': '100.0', 'askPrice': '100.1', 'bidQty': '10', 'askQty': '8'},
            ],
        },
    })
    calls = []

    class StubClient:
        def get(self, path, params=None, timeout=15):
            calls.append({'path': path, 'params': params, 'timeout': timeout})
            return {'bidPrice': '100.0', 'askPrice': '100.1', 'bidQty': '7', 'askQty': '5'}

    samples = mod.collect_book_ticker_samples(StubClient(), 'TESTUSDT', sample_count=2, interval_ms=0, store=store, cache_max_age_seconds=3.0)

    assert len(samples) == 2
    assert calls == [
        {'path': '/fapi/v1/ticker/bookTicker', 'params': {'symbol': 'TESTUSDT'}, 'timeout': 15},
        {'path': '/fapi/v1/ticker/bookTicker', 'params': {'symbol': 'TESTUSDT'}, 'timeout': 15},
    ]
    events = store.read_events(limit=10)
    assert events[-1]['event_type'] == 'book_ticker_cache_miss'
    assert events[-1]['fallback'] == 'rest_polling'


def test_derive_microstructure_inputs_includes_orderbook_metrics_from_real_samples():
    oi_history = [
        {'sumOpenInterestValue': '1000000'},
        {'sumOpenInterestValue': '1100000'},
        {'sumOpenInterestValue': '1250000'},
    ]
    taker_5m = make_kline(100, 103, 99, 102, volume=2000, quote_volume=204000)
    taker_5m[9] = '1300'
    taker_15m = [make_kline(99, 100, 98, 99.5, volume=1500 + i * 50, quote_volume=150000 + i * 5000) for i in range(20)]
    for idx, candle in enumerate(taker_15m):
        candle[9] = str(800 + idx * 10)
    top_ratio = [{'longShortRatio': '0.8'}]
    order_book = {
        'lastUpdateId': 11,
        'bids': [['100.0', '10'], ['99.9', '25'], ['99.8', '40']],
        'asks': [['100.1', '8'], ['100.2', '12'], ['100.3', '16']],
    }
    ticker_samples = [
        {'bidPrice': '100.0', 'askPrice': '100.1', 'bidQty': '10', 'askQty': '8'},
        {'bidPrice': '100.0', 'askPrice': '100.1', 'bidQty': '9', 'askQty': '7'},
        {'bidPrice': '100.0', 'askPrice': '100.2', 'bidQty': '7', 'askQty': '6'},
        {'bidPrice': '100.0', 'askPrice': '100.1', 'bidQty': '11', 'askQty': '8'},
    ]

    midpoint = (100.0 + 100.1) / 2.0
    expected_spread_bps = round(((100.1 - 100.0) / midpoint) * 10000, 4)
    expected_orderbook_slope = round(((10 + 25 + 40) + (8 + 12 + 16)) / (abs(100.0 - 99.8) + abs(100.3 - 100.1)), 4)
    expected_cancel_rate = round(2 / 4, 4)

    micro = mod.derive_microstructure_inputs(
        oi_history=oi_history,
        taker_5m=taker_5m,
        taker_15m=taker_15m,
        top_account_long_short=top_ratio,
        order_book=order_book,
        book_ticker_samples=ticker_samples,
    )

    assert micro['spread_bps'] == expected_spread_bps
    assert micro['orderbook_slope'] == expected_orderbook_slope
    assert micro['book_depth_fill_ratio'] == 1.0
    assert micro['cancel_rate'] == expected_cancel_rate


def test_run_scan_once_passes_real_orderbook_microstructure_into_build_candidate(monkeypatch, tmp_path):
    args = argparse.Namespace(
        symbol='',
        square_symbols='',
        square_symbols_file='',
        use_square_page=False,
        top_gainers=5,
        max_candidates=5,
        lookback_bars=12,
        swing_bars=6,
        risk_usdt=10.0,
        max_notional_usdt=0.0,
        min_5m_change_pct=0.0,
        min_quote_volume=0.0,
        stop_buffer_pct=0.01,
        max_rsi_5m=100.0,
        min_volume_multiple=0.0,
        max_distance_from_ema_pct=100.0,
        max_distance_from_vwap_pct=100.0,
        leverage=5,
        max_funding_rate=1.0,
        max_funding_rate_avg=1.0,
        okx_sentiment_inline='',
        okx_sentiment_file='',
        okx_sentiment_command='',
        okx_auto=False,
        okx_mcp_command='',
        okx_sentiment_timeout=5,
        external_signal_json='',
        smart_money_inline='',
        smart_money_file='',
        runtime_state_dir=str(tmp_path),
    )
    meta = make_meta(symbol='TESTUSDT')
    captured = {}
    sample_micro = {
        'spread_bps': 9.995,
        'orderbook_slope': 1.4182,
        'cancel_rate': 0.5,
        'book_depth_fill_ratio': 1.0,
    }
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        state_reasons=['launch_setup'],
        alert_tier='high',
        position_size_pct=0.0,
        liquidity_grade='A',
        expected_slippage_pct=0.08,
        book_depth_fill_ratio=1.0,
        spread_bps=9.995,
        orderbook_slope=1.4182,
        cancel_rate=0.5,
        setup_ready=True,
        trigger_fired=True,
    )

    monkeypatch.setattr(mod, 'load_manual_square_symbols', lambda _args: [])
    monkeypatch.setattr(mod, 'fetch_exchange_meta', lambda _client: {'TESTUSDT': meta})
    monkeypatch.setattr(mod, 'fetch_tickers', lambda _client: [{'symbol': 'TESTUSDT', 'quoteVolume': '1000000', 'lastPrice': '100'}])
    monkeypatch.setattr(mod, 'merged_candidate_symbols', lambda **kwargs: (['TESTUSDT'], {'TESTUSDT': 1}, {'TESTUSDT': 1}))
    monkeypatch.setattr(mod, 'fetch_klines', lambda _client, symbol, interval, limit: [make_kline(100, 101, 99, 100, volume=1000, quote_volume=100000)] * max(limit, 30))
    monkeypatch.setattr(mod, 'fetch_funding_rates', lambda _client, _symbol, limit=3: [0.0, 0.0, 0.0])
    monkeypatch.setattr(mod, 'fetch_open_interest_hist', lambda _client, _symbol, period='5m', limit=30: [])
    monkeypatch.setattr(mod, 'fetch_top_account_long_short_ratio', lambda _client, _symbol, period='5m', limit=10: [])
    monkeypatch.setattr(mod, 'fetch_order_book', lambda _client, _symbol, limit=20: {'bids': [['100.0', '10']], 'asks': [['100.1', '8']]}, raising=False)
    monkeypatch.setattr(mod, 'collect_book_ticker_samples', lambda _client, _symbol, sample_count=6, interval_ms=150, store=None, cache_max_age_seconds=3.0: [
        {'bidPrice': '100.0', 'askPrice': '100.1', 'bidQty': '10', 'askQty': '8'},
        {'bidPrice': '100.0', 'askPrice': '100.2', 'bidQty': '8', 'askQty': '6'},
    ], raising=False)

    def fake_derive_microstructure_inputs(**kwargs):
        captured['derive_kwargs'] = kwargs
        return sample_micro

    monkeypatch.setattr(mod, 'derive_microstructure_inputs', fake_derive_microstructure_inputs)
    monkeypatch.setattr(mod, 'compute_market_regime_filter', lambda **kwargs: {'risk_on': True, 'score_multiplier': 1.0, 'reasons': [], 'label': 'risk_on'})

    def fake_build_candidate(**kwargs):
        captured['build_kwargs'] = kwargs
        return candidate

    monkeypatch.setattr(mod, 'build_candidate', fake_build_candidate)

    payload, best, _meta = mod.run_scan_once(client=object(), args=args)

    assert best is candidate
    assert payload['selected']['spread_bps'] == 9.995
    assert captured['build_kwargs']['spread_bps'] == 9.995
    assert captured['build_kwargs']['orderbook_slope'] == 1.4182
    assert captured['build_kwargs']['cancel_rate'] == 0.5
    assert captured['build_kwargs']['book_depth_fill_ratio'] == 1.0
    assert 'order_book' in captured['derive_kwargs']
    assert 'book_ticker_samples' in captured['derive_kwargs']


def test_execution_liquidity_grade_v2_penalizes_wide_spread_thin_slope_and_high_cancel_rate():
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        liquidity_grade='A',
        expected_slippage_pct=0.12,
        book_depth_fill_ratio=0.86,
    )
    candidate.spread_bps = 14.0
    candidate.orderbook_slope = 0.18
    candidate.cancel_rate = 0.44

    quality = mod.compute_execution_quality_size_adjustment(candidate)
    alert = mod.build_standardized_alert(candidate, {'label': 'neutral', 'score_multiplier': 1.0, 'reasons': []})

    assert quality['execution_liquidity_grade'] == 'C'
    assert quality['size_multiplier'] == 0.35
    assert quality['size_bucket'] == 'caution'
    assert quality['spread_bps'] == 14.0
    assert quality['orderbook_slope'] == 0.18
    assert quality['cancel_rate'] == 0.44
    assert alert['execution_liquidity_grade'] == 'C'
    assert alert['spread_bps'] == 14.0
    assert alert['orderbook_slope'] == 0.18
    assert alert['cancel_rate'] == 0.44


def test_execution_liquidity_grade_v2_keeps_top_grade_when_microstructure_is_clean():
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        liquidity_grade='A',
        expected_slippage_pct=0.08,
        book_depth_fill_ratio=0.91,
    )
    candidate.spread_bps = 2.0
    candidate.orderbook_slope = 1.45
    candidate.cancel_rate = 0.03

    quality = mod.compute_execution_quality_size_adjustment(candidate)

    assert quality['execution_liquidity_grade'] == 'A+'
    assert quality['size_multiplier'] == 1.0
    assert quality['size_bucket'] == 'full'


def test_build_standardized_alert_exposes_execution_quality_size_adjustment():
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        position_size_pct=1.5,
        liquidity_grade='B',
        expected_slippage_pct=0.22,
        book_depth_fill_ratio=0.68,
        setup_ready=True,
        trigger_fired=True,
    )

    alert = mod.build_standardized_alert(candidate, {'label': 'neutral', 'score_multiplier': 1.0, 'reasons': []})

    assert alert['position_size_pct'] == 1.5
    assert alert['base_position_size_pct'] == 3.0
    assert alert['execution_quality_size_multiplier'] == 0.65
    assert alert['execution_quality_size_bucket'] == 'reduced'
    assert alert['execution_liquidity_grade'] == 'B'


def test_compute_execution_quality_size_adjustment_supports_finer_granularity():
    premium = mod.Candidate(
        symbol='PREMIUMUSDT',
        last_price=100.0,
        price_change_pct_24h=12.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        position_size_pct=3.0,
        liquidity_grade='A',
        expected_slippage_pct=0.08,
        book_depth_fill_ratio=0.92,
        setup_ready=True,
        trigger_fired=True,
    )
    reduced = mod.Candidate(
        symbol='REDUCEDUSDT',
        last_price=100.0,
        price_change_pct_24h=12.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        position_size_pct=3.0,
        liquidity_grade='B',
        expected_slippage_pct=0.22,
        book_depth_fill_ratio=0.68,
        setup_ready=True,
        trigger_fired=True,
    )
    caution = mod.Candidate(
        symbol='CAUTIONUSDT',
        last_price=100.0,
        price_change_pct_24h=12.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        position_size_pct=3.0,
        liquidity_grade='B',
        expected_slippage_pct=0.26,
        book_depth_fill_ratio=0.58,
        setup_ready=True,
        trigger_fired=True,
    )
    thin = mod.Candidate(
        symbol='THINUSDT',
        last_price=100.0,
        price_change_pct_24h=12.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        position_size_pct=3.0,
        liquidity_grade='C',
        expected_slippage_pct=0.28,
        book_depth_fill_ratio=0.52,
        setup_ready=True,
        trigger_fired=True,
    )

    premium_payload = mod.compute_execution_quality_size_adjustment(premium)
    reduced_payload = mod.compute_execution_quality_size_adjustment(reduced)
    caution_payload = mod.compute_execution_quality_size_adjustment(caution)
    thin_payload = mod.compute_execution_quality_size_adjustment(thin)

    assert premium_payload['execution_liquidity_grade'] == 'A+'
    assert premium_payload['size_multiplier'] == 1.0
    assert premium_payload['size_bucket'] == 'full'
    assert reduced_payload['execution_liquidity_grade'] == 'B'
    assert reduced_payload['size_multiplier'] == 0.65
    assert reduced_payload['size_bucket'] == 'reduced'
    assert caution_payload['execution_liquidity_grade'] == 'C'
    assert caution_payload['size_multiplier'] == 0.35
    assert caution_payload['size_bucket'] == 'caution'
    assert thin_payload['execution_liquidity_grade'] == 'C'
    assert thin_payload['size_multiplier'] == 0.35
    assert thin_payload['size_bucket'] == 'caution'


def test_compute_execution_quality_size_adjustment_supports_finer_live_sizing_buckets():
    reduced_plus = mod.Candidate(
        symbol='REDUCEDPLUSUSDT',
        last_price=100.0,
        price_change_pct_24h=12.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        position_size_pct=3.0,
        liquidity_grade='B',
        expected_slippage_pct=0.18,
        book_depth_fill_ratio=0.83,
        setup_ready=True,
        trigger_fired=True,
    )
    caution_plus = mod.Candidate(
        symbol='CAUTIONPLUSUSDT',
        last_price=100.0,
        price_change_pct_24h=12.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        position_size_pct=3.0,
        liquidity_grade='B',
        expected_slippage_pct=0.23,
        book_depth_fill_ratio=0.63,
        setup_ready=True,
        trigger_fired=True,
    )
    minimal_plus = mod.Candidate(
        symbol='MINIMALPLUSUSDT',
        last_price=100.0,
        price_change_pct_24h=12.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        position_size_pct=3.0,
        liquidity_grade='C',
        expected_slippage_pct=0.28,
        book_depth_fill_ratio=0.52,
        setup_ready=True,
        trigger_fired=True,
    )

    caution = mod.Candidate(
        symbol='CAUTIONUSDT',
        last_price=100.0,
        price_change_pct_24h=12.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        position_size_pct=3.0,
        liquidity_grade='B',
        expected_slippage_pct=0.26,
        book_depth_fill_ratio=0.58,
        setup_ready=True,
        trigger_fired=True,
    )

    reduced_plus_payload = mod.compute_execution_quality_size_adjustment(reduced_plus)
    caution_plus_payload = mod.compute_execution_quality_size_adjustment(caution_plus)
    caution_payload = mod.compute_execution_quality_size_adjustment(caution)
    minimal_plus_payload = mod.compute_execution_quality_size_adjustment(minimal_plus)

    assert reduced_plus_payload['execution_liquidity_grade'] == 'A'
    assert reduced_plus_payload['size_multiplier'] == 1.0
    assert reduced_plus_payload['size_bucket'] == 'full'
    assert caution_plus_payload['execution_liquidity_grade'] == 'B'
    assert caution_plus_payload['size_multiplier'] == 0.65
    assert caution_plus_payload['size_bucket'] == 'reduced'
    assert caution_payload['execution_liquidity_grade'] == 'C'
    assert caution_payload['size_multiplier'] == 0.35
    assert caution_payload['size_bucket'] == 'caution'
    assert minimal_plus_payload['execution_liquidity_grade'] == 'C'
    assert minimal_plus_payload['size_multiplier'] == 0.35
    assert minimal_plus_payload['size_bucket'] == 'caution'


def test_append_candidate_rejected_event_uses_enum_reason_and_phase2_execution_fields(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path / 'runtime'))
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=16.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=79.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='overheated',
        state_reasons=['extension'],
        liquidity_grade='B',
        expected_slippage_pct=0.31,
        book_depth_fill_ratio=0.62,
    )

    event = mod.append_candidate_rejected_event(store, candidate, ['extended_chase_veto'])

    assert event['reject_reason'] == 'extended_chase_veto'
    assert event['reject_reason_label'] == 'price_extension_chase'
    assert event['expected_slippage_r'] == 0.155
    assert event['execution_liquidity_grade'] == 'C'


def test_summarize_candidate_rejected_events_aggregates_reason_grade_and_overextension_metrics(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path / 'runtime'))
    rows = [
        {
            'event_type': 'candidate_rejected',
            'symbol': 'DOGEUSDT',
            'reject_reason': 'execution_slippage_veto',
            'reject_reason_label': 'execution_slippage',
            'execution_liquidity_grade': 'C',
            'overextension_flag': 'mild',
        },
        {
            'event_type': 'candidate_rejected',
            'symbol': 'DOGEUSDT',
            'reject_reason': 'execution_slippage_veto',
            'reject_reason_label': 'execution_slippage',
            'execution_liquidity_grade': 'D',
            'overextension_flag': 'mild',
        },
        {
            'event_type': 'candidate_rejected',
            'symbol': 'PEPEUSDT',
            'reject_reason': 'extended_chase_veto',
            'reject_reason_label': 'price_extension_chase',
            'execution_liquidity_grade': 'B',
            'overextension_flag': 'severe',
        },
        {
            'event_type': 'entry_filled',
            'symbol': 'BTCUSDT',
        },
    ]
    events_path = store._events_path()
    events_path.parent.mkdir(parents=True, exist_ok=True)
    events_path.write_text('\n'.join(mod.json.dumps(row, ensure_ascii=False) for row in rows) + '\n', encoding='utf-8')

    summary = mod.summarize_candidate_rejected_events(store)

    assert summary['total_candidate_rejected'] == 3
    assert summary['by_reject_reason'] == {
        'execution_slippage_veto': 2,
        'extended_chase_veto': 1,
    }
    assert summary['by_reject_reason_label'] == {
        'execution_slippage': 2,
        'price_extension_chase': 1,
    }
    assert summary['by_execution_liquidity_grade'] == {
        'C': 1,
        'D': 1,
        'B': 1,
    }
    assert summary['by_overextension_flag'] == {
        'mild': 2,
        'severe': 1,
    }
    assert summary['top_symbols'] == {
        'DOGEUSDT': 2,
        'PEPEUSDT': 1,
    }


def test_evaluate_risk_guards_blocks_portfolio_theme_and_correlation_overexposure():
    candidate = mod.Candidate(
        symbol='DOGEUSDT',
        last_price=100.0,
        price_change_pct_24h=14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        position_size_pct=2.2,
        expected_slippage_pct=0.1,
        book_depth_fill_ratio=0.85,
    )

    result = mod.evaluate_risk_guards(
        symbol='DOGEUSDT',
        candidate=candidate,
        risk_state={
            'portfolio_exposure_pct_by_theme': {'meme': 4.5},
            'portfolio_exposure_pct_by_correlation': {'dog-family': 2.3},
        },
        portfolio_narrative_bucket='meme',
        portfolio_correlation_group='dog-family',
        max_portfolio_exposure_pct_per_theme=5.0,
        max_portfolio_exposure_pct_per_correlation_group=3.0,
    )

    assert result['allowed'] is False
    assert 'candidate_portfolio_theme_overexposure' in result['reasons']
    assert 'candidate_portfolio_correlation_overexposure' in result['reasons']
    assert result['normalized_risk_state']['portfolio_exposure_pct_by_theme']['meme'] == 4.5
    assert result['normalized_risk_state']['portfolio_exposure_pct_by_correlation']['dog-family'] == 2.3


def test_run_scan_once_applies_execution_quality_position_sizing(monkeypatch, tmp_path):
    args = argparse.Namespace(
        symbol='',
        square_symbols='',
        square_symbols_file='',
        use_square_page=False,
        top_gainers=5,
        max_candidates=5,
        lookback_bars=12,
        swing_bars=6,
        risk_usdt=10.0,
        max_notional_usdt=0.0,
        min_5m_change_pct=0.0,
        min_quote_volume=0.0,
        stop_buffer_pct=0.01,
        max_rsi_5m=100.0,
        min_volume_multiple=0.0,
        max_distance_from_ema_pct=100.0,
        max_distance_from_vwap_pct=100.0,
        leverage=5,
        max_funding_rate=1.0,
        max_funding_rate_avg=1.0,
        okx_sentiment_inline='',
        okx_sentiment_file='',
        okx_sentiment_command='',
        okx_auto=False,
        okx_mcp_command='',
        okx_sentiment_timeout=5,
        external_signal_json='',
        smart_money_inline='',
        smart_money_file='',
        runtime_state_dir=str(tmp_path),
    )

    meta = make_meta(symbol='TESTUSDT')
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        state_reasons=['launch_setup'],
        alert_tier='high',
        position_size_pct=0.0,
        quality_score=63.0,
        execution_priority_score=22.5,
        entry_distance_from_breakout_pct=0.8,
        entry_distance_from_vwap_pct=0.7,
        candle_extension_pct=1.1,
        recent_3bar_runup_pct=2.4,
        overextension_flag='mild',
        entry_pattern='breakout',
        trend_regime='weak_up',
        liquidity_grade='B',
        setup_ready=True,
        trigger_fired=True,
        expected_slippage_pct=0.22,
        book_depth_fill_ratio=0.68,
    )

    monkeypatch.setattr(mod, 'load_manual_square_symbols', lambda _args: [])
    monkeypatch.setattr(mod, 'fetch_exchange_meta', lambda _client: {'TESTUSDT': meta})
    monkeypatch.setattr(mod, 'fetch_tickers', lambda _client: [{'symbol': 'TESTUSDT', 'quoteVolume': '1000000', 'lastPrice': '100'}])
    monkeypatch.setattr(mod, 'merged_candidate_symbols', lambda **kwargs: (['TESTUSDT'], {'TESTUSDT': 1}, {'TESTUSDT': 1}))
    monkeypatch.setattr(mod, 'fetch_klines', lambda _client, symbol, interval, limit: [make_kline(100, 101, 99, 100, volume=1000, quote_volume=100000)] * max(limit, 30))
    monkeypatch.setattr(mod, 'fetch_funding_rates', lambda _client, _symbol, limit=3: [0.0, 0.0, 0.0])
    monkeypatch.setattr(mod, 'fetch_open_interest_hist', lambda _client, _symbol, period='5m', limit=30: [])
    monkeypatch.setattr(mod, 'fetch_top_account_long_short_ratio', lambda _client, _symbol, period='5m', limit=10: [])
    monkeypatch.setattr(mod, 'derive_microstructure_inputs', lambda **kwargs: {})
    monkeypatch.setattr(mod, 'compute_market_regime_filter', lambda **kwargs: {'risk_on': True, 'score_multiplier': 1.0, 'reasons': [], 'label': 'neutral'})
    monkeypatch.setattr(mod, 'build_candidate', lambda **kwargs: candidate)

    payload, best, _meta = mod.run_scan_once(client=object(), args=args)

    assert best is candidate
    assert candidate.position_size_pct == 1.95
    assert 'execution_quality_size_multiplier=0.65' in candidate.reasons
    assert 'execution_quality_size_bucket=reduced' in candidate.reasons
    assert payload['selected']['position_size_pct'] == 1.95
    assert payload['selected']['base_position_size_pct'] == 3.0
    assert payload['selected']['execution_quality_size_multiplier'] == 0.65
    assert payload['selected']['execution_quality_size_bucket'] == 'reduced'


def test_run_scan_once_applies_caution_execution_quality_position_sizing(monkeypatch, tmp_path):
    args = argparse.Namespace(
        symbol='',
        square_symbols='',
        square_symbols_file='',
        use_square_page=False,
        top_gainers=5,
        max_candidates=5,
        lookback_bars=12,
        swing_bars=6,
        risk_usdt=10.0,
        max_notional_usdt=0.0,
        min_5m_change_pct=0.0,
        min_quote_volume=0.0,
        stop_buffer_pct=0.01,
        max_rsi_5m=100.0,
        min_volume_multiple=0.0,
        max_distance_from_ema_pct=100.0,
        max_distance_from_vwap_pct=100.0,
        leverage=5,
        max_funding_rate=1.0,
        max_funding_rate_avg=1.0,
        okx_sentiment_inline='',
        okx_sentiment_file='',
        okx_sentiment_command='',
        okx_auto=False,
        okx_mcp_command='',
        okx_sentiment_timeout=5,
        external_signal_json='',
        smart_money_inline='',
        smart_money_file='',
        runtime_state_dir=str(tmp_path),
    )

    meta = make_meta(symbol='TESTUSDT')
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        state_reasons=['launch_setup'],
        alert_tier='high',
        position_size_pct=0.0,
        quality_score=63.0,
        execution_priority_score=22.5,
        entry_distance_from_breakout_pct=0.8,
        entry_distance_from_vwap_pct=0.7,
        candle_extension_pct=1.1,
        recent_3bar_runup_pct=2.4,
        overextension_flag='mild',
        entry_pattern='breakout',
        trend_regime='weak_up',
        liquidity_grade='B',
        setup_ready=True,
        trigger_fired=True,
        expected_slippage_pct=0.26,
        book_depth_fill_ratio=0.58,
    )

    monkeypatch.setattr(mod, 'load_manual_square_symbols', lambda _args: [])
    monkeypatch.setattr(mod, 'fetch_exchange_meta', lambda _client: {'TESTUSDT': meta})
    monkeypatch.setattr(mod, 'fetch_tickers', lambda _client: [{'symbol': 'TESTUSDT', 'quoteVolume': '1000000', 'lastPrice': '100'}])
    monkeypatch.setattr(mod, 'merged_candidate_symbols', lambda **kwargs: (['TESTUSDT'], {'TESTUSDT': 1}, {'TESTUSDT': 1}))
    monkeypatch.setattr(mod, 'fetch_klines', lambda _client, symbol, interval, limit: [make_kline(100, 101, 99, 100, volume=1000, quote_volume=100000)] * max(limit, 30))
    monkeypatch.setattr(mod, 'fetch_funding_rates', lambda _client, _symbol, limit=3: [0.0, 0.0, 0.0])
    monkeypatch.setattr(mod, 'fetch_open_interest_hist', lambda _client, _symbol, period='5m', limit=30: [])
    monkeypatch.setattr(mod, 'fetch_top_account_long_short_ratio', lambda _client, _symbol, period='5m', limit=10: [])
    monkeypatch.setattr(mod, 'derive_microstructure_inputs', lambda **kwargs: {})
    monkeypatch.setattr(mod, 'compute_market_regime_filter', lambda **kwargs: {'risk_on': True, 'score_multiplier': 1.0, 'reasons': [], 'label': 'neutral'})
    monkeypatch.setattr(mod, 'build_candidate', lambda **kwargs: candidate)

    payload, best, _meta = mod.run_scan_once(client=object(), args=args)

    assert best is candidate
    assert candidate.position_size_pct == 1.05
    assert 'execution_quality_size_multiplier=0.35' in candidate.reasons
    assert 'execution_quality_size_bucket=caution' in candidate.reasons
    assert payload['selected']['position_size_pct'] == 1.05
    assert payload['selected']['base_position_size_pct'] == 3.0
    assert payload['selected']['execution_quality_size_multiplier'] == 0.35
    assert payload['selected']['execution_quality_size_bucket'] == 'caution'


def test_apply_hard_veto_filters_rejects_extreme_execution_quality_before_live_trade():
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=10.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        liquidity_grade='C',
        expected_slippage_pct=0.34,
        book_depth_fill_ratio=0.41,
        setup_ready=True,
        trigger_fired=True,
    )

    reason = mod.apply_hard_veto_filters(candidate)

    assert reason == 'execution_depth_veto'


def test_evaluate_risk_guards_only_blocks_thinnest_execution_quality_tier():
    caution_candidate = mod.Candidate(
        symbol='CAUTIONUSDT',
        last_price=100.0,
        price_change_pct_24h=8.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=69.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        liquidity_grade='B',
        expected_slippage_pct=0.26,
        book_depth_fill_ratio=0.58,
        setup_ready=True,
        trigger_fired=True,
    )
    thin_candidate = mod.Candidate(
        symbol='THINUSDT',
        last_price=100.0,
        price_change_pct_24h=8.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=69.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        liquidity_grade='C',
        expected_slippage_pct=0.28,
        book_depth_fill_ratio=0.52,
        setup_ready=True,
        trigger_fired=True,
    )

    caution_payload = mod.evaluate_risk_guards(symbol='CAUTIONUSDT', risk_state=mod.default_risk_state(), candidate=caution_candidate)
    thin_payload = mod.evaluate_risk_guards(symbol='THINUSDT', risk_state=mod.default_risk_state(), candidate=thin_candidate)

    assert caution_payload['allowed'] is True
    assert caution_payload['reasons'] == []
    assert thin_payload['allowed'] is True
    assert thin_payload['reasons'] == []


def test_place_live_trade_applies_execution_quality_size_multiplier(monkeypatch):
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        position_size_pct=1.5,
        liquidity_grade='B',
        expected_slippage_pct=0.23,
        book_depth_fill_ratio=0.63,
        setup_ready=True,
        trigger_fired=True,
    )
    meta = mod.SymbolMeta(
        symbol='TESTUSDT',
        price_precision=2,
        quantity_precision=1,
        tick_size=0.01,
        step_size=0.1,
        min_qty=0.1,
        quote_asset='USDT',
        status='TRADING',
        contract_type='PERPETUAL',
    )
    args = argparse.Namespace(tp1_r=1.5, tp1_close_pct=0.3, tp2_r=2.0, tp2_close_pct=0.4, breakeven_r=1.0, profile='test-profile')
    calls = []

    class Client:
        def signed_post(self, path, params):
            calls.append((path, params))
            if path == '/fapi/v1/leverage':
                return {'leverage': params['leverage']}
            if path == '/fapi/v1/order':
                return {
                    'symbol': 'TESTUSDT',
                    'orderId': 12345,
                    'status': 'FILLED',
                    'avgPrice': '100.2',
                    'executedQty': params['quantity'],
                    'cumQuote': '501.0',
                    'updateTime': 1710000000123,
                    'clientOrderId': 'entry-order-1',
                }
            raise AssertionError(f'unexpected path: {path}')

    monkeypatch.setattr(mod, 'log_runtime_event', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'place_stop_market_order', lambda *a, **k: {'orderId': 54321, 'clientOrderId': 'stop-1'})
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *a, **k: {'status': 'protected', 'expected_order_id': 54321})

    result = mod.place_live_trade(Client(), candidate, leverage=5, meta=meta, args=args)

    assert calls[1][0] == '/fapi/v1/order'
    assert calls[1][1]['quantity'] == '6.5'
    assert result['filled_quantity'] == 6.5
    assert result['trade_management_plan']['quantity'] == 6.5


def test_run_scan_once_records_execution_quality_reject_stats(monkeypatch, tmp_path):
    args = argparse.Namespace(
        symbol='',
        square_symbols='',
        square_symbols_file='',
        use_square_page=False,
        top_gainers=5,
        max_candidates=5,
        lookback_bars=12,
        swing_bars=6,
        risk_usdt=10.0,
        max_notional_usdt=0.0,
        min_5m_change_pct=0.0,
        min_quote_volume=0.0,
        stop_buffer_pct=0.01,
        max_rsi_5m=100.0,
        min_volume_multiple=0.0,
        max_distance_from_ema_pct=100.0,
        max_distance_from_vwap_pct=100.0,
        leverage=5,
        max_funding_rate=1.0,
        max_funding_rate_avg=1.0,
        okx_sentiment_inline='',
        okx_sentiment_file='',
        okx_sentiment_command='',
        okx_auto=False,
        okx_mcp_command='',
        okx_sentiment_timeout=5,
        external_signal_json='',
        smart_money_inline='',
        smart_money_file='',
        runtime_state_dir=str(tmp_path),
    )

    meta = make_meta(symbol='TESTUSDT')
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=68.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        state_reasons=['launch_setup'],
        alert_tier='high',
        must_pass_flags={
            'execution_slippage_ok': False,
            'execution_depth_ok': False,
        },
        quality_score=63.0,
        execution_priority_score=22.5,
        entry_distance_from_breakout_pct=0.8,
        entry_distance_from_vwap_pct=0.7,
        candle_extension_pct=2.2,
        recent_3bar_runup_pct=3.8,
        overextension_flag='mild',
        entry_pattern='breakout',
        trend_regime='weak_up',
        liquidity_grade='C',
        setup_ready=True,
        trigger_fired=True,
        expected_slippage_pct=0.62,
        book_depth_fill_ratio=0.44,
    )

    monkeypatch.setattr(mod, 'load_manual_square_symbols', lambda _args: [])
    monkeypatch.setattr(mod, 'fetch_exchange_meta', lambda _client: {'TESTUSDT': meta})
    monkeypatch.setattr(mod, 'fetch_tickers', lambda _client: [{'symbol': 'TESTUSDT', 'quoteVolume': '1000000', 'lastPrice': '100'}])
    monkeypatch.setattr(mod, 'merged_candidate_symbols', lambda **kwargs: (['TESTUSDT'], {'TESTUSDT': 1}, {'TESTUSDT': 1}))
    monkeypatch.setattr(mod, 'fetch_klines', lambda _client, symbol, interval, limit: [make_kline(100, 101, 99, 100, volume=1000, quote_volume=100000)] * max(limit, 30))
    monkeypatch.setattr(mod, 'fetch_funding_rates', lambda _client, _symbol, limit=3: [0.0, 0.0, 0.0])
    monkeypatch.setattr(mod, 'fetch_open_interest_hist', lambda _client, _symbol, period='5m', limit=30: [])
    monkeypatch.setattr(mod, 'fetch_top_account_long_short_ratio', lambda _client, _symbol, period='5m', limit=10: [])
    monkeypatch.setattr(mod, 'derive_microstructure_inputs', lambda **kwargs: {})
    monkeypatch.setattr(mod, 'compute_market_regime_filter', lambda **kwargs: {'risk_on': True, 'score_multiplier': 1.0, 'reasons': [], 'label': 'neutral'})
    monkeypatch.setattr(mod, 'build_candidate', lambda **kwargs: candidate)

    payload, best, _meta = mod.run_scan_once(client=object(), args=args)

    assert best is None
    assert payload['candidate_count'] == 0
    assert payload['rejected_stats']['total'] == 2
    assert payload['rejected_stats']['by_reason']['execution_slippage_veto'] == 2
    assert payload['rejected_stats']['by_reject_label']['execution_slippage'] == 2
    assert payload['rejected_stats']['by_execution_liquidity_grade']['D'] == 2


def test_evaluate_risk_guards_blocks_live_entry_for_execution_quality_candidate():
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=8.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=69.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['seed'],
        state='launch',
        alert_tier='high',
        liquidity_grade='C',
        expected_slippage_pct=0.62,
        book_depth_fill_ratio=0.44,
        setup_ready=True,
        trigger_fired=True,
    )

    payload = mod.evaluate_risk_guards(symbol='TESTUSDT', risk_state=mod.default_risk_state(), candidate=candidate)

    assert payload['allowed'] is False
    assert 'candidate_execution_slippage_risk' in payload['reasons']
    assert payload['reasons'] == ['candidate_execution_slippage_risk']


def test_run_loop_records_execution_quality_rejection_event_when_risk_guard_blocks_live_trade(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    args = argparse.Namespace(
        reconcile_only=False,
        halt_on_orphan_position=False,
        daily_max_loss_usdt=0.0,
        max_consecutive_losses=0,
        symbol_cooldown_minutes=0,
        live=True,
        max_open_positions=1,
        profile='test',
        auto_loop=False,
        disable_notify=True,
        notify_target='',
    )
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=8.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=99.0,
        recent_swing_low=97.0,
        stop_price=98.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=69.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=0.8,
        distance_from_vwap_15m_pct=0.7,
        higher_tf_summary='aligned',
        score=72.0,
        reasons=['candidate_selected'],
        state='launch',
        alert_tier='high',
        liquidity_grade='C',
        expected_slippage_pct=0.62,
        book_depth_fill_ratio=0.44,
        setup_ready=True,
        trigger_fired=True,
    )

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda _args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda *a, **k: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *a, **k: ({'ok': True, 'candidate_count': 1, 'candidates': [{'symbol': 'TESTUSDT'}]}, candidate, {'TESTUSDT': make_meta()}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda _store: mod.default_risk_state())

    result = mod.run_loop(client=object(), args=args)
    events_path = tmp_path / 'events.jsonl'
    rows = [mod.json.loads(line) for line in events_path.read_text(encoding='utf-8').splitlines() if line.strip()]

    assert result['cycles'][0]['live_skipped_due_to_risk_guard'] == [
        'candidate_execution_slippage_risk',
    ]
    assert rows[-1]['event_type'] == 'candidate_rejected'
    assert rows[-1]['symbol'] == 'TESTUSDT'
    assert rows[-1]['reasons'] == [
        'candidate_execution_slippage_risk',
    ]
    assert rows[-1]['reject_reason'] == 'execution_slippage_veto'
    assert rows[-1]['reject_reason_label'] == 'execution_slippage'
    assert rows[-1]['expected_slippage_r'] == 0.31
    assert rows[-1]['execution_liquidity_grade'] == 'D'


def test_parse_args_accepts_okx_auto_flags(monkeypatch):
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'prog',
            '--okx-auto',
            '--okx-sentiment-command',
            'python okx_feed.py',
            '--okx-mcp-command',
            '/tmp/okx-trade-mcp',
            '--okx-sentiment-timeout',
            '33',
        ],
    )

    args = mod.parse_args()

    assert args.okx_auto is True
    assert args.okx_sentiment_command == 'python okx_feed.py'
    assert args.okx_mcp_command == '/tmp/okx-trade-mcp'
    assert args.okx_sentiment_timeout == 33
    assert 'okx_sentiment_command' in args._explicit_cli_dests
    assert 'okx_mcp_command' in args._explicit_cli_dests
    assert 'okx_sentiment_timeout' in args._explicit_cli_dests


def test_compute_sentiment_resonance_bonus_rewards_early_turn_and_penalizes_overheated_sentiment():
    payload = mod.compute_sentiment_resonance_bonus(
        okx_sentiment_score=0.34,
        okx_sentiment_acceleration=0.42,
        sector_resonance_score=0.68,
        smart_money_flow_score=0.18,
    )

    assert payload['bonus'] > payload['penalty']
    assert payload['net'] > 6.0
    assert 'okx_sentiment_positive' in payload['reasons']
    assert 'okx_sentiment_accelerating' in payload['reasons']
    assert 'sector_resonance_positive' in payload['reasons']
    assert 'sentiment_early_turn' in payload['reasons']
    assert 'sector_alignment_confirmed' in payload['reasons']

    overheated = mod.compute_sentiment_resonance_bonus(
        okx_sentiment_score=0.91,
        okx_sentiment_acceleration=0.58,
        sector_resonance_score=0.41,
        smart_money_flow_score=0.05,
    )

    assert overheated['penalty'] > 4.0
    assert overheated['net'] < payload['net']
    assert 'sentiment_overheated' in overheated['reasons']


def test_compute_market_regime_filter_labels_dual_breakdown_as_risk_off():
    btc = [make_kline(100 - i, 101 - i, 99 - i, 99 - i) for i in range(30)]
    sol = [make_kline(200 - (i * 2), 201 - (i * 2), 199 - (i * 2), 198 - (i * 2)) for i in range(30)]

    regime = mod.compute_market_regime_filter(btc_klines=btc, sol_klines=sol)

    assert regime['label'] == 'risk_off'
    assert regime['risk_on'] is False
    assert regime['score_multiplier'] <= 0.55
    assert 'btc_trend_down' in regime['reasons']
    assert 'sol_trend_down' in regime['reasons']
