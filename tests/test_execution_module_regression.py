import argparse
import copy
import importlib.util
import pathlib
import sys
import tempfile
import time


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

MAIN_MODULE_PATH = SCRIPTS_DIR / 'binance_futures_momentum_long.py'
EXECUTION_MODULE_PATH = SCRIPTS_DIR / 'execution_engine.py'
CANDIDATE_MODULE_PATH = SCRIPTS_DIR / 'candidate_builder.py'


def _load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mod = _load_module('strategy_execution_mod', MAIN_MODULE_PATH)
exec_mod = _load_module('execution_engine_mod', EXECUTION_MODULE_PATH)
candidate_mod = _load_module('candidate_builder_mod', CANDIDATE_MODULE_PATH)


class Client:
    def __init__(self):
        self.calls = []

    def signed_post(self, path, params):
        self.calls.append((path, dict(params)))
        if path == '/fapi/v1/marginType':
            return {'code': 200, 'msg': 'success'}
        if path == '/fapi/v1/leverage':
            return {'leverage': params['leverage']}
        if path == '/fapi/v1/order':
            quantity = params['quantity']
            return {
                'symbol': params['symbol'],
                'orderId': 12345,
                'status': 'FILLED',
                'avgPrice': '100.2',
                'executedQty': quantity,
                'cumQuote': '651.3',
                'updateTime': 1710000000123,
                'clientOrderId': 'entry-order-1',
            }
        if path == '/fapi/v1/algoOrder':
            return {
                'symbol': params['symbol'],
                'orderId': len([call for call in self.calls if call[0] == path]),
                'clientAlgoId': f"algo-{len([call for call in self.calls if call[0] == path])}",
                'triggerPrice': params.get('triggerPrice'),
                'quantity': params.get('quantity'),
                'side': params.get('side'),
                'positionSide': params.get('positionSide'),
            }
        raise AssertionError(f'unexpected path: {path}')


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


def make_candidate(symbol='TESTUSDT'):
    return mod.Candidate(
        symbol=symbol,
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


def make_args():
    return argparse.Namespace(
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        tp1_profit_usdt=0.0,
        tp2_profit_usdt=0.0,
        breakeven_r=1.0,
        profile='test-profile',
        margin_type='ISOLATED',
        disable_notify=True,
        notify_target='',
        max_monitor_cycles=1,
        monitor_poll_interval_sec=0,
        trailing_buffer_pct=0.01,
        initial_stop_max_attempts=3,
        initial_stop_retry_sleep_sec=0,
    )


def make_trade(side='LONG'):
    stop_price = 95.0 if str(side).upper() == 'LONG' else 105.0
    return {
        'symbol': 'TESTUSDT',
        'side': str(side).upper(),
        'entry_price': 100.0,
        'quantity': 1.0,
        'stop_order': {'orderId': 321},
        'protection_check': {'status': 'protected', 'matched_via': 'order_id'},
        'trade_management_plan': {
            'entry_price': 100.0,
            'stop_price': stop_price,
            'quantity': 1.0,
            'initial_risk_per_unit': 5.0,
            'breakeven_trigger_price': 101.0 if str(side).upper() == 'LONG' else 99.0,
            'tp1_trigger_price': 105.0 if str(side).upper() == 'LONG' else 95.0,
            'tp1_close_qty': 0.5,
            'tp2_trigger_price': 110.0 if str(side).upper() == 'LONG' else 90.0,
            'tp2_close_qty': 0.3,
            'runner_qty': 0.2,
            'breakeven_confirmation_mode': 'price_only',
            'breakeven_min_buffer_pct': 0.0,
            'side': str(side).lower(),
            'position_side': str(side).upper(),
        },
    }


def test_build_candidate_module_exports_entry_point():
    assert hasattr(candidate_mod, 'build_candidate')


def test_build_candidate_wrapper_matches_extracted_module(monkeypatch):
    sentinel = object()
    captured = {}

    def fake_build_candidate_impl(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(mod, 'build_candidate_impl', fake_build_candidate_impl)

    result = mod.build_candidate(
        symbol='TESTUSDT',
        ticker={'quoteVolume': '1000', 'priceChangePercent': '1.2'},
        klines_5m=[[0, 1, 2, 0.5, 1.5]] * 40,
        klines_15m=[[0, 1, 2, 0.5, 1.5]] * 30,
        klines_1h=[[0, 1, 2, 0.5, 1.5]] * 30,
        klines_4h=[[0, 1, 2, 0.5, 1.5]] * 30,
        meta=make_meta(),
        hot_rank=3,
        gainer_rank=5,
        funding_rate=0.001,
        funding_rate_avg=0.0005,
        open_interest_rows=[{'sumOpenInterest': '1'}],
        taker_long_short_ratio_rows=[{'buySellRatio': '1.1'}],
        top_long_short_position_ratio_rows=[{'longShortRatio': '1.2'}],
        top_long_short_account_ratio_rows=[{'longShortRatio': '1.3'}],
        symbol_open_interest_rows_5m=[{'sumOpenInterest': '1'}],
        symbol_open_interest_rows_15m=[{'sumOpenInterest': '2'}],
        market_regime={'label': 'trend'},
        current_timestamp_ms=1710000000000,
        okx_sentiment={'okx_sentiment_score': 1.0},
        smart_money_context={'smart_money_flow_score': 2.0},
    )

    assert result is sentinel
    assert captured['symbol'] == 'TESTUSDT'
    assert captured['ticker']['quoteVolume'] == '1000'
    assert captured['market_regime'] == {'label': 'trend'}
    assert captured['current_timestamp_ms'] == 1710000000000
    assert captured['Candidate'] is mod.Candidate
    assert captured['build_trade_management_plan'] is mod.build_trade_management_plan
    assert captured['compute_atr'] is mod.compute_atr
    assert captured['compute_rsi'] is mod.compute_rsi
    assert captured['compute_vwap'] is mod.compute_vwap


def test_build_candidate_wrapper_accepts_legacy_keyword_contract(monkeypatch):
    sentinel = object()
    captured = {}

    def fake_build_candidate_impl(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(mod, 'build_candidate_impl', fake_build_candidate_impl)

    result = mod.build_candidate(
        symbol='TESTUSDT',
        ticker={'quoteVolume': '2000', 'priceChangePercent': '2.4'},
        klines_5m=[[0, 1, 2, 0.5, 1.5]] * 40,
        klines_15m=[[0, 1, 2, 0.5, 1.5]] * 30,
        klines_1h=[[0, 1, 2, 0.5, 1.5]] * 30,
        klines_4h=[[0, 1, 2, 0.5, 1.5]] * 30,
        meta=make_meta(),
        hot_rank=3,
        gainer_rank=5,
        risk_usdt=25.0,
        lookback_bars=12,
        swing_bars=6,
        min_5m_change_pct=3.5,
        min_quote_volume=1500000.0,
        stop_buffer_pct=0.01,
        max_rsi_5m=78.0,
        min_volume_multiple=1.8,
        max_distance_from_ema_pct=6.0,
        funding_rate=0.001,
        funding_rate_threshold=0.003,
        funding_rate_avg=0.0005,
        funding_rate_avg_threshold=0.0003,
        loser_rank=9,
        side='short',
        short_bias=True,
        oi_now=123.0,
        oi_5m_ago=100.0,
        oi_15m_ago=90.0,
        cvd_delta=-12.0,
        cvd_zscore=-2.5,
        market_regime={'label': 'distribution'},
        okx_sentiment_score=-1.5,
        okx_sentiment_acceleration=0.4,
        sector_resonance_score=0.7,
        smart_money_flow_score=-0.9,
    )

    assert result is sentinel
    assert captured['risk_usdt'] == 25.0
    assert captured['lookback_bars'] == 12
    assert captured['swing_bars'] == 6
    assert captured['min_5m_change_pct'] == 3.5
    assert captured['stop_buffer_pct'] == 0.01
    assert captured['loser_rank'] == 9
    assert captured['side'] == 'short'
    assert captured['market_regime'] == {'label': 'distribution'}
    assert captured['okx_sentiment']['okx_sentiment_score'] == -1.5
    assert captured['okx_sentiment']['okx_sentiment_acceleration'] == 0.4
    assert captured['okx_sentiment']['sector_resonance_score'] == 0.7
    assert captured['smart_money_context']['smart_money_flow_score'] == -0.9
    assert captured['microstructure_inputs']['short_bias'] is True
    assert captured['microstructure_inputs']['oi_now'] == 123.0
    assert captured['microstructure_inputs']['oi_5m_ago'] == 100.0
    assert captured['microstructure_inputs']['oi_15m_ago'] == 90.0
    assert captured['microstructure_inputs']['cvd_delta'] == -12.0
    assert captured['microstructure_inputs']['cvd_zscore'] == -2.5


def test_execution_module_matches_script_ensure_symbol_margin_type():
    client_a = Client()
    client_b = Client()

    script_result = mod.ensure_symbol_margin_type(client_a, 'TESTUSDT')
    extracted_result = exec_mod.ensure_symbol_margin_type(
        client_b,
        'TESTUSDT',
        binance_api_error=mod.BinanceAPIError,
    )

    assert script_result == extracted_result
    assert client_a.calls == client_b.calls == [
        ('/fapi/v1/marginType', {'symbol': 'TESTUSDT', 'marginType': 'ISOLATED'})
    ]


def test_execution_module_ensure_symbol_margin_type_falls_back_to_crossed_under_multi_assets_mode():
    class MultiAssetsClient:
        def __init__(self):
            self.calls = []

        def signed_post(self, path, params):
            self.calls.append((path, dict(params)))
            raise mod.BinanceAPIError("Binance API error 400: {'code': -4168, 'msg': 'Unable to adjust to isolated-margin mode under the Multi-Assets mode.'}")

    client = MultiAssetsClient()

    result = exec_mod.ensure_symbol_margin_type(
        client,
        'TESTUSDT',
        binance_api_error=mod.BinanceAPIError,
    )

    assert result['ok'] is True
    assert result['requested'] == 'ISOLATED'
    assert result['actual'] == 'CROSSED'
    assert result['applied'] is False
    assert result['multi_assets_mode'] is True
    assert result['fallback_reason'] == 'binance_multi_assets_mode_blocks_isolated'
    assert client.calls == [
        ('/fapi/v1/marginType', {'symbol': 'TESTUSDT', 'marginType': 'ISOLATED'})
    ]


def test_place_live_trade_extracted_module_matches_main_module(monkeypatch):
    candidate = make_candidate()
    meta = make_meta()
    args = make_args()

    monkeypatch.setattr(mod, 'log_runtime_event', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'place_stop_market_order', lambda *a, **k: {'orderId': 54321, 'clientOrderId': 'stop-1'})
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *a, **k: {'status': 'protected', 'expected_order_id': 54321})
    monkeypatch.setattr(mod, 'place_take_profit_market_order', lambda *a, **k: None)

    script_client = Client()
    extracted_client = Client()

    script_result = mod.place_live_trade(script_client, candidate, leverage=5, meta=meta, args=args)
    extracted_result = exec_mod.place_live_trade(
        extracted_client,
        candidate,
        leverage=5,
        meta=meta,
        args=args,
        binance_api_error=mod.BinanceAPIError,
        ensure_symbol_margin_type_fn=lambda client, symbol, margin_type='ISOLATED': exec_mod.ensure_symbol_margin_type(
            client,
            symbol,
            binance_api_error=mod.BinanceAPIError,
            margin_type=margin_type,
        ),
        round_step=mod.round_step,
        format_decimal=mod.format_decimal,
        should_send_position_side=mod.should_send_position_side,
        is_position_side_mode_error=mod.is_position_side_mode_error,
        mark_one_way_position_mode=mod.mark_one_way_position_mode,
        build_trade_management_plan=mod.build_trade_management_plan,
        fetch_open_positions=mod.fetch_open_positions,
        fetch_open_orders=mod.fetch_open_orders,
        fetch_open_algo_orders=mod.fetch_open_algo_orders,
        place_stop_market_order=mod.place_stop_market_order,
        place_take_profit_market_order=mod.place_take_profit_market_order,
        resolve_position_protection_status=mod.resolve_position_protection_status,
        recover_unknown_entry_order=mod.recover_unknown_entry_order,
        query_order=mod.query_order,
        log_runtime_event=mod.log_runtime_event,
        emit_notification=mod.emit_notification,
        normalize_position_side=mod.normalize_position_side,
        build_position_key=mod.build_position_key,
        position_row_matches_symbol_side=mod.position_row_matches_symbol_side,
        _to_float=mod._to_float,
        compute_execution_quality_size_adjustment=mod.compute_execution_quality_size_adjustment,
        asdict=mod.asdict,
        position_side_long=mod.POSITION_SIDE_LONG,
        time_module=time,
    )

    assert script_result == extracted_result
    assert script_client.calls == extracted_client.calls
    assert extracted_result['filled_quantity'] == 6.5
    assert extracted_result['trade_management_plan']['quantity'] == 6.5
    assert extracted_result['entry_order_feedback']['status'] == 'FILLED'



class ReduceOnlyRejectingClient:
    def __init__(self):
        self.calls = []
        self.position_mode = 'HEDGE'

    def signed_post(self, path, params):
        self.calls.append((path, dict(params)))
        if path == '/fapi/v1/algoOrder' and 'reduceOnly' in params:
            raise mod.BinanceAPIError("Binance API error 400: {'code': -1106, 'msg': \"Parameter 'reduceonly' sent when not required.\"}")
        return {'orderId': len(self.calls), 'clientAlgoId': 'algo-ok', 'params': dict(params)}


def test_place_stop_market_order_retries_algo_order_without_reduce_only_when_binance_rejects_it():
    client = ReduceOnlyRejectingClient()

    result = mod.place_stop_market_order(client, 'TESTUSDT', 98.0, 6.0, make_meta(), side=mod.POSITION_SIDE_LONG)

    assert result['clientAlgoId'] == 'algo-ok'
    assert len(client.calls) == 2
    assert client.calls[0][1]['reduceOnly'] == 'true'
    assert 'reduceOnly' not in client.calls[1][1]
    assert client.calls[1][1]['positionSide'] == mod.POSITION_SIDE_LONG


def test_place_take_profit_market_order_retries_algo_order_without_reduce_only_when_binance_rejects_it():
    client = ReduceOnlyRejectingClient()

    result = mod.place_take_profit_market_order(client, 'TESTUSDT', 105.0, 3.0, make_meta(), side=mod.POSITION_SIDE_SHORT)

    assert result['clientAlgoId'] == 'algo-ok'
    assert len(client.calls) == 2
    assert client.calls[0][1]['reduceOnly'] == 'true'
    assert 'reduceOnly' not in client.calls[1][1]
    assert client.calls[1][1]['positionSide'] == mod.POSITION_SIDE_SHORT


def test_execution_module_matches_script_resolve_position_protection_status(monkeypatch):
    positions = [{'symbol': 'DOGEUSDT', 'positionAmt': '5', 'positionSide': 'LONG'}]
    algo_orders = [{
        'clientAlgoId': 'expected-stop',
        'orderType': 'STOP_MARKET',
        'triggerPrice': '0.1234',
        'quantity': '5',
        'positionSide': 'LONG',
        'side': 'SELL',
        'symbol': 'DOGEUSDT',
    }]

    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: copy.deepcopy(positions))
    monkeypatch.setattr(mod, 'fetch_open_orders', lambda client, symbol: [])
    monkeypatch.setattr(mod, 'fetch_open_algo_orders', lambda client, symbol: copy.deepcopy(algo_orders))

    expected_stop_order = {
        'clientAlgoId': 'expected-stop',
        'triggerPrice': '0.1234',
        'quantity': '5',
        'positionSide': 'LONG',
        'side': 'SELL',
    }

    script_result = mod.resolve_position_protection_status(
        client=object(),
        symbol='DOGEUSDT',
        expected_stop_order=copy.deepcopy(expected_stop_order),
        side='LONG',
    )
    extracted_result = exec_mod.resolve_position_protection_status(
        client=object(),
        symbol='DOGEUSDT',
        expected_stop_order=copy.deepcopy(expected_stop_order),
        side='LONG',
        position_side_long=mod.POSITION_SIDE_LONG,
        normalize_position_side=mod.normalize_position_side,
        fetch_open_positions=mod.fetch_open_positions,
        fetch_open_orders=mod.fetch_open_orders,
        fetch_open_algo_orders=mod.fetch_open_algo_orders,
        position_row_matches_symbol_side=mod.position_row_matches_symbol_side,
        _to_float=mod._to_float,
    )

    assert script_result == extracted_result
    assert extracted_result['status'] == 'protected'
    assert extracted_result['matched_via'] == 'open_algo_orders'



def test_execution_module_matches_script_repair_missing_protection(monkeypatch):
    tracked = {'side': 'LONG', 'stop_price': 98.5}
    active_position = {'positionAmt': '3', 'positionSide': 'LONG'}
    meta = make_meta('DOGEUSDT')

    monkeypatch.setattr(mod, 'place_stop_market_order', lambda *a, **k: {'orderId': 777, 'clientOrderId': 'repair-stop'})

    script_result = mod.repair_missing_protection(
        client=object(),
        symbol='DOGEUSDT',
        tracked=copy.deepcopy(tracked),
        active_position=copy.deepcopy(active_position),
        meta=meta,
    )
    extracted_result = exec_mod.repair_missing_protection(
        client=object(),
        symbol='DOGEUSDT',
        tracked=copy.deepcopy(tracked),
        active_position=copy.deepcopy(active_position),
        meta=meta,
        normalize_position_side=mod.normalize_position_side,
        place_stop_market_order=mod.place_stop_market_order,
        fetch_exchange_meta=mod.fetch_exchange_meta,
        _to_float=mod._to_float,
    )

    assert script_result == extracted_result
    assert extracted_result['ok'] is True
    assert extracted_result['stop_order']['orderId'] == 777


def test_execution_module_matches_script_place_initial_stop_with_retries(monkeypatch):
    candidate = make_candidate()
    args = make_args()
    meta = make_meta()

    script_events = []
    extracted_events = []
    script_notifications = []
    extracted_notifications = []
    script_stop_attempts = []
    extracted_stop_attempts = []

    def build_positions():
        return iter([
            [],
            [{'symbol': 'TESTUSDT', 'positionAmt': '1.9', 'positionSide': 'LONG', 'entryPrice': '100.2'}],
        ])

    script_positions = build_positions()
    extracted_positions = build_positions()

    def script_fetch_open_positions(_client):
        return copy.deepcopy(next(script_positions))

    def extracted_fetch_open_positions(_client):
        return copy.deepcopy(next(extracted_positions))

    def script_stop(_client, _symbol, _stop_price, quantity, _meta, side=None):
        script_stop_attempts.append((quantity, side))
        if len(script_stop_attempts) == 1:
            raise RuntimeError('reduceOnly rejected')
        return {'orderId': 54321, 'clientOrderId': 'stop-2'}

    def extracted_stop(_client, _symbol, _stop_price, quantity, _meta, side=None):
        extracted_stop_attempts.append((quantity, side))
        if len(extracted_stop_attempts) == 1:
            raise RuntimeError('reduceOnly rejected')
        return {'orderId': 54321, 'clientOrderId': 'stop-2'}

    script_result = exec_mod.place_initial_stop_with_retries(
        client=object(),
        candidate=candidate,
        meta=meta,
        args=args,
        filled_quantity=6.5,
        position_side='LONG',
        fetch_open_positions=script_fetch_open_positions,
        position_row_matches_symbol_side=mod.position_row_matches_symbol_side,
        place_stop_market_order=script_stop,
        log_runtime_event=lambda event, payload: script_events.append((event, payload)),
        emit_notification=lambda _args, event, payload: script_notifications.append((event, payload)),
        binance_api_error=mod.BinanceAPIError,
        _to_float=mod._to_float,
        time_module=time,
    )

    extracted_result = exec_mod.place_initial_stop_with_retries(
        client=object(),
        candidate=candidate,
        meta=meta,
        args=args,
        filled_quantity=6.5,
        position_side='LONG',
        fetch_open_positions=extracted_fetch_open_positions,
        position_row_matches_symbol_side=mod.position_row_matches_symbol_side,
        place_stop_market_order=extracted_stop,
        log_runtime_event=lambda event, payload: extracted_events.append((event, payload)),
        emit_notification=lambda _args, event, payload: extracted_notifications.append((event, payload)),
        binance_api_error=mod.BinanceAPIError,
        _to_float=mod._to_float,
        time_module=time,
    )

    assert script_result == extracted_result == {'orderId': 54321, 'clientOrderId': 'stop-2'}
    assert script_stop_attempts == extracted_stop_attempts == [(6.5, 'LONG'), (1.9, 'LONG')]
    assert script_events == extracted_events
    assert [event for event, _payload in extracted_notifications] == [
        'initial_stop_place_attempt_failed',
        'initial_stop_place_attempt_succeeded',
    ]
    assert script_notifications == extracted_notifications


def test_execution_module_matches_script_place_initial_stop_with_retries_for_short_side_and_sleep(monkeypatch):
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=-14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=-1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=101.0,
        recent_swing_low=97.0,
        stop_price=102.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=32.0,
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
        side='SHORT',
        position_side='SHORT',
        setup_ready=True,
        trigger_fired=True,
    )
    args = make_args()
    args.initial_stop_max_attempts = 3
    args.initial_stop_retry_sleep_sec = 0.25
    meta = make_meta()

    script_events = []
    extracted_events = []
    script_notifications = []
    extracted_notifications = []
    script_stop_attempts = []
    extracted_stop_attempts = []
    script_sleep_calls = []
    extracted_sleep_calls = []

    def build_positions():
        return iter([
            [],
            [{'symbol': 'TESTUSDT', 'positionAmt': '-2.4', 'positionSide': 'SHORT', 'entryPrice': '100.2'}],
        ])

    script_positions = build_positions()
    extracted_positions = build_positions()

    def script_fetch_open_positions(_client):
        return copy.deepcopy(next(script_positions))

    def extracted_fetch_open_positions(_client):
        return copy.deepcopy(next(extracted_positions))

    def script_stop(_client, _symbol, _stop_price, quantity, _meta, side=None):
        script_stop_attempts.append((quantity, side))
        if len(script_stop_attempts) == 1:
            raise RuntimeError('reduceOnly rejected')
        return {'orderId': 54321, 'clientOrderId': 'stop-2'}

    def extracted_stop(_client, _symbol, _stop_price, quantity, _meta, side=None):
        extracted_stop_attempts.append((quantity, side))
        if len(extracted_stop_attempts) == 1:
            raise RuntimeError('reduceOnly rejected')
        return {'orderId': 54321, 'clientOrderId': 'stop-2'}

    class ScriptTimeModule:
        @staticmethod
        def sleep(seconds):
            script_sleep_calls.append(seconds)

    class ExtractedTimeModule:
        @staticmethod
        def sleep(seconds):
            extracted_sleep_calls.append(seconds)

    script_result = exec_mod.place_initial_stop_with_retries(
        client=object(),
        candidate=candidate,
        meta=meta,
        args=args,
        filled_quantity=6.5,
        position_side='SHORT',
        fetch_open_positions=script_fetch_open_positions,
        position_row_matches_symbol_side=mod.position_row_matches_symbol_side,
        place_stop_market_order=script_stop,
        log_runtime_event=lambda event, payload: script_events.append((event, payload)),
        emit_notification=lambda _args, event, payload: script_notifications.append((event, payload)),
        binance_api_error=mod.BinanceAPIError,
        _to_float=mod._to_float,
        time_module=ScriptTimeModule,
    )

    extracted_result = exec_mod.place_initial_stop_with_retries(
        client=object(),
        candidate=candidate,
        meta=meta,
        args=args,
        filled_quantity=6.5,
        position_side='SHORT',
        fetch_open_positions=extracted_fetch_open_positions,
        position_row_matches_symbol_side=mod.position_row_matches_symbol_side,
        place_stop_market_order=extracted_stop,
        log_runtime_event=lambda event, payload: extracted_events.append((event, payload)),
        emit_notification=lambda _args, event, payload: extracted_notifications.append((event, payload)),
        binance_api_error=mod.BinanceAPIError,
        _to_float=mod._to_float,
        time_module=ExtractedTimeModule,
    )

    assert script_result == extracted_result == {'orderId': 54321, 'clientOrderId': 'stop-2'}
    assert script_stop_attempts == extracted_stop_attempts == [(6.5, 'SHORT'), (2.4, 'SHORT')]
    assert script_sleep_calls == extracted_sleep_calls == [0.25]
    assert script_events == extracted_events
    assert [event for event, _payload in extracted_notifications] == [
        'initial_stop_place_attempt_failed',
        'initial_stop_place_attempt_succeeded',
    ]
    assert script_notifications == extracted_notifications


def test_execution_module_matches_script_place_initial_stop_with_retries_for_long_retry_chain(monkeypatch):
    candidate = make_candidate()
    args = make_args()
    args.initial_stop_max_attempts = 4
    meta = make_meta()

    script_events = []
    extracted_events = []
    script_notifications = []
    extracted_notifications = []
    script_stop_attempts = []
    extracted_stop_attempts = []

    def build_positions():
        return iter([
            [{'symbol': 'TESTUSDT', 'positionAmt': '4.8', 'positionSide': 'LONG', 'entryPrice': '100.2'}],
            [{'symbol': 'TESTUSDT', 'positionAmt': '3.1', 'positionSide': 'LONG', 'entryPrice': '100.2'}],
            [{'symbol': 'TESTUSDT', 'positionAmt': '1.4', 'positionSide': 'LONG', 'entryPrice': '100.2'}],
        ])

    script_positions = build_positions()
    extracted_positions = build_positions()

    def script_fetch_open_positions(_client):
        if script_stop_attempts:
            return copy.deepcopy(next(script_positions))
        return []

    def extracted_fetch_open_positions(_client):
        if extracted_stop_attempts:
            return copy.deepcopy(next(extracted_positions))
        return []

    def script_stop(_client, _symbol, _stop_price, quantity, _meta, side=None):
        script_stop_attempts.append((quantity, side))
        if len(script_stop_attempts) < 4:
            raise RuntimeError(f'reduceOnly rejected #{len(script_stop_attempts)}')
        return {'orderId': 98765, 'clientOrderId': 'stop-4'}

    def extracted_stop(_client, _symbol, _stop_price, quantity, _meta, side=None):
        extracted_stop_attempts.append((quantity, side))
        if len(extracted_stop_attempts) < 4:
            raise RuntimeError(f'reduceOnly rejected #{len(extracted_stop_attempts)}')
        return {'orderId': 98765, 'clientOrderId': 'stop-4'}

    script_result = exec_mod.place_initial_stop_with_retries(
        client=object(),
        candidate=candidate,
        meta=meta,
        args=args,
        filled_quantity=6.5,
        position_side='LONG',
        fetch_open_positions=script_fetch_open_positions,
        position_row_matches_symbol_side=mod.position_row_matches_symbol_side,
        place_stop_market_order=script_stop,
        log_runtime_event=lambda event, payload: script_events.append((event, payload)),
        emit_notification=lambda _args, event, payload: script_notifications.append((event, payload)),
        binance_api_error=mod.BinanceAPIError,
        _to_float=mod._to_float,
        time_module=time,
    )

    extracted_result = exec_mod.place_initial_stop_with_retries(
        client=object(),
        candidate=candidate,
        meta=meta,
        args=args,
        filled_quantity=6.5,
        position_side='LONG',
        fetch_open_positions=extracted_fetch_open_positions,
        position_row_matches_symbol_side=mod.position_row_matches_symbol_side,
        place_stop_market_order=extracted_stop,
        log_runtime_event=lambda event, payload: extracted_events.append((event, payload)),
        emit_notification=lambda _args, event, payload: extracted_notifications.append((event, payload)),
        binance_api_error=mod.BinanceAPIError,
        _to_float=mod._to_float,
        time_module=time,
    )

    assert script_result == extracted_result == {'orderId': 98765, 'clientOrderId': 'stop-4'}
    assert script_stop_attempts == extracted_stop_attempts == [
        (6.5, 'LONG'),
        (4.8, 'LONG'),
        (3.1, 'LONG'),
        (1.4, 'LONG'),
    ]
    assert script_events == extracted_events
    assert [event for event, _payload in extracted_notifications] == [
        'initial_stop_place_attempt_failed',
        'initial_stop_place_attempt_failed',
        'initial_stop_place_attempt_failed',
        'initial_stop_place_attempt_succeeded',
    ]
    assert script_notifications == extracted_notifications


def test_execution_module_matches_script_place_initial_stop_with_retries_for_short_longer_retry_chain(monkeypatch):
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=100.0,
        price_change_pct_24h=-14.0,
        quote_volume_24h=1_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=-1.2,
        acceleration_ratio_5m_vs_15m=1.1,
        breakout_level=101.0,
        recent_swing_low=97.0,
        stop_price=102.0,
        quantity=10.0,
        risk_per_unit=2.0,
        recommended_leverage=3,
        rsi_5m=32.0,
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
        side='SHORT',
        position_side='SHORT',
        setup_ready=True,
        trigger_fired=True,
    )
    args = make_args()
    args.initial_stop_max_attempts = 4
    meta = make_meta()

    script_events = []
    extracted_events = []
    script_notifications = []
    extracted_notifications = []
    script_stop_attempts = []
    extracted_stop_attempts = []

    def build_positions():
        return iter([
            [{'symbol': 'TESTUSDT', 'positionAmt': '-4.4', 'positionSide': 'SHORT', 'entryPrice': '100.2'}],
            [{'symbol': 'TESTUSDT', 'positionAmt': '-2.7', 'positionSide': 'SHORT', 'entryPrice': '100.2'}],
            [{'symbol': 'TESTUSDT', 'positionAmt': '-1.2', 'positionSide': 'SHORT', 'entryPrice': '100.2'}],
        ])

    script_positions = build_positions()
    extracted_positions = build_positions()

    def script_fetch_open_positions(_client):
        if script_stop_attempts:
            return copy.deepcopy(next(script_positions))
        return []

    def extracted_fetch_open_positions(_client):
        if extracted_stop_attempts:
            return copy.deepcopy(next(extracted_positions))
        return []

    def script_stop(_client, _symbol, _stop_price, quantity, _meta, side=None):
        script_stop_attempts.append((quantity, side))
        if len(script_stop_attempts) < 4:
            raise RuntimeError(f'reduceOnly rejected #{len(script_stop_attempts)}')
        return {'orderId': 98766, 'clientOrderId': 'short-stop-4'}

    def extracted_stop(_client, _symbol, _stop_price, quantity, _meta, side=None):
        extracted_stop_attempts.append((quantity, side))
        if len(extracted_stop_attempts) < 4:
            raise RuntimeError(f'reduceOnly rejected #{len(extracted_stop_attempts)}')
        return {'orderId': 98766, 'clientOrderId': 'short-stop-4'}

    script_result = exec_mod.place_initial_stop_with_retries(
        client=object(),
        candidate=candidate,
        meta=meta,
        args=args,
        filled_quantity=6.5,
        position_side='SHORT',
        fetch_open_positions=script_fetch_open_positions,
        position_row_matches_symbol_side=mod.position_row_matches_symbol_side,
        place_stop_market_order=script_stop,
        log_runtime_event=lambda event, payload: script_events.append((event, payload)),
        emit_notification=lambda _args, event, payload: script_notifications.append((event, payload)),
        binance_api_error=mod.BinanceAPIError,
        _to_float=mod._to_float,
        time_module=time,
    )

    extracted_result = exec_mod.place_initial_stop_with_retries(
        client=object(),
        candidate=candidate,
        meta=meta,
        args=args,
        filled_quantity=6.5,
        position_side='SHORT',
        fetch_open_positions=extracted_fetch_open_positions,
        position_row_matches_symbol_side=mod.position_row_matches_symbol_side,
        place_stop_market_order=extracted_stop,
        log_runtime_event=lambda event, payload: extracted_events.append((event, payload)),
        emit_notification=lambda _args, event, payload: extracted_notifications.append((event, payload)),
        binance_api_error=mod.BinanceAPIError,
        _to_float=mod._to_float,
        time_module=time,
    )

    assert script_result == extracted_result == {'orderId': 98766, 'clientOrderId': 'short-stop-4'}
    assert script_stop_attempts == extracted_stop_attempts == [
        (6.5, 'SHORT'),
        (4.4, 'SHORT'),
        (2.7, 'SHORT'),
        (1.2, 'SHORT'),
    ]
    assert script_events == extracted_events
    assert [event for event, _payload in extracted_notifications] == [
        'initial_stop_place_attempt_failed',
        'initial_stop_place_attempt_failed',
        'initial_stop_place_attempt_failed',
        'initial_stop_place_attempt_succeeded',
    ]
    assert script_notifications == extracted_notifications


    candidate = make_candidate()
    meta = make_meta()
    args = make_args()

    monkeypatch.setattr(mod, 'log_runtime_event', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *a, **k: {'status': 'protected', 'expected_order_id': 54321})
    monkeypatch.setattr(mod, 'place_take_profit_market_order', lambda *a, **k: None)

    script_stop_attempts = []
    extracted_stop_attempts = []

    def build_positions():
        return iter([
            [{'symbol': 'TESTUSDT', 'positionAmt': '1.9', 'positionSide': 'LONG', 'entryPrice': '100.2'}],
            [{'symbol': 'TESTUSDT', 'positionAmt': '1.9', 'positionSide': 'LONG', 'entryPrice': '100.2'}],
        ])

    script_positions = build_positions()
    extracted_positions = build_positions()

    def script_fetch_open_positions(_client):
        if script_stop_attempts:
            return copy.deepcopy(next(script_positions))
        return []

    monkeypatch.setattr(mod, 'fetch_open_positions', script_fetch_open_positions)
    monkeypatch.setattr(mod, 'fetch_open_orders', lambda client, symbol: [])
    monkeypatch.setattr(mod, 'fetch_open_algo_orders', lambda client, symbol: [])

    def script_stop(*_args, **kwargs):
        quantity = _args[3]
        side = kwargs.get('side')
        script_stop_attempts.append((quantity, side))
        if len(script_stop_attempts) == 1:
            raise RuntimeError('reduceOnly rejected')
        return {'orderId': 54321, 'clientOrderId': 'stop-2'}

    monkeypatch.setattr(mod, 'place_stop_market_order', script_stop)
    monkeypatch.setattr(
        mod,
        'resolve_position_protection_status',
        lambda *a, **k: {'status': 'protected', 'expected_order_id': 54321},
    )

    script_client = Client()
    script_result = mod.place_live_trade(script_client, candidate, leverage=5, meta=meta, args=args)

    def extracted_fetch_open_positions(_client):
        if extracted_stop_attempts:
            return copy.deepcopy(next(extracted_positions))
        return []

    def extracted_stop(_client, _symbol, _stop_price, quantity, _meta, side=None):
        extracted_stop_attempts.append((quantity, side))
        if len(extracted_stop_attempts) == 1:
            raise RuntimeError('reduceOnly rejected')
        return {'orderId': 54321, 'clientOrderId': 'stop-2'}

    def extracted_resolve_protection(*_args, **_kwargs):
        next(extracted_positions)
        return {'status': 'protected', 'expected_order_id': 54321}

    extracted_client = Client()
    extracted_result = exec_mod.place_live_trade(
        extracted_client,
        candidate,
        leverage=5,
        meta=meta,
        args=args,
        binance_api_error=mod.BinanceAPIError,
        ensure_symbol_margin_type_fn=lambda client, symbol, margin_type='ISOLATED': exec_mod.ensure_symbol_margin_type(
            client,
            symbol,
            binance_api_error=mod.BinanceAPIError,
            margin_type=margin_type,
        ),
        round_step=mod.round_step,
        format_decimal=mod.format_decimal,
        should_send_position_side=mod.should_send_position_side,
        is_position_side_mode_error=mod.is_position_side_mode_error,
        mark_one_way_position_mode=mod.mark_one_way_position_mode,
        build_trade_management_plan=mod.build_trade_management_plan,
        fetch_open_positions=extracted_fetch_open_positions,
        fetch_open_orders=mod.fetch_open_orders,
        fetch_open_algo_orders=mod.fetch_open_algo_orders,
        place_stop_market_order=extracted_stop,
        resolve_position_protection_status=extracted_resolve_protection,
        recover_unknown_entry_order=mod.recover_unknown_entry_order,
        query_order=mod.query_order,
        log_runtime_event=mod.log_runtime_event,
        emit_notification=mod.emit_notification,
        normalize_position_side=mod.normalize_position_side,
        build_position_key=mod.build_position_key,
        position_row_matches_symbol_side=mod.position_row_matches_symbol_side,
        _to_float=mod._to_float,
        compute_execution_quality_size_adjustment=mod.compute_execution_quality_size_adjustment,
        asdict=mod.asdict,
        position_side_long=mod.POSITION_SIDE_LONG,
        time_module=time,
        place_take_profit_market_order=mod.place_take_profit_market_order,
    )

    assert script_result == extracted_result
    assert script_client.calls == extracted_client.calls
    assert script_stop_attempts == extracted_stop_attempts == [(6.5, 'LONG'), (1.9, 'LONG')]
    assert extracted_result['stop_order']['orderId'] == 54321


def test_execution_module_matches_script_monitor_live_trade(monkeypatch):
    store_a = mod.RuntimeStateStore(tempfile.mkdtemp())
    store_b = mod.RuntimeStateStore(tempfile.mkdtemp())
    symbol = 'TESTUSDT'
    position_key = 'TESTUSDT:LONG'
    base_positions = {
        position_key: {
            'symbol': symbol,
            'side': 'LONG',
            'position_key': position_key,
            'status': 'monitoring',
            'quantity': 1.0,
            'remaining_quantity': 1.0,
            '_debug_current_price': 111.0,
            '_debug_ema5m': 110.0,
            '_debug_trailing_reference': 112.0,
        }
    }
    store_a.save_json('positions', copy.deepcopy(base_positions))
    store_b.save_json('positions', copy.deepcopy(base_positions))

    args = make_args()
    trade = make_trade('LONG')
    meta = make_meta(symbol)

    monkeypatch.setattr(mod, 'fetch_klines', lambda *a, **k: [[0, 0, 0, 0, 100.0]] * 21)
    monkeypatch.setattr(mod, 'extract_closes', lambda klines: [100.0] * len(klines))
    monkeypatch.setattr(mod, 'extract_highs', lambda klines: [112.0] * len(klines))
    monkeypatch.setattr(mod, 'extract_lows', lambda klines: [99.0] * len(klines))
    monkeypatch.setattr(mod, 'evaluate_management_actions', lambda *a, **k: [{'type': 'take_profit_1', 'close_qty': 1.0, 'exit_reason': 'tp1'}])
    monkeypatch.setattr(mod, 'log_runtime_event', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: None)
    monkeypatch.setattr(mod.time, 'sleep', lambda *_a, **_k: None)

    def fake_apply(_client, _symbol, _meta, state, action, _active_stop_order):
        action['exit_reason'] = 'tp1'
        state.remaining_quantity = 0.0
        state.tp1_hit = True
        return state, None, {'reduce_order': {'orderId': 888, 'avgPrice': '111.0'}}

    monkeypatch.setattr(mod, 'apply_management_action', fake_apply)

    script_result = mod.monitor_live_trade(client=object(), symbol=symbol, meta=meta, args=args, trade=copy.deepcopy(trade), store=store_a)
    extracted_result = exec_mod.monitor_live_trade(
        client=object(),
        symbol=symbol,
        meta=meta,
        args=args,
        trade=copy.deepcopy(trade),
        store=store_b,
        trade_management_plan_type=mod.TradeManagementPlan,
        trade_management_state_type=mod.TradeManagementState,
        position_side_long=mod.POSITION_SIDE_LONG,
        position_side_short=mod.POSITION_SIDE_SHORT,
        binance_api_error=mod.BinanceAPIError,
        _to_float=mod._to_float,
        normalize_position_side=mod.normalize_position_side,
        position_side_to_trade_side=mod.position_side_to_trade_side,
        build_position_key=mod.build_position_key,
        get_position_by_symbol_side=mod.get_position_by_symbol_side,
        build_trade_analytics_snapshot=mod.build_trade_analytics_snapshot,
        upsert_position_record=mod.upsert_position_record,
        materialize_positions_state=mod.materialize_positions_state,
        asdict=mod.asdict,
        log_runtime_event=mod.log_runtime_event,
        emit_notification=mod.emit_notification,
        fetch_klines=mod.fetch_klines,
        extract_closes=mod.extract_closes,
        extract_highs=mod.extract_highs,
        extract_lows=mod.extract_lows,
        resolve_monitor_current_price=mod.resolve_monitor_current_price,
        evaluate_management_actions=mod.evaluate_management_actions,
        update_trade_progress_metrics=mod.update_trade_progress_metrics,
        apply_management_action=mod.apply_management_action,
        resolve_reduce_order_exit_price=mod.resolve_reduce_order_exit_price,
        compute_trade_realized_r_increment=mod.compute_trade_realized_r_increment,
        score_to_decile_label=mod.score_to_decile_label,
        resolve_trigger_class=mod.resolve_trigger_class,
        utc_now=mod._utc_now,
        isoformat_utc=mod._isoformat_utc,
        time_module=mod.time,
    )

    events_a = store_a.read_events(limit=20)
    events_b = store_b.read_events(limit=20)

    normalized_events_a = exec_mod.normalize_monitor_event_rows(events_a)
    normalized_events_b = exec_mod.normalize_monitor_event_rows(events_b)

    trade_invalidated_a = next((row for row in normalized_events_a if row.get('event_type') == 'trade_invalidated'), None)
    trade_invalidated_b = next((row for row in normalized_events_b if row.get('event_type') == 'trade_invalidated'), None)
    assert trade_invalidated_a is not None
    assert trade_invalidated_b is not None

    assert script_result == extracted_result
    assert store_a.load_json('positions', {}) == store_b.load_json('positions', {})
    assert store_a.load_json('monitor_debug', {}) == store_b.load_json('monitor_debug', {})
    assert [row.get('event_type') for row in normalized_events_a] == [row.get('event_type') for row in normalized_events_b]
    assert normalized_events_a[-1] == normalized_events_b[-1]
    assert extracted_result['status'] == 'closed'
    assert extracted_result['exit_reason'] == 'tp1'
    assert extracted_result['realized_r'] == 2.2


def test_normalize_monitor_event_rows_strips_runtime_noise_fields():
    rows = [
        {
            'event_type': 'entry_filled',
            'recorded_at': '2026-05-10T00:00:00Z',
            'consumer': 'script',
            'payload': {
                'symbol': 'TESTUSDT',
                'recorded_at': '2026-05-10T00:00:00Z',
                'opened_at': '2026-05-10T00:00:01Z',
            },
        },
        {
            'event_type': 'trade_invalidated',
            'time_in_trade_minutes': 3.14,
            'closed_at': '2026-05-10T00:02:00Z',
            'payload': {
                'exit_reason': 'tp1',
                'closed_at': '2026-05-10T00:02:00Z',
            },
        },
    ]

    assert exec_mod.normalize_monitor_event_rows(rows) == [
        {
            'event_type': 'entry_filled',
            'payload': {
                'symbol': 'TESTUSDT',
            },
        },
        {
            'event_type': 'trade_invalidated',
            'payload': {
                'exit_reason': 'tp1',
            },
        },
    ]


def test_execution_module_matches_script_start_trade_monitor_thread(monkeypatch):
    assert hasattr(exec_mod, 'start_trade_monitor_thread')


def test_execution_module_monitor_live_trade_supports_injected_position_state(monkeypatch):
    symbol = 'TESTUSDT'
    position_key = 'TESTUSDT:LONG'
    args = make_args()
    trade = make_trade('LONG')
    meta = make_meta(symbol)
    shared_store = mod.RuntimeStateStore(tempfile.mkdtemp())
    seeded_positions = {
        position_key: {
            'symbol': symbol,
            'side': 'LONG',
            'position_key': position_key,
            'status': 'monitoring',
            'quantity': 1.0,
            'remaining_quantity': 1.0,
            '_debug_current_price': 111.0,
            '_debug_ema5m': 110.0,
            '_debug_trailing_reference': 112.0,
        }
    }
    shared_store.save_json('positions', copy.deepcopy(seeded_positions))

    monkeypatch.setattr(mod, 'fetch_klines', lambda *a, **k: [[0, 0, 0, 0, 100.0]] * 21)
    monkeypatch.setattr(mod, 'extract_closes', lambda klines: [100.0] * len(klines))
    monkeypatch.setattr(mod, 'extract_highs', lambda klines: [112.0] * len(klines))
    monkeypatch.setattr(mod, 'extract_lows', lambda klines: [99.0] * len(klines))
    monkeypatch.setattr(mod, 'evaluate_management_actions', lambda *a, **k: [{'type': 'take_profit_1', 'close_qty': 1.0, 'exit_reason': 'tp1'}])
    monkeypatch.setattr(mod, 'log_runtime_event', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: None)
    monkeypatch.setattr(mod.time, 'sleep', lambda *_a, **_k: None)

    def fake_apply(_client, _symbol, _meta, state, action, _active_stop_order):
        action['exit_reason'] = 'tp1'
        state.remaining_quantity = 0.0
        state.tp1_hit = True
        return state, None, {'reduce_order': {'orderId': 888, 'avgPrice': '111.0'}}

    monkeypatch.setattr(mod, 'apply_management_action', fake_apply)

    initial_positions = copy.deepcopy(seeded_positions)
    result = exec_mod.monitor_live_trade(
        client=object(),
        symbol=symbol,
        meta=meta,
        args=args,
        trade=copy.deepcopy(trade),
        store=shared_store,
        initial_positions_state=initial_positions,
        trade_management_plan_type=mod.TradeManagementPlan,
        trade_management_state_type=mod.TradeManagementState,
        position_side_long=mod.POSITION_SIDE_LONG,
        position_side_short=mod.POSITION_SIDE_SHORT,
        binance_api_error=mod.BinanceAPIError,
        _to_float=mod._to_float,
        normalize_position_side=mod.normalize_position_side,
        position_side_to_trade_side=mod.position_side_to_trade_side,
        build_position_key=mod.build_position_key,
        get_position_by_symbol_side=mod.get_position_by_symbol_side,
        build_trade_analytics_snapshot=mod.build_trade_analytics_snapshot,
        upsert_position_record=mod.upsert_position_record,
        materialize_positions_state=mod.materialize_positions_state,
        asdict=mod.asdict,
        log_runtime_event=mod.log_runtime_event,
        emit_notification=mod.emit_notification,
        fetch_klines=mod.fetch_klines,
        extract_closes=mod.extract_closes,
        extract_highs=mod.extract_highs,
        extract_lows=mod.extract_lows,
        resolve_monitor_current_price=mod.resolve_monitor_current_price,
        evaluate_management_actions=mod.evaluate_management_actions,
        update_trade_progress_metrics=mod.update_trade_progress_metrics,
        apply_management_action=mod.apply_management_action,
        resolve_reduce_order_exit_price=mod.resolve_reduce_order_exit_price,
        compute_trade_realized_r_increment=mod.compute_trade_realized_r_increment,
        score_to_decile_label=mod.score_to_decile_label,
        resolve_trigger_class=mod.resolve_trigger_class,
        utc_now=mod._utc_now,
        isoformat_utc=mod._isoformat_utc,
        time_module=mod.time,
    )

    assert result['status'] == 'closed'
    assert result['exit_reason'] == 'tp1'
    persisted_positions = shared_store.load_json('positions', {})
    assert persisted_positions == {}
    assert shared_store.load_json('monitor_debug', {})['position_state_source'] == 'injected'
    assert initial_positions[position_key]['status'] == 'monitoring'



def test_execution_module_requires_monitor_live_trade_export():
    assert hasattr(exec_mod, 'monitor_live_trade')


def test_execution_module_requires_place_live_trade_export():
    assert hasattr(exec_mod, 'place_live_trade')
