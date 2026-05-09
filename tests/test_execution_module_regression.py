import argparse
import copy
import importlib.util
import pathlib
import sys
import tempfile
import time
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

MAIN_MODULE_PATH = SCRIPTS_DIR / 'binance_futures_momentum_long.py'
EXECUTION_MODULE_PATH = SCRIPTS_DIR / 'execution_engine.py'


def _load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mod = _load_module('strategy_execution_mod', MAIN_MODULE_PATH)
exec_mod = _load_module('execution_engine_mod', EXECUTION_MODULE_PATH)


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
        breakeven_r=1.0,
        profile='test-profile',
        margin_type='ISOLATED',
        disable_notify=True,
        notify_target='',
        max_monitor_cycles=1,
        monitor_poll_interval_sec=0,
        trailing_buffer_pct=0.01,
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


def test_place_live_trade_extracted_module_matches_main_module(monkeypatch):
    candidate = make_candidate()
    meta = make_meta()
    args = make_args()

    monkeypatch.setattr(mod, 'log_runtime_event', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'place_stop_market_order', lambda *a, **k: {'orderId': 54321, 'clientOrderId': 'stop-1'})
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *a, **k: {'status': 'protected', 'expected_order_id': 54321})

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

    def normalize_event_rows(rows):
        normalized = []
        for row in rows:
            cleaned = {k: v for k, v in row.items() if k not in {'recorded_at', 'opened_at', 'closed_at'}}
            payload = cleaned.get('payload')
            if isinstance(payload, dict):
                payload = {k: v for k, v in payload.items() if k not in {'recorded_at', 'opened_at', 'closed_at'}}
                cleaned['payload'] = payload
            normalized.append(cleaned)
        return normalized

    normalized_events_a = normalize_event_rows(events_a)
    normalized_events_b = normalize_event_rows(events_b)

    assert script_result == extracted_result
    assert store_a.load_json('positions', {}) == store_b.load_json('positions', {})
    assert store_a.load_json('monitor_debug', {}) == store_b.load_json('monitor_debug', {})
    assert normalized_events_a == normalized_events_b
    assert extracted_result['status'] == 'closed'
    assert extracted_result['exit_reason'] == 'tp1'
    assert extracted_result['realized_r'] == 2.2


def test_execution_module_matches_script_start_trade_monitor_thread(monkeypatch):
    assert hasattr(exec_mod, 'start_trade_monitor_thread')


def test_execution_module_requires_monitor_live_trade_export():
    assert hasattr(exec_mod, 'monitor_live_trade')


def test_execution_module_requires_place_live_trade_export():
    assert hasattr(exec_mod, 'place_live_trade')
