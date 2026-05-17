import dataclasses
import datetime
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
SCRIPT_PATH = SCRIPTS_DIR / 'binance_futures_momentum_long.py'


def load_module():
    spec = importlib.util.spec_from_file_location('bfml_runtime_test', SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class DummyClient:
    def signed_post(self, path, params):
        if path == '/fapi/v1/listenKey':
            return {'listenKey': 'dummy-listen-key'}
        raise AssertionError(f'unexpected signed_post path: {path}')

    def signed_put(self, path, params):
        if path == '/fapi/v1/listenKey':
            return {'result': 'ok', 'listenKey': params.get('listenKey')}
        raise AssertionError(f'unexpected signed_put path: {path}')

    def signed_delete(self, path, params):
        if path == '/fapi/v1/listenKey':
            return {'result': 'closed', 'listenKey': params.get('listenKey')}
        raise AssertionError(f'unexpected signed_delete path: {path}')


class DummyStore:
    def __init__(self):
        self.saved = []
        self.events = []
        self.json_state = {}

    def load_json(self, name, default=None):
        return self.json_state.get(name, default)

    def save_json(self, name, payload):
        self.json_state[name] = payload
        self.saved.append((name, payload))

    def append_event(self, event_type, payload):
        row = {'event_type': event_type, **(payload or {})}
        self.events.append(row)
        return row



def make_args(**overrides):
    base = dict(
        runtime_state_dir='/root/.hermes/binance-futures-momentum-long/runtime-state',
        halt_on_orphan_position=False,
        live=False,
        reconcile_only=False,
        daily_max_loss_usdt=0.0,
        max_consecutive_losses=0,
        symbol_cooldown_minutes=0,
        scan_only=False,
        max_open_positions=1,
        leverage=3,
        auto_loop=False,
        disable_notify=True,
        notify_target='',
        telegram_bot_token_env='TELEGRAM_BOT_TOKEN',
        output_format='json',
        require_book_ticker_ws=False,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_run_loop_live_path_uses_binance_only_without_okx_args(monkeypatch):
    mod = load_module()
    store = DummyStore()
    candidate = SimpleNamespace(symbol='DOGEUSDT', recommended_leverage=3)
    live_execution = {'symbol': 'DOGEUSDT', 'side': 'LONG', 'quantity': 10.0}
    persisted_positions = {'DOGEUSDT:LONG': {'symbol': 'DOGEUSDT', 'side': 'LONG'}}

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'normalized_risk_state': {}})
    monkeypatch.setattr(mod, 'evaluate_portfolio_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidate_count': 1, 'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': {'symbol': 'DOGEUSDT'}}))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'persist_live_open_position', lambda store, candidate, execution: (persisted_positions, 'DOGEUSDT:LONG'))
    monkeypatch.setattr(mod, 'monitor_live_trade', lambda *a, **k: {'status': 'monitored'})

    def fake_place_live_trade(client, picked_candidate, requested_leverage, meta, args):
        assert picked_candidate.symbol == 'DOGEUSDT'
        assert requested_leverage == 3
        assert meta == {'symbol': 'DOGEUSDT'}
        assert not hasattr(args, 'okx_simulated_trading')
        assert not hasattr(args, 'okx_base_url')
        return live_execution

    monkeypatch.setattr(mod, 'place_live_trade', fake_place_live_trade)

    result = mod.run_loop(DummyClient(), make_args(live=True))

    assert result['ok'] is True
    cycle = result['cycles'][0]
    assert cycle['live_execution'] == live_execution
    assert cycle['trade_management'] == {'status': 'monitored'}


def test_run_loop_scan_only_returns_before_live_execution(monkeypatch):
    mod = load_module()
    store = DummyStore()
    candidate = SimpleNamespace(symbol='DOGEUSDT')

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': {'symbol': 'DOGEUSDT'}}))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})

    def unexpected(*args, **kwargs):
        raise AssertionError('live path should not be called in scan-only mode')

    monkeypatch.setattr(mod, 'fetch_open_positions', unexpected)
    monkeypatch.setattr(mod, 'place_live_trade', unexpected)
    monkeypatch.setattr(mod, 'monitor_live_trade', unexpected)

    result = mod.run_loop(DummyClient(), make_args(scan_only=True))
    assert result['ok'] is True
    assert len(result['cycles']) == 1
    cycle = result['cycles'][0]
    assert cycle['scan']['candidates'] == ['DOGEUSDT']
    assert 'live_execution' not in cycle


def test_run_loop_persists_wait_trigger_state_for_setup_ready_candidate(monkeypatch):
    mod = load_module()
    store = DummyStore()
    candidate = SimpleNamespace(
        symbol='DOGEUSDT',
        side='LONG',
        position_side='LONG',
        setup_ready=True,
        trigger_fired=False,
        candidate_stage='setup',
        score=88.0,
        state='setup',
        alert_tier='critical',
        must_pass_flags={},
        quality_score=0.0,
        execution_priority_score=0.0,
        entry_distance_from_breakout_pct=0.0,
        entry_distance_from_vwap_pct=0.0,
        candle_extension_pct=0.0,
        recent_3bar_runup_pct=0.0,
        overextension_flag=False,
        entry_pattern='',
        trend_regime='',
        liquidity_grade='A',
        setup_missing=[],
        trigger_missing=['waiting_breakout'],
        trade_missing=[],
        trigger_confirmation_flags={},
        trigger_confirmation_count=0,
        trigger_min_confirmations=2,
        portfolio_narrative_bucket='',
        portfolio_correlation_group='',
        expected_slippage_pct=0.0,
        book_depth_fill_ratio=1.0,
        cvd_delta=0.0,
        cvd_zscore=0.0,
        oi_change_pct_5m=0.0,
        oi_change_pct_15m=0.0,
        spread_bps=1.0,
        orderbook_slope=0.0,
        cancel_rate=0.0,
    )

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'normalized_risk_state': {}})
    monkeypatch.setattr(mod, 'evaluate_portfolio_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidate_count': 1, 'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': {'symbol': 'DOGEUSDT'}}))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})

    result = mod.run_loop(DummyClient(), make_args(scan_only=True, auto_loop=True, profile='state-machine-test'))

    assert result['ok'] is True
    loop_state = store.json_state['auto_loop_state']
    assert loop_state['state'] == 'WAIT_TRIGGER'
    assert loop_state['profile'] == 'state-machine-test'
    assert loop_state['active_candidate']['symbol'] == 'DOGEUSDT'
    assert loop_state['active_candidate']['side'] == 'LONG'
    assert loop_state['active_candidate']['setup_ready'] is True
    assert loop_state['active_candidate']['trigger_fired'] is False
    assert loop_state['active_candidate']['candidate_stage'] == 'setup'
    assert loop_state['active_candidate']['trigger_missing'] == ['waiting_breakout']



def test_run_loop_auto_loop_resumes_managing_open_position_before_scan(monkeypatch):
    mod = load_module()
    store = DummyStore()
    store.json_state['positions'] = {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_side': 'LONG',
            'position_key': 'DOGEUSDT:LONG',
            'status': 'monitoring',
            'remaining_quantity': 12.5,
            'protection_status': 'protected',
        }
    }

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'run_auto_loop_book_ticker_websocket_monitor_core', lambda **kwargs: {'summary': {'status': 'healthy'}, 'health': {'status': 'healthy'}})
    monkeypatch.setattr(mod, 'run_auto_loop_user_data_stream_monitor', lambda **kwargs: {'monitor': {'status': 'healthy'}})

    scan_calls = []
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'normalized_risk_state': {}})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: scan_calls.append(True) or ({'candidate_count': 0, 'candidates': []}, None, {}))

    result = mod.run_loop(DummyClient(), make_args(live=True, auto_loop=True, profile='state-machine-test'))

    assert result['ok'] is True
    cycle = result['cycles'][0]
    assert cycle['resident_resume']['state'] == 'MANAGING'
    assert cycle['resident_resume']['position_key'] == 'DOGEUSDT:LONG'
    assert scan_calls == [True]
    loop_state = store.json_state['auto_loop_state']
    assert loop_state['state'] == 'SCAN'
    assert loop_state['reason'] == 'no_candidate'


def test_run_loop_scan_only_persists_candidate_selected_event(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = mod.Candidate(
        symbol='DOGEUSDT',
        last_price=0.142,
        price_change_pct_24h=8.4,
        quote_volume_24h=75_000_000.0,
        hot_rank=1,
        gainer_rank=2,
        funding_rate=-0.0002,
        funding_rate_avg=-0.0001,
        recent_5m_change_pct=1.8,
        acceleration_ratio_5m_vs_15m=1.4,
        breakout_level=0.14,
        recent_swing_low=0.136,
        stop_price=0.137,
        quantity=1000.0,
        risk_per_unit=0.005,
        recommended_leverage=5,
        rsi_5m=68.0,
        volume_multiple=2.2,
        distance_from_ema20_5m_pct=1.1,
        distance_from_vwap_15m_pct=0.8,
        higher_tf_summary={'1h': 'trend_up'},
        score=82.6,
        reasons=['candidate_selected'],
        side='LONG',
        position_side='LONG',
        state='launch',
        state_reasons=['launch_breakout'],
        alert_tier='critical',
        position_size_pct=3.3,
        regime_label='risk_on',
        regime_multiplier=1.1,
        side_risk_multiplier=1.0,
        quality_score=18.2,
        execution_priority_score=9.1,
        setup_ready=True,
        trigger_fired=True,
        candidate_stage='launch',
        expected_slippage_pct=0.04,
        book_depth_fill_ratio=0.92,
        spread_bps=2.1,
        orderbook_slope=1.3,
        cancel_rate=0.04,
        portfolio_narrative_bucket='meme',
        portfolio_correlation_group='dog-family',
    )

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'normalized_risk_state': {}})
    monkeypatch.setattr(mod, 'evaluate_portfolio_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({
        'candidate_count': 1,
        'candidates': ['DOGEUSDT'],
        'market_regime': {'label': 'risk_on', 'score_multiplier': 1.1, 'reasons': ['btc_above_ema20']},
    }, candidate, {'DOGEUSDT': {'symbol': 'DOGEUSDT'}}))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})

    result = mod.run_loop(DummyClient(), make_args(scan_only=True, runtime_state_dir=str(tmp_path), profile='scan-only'))

    assert result['ok'] is True
    rows = store.read_events(limit=10)
    assert rows[-1]['event_type'] == 'candidate_selected'
    assert rows[-1]['symbol'] == 'DOGEUSDT'
    assert rows[-1]['position_key'] == 'DOGEUSDT:LONG'
    assert rows[-1]['profile'] == 'scan-only'
    assert rows[-1]['scan_only'] is True
    assert rows[-1]['live_requested'] is False


def test_run_loop_scan_only_skips_reconcile_when_api_secret_missing(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'ok': True, 'candidate_count': 0, 'candidates': []}, None, {}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})

    result = mod.run_loop(mod.BinanceFuturesClient(base_url='https://fapi.binance.com'), make_args(scan_only=True))

    assert result['ok'] is True
    cycle = result['cycles'][0]
    assert cycle['reconcile']['skipped'] is True
    assert cycle['reconcile']['skip_reason'] == 'missing_api_secret'
    assert cycle['scan']['candidate_count'] == 0


def test_run_loop_binance_live_submits_without_simulation_flags(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = SimpleNamespace(
        symbol='APEUSDT',
        side='LONG',
        last_price=0.19,
        stop_price=0.17,
        quantity=100.0,
        recommended_leverage=5,
        atr_stop_distance=0.01,
        portfolio_narrative_bucket='test',
        portfolio_correlation_group='test',
    )

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda *args, **kwargs: {
        'ok': True,
        'skipped': False,
        'skip_reason': '',
        'orphan_positions': [],
        'positions_missing_protection': [],
        'protection_repairs': [],
    })
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'ok': True, 'candidate_count': 1, 'candidates': [candidate]}, candidate, {'APEUSDT': object()}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'normalized_risk_state': {}})
    monkeypatch.setattr(mod, 'evaluate_portfolio_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    submitted = {}
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, leverage, meta, args: submitted.setdefault('trade', {
        'exchange': 'BINANCE',
        'simulated': False,
        'symbol': best_candidate.symbol,
        'side': best_candidate.side,
        'entry_price': best_candidate.last_price,
        'filled_quantity': best_candidate.quantity,
        'leverage': leverage,
        'stop_order': {},
        'trade_management_plan': dataclasses.asdict(mod.build_trade_management_plan(
            entry_price=best_candidate.last_price,
            stop_price=best_candidate.stop_price,
            quantity=best_candidate.quantity,
            tp1_r=1.5,
            tp1_close_pct=0.3,
            tp2_r=2.0,
            tp2_close_pct=0.4,
            side='LONG',
        )),
        'protection_check': {'status': 'submitted', 'side': 'LONG'},
    }))
    monkeypatch.setattr(mod, 'monitor_live_trade', lambda **kwargs: {
        'mode': 'binance_live',
        'status': 'submitted',
    })

    result = mod.run_loop(DummyClient(), make_args(
        live=True,
        runtime_state_dir=str(tmp_path),
        margin_type='ISOLATED',
        profile='10u-aggressive',
    ))

    cycle = result['cycles'][0]
    assert cycle['execution_exchange'] == 'BINANCE'
    assert cycle['live_execution']['exchange'] == 'BINANCE'
    assert cycle['trade_management']['mode'] == 'binance_live'
    assert submitted['trade']['symbol'] == 'APEUSDT'


def test_apply_management_action_executes_stop_exit_without_rearming_stop(monkeypatch):
    mod = load_module()
    meta = SimpleNamespace(step_size=0.1, quantity_precision=1, tick_size=0.1, price_precision=1)
    state = mod.TradeManagementState(
        symbol='DOGEUSDT',
        side='LONG',
        position_side='LONG',
        position_key='DOGEUSDT:LONG',
        initial_quantity=10.0,
        remaining_quantity=10.0,
        current_stop_price=95.0,
        moved_to_breakeven=True,
        tp1_hit=True,
        tp2_hit=False,
        highest_price_seen=110.0,
        lowest_price_seen=95.0,
    )
    cancelled = []
    reduce_calls = []
    new_stop_calls = []

    monkeypatch.setattr(mod, 'cancel_order', lambda client, symbol, order_id=None, client_order_id=None: cancelled.append({'symbol': symbol, 'order_id': order_id, 'client_order_id': client_order_id}) or {'ok': True})
    monkeypatch.setattr(mod, 'place_reduce_only_market', lambda client, symbol, quantity, meta, side='LONG': reduce_calls.append({'symbol': symbol, 'quantity': quantity, 'side': side}) or {'status': 'FILLED', 'orderId': 888})
    monkeypatch.setattr(mod, 'place_stop_market_order', lambda *args, **kwargs: new_stop_calls.append({'args': args, 'kwargs': kwargs}) or {'orderId': 999})

    next_state, active_stop_order, log_payload = mod.apply_management_action(
        client=DummyClient(),
        symbol='DOGEUSDT',
        meta=meta,
        state=state,
        action={'type': 'stop_exit', 'close_qty': 10.0, 'exit_reason': 'stop'},
        active_stop_order={'orderId': 555},
    )

    assert reduce_calls == [{'symbol': 'DOGEUSDT', 'quantity': 10.0, 'side': 'LONG'}]
    assert cancelled == []
    assert new_stop_calls == []
    assert next_state.remaining_quantity == 0.0
    assert active_stop_order == {'orderId': 555}
    assert log_payload['action'] == 'stop_exit'
    assert log_payload['reduce_order']['status'] == 'FILLED'
    assert log_payload['new_stop_order'] == {'orderId': 555}


def test_apply_management_action_tightens_short_stop_after_take_profit(monkeypatch):
    mod = load_module()
    meta = SimpleNamespace(step_size=0.1, quantity_precision=1, tick_size=0.1, price_precision=1)
    state = mod.TradeManagementState(
        symbol='DOGEUSDT',
        side='short',
        position_side='SHORT',
        position_key='DOGEUSDT:SHORT',
        initial_quantity=10.0,
        remaining_quantity=10.0,
        current_stop_price=104.0,
        moved_to_breakeven=True,
        highest_price_seen=104.0,
        lowest_price_seen=95.0,
    )
    cancelled = []
    reduce_calls = []
    new_stop_calls = []

    monkeypatch.setattr(mod, 'cancel_order', lambda client, symbol, order_id=None, client_order_id=None: cancelled.append({'symbol': symbol, 'order_id': order_id, 'client_order_id': client_order_id}) or {'ok': True})
    monkeypatch.setattr(mod, 'place_reduce_only_market', lambda client, symbol, quantity, meta, side='LONG': reduce_calls.append({'symbol': symbol, 'quantity': quantity, 'side': side}) or {'status': 'FILLED', 'orderId': 321})
    monkeypatch.setattr(mod, 'place_stop_market_order', lambda client, symbol, stop_price, quantity, meta, side='LONG': new_stop_calls.append({'symbol': symbol, 'stop_price': stop_price, 'quantity': quantity, 'side': side}) or {'orderId': 654, 'triggerPrice': stop_price})

    next_state, active_stop_order, log_payload = mod.apply_management_action(
        client=DummyClient(),
        symbol='DOGEUSDT',
        meta=meta,
        state=state,
        action={'type': 'take_profit_1', 'close_qty': 4.0, 'new_stop_price': 98.0, 'exit_reason': 'tp1'},
        active_stop_order={'orderId': 555},
    )

    assert reduce_calls == [{'symbol': 'DOGEUSDT', 'quantity': 4.0, 'side': 'SHORT'}]
    assert cancelled == [{'symbol': 'DOGEUSDT', 'order_id': 555, 'client_order_id': None}]
    assert new_stop_calls == [{'symbol': 'DOGEUSDT', 'stop_price': 98.0, 'quantity': 6.0, 'side': 'SHORT'}]
    assert next_state.remaining_quantity == 6.0
    assert next_state.current_stop_price == 98.0
    assert active_stop_order == {'orderId': 654, 'triggerPrice': 98.0}
    assert log_payload['new_stop_order'] == {'orderId': 654, 'triggerPrice': 98.0}


def test_evaluate_management_actions_emits_stacked_take_profit_actions_for_short():
    mod = load_module()
    plan = mod.build_trade_management_plan(
        entry_price=4.2708,
        stop_price=4.3694,
        quantity=11.0,
        tp1_r=1.2,
        tp1_close_pct=0.5,
        tp2_r=1.8,
        tp2_close_pct=0.3,
        breakeven_r=0.8,
        side='SHORT',
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.001,
    )
    state = mod.TradeManagementState(
        symbol='LABUSDT',
        initial_quantity=11.0,
        remaining_quantity=11.0,
        side='short',
        position_side='SHORT',
        current_stop_price=4.3694,
        moved_to_breakeven=False,
        tp1_hit=False,
        tp2_hit=False,
        highest_price_seen=4.2708,
        lowest_price_seen=4.2162,
    )

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=4.05,
        ema5m=4.05,
        trailing_reference=4.03,
        trailing_buffer_pct=0.02,
        allow_runner_exit=True,
    )

    assert [action['type'] for action in actions] == [
        'move_stop_to_breakeven',
        'take_profit_1',
        'take_profit_2',
    ]


def test_evaluate_management_actions_emits_stacked_take_profit_actions_for_long():
    mod = load_module()
    plan = mod.build_trade_management_plan(
        entry_price=100.0,
        stop_price=95.0,
        quantity=10.0,
        tp1_r=1.0,
        tp1_close_pct=0.5,
        tp2_r=2.0,
        tp2_close_pct=0.3,
        breakeven_r=0.8,
        side='LONG',
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.001,
    )
    state = mod.TradeManagementState(
        symbol='BTCUSDT',
        initial_quantity=10.0,
        remaining_quantity=10.0,
        side='long',
        position_side='LONG',
        current_stop_price=95.0,
        moved_to_breakeven=False,
        tp1_hit=False,
        tp2_hit=False,
        highest_price_seen=100.0,
        lowest_price_seen=100.0,
    )

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=110.0,
        ema5m=109.0,
        trailing_reference=110.0,
        trailing_buffer_pct=0.02,
        allow_runner_exit=True,
    )

    assert [action['type'] for action in actions] == [
        'move_stop_to_breakeven',
        'take_profit_1',
        'take_profit_2',
    ]


def test_run_loop_live_output_reports_live_mode_when_trade_skipped_after_live_gate(monkeypatch):
    mod = load_module()
    result = {
        'ok': True,
        'cycles': [
            {
                'scan': {
                    'market_regime': {'label': 'neutral', 'score_multiplier': 1.0, 'reasons': []},
                    'rejected_stats': {'total': 0, 'by_reject_label': {}},
                    'candidate_count': 1,
                    'candidate_alerts': [],
                },
                'live_requested': True,
                'live_skipped_due_to_existing_positions': [{'symbol': 'BTCUSDT', 'positionAmt': '0.01'}],
            }
        ],
    }

    rendered = mod.render_cn_scan_summary(result)

    assert '扫描模式: live' in rendered


def test_run_loop_reconcile_only_short_circuits_before_scan(monkeypatch):
    mod = load_module()
    store = DummyStore()
    reconcile_payload = {'ok': True, 'orphan_positions': [], 'positions_missing_protection': []}

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: reconcile_payload)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})

    def unexpected(*args, **kwargs):
        raise AssertionError('scan path should not be called in reconcile-only mode')

    monkeypatch.setattr(mod, 'run_scan_once', unexpected)

    result = mod.run_loop(DummyClient(), make_args(reconcile_only=True))
    assert result == {'mode': 'reconcile_only', 'ok': True, 'reconcile': reconcile_payload, 'cycles': []}


def test_run_loop_skips_protection_missing_halt_after_successful_auto_repair(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    reconcile_payload = {
        'ok': True,
        'orphan_positions': [],
        'positions_missing_protection': [],
        'protection_repairs': [{'symbol': 'DOGEUSDT', 'status': 'protected', 'ok': True}],
    }
    notifications = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: reconcile_payload)
    monkeypatch.setattr(mod, 'emit_notification', lambda args, event_type, payload: notifications.append((event_type, payload)) or {'ok': True})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *args, **kwargs: ({'ok': True, 'candidate_count': 0, 'candidates': []}, None, {}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False, 'halt_reason': '', 'symbol_cooldowns': {}, 'daily_realized_pnl_usdt': 0.0, 'consecutive_losses': 0})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})

    result = mod.run_loop(DummyClient(), make_args(live=True, repair_missing_protection=True))

    assert result['ok'] is True
    assert notifications == []


def test_run_loop_logs_and_notifies_protection_missing_event(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    reconcile_payload = {
        'ok': True,
        'orphan_positions': [],
        'positions_missing_protection': ['DOGEUSDT'],
        'protection_repairs': [],
    }
    notifications = []
    notifications_path = tmp_path / 'notifications.jsonl'

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: reconcile_payload)

    def fake_emit(args, event_type, payload):
        notifications.append((event_type, payload))
        with notifications_path.open('a', encoding='utf-8') as fh:
            fh.write(mod.json.dumps({'event_type': event_type, 'payload': payload}, ensure_ascii=False) + '\n')
        return {'ok': True}

    monkeypatch.setattr(mod, 'emit_notification', fake_emit)
    monkeypatch.setattr(mod, 'run_scan_once', lambda *args, **kwargs: ({'ok': True, 'candidate_count': 0, 'candidates': []}, None, {}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False, 'halt_reason': '', 'symbol_cooldowns': {}, 'daily_realized_pnl_usdt': 0.0, 'consecutive_losses': 0})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})

    result = mod.run_loop(DummyClient(), make_args(live=True, repair_missing_protection=False, profile='test-profile'))

    assert result['ok'] is True
    risk_state = store.load_json('risk_state', {})
    assert risk_state['halted'] is True
    assert risk_state['halt_reason'] == 'missing_protection:DOGEUSDT'
    assert notifications == [(
        'protection_missing',
        {
            'halt_reason': 'missing_protection:DOGEUSDT',
            'positions_missing_protection': ['DOGEUSDT'],
            'orphan_positions': [],
            'profile': 'test-profile',
        },
    )]
    rows = [mod.json.loads(line) for line in notifications_path.read_text(encoding='utf-8').splitlines() if line.strip()]
    assert rows[-1]['event_type'] == 'protection_missing'
    assert rows[-1]['payload']['halt_reason'] == 'missing_protection:DOGEUSDT'


def test_run_loop_logs_and_notifies_strategy_halted_event(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    reconcile_payload = {
        'ok': False,
        'orphan_positions': ['BTCUSDT'],
        'positions_missing_protection': ['DOGEUSDT'],
        'protection_repairs': [],
    }
    notifications = []
    notifications_path = tmp_path / 'notifications.jsonl'

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: reconcile_payload)

    def fake_emit(args, event_type, payload):
        notifications.append((event_type, payload))
        with notifications_path.open('a', encoding='utf-8') as fh:
            fh.write(mod.json.dumps({'event_type': event_type, 'payload': payload}, ensure_ascii=False) + '\n')
        return {'ok': True}

    monkeypatch.setattr(mod, 'emit_notification', fake_emit)

    result = mod.run_loop(DummyClient(), make_args(live=True, profile='test-profile'))

    assert result['ok'] is False
    assert notifications == [(
        'strategy_halted',
        {
            'halt_reason': 'orphan_positions:BTCUSDT',
            'orphan_positions': ['BTCUSDT'],
            'positions_missing_protection': ['DOGEUSDT'],
            'profile': 'test-profile',
        },
    )]
    rows = [mod.json.loads(line) for line in notifications_path.read_text(encoding='utf-8').splitlines() if line.strip()]
    assert rows[-1]['event_type'] == 'strategy_halted'
    assert rows[-1]['payload']['orphan_positions'] == ['BTCUSDT']


def test_main_auto_loop_degrades_to_single_scan_when_run_loop_missing(monkeypatch, capsys):
    mod = load_module()
    monkeypatch.setattr(mod, 'parse_args', lambda argv=None: make_args(auto_loop=True, max_scan_cycles=0, base_url='https://example.com'))
    monkeypatch.setattr(mod, 'BinanceFuturesClient', lambda **kwargs: DummyClient())
    monkeypatch.delattr(mod, 'run_loop', raising=False)
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'ok': True, 'selected': 'DOGEUSDT', 'auto_loop_requested': True}, None, {}))

    exit_code = mod.main([])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert 'DOGEUSDT' in captured.out
    assert 'auto_loop_requested' in captured.out


def test_main_single_run_prints_scan_payload(monkeypatch, capsys):
    mod = load_module()
    monkeypatch.setattr(mod, 'parse_args', lambda argv=None: make_args(auto_loop=False, base_url='https://example.com'))
    monkeypatch.setattr(mod, 'BinanceFuturesClient', lambda **kwargs: DummyClient())
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'ok': True, 'selected': 'DOGEUSDT'}, None, {}))

    exit_code = mod.main([])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert 'DOGEUSDT' in captured.out



def test_resident_runtime_uses_split_cycles_instead_of_run_loop(monkeypatch, capsys):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=1, poll_interval_sec=0, base_url='https://example.com')
    calls = []
    store = mod.RuntimeStateStore(runtime_state_dir='/tmp/runtime-split-cycle-test')

    monkeypatch.setattr(mod, 'parse_args', lambda argv=None: args)
    monkeypatch.setattr(mod, 'BinanceFuturesClient', lambda **kwargs: DummyClient())
    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'run_loop', lambda *a, **k: (_ for _ in ()).throw(AssertionError('resident runtime must not call monolithic run_loop')))

    def fake_scan_only_cycle(client, passed_args, *, store=None, cycle_no=None, websocket_status=None):
        calls.append(('scan', cycle_no, websocket_status))
        return {'ok': True, 'cycle': {'scan': {'candidate_count': 1}}, 'execution_request': {'symbol': 'DOGEUSDT'}, 'manager_update': {'state': 'SCAN_READY', 'cycle': {'scan': {'candidate_count': 1}}}}

    def fake_execution_cycle(client, passed_args, execution_request, *, store=None):
        calls.append(('execution', execution_request.get('symbol')))
        return {'ok': True, 'live_execution': {'symbol': execution_request.get('symbol')}}

    def fake_management_cycle(passed_args, manager_update, *, store=None):
        calls.append(('manager', manager_update.get('state')))
        return {'ok': True, 'state': manager_update.get('state')}

    monkeypatch.setattr(mod, 'scan_only_cycle', fake_scan_only_cycle)
    monkeypatch.setattr(mod, 'execution_cycle', fake_execution_cycle)
    monkeypatch.setattr(mod, 'management_cycle', fake_management_cycle)

    exit_code = mod.main([])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert ('scan', 1, None) in calls
    assert ('execution', 'DOGEUSDT') in calls
    assert ('manager', 'SCAN_READY') in calls
    assert '"auto_loop": true' in captured.out
    assert '"cycle_no": 1' in captured.out



def test_resident_ws_task_starts_monitor_once_across_multiple_scan_cycles(monkeypatch, capsys):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=3, poll_interval_sec=0, base_url='https://example.com', require_book_ticker_ws=True)
    store = mod.RuntimeStateStore(runtime_state_dir='/tmp/runtime-single-ws-task-test')
    scan_ws_statuses = []
    monitor_calls = []

    monkeypatch.setattr(mod, 'parse_args', lambda argv=None: args)
    monkeypatch.setattr(mod, 'BinanceFuturesClient', lambda **kwargs: DummyClient())
    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'run_loop', lambda *a, **k: (_ for _ in ()).throw(AssertionError('resident runtime must not call monolithic run_loop')))

    def fake_monitor(*, client, store, args):
        monitor_calls.append('started')
        store.save_json('book_ticker_ws_status', {
            'status': 'healthy',
            'updated_at': mod._isoformat_utc(mod._utc_now()),
            'messages_processed': 1,
            'samples_written': 1,
        })
        return {'status': 'resident_started', 'summary': {'running': True}, 'health': store.load_json('book_ticker_ws_status', {})}

    def fake_scan_only_cycle(client, passed_args, *, store=None, cycle_no=None, websocket_status=None):
        scan_ws_statuses.append(websocket_status)
        return {'ok': True, 'cycle': {'cycle_no': cycle_no}, 'manager_update': {'state': 'SCAN', 'cycle': {'cycle_no': cycle_no}}}

    monkeypatch.setattr(mod, 'run_auto_loop_book_ticker_websocket_monitor', fake_monitor)
    monkeypatch.setattr(mod, 'scan_only_cycle', fake_scan_only_cycle)
    monkeypatch.setattr(mod.time, 'sleep', lambda seconds: None)

    exit_code = mod.main([])
    capsys.readouterr()

    assert exit_code == 0
    assert monitor_calls == ['started']
    assert len(scan_ws_statuses) == 3



def test_resident_scanner_execution_do_not_write_runtime_state_directly(monkeypatch):
    mod = load_module()
    args = make_args(auto_loop=True, live=False, scan_only=True, max_scan_cycles=1, require_book_ticker_ws=False)

    class GuardedStore(DummyStore):
        def save_json(self, name, payload):
            raise AssertionError(f'non-manager actor wrote runtime state: {name}')

    store = GuardedStore()
    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda passed_args: store)
    monkeypatch.setattr(mod, 'cleanup_symbol_runtime_state_ttl', lambda *a, **k: {'removed': {}})
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda *a, **k: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *a, **k: ({'ok': True, 'funnel': {}}, None, {}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: mod.default_risk_state())
    monkeypatch.setattr(mod, 'record_runtime_heartbeat', lambda *a, **k: {'ok': True})

    result = mod.scan_only_cycle(DummyClient(), args, store=store, cycle_no=1)

    assert result['manager_update']['kind'] == 'cycle'


def test_execution_cycle_emits_position_manager_message_without_starting_monitor(monkeypatch):
    mod = load_module()
    args = make_args(auto_loop=True, live=True, require_book_ticker_ws=False)
    store = DummyStore()
    candidate = SimpleNamespace(symbol='DOGEUSDT', side='LONG', position_side='LONG', recommended_leverage=3)
    execution_request = {
        'candidate': candidate,
        'meta': {'symbol': 'DOGEUSDT'},
        'risk_guard': {'allowed': True, 'reasons': []},
        'reconcile': {'ok': True},
        'cycle': {'cycle_no': 7},
        'requested_leverage': 3,
    }
    monkeypatch.setattr(mod, 'place_live_trade', lambda *a, **k: {'symbol': 'DOGEUSDT', 'side': 'LONG', 'quantity': 1})
    monkeypatch.setattr(mod, 'persist_auto_loop_state', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'persist_live_open_position', lambda *a, **k: ({'DOGEUSDT:LONG': {}}, 'DOGEUSDT:LONG'))
    monkeypatch.setattr(mod, 'start_trade_monitor_thread', lambda *a, **k: (_ for _ in ()).throw(AssertionError('execution actor must not start long trade monitor')))
    monkeypatch.setattr(mod, 'monitor_live_trade', lambda *a, **k: (_ for _ in ()).throw(AssertionError('execution actor must not monitor trade inline')))

    result = mod.execution_cycle(DummyClient(), args, execution_request, store=store)

    assert result['ok'] is True
    assert result['position_manager_request']['position_key'] == 'DOGEUSDT:LONG'
    assert result['manager_update']['kind'] == 'execution_result'


def test_runtime_backpressure_triggers_degrade_instead_of_only_logging(monkeypatch):
    mod = load_module()
    store = DummyStore()
    queue = __import__('asyncio').Queue(maxsize=1)
    queue.put_nowait({'existing': True})
    events = []
    monkeypatch.setattr(mod, 'append_runtime_event', lambda store, event_type, payload: events.append((event_type, payload)) or payload)

    result = __import__('asyncio').run(mod.apply_queue_backpressure(queue, store=store, component='scanner', reason='execution_queue_full'))

    assert result['accepted'] is False
    assert result['degraded'] is True
    assert events[-1][0] == 'runtime_backpressure_degrade'


def test_event_loop_latency_monitor_records_lag_metric(monkeypatch):
    mod = load_module()
    store = DummyStore()
    samples = []
    monkeypatch.setattr(mod, 'record_runtime_heartbeat', lambda store, component, status, blocked_reason='', **kwargs: samples.append({'component': component, 'status': status, 'blocked_reason': blocked_reason, **kwargs}) or samples[-1])

    __import__('asyncio').run(mod.event_loop_latency_task(store, __import__('asyncio').Event(), interval=0.001, warn_threshold_seconds=0.0, max_samples=1))

    assert samples
    assert samples[-1]['component'] == 'event_loop'
    assert 'lag_seconds' in samples[-1]['extra']



def test_execution_cycle_returns_manager_writes_without_direct_runtime_state(monkeypatch):
    mod = load_module()
    args = make_args(auto_loop=True, live=True, require_book_ticker_ws=False)

    class GuardedStore(DummyStore):
        def save_json(self, name, payload):
            raise AssertionError(f'execution actor direct write: {name}')
        def append_event(self, event_type, payload):
            raise AssertionError(f'execution actor direct event write: {event_type}')

    candidate = SimpleNamespace(symbol='DOGEUSDT', side='LONG', position_side='LONG', recommended_leverage=3)
    req = {'candidate': candidate, 'meta': {'symbol': 'DOGEUSDT'}, 'risk_guard': {'allowed': True}, 'reconcile': {'ok': True}, 'cycle': {'cycle_no': 9}, 'requested_leverage': 3}
    monkeypatch.setattr(mod, 'place_live_trade', lambda *a, **k: {'symbol': 'DOGEUSDT', 'quantity': 1})
    monkeypatch.setattr(mod, 'start_trade_monitor_thread', lambda *a, **k: (_ for _ in ()).throw(AssertionError('execution actor started monitor thread')))

    result = mod.execution_cycle(DummyClient(), args, req, store=GuardedStore())

    assert result['ok'] is True
    assert result['manager_update']['kind'] == 'execution_result'
    assert result['manager_update'].get('state_transition', {}).get('state') == 'ENTERING'
    assert result['position_manager_request']['kind'] == 'position_opened'


def test_position_manager_actor_does_not_write_state_or_start_background_thread(monkeypatch):
    mod = load_module()
    args = make_args(auto_loop=True, live=True, require_book_ticker_ws=False)

    class GuardedStore(DummyStore):
        def save_json(self, name, payload):
            raise AssertionError(f'position manager direct write: {name}')
        def append_event(self, event_type, payload):
            raise AssertionError(f'position manager direct event write: {event_type}')

    async def run_case():
        q = {'position_manager': __import__('asyncio').Queue(maxsize=4), 'manager': __import__('asyncio').Queue(maxsize=4)}
        stop = __import__('asyncio').Event()
        candidate = SimpleNamespace(symbol='DOGEUSDT', side='LONG', position_side='LONG')
        await q['position_manager'].put({'kind': 'position_manager_request', 'cycle_no': 3, 'request': {'kind': 'position_opened', 'candidate': candidate, 'symbol': 'DOGEUSDT', 'position_key': 'DOGEUSDT:LONG', 'meta': {}, 'trade': {'symbol': 'DOGEUSDT'}, 'cycle': {'cycle_no': 3}}})
        stop.set()
        await mod.position_manager_task(DummyClient(), args, GuardedStore(), q, stop)
        return await q['manager'].get()

    monkeypatch.setattr(mod, 'start_trade_monitor_thread', lambda *a, **k: (_ for _ in ()).throw(AssertionError('position manager started background monitor thread')))
    monkeypatch.setattr(mod, 'monitor_live_trade', lambda *a, **k: (_ for _ in ()).throw(AssertionError('position manager delegated FSM to monitor thread')))

    item = __import__('asyncio').run(run_case())

    assert item['kind'] == 'manager_update'
    assert item['update']['kind'] == 'position_opened'
    assert item['update']['position_key'] == 'DOGEUSDT:LONG'


def test_ws_task_stale_recovery_uses_singleton_and_does_not_start_duplicate_monitor(monkeypatch):
    mod = load_module()
    args = make_args(require_book_ticker_ws=True, websocket_healthcheck_interval_seconds=0.001, websocket_healthcheck_timeout_seconds=0.1, book_ticker_ws_stale_seconds=0.0, websocket_restart_backoff_seconds=0.0)
    store = DummyStore()
    calls = []

    def fake_monitor(*, client, store, args):
        calls.append('start')
        store.save_json('book_ticker_ws_status', {'status': 'stale', 'updated_at': '2000-01-01T00:00:00Z', 'messages_processed': 0})
        return {'status': 'resident_started', 'summary': {'running': True}}

    async def run_case():
        stop = __import__('asyncio').Event()
        task = __import__('asyncio').create_task(mod.ws_task(DummyClient(), args, store, stop))
        await __import__('asyncio').sleep(0.01)
        stop.set()
        await task

    monkeypatch.setattr(mod, 'run_auto_loop_book_ticker_websocket_monitor', fake_monitor)
    __import__('asyncio').run(run_case())

    assert calls == ['start']


def test_backpressure_policy_degrades_scan_rate_and_drops_low_score_candidates(monkeypatch):
    mod = load_module()
    store = DummyStore()
    q = __import__('asyncio').Queue(maxsize=1)
    q.put_nowait({'existing': True})
    low_score_item = {'candidate_score': 0.01, 'candidate_rank': 99}

    result = __import__('asyncio').run(mod.apply_queue_backpressure(q, store=store, component='scanner', reason='execution_queue_full', item=low_score_item))

    assert result['accepted'] is False
    assert result['policy']['scan_delay_multiplier'] > 1
    assert result['policy']['drop_candidate'] is True
    assert result['policy']['pause_non_core_tasks'] is True


def test_watchdog_detects_stale_core_heartbeats_and_emits_recovery(monkeypatch):
    mod = load_module()
    store = DummyStore()
    store.save_json('runtime_heartbeat', {
        'scanner': {'updated_at_ts': 1.0, 'extra': {'last_scan_ts': 1.0}},
        'ws': {'updated_at_ts': 1.0, 'extra': {'last_ws_msg_ts': 1.0}},
        'execution': {'updated_at_ts': 1.0, 'extra': {'last_execution_ts': 1.0}},
        'event_loop': {'updated_at_ts': 1.0, 'extra': {'lag_seconds': 9.0}},
    })
    events = []
    monkeypatch.setattr(mod.time, 'time', lambda: 100.0)
    monkeypatch.setattr(mod, 'append_runtime_event', lambda store, event_type, payload: events.append((event_type, payload)) or payload)

    async def run_case():
        stop = __import__('asyncio').Event()
        task = __import__('asyncio').create_task(mod.watchdog_task(store, {'scanner': __import__('asyncio').Queue(maxsize=1)}, stop, interval=0.001, max_samples=1, stale_seconds=10.0, event_loop_lag_seconds=1.0))
        await task

    __import__('asyncio').run(run_case())

    assert any(event_type == 'resident_watchdog_recovery' for event_type, payload in events)
    recovery = [payload for event_type, payload in events if event_type == 'resident_watchdog_recovery'][-1]
    assert {'scanner_stale', 'ws_stale', 'execution_stale', 'event_loop_lag'} <= set(recovery['actions'])


def test_scan_only_cycle_no_candidate_returns_manager_update_without_direct_state_write(monkeypatch):
    mod = load_module()
    args = make_args(auto_loop=True, live=True, require_book_ticker_ws=False)

    class GuardedStore(DummyStore):
        def save_json(self, name, payload):
            if name == 'runtime_heartbeat':
                return super().save_json(name, payload)
            raise AssertionError(f'scanner direct write: {name}')
        def append_event(self, event_type, payload):
            raise AssertionError(f'scanner direct event write: {event_type}')

    monkeypatch.setattr(mod, 'cleanup_symbol_runtime_state_ttl', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'is_binance_simulated_trading', lambda args: True)
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidate_count': 0}, None, {}))
    monkeypatch.setattr(mod, 'apply_reconcile_close_risk_state_updates', lambda *a, **k: mod.default_risk_state())
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'normalized_risk_state': mod.default_risk_state()})

    result = mod.scan_only_cycle(DummyClient(), args, store=GuardedStore(), cycle_no=4)

    assert result['ok'] is True
    assert result['manager_update']['kind'] == 'cycle'
    assert result['manager_update']['reason'] == 'no_candidate'
    assert result['manager_update']['cycle']['cycle_no'] == 4


def test_ws_task_resets_singleton_guard_after_exit(monkeypatch):
    mod = load_module()
    args = make_args(require_book_ticker_ws=True, websocket_healthcheck_interval_seconds=0.001, websocket_healthcheck_timeout_seconds=0.1, book_ticker_ws_stale_seconds=30.0)
    store = DummyStore()
    calls = []

    def fake_monitor(*, client, store, args):
        calls.append('start')
        store.save_json('book_ticker_ws_status', {'status': 'healthy', 'updated_at': mod.datetime.now(mod.timezone.utc).isoformat(), 'messages_processed': 1})
        return {'status': 'resident_started'}

    async def run_once():
        stop = __import__('asyncio').Event()
        task = __import__('asyncio').create_task(mod.ws_task(DummyClient(), args, store, stop))
        await __import__('asyncio').sleep(0.003)
        stop.set()
        await task

    monkeypatch.setattr(mod, 'run_auto_loop_book_ticker_websocket_monitor', fake_monitor)
    mod._BOOK_TICKER_WS_SUPERVISOR_ACTIVE = False
    __import__('asyncio').run(run_once())
    assert mod._BOOK_TICKER_WS_SUPERVISOR_ACTIVE is False
    __import__('asyncio').run(run_once())
    assert calls == ['start', 'start']


def test_scanner_task_applies_backpressure_scan_delay_multiplier(monkeypatch):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=2, poll_interval_sec=2, require_book_ticker_ws=False)
    store = DummyStore()
    sleeps = []

    async def fake_to_thread(fn, *args, **kwargs):
        if fn is mod.scan_only_cycle:
            cycle_no = kwargs.get('cycle_no')
            return {'ok': True, 'cycle': {'cycle_no': cycle_no}, 'execution_request': {'candidate_score': 0.01}, 'manager_update': {'kind': 'cycle', 'cycle': {'cycle_no': cycle_no}}}
        if fn is mod.time.sleep:
            sleeps.append(args[0])
            return None
        return fn(*args, **kwargs)

    queues = {'execution': __import__('asyncio').Queue(maxsize=1), 'manager': __import__('asyncio').Queue(maxsize=4)}
    queues['execution'].put_nowait({'existing': True})
    monkeypatch.setattr(mod.asyncio, 'to_thread', fake_to_thread)

    __import__('asyncio').run(mod.scanner_task(DummyClient(), args, store, queues, None, __import__('asyncio').Event()))

    assert sleeps == [6]


def test_watchdog_persists_recovery_request_for_supervisor(monkeypatch):
    mod = load_module()
    store = DummyStore()
    store.save_json('runtime_heartbeat', {
        'components': {
            'scanner': {'updated_at_ts': 1.0, 'extra': {'last_scan_ts': 1.0}},
            'ws': {'updated_at_ts': 1.0, 'extra': {'last_ws_msg_ts': 1.0}},
            'execution': {'updated_at_ts': 1.0, 'extra': {'last_execution_ts': 1.0}},
            'event_loop': {'updated_at_ts': 1.0, 'extra': {'lag_seconds': 5.0}},
        }
    })
    monkeypatch.setattr(mod.time, 'time', lambda: 100.0)

    __import__('asyncio').run(mod.watchdog_task(store, {'manager': __import__('asyncio').Queue(maxsize=1)}, __import__('asyncio').Event(), interval=0.001, max_samples=1, stale_seconds=10.0, event_loop_lag_seconds=1.0))

    recovery = store.load_json('runtime_recovery_request', {})
    assert recovery['action'] == 'supervisor_restart'
    assert {'scanner_stale', 'ws_stale', 'execution_stale', 'event_loop_lag'} <= set(recovery['actions'])


def test_scan_only_cycle_emits_manager_side_effects_without_direct_event_or_risk_writes(monkeypatch):
    mod = load_module()
    candidate = mod.Candidate(symbol='DOGEUSDT', last_price=1.0, price_change_pct_24h=5.0, quote_volume_24h=1000000.0, hot_rank=None, gainer_rank=None, funding_rate=None, funding_rate_avg=None, recent_5m_change_pct=1.0, acceleration_ratio_5m_vs_15m=1.0, breakout_level=1.0, recent_swing_low=0.9, stop_price=0.9, quantity=1.0, risk_per_unit=0.1, recommended_leverage=3, rsi_5m=60.0, volume_multiple=2.0, distance_from_ema20_5m_pct=1.0, distance_from_vwap_15m_pct=1.0, higher_tf_summary={}, score=0.9, reasons=[], side='LONG', position_side='LONG', setup_missing=['waiting_breakout'])
    args = make_args(auto_loop=True, live=True, require_book_ticker_ws=False)

    class GuardedStore(DummyStore):
        def append_event(self, event_type, payload):
            raise AssertionError(f'scanner direct event write: {event_type}')
        def save_json(self, name, payload):
            if name == 'runtime_heartbeat':
                return super().save_json(name, payload)
            raise AssertionError(f'scanner direct state write: {name}')

    monkeypatch.setattr(mod, 'cleanup_symbol_runtime_state_ttl', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'is_binance_simulated_trading', lambda args: False)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda *a, **k: {'ok': True, 'closed_positions': ['OLD:LONG']})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidate_count': 1, 'market_regime': {}}, candidate, {'DOGEUSDT': {'symbol': 'DOGEUSDT'}}))
    monkeypatch.setattr(mod, 'apply_reconcile_close_risk_state_updates', lambda *a, **k: (_ for _ in ()).throw(AssertionError('scanner direct risk-state write')))
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: mod.default_risk_state())
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': False, 'reasons': ['unit_risk_block'], 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'evaluate_portfolio_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])

    result = mod.scan_only_cycle(DummyClient(), args, store=GuardedStore(), cycle_no=9)

    update = result['manager_update']
    assert update['reason'] == 'risk_guard_blocked'
    assert update['reconcile'] == {'ok': True, 'closed_positions': ['OLD:LONG']}
    assert any(e['event_type'] == 'candidate_selected' for e in update['event_updates'])
    assert any(e['event_type'] == 'candidate_rejected' for e in update['event_updates'])
    assert any(e['event_type'] == 'missed_trade' for e in update['event_updates'])


def test_execution_task_routes_completion_event_through_manager_queue(monkeypatch):
    mod = load_module()
    args = make_args(auto_loop=True, execution_timeout_seconds=0.1)
    store = DummyStore()
    queues = {'execution': __import__('asyncio').Queue(maxsize=4), 'manager': __import__('asyncio').Queue(maxsize=4), 'position_manager': __import__('asyncio').Queue(maxsize=4)}
    queues['execution'].put_nowait({'kind': 'execution_request', 'cycle_no': 7, 'request': {'x': 1}})
    seen = []

    async def fake_to_thread(fn, *args, **kwargs):
        return {'ok': True, 'manager_update': {'kind': 'execution_result', 'cycle': {'cycle_no': 7}}}

    def forbidden_event(*args, **kwargs):
        raise AssertionError('execution direct runtime event write')

    monkeypatch.setattr(mod.asyncio, 'to_thread', fake_to_thread)
    monkeypatch.setattr(mod, 'append_runtime_event', forbidden_event)
    stop = __import__('asyncio').Event()
    stop.set()
    __import__('asyncio').run(mod.execution_task(DummyClient(), args, store, queues, stop))

    while not queues['manager'].empty():
        seen.append(queues['manager'].get_nowait())
    kinds = [item['update']['kind'] for item in seen]
    assert 'execution_result' in kinds
    assert 'runtime_event' in kinds
    assert any(item['update'].get('event_type') == 'resident_execution_completed' for item in seen)


def test_ws_task_forces_restart_when_singleton_guard_is_stale(monkeypatch):
    mod = load_module()
    args = make_args(require_book_ticker_ws=True, websocket_healthcheck_interval_seconds=0.001, websocket_healthcheck_timeout_seconds=0.1, book_ticker_ws_stale_seconds=0.001, websocket_restart_backoff_seconds=0.001)
    store = DummyStore()
    calls = []

    def fake_monitor(*, client, store, args):
        calls.append('start')
        store.save_json('book_ticker_ws_status', {'status': 'healthy', 'updated_at': (mod.datetime.now(mod.timezone.utc) - mod.timedelta(seconds=10)).isoformat(), 'messages_processed': len(calls)})
        return {'status': 'started'}

    async def run_once():
        stop = __import__('asyncio').Event()
        task = __import__('asyncio').create_task(mod.ws_task(DummyClient(), args, store, stop))
        await __import__('asyncio').sleep(0.01)
        stop.set()
        await task

    monkeypatch.setattr(mod, 'run_auto_loop_book_ticker_websocket_monitor', fake_monitor)
    mod._BOOK_TICKER_WS_SUPERVISOR_ACTIVE = True
    __import__('asyncio').run(run_once())
    assert len(calls) >= 1
    assert any(e['event_type'] == 'book_ticker_ws_singleton_recovery_requested' and e.get('action') == 'forced_restart' for e in store.events)


def test_resident_runtime_consumes_recovery_request_and_restarts_tasks(monkeypatch):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=1, runtime_queue_maxsize=4, require_book_ticker_ws=False, resident_shutdown_timeout_seconds=0.1)
    store = DummyStore()
    store.save_json('runtime_recovery_request', {'action': 'supervisor_restart', 'actions': ['scanner_stale']})
    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)

    async def one_tick(*args, **kwargs):
        return None
    async def scanner(client, args, store, queues, run_loop_fn, stop_event):
        stop_event.set()
    monkeypatch.setattr(mod, 'scanner_task', scanner)
    monkeypatch.setattr(mod, 'execution_task', one_tick)
    monkeypatch.setattr(mod, 'manager_task', one_tick)
    monkeypatch.setattr(mod, 'position_manager_task', one_tick)
    monkeypatch.setattr(mod, 'watchdog_task', one_tick)
    monkeypatch.setattr(mod, 'event_loop_latency_task', one_tick)

    result = __import__('asyncio').run(mod.run_resident_runtime_async(DummyClient(), args, lambda c, a: {'ok': True}))
    assert result.get('ok') is True
    assert store.load_json('runtime_recovery_request', {}).get('consumed') is True
    assert any(e['event_type'] == 'resident_supervisor_restart_consumed' for e in store.events)


def test_resident_runtime_shutdown_cancels_stuck_tasks_with_timeout(monkeypatch):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=1, runtime_queue_maxsize=4, require_book_ticker_ws=False, resident_shutdown_timeout_seconds=0.01)
    store = DummyStore()
    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)

    async def scanner(client, args, store, queues, run_loop_fn, stop_event):
        stop_event.set()
    async def stuck(*args, **kwargs):
        await __import__('asyncio').sleep(10)
    async def one_tick(*args, **kwargs):
        return None
    monkeypatch.setattr(mod, 'scanner_task', scanner)
    monkeypatch.setattr(mod, 'execution_task', stuck)
    monkeypatch.setattr(mod, 'manager_task', one_tick)
    monkeypatch.setattr(mod, 'position_manager_task', one_tick)
    monkeypatch.setattr(mod, 'watchdog_task', one_tick)
    monkeypatch.setattr(mod, 'event_loop_latency_task', one_tick)

    result = __import__('asyncio').run(mod.run_resident_runtime_async(DummyClient(), args, lambda c, a: {'ok': True}))
    assert result.get('ok') is True
    assert any(e['event_type'] == 'resident_shutdown_forced_cancel' for e in store.events)


def test_scanner_task_drops_low_score_candidate_under_backpressure(monkeypatch):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=1, poll_interval_sec=0, require_book_ticker_ws=False)
    store = DummyStore()
    queues = {'execution': __import__('asyncio').Queue(maxsize=1), 'manager': __import__('asyncio').Queue(maxsize=4)}

    async def fake_to_thread(fn, *args, **kwargs):
        if fn is mod.scan_only_cycle:
            return {'ok': True, 'cycle': {'cycle_no': 1}, 'execution_request': {'candidate_score': 0.01, 'candidate': SimpleNamespace(score=0.01)}, 'manager_update': {'kind': 'cycle', 'cycle': {'cycle_no': 1}}}
        return None

    monkeypatch.setattr(mod.asyncio, 'to_thread', fake_to_thread)
    monkeypatch.setattr(mod, 'build_backpressure_policy', lambda component, reason, item=None: {'scan_delay_multiplier': 3.0, 'drop_candidate': True, 'pause_non_core_tasks': True, 'min_candidate_score': 0.2})
    __import__('asyncio').run(mod.scanner_task(DummyClient(), args, store, queues, None, __import__('asyncio').Event()))

    assert queues['execution'].empty()
    assert any(e['event_type'] == 'runtime_candidate_dropped_by_backpressure' for e in store.events)


def test_main_auto_loop_runs_multiple_cycles_and_sleeps(monkeypatch, capsys):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=2, poll_interval_sec=7, base_url='https://example.com')
    cycle_calls = []
    sleeps = []
    store = mod.RuntimeStateStore(runtime_state_dir='/tmp/runtime-split-main-loop-test')

    monkeypatch.setattr(mod, 'parse_args', lambda argv=None: args)
    monkeypatch.setattr(mod, 'BinanceFuturesClient', lambda **kwargs: DummyClient())
    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)

    def fake_scan_only_cycle(client, passed_args, *, store=None, cycle_no=None, websocket_status=None):
        cycle_calls.append(cycle_no)
        return {'ok': True, 'cycle': {'cycle_no': cycle_no, 'scan': {'candidate_count': cycle_no}}, 'manager_update': {'state': 'SCAN', 'cycle': {'cycle_no': cycle_no, 'scan': {'candidate_count': cycle_no}}}}

    monkeypatch.setattr(mod, 'scan_only_cycle', fake_scan_only_cycle)
    monkeypatch.setattr(mod.time, 'sleep', lambda seconds: sleeps.append(seconds))

    exit_code = mod.main([])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert cycle_calls == [1, 2]
    assert sleeps == [7]
    assert '"auto_loop": true' in captured.out
    assert '"cycle_no": 2' in captured.out


def test_main_auto_loop_zero_cycles_runs_forever_until_keyboard_interrupt(monkeypatch, capsys):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=0, poll_interval_sec=5, base_url='https://example.com')
    cycle_calls = []
    sleeps = []
    store = mod.RuntimeStateStore(runtime_state_dir='/tmp/runtime-split-interrupt-sleep-test')

    monkeypatch.setattr(mod, 'parse_args', lambda argv=None: args)
    monkeypatch.setattr(mod, 'BinanceFuturesClient', lambda **kwargs: DummyClient())
    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)

    def fake_scan_only_cycle(client, passed_args, *, store=None, cycle_no=None, websocket_status=None):
        cycle_calls.append(cycle_no)
        return {'ok': True, 'cycle': {'cycle_no': cycle_no}, 'manager_update': {'state': 'SCAN', 'cycle': {'cycle_no': cycle_no}}}

    def fake_sleep(seconds):
        sleeps.append(seconds)
        raise KeyboardInterrupt()

    monkeypatch.setattr(mod, 'scan_only_cycle', fake_scan_only_cycle)
    monkeypatch.setattr(mod.time, 'sleep', fake_sleep)

    exit_code = mod.main([])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert cycle_calls == [1]
    assert sleeps == [5]
    assert 'interrupted' in captured.out


def test_main_auto_loop_zero_cycles_exits_cleanly_when_scan_cycle_interrupts(monkeypatch, capsys):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=0, poll_interval_sec=5, base_url='https://example.com')
    cycle_calls = []
    sleeps = []
    store = mod.RuntimeStateStore(runtime_state_dir='/tmp/runtime-split-interrupt-scan-test')

    monkeypatch.setattr(mod, 'parse_args', lambda argv=None: args)
    monkeypatch.setattr(mod, 'BinanceFuturesClient', lambda **kwargs: DummyClient())
    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod.time, 'sleep', lambda seconds: sleeps.append(seconds))

    def fake_scan_only_cycle(client, passed_args, *, store=None, cycle_no=None, websocket_status=None):
        cycle_calls.append(cycle_no)
        raise KeyboardInterrupt()

    monkeypatch.setattr(mod, 'scan_only_cycle', fake_scan_only_cycle)

    exit_code = mod.main([])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert cycle_calls == [1]
    assert sleeps == []
    assert 'interrupted' in captured.out


def test_run_loop_auto_loop_background_monitor_records_event_and_state(monkeypatch):
    mod = load_module()
    store = mod.RuntimeStateStore(runtime_state_dir='/tmp/runtime-for-monitor-test')
    candidate = SimpleNamespace(
        symbol='DOGEUSDT',
        quantity=12.5,
        stop_price=0.1234,
        recommended_leverage=3,
    )
    meta = SimpleNamespace(symbol='DOGEUSDT')
    live_execution = {
        'entry_price': 0.135,
        'trade_management_plan': {'quantity': 12.5, 'entry_price': 0.135, 'stop_price': 0.1234, 'initial_risk_per_unit': 0.0116, 'side': 'long', 'position_side': 'LONG'},
        'stop_order': {'orderId': 98765},
        'protection_check': {'status': 'protected'},
    }
    captured = {}
    if store._dir().exists():
        for path in store._dir().glob('*'):
            path.unlink()

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': meta}))
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, leverage, meta, args: live_execution)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})

    book_ticker_calls = []

    def fake_run_book_ticker_websocket_supervisor(store, initial_symbols, symbol_provider, ws_module, **kwargs):
        book_ticker_calls.append({
            'store': store,
            'initial_symbols': list(initial_symbols),
            'provided_symbols': list(symbol_provider()),
            'ws_module': ws_module,
            'kwargs': kwargs,
        })
        store.save_json('book_ticker_ws_status', {
            'status': 'healthy',
            'symbols': ['DOGEUSDT'],
            'symbol_count': 1,
            'reconnect_count': 0,
            'subscription_version': 1,
            'messages_processed': 3,
            'samples_written': 3,
            'active_streams': ['dogeusdt@bookTicker'],
            'last_error': '',
        })
        return {
            'cycles_completed': 1,
            'reconnect_count': 0,
            'messages_processed_total': 3,
            'samples_written_total': 3,
            'symbols': ['DOGEUSDT'],
            'subscription_version': 1,
        }

    monkeypatch.setattr(mod, 'run_book_ticker_websocket_supervisor', fake_run_book_ticker_websocket_supervisor)
    monkeypatch.setattr(mod, 'resolve_auto_loop_book_ticker_symbols', lambda client, args: ['DOGEUSDT'], raising=False)
    monkeypatch.setattr(mod, 'websocket', SimpleNamespace(create_connection=lambda *args, **kwargs: None), raising=False)

    def fake_start_trade_monitor_thread(*args, **kwargs):
        captured['kwargs'] = kwargs
        return SimpleNamespace(name='trade-monitor-DOGEUSDT')

    monkeypatch.setattr(mod, 'start_trade_monitor_thread', fake_start_trade_monitor_thread)

    result = mod.run_loop(DummyClient(), make_args(live=True, auto_loop=True))

    cycle = result['cycles'][0]
    assert cycle['trade_management']['mode'] == 'background_thread'
    assert captured['kwargs']['store'] is store
    assert captured['kwargs']['symbol'] == 'DOGEUSDT'
    assert cycle['book_ticker_websocket']['symbols'] == ['DOGEUSDT']
    assert book_ticker_calls[0]['store'] is store
    assert book_ticker_calls[0]['initial_symbols'] == ['DOGEUSDT']
    assert book_ticker_calls[0]['provided_symbols'] == ['DOGEUSDT']

    positions = store.load_json('positions', {})
    tracked = positions['DOGEUSDT:LONG']
    assert tracked['status'] == 'monitoring'
    assert tracked['monitor_mode'] == 'background_thread'
    assert tracked['monitor_thread_name'] == 'trade-monitor-DOGEUSDT'
    assert tracked['user_data_stream']['status'] == 'started'
    assert tracked['user_data_stream']['listen_key'] == 'dumm***-key'
    assert result['cycles'][0]['trade_management']['user_data_stream']['listen_key'] == 'dummy-listen-key'
    assert tracked['book_ticker_websocket']['status'] == 'healthy'
    assert tracked['book_ticker_websocket']['active_streams'] == ['dogeusdt@bookTicker']

    events_path = store._events_path()
    assert events_path.exists()
    rows = [line for line in events_path.read_text(encoding='utf-8').splitlines() if line.strip()]
    assert rows
    assert 'buy_fill_confirmed' in rows[-1]
    assert 'background_thread' in rows[-1]


def test_run_loop_auto_loop_starts_book_ticker_supervisor_before_scan(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    call_order = []
    candidate = SimpleNamespace(
        symbol='DOGEUSDT',
        quantity=12.5,
        stop_price=0.1234,
        recommended_leverage=3,
    )
    meta = SimpleNamespace(symbol='DOGEUSDT')
    live_execution = {
        'entry_price': 0.135,
        'trade_management_plan': {'quantity': 12.5, 'entry_price': 0.135, 'stop_price': 0.1234, 'initial_risk_per_unit': 0.0116, 'side': 'long', 'position_side': 'LONG'},
        'stop_order': {'orderId': 98765},
        'protection_check': {'status': 'protected'},
    }

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, leverage, meta, args: live_execution)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'run_user_data_stream_monitor_cycle', lambda **kwargs: {'status': 'started', 'listen_key': 'dummy-listen-key', 'health': {}, 'action': 'started', 'now_utc': '2026-04-20T00:00:00+00:00'})
    monkeypatch.setattr(mod, 'start_trade_monitor_thread', lambda *args, **kwargs: SimpleNamespace(name='trade-monitor-DOGEUSDT'))
    monkeypatch.setattr(mod, 'websocket', SimpleNamespace(create_connection=lambda *args, **kwargs: None), raising=False)

    def fake_ensure_book_ticker_supervisor(*, store, symbol_provider, ws_module):
        call_order.append('book_ticker_supervisor')
        store.save_json('book_ticker_ws_status', {
            'status': 'healthy',
            'symbols': ['BTCUSDT', 'ETHUSDT'],
            'symbol_count': 2,
            'reconnect_count': 0,
            'subscription_version': 1,
            'messages_processed': 8,
            'samples_written': 8,
            'active_streams': ['btcusdt@bookTicker', 'ethusdt@bookTicker'],
            'last_error': '',
        })
        assert list(symbol_provider()) == ['BTCUSDT', 'ETHUSDT']
        return {
            'mode': 'background_thread',
            'running': True,
            'symbols': ['BTCUSDT', 'ETHUSDT'],
            'thread_name': 'book-ticker-ws-supervisor',
        }

    def fake_run_scan_once(client, args):
        call_order.append('run_scan_once')
        health = store.load_json('book_ticker_ws_status', {})
        assert health['symbols'] == ['BTCUSDT', 'ETHUSDT']
        return ({'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': meta})

    monkeypatch.setattr(mod, 'ensure_auto_loop_book_ticker_websocket_supervisor_running', fake_ensure_book_ticker_supervisor)
    monkeypatch.setattr(mod, 'run_scan_once', fake_run_scan_once)
    monkeypatch.setattr(mod, 'resolve_auto_loop_book_ticker_symbols', lambda client, args: ['BTCUSDT', 'ETHUSDT'], raising=False)

    result = mod.run_loop(DummyClient(), make_args(live=True, auto_loop=True, runtime_state_dir=str(tmp_path)))

    assert call_order[:2] == ['book_ticker_supervisor', 'run_scan_once']
    assert result['cycles'][0]['book_ticker_websocket']['symbols'] == ['BTCUSDT', 'ETHUSDT']


def test_run_loop_auto_loop_persists_book_ticker_supervisor_health_without_live_trade(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = None
    book_ticker_calls = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False, 'halt_reason': '', 'symbol_cooldowns': {}, 'daily_realized_pnl_usdt': 0.0, 'consecutive_losses': 0})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *args, **kwargs: ({'ok': True, 'candidate_count': 0, 'candidates': []}, candidate, {}))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'websocket', SimpleNamespace(create_connection=lambda *args, **kwargs: None), raising=False)

    def fake_ensure_book_ticker_supervisor(*, store, symbol_provider, ws_module):
        book_ticker_calls.append({
            'store': store,
            'provided_symbols': list(symbol_provider()),
            'ws_module': ws_module,
        })
        store.save_json('book_ticker_ws_status', {
            'status': 'healthy',
            'symbols': ['DOGEUSDT'],
            'symbol_count': 1,
            'reconnect_count': 0,
            'subscription_version': 1,
            'messages_processed': 5,
            'samples_written': 5,
            'active_streams': ['dogeusdt@bookTicker'],
            'last_error': '',
        })
        return {
            'mode': 'background_thread',
            'running': True,
            'symbols': ['DOGEUSDT'],
            'thread_name': 'book-ticker-ws-supervisor',
        }

    monkeypatch.setattr(mod, 'ensure_auto_loop_book_ticker_websocket_supervisor_running', fake_ensure_book_ticker_supervisor)
    monkeypatch.setattr(mod, 'resolve_auto_loop_book_ticker_symbols', lambda client, args: ['DOGEUSDT'], raising=False)

    result = mod.run_loop(DummyClient(), make_args(auto_loop=True, live=False, runtime_state_dir=str(tmp_path)))

    cycle = result['cycles'][0]
    assert cycle['book_ticker_websocket']['symbols'] == ['DOGEUSDT']
    assert cycle['book_ticker_websocket']['health']['status'] == 'healthy'
    assert cycle['book_ticker_websocket']['health']['active_streams'] == ['dogeusdt@bookTicker']
    assert book_ticker_calls[0]['store'] is store
    assert book_ticker_calls[0]['provided_symbols'] == ['DOGEUSDT']


def test_run_loop_auto_loop_records_book_ticker_unavailable_when_dependency_missing(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False, 'halt_reason': '', 'symbol_cooldowns': {}, 'daily_realized_pnl_usdt': 0.0, 'consecutive_losses': 0})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *args, **kwargs: ({'ok': True, 'candidate_count': 0, 'candidates': []}, None, {}))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'websocket', None, raising=False)

    result = mod.run_loop(DummyClient(), make_args(auto_loop=True, live=False, runtime_state_dir=str(tmp_path)))

    cycle = result['cycles'][0]
    assert cycle['book_ticker_websocket'] == {
        'status': 'unavailable',
        'reason': 'websocket_client_missing',
    }
    events = store.read_events(limit=10)
    assert any(row['event_type'] == 'book_ticker_ws_unavailable' for row in events)


def test_run_loop_auto_loop_hard_gates_live_trade_when_book_ticker_websocket_unavailable(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = mod.Candidate(
        symbol='DOGEUSDT',
        last_price=0.12,
        price_change_pct_24h=8.0,
        quote_volume_24h=5_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.5,
        acceleration_ratio_5m_vs_15m=1.2,
        breakout_level=0.119,
        recent_swing_low=0.115,
        stop_price=0.114,
        quantity=100.0,
        risk_per_unit=0.006,
        recommended_leverage=3,
        rsi_5m=66.0,
        volume_multiple=1.6,
        distance_from_ema20_5m_pct=2.0,
        distance_from_vwap_15m_pct=1.7,
        higher_tf_summary={'1h': 'up'},
        score=70.0,
        reasons=['candidate_selected'],
        state='launch',
        state_reasons=['impulse_ready'],
    )
    placed = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False, 'halt_reason': '', 'symbol_cooldowns': {}, 'daily_realized_pnl_usdt': 0.0, 'consecutive_losses': 0})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'evaluate_portfolio_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *args, **kwargs: ({'ok': True, 'candidate_count': 1, 'candidates': [{'symbol': 'DOGEUSDT'}]}, candidate, {'DOGEUSDT': {'score': candidate.score, 'quote_volume_24h': candidate.quote_volume_24h}}))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'websocket', None, raising=False)
    monkeypatch.setattr(mod, 'place_live_trade', lambda *a, **k: placed.append(True) or {'symbol': 'DOGEUSDT'})

    result = mod.run_loop(DummyClient(), make_args(auto_loop=True, live=True, runtime_state_dir=str(tmp_path)))

    cycle = result['cycles'][0]
    assert placed == []
    assert cycle['book_ticker_websocket'] == {
        'status': 'unavailable',
        'reason': 'websocket_client_missing',
    }
    assert cycle['live_skipped_due_to_websocket_gate'] == ['book_ticker_websocket_unavailable:websocket_client_missing']


def test_run_loop_auto_loop_refreshes_existing_user_data_stream_without_new_trade(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('user_data_stream', {
        'symbol': 'DOGEUSDT',
        'listen_key': 'existing-listen-key',
        'status': 'started',
        'started_at': '2026-04-20T12:00:00Z',
        'last_refresh_at': '2026-04-20T12:00:00Z',
        'updated_at': '2026-04-20T12:00:00Z',
    })
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_key': 'DOGEUSDT:LONG',
            'quantity': 12.5,
            'status': 'monitoring',
        }
    })
    monitor_calls = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False, 'halt_reason': '', 'symbol_cooldowns': {}, 'daily_realized_pnl_usdt': 0.0, 'consecutive_losses': 0})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *args, **kwargs: ({'ok': True, 'candidate_count': 0, 'candidates': []}, None, {}))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})

    def fake_run_user_data_stream_monitor_cycle(client, store, symbol=None, now=None, refresh_interval_minutes=30.0, disconnect_timeout_minutes=65.0):
        monitor_calls.append({
            'symbol': symbol,
            'refresh_interval_minutes': refresh_interval_minutes,
            'disconnect_timeout_minutes': disconnect_timeout_minutes,
        })
        return {
            'listen_key': 'existing-listen-key',
            'status': 'refreshed',
            'action': 'refreshed',
            'health': {
                'symbol': 'DOGEUSDT',
                'listen_key': 'existing-listen-key',
                'status': 'refreshed',
                'refresh_failure_count': 0,
                'disconnect_count': 0,
                'updated_at': '2026-04-20T12:31:00Z',
            },
            'now_utc': '2026-04-20T12:31:00Z',
        }

    monkeypatch.setattr(mod, 'run_user_data_stream_monitor_cycle', fake_run_user_data_stream_monitor_cycle, raising=False)

    result = mod.run_loop(DummyClient(), make_args(
        live=True,
        auto_loop=True,
        runtime_state_dir=str(tmp_path),
        user_stream_refresh_interval_minutes=12.0,
        user_stream_disconnect_timeout_minutes=34.0,
    ))

    assert monitor_calls == [{'symbol': 'DOGEUSDT', 'refresh_interval_minutes': 12.0, 'disconnect_timeout_minutes': 34.0}]
    cycle = result['cycles'][0]
    assert cycle['user_data_stream_monitor']['status'] == 'refreshed'
    assert cycle['scan']['candidate_count'] == 0
    tracked = store.load_json('positions', {})['DOGEUSDT:LONG']
    assert tracked['user_data_stream']['status'] == 'refreshed'
    assert tracked['user_data_stream']['listen_key'] == 'exis***-key'


def test_run_loop_auto_loop_alerts_existing_user_data_stream_refresh_failure(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('user_data_stream', {
        'symbol': 'DOGEUSDT',
        'listen_key': 'existing-listen-key',
        'status': 'started',
        'started_at': '2026-04-20T12:00:00Z',
        'last_refresh_at': '2026-04-20T12:00:00Z',
        'updated_at': '2026-04-20T12:00:00Z',
    })
    notifications = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False, 'halt_reason': '', 'symbol_cooldowns': {}, 'daily_realized_pnl_usdt': 0.0, 'consecutive_losses': 0})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *args, **kwargs: ({'ok': True, 'candidate_count': 0, 'candidates': []}, None, {}))
    monkeypatch.setattr(mod, 'emit_notification', lambda args, event_type, payload: notifications.append((event_type, payload)) or {'ok': True})
    monkeypatch.setattr(mod, 'run_user_data_stream_monitor_cycle', lambda **kwargs: {
        'listen_key': 'existing-listen-key',
        'status': 'refresh_failed',
        'action': 'refresh_failed',
        'health': {
            'symbol': 'DOGEUSDT',
            'listen_key': 'existing-listen-key',
            'status': 'refresh_failed',
            'detail': 'refresh timeout',
            'refresh_failure_count': 2,
            'disconnect_count': 0,
            'reconnect_count': 0,
            'updated_at': '2026-04-20T12:31:00Z',
        },
        'error': 'refresh timeout',
        'now_utc': '2026-04-20T12:31:00Z',
    }, raising=False)

    result = mod.run_loop(DummyClient(), make_args(live=True, auto_loop=True, runtime_state_dir=str(tmp_path), disable_notify=False))

    assert result['cycles'][0]['user_data_stream_monitor']['status'] == 'refresh_failed'
    assert result['cycles'][0]['user_data_stream_alert']['refresh_failure_count'] == 2
    assert notifications == [(
        'user_data_stream_alert',
        {
            'symbol': 'DOGEUSDT',
            'listen_key': 'exis***-key',
            'status': 'refresh_failed',
            'action': 'refresh_failed',
            'error': 'refresh timeout',
            'detail': 'refresh timeout',
            'disconnect_count': 0,
            'refresh_failure_count': 2,
            'reconnect_count': 0,
            'started_at': None,
            'last_refresh_at': None,
            'updated_at': '2026-04-20T12:31:00Z',
        },
    )]



def test_run_user_data_stream_monitor_cycle_restarts_when_listen_key_missing(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('user_data_stream', {
        'symbol': 'SOLUSDT',
        'listen_key': 'expired-listen-key',
        'status': 'started',
        'started_at': '2026-05-03T07:00:00Z',
        'last_refresh_at': '2026-05-03T07:30:00Z',
        'updated_at': '2026-05-03T07:30:00Z',
        'refresh_failure_count': 48,
    })

    class DummyClient:
        def signed_put(self, path, params=None):
            raise mod.BinanceAPIError("Binance API error 400: {'code': -1125, 'msg': 'This listenKey does not exist.'}")

    started = []

    def fake_start_user_data_stream_monitor(client, store, symbol=None, now=None):
        started.append({'symbol': symbol, 'now': now.isoformat() if now else None})
        health = {
            'symbol': symbol,
            'listen_key': 'new-listen-key',
            'status': 'started',
            'detail': 'listen_key_started',
            'disconnect_count': 0,
            'refresh_failure_count': 0,
            'reconnect_count': 1,
            'started_at': '2026-05-03T08:00:00Z',
            'last_refresh_at': '2026-05-03T08:00:00Z',
            'updated_at': '2026-05-03T08:00:00Z',
        }
        store.save_json('user_data_stream', dict(health))
        return {
            'listen_key': 'new-listen-key',
            'status': 'started',
            'action': 'started',
            'health': health,
        }

    monkeypatch.setattr(mod, 'start_user_data_stream_monitor', fake_start_user_data_stream_monitor)

    result = mod.run_user_data_stream_monitor_cycle(
        DummyClient(),
        store,
        symbol='SOLUSDT',
        now=datetime.datetime(2026, 5, 3, 8, 0, tzinfo=datetime.timezone.utc),
        refresh_interval_minutes=5.0,
        disconnect_timeout_minutes=65.0,
    )

    assert started == [{'symbol': 'SOLUSDT', 'now': '2026-05-03T08:00:00+00:00'}]
    assert result['status'] == 'started'
    assert result['action'] == 'restarted_after_missing_listen_key'
    assert result['listen_key'] == 'new-listen-key'
    assert result['previous_listen_key'] == 'expired-listen-key'
    assert result['recovery_reason'] == 'listen_key_missing'
    assert 'listenKey does not exist' in result['recovery_error']
    assert result['health']['previous_listen_key'] == 'expired-listen-key'
    assert result['health']['recovery_reason'] == 'listen_key_missing'
    assert store.load_json('user_data_stream', {})['listen_key'] == 'new-listen-key'


    mod = load_module()
    candidate = SimpleNamespace(
        symbol='DOGEUSDT',
        quantity=12.78,
        last_price=0.135,
        stop_price=0.1234,
        atr_stop_distance=0.01,
        risk_per_unit=0.01,
        expected_slippage_pct=0.0,
        book_depth_fill_ratio=1.0,
    )
    meta = SimpleNamespace(step_size=0.1, quantity_precision=1)
    args = make_args(profile='test-profile')
    calls = []
    runtime_events = []
    notifications = []

    class Client:
        def signed_post(self, path, params):
            calls.append((path, params))
            if path == '/fapi/v1/marginType':
                return {'code': 200, 'msg': 'success'}
            if path == '/fapi/v1/leverage':
                return {'leverage': params['leverage']}
            if path == '/fapi/v1/order':
                return {
                    'symbol': 'DOGEUSDT',
                    'orderId': 12345,
                    'status': 'FILLED',
                    'avgPrice': '0.1365',
                    'executedQty': '12.7',
                    'cumQuote': '1.73355',
                    'updateTime': 1710000000123,
                    'clientOrderId': 'entry-order-1',
                }
            raise AssertionError(f'unexpected path: {path}')

    monkeypatch.setattr(mod, 'log_runtime_event', lambda event_type, payload: runtime_events.append((event_type, payload)))
    monkeypatch.setattr(mod, 'emit_notification', lambda args, event_type, payload, post_func=None: notifications.append((event_type, payload)) or {'ok': True})
    monkeypatch.setattr(mod, 'place_stop_market_order', lambda *a, **k: {'orderId': 54321, 'clientOrderId': 'stop-1'})
    monkeypatch.setattr(mod, 'place_take_profit_market_order', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *a, **k: {'status': 'protected', 'expected_order_id': 54321})

    result = mod.place_live_trade(Client(), candidate, leverage=5, meta=meta, args=args)

    assert calls[0][0] == '/fapi/v1/marginType'
    assert calls[0][1]['marginType'] == 'ISOLATED'
    assert calls[1][0] == '/fapi/v1/leverage'
    assert calls[2][0] == '/fapi/v1/order'
    assert calls[2][1]['quantity'] == '12.7'
    assert runtime_events[0][0] == 'entry_filled'
    assert notifications[0][0] == 'entry_filled'
    assert runtime_events[1][0] == 'initial_stop_place_attempt_succeeded'
    assert runtime_events[2][0] == 'initial_stop_placed'
    assert notifications[1][0] == 'initial_stop_placed'
    assert result['entry_price'] == 0.1365
    assert result['filled_quantity'] == 12.7
    assert result['entry_order_feedback']['order_id'] == 12345
    assert result['entry_order_feedback']['avg_price'] == 0.1365
    assert result['entry_order_feedback']['executed_qty'] == 12.7
    assert result['entry_order_feedback']['cum_quote'] == 1.73355
    assert result['entry_order_feedback']['status'] == 'FILLED'
    assert result['entry_order_feedback']['client_order_id'] == 'entry-order-1'
    assert result['entry_order_feedback']['update_time'] == 1710000000123
    assert result['margin_type'] == 'ISOLATED'
    assert result['margin_type_check']['ok'] is True
    assert result['trade_management_plan']['breakeven_confirmation_mode'] == 'ema_support'
    assert result['trade_management_plan']['breakeven_min_buffer_pct'] == 0.001


def test_run_loop_background_buy_fill_confirmed_persists_entry_feedback(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = SimpleNamespace(
        symbol='DOGEUSDT',
        quantity=12.5,
        stop_price=0.1234,
        recommended_leverage=3,
        portfolio_narrative_bucket='meme',
        portfolio_correlation_group='dog-family',
    )
    meta = SimpleNamespace(symbol='DOGEUSDT')
    live_execution = {
        'entry_price': 0.1365,
        'filled_quantity': 12.7,
        'trade_management_plan': {'quantity': 12.5, 'entry_price': 0.1365, 'stop_price': 0.1234, 'initial_risk_per_unit': 0.0131, 'side': 'long', 'position_side': 'LONG'},
        'stop_order': {'orderId': 98765},
        'protection_check': {'status': 'protected'},
        'entry_order_feedback': {
            'order_id': 12345,
            'client_order_id': 'entry-order-1',
            'status': 'FILLED',
            'avg_price': 0.1365,
            'executed_qty': 12.7,
            'cum_quote': 1.73355,
            'update_time': 1710000000123,
        },
    }

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': meta}))
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, leverage, meta, args: live_execution)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'start_trade_monitor_thread', lambda *args, **kwargs: SimpleNamespace(name='trade-monitor-DOGEUSDT'))

    result = mod.run_loop(DummyClient(), make_args(live=True, auto_loop=True, runtime_state_dir=str(tmp_path)))

    assert result['cycles'][0]['trade_management']['mode'] == 'background_thread'
    positions = store.load_json('positions', {})
    tracked = positions['DOGEUSDT:LONG']
    assert tracked['status'] == 'monitoring'
    assert tracked['entry_order_id'] == 12345
    assert tracked['entry_client_order_id'] == 'entry-order-1'
    assert tracked['entry_order_status'] == 'FILLED'
    assert tracked['filled_quantity'] == 12.7
    assert tracked['entry_cum_quote'] == 1.73355
    assert tracked['entry_update_time'] == 1710000000123
    assert tracked['portfolio_narrative_bucket'] == 'meme'
    assert tracked['portfolio_correlation_group'] == 'dog-family'
    assert tracked['user_data_stream']['status'] == 'started'
    assert tracked['user_data_stream']['listen_key'] == 'dumm***-key'
    assert result['cycles'][0]['trade_management']['user_data_stream']['listen_key'] == 'dummy-listen-key'

    rows = [mod.json.loads(line) for line in store._events_path().read_text(encoding='utf-8').splitlines() if line.strip()]
    buy_fill = rows[-1]
    assert buy_fill['event_type'] == 'buy_fill_confirmed'
    assert buy_fill['entry_order_id'] == 12345
    assert buy_fill['entry_client_order_id'] == 'entry-order-1'
    assert buy_fill['entry_order_status'] == 'FILLED'
    assert buy_fill['filled_quantity'] == 12.7
    assert buy_fill['entry_cum_quote'] == 1.73355
    assert buy_fill['entry_update_time'] == 1710000000123


def test_run_loop_live_open_persists_candidate_portfolio_buckets_before_monitor(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = SimpleNamespace(
        symbol='SUIUSDT',
        quantity=4.0,
        stop_price=1.9,
        recommended_leverage=3,
        portfolio_narrative_bucket='l1-beta',
        portfolio_correlation_group='move-family',
    )
    meta = SimpleNamespace(symbol='SUIUSDT')
    live_execution = {
        'symbol': 'SUIUSDT',
        'side': 'LONG',
        'entry_price': 2.1,
        'filled_quantity': 4.0,
        'trade_management_plan': {'quantity': 4.0},
        'stop_order': {'orderId': 98765},
        'protection_check': {'status': 'protected'},
        'entry_order_feedback': {
            'order_id': 12345,
            'client_order_id': 'entry-order-1',
            'status': 'FILLED',
        },
    }

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidates': ['SUIUSDT']}, candidate, {'SUIUSDT': meta}))
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, leverage, meta, args: live_execution)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'monitor_live_trade', lambda **kwargs: {'status': 'stubbed'})

    result = mod.run_loop(DummyClient(), make_args(live=True, auto_loop=False, runtime_state_dir=str(tmp_path)))

    assert result['cycles'][0]['trade_management'] == {'status': 'stubbed'}
    positions = store.load_json('positions', {})
    tracked = positions['SUIUSDT:LONG']
    assert tracked['portfolio_narrative_bucket'] == 'l1-beta'
    assert tracked['portfolio_correlation_group'] == 'move-family'
    assert tracked['position_key'] == 'SUIUSDT:LONG'


def test_run_loop_background_buy_fill_confirmed_persists_short_position_key_and_side(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = SimpleNamespace(
        symbol='DOGEUSDT',
        quantity=12.5,
        stop_price=0.1434,
        recommended_leverage=3,
    )
    meta = SimpleNamespace(symbol='DOGEUSDT')
    live_execution = {
        'symbol': 'DOGEUSDT',
        'side': 'SHORT',
        'entry_price': 0.1365,
        'filled_quantity': 12.7,
        'trade_management_plan': {'quantity': 12.5, 'entry_price': 0.1365, 'stop_price': 0.1434, 'initial_risk_per_unit': 0.0069, 'side': 'short', 'position_side': 'SHORT'},
        'stop_order': {'orderId': 98765},
        'protection_check': {'status': 'protected'},
        'entry_order_feedback': {
            'order_id': 12345,
            'client_order_id': 'entry-order-1',
            'status': 'FILLED',
            'avg_price': 0.1365,
            'executed_qty': 12.7,
            'cum_quote': 1.73355,
            'update_time': 1710000000123,
        },
    }

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': meta}))
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, leverage, meta, args: live_execution)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(mod, 'start_trade_monitor_thread', lambda *args, **kwargs: SimpleNamespace(name='trade-monitor-DOGEUSDT-SHORT'))

    result = mod.run_loop(DummyClient(), make_args(live=True, auto_loop=True, runtime_state_dir=str(tmp_path)))

    assert result['cycles'][0]['trade_management']['mode'] == 'background_thread'
    positions = store.load_json('positions', {})
    tracked = positions['DOGEUSDT:SHORT']
    assert tracked['status'] == 'monitoring'
    assert tracked['side'] == 'short'
    assert tracked['position_key'] == 'DOGEUSDT:SHORT'
    assert tracked['monitor_thread_name'] == 'trade-monitor-DOGEUSDT-SHORT'

    rows = [mod.json.loads(line) for line in store._events_path().read_text(encoding='utf-8').splitlines() if line.strip()]
    buy_fill = rows[-1]
    assert buy_fill['event_type'] == 'buy_fill_confirmed'
    assert buy_fill['side'] == 'SHORT'
    assert buy_fill['position_key'] == 'DOGEUSDT:SHORT'
    assert buy_fill['entry_order_id'] == 12345
    assert buy_fill['entry_client_order_id'] == 'entry-order-1'
    assert buy_fill['entry_order_status'] == 'FILLED'


def test_place_live_trade_retries_initial_stop_with_live_position_quantity(monkeypatch):
    mod = load_module()
    candidate = mod.Candidate(
        symbol='DOGEUSDT',
        last_price=0.1365,
        price_change_pct_24h=8.4,
        quote_volume_24h=75_000_000.0,
        hot_rank=1,
        gainer_rank=2,
        funding_rate=-0.0002,
        funding_rate_avg=-0.0001,
        recent_5m_change_pct=1.8,
        acceleration_ratio_5m_vs_15m=1.4,
        breakout_level=0.14,
        recent_swing_low=0.1234,
        stop_price=0.1234,
        quantity=12.5,
        risk_per_unit=0.0131,
        recommended_leverage=5,
        rsi_5m=68.0,
        volume_multiple=2.2,
        distance_from_ema20_5m_pct=1.1,
        distance_from_vwap_15m_pct=0.8,
        higher_tf_summary={'1h': 'trend_up'},
        score=72.0,
        reasons=['candidate_selected'],
        side='LONG',
        position_side='LONG',
        state='launch',
        state_reasons=['launch_breakout'],
        alert_tier='critical',
        position_size_pct=3.3,
        regime_label='risk_on',
        regime_multiplier=1.0,
        side_risk_multiplier=1.0,
        quality_score=18.2,
        execution_priority_score=9.1,
        setup_ready=True,
        trigger_fired=True,
        candidate_stage='launch',
        expected_slippage_pct=0.04,
        book_depth_fill_ratio=0.92,
        spread_bps=2.1,
        orderbook_slope=1.3,
        cancel_rate=0.04,
        portfolio_narrative_bucket='meme',
        portfolio_correlation_group='dog-family',
    )
    meta = SimpleNamespace(
        symbol='DOGEUSDT',
        price_precision=4,
        quantity_precision=1,
        tick_size=0.0001,
        step_size=0.1,
        min_qty=0.1,
    )
    args = make_args(initial_stop_max_attempts=3, initial_stop_retry_sleep_sec=0.0)
    runtime_events = []
    notifications = []
    stop_calls = []
    position_snapshots = [
        [],
        [{'symbol': 'DOGEUSDT', 'positionSide': 'LONG', 'positionAmt': '12.7', 'entryPrice': '0.1365'}],
        [{'symbol': 'DOGEUSDT', 'positionSide': 'LONG', 'positionAmt': '12.8', 'entryPrice': '0.1365'}],
    ]
    fetch_positions_calls = {'count': 0}

    class Client:
        def signed_post(self, path, params):
            if path == '/fapi/v1/marginType':
                return {'code': 200, 'msg': 'success'}
            if path == '/fapi/v1/leverage':
                return {'leverage': params['leverage']}
            if path == '/fapi/v1/order':
                return {
                    'symbol': 'DOGEUSDT',
                    'orderId': 12345,
                    'status': 'FILLED',
                    'avgPrice': '0.1365',
                    'executedQty': '12.7',
                    'cumQuote': '1.73355',
                    'updateTime': 1710000000123,
                    'clientOrderId': 'entry-order-1',
                }
            raise AssertionError(f'unexpected path: {path}')

    def fake_fetch_open_positions(client):
        idx = min(fetch_positions_calls['count'], len(position_snapshots) - 1)
        fetch_positions_calls['count'] += 1
        return position_snapshots[idx]

    def fake_place_stop_market_order(client, symbol, stop_price, quantity, meta, side=None):
        stop_calls.append({'symbol': symbol, 'stop_price': stop_price, 'quantity': quantity, 'side': side})
        if len(stop_calls) == 1:
            raise mod.BinanceAPIError('APIError(code=-5021): exchange busy')
        return {'orderId': 54321, 'clientOrderId': 'stop-1'}

    monkeypatch.setattr(mod, 'fetch_open_positions', fake_fetch_open_positions)
    monkeypatch.setattr(mod, 'fetch_open_orders', lambda client, symbol: [])
    monkeypatch.setattr(mod, 'fetch_open_algo_orders', lambda client, symbol: [])
    monkeypatch.setattr(mod, 'log_runtime_event', lambda event_type, payload: runtime_events.append((event_type, dict(payload))))
    monkeypatch.setattr(mod, 'emit_notification', lambda args, event_type, payload, post_func=None: notifications.append((event_type, dict(payload))) or {'ok': True})
    monkeypatch.setattr(mod, 'place_stop_market_order', fake_place_stop_market_order)
    monkeypatch.setattr(mod, 'place_take_profit_market_order', lambda *a, **k: {'orderId': 60001})
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *a, **k: {'status': 'protected', 'expected_order_id': 54321})
    monkeypatch.setattr(mod.time, 'sleep', lambda *_a, **_k: None)

    result = mod.place_live_trade(Client(), candidate, leverage=5, meta=meta, args=args)

    assert [call['quantity'] for call in stop_calls] == [12.7, 12.8]
    assert runtime_events[1][0] == 'initial_stop_place_attempt_failed'
    assert runtime_events[1][1]['attempt'] == 1
    assert runtime_events[1][1]['quantity'] == 12.7
    assert runtime_events[2][0] == 'initial_stop_place_attempt_succeeded'
    assert runtime_events[2][1]['attempt'] == 2
    assert runtime_events[2][1]['quantity'] == 12.8
    assert notifications[1][0] == 'initial_stop_place_attempt_failed'
    assert notifications[2][0] == 'initial_stop_place_attempt_succeeded'
    assert runtime_events[3][0] == 'initial_stop_placed'
    assert result['stop_order']['orderId'] == 54321


def test_place_live_trade_raises_after_initial_stop_retries_exhausted(monkeypatch):
    mod = load_module()
    candidate = mod.Candidate(
        symbol='DOGEUSDT',
        last_price=0.1365,
        price_change_pct_24h=8.4,
        quote_volume_24h=75_000_000.0,
        hot_rank=1,
        gainer_rank=2,
        funding_rate=-0.0002,
        funding_rate_avg=-0.0001,
        recent_5m_change_pct=1.8,
        acceleration_ratio_5m_vs_15m=1.4,
        breakout_level=0.14,
        recent_swing_low=0.1234,
        stop_price=0.1234,
        quantity=12.5,
        risk_per_unit=0.0131,
        recommended_leverage=5,
        rsi_5m=68.0,
        volume_multiple=2.2,
        distance_from_ema20_5m_pct=1.1,
        distance_from_vwap_15m_pct=0.8,
        higher_tf_summary={'1h': 'trend_up'},
        score=72.0,
        reasons=['candidate_selected'],
        side='LONG',
        position_side='LONG',
        state='launch',
        state_reasons=['launch_breakout'],
        alert_tier='critical',
        position_size_pct=3.3,
        regime_label='risk_on',
        regime_multiplier=1.0,
        side_risk_multiplier=1.0,
        quality_score=18.2,
        execution_priority_score=9.1,
        setup_ready=True,
        trigger_fired=True,
        candidate_stage='launch',
        expected_slippage_pct=0.04,
        book_depth_fill_ratio=0.92,
        spread_bps=2.1,
        orderbook_slope=1.3,
        cancel_rate=0.04,
        portfolio_narrative_bucket='meme',
        portfolio_correlation_group='dog-family',
    )
    meta = SimpleNamespace(
        symbol='DOGEUSDT',
        price_precision=4,
        quantity_precision=1,
        tick_size=0.0001,
        step_size=0.1,
        min_qty=0.1,
    )
    args = make_args(initial_stop_max_attempts=3, initial_stop_retry_sleep_sec=0.0)
    runtime_events = []
    notifications = []
    stop_calls = []

    class Client:
        def signed_post(self, path, params):
            if path == '/fapi/v1/marginType':
                return {'code': 200, 'msg': 'success'}
            if path == '/fapi/v1/leverage':
                return {'leverage': params['leverage']}
            if path == '/fapi/v1/order':
                return {
                    'symbol': 'DOGEUSDT',
                    'orderId': 12345,
                    'status': 'FILLED',
                    'avgPrice': '0.1365',
                    'executedQty': '12.7',
                    'cumQuote': '1.73355',
                    'updateTime': 1710000000123,
                    'clientOrderId': 'entry-order-1',
                }
            raise AssertionError(f'unexpected path: {path}')

    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'DOGEUSDT', 'positionSide': 'LONG', 'positionAmt': '12.9', 'entryPrice': '0.1365'}] if stop_calls else [])
    monkeypatch.setattr(mod, 'fetch_open_orders', lambda client, symbol: [])
    monkeypatch.setattr(mod, 'fetch_open_algo_orders', lambda client, symbol: [])
    monkeypatch.setattr(mod, 'log_runtime_event', lambda event_type, payload: runtime_events.append((event_type, dict(payload))))
    monkeypatch.setattr(mod, 'emit_notification', lambda args, event_type, payload, post_func=None: notifications.append((event_type, dict(payload))) or {'ok': True})
    monkeypatch.setattr(mod.time, 'sleep', lambda *_a, **_k: None)

    def always_fail_stop(client, symbol, stop_price, quantity, meta, side=None):
        stop_calls.append({'symbol': symbol, 'quantity': quantity, 'side': side})
        raise mod.BinanceAPIError('APIError(code=-5021): exchange busy')

    monkeypatch.setattr(mod, 'place_stop_market_order', always_fail_stop)

    with pytest.raises(mod.BinanceAPIError, match='开仓成功，但初始止损重挂全部失败'):
        mod.place_live_trade(Client(), candidate, leverage=5, meta=meta, args=args)

    assert [call['quantity'] for call in stop_calls] == [12.7, 12.9, 12.9]
    assert [event_type for event_type, _ in runtime_events] == [
        'entry_filled',
        'initial_stop_place_attempt_failed',
        'initial_stop_place_attempt_failed',
        'initial_stop_place_attempt_failed',
        'initial_stop_retry_exhausted',
    ]
    assert notifications[-1][0] == 'initial_stop_retry_exhausted'
    assert runtime_events[-1][1]['max_attempts'] == 3


def test_run_loop_live_skips_when_short_position_already_open(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = SimpleNamespace(
        symbol='DOGEUSDT',
        quantity=12.5,
        stop_price=0.1434,
        recommended_leverage=3,
    )
    meta = SimpleNamespace(symbol='DOGEUSDT')

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': meta}))
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'DOGEUSDT', 'positionAmt': '-2.5', 'positionSide': 'SHORT'}])
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})
    candidate.state = 'launch'
    candidate.alert_tier = 'A'
    candidate.score = 72.0
    candidate.must_pass_flags = {}
    candidate.quality_score = 0.0
    candidate.execution_priority_score = 0.0
    candidate.entry_distance_from_breakout_pct = 0.0
    candidate.entry_distance_from_vwap_pct = 0.0
    candidate.candle_extension_pct = 0.0
    candidate.recent_3bar_runup_pct = 0.0
    candidate.overextension_flag = False
    candidate.entry_pattern = 'breakout'
    candidate.trend_regime = 'trend'
    candidate.liquidity_grade = 'A'
    candidate.setup_ready = True
    candidate.trigger_fired = True
    candidate.expected_slippage_pct = 0.0
    candidate.book_depth_fill_ratio = 0.0

    def unexpected(*args, **kwargs):
        raise AssertionError('live execution should be skipped when exchange already reports a short position')

    monkeypatch.setattr(mod, 'place_live_trade', unexpected)
    monkeypatch.setattr(mod, 'start_trade_monitor_thread', unexpected)
    monkeypatch.setattr(mod, 'monitor_live_trade', unexpected)

    result = mod.run_loop(DummyClient(), make_args(live=True, auto_loop=True, runtime_state_dir=str(tmp_path)))

    cycle = result['cycles'][0]
    assert cycle['live_requested'] is True
    assert cycle['live_skipped_due_to_existing_positions'] == [{'symbol': 'DOGEUSDT', 'positionAmt': '-2.5', 'positionSide': 'SHORT'}]


def test_run_loop_auto_loop_user_data_stream_failure_blocks_background_monitor(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = SimpleNamespace(
        symbol='DOGEUSDT',
        quantity=12.5,
        stop_price=0.1234,
        recommended_leverage=3,
    )
    meta = SimpleNamespace(symbol='DOGEUSDT')
    live_execution = {
        'entry_price': 0.1365,
        'filled_quantity': 12.7,
        'trade_management_plan': {'quantity': 12.5},
        'stop_order': {'orderId': 98765},
        'protection_check': {'status': 'protected'},
        'entry_order_feedback': {
            'order_id': 12345,
            'client_order_id': 'entry-order-1',
            'status': 'FILLED',
            'avg_price': 0.1365,
            'executed_qty': 12.7,
            'cum_quote': 1.73355,
            'update_time': 1710000000123,
        },
    }

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': meta}))
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, leverage, meta, args: live_execution)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})

    def uds_failed(*args, **kwargs):
        raise mod.BinanceAPIError('listen key rejected')

    monkeypatch.setattr(mod, 'start_user_data_stream_monitor', uds_failed, raising=False)
    monkeypatch.setattr(mod, 'start_trade_monitor_thread', lambda *args, **kwargs: pytest.fail('background thread should wait for user data stream readiness'))

    with pytest.raises(mod.BinanceAPIError, match='listen key rejected'):
        mod.run_loop(DummyClient(), make_args(live=True, auto_loop=True, runtime_state_dir=str(tmp_path)))


def test_apply_user_data_stream_order_update_persists_lifecycle_event(tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    positions = {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_key': 'DOGEUSDT:LONG',
            'status': 'monitoring',
            'quantity': 12.7,
            'filled_quantity': 12.7,
            'remaining_quantity': 12.7,
            'entry_price': 0.1365,
            'stop_price': 0.1234,
            'current_stop_price': 0.1234,
            'protection_status': 'protected',
            'entry_order_id': 12345,
            'entry_client_order_id': 'entry-order-1',
            'entry_order_status': 'FILLED',
            'entry_cum_quote': 1.73355,
            'entry_update_time': 1710000000123,
            'profile': 'test-profile',
        }
    }
    store.save_json('positions', positions)

    payload = {
        'e': 'ORDER_TRADE_UPDATE',
        'E': 1710000001123,
        'T': 1710000001120,
        'o': {
            's': 'DOGEUSDT',
            'i': 12345,
            'c': 'entry-order-1',
            'X': 'PARTIALLY_FILLED',
            'x': 'TRADE',
            'S': 'BUY',
            'o': 'MARKET',
            'ap': '0.1368',
            'L': '0.1369',
            'z': '9.1',
            'l': '3.2',
            'q': '12.7',
            'zq': '1.24488',
            'n': '0.00012',
            'N': 'USDT',
        },
    }

    row = mod.apply_user_data_stream_order_update(store, payload)

    assert row['event_type'] == 'user_data_stream_order_update'
    assert row['symbol'] == 'DOGEUSDT'
    assert row['entry_order_status'] == 'PARTIALLY_FILLED'
    assert row['entry_last_filled_qty'] == 3.2
    assert row['entry_cumulative_filled_qty'] == 9.1
    assert row['entry_average_price'] == 0.1368
    assert row['entry_execution_type'] == 'TRADE'

    tracked = store.load_json('positions', {})['DOGEUSDT:LONG']
    assert tracked['entry_order_status'] == 'PARTIALLY_FILLED'
    assert tracked['entry_last_filled_qty'] == 3.2
    assert tracked['entry_cumulative_filled_qty'] == 9.1
    assert tracked['entry_average_price'] == 0.1368
    assert tracked['entry_execution_type'] == 'TRADE'
    assert tracked['entry_update_time'] == 1710000001120

    rows = [mod.json.loads(line) for line in store._events_path().read_text(encoding='utf-8').splitlines() if line.strip()]
    assert rows[-1]['event_type'] == 'user_data_stream_order_update'
    assert rows[-1]['entry_order_status'] == 'PARTIALLY_FILLED'
    assert rows[-1]['entry_last_filled_qty'] == 3.2


def test_record_user_data_stream_health_event_persists_refresh_and_disconnect_status(tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    now = datetime.datetime(2026, 4, 20, 12, 0, tzinfo=datetime.timezone.utc)

    refresh_row = mod.record_user_data_stream_health_event(
        store,
        'DOGEUSDT',
        listen_key='abc123',
        status='refresh_failed',
        detail='timeout',
        now=now,
    )
    disconnect_row = mod.record_user_data_stream_health_event(
        store,
        'DOGEUSDT',
        listen_key='abc123',
        status='disconnected',
        detail='websocket closed',
        now=now + datetime.timedelta(minutes=40),
    )

    assert refresh_row['event_type'] == 'user_data_stream_health'
    assert refresh_row['status'] == 'refresh_failed'
    assert disconnect_row['status'] == 'disconnected'

    uds_state = store.load_json('user_data_stream', {})
    assert uds_state['symbol'] == 'DOGEUSDT'
    assert uds_state['listen_key'] == 'abc123'
    assert uds_state['status'] == 'disconnected'
    assert uds_state['detail'] == 'websocket closed'
    assert uds_state['disconnect_count'] == 1
    assert uds_state['refresh_failure_count'] == 1
    assert uds_state['started_at'] == '2026-04-20T12:00:00Z'
    assert uds_state['updated_at'] == '2026-04-20T12:40:00Z'


def test_apply_user_data_stream_order_update_emits_unified_exit_event_for_reduce_only_close(tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_side': 'LONG',
            'status': 'monitoring',
            'monitor_mode': 'trade_management',
            'quantity': 12.7,
            'filled_quantity': 12.7,
            'remaining_quantity': 12.7,
            'entry_price': 0.1365,
            'current_stop_price': 0.129,
            'opened_at': '2026-04-29T00:00:00Z',
            'selected_score': 82.6,
            'selected_state': 'launch',
            'selected_alert_tier': 'critical',
            'candidate_stage': 'launch',
            'trigger_class': 'breakout_retest',
            'market_regime_label': 'expansion',
            'market_regime_multiplier': 1.25,
            'setup_ready': True,
            'trigger_fired': True,
            'trade_management_plan': {
                'position_side': 'LONG',
                'side': 'BUY',
                'quantity': 12.7,
                'stop_price': 0.129,
                'initial_stop_price': 0.129,
                'initial_risk_per_unit': 0.0075,
            },
        },
    })

    row = mod.apply_user_data_stream_order_update(store, {
        'e': 'ORDER_TRADE_UPDATE',
        'E': 1710000002222,
        'o': {
            's': 'DOGEUSDT',
            'S': 'SELL',
            'ps': 'LONG',
            'o': 'MARKET',
            'x': 'TRADE',
            'X': 'FILLED',
            'i': 45678,
            'c': 'close-order-1',
            'ap': '0.1412',
            'L': '0.1412',
            'z': '12.7',
            'l': '12.7',
            'q': '12.7',
            'zq': '1.79324',
            'n': '0.0002',
            'N': 'USDT',
            'R': True,
            'rp': '0.05969',
            'T': 1710000002211,
        },
    })

    assert row['event_type'] == 'trade_invalidated'
    assert row['symbol'] == 'DOGEUSDT'
    assert row['position_side'] == 'LONG'
    assert row['exit_reason'] == 'order_trade_update_reduce_only_filled'
    assert row['exit_price'] == 0.1412
    assert row['opened_at'] == '2026-04-29T00:00:00Z'
    assert row['score'] == 82.6
    assert row['state'] == 'launch'
    assert row['alert_tier'] == 'critical'

    tracked = store.load_json('positions', {})['DOGEUSDT:LONG']
    assert tracked['status'] == 'closed'
    assert tracked['protection_status'] == 'flat'
    assert tracked['remaining_quantity'] == 0.0
    assert tracked['exit_reason'] == 'order_trade_update_reduce_only_filled'
    assert tracked['opened_at'] == '2026-04-29T00:00:00Z'
    assert tracked['selected_score'] == 82.6
    assert tracked['selected_state'] == 'launch'

    rows = [mod.json.loads(line) for line in store._events_path().read_text(encoding='utf-8').splitlines() if line.strip()]
    assert rows[-1]['event_type'] == 'trade_invalidated'
    assert rows[-1]['position_side'] == 'LONG'
    assert rows[-1]['opened_at'] == '2026-04-29T00:00:00Z'


def test_run_loop_auto_loop_persists_user_data_stream_health_state(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = SimpleNamespace(symbol='DOGEUSDT', stop_price=0.1234, quantity=12.7, recommended_leverage=3)
    meta = SimpleNamespace(symbol='DOGEUSDT')
    live_execution = {
        'entry_price': 0.1365,
        'filled_quantity': 12.7,
        'trade_management_plan': {'quantity': 12.5},
        'stop_order': {'orderId': 98765},
        'protection_check': {'status': 'protected'},
        'entry_order_feedback': {
            'order_id': 12345,
            'client_order_id': 'entry-order-1',
            'status': 'FILLED',
            'avg_price': 0.1365,
            'executed_qty': 12.7,
            'cum_quote': 1.73355,
            'update_time': 1710000000123,
        },
    }
    notifications = []
    monitor_calls = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': meta}))
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, leverage, meta, args: live_execution)
    monkeypatch.setattr(mod, 'emit_notification', lambda args, event_type, payload: notifications.append((event_type, payload)) or {'ok': True})
    monkeypatch.setattr(mod, 'start_trade_monitor_thread', lambda *args, **kwargs: SimpleNamespace(name='trade-monitor-1'))

    def fake_run_user_data_stream_monitor_cycle(client, store, symbol=None, now=None, refresh_interval_minutes=30.0, disconnect_timeout_minutes=65.0):
        monitor_calls.append({
            'symbol': symbol,
            'refresh_interval_minutes': refresh_interval_minutes,
            'disconnect_timeout_minutes': disconnect_timeout_minutes,
        })
        return {
            'listen_key': 'dummy-listen-key',
            'status': 'started',
            'action': 'started',
            'health': {
                'status': 'started',
                'refresh_failure_count': 0,
                'disconnect_count': 0,
                'started_at': '2026-04-20T12:00:00Z',
                'last_refresh_at': '2026-04-20T12:00:00Z',
                'updated_at': '2026-04-20T12:00:00Z',
            },
            'now_utc': '2026-04-20T12:00:00Z',
        }

    monkeypatch.setattr(mod, 'run_user_data_stream_monitor_cycle', fake_run_user_data_stream_monitor_cycle, raising=False)

    result = mod.run_loop(DummyClient(), make_args(live=True, auto_loop=True, runtime_state_dir=str(tmp_path)))

    assert result['ok'] is True
    assert monitor_calls == [{'symbol': 'DOGEUSDT', 'refresh_interval_minutes': 30.0, 'disconnect_timeout_minutes': 65.0}]
    position = store.load_json('positions', {})['DOGEUSDT:LONG']
    assert position['user_data_stream']['status'] == 'started'
    assert position['user_data_stream']['listen_key'] == 'dumm***-key'
    assert position['user_data_stream']['health']['refresh_failure_count'] == 0
    assert position['user_data_stream']['health']['disconnect_count'] == 0
    assert result['cycles'][0]['trade_management']['user_data_stream']['health']['status'] == 'started'
    buy_fill_events = [row for row in store.read_events(limit=20) if row.get('event_type') == 'buy_fill_confirmed']
    assert buy_fill_events[-1]['listen_key'] == 'dumm***-key'
    assert notifications == []


def test_run_loop_auto_loop_emits_user_stream_alerts_and_reconnect_metrics(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = SimpleNamespace(symbol='DOGEUSDT', stop_price=0.1234, quantity=12.7, recommended_leverage=3)
    meta = SimpleNamespace(symbol='DOGEUSDT')
    notifications = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []})
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client, args: ({'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': meta}))
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, leverage, meta, args: {
        'entry_price': 0.1365,
        'filled_quantity': 12.7,
        'trade_management_plan': {'quantity': 12.5, 'entry_price': 0.1365, 'stop_price': 0.1234, 'initial_risk_per_unit': 0.0131, 'side': 'long', 'position_side': 'LONG'},
        'stop_order': {'orderId': 98765},
        'protection_check': {'status': 'protected'},
        'entry_order_feedback': {'order_id': 12345, 'client_order_id': 'entry-order-1', 'status': 'FILLED', 'cum_quote': 1.73355, 'update_time': 1710000000123},
    })
    monkeypatch.setattr(mod, 'emit_notification', lambda args, event_type, payload: notifications.append((event_type, payload)) or {'ok': True})
    monkeypatch.setattr(mod, 'start_trade_monitor_thread', lambda *args, **kwargs: SimpleNamespace(name='trade-monitor-1'))
    monkeypatch.setattr(mod, 'run_user_data_stream_monitor_cycle', lambda *args, **kwargs: {
        'listen_key': 'dummy-listen-key',
        'status': 'refresh_failed',
        'action': 'refresh_failed',
        'health': {
            'symbol': 'DOGEUSDT',
            'listen_key': 'dummy-listen-key',
            'status': 'refresh_failed',
            'detail': 'refresh timeout',
            'disconnect_count': 2,
            'refresh_failure_count': 3,
            'reconnect_count': 2,
            'started_at': '2026-04-20T12:00:00Z',
            'last_refresh_at': '2026-04-20T12:00:00Z',
            'updated_at': '2026-04-20T12:31:00Z',
        },
        'error': 'refresh timeout',
        'now_utc': '2026-04-20T12:31:00Z',
    }, raising=False)

    result = mod.run_loop(DummyClient(), make_args(live=True, auto_loop=True, runtime_state_dir=str(tmp_path), disable_notify=False, notify_target='telegram:test'))

    user_stream_notifications = [item for item in notifications if item[0] == 'user_data_stream_alert']
    assert len(user_stream_notifications) == 1
    payload = user_stream_notifications[0][1]
    assert payload['symbol'] == 'DOGEUSDT'
    assert payload['status'] == 'refresh_failed'
    assert payload['refresh_failure_count'] == 3
    assert payload['disconnect_count'] == 2
    assert payload['reconnect_count'] == 2
    assert payload['error'] == 'refresh timeout'
    position = store.load_json('positions', {})['DOGEUSDT:LONG']
    assert position['user_data_stream']['health']['reconnect_count'] == 2
    assert result['cycles'][0]['trade_management']['user_data_stream']['health']['reconnect_count'] == 2


def test_apply_user_data_stream_order_update_reconciles_fill_feedback(tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_key': 'DOGEUSDT:LONG',
            'status': 'monitoring',
            'quantity': 12.5,
            'filled_quantity': 12.5,
            'entry_price': 0.1365,
            'entry_order_id': 12345,
            'entry_client_order_id': 'entry-order-1',
            'entry_order_status': 'FILLED',
            'entry_cum_quote': 1.70625,
            'entry_update_time': 1710000000000,
        }
    })

    row = mod.apply_user_data_stream_order_update(store, {
        'e': 'ORDER_TRADE_UPDATE',
        'E': 1710000002222,
        'T': 1710000002222,
        'o': {
            's': 'DOGEUSDT',
            'i': 12345,
            'c': 'entry-order-1',
            'X': 'FILLED',
            'x': 'TRADE',
            'ap': '0.1370',
            'z': '12.5',
            'q': '12.5',
            'L': '0.1370',
            'l': '12.5',
            'S': 'BUY',
        },
    })

    assert row['event_type'] == 'user_data_stream_order_update'
    positions = store.load_json('positions', {})
    position = positions['DOGEUSDT:LONG']
    assert position['entry_price'] == 0.137
    assert position['filled_quantity'] == 12.5
    assert position['entry_order_status'] == 'FILLED'
    assert position['entry_update_time'] == 1710000002222
    reconciliation = position['entry_fill_reconciliation']
    assert reconciliation['rest_entry_price'] == 0.1365
    assert reconciliation['ws_entry_price'] == 0.137
    assert reconciliation['rest_entry_cum_quote'] == 1.70625
    assert reconciliation['ws_last_filled_price'] == 0.137
    assert reconciliation['price_delta'] == pytest.approx(0.0005)
    assert reconciliation['cum_quote_delta'] == pytest.approx(0.00625)


def test_build_notification_message_user_data_stream_alert_refresh_failed_cn():
    mod = load_module()

    message = mod.build_notification_message('user_data_stream_alert', {
        'symbol': 'DOGEUSDT',
        'status': 'refresh_failed',
        'listen_key': 'listen-key-1',
        'action': 'refresh_listen_key',
        'error': 'refresh timeout',
        'disconnect_count': 2,
        'refresh_failure_count': 3,
        'reconnect_count': 2,
        'last_refresh_at': '2026-04-20T12:00:00Z',
        'updated_at': '2026-04-20T12:31:00Z',
    })

    assert message == (
        '用户数据流告警 DOGEUSDT 状态=续期失败 动作=refresh_listen_key '
        '错误=refresh timeout 断线次数=2 续期失败次数=3 重连次数=2 '
        '最近续期=2026-04-20T12:00:00Z 更新时间=2026-04-20T12:31:00Z '
        'listenKey=list***ey-1'
    )


def test_run_user_data_stream_monitor_cycle_starts_and_refreshes_listen_key(tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    client = DummyClient()
    now = datetime.datetime(2026, 4, 20, 12, 0, tzinfo=datetime.timezone.utc)

    started = mod.run_user_data_stream_monitor_cycle(
        client,
        store,
        symbol='DOGEUSDT',
        now=now,
        refresh_interval_minutes=30,
        disconnect_timeout_minutes=65,
    )

    assert started['action'] == 'started'
    assert started['listen_key'] == 'dummy-listen-key'
    health = store.load_json('user_data_stream', {})
    assert health['status'] == 'started'
    assert health['started_at'] == '2026-04-20T12:00:00Z'
    assert health['last_refresh_at'] == '2026-04-20T12:00:00Z'

    refreshed = mod.run_user_data_stream_monitor_cycle(
        client,
        store,
        symbol='DOGEUSDT',
        now=now + datetime.timedelta(minutes=31),
        refresh_interval_minutes=30,
        disconnect_timeout_minutes=65,
    )

    assert refreshed['action'] == 'refreshed'
    assert refreshed['status'] == 'refreshed'
    refreshed_state = store.load_json('user_data_stream', {})
    assert refreshed_state['status'] == 'refreshed'
    assert refreshed_state['refresh_failure_count'] == 0
    assert refreshed_state['disconnect_count'] == 0
    assert refreshed_state['last_refresh_at'] == '2026-04-20T12:31:00Z'


def test_run_user_data_stream_monitor_cycle_records_refresh_failures(tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    now = datetime.datetime(2026, 4, 20, 12, 0, tzinfo=datetime.timezone.utc)
    store.save_json('user_data_stream', {
        'symbol': 'DOGEUSDT',
        'listen_key': 'abc123',
        'status': 'started',
        'detail': 'listen_key_created',
        'disconnect_count': 0,
        'refresh_failure_count': 0,
        'started_at': '2026-04-20T12:00:00Z',
        'last_refresh_at': '2026-04-20T12:00:00Z',
        'updated_at': '2026-04-20T12:00:00Z',
    })

    class RefreshFailClient(DummyClient):
        def signed_put(self, path, params):
            raise mod.BinanceAPIError('refresh timeout')

    result = mod.run_user_data_stream_monitor_cycle(
        RefreshFailClient(),
        store,
        symbol='DOGEUSDT',
        now=now + datetime.timedelta(minutes=31),
        refresh_interval_minutes=30,
        disconnect_timeout_minutes=65,
    )

    assert result['action'] == 'refresh_failed'
    assert result['status'] == 'refresh_failed'
    assert 'refresh timeout' in result['error']
    health = store.load_json('user_data_stream', {})
    assert health['status'] == 'refresh_failed'
    assert health['refresh_failure_count'] == 1
    assert health['disconnect_count'] == 0
    assert health['last_refresh_at'] == '2026-04-20T12:00:00Z'


def test_run_user_data_stream_monitor_cycle_marks_disconnect_when_refresh_stale(tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('user_data_stream', {
        'symbol': 'DOGEUSDT',
        'listen_key': 'abc123',
        'status': 'refresh_failed',
        'detail': 'timeout',
        'disconnect_count': 0,
        'refresh_failure_count': 1,
        'started_at': '2026-04-20T12:00:00Z',
        'last_refresh_at': '2026-04-20T12:00:00Z',
        'updated_at': '2026-04-20T12:31:00Z',
    })

    result = mod.run_user_data_stream_monitor_cycle(
        DummyClient(),
        store,
        symbol='DOGEUSDT',
        now=datetime.datetime(2026, 4, 20, 13, 10, tzinfo=datetime.timezone.utc),
        refresh_interval_minutes=999,
        disconnect_timeout_minutes=65,
    )

    assert result['action'] == 'disconnected'
    assert result['status'] == 'disconnected'
    health = store.load_json('user_data_stream', {})
    assert health['status'] == 'disconnected'
    assert health['disconnect_count'] == 1
    assert health['refresh_failure_count'] == 1
    assert health['detail'] == 'listen_key_refresh_stale'


def test_monitor_live_trade_records_lifecycle_events_and_updates_position_state(monkeypatch):
    mod = load_module()
    store = mod.RuntimeStateStore(runtime_state_dir='/tmp/runtime-for-foreground-monitor-test')
    if store._dir().exists():
        for path in store._dir().glob('*'):
            path.unlink()

    args = make_args(
        live=True,
        auto_loop=False,
        profile='test-profile',
        trailing_buffer_pct=0.02,
        monitor_poll_interval_sec=0,
        disable_notify=False,
        notify_target='telegram:demo',
    )
    meta = SimpleNamespace(step_size=0.1, quantity_precision=1, tick_size=0.01, price_precision=2)
    plan = mod.build_trade_management_plan(
        entry_price=100.0,
        stop_price=95.0,
        quantity=10.0,
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        breakeven_r=1.0,
    )
    trade = {
        'symbol': 'DOGEUSDT',
        'entry_price': 100.0,
        'stop_order': {'orderId': 555},
        'trade_management_plan': mod.asdict(plan),
        'protection_check': {'status': 'protected'},
    }
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_side': 'LONG',
            'position_key': 'DOGEUSDT:LONG',
            'status': 'monitoring',
            'quantity': 10.0,
            'remaining_quantity': 10.0,
            'entry_price': 100.0,
            'stop_price': 95.0,
            'stop_order_id': 555,
            'protection_status': 'protected',
        }
    })

    price_samples = [
        {'price': 100.0, 'ema5m': 100.0, 'trailing_reference': 100.0},
        {'price': 105.0, 'ema5m': 104.0, 'trailing_reference': 105.0},
        {'price': 107.5, 'ema5m': 106.0, 'trailing_reference': 107.5},
        {'price': 110.0, 'ema5m': 108.0, 'trailing_reference': 110.0},
        {'price': 107.0, 'ema5m': 107.0, 'trailing_reference': 110.0},
    ]
    sample_index = {'value': 0}
    applied_actions = []
    notifications = []

    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda client, symbol, expected_stop_order=None, allow_missing_when_flat=True: {'status': 'protected', 'expected_order_id': 555})
    monkeypatch.setattr(mod.time, 'sleep', lambda seconds: None)

    def fake_fetch_mark_price(client, symbol):
        idx = min(sample_index['value'], len(price_samples) - 1)
        return price_samples[idx]['price']

    def fake_fetch_klines(client, symbol, interval='5m', limit=21):
        idx = min(sample_index['value'], len(price_samples) - 1)
        sample = price_samples[idx]
        positions = store.load_json('positions', {})
        tracked = positions.get('DOGEUSDT:LONG', {})
        tracked['_debug_current_price'] = sample['price']
        tracked['_debug_ema5m'] = sample['ema5m']
        tracked['_debug_trailing_reference'] = sample['trailing_reference']
        positions['DOGEUSDT:LONG'] = tracked
        store.save_json('positions', positions)
        return [[0, 0, sample['price'], 0, sample['ema5m'], 0, 0, 0, 0, 0, 0, 0] for _ in range(limit)]

    def fake_apply_management_action(client, symbol, meta, state, action, active_stop_order):
        applied_actions.append(action['type'])
        state = dataclasses.replace(state)
        if action['type'] == 'move_stop_to_breakeven':
            state.current_stop_price = action['new_stop_price']
            state.moved_to_breakeven = True
            sample_index['value'] += 1
            return state, {'orderId': 556}, {'action': action['type'], 'new_stop_order': {'orderId': 556}}
        if action['type'] == 'take_profit_1':
            state.remaining_quantity = round(state.remaining_quantity - action['close_qty'], 10)
            state.tp1_hit = True
            state.current_stop_price = action['new_stop_price']
            sample_index['value'] += 1
            return state, {'orderId': 557}, {'action': action['type'], 'reduce_order': {'status': 'FILLED'}, 'new_stop_order': {'orderId': 557}}
        if action['type'] == 'take_profit_2':
            state.remaining_quantity = round(state.remaining_quantity - action['close_qty'], 10)
            state.tp2_hit = True
            state.current_stop_price = action['new_stop_price']
            sample_index['value'] += 1
            return state, {'orderId': 558}, {'action': action['type'], 'reduce_order': {'status': 'FILLED'}, 'new_stop_order': {'orderId': 558}}
        if action['type'] == 'runner_exit':
            state.remaining_quantity = 0.0
            sample_index['value'] += 1
            return state, None, {'action': action['type'], 'reduce_order': {'status': 'FILLED'}}
        sample_index['value'] += 1
        return state, active_stop_order, {'action': action['type']}

    monkeypatch.setattr(mod, 'fetch_klines', fake_fetch_klines)
    monkeypatch.setattr(mod, 'evaluate_management_actions', lambda state, plan, current_price, ema5m, trailing_reference, trailing_buffer_pct, allow_runner_exit=False: [
        {'type': 'move_stop_to_breakeven', 'new_stop_price': 100.0},
    ] if sample_index['value'] == 0 else [
        {'type': 'take_profit_1', 'close_qty': 3.0, 'new_stop_price': 104.0},
    ] if sample_index['value'] == 1 else [
        {'type': 'take_profit_2', 'close_qty': 4.0, 'new_stop_price': 108.0},
    ] if sample_index['value'] == 2 else [
        {'type': 'runner_exit', 'close_qty': 3.0, 'trailing_floor': 107.8},
    ] if sample_index['value'] == 3 else [])
    monkeypatch.setattr(mod, 'apply_management_action', fake_apply_management_action)
    monkeypatch.setattr(mod, 'emit_notification', lambda args, event_type, payload, post_func=None: notifications.append((event_type, payload)) or {'ok': True})

    result = mod.monitor_live_trade(client=DummyClient(), symbol='DOGEUSDT', meta=meta, args=args, trade=trade, store=store)

    assert result['ok'] is True
    assert result['symbol'] == 'DOGEUSDT'
    assert applied_actions == ['move_stop_to_breakeven', 'take_profit_1', 'take_profit_2', 'runner_exit']

    positions = store.load_json('positions', {})
    assert 'DOGEUSDT' not in positions
    assert 'DOGEUSDT:LONG' not in positions
    assert positions == {}

    rows = [mod.json.loads(line) for line in store._events_path().read_text(encoding='utf-8').splitlines() if line.strip()]
    event_types = [row['event_type'] for row in rows]
    assert event_types == [
        'entry_filled',
        'protection_confirmed',
        'breakeven_moved',
        'tp1_hit',
        'tp2_hit',
        'runner_exited',
        'trade_invalidated',
    ]
    assert rows[0]['quantity'] == 10.0
    assert rows[1]['protection_status'] == 'protected'
    assert rows[2]['new_stop_price'] == 100.0
    assert rows[3]['close_qty'] == 3.0
    assert rows[4]['close_qty'] == 4.0
    assert rows[5]['close_qty'] == 3.0
    assert rows[5]['exit_reason'] == 'runner'
    assert rows[5]['protection_status'] == 'flat'
    assert rows[5]['realized_r_after_action'] == 1.5
    assert rows[6]['exit_reason'] == 'runner'
    assert rows[6]['protection_status'] == 'flat'
    assert rows[6]['realized_r'] == 1.5
    assert rows[6]['mfe_r'] == 2.0
    assert rows[6]['mae_r'] == 0.0
    assert rows[6]['time_to_1r'] is not None
    assert rows[6]['time_in_trade_minutes'] is not None

    notified_types = [item[0] for item in notifications]
    assert notified_types == event_types[:-1]

    heartbeat = store.load_json('runtime_heartbeat', {})
    monitor_heartbeat = heartbeat['components']['execution_monitor']
    assert monitor_heartbeat['status'] == 'flat'
    assert monitor_heartbeat['symbol'] == 'DOGEUSDT'
    assert monitor_heartbeat['remaining_quantity'] == 0.0


def test_monitor_live_trade_restores_checkpoint_state_before_evaluating_actions(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    args = make_args(
        live=True,
        auto_loop=False,
        profile='test-profile',
        trailing_buffer_pct=0.02,
        monitor_poll_interval_sec=0,
        max_monitor_cycles=1,
        disable_notify=True,
    )
    meta = SimpleNamespace(step_size=0.01, quantity_precision=2, tick_size=0.01, price_precision=2)
    plan = mod.build_trade_management_plan(
        entry_price=100.0,
        stop_price=95.0,
        quantity=10.0,
        tp1_r=1.0,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        breakeven_r=1.0,
    )
    trade = {
        'symbol': 'DOGEUSDT',
        'entry_price': 100.0,
        'side': 'LONG',
        'stop_order': {'orderId': 777},
        'trade_management_plan': mod.asdict(plan),
        'protection_check': {'status': 'protected'},
    }
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_side': 'LONG',
            'position_key': 'DOGEUSDT:LONG',
            'status': 'monitoring',
            'quantity': 10.0,
            'remaining_quantity': 3.0,
            'entry_price': 100.0,
            'current_stop_price': 108.0,
            'stop_order_id': 777,
            'protection_status': 'protected',
            'trade_management_plan': mod.asdict(plan),
        }
    })
    captured = {}

    monkeypatch.setattr(mod, 'fetch_klines', lambda client, symbol, interval='5m', limit=21: [[0, 0, 110.0, 0, 109.0, 0, 0, 0, 0, 0, 0, 0] for _ in range(limit)])
    monkeypatch.setattr(mod.time, 'sleep', lambda seconds: None)

    def fake_evaluate_management_actions(state, plan, current_price, ema5m, trailing_reference, trailing_buffer_pct, allow_runner_exit=False):
        captured['state'] = dataclasses.replace(state)
        return []

    monkeypatch.setattr(mod, 'evaluate_management_actions', fake_evaluate_management_actions)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})

    result = mod.monitor_live_trade(client=DummyClient(), symbol='DOGEUSDT', meta=meta, args=args, trade=trade, store=store)

    assert result['ok'] is True
    restored_state = captured['state']
    assert restored_state.remaining_quantity == 3.0
    assert restored_state.moved_to_breakeven is True
    assert restored_state.tp1_hit is True
    assert restored_state.tp2_hit is True
    assert restored_state.current_stop_price == 108.0


def test_collect_book_ticker_samples_rate_limits_cache_miss_runtime_events(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))

    calls = []

    class DummyClient:
        def get(self, path, params=None):
            calls.append((path, params))
            return {'symbol': 'DOGEUSDT', 'bidPrice': '1.0', 'askPrice': '1.1'}

    monkeypatch.setattr(mod.time, 'sleep', lambda seconds: None)

    for _ in range(3):
        samples = mod.collect_book_ticker_samples(
            DummyClient(),
            'DOGEUSDT',
            sample_count=1,
            interval_ms=0,
            store=store,
            cache_max_age_seconds=0.0,
        )
        assert len(samples) == 1

    events = store.read_events(limit=20)
    miss_events = [row for row in events if row['event_type'] == 'book_ticker_cache_miss']
    assert len(miss_events) == 1
    assert miss_events[0]['symbol'] == 'DOGEUSDT'
    assert miss_events[0]['fallback'] == 'rest_polling'

    rate_state = store.load_json('event_rate_limit_state', {})
    assert rate_state['book_ticker_cache_miss']['DOGEUSDT']['suppressed_since_last'] == 2
    assert len(calls) == 3


def test_monitor_live_trade_prefers_book_ticker_cache_price_and_uses_close_fallback_for_ema(monkeypatch, tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))

    args = make_args(
        live=True,
        auto_loop=False,
        profile='test-profile',
        trailing_buffer_pct=0.02,
        monitor_poll_interval_sec=0,
        max_monitor_cycles=1,
        disable_notify=True,
    )
    meta = SimpleNamespace(step_size=0.1, quantity_precision=1, tick_size=0.01, price_precision=2)
    plan = mod.build_trade_management_plan(
        entry_price=100.0,
        stop_price=95.0,
        quantity=10.0,
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        breakeven_r=1.0,
    )
    trade = {
        'symbol': 'DOGEUSDT',
        'entry_price': 100.0,
        'stop_order': {'orderId': 555},
        'trade_management_plan': mod.asdict(plan),
        'protection_check': {'status': 'protected'},
    }
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_side': 'LONG',
            'position_key': 'DOGEUSDT:LONG',
            'status': 'monitoring',
            'quantity': 10.0,
            'remaining_quantity': 10.0,
            'entry_price': 100.0,
            'stop_price': 95.0,
            'stop_order_id': 555,
            'protection_status': 'protected',
        }
    })
    store.save_json('book_ticker_cache', {
        'DOGEUSDT': {
            'updated_at': mod._isoformat_utc(mod._utc_now()),
            'samples': [
                {'bidPrice': '104.9', 'askPrice': '105.1', 'bidQty': '10', 'askQty': '8'},
            ],
        },
    })

    captured = {}

    monkeypatch.setattr(mod.time, 'sleep', lambda seconds: None)
    monkeypatch.setattr(mod, 'fetch_klines', lambda client, symbol, interval='5m', limit=21: [[0, '100', '130', '90', '103', '0', 0, '0', 0, 0, 0, 0] for _ in range(limit)])
    monkeypatch.setattr(mod, 'evaluate_management_actions', lambda state, plan, current_price, ema5m, trailing_reference, trailing_buffer_pct, allow_runner_exit=False: captured.update({
        'current_price': current_price,
        'ema5m': ema5m,
        'trailing_reference': trailing_reference,
        'allow_runner_exit': allow_runner_exit,
    }) or [])
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: {'ok': True})

    result = mod.monitor_live_trade(client=DummyClient(), symbol='DOGEUSDT', meta=meta, args=args, trade=trade, store=store)

    assert result['ok'] is True
    assert captured == {
        'current_price': 104.9,
        'ema5m': 103.0,
        'trailing_reference': 130.0,
        'allow_runner_exit': True,
    }
    debug_payload = store.load_json('monitor_debug', {})
    assert debug_payload['current_price_source'] == 'book_ticker_cache_bid'
    assert debug_payload['book_ticker_snapshot']['mid_price'] == 105.0


def test_run_with_timeout_returns_deadman_timeout_without_blocking_forever(monkeypatch):
    mod = load_module()
    store = DummyStore()

    def blocking_task():
        import time
        time.sleep(0.25)
        return {'ok': True}

    result = mod.run_with_deadman_timeout(
        blocking_task,
        timeout_seconds=0.01,
        store=store,
        component='scanner',
        operation='scan_cycle',
    )

    assert result['ok'] is False
    assert result['reason'] == 'deadman_timeout'
    assert result['component'] == 'scanner'
    assert result['operation'] == 'scan_cycle'
    assert store.json_state['runtime_heartbeat']['components']['scanner']['status'] == 'timeout'
    assert store.events[-1]['event_type'] == 'runtime_deadman_timeout'


def test_cleanup_symbol_runtime_state_ttl_removes_stale_symbol_cache():
    mod = load_module()
    store = DummyStore()
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    stale = (now - datetime.timedelta(seconds=120)).isoformat()
    fresh = (now - datetime.timedelta(seconds=10)).isoformat()
    store.json_state['book_ticker_cache'] = {
        'OLDUSDT': [{'event_time': stale, 'bid_price': 1}],
        'FRESHUSDT': [{'event_time': fresh, 'bid_price': 2}],
    }
    store.json_state['symbol_runtime_state'] = {
        'OLDUSDT': {'updated_at': stale},
        'FRESHUSDT': {'updated_at': fresh},
    }

    result = mod.cleanup_symbol_runtime_state_ttl(store, ttl_seconds=60, now=now)

    assert result['removed_symbols'] == ['OLDUSDT']
    assert sorted(store.json_state['book_ticker_cache'].keys()) == ['FRESHUSDT']
    assert sorted(store.json_state['symbol_runtime_state'].keys()) == ['FRESHUSDT']
    assert store.events[-1]['event_type'] == 'runtime_ttl_cleanup'


def test_record_runtime_heartbeat_logs_queue_backlog_and_blocked_reason():
    mod = load_module()
    store = DummyStore()

    payload = mod.record_runtime_heartbeat(
        store,
        component='scanner',
        status='blocked',
        blocked_reason='queue_backlog:execution_queue_full',
        queue_depth=10,
        queue_maxsize=10,
    )

    assert payload['status'] == 'blocked'
    assert payload['blocked_reason'] == 'queue_backlog:execution_queue_full'
    assert payload['queue_depth'] == 10
    assert payload['queue_maxsize'] == 10
    assert store.json_state['runtime_heartbeat']['components']['scanner']['blocked_reason'] == 'queue_backlog:execution_queue_full'


def test_supervised_auto_loop_restarts_after_cycle_exception(monkeypatch):
    mod = load_module()
    store = DummyStore()
    args = make_args(auto_loop=True, max_scan_cycles=2, poll_interval_sec=0, supervisor_restart_limit=2)
    calls = {'count': 0}

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda passed_args: store)
    monkeypatch.setattr(mod, 'print_scan_output', lambda result, output_format: None)
    monkeypatch.setattr(mod.time, 'sleep', lambda seconds: None)

    def flaky_run_loop(client, passed_args):
        calls['count'] += 1
        if calls['count'] == 1:
            raise RuntimeError('scanner exploded')
        return {'ok': True, 'cycles': [{'scan': {'candidate_count': 0}}]}

    result = mod.run_supervised_auto_loop(DummyClient(), args, flaky_run_loop)

    assert result == {'ok': True, 'cycles': [{'scan': {'candidate_count': 0}}], 'cycle_no': 2, 'auto_loop': True}
    assert calls['count'] == 2
    heartbeat = store.load_json('runtime_heartbeat', {})
    assert heartbeat['components']['supervisor']['status'] == 'running'
    events = [row['event_type'] for row in store.events]
    assert 'supervisor_restart' in events
    restart_event = next(row for row in store.events if row['event_type'] == 'supervisor_restart')
    assert restart_event['status'] == 'restarting'
    assert restart_event['blocked_reason'] == 'cycle_exception'


def test_supervised_auto_loop_stops_after_restart_limit(monkeypatch):
    mod = load_module()
    store = DummyStore()
    args = make_args(auto_loop=True, max_scan_cycles=3, poll_interval_sec=0, supervisor_restart_limit=1)

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda passed_args: store)
    monkeypatch.setattr(mod, 'print_scan_output', lambda result, output_format: None)
    monkeypatch.setattr(mod.time, 'sleep', lambda seconds: None)

    def broken_run_loop(client, passed_args):
        raise RuntimeError('event loop wedged')

    result = mod.run_supervised_auto_loop(DummyClient(), args, broken_run_loop)

    assert result['ok'] is False
    assert result['reason'] == 'supervisor_restart_limit_exceeded'
    assert result['cycle_no'] == 2
    supervisor = store.load_json('runtime_heartbeat', {})['components']['supervisor']
    assert supervisor['status'] == 'halted'
    assert supervisor['blocked_reason'] == 'restart_limit_exceeded'
    assert [row['event_type'] for row in store.events].count('supervisor_restart') == 1
    assert [row['event_type'] for row in store.events].count('supervisor_halted') == 1


def test_book_ticker_supervisor_reconnects_after_monitor_exception():
    class Store:
        def __init__(self):
            self.saved = {}
            self.events = []
        def save_json(self, name, payload):
            self.saved[name] = payload
        def append_event(self, event_type, payload):
            self.events.append({'event_type': event_type, 'payload': payload})
            return self.events[-1]
    mod = load_module()
    store = Store()
    sockets = []
    def open_ws(symbols, **kwargs):
        ws = object()
        sockets.append(ws)
        return ws
    calls = {'count': 0}
    def monitor_cycle(store, ws, **kwargs):
        calls['count'] += 1
        if calls['count'] == 1:
            raise RuntimeError('recv loop wedged')
        return {'status': 'healthy', 'messages_processed': 2, 'samples_written': 2}
    result = mod.run_book_ticker_websocket_supervisor(
        store,
        initial_symbols=['BTCUSDT'],
        symbol_provider=lambda: ['BTCUSDT'],
        ws_module=object(),
        open_websocket_fn=open_ws,
        monitor_cycle_fn=monitor_cycle,
        sleep_fn=lambda seconds: None,
        max_supervisor_cycles=2,
        reconnect_backoff_seconds=0,
    )
    assert result['cycles_completed'] == 2
    assert result['reconnect_count'] == 1
    assert len(sockets) == 2
    assert any(event['event_type'] == 'book_ticker_ws_monitor_error' for event in store.events)
    assert store.saved['book_ticker_ws_status']['status'] == 'healthy'


def test_websocket_freshness_rejects_legacy_health_without_updated_at():
    mod = load_module()

    result = mod.evaluate_websocket_freshness({'status': 'healthy', 'messages_processed': 10})

    assert result['fresh'] is False
    assert result['reason'] == 'unknown_websocket_health_without_updated_at'


def test_book_ticker_monitor_zero_message_timeout_forces_reconnect_status():
    mod = load_module()
    store = DummyStore()

    class TimeoutErrorForTest(Exception):
        pass

    class WsModule:
        WebSocketTimeoutException = TimeoutErrorForTest
        WebSocketException = RuntimeError

    class Ws:
        def settimeout(self, value):
            self.timeout = value
        def recv(self):
            raise TimeoutErrorForTest('idle')

    result = mod.run_book_ticker_cache_monitor_cycle(store, Ws(), WsModule, max_messages=1, recv_timeout_seconds=0.01)

    assert result['status'] == 'idle_timeout'
    assert result['zero_message_timeout'] is True
    assert result['messages_processed'] == 0


def test_book_ticker_supervisor_reconnects_after_consecutive_zero_message_timeouts():
    mod = load_module()
    store = DummyStore()
    sockets = []

    def open_ws(symbols, **kwargs):
        ws = object()
        sockets.append(ws)
        return ws

    def idle_cycle(store, ws, **kwargs):
        return {'status': 'idle_timeout', 'messages_processed': 0, 'samples_written': 0, 'zero_message_timeout': True}

    result = mod.run_book_ticker_websocket_supervisor(
        store,
        initial_symbols=['BTCUSDT'],
        symbol_provider=lambda: ['BTCUSDT'],
        ws_module=object(),
        open_websocket_fn=open_ws,
        monitor_cycle_fn=idle_cycle,
        sleep_fn=lambda seconds: None,
        max_supervisor_cycles=2,
        zero_message_timeout_reconnect_threshold=2,
    )

    assert result['reconnect_count'] == 1
    assert len(sockets) == 2
    assert 'book_ticker_ws_zero_message_timeout' in [row['event_type'] for row in store.events]


def test_cleanup_symbol_runtime_state_ttl_removes_bad_state_without_timestamp():
    mod = load_module()
    store = DummyStore()
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    fresh = (now - datetime.timedelta(seconds=10)).isoformat()
    store.json_state['book_ticker_cache'] = {
        'BADUSDT': [{'bid_price': 1}],
        'FRESHUSDT': [{'event_time': fresh, 'bid_price': 2}],
    }
    store.json_state['symbol_runtime_state'] = {
        'BADUSDT': {'state': 'legacy'},
        'FRESHUSDT': {'updated_at': fresh},
    }

    result = mod.cleanup_symbol_runtime_state_ttl(store, ttl_seconds=60, now=now)

    assert result['removed_symbols'] == ['BADUSDT']
    assert sorted(store.json_state['book_ticker_cache'].keys()) == ['FRESHUSDT']
    assert sorted(store.json_state['symbol_runtime_state'].keys()) == ['FRESHUSDT']


@pytest.mark.asyncio
async def test_runtime_task_queue_consumer_put_get_executes_handler():
    mod = load_module()
    store = DummyStore()
    queues = mod.build_runtime_task_queues(maxsize=1)
    handled = []

    queued = await mod.submit_runtime_task(queues['scanner'], {'cycle': 1}, store=store, component='scanner')
    await mod.runtime_queue_consumer('scanner', queues['scanner'], lambda item: handled.append(item), store=store, stop_after_one=True)

    assert queued is True
    assert handled == [{'cycle': 1}]
    assert queues['scanner'].qsize() == 0
    assert store.json_state['runtime_heartbeat']['components']['scanner']['status'] == 'idle'
