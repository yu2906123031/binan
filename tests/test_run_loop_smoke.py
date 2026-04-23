import dataclasses
import datetime
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT_PATH = Path('/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py')


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

    def save_json(self, name, payload):
        self.saved.append((name, payload))



def make_args(**overrides):
    base = dict(
        runtime_state_dir='/tmp/runtime',
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
    )
    base.update(overrides)
    return SimpleNamespace(**base)


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


def test_run_loop_scan_only_output_reports_scan_only_mode(monkeypatch):
    mod = load_module()
    result = {
        'ok': True,
        'cycles': [
            {
                'scan_only': True,
                'scan': {
                    'market_regime': {'label': 'neutral', 'score_multiplier': 1.0, 'reasons': []},
                    'rejected_stats': {'total': 0, 'by_reject_label': {}},
                    'candidate_count': 0,
                    'candidate_alerts': [],
                },
            }
        ],
    }

    rendered = mod.render_cn_scan_summary(result)

    assert '扫描模式: scan-only' in rendered


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


def test_main_auto_loop_runs_multiple_cycles_and_sleeps(monkeypatch, capsys):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=2, poll_interval_sec=7, base_url='https://example.com')
    cycle_calls = []
    sleeps = []

    monkeypatch.setattr(mod, 'parse_args', lambda argv=None: args)
    monkeypatch.setattr(mod, 'BinanceFuturesClient', lambda **kwargs: DummyClient())

    def fake_run_loop(client, passed_args):
        cycle_calls.append(len(cycle_calls) + 1)
        return {'ok': True, 'cycles': [{'scan': {'candidate_count': len(cycle_calls)}}], 'cycle_no': len(cycle_calls)}

    monkeypatch.setattr(mod, 'run_loop', fake_run_loop)
    monkeypatch.setattr(mod.time, 'sleep', lambda seconds: sleeps.append(seconds))

    exit_code = mod.main([])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert cycle_calls == [1, 2]
    assert sleeps == [7]
    assert '"cycle_no": 2' in captured.out


def test_main_auto_loop_zero_cycles_runs_forever_until_keyboard_interrupt(monkeypatch, capsys):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=0, poll_interval_sec=5, base_url='https://example.com')
    cycle_calls = []
    sleeps = []

    monkeypatch.setattr(mod, 'parse_args', lambda argv=None: args)
    monkeypatch.setattr(mod, 'BinanceFuturesClient', lambda **kwargs: DummyClient())

    def fake_run_loop(client, passed_args):
        cycle_calls.append(len(cycle_calls) + 1)
        return {'ok': True, 'cycles': [], 'cycle_no': len(cycle_calls)}

    def fake_sleep(seconds):
        sleeps.append(seconds)
        raise KeyboardInterrupt()

    monkeypatch.setattr(mod, 'run_loop', fake_run_loop)
    monkeypatch.setattr(mod.time, 'sleep', fake_sleep)

    exit_code = mod.main([])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert cycle_calls == [1]
    assert sleeps == [5]
    assert 'interrupted' in captured.out


def test_main_auto_loop_zero_cycles_exits_cleanly_when_run_loop_interrupts(monkeypatch, capsys):
    mod = load_module()
    args = make_args(auto_loop=True, max_scan_cycles=0, poll_interval_sec=5, base_url='https://example.com')
    cycle_calls = []
    sleeps = []

    monkeypatch.setattr(mod, 'parse_args', lambda argv=None: args)
    monkeypatch.setattr(mod, 'BinanceFuturesClient', lambda **kwargs: DummyClient())
    monkeypatch.setattr(mod.time, 'sleep', lambda seconds: sleeps.append(seconds))

    def fake_run_loop(client, passed_args):
        cycle_calls.append(len(cycle_calls) + 1)
        raise KeyboardInterrupt()

    monkeypatch.setattr(mod, 'run_loop', fake_run_loop)

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
        'trade_management_plan': {'quantity': 12.5},
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
    assert positions['DOGEUSDT']['status'] == 'monitoring'
    assert positions['DOGEUSDT']['monitor_mode'] == 'background_thread'
    assert positions['DOGEUSDT']['monitor_thread_name'] == 'trade-monitor-DOGEUSDT'
    assert positions['DOGEUSDT']['user_data_stream']['status'] == 'started'
    assert positions['DOGEUSDT']['user_data_stream']['listen_key'] == 'dummy-listen-key'
    assert result['cycles'][0]['trade_management']['user_data_stream']['listen_key'] == 'dummy-listen-key'
    assert positions['DOGEUSDT']['book_ticker_websocket']['status'] == 'healthy'
    assert positions['DOGEUSDT']['book_ticker_websocket']['active_streams'] == ['dogeusdt@bookTicker']

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
        'trade_management_plan': {'quantity': 12.5},
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

    def fake_run_book_ticker_websocket_supervisor(store, initial_symbols, symbol_provider, ws_module, **kwargs):
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
            'cycles_completed': 1,
            'reconnect_count': 0,
            'messages_processed_total': 8,
            'samples_written_total': 8,
            'symbols': ['BTCUSDT', 'ETHUSDT'],
            'subscription_version': 1,
        }

    def fake_run_scan_once(client, args):
        call_order.append('run_scan_once')
        health = store.load_json('book_ticker_ws_status', {})
        assert health['symbols'] == ['BTCUSDT', 'ETHUSDT']
        return ({'candidates': ['DOGEUSDT']}, candidate, {'DOGEUSDT': meta})

    monkeypatch.setattr(mod, 'run_book_ticker_websocket_supervisor', fake_run_book_ticker_websocket_supervisor)
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
            'messages_processed': 5,
            'samples_written': 5,
            'active_streams': ['dogeusdt@bookTicker'],
            'last_error': '',
        })
        return {
            'cycles_completed': 1,
            'reconnect_count': 0,
            'messages_processed_total': 5,
            'samples_written_total': 5,
            'symbols': ['DOGEUSDT'],
            'subscription_version': 1,
        }

    monkeypatch.setattr(mod, 'run_book_ticker_websocket_supervisor', fake_run_book_ticker_websocket_supervisor)
    monkeypatch.setattr(mod, 'resolve_auto_loop_book_ticker_symbols', lambda client, args: ['DOGEUSDT'], raising=False)

    result = mod.run_loop(DummyClient(), make_args(auto_loop=True, live=False, runtime_state_dir=str(tmp_path)))

    cycle = result['cycles'][0]
    assert cycle['book_ticker_websocket']['symbols'] == ['DOGEUSDT']
    assert cycle['book_ticker_websocket']['health']['status'] == 'healthy'
    assert cycle['book_ticker_websocket']['health']['active_streams'] == ['dogeusdt@bookTicker']
    assert book_ticker_calls[0]['store'] is store
    assert book_ticker_calls[0]['initial_symbols'] == ['DOGEUSDT']
    assert book_ticker_calls[0]['provided_symbols'] == ['DOGEUSDT']



def test_place_live_trade_returns_fill_feedback_and_emits_entry_before_stop(monkeypatch):
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
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *a, **k: {'status': 'protected', 'expected_order_id': 54321})

    result = mod.place_live_trade(Client(), candidate, leverage=5, meta=meta, args=args)

    assert calls[0][0] == '/fapi/v1/leverage'
    assert calls[1][0] == '/fapi/v1/order'
    assert calls[1][1]['quantity'] == '12.7'
    assert runtime_events[0][0] == 'entry_filled'
    assert notifications[0][0] == 'entry_filled'
    assert runtime_events[1][0] == 'initial_stop_placed'
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


def test_run_loop_background_buy_fill_confirmed_persists_entry_feedback(monkeypatch, tmp_path):
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
    monkeypatch.setattr(mod, 'start_trade_monitor_thread', lambda *args, **kwargs: SimpleNamespace(name='trade-monitor-DOGEUSDT'))

    result = mod.run_loop(DummyClient(), make_args(live=True, auto_loop=True, runtime_state_dir=str(tmp_path)))

    assert result['cycles'][0]['trade_management']['mode'] == 'background_thread'
    positions = store.load_json('positions', {})
    tracked = positions['DOGEUSDT']
    assert tracked['status'] == 'monitoring'
    assert tracked['entry_order_id'] == 12345
    assert tracked['entry_client_order_id'] == 'entry-order-1'
    assert tracked['entry_order_status'] == 'FILLED'
    assert tracked['filled_quantity'] == 12.7
    assert tracked['entry_cum_quote'] == 1.73355
    assert tracked['entry_update_time'] == 1710000000123
    assert tracked['user_data_stream']['status'] == 'started'
    assert tracked['user_data_stream']['listen_key'] == 'dummy-listen-key'
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
        'trade_management_plan': {'quantity': 12.5, 'side': 'SHORT'},
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
    assert tracked['side'] == 'SHORT'
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
        'DOGEUSDT': {
            'symbol': 'DOGEUSDT',
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

    tracked = store.load_json('positions', {})['DOGEUSDT']
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
    position = store.load_json('positions', {})['DOGEUSDT']
    assert position['user_data_stream']['status'] == 'started'
    assert position['user_data_stream']['listen_key'] == 'dummy-listen-key'
    assert position['user_data_stream']['health']['refresh_failure_count'] == 0
    assert position['user_data_stream']['health']['disconnect_count'] == 0
    assert result['cycles'][0]['trade_management']['user_data_stream']['health']['status'] == 'started'
    buy_fill_events = [row for row in store.read_events(limit=20) if row.get('event_type') == 'buy_fill_confirmed']
    assert buy_fill_events[-1]['listen_key'] == 'dummy-listen-key'
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
        'trade_management_plan': {'quantity': 12.5},
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
    position = store.load_json('positions', {})['DOGEUSDT']
    assert position['user_data_stream']['health']['reconnect_count'] == 2
    assert result['cycles'][0]['trade_management']['user_data_stream']['health']['reconnect_count'] == 2


def test_apply_user_data_stream_order_update_reconciles_fill_feedback(tmp_path):
    mod = load_module()
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT': {
            'symbol': 'DOGEUSDT',
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
    position = positions['DOGEUSDT']
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
        'DOGEUSDT': {
            'symbol': 'DOGEUSDT',
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
        tracked = positions.get('DOGEUSDT', {})
        tracked['_debug_current_price'] = sample['price']
        tracked['_debug_ema5m'] = sample['ema5m']
        tracked['_debug_trailing_reference'] = sample['trailing_reference']
        positions['DOGEUSDT'] = tracked
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
    assert rows[6]['exit_reason'] == 'runner'
    assert rows[6]['protection_status'] == 'flat'

    notified_types = [item[0] for item in notifications]
    assert notified_types == event_types[:-1]
