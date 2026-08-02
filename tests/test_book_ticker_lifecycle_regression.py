import argparse
import datetime
import importlib.util
import threading
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
SCRIPT_PATH = SCRIPTS_DIR / 'binance_futures_momentum_long.py'
spec = importlib.util.spec_from_file_location('book_ticker_lifecycle_subject', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class Store:
    def __init__(self, payload=None): self.data = dict(payload or {})
    def load_json(self, key, default=None): return self.data.get(key, default)
    def save_json(self, key, value): self.data[key] = value
    def append_event(self, event_type, payload): return {'event_type': event_type, **payload}


def test_fresh_heartbeat_cannot_hide_stale_message_or_sample():
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    health = {'status': 'healthy', 'updated_at': '2026-01-01T00:00:00Z',
              'last_message_at': '2025-12-31T23:58:00Z', 'last_sample_at': '2026-01-01T00:00:00Z',
              'messages_processed': 2, 'samples_written': 2}
    result = mod.evaluate_websocket_freshness(health, now=now, max_age_seconds=30)
    assert result['state'] == 'dead'
    assert result['age_seconds'] == 120.0


def test_updated_at_is_not_used_as_market_data_timestamp():
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    result = mod.evaluate_websocket_freshness({'status': 'healthy', 'updated_at': '2026-01-01T00:00:00Z', 'messages_processed': 2, 'samples_written': 2}, now=now)
    assert result['state'] == 'dead'


def test_execution_fails_closed_when_persisted_status_missing(monkeypatch):
    monkeypatch.setattr(mod, 'place_live_trade', lambda *a, **k: (_ for _ in ()).throw(AssertionError('must not execute')))
    request = {'candidate': argparse.Namespace(symbol='BTCUSDT', side='LONG'), 'meta': {}, 'cycle': {}}
    result = mod.execution_cycle(None, argparse.Namespace(require_book_ticker_ws=True), request, store=Store())
    assert result['cycle']['final_execution_gate_action'] == 'veto'
    assert result['manager_update']['state'] == 'SCAN'


def test_unavailable_branch_overwrites_old_healthy_status():
    store = Store({'book_ticker_ws_status': {'status': 'healthy'}})
    config = mod.AutoLoopBookTickerWebsocketMonitorConfig(websocket_capability_probe=lambda: None)
    result = mod.run_auto_loop_book_ticker_websocket_monitor_core(store=store, config=config)
    assert result['health']['status'] == 'unavailable'
    assert store.data['book_ticker_ws_status']['active'] is False
    assert store.data['book_ticker_ws_status']['thread_count'] == 0


def test_superseded_generation_does_not_reconnect_after_close():
    opened = []
    class WS: pass
    def open_ws(*a, **k): opened.append(WS()); return opened[-1]
    def monitor(*a, **k):
        mod._BOOK_TICKER_WS_SUPERVISOR_STATE['generation_id'] = 8
        return {'status': 'disconnected', 'messages_processed': 0, 'samples_written': 0}
    mod._BOOK_TICKER_WS_SUPERVISOR_STATE['generation_id'] = 7
    result = mod.run_book_ticker_websocket_supervisor(Store(), ['BTCUSDT'], None, object(), open_websocket_fn=open_ws, monitor_cycle_fn=monitor, generation_id=7)
    assert len(opened) == 1
    assert result['cycles_completed'] == 1


def test_initial_open_superseded_socket_is_closed_and_not_registered():
    class WS:
        def __init__(self): self.closed = False
        def close(self): self.closed = True
    replacement = object(); opened = WS()
    mod._BOOK_TICKER_WS_SUPERVISOR_STATE.update(generation_id=11, ws=replacement)
    def open_ws(*args, **kwargs):
        mod._BOOK_TICKER_WS_SUPERVISOR_STATE.update(generation_id=12, ws=replacement)
        return opened
    result = mod.run_book_ticker_websocket_supervisor(Store(), ['BTCUSDT'], None, object(), open_websocket_fn=open_ws, generation_id=11)
    assert result['stop_reason'] == 'superseded_generation'
    assert opened.closed is True
    assert mod._BOOK_TICKER_WS_SUPERVISOR_STATE['ws'] is replacement


def test_reconnect_open_superseded_socket_is_closed_and_not_registered():
    class WS:
        def __init__(self): self.closed = False
        def close(self): self.closed = True
    sockets = []; replacement = object()
    mod._BOOK_TICKER_WS_SUPERVISOR_STATE.update(generation_id=21, ws=None)
    def open_ws(*args, **kwargs):
        ws = WS(); sockets.append(ws)
        if len(sockets) == 2:
            mod._BOOK_TICKER_WS_SUPERVISOR_STATE.update(generation_id=22, ws=replacement)
        return ws
    result = mod.run_book_ticker_websocket_supervisor(
        Store(), ['BTCUSDT'], None, object(), open_websocket_fn=open_ws,
        monitor_cycle_fn=lambda *a, **k: {'status': 'disconnected', 'messages_processed': 0, 'samples_written': 0},
        generation_id=21, reconnect_backoff_seconds=0)
    assert result['cycles_completed'] == 1
    assert sockets[1].closed is True
    assert mod._BOOK_TICKER_WS_SUPERVISOR_STATE['ws'] is replacement


def test_stale_generation_health_writer_refuses_persistence():
    store = Store({'book_ticker_ws_status': {'status': 'healthy', 'generation_id': 32}})
    mod._BOOK_TICKER_WS_SUPERVISOR_STATE['generation_id'] = 32
    result = mod.update_book_ticker_ws_health_state(store, 'disconnected', ['BTCUSDT'], 1, 1, generation_id=31)
    assert result['persisted'] is False
    assert store.data['book_ticker_ws_status']['status'] == 'healthy'


def test_real_monitor_results_persist_and_preserve_market_data_timestamps():
    store = Store(); mod._BOOK_TICKER_WS_SUPERVISOR_STATE['generation_id'] = 41
    results = iter([{'status': 'healthy', 'messages_processed': 1, 'samples_written': 1},
                    {'status': 'idle_timeout', 'messages_processed': 0, 'samples_written': 0}])
    snapshots = []; original_save = store.save_json
    def save(key, value):
        original_save(key, dict(value)); snapshots.append(dict(value))
    store.save_json = save
    mod.run_book_ticker_websocket_supervisor(
        store, ['BTCUSDT'], None, object(), open_websocket_fn=lambda *a, **k: object(),
        monitor_cycle_fn=lambda *a, **k: next(results), generation_id=41,
        max_supervisor_cycles=2, zero_message_timeout_reconnect_threshold=3)
    cycle_health = [row for row in snapshots if row.get('messages_processed') == 1]
    assert cycle_health[0]['last_message_at'] and cycle_health[0]['last_sample_at']
    assert cycle_health[-1]['last_message_at'] == cycle_health[0]['last_message_at']
    assert cycle_health[-1]['last_sample_at'] == cycle_health[0]['last_sample_at']


def test_runtime_doctor_rejects_stale_historical_healthy_status(tmp_path):
    path = Path('/root/.hermes/scripts/binance_runtime_ctl.py')
    spec = importlib.util.spec_from_file_location('runtime_ctl_lifecycle', path)
    ctl = importlib.util.module_from_spec(spec); spec.loader.exec_module(ctl)
    state = tmp_path / 'state'; state.mkdir()
    (state / 'book_ticker_ws_status.json').write_text('{"status":"healthy","thread_count":1,"updated_at":"2020-01-01T00:00:00Z"}')
    (state / 'ticker_24hr_cache_refresher_heartbeat.json').write_text('{"status":"healthy","thread_count":1}')
    report = ctl.build_doctor_report(runtime_state_dir=state, supervisor_pids=[1], live_child_pids=[2], scanner_deadman_pids=[], account_state={'positions': [], 'open_orders': []})
    assert report['websocket_supervisor_thread_count'] == 0
    assert 'websocket_supervisor_thread_count_not_one' in report['reasons']
