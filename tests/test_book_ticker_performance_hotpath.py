import gzip
import importlib.util
import os
import sys
import time
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

SCRIPT_PATH = SCRIPTS_DIR / 'binance_futures_momentum_long.py'
spec = importlib.util.spec_from_file_location('binance_futures_momentum_long_perf', SCRIPT_PATH)
assert spec is not None
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)

RUNTIME_STORE_PATH = SCRIPTS_DIR / 'runtime_store.py'
runtime_store_spec = importlib.util.spec_from_file_location('runtime_store_perf', RUNTIME_STORE_PATH)
assert runtime_store_spec is not None
runtime_store = importlib.util.module_from_spec(runtime_store_spec)
sys.modules[runtime_store_spec.name] = runtime_store
assert runtime_store_spec.loader is not None
runtime_store_spec.loader.exec_module(runtime_store)


def _reset_perf_caches():
    mod._EVENT_RATE_LIMIT_STATE_CACHE.clear()
    mod._EVENT_RATE_LIMIT_STATE_LAST_SAVE_TS.clear()
    mod._BOOK_TICKER_CACHE_STATE_CACHE.clear()
    mod._BOOK_TICKER_CACHE_LAST_FLUSH_TS.clear()


def test_rate_limited_events_load_state_once_and_flush_periodically(monkeypatch, tmp_path):
    _reset_perf_caches()
    store = mod.RuntimeStateStore(str(tmp_path))
    calls = {'load': 0, 'save': 0, 'append': 0}
    original_load_json = store.load_json
    original_save_json = store.save_json
    original_append_event = store.append_event

    def load_json(name, default=None):
        calls['load'] += 1
        return original_load_json(name, default)

    def save_json(name, payload):
        calls['save'] += 1
        return original_save_json(name, payload)

    def append_event(event_type, payload):
        calls['append'] += 1
        return original_append_event(event_type, payload)

    monkeypatch.setattr(store, 'load_json', load_json)
    monkeypatch.setattr(store, 'save_json', save_json)
    monkeypatch.setattr(store, 'append_event', append_event)
    monkeypatch.setattr(mod, '_EVENT_RATE_LIMIT_STATE_FLUSH_INTERVAL_SECONDS', 45.0)
    ticks = iter([100.0, 110.0, 120.0, 160.0])
    monkeypatch.setattr(mod.time, 'monotonic', lambda: next(ticks))

    assert mod.append_rate_limited_runtime_event(store, 'book_ticker_cache_hit', {}, key='BTCUSDT', min_interval_seconds=0.0)
    assert mod.append_rate_limited_runtime_event(store, 'book_ticker_cache_hit', {}, key='ETHUSDT', min_interval_seconds=0.0)
    assert mod.append_rate_limited_runtime_event(store, 'book_ticker_cache_hit', {}, key='BNBUSDT', min_interval_seconds=0.0)

    assert calls['load'] == 1
    assert calls['append'] == 3
    assert calls['save'] == 1


def test_book_ticker_sample_updates_memory_without_sample_event_or_per_tick_json_load(monkeypatch, tmp_path):
    _reset_perf_caches()
    store = mod.RuntimeStateStore(str(tmp_path))
    calls = {'load': 0, 'save': 0, 'append': 0}
    original_load_json = store.load_json
    original_save_json = store.save_json

    monkeypatch.setattr(mod, '_BOOK_TICKER_DEBUG_SAMPLE_EVENTS', False)
    monkeypatch.setattr(store, 'load_json', lambda name, default=None: calls.__setitem__('load', calls['load'] + 1) or original_load_json(name, default))
    monkeypatch.setattr(store, 'save_json', lambda name, payload: calls.__setitem__('save', calls['save'] + 1) or original_save_json(name, payload))
    monkeypatch.setattr(store, 'append_event', lambda event_type, payload: calls.__setitem__('append', calls['append'] + 1) or {'event_type': event_type, **payload})
    monkeypatch.setattr(mod.time, 'monotonic', lambda: 100.0)

    for i in range(3):
        result = mod.append_book_ticker_cache_sample(
            store,
            'BTCUSDT',
            {'s': 'BTCUSDT', 'b': str(100 + i), 'a': str(101 + i), 'B': '1', 'A': '2', 'E': 123456 + i},
        )
        assert result['event'] is None

    assert calls['load'] == 1
    assert calls['save'] == 1
    assert calls['append'] == 0
    state = mod._BOOK_TICKER_CACHE_STATE_CACHE[mod._runtime_store_cache_key(store)]
    assert state['BTCUSDT']['last_bid'] == '102'


def test_book_ticker_batch_flush_obeys_minimum_30_second_flush_interval(monkeypatch, tmp_path):
    _reset_perf_caches()
    store = mod.RuntimeStateStore(str(tmp_path))
    calls = {'save': 0, 'append': 0}
    original_save_json = store.save_json
    monkeypatch.setattr(mod, '_BOOK_TICKER_DEBUG_SAMPLE_EVENTS', False)
    def save_json(name, payload):
        if name == 'book_ticker_cache':
            calls['save'] += 1
        return original_save_json(name, payload)
    monkeypatch.setattr(store, 'save_json', save_json)
    monkeypatch.setattr(store, 'append_event', lambda event_type, payload: calls.__setitem__('append', calls['append'] + 1) or {'event_type': event_type, **payload})
    ticks = iter([100.0, 100.0, 110.0, 110.0, 161.0, 161.0])
    monkeypatch.setattr(mod.time, 'monotonic', lambda: next(ticks))

    samples = [{'symbol': 'BTCUSDT', 'sample': {'bidPrice': '1', 'askPrice': '2', 'bidQty': '1', 'askQty': '1'}}]
    assert mod.flush_book_ticker_cache_samples(store, samples)['events_written'] == 1
    assert calls['save'] == 1
    assert mod.flush_book_ticker_cache_samples(store, samples)['events_written'] == 0
    assert calls['save'] == 1
    assert mod.flush_book_ticker_cache_samples(store, samples)['samples_flushed'] == 1
    assert calls['save'] == 2
    assert calls['append'] == 1


def test_events_jsonl_rotates_above_50mb_and_gzips_old_file(tmp_path):
    store = runtime_store.RuntimeStateStore(str(tmp_path))
    path = tmp_path / 'events.jsonl'
    path.write_bytes(b'x' * (50 * 1024 * 1024))

    store.append_event('probe', {'symbol': 'btcusdt'})

    rotated = list(tmp_path.glob('events.jsonl.*.gz'))
    assert len(rotated) == 1
    with gzip.open(rotated[0], 'rb') as fh:
        assert fh.read(1) == b'x'
    rows = path.read_text(encoding='utf-8').splitlines()
    assert len(rows) == 1
    assert 'probe' in rows[0]


def test_events_jsonl_rotation_prunes_archives_older_than_seven_days(tmp_path):
    store = runtime_store.RuntimeStateStore(str(tmp_path))
    path = tmp_path / 'events.jsonl'
    old = tmp_path / 'events.jsonl.20000101T000000Z.gz'
    old.write_bytes(gzip.compress(b'old'))
    old_time = time.time() - 8 * 86400
    os.utime(old, (old_time, old_time))
    path.write_bytes(b'x' * (50 * 1024 * 1024))

    store.append_event('probe', {})

    assert not old.exists()
