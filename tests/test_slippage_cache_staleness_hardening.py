import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace


SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'slippage_cache_staleness_hardening.py'
spec = importlib.util.spec_from_file_location('slippage_cache_staleness_hardening_test_mod', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def _fake_policy():
    cache = {
        'loaded_at_monotonic': 0.0,
        'events_mtime_ns': None,
        'payload': None,
        'last_error': '',
    }
    calls = {'load': 0}
    state = {'fail': False, 'generation': 0}

    def load_calibration_payload(*, now_monotonic=None, refresh_seconds=300.0, **kwargs):
        calls['load'] += 1
        current = float(now_monotonic or 0.0)
        cached = cache.get('payload')
        age = current - float(cache.get('loaded_at_monotonic') or 0.0)
        if isinstance(cached, dict) and age >= 0 and age < refresh_seconds:
            return cached
        if isinstance(cached, dict) and cache.get('events_mtime_ns') == 1:
            cache['loaded_at_monotonic'] = current
            return cached
        if state['fail']:
            cache['loaded_at_monotonic'] = current
            cache['last_error'] = 'RuntimeError: failed'
            return cached if isinstance(cached, dict) else {}
        state['generation'] += 1
        payload = {'generation': state['generation']}
        cache.update({
            'loaded_at_monotonic': current,
            'events_mtime_ns': 1,
            'payload': payload,
            'last_error': '',
        })
        return payload

    def reset_calibration_cache():
        cache.update({
            'loaded_at_monotonic': 0.0,
            'events_mtime_ns': None,
            'payload': None,
            'last_error': '',
        })

    policy = SimpleNamespace(
        _POLICY_CACHE=cache,
        load_calibration_payload=load_calibration_payload,
        reset_calibration_cache=reset_calibration_cache,
        time=SimpleNamespace(monotonic=lambda: 0.0),
    )
    return policy, cache, calls, state


def test_unchanged_events_are_forced_to_rebuild_after_max_staleness():
    policy, cache, _, _ = _fake_policy()
    mod.install_slippage_cache_staleness_hardening(policy)
    first = policy.load_calibration_payload(now_monotonic=100.0, refresh_seconds=0.0, max_stale_seconds=1000.0)
    second = policy.load_calibration_payload(now_monotonic=500.0, refresh_seconds=0.0, max_stale_seconds=1000.0)
    third = policy.load_calibration_payload(now_monotonic=1100.0, refresh_seconds=0.0, max_stale_seconds=1000.0)
    assert first == {'generation': 1}
    assert second == first
    assert third == {'generation': 2}
    assert cache['successful_at_monotonic'] == 1100.0


def test_failed_rebuild_keeps_recent_last_known_good():
    policy, cache, _, state = _fake_policy()
    mod.install_slippage_cache_staleness_hardening(policy)
    good = policy.load_calibration_payload(now_monotonic=100.0, refresh_seconds=0.0, max_stale_seconds=1000.0)
    state['fail'] = True
    cache['events_mtime_ns'] = -1
    failed = policy.load_calibration_payload(now_monotonic=500.0, refresh_seconds=0.0, max_stale_seconds=1000.0)
    assert failed == good
    assert cache['successful_at_monotonic'] == 100.0
    assert cache['last_error'] == 'RuntimeError: failed'


def test_failed_rebuild_drops_stale_last_known_good_to_neutral():
    policy, cache, _, state = _fake_policy()
    mod.install_slippage_cache_staleness_hardening(policy)
    policy.load_calibration_payload(now_monotonic=100.0, refresh_seconds=0.0, max_stale_seconds=1000.0)
    state['fail'] = True
    failed = policy.load_calibration_payload(now_monotonic=1100.0, refresh_seconds=0.0, max_stale_seconds=1000.0)
    assert failed == {}
    assert cache['payload'] == {}
    assert cache['successful_at_monotonic'] == 0.0
    assert cache['last_error'] == 'RuntimeError: failed'


def test_successful_rebuild_resets_failure_and_staleness_clock():
    policy, cache, _, state = _fake_policy()
    mod.install_slippage_cache_staleness_hardening(policy)
    policy.load_calibration_payload(now_monotonic=100.0, refresh_seconds=0.0, max_stale_seconds=1000.0)
    state['fail'] = True
    cache['events_mtime_ns'] = -1
    policy.load_calibration_payload(now_monotonic=500.0, refresh_seconds=0.0, max_stale_seconds=1000.0)
    state['fail'] = False
    cache['events_mtime_ns'] = -1
    recovered = policy.load_calibration_payload(now_monotonic=600.0, refresh_seconds=0.0, max_stale_seconds=1000.0)
    assert recovered == {'generation': 2}
    assert cache['successful_at_monotonic'] == 600.0
    assert cache['last_error'] == ''


def test_reset_clears_successful_timestamp_and_install_is_idempotent():
    policy, cache, _, _ = _fake_policy()
    mod.install_slippage_cache_staleness_hardening(policy)
    first_loader = policy.load_calibration_payload
    mod.install_slippage_cache_staleness_hardening(policy)
    assert policy.load_calibration_payload is first_loader
    policy.load_calibration_payload(now_monotonic=100.0, refresh_seconds=0.0)
    policy.reset_calibration_cache()
    assert cache['successful_at_monotonic'] == 0.0
