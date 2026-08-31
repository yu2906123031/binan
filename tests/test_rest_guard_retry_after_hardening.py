import datetime
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace


SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'rest_guard_retry_after_hardening.py'
spec = importlib.util.spec_from_file_location('rest_guard_retry_after_hardening_test_mod', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class Response:
    def __init__(self, status_code=429, headers=None, payload=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload or {}
        self.text = str(self._payload)

    def json(self):
        return self._payload


def test_retry_after_delta_seconds_becomes_absolute_timestamp():
    assert mod.extract_retry_after_header_ms({'Retry-After': '7'}, now_ms=1_000_000) == 1_007_000


def test_retry_after_http_date_becomes_absolute_timestamp():
    target = datetime.datetime(2026, 8, 31, 2, 0, 0, tzinfo=datetime.timezone.utc)
    header = 'Mon, 31 Aug 2026 02:00:00 GMT'
    assert mod.extract_retry_after_header_ms({'retry-after': header}, now_ms=int(target.timestamp() * 1000) - 5000) == int(target.timestamp() * 1000)


def test_response_chooses_later_of_retry_after_header_and_ban_message():
    now_ms = 1_000_000
    strategy = SimpleNamespace(
        _extract_retry_after_ms_from_message=lambda payload: 1_020_000,
    )
    response = Response(headers={'Retry-After': '5'}, payload={'msg': 'banned until 1020000'})
    assert mod.extract_response_retry_after_ms(strategy, response, now_ms=now_ms) == 1_020_000


def _fake_strategy(now_ms=1_000_000):
    state = {
        'rest_circuit_state': 'CLOSED',
        'rest_circuit_reason': '',
        'circuit_open_until_ms': 0,
        'next_rest_probe_at_ms': 0,
        'half_open_probe_used': False,
    }
    calls = {'after_response': 0, 'after_error': 0}

    def with_state(mutator):
        return mutator(state)

    def snapshot():
        return dict(state)

    def original_after_response(response, *, purpose='', path='', request_latency_ms=0):
        calls['after_response'] += 1
        status = int(response.status_code)
        if status in {418, 429}:
            fallback = 3600.0 if status == 418 else strategy.REST_429_COOLDOWN_SECONDS
            state['rest_circuit_state'] = 'OPEN'
            state['rest_circuit_reason'] = f'http_{status}_rate_limit'
            state['circuit_open_until_ms'] = now_ms + int(fallback * 1000)
            state['next_rest_probe_at_ms'] = state['circuit_open_until_ms']

    def original_after_error(error, fallback_cooldown_seconds=900.0):
        calls['after_error'] += 1
        message = str(error).lower()
        if '429' in message:
            state['rest_circuit_state'] = 'OPEN'
            state['rest_circuit_reason'] = 'binance_429_rate_limit'
            state['circuit_open_until_ms'] = now_ms + int(strategy.REST_429_COOLDOWN_SECONDS * 1000)
            state['next_rest_probe_at_ms'] = state['circuit_open_until_ms']

    strategy = SimpleNamespace(
        REST_429_COOLDOWN_SECONDS=120.0,
        REST_418_COOLDOWN_SECONDS=3600.0,
        _rest_now_ms=lambda: now_ms,
        _with_binance_rest_guard_state=with_state,
        _binance_rest_guard_snapshot=snapshot,
        _binance_rest_guard_after_response=original_after_response,
        _binance_rest_guard_after_error=original_after_error,
        _extract_retry_after_ms_from_message=lambda message: None,
    )
    return strategy, state, calls


def test_ordinary_429_uses_short_fallback_in_real_rest_guard_layer():
    strategy, state, _ = _fake_strategy()
    mod.install_rest_guard_retry_after_hardening(strategy)
    strategy._binance_rest_guard_after_response(Response(status_code=429))
    assert strategy.REST_429_COOLDOWN_SECONDS == 5.0
    assert state['circuit_open_until_ms'] == 1_005_000


def test_response_retry_after_is_preserved_when_error_handler_runs_afterward():
    strategy, state, calls = _fake_strategy()
    mod.install_rest_guard_retry_after_hardening(strategy)
    strategy._binance_rest_guard_after_response(Response(status_code=429, headers={'Retry-After': '17'}))
    assert state['circuit_open_until_ms'] == 1_017_000
    strategy._binance_rest_guard_after_error(RuntimeError('Binance API error 429: too many requests'))
    assert state['circuit_open_until_ms'] == 1_017_000
    assert calls['after_error'] == 0


def test_418_without_hint_retains_long_ip_ban_cooldown():
    strategy, state, _ = _fake_strategy()
    mod.install_rest_guard_retry_after_hardening(strategy)
    strategy._binance_rest_guard_after_response(Response(status_code=418))
    assert state['circuit_open_until_ms'] == 4_600_000


def test_installation_is_idempotent():
    strategy, _, _ = _fake_strategy()
    mod.install_rest_guard_retry_after_hardening(strategy)
    first_response = strategy._binance_rest_guard_after_response
    first_error = strategy._binance_rest_guard_after_error
    mod.install_rest_guard_retry_after_hardening(strategy)
    assert strategy._binance_rest_guard_after_response is first_response
    assert strategy._binance_rest_guard_after_error is first_error
