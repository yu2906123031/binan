import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
SCRIPT_PATH = SCRIPTS_DIR / 'request_throttle_hardening.py'
spec = importlib.util.spec_from_file_location('request_throttle_hardening', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_ordinary_429_uses_short_cooldown(monkeypatch):
    calls = []

    class Breaker:
        def open(self, *, reason, retry_after_ms=None):
            calls.append((reason, retry_after_ms))

    monkeypatch.setattr(mod.time, 'time', lambda: 1000.0)
    request_manager = SimpleNamespace(CircuitBreaker=Breaker)
    mod.install_request_throttle_hardening(request_manager, ordinary_429_cooldown_seconds=5.0)
    Breaker().open(reason='binance_429_rate_limit', retry_after_ms=None)
    assert calls == [('binance_429_rate_limit', 1_005_000)]


def test_explicit_retry_after_is_preserved_for_429(monkeypatch):
    calls = []

    class Breaker:
        def open(self, *, reason, retry_after_ms=None):
            calls.append((reason, retry_after_ms))

    monkeypatch.setattr(mod.time, 'time', lambda: 1000.0)
    request_manager = SimpleNamespace(CircuitBreaker=Breaker)
    mod.install_request_throttle_hardening(request_manager, ordinary_429_cooldown_seconds=5.0)
    Breaker().open(reason='binance_429_rate_limit', retry_after_ms=2_000_000)
    assert calls == [('binance_429_rate_limit', 2_000_000)]


def test_418_keeps_original_long_cooldown_behavior():
    calls = []

    class Breaker:
        def open(self, *, reason, retry_after_ms=None):
            calls.append((reason, retry_after_ms))

    request_manager = SimpleNamespace(CircuitBreaker=Breaker)
    mod.install_request_throttle_hardening(request_manager, ordinary_429_cooldown_seconds=5.0)
    Breaker().open(reason='binance_418_ip_ban', retry_after_ms=None)
    assert calls == [('binance_418_ip_ban', None)]


def test_throttle_hardening_install_is_idempotent():
    class Breaker:
        def open(self, *, reason, retry_after_ms=None):
            return (reason, retry_after_ms)

    request_manager = SimpleNamespace(CircuitBreaker=Breaker)
    mod.install_request_throttle_hardening(request_manager)
    first = Breaker.open
    mod.install_request_throttle_hardening(request_manager)
    assert Breaker.open is first
