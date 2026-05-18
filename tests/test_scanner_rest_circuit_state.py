import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import binance_futures_momentum_long as m


class FakeStore:
    def __init__(self, payload):
        self.payload = payload
        self.saved = []

    def load_json(self, name, default=None):
        assert name == 'scanner_rest_circuit_breaker'
        return self.payload

    def save_json(self, name, payload):
        self.saved.append((name, payload))
        self.payload = payload
        return payload


def test_load_open_scanner_rest_circuit_closes_expired_open_state(monkeypatch):
    monkeypatch.setattr(m, 'current_time_ms', lambda: 2_000_000)
    store = FakeStore({'state': 'open', 'reason': 'binance_ip_ban', 'retry_after_ms': 1_000_000})

    assert m.load_open_scanner_rest_circuit(store) is None

    assert store.saved
    name, payload = store.saved[-1]
    assert name == 'scanner_rest_circuit_breaker'
    assert payload['state'] == 'closed'
    assert payload['previous_state'] == 'open'
    assert payload['closed_reason'] == 'retry_after_elapsed'


def test_load_open_scanner_rest_circuit_keeps_active_open_state(monkeypatch):
    monkeypatch.setattr(m, 'current_time_ms', lambda: 2_000_000)
    payload = {'state': 'open', 'reason': 'binance_ip_ban', 'retry_after_ms': 3_000_000}
    store = FakeStore(payload)

    assert m.load_open_scanner_rest_circuit(store) == payload
    assert store.saved == []
