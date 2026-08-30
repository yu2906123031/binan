import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace


SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'lifecycle_snapshot.py'
spec = importlib.util.spec_from_file_location('lifecycle_snapshot_test_mod', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class Store:
    def __init__(self):
        self.saved = []
        self.events = []

    def save_json(self, name, payload):
        self.saved.append((name, payload))

    def append_event(self, event_type, payload):
        row = {'event_type': event_type, **payload}
        self.events.append(row)
        return row


def test_build_entry_prediction_snapshot_merges_candidate_and_fill_feedback():
    candidate = SimpleNamespace(
        expected_edge=1.2,
        expected_slippage_pct=0.08,
        last_price=100.0,
        stop_price=98.0,
        trigger_confirmation_count=4,
        state='launch',
        candidate_stage='trade_candidate',
        regime_label='risk_on',
        regime_multiplier=1.1,
        score=82.0,
    )
    live_execution = {
        'entry_price': 100.1,
        'entry_order_feedback': {
            'execution_mode': 'maker_only',
            'maker_or_taker': 'maker',
            'predicted_slippage_bps': 8.0,
            'actual_fill_slippage_bps': 10.0,
            'liquidity_grade_at_entry': 'A',
            'fill_ratio': 1.0,
        },
    }
    snapshot = mod.build_entry_prediction_snapshot(candidate, live_execution)
    assert snapshot['expected_edge'] == 1.2
    assert snapshot['predicted_slippage_bps'] == 8.0
    assert snapshot['actual_fill_slippage_bps'] == 10.0
    assert snapshot['slippage_error_bps'] == 2.0
    assert snapshot['maker_or_taker'] == 'maker'
    assert snapshot['liquidity_grade_at_entry'] == 'A'
    assert snapshot['market_regime_label'] == 'risk_on'
    assert snapshot['market_regime_multiplier'] == 1.1
    assert snapshot['stop_distance_pct'] == 2.0
    assert snapshot['prediction_snapshot_native'] is True


def test_install_hooks_persists_snapshot_and_emits_enriched_entry_fill():
    store = Store()

    def original_persist(_store, _candidate, _live_execution):
        return {
            'DOGEUSDT:LONG': {
                'symbol': 'DOGEUSDT',
                'side': 'LONG',
                'position_key': 'DOGEUSDT:LONG',
                'entry_price': 100.1,
            }
        }, 'DOGEUSDT:LONG'

    def original_append(_store, _symbol, _positions_state, _position_key):
        return {'event_type': 'buy_fill_confirmed'}

    def upsert(positions, payload, key):
        positions = dict(positions)
        positions[key] = dict(payload)
        return positions, key

    strategy = SimpleNamespace(
        persist_live_open_position=original_persist,
        append_buy_fill_confirmed_event=original_append,
        upsert_position_record=upsert,
    )
    mod.install_lifecycle_snapshot_hooks(strategy)
    candidate = SimpleNamespace(
        expected_edge=0.9,
        expected_slippage_pct=0.05,
        last_price=100.0,
        stop_price=98.0,
        state='launch',
        score=80.0,
    )
    live_execution = {
        'entry_price': 100.1,
        'entry_order_feedback': {
            'maker_or_taker': 'maker',
            'execution_mode': 'maker_only',
            'predicted_slippage_bps': 5.0,
            'actual_fill_slippage_bps': 10.0,
            'liquidity_grade_at_entry': 'A',
        },
    }
    positions, key = strategy.persist_live_open_position(store, candidate, live_execution)
    position = positions[key]
    assert position['expected_edge'] == 0.9
    assert position['predicted_slippage_bps'] == 5.0
    assert position['actual_fill_slippage_bps'] == 10.0
    assert position['entry_prediction_snapshot']['prediction_snapshot_native'] is True
    assert store.saved[-1][0] == 'positions'

    result = strategy.append_buy_fill_confirmed_event(store, 'DOGEUSDT', positions, key)
    assert result == {'event_type': 'buy_fill_confirmed'}
    enriched = store.events[-1]
    assert enriched['event_type'] == 'entry_filled'
    assert enriched['expected_edge'] == 0.9
    assert enriched['maker_or_taker'] == 'maker'
    assert enriched['actual_fill_slippage_bps'] == 10.0


def test_install_hooks_is_idempotent():
    calls = {'persist': 0}

    def original_persist(_store, _candidate, _live_execution):
        calls['persist'] += 1
        return {'A:LONG': {'symbol': 'A', 'side': 'LONG'}}, 'A:LONG'

    strategy = SimpleNamespace(persist_live_open_position=original_persist)
    mod.install_lifecycle_snapshot_hooks(strategy)
    first_wrapper = strategy.persist_live_open_position
    mod.install_lifecycle_snapshot_hooks(strategy)
    assert strategy.persist_live_open_position is first_wrapper
