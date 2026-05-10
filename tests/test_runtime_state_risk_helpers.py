import pathlib
import sys
from typing import Any, Dict, List, Tuple

SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent.parent / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import risk_state_helpers
import runtime_state_risk_helpers as helpers


class StubStore:
    def __init__(self, *, error_map: Dict[str, Tuple[Any, Dict[str, Any] | None]] | None = None, json_map: Dict[str, Any] | None = None):
        self.error_map = error_map or {}
        self.json_map = json_map or {}

    def load_json_with_error(self, key: str, default: Any):
        return self.error_map.get(key, (default, None))

    def load_json(self, key: str, default: Any):
        return self.json_map.get(key, default)


def _iter_positions(state: Any) -> List[Tuple[str, Dict[str, Any]]]:
    assert isinstance(state, dict)
    return list(state.items())


def test_load_local_open_positions_for_risk_emits_degraded_event_and_returns_empty_rows_on_malformed_positions_json():
    store = StubStore(
        error_map={
            'positions': (
                {},
                {'reason': 'json_decode_error', 'path': 'positions.json'},
            )
        }
    )
    emitted: list[tuple[str, Dict[str, Any], str]] = []

    rows = helpers.load_local_open_positions_for_risk(
        store,
        should_emit_runtime_state_degraded=lambda _store, key: key == 'positions',
        append_runtime_state_degraded_event=lambda _store, event_type, payload, key, min_interval_seconds=60.0: emitted.append((event_type, payload, key)),
        build_local_open_positions_from_state=helpers.build_local_open_positions_from_state,
        normalize_position_side=lambda side: str(side or '').upper(),
        to_float=float,
        iter_canonical_open_positions=_iter_positions,
    )

    assert rows == []
    assert emitted == [
        (
            'runtime_state_degraded',
            {
                'reason': 'json_decode_error',
                'path': 'positions.json',
                'fallback_used': 'empty_positions',
                'consumer': 'build_local_open_positions_for_risk',
            },
            'positions',
        )
    ]


def test_load_runtime_risk_state_uses_default_state_and_refreshes_heat_snapshot_on_malformed_json():
    default_state = {
        'portfolio_heat_open_r': 0.0,
        'portfolio_heat_r_by_theme': {},
        'portfolio_heat_r_by_correlation': {},
        'max_concurrent_positions': 3,
    }
    positions_state = {
        'BTCUSDT:LONG': {
            'symbol': 'BTCUSDT',
            'side': 'LONG',
            'remaining_quantity': 1.2,
            'entry_price': 50000,
        }
    }
    store = StubStore(
        error_map={
            'risk_state': (
                {'corrupted': True},
                {'reason': 'json_decode_error', 'path': 'risk_state.json'},
            )
        },
        json_map={'positions': positions_state},
    )
    emitted: list[tuple[str, Dict[str, Any], str]] = []

    def default_risk_state() -> Dict[str, Any]:
        return dict(default_state)

    def normalize_loaded_risk_state(state: Any) -> Dict[str, Any]:
        normalized = dict(state)
        normalized['normalized'] = True
        return normalized

    def compute_positions_heat_snapshot(state: Any) -> Dict[str, Any]:
        assert state == positions_state
        return {
            'tracked_positions': 1,
            'open_heat_r': 1.7,
            'heat_r_by_theme': {'majors': 1.0},
            'heat_r_by_correlation': {'btc-beta': 0.7},
        }

    result = helpers.load_runtime_risk_state(
        store,
        should_emit_runtime_state_degraded=lambda _store, key: key == 'risk_state',
        append_runtime_state_degraded_event=lambda _store, event_type, payload, key, min_interval_seconds=60.0: emitted.append((event_type, payload, key)),
        default_risk_state=default_risk_state,
        normalize_loaded_risk_state=normalize_loaded_risk_state,
        refresh_risk_state_heat_snapshot=risk_state_helpers.refresh_risk_state_heat_snapshot,
        compute_positions_heat_snapshot=compute_positions_heat_snapshot,
    )

    assert result == {
        'portfolio_heat_open_r': 1.7,
        'portfolio_heat_r_by_theme': {'majors': 1.0},
        'portfolio_heat_r_by_correlation': {'btc-beta': 0.7},
        'max_concurrent_positions': 3,
        'normalized': True,
    }
    assert emitted == [
        (
            'runtime_state_degraded',
            {
                'reason': 'json_decode_error',
                'path': 'risk_state.json',
                'fallback_used': 'default_risk_state',
                'consumer': 'load_risk_state',
            },
            'risk_state',
        )
    ]


def test_load_runtime_risk_state_preserves_existing_heat_when_snapshot_returns_empty_open_positions():
    store = StubStore(
        error_map={
            'risk_state': (
                {
                    'portfolio_heat_open_r': 2.4,
                    'portfolio_heat_r_by_theme': {'ai': 1.2},
                    'portfolio_heat_r_by_correlation': {'sol-beta': 0.8},
                },
                None,
            )
        },
        json_map={'positions': {}},
    )

    result = helpers.load_runtime_risk_state(
        store,
        should_emit_runtime_state_degraded=lambda *_args, **_kwargs: False,
        append_runtime_state_degraded_event=lambda *_args, **_kwargs: None,
        default_risk_state=lambda: {},
        normalize_loaded_risk_state=lambda state: dict(state),
        refresh_risk_state_heat_snapshot=risk_state_helpers.refresh_risk_state_heat_snapshot,
        compute_positions_heat_snapshot=lambda _state: {'open_risk_r': 0.0},
    )

    assert result == {
        'portfolio_heat_open_r': 2.4,
        'portfolio_heat_r_by_theme': {'ai': 1.2},
        'portfolio_heat_r_by_correlation': {'sol-beta': 0.8},
    }
