from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple


CanonicalOpenPositionsIter = Callable[[Any], List[Tuple[str, Dict[str, Any]]]]
NormalizePositionSide = Callable[[Any], str]
ShouldEmitRuntimeStateDegraded = Callable[[Any, str], bool]
AppendRuntimeStateDegradedEvent = Callable[[Any, str, Dict[str, Any], str], None]
ToFloat = Callable[..., float]
DefaultRiskState = Callable[[], Dict[str, Any]]
NormalizeLoadedRiskState = Callable[[Any], Dict[str, Any]]
RefreshRiskStateHeatSnapshot = Callable[[Dict[str, Any], Any], Dict[str, Any]]
ComputePositionsHeatSnapshot = Callable[[Any], Dict[str, Any]]


def build_local_open_positions_from_state(
    positions_state: Any,
    *,
    error: Optional[Dict[str, Any]],
    normalize_position_side: NormalizePositionSide,
    to_float: ToFloat,
    iter_canonical_open_positions: CanonicalOpenPositionsIter,
) -> List[Dict[str, Any]]:
    if error:
        return []
    rows: List[Dict[str, Any]] = []
    for _key, position in iter_canonical_open_positions(positions_state):
        side = normalize_position_side(position.get('side') or position.get('position_side'))
        quantity = abs(to_float(position.get('remaining_quantity') or position.get('quantity') or position.get('filled_quantity')))
        entry_price = abs(to_float(position.get('entry_price')))
        rows.append({
            'symbol': str(position.get('symbol') or '').upper(),
            'side': side,
            'positionSide': side,
            'quantity': quantity,
            'positionAmt': quantity if side == 'LONG' else -quantity,
            'entryPrice': entry_price,
            'notional': abs(to_float(position.get('notional'))) or quantity * entry_price,
        })
    return rows


def load_local_open_positions_for_risk(
    store: Any,
    *,
    should_emit_runtime_state_degraded: ShouldEmitRuntimeStateDegraded,
    append_runtime_state_degraded_event: AppendRuntimeStateDegradedEvent,
    build_local_open_positions_from_state: Callable[..., List[Dict[str, Any]]],
    normalize_position_side: NormalizePositionSide,
    to_float: ToFloat,
    iter_canonical_open_positions: CanonicalOpenPositionsIter,
) -> List[Dict[str, Any]]:
    positions_state, error = store.load_json_with_error('positions', {})
    if error and should_emit_runtime_state_degraded(store, 'positions'):
        append_runtime_state_degraded_event(
            store,
            'runtime_state_degraded',
            {
                **error,
                'fallback_used': 'empty_positions',
                'consumer': 'build_local_open_positions_for_risk',
            },
            key='positions',
        )
    return build_local_open_positions_from_state(
        positions_state,
        error=error,
        normalize_position_side=normalize_position_side,
        to_float=to_float,
        iter_canonical_open_positions=iter_canonical_open_positions,
    )


def load_runtime_risk_state(
    store: Any,
    *,
    should_emit_runtime_state_degraded: ShouldEmitRuntimeStateDegraded,
    append_runtime_state_degraded_event: AppendRuntimeStateDegradedEvent,
    default_risk_state: DefaultRiskState,
    normalize_loaded_risk_state: NormalizeLoadedRiskState,
    refresh_risk_state_heat_snapshot: RefreshRiskStateHeatSnapshot,
    compute_positions_heat_snapshot: ComputePositionsHeatSnapshot,
) -> Dict[str, Any]:
    state, error = store.load_json_with_error('risk_state', default_risk_state())
    if error and should_emit_runtime_state_degraded(store, 'risk_state'):
        append_runtime_state_degraded_event(
            store,
            'runtime_state_degraded',
            {
                **error,
                'fallback_used': 'default_risk_state',
                'consumer': 'load_risk_state',
            },
            key='risk_state',
        )
    normalized = normalize_loaded_risk_state(default_risk_state() if error else state)
    positions_state = store.load_json('positions', {})
    return refresh_risk_state_heat_snapshot(normalized, positions_state, compute_positions_heat_snapshot)
