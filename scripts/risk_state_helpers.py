from __future__ import annotations

from typing import Any, Callable, Dict


RiskStateFactory = Callable[[], Dict[str, Any]]
HeatSnapshotBuilder = Callable[[Any], Dict[str, Any]]


def normalize_loaded_risk_state(state: Any, default_risk_state: RiskStateFactory) -> Dict[str, Any]:
    if not isinstance(state, dict):
        return default_risk_state()
    merged = default_risk_state()
    merged.update(state)
    if not isinstance(merged.get('symbol_cooldowns'), dict):
        merged['symbol_cooldowns'] = {}
    if not isinstance(merged.get('portfolio_exposure_pct_by_theme'), dict):
        merged['portfolio_exposure_pct_by_theme'] = {}
    if not isinstance(merged.get('portfolio_exposure_pct_by_correlation'), dict):
        merged['portfolio_exposure_pct_by_correlation'] = {}
    if not isinstance(merged.get('portfolio_heat_r_by_theme'), dict):
        merged['portfolio_heat_r_by_theme'] = {}
    if not isinstance(merged.get('portfolio_heat_r_by_correlation'), dict):
        merged['portfolio_heat_r_by_correlation'] = {}
    return merged


def refresh_risk_state_heat_snapshot(
    risk_state: Dict[str, Any],
    positions_state: Any,
    compute_positions_heat_snapshot: HeatSnapshotBuilder,
) -> Dict[str, Any]:
    merged = dict(risk_state)
    heat_snapshot = compute_positions_heat_snapshot(positions_state)
    if int(heat_snapshot.get('tracked_positions', 0) or 0) > 0:
        merged['portfolio_heat_open_r'] = heat_snapshot.get('open_heat_r', 0.0)
        if heat_snapshot.get('heat_r_by_theme'):
            merged['portfolio_heat_r_by_theme'] = heat_snapshot['heat_r_by_theme']
        if heat_snapshot.get('heat_r_by_correlation'):
            merged['portfolio_heat_r_by_correlation'] = heat_snapshot['heat_r_by_correlation']
    return merged
