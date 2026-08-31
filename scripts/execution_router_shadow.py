from __future__ import annotations

from typing import Any

ROUTES = ('market', 'aggressive_limit', 'passive_limit', 'aggressive_limit_replace')


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def effective_execution_cost_bps(
    *,
    slippage_bps: float,
    missed_alpha_bps: float = 0.0,
    partial_fill_ratio: float = 1.0,
    cancel_replace_penalty_bps: float = 0.0,
) -> float:
    fill_ratio = max(0.0, min(float(partial_fill_ratio), 1.0))
    unfilled = 1.0 - fill_ratio
    return max(float(slippage_bps), 0.0) + max(float(missed_alpha_bps), 0.0) * unfilled + max(float(cancel_replace_penalty_bps), 0.0)


def evaluate_execution_routes(observations: dict[str, dict[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for route in ROUTES:
        row = observations.get(route)
        if not isinstance(row, dict):
            continue
        effective = effective_execution_cost_bps(
            slippage_bps=_num(row.get('slippage_bps')),
            missed_alpha_bps=_num(row.get('missed_alpha_bps')),
            partial_fill_ratio=_num(row.get('fill_ratio'), 1.0),
            cancel_replace_penalty_bps=_num(row.get('cancel_replace_penalty_bps')),
        )
        rows.append({
            'route': route,
            'effective_cost_bps': round(effective, 4),
            'fill_ratio': round(max(0.0, min(_num(row.get('fill_ratio'), 1.0), 1.0)), 4),
            'sample_count': int(_num(row.get('sample_count'))),
        })
    eligible = [row for row in rows if row['sample_count'] >= 30]
    winner = min(eligible, key=lambda row: row['effective_cost_bps']) if eligible else None
    return {
        'routes': rows,
        'shadow_winner': winner,
        'live_route_change_allowed': False,
        'reason': 'research_gate_required',
    }
