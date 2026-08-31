from __future__ import annotations

from typing import Any


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def estimate_funding_cost_r(position: dict[str, Any], *, projected_hours: float = 8.0) -> dict[str, Any]:
    side = str(position.get('position_side') or position.get('side') or 'LONG').upper()
    funding_rate = _num(position.get('funding_rate'))
    stop_distance_pct = abs(_num(position.get('stop_distance_pct')))
    hours = max(float(projected_hours), 0.0)
    intervals = hours / 8.0
    # Positive Binance funding: longs pay shorts. Negative funding: shorts pay longs.
    directional_cost_pct = funding_rate * 100.0 * intervals * (1.0 if side in {'LONG', 'BUY'} else -1.0)
    cost_r = max(directional_cost_pct, 0.0) / stop_distance_pct if stop_distance_pct > 1e-12 else 0.0
    credit_r = max(-directional_cost_pct, 0.0) / stop_distance_pct if stop_distance_pct > 1e-12 else 0.0
    return {
        'projected_hours': round(hours, 4),
        'funding_rate': funding_rate,
        'funding_cost_pct': round(max(directional_cost_pct, 0.0), 6),
        'funding_credit_pct': round(max(-directional_cost_pct, 0.0), 6),
        'funding_cost_r': round(cost_r, 6),
        'funding_credit_r': round(credit_r, 6),
    }


def evaluate_funding_hold(position: dict[str, Any], *, projected_hours: float = 8.0) -> dict[str, Any]:
    funding = estimate_funding_cost_r(position, projected_hours=projected_hours)
    remaining_edge_r = _num(position.get('remaining_expected_edge_r'), _num(position.get('expected_reward_r')))
    net_edge_r = remaining_edge_r - float(funding['funding_cost_r'])
    action = 'hold'
    reason = 'funding_cost_acceptable'
    if funding['funding_cost_r'] > 0 and net_edge_r <= 0:
        action = 'reduce'
        reason = 'funding_consumes_remaining_edge'
    return {
        **funding,
        'remaining_edge_r': round(remaining_edge_r, 6),
        'net_edge_after_funding_r': round(net_edge_r, 6),
        'action': action,
        'reason': reason,
    }
