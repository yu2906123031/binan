from __future__ import annotations

from typing import Any


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def compute_edge_size_multiplier(candidate: Any) -> dict[str, Any]:
    """Return a bounded risk multiplier; never increases leverage limits."""
    edge_r = _num(
        getattr(candidate, 'realizable_edge_margin_r', None),
        _num(getattr(candidate, 'expected_edge_r', 0.0)),
    )
    liquidity = str(getattr(candidate, 'execution_liquidity_grade', '') or getattr(candidate, 'liquidity_grade', '') or '').upper()
    fill_ratio = max(0.0, min(_num(getattr(candidate, 'book_depth_fill_ratio', 1.0), 1.0), 1.0))
    stability = max(0.0, min(_num(getattr(candidate, 'relative_selection_percentile', 0.5), 0.5), 1.0))
    slippage_bps = max(_num(getattr(candidate, 'hierarchical_cost_estimate_bps', 0.0)), 0.0)

    if edge_r <= 0:
        base = 0.0
    elif edge_r < 0.20:
        base = 0.50
    elif edge_r < 0.55:
        base = 0.75
    elif edge_r < 0.90:
        base = 1.00
    else:
        base = 1.10

    liquidity_penalty = 1.0
    if liquidity in {'C', 'D', 'E', 'F'}:
        liquidity_penalty = 0.70
    elif liquidity in {'B', 'B-'}:
        liquidity_penalty = 0.85
    if fill_ratio < 0.65:
        liquidity_penalty = min(liquidity_penalty, 0.70)
    elif fill_ratio < 0.85:
        liquidity_penalty = min(liquidity_penalty, 0.88)
    if slippage_bps >= 20:
        liquidity_penalty = min(liquidity_penalty, 0.65)
    elif slippage_bps >= 10:
        liquidity_penalty = min(liquidity_penalty, 0.85)

    confidence = 1.0
    if stability < 0.50:
        confidence = 0.90
    elif stability >= 0.80:
        confidence = 1.03

    multiplier = max(0.0, min(base * liquidity_penalty * confidence, 1.10))
    return {
        'multiplier': round(multiplier, 4),
        'edge_r': round(edge_r, 4),
        'liquidity_penalty': round(liquidity_penalty, 4),
        'confidence_multiplier': round(confidence, 4),
        'max_multiplier': 1.10,
    }


def apply_edge_size_annotation(candidate: Any) -> dict[str, Any]:
    result = compute_edge_size_multiplier(candidate)
    candidate.edge_size_multiplier = result['multiplier']
    planned = _num(
        getattr(candidate, 'planned_notional_usdt', 0.0)
        or getattr(candidate, 'planned_notional', 0.0)
        or getattr(candidate, 'notional', 0.0)
    )
    if planned > 0:
        candidate.edge_sized_notional_usdt = round(planned * result['multiplier'], 8)
    return result
