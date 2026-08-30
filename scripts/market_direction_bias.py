"""Conservative whole-market directional bias for the futures scanner.

This module intentionally changes ranking scores only; it never vetoes a side or
alters position/risk/protection decisions.
"""
from __future__ import annotations

from typing import Any, Dict, Sequence


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def compute_market_direction_bias(
    tickers: Sequence[Dict[str, Any]] | None,
    regime: Dict[str, Any] | None,
    *,
    min_sample_size: int = 5,
) -> Dict[str, Any]:
    """Combine equal-weight USDT breadth with the existing BTC/SOL regime.

    A directional result requires meaningful breadth and regime confirmation.
    This makes stale, sparse, or conflicted inputs safely NEUTRAL.
    """
    changes = []
    for row in tickers or []:
        if not isinstance(row, dict) or not str(row.get('symbol', '')).endswith('USDT'):
            continue
        change = _number(row.get('priceChangePercent'))
        if change is not None:
            changes.append(change)
    sample_size = len(changes)
    advancing = sum(change > 0 for change in changes)
    declining = sum(change < 0 for change in changes)
    breadth_ratio = advancing / sample_size if sample_size else 0.5
    breadth_signal = (breadth_ratio - 0.5) * 2.0
    structural_label = str((regime or {}).get('structural_label') or (regime or {}).get('label') or 'RANGE').upper()
    regime_signal = 1.0 if structural_label in {'BULL_TREND', 'EUPHORIA'} else -1.0 if structural_label in {'BEAR_TREND', 'PANIC'} else 0.0
    combined = 0.7 * breadth_signal + 0.3 * regime_signal

    bias = 'NEUTRAL'
    if sample_size >= max(int(min_sample_size), 1):
        if breadth_ratio >= 0.60 and regime_signal >= 0 and combined >= 0.25:
            bias = 'LONG'
        elif breadth_ratio <= 0.40 and regime_signal <= 0 and combined <= -0.25:
            bias = 'SHORT'
    strength = min(abs(combined), 1.0) if bias != 'NEUTRAL' else 0.0
    return {
        'bias': bias,
        'strength': round(strength, 4),
        'breadth_ratio': round(breadth_ratio, 4),
        'advancing': advancing,
        'declining': declining,
        'sample_size': sample_size,
        'regime_label': structural_label,
        'combined_signal': round(combined, 4),
    }


def apply_market_direction_bias(candidate: Any, payload: Dict[str, Any] | None, *, max_score_tilt: float = 0.04) -> Any:
    """Apply at most +/-4% score tilt; never reject the counter-trend side."""
    data = payload or {}
    bias = str(data.get('bias') or 'NEUTRAL').upper()
    side = str(getattr(candidate, 'side', getattr(candidate, 'position_side', 'LONG')) or 'LONG').upper()
    strength = max(0.0, min(float(data.get('strength') or 0.0), 1.0))
    tilt = max(0.0, min(float(max_score_tilt or 0.0), 0.10)) * strength
    multiplier = 1.0 if bias == 'NEUTRAL' else (1.0 + tilt if side == bias else 1.0 - tilt)
    candidate.score = round(float(getattr(candidate, 'score', 0.0) or 0.0) * multiplier, 4)
    candidate.market_direction_bias = bias
    candidate.market_direction_bias_strength = strength
    candidate.market_direction_score_multiplier = round(multiplier, 4)
    candidate.market_breadth_ratio = float(data.get('breadth_ratio', 0.5) or 0.5)
    candidate.reasons = list(getattr(candidate, 'reasons', []) or [])
    candidate.reasons.append(
        f"market_direction_bias={bias}:strength={strength:.2f}:breadth={candidate.market_breadth_ratio:.2f}:multiplier={multiplier:.4f}"
    )
    return candidate
