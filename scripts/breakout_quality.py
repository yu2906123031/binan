"""Breakout quality checks used before a momentum candidate may fire.

The filter is deliberately conservative about *contradictory* evidence, while
remaining tolerant of missing optional microstructure feeds. Price action and
volume must be credible on their own; OI/CVD/taker flow can confirm the move or
veto an explicit divergence.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


def _number(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def evaluate_breakout_quality(
    *,
    side: str,
    last_price: float,
    breakout_level: float,
    current_open: float,
    current_high: float,
    current_low: float,
    volume_multiple: float,
    min_volume_multiple: float,
    oi_change_pct_5m: Any = None,
    cvd_delta: Any = None,
    cvd_zscore: Any = None,
    taker_buy_ratio: Any = None,
    min_breakout_distance_pct: float = 0.08,
    max_rejection_wick_ratio: float = 0.60,
    min_close_location: float = 0.55,
    hard_oi_contradiction_pct: float = 0.35,
) -> Dict[str, Any]:
    """Return quality/confirmation flags for an actual breakout candle."""
    direction = str(side or '').lower()
    is_long = direction == 'long'
    if direction not in {'long', 'short'}:
        return {
            'quality_pass': False,
            'hard_reject': True,
            'confirmation_count': 0,
            'flags': {'invalid_side': True},
            'reasons': ['invalid_breakout_side'],
        }

    last = max(float(last_price or 0.0), 0.0)
    level = max(float(breakout_level or 0.0), 0.0)
    if last <= 0 or level <= 0:
        return {
            'quality_pass': False,
            'hard_reject': True,
            'confirmation_count': 0,
            'flags': {'invalid_price': True},
            'reasons': ['invalid_breakout_price'],
        }

    breakout_distance_pct = ((last / level) - 1.0) * 100.0 if is_long else ((level / last) - 1.0) * 100.0
    distance_ok = breakout_distance_pct >= max(float(min_breakout_distance_pct or 0.0), 0.0)

    bar_open = float(current_open or last)
    bar_high = max(float(current_high or last), last, bar_open)
    bar_low = min(float(current_low or last), last, bar_open)
    bar_range = max(bar_high - bar_low, 0.0)
    if bar_range <= 1e-12:
        close_location = 0.5
        rejection_wick_ratio = 1.0
    elif is_long:
        close_location = (last - bar_low) / bar_range
        rejection_wick_ratio = max(bar_high - max(last, bar_open), 0.0) / bar_range
    else:
        close_location = (bar_high - last) / bar_range
        rejection_wick_ratio = max(min(last, bar_open) - bar_low, 0.0) / bar_range

    close_ok = close_location >= max(min(float(min_close_location or 0.0), 1.0), 0.0)
    wick_ok = rejection_wick_ratio <= max(min(float(max_rejection_wick_ratio or 0.0), 1.0), 0.0)
    required_volume = max(float(min_volume_multiple or 0.0), 1.0)
    volume_ok = float(volume_multiple or 0.0) >= required_volume

    oi = _number(oi_change_pct_5m)
    cvd = _number(cvd_delta)
    cvd_z = _number(cvd_zscore)
    taker = _number(taker_buy_ratio)
    directional_oi = oi is not None and oi > 0.0
    directional_cvd = cvd is not None and (cvd > 0.0 if is_long else cvd < 0.0)
    directional_cvd_z = cvd_z is not None and (cvd_z > 0.25 if is_long else cvd_z < -0.25)
    directional_taker = taker is not None and (taker >= 0.52 if is_long else taker <= 0.48)
    flow_confirmation_count = sum((directional_oi, directional_cvd, directional_cvd_z, directional_taker))

    oi_contradiction = oi is not None and oi <= -abs(float(hard_oi_contradiction_pct or 0.0))
    cvd_contradiction = (
        (cvd is not None and (cvd < 0.0 if is_long else cvd > 0.0))
        or (cvd_z is not None and (cvd_z <= -0.50 if is_long else cvd_z >= 0.50))
    )
    hard_reject = bool(oi_contradiction and cvd_contradiction)
    quality_pass = bool(distance_ok and close_ok and wick_ok and volume_ok and not hard_reject)

    flags = {
        'breakout_distance_ok': distance_ok,
        'breakout_close_ok': close_ok,
        'breakout_wick_ok': wick_ok,
        'breakout_volume_ok': volume_ok,
        'breakout_flow_confirmed': flow_confirmation_count > 0,
        'breakout_flow_confirmation_count': flow_confirmation_count,
        'breakout_min_volume_multiple': required_volume,
        'breakout_oi_contradiction': oi_contradiction,
        'breakout_cvd_contradiction': cvd_contradiction,
    }
    reasons = [
        f'breakout_distance_pct={breakout_distance_pct:.3f}',
        f'breakout_close_location={close_location:.3f}',
        f'breakout_rejection_wick_ratio={rejection_wick_ratio:.3f}',
        f'breakout_volume_multiple={float(volume_multiple or 0.0):.2f}',
        f'breakout_flow_confirmation_count={flow_confirmation_count}',
    ]
    if hard_reject:
        reasons.append('breakout_flow_divergence_veto')
    elif quality_pass:
        reasons.append('breakout_quality_confirmed')
    else:
        reasons.append('breakout_quality_insufficient')

    return {
        'quality_pass': quality_pass,
        'hard_reject': hard_reject,
        'confirmation_count': flow_confirmation_count,
        'breakout_distance_pct': round(breakout_distance_pct, 4),
        'close_location': round(close_location, 4),
        'rejection_wick_ratio': round(rejection_wick_ratio, 4),
        'flags': flags,
        'reasons': reasons,
    }
