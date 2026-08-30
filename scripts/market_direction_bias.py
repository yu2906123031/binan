"""Conservative whole-market directional bias for the futures scanner.

This module intentionally changes ranking scores only; it never vetoes a side or
alters position/risk/protection decisions.
"""
from __future__ import annotations

import math
import statistics
from typing import Any, Dict, Sequence


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _capped_liquidity_weights(rows: Sequence[tuple[float, float]]) -> list[float]:
    """Return robust liquidity weights without letting one mega-cap dominate.

    Square-root volume compresses the enormous spread between large and small
    contracts, then a median-based cap prevents BTC/ETH-sized symbols from
    becoming the market breadth signal by themselves.
    """
    if not rows:
        return []
    raw = [math.sqrt(max(volume, 0.0)) for _, volume in rows]
    positive = [weight for weight in raw if weight > 0]
    if not positive:
        return [1.0] * len(rows)
    median_weight = statistics.median(positive)
    cap = max(median_weight * 3.0, 1.0)
    return [min(weight, cap) if weight > 0 else 0.0 for weight in raw]


def compute_market_direction_bias(
    tickers: Sequence[Dict[str, Any]] | None,
    regime: Dict[str, Any] | None,
    *,
    min_sample_size: int = 5,
    min_quote_volume: float = 100_000.0,
) -> Dict[str, Any]:
    """Combine robust USDT breadth with the existing BTC/SOL regime.

    Direction is confirmed by three independent views:
    1. raw symbol breadth, so the move is genuinely broad;
    2. liquidity-weighted breadth, so illiquid noise cannot dominate;
    3. median 24h change, so a few extreme movers cannot flip the result.

    Sparse, stale, conflicted, or low-liquidity inputs safely remain NEUTRAL.
    """
    rows: list[tuple[float, float]] = []
    ignored_low_liquidity = 0
    minimum_volume = max(float(min_quote_volume or 0.0), 0.0)
    for row in tickers or []:
        if not isinstance(row, dict) or not str(row.get('symbol', '')).endswith('USDT'):
            continue
        change = _number(row.get('priceChangePercent'))
        volume = _number(row.get('quoteVolume'))
        if change is None:
            continue
        if volume is None:
            volume = 0.0
        if minimum_volume > 0 and volume < minimum_volume:
            ignored_low_liquidity += 1
            continue
        rows.append((change, max(volume, 0.0)))

    changes = [change for change, _ in rows]
    sample_size = len(changes)
    advancing = sum(change > 0 for change in changes)
    declining = sum(change < 0 for change in changes)
    breadth_ratio = advancing / sample_size if sample_size else 0.5
    median_change_pct = statistics.median(changes) if changes else 0.0

    weights = _capped_liquidity_weights(rows)
    total_weight = sum(weights)
    if total_weight > 0:
        weighted_advancing = sum(weight for (change, _), weight in zip(rows, weights) if change > 0)
        weighted_breadth_ratio = weighted_advancing / total_weight
    else:
        weighted_breadth_ratio = breadth_ratio

    # Blend robust weighted breadth with raw breadth. The weighted view gets a
    # little more influence because it rejects micro-cap noise, but raw breadth
    # still prevents a handful of liquid names from defining the whole market.
    robust_breadth_ratio = (0.65 * weighted_breadth_ratio) + (0.35 * breadth_ratio)
    breadth_signal = (robust_breadth_ratio - 0.5) * 2.0

    structural_label = str((regime or {}).get('structural_label') or (regime or {}).get('label') or 'RANGE').upper()
    regime_signal = 1.0 if structural_label in {'BULL_TREND', 'EUPHORIA'} else -1.0 if structural_label in {'BEAR_TREND', 'PANIC'} else 0.0
    combined = 0.7 * breadth_signal + 0.3 * regime_signal

    bias = 'NEUTRAL'
    enough_data = sample_size >= max(int(min_sample_size), 1)
    long_breadth_confirmed = breadth_ratio >= 0.55 and weighted_breadth_ratio >= 0.60 and median_change_pct > 0
    short_breadth_confirmed = breadth_ratio <= 0.45 and weighted_breadth_ratio <= 0.40 and median_change_pct < 0
    if enough_data:
        if long_breadth_confirmed and regime_signal >= 0 and combined >= 0.25:
            bias = 'LONG'
        elif short_breadth_confirmed and regime_signal <= 0 and combined <= -0.25:
            bias = 'SHORT'

    strength = min(abs(combined), 1.0) if bias != 'NEUTRAL' else 0.0
    return {
        'bias': bias,
        'strength': round(strength, 4),
        'breadth_ratio': round(breadth_ratio, 4),
        'weighted_breadth_ratio': round(weighted_breadth_ratio, 4),
        'robust_breadth_ratio': round(robust_breadth_ratio, 4),
        'median_change_pct': round(median_change_pct, 4),
        'advancing': advancing,
        'declining': declining,
        'sample_size': sample_size,
        'ignored_low_liquidity': ignored_low_liquidity,
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
    candidate.market_weighted_breadth_ratio = float(data.get('weighted_breadth_ratio', candidate.market_breadth_ratio) or candidate.market_breadth_ratio)
    candidate.market_median_change_pct = float(data.get('median_change_pct', 0.0) or 0.0)
    candidate.reasons = list(getattr(candidate, 'reasons', []) or [])
    candidate.reasons.append(
        f"market_direction_bias={bias}:strength={strength:.2f}:breadth={candidate.market_breadth_ratio:.2f}:"
        f"weighted={candidate.market_weighted_breadth_ratio:.2f}:median={candidate.market_median_change_pct:.2f}:multiplier={multiplier:.4f}"
    )
    return candidate
