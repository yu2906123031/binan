"""Conservative whole-market directional bias for the futures scanner.

This module changes ranking scores only; it never directly vetoes a side or alters
position/protection decisions. It also applies a small economics-aware ranking
adjustment after the market-direction tilt so expensive/extended candidates do
not outrank cleaner setups merely because their raw momentum score is high.
"""
from __future__ import annotations

import math
import statistics
from typing import Any, Dict, Sequence

from strategy_edge import estimate_realizable_reward_r


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


def _directional_conviction(changes: Sequence[float]) -> float:
    """Measure directional drift relative to the cross-sectional move size.

    The signed median captures the typical direction while the mean absolute
    move keeps a few large counter-moves visible. This is intentionally more
    conservative than dividing by median absolute change: a market with many
    tiny winners and a few violent losers should not be treated as a clean
    bullish impulse merely because advancing breadth is above 50%.
    """
    if not changes:
        return 0.0
    median_change = statistics.median(changes)
    mean_abs_change = sum(abs(change) for change in changes) / len(changes)
    if mean_abs_change <= 1e-12:
        return 0.0
    return min(abs(median_change) / mean_abs_change, 1.0)


def compute_market_direction_bias(
    tickers: Sequence[Dict[str, Any]] | None,
    regime: Dict[str, Any] | None,
    *,
    min_sample_size: int = 5,
    min_quote_volume: float = 100_000.0,
    min_directional_conviction: float = 0.20,
) -> Dict[str, Any]:
    """Combine robust USDT breadth with the existing BTC/SOL regime.

    Direction is confirmed by four independent views:
    1. raw symbol breadth, so the move is genuinely broad;
    2. liquidity-weighted breadth, so illiquid noise cannot dominate;
    3. median 24h change, so a few extreme movers cannot flip the result;
    4. directional conviction, so tiny median drift in a choppy market does not
       boost breakout candidates.

    Sparse, stale, conflicted, low-liquidity, or low-conviction inputs safely
    remain NEUTRAL.
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
    directional_conviction = _directional_conviction(changes)

    weights = _capped_liquidity_weights(rows)
    total_weight = sum(weights)
    if total_weight > 0:
        weighted_advancing = sum(weight for (change, _), weight in zip(rows, weights) if change > 0)
        weighted_breadth_ratio = weighted_advancing / total_weight
    else:
        weighted_breadth_ratio = breadth_ratio

    robust_breadth_ratio = (0.65 * weighted_breadth_ratio) + (0.35 * breadth_ratio)
    breadth_signal = (robust_breadth_ratio - 0.5) * 2.0

    structural_label = str((regime or {}).get('structural_label') or (regime or {}).get('label') or 'RANGE').upper()
    regime_signal = 1.0 if structural_label in {'BULL_TREND', 'EUPHORIA'} else -1.0 if structural_label in {'BEAR_TREND', 'PANIC'} else 0.0
    combined = 0.7 * breadth_signal + 0.3 * regime_signal

    bias = 'NEUTRAL'
    enough_data = sample_size >= max(int(min_sample_size), 1)
    conviction_floor = max(min(float(min_directional_conviction or 0.0), 1.0), 0.0)
    conviction_confirmed = directional_conviction >= conviction_floor
    long_breadth_confirmed = breadth_ratio >= 0.55 and weighted_breadth_ratio >= 0.60 and median_change_pct > 0
    short_breadth_confirmed = breadth_ratio <= 0.45 and weighted_breadth_ratio <= 0.40 and median_change_pct < 0
    if enough_data and conviction_confirmed:
        if long_breadth_confirmed and regime_signal >= 0 and combined >= 0.25:
            bias = 'LONG'
        elif short_breadth_confirmed and regime_signal <= 0 and combined <= -0.25:
            bias = 'SHORT'

    strength = min(abs(combined) * directional_conviction, 1.0) if bias != 'NEUTRAL' else 0.0
    return {
        'bias': bias,
        'strength': round(strength, 4),
        'breadth_ratio': round(breadth_ratio, 4),
        'weighted_breadth_ratio': round(weighted_breadth_ratio, 4),
        'robust_breadth_ratio': round(robust_breadth_ratio, 4),
        'median_change_pct': round(median_change_pct, 4),
        'directional_conviction': round(directional_conviction, 4),
        'advancing': advancing,
        'declining': declining,
        'sample_size': sample_size,
        'ignored_low_liquidity': ignored_low_liquidity,
        'regime_label': structural_label,
        'combined_signal': round(combined, 4),
    }


def _apply_realizable_edge_adjustment(candidate: Any) -> float:
    """Feed realizable reward/cost quality into ranking and downstream edge gates.

    Candidate builder already provides a base expected edge using configured TP R.
    Here we conservatively discount/boost that edge using the actual setup state,
    confirmation stage, extension, volume, observed depth and slippage. Candidates
    lacking the explicit edge contract keep a neutral multiplier for compatibility.
    """
    if not hasattr(candidate, 'expected_edge') or not hasattr(candidate, 'stop_distance_pct'):
        candidate.realizable_edge_score_multiplier = 1.0
        return 1.0

    stop_distance_pct = max(float(getattr(candidate, 'stop_distance_pct', 0.0) or 0.0), 0.0)
    base_expected_edge = max(float(getattr(candidate, 'expected_edge', 0.0) or 0.0), 0.0)
    if stop_distance_pct <= 1e-12 or base_expected_edge <= 0:
        candidate.realizable_edge_score_multiplier = 1.0
        return 1.0

    base_reward_r = base_expected_edge / stop_distance_pct
    flags = dict(getattr(candidate, 'trigger_confirmation_flags', {}) or {})
    trigger_type = str(getattr(candidate, 'trigger_type', 'breakout') or 'breakout').lower()
    breakout_quality = None
    if trigger_type == 'breakout':
        breakout_quality = {
            'quality_pass': bool(flags.get('breakout_quality_pass', True)),
            'hard_reject': bool(flags.get('breakout_quality_hard_reject', False)),
            'confirmation_count': 1 if bool(flags.get('breakout_flow_confirmed', False)) else 0,
        }

    top_depth = max(float(getattr(candidate, 'top_depth_usdt', 0.0) or 0.0), 0.0)
    available_depth = max(float(getattr(candidate, 'available_depth_usdt', 0.0) or 0.0), 0.0)
    has_depth = top_depth > 0 or available_depth > 0
    edge_model = estimate_realizable_reward_r(
        base_reward_r=base_reward_r,
        trigger_type=trigger_type,
        state=str(getattr(candidate, 'state', 'watch') or 'watch'),
        candidate_stage=str(getattr(candidate, 'candidate_stage', '') or ''),
        setup_ready=bool(getattr(candidate, 'setup_ready', True)),
        trigger_fired=bool(getattr(candidate, 'trigger_fired', True)),
        overextension_flag=bool(getattr(candidate, 'overextension_flag', False)),
        breakout_quality=breakout_quality,
        volume_multiple=float(getattr(candidate, 'volume_multiple', 0.0) or 0.0),
        min_volume_multiple=1.0,
        stop_distance_pct=stop_distance_pct,
        expected_slippage_pct=max(float(getattr(candidate, 'expected_slippage_pct', 0.0) or 0.0), 0.0),
        book_depth_fill_ratio=float(getattr(candidate, 'book_depth_fill_ratio', 0.0) or 0.0),
        has_orderbook_depth=has_depth,
    )
    realizable_reward_r = float(edge_model['reward_r'])
    realizable_edge = round(stop_distance_pct * realizable_reward_r, 4)
    candidate.base_expected_edge = round(base_expected_edge, 4)
    candidate.realizable_reward_r = realizable_reward_r
    candidate.realizable_expected_edge = realizable_edge
    candidate.realizable_edge_model = edge_model
    candidate.expected_edge = realizable_edge

    total_cost_floor = (
        max(float(getattr(candidate, 'expected_total_fee_pct', 0.0) or 0.0), 0.0)
        + max(float(getattr(candidate, 'execution_slippage_buffer_pct', 0.0) or 0.0), 0.0)
        + max(float(getattr(candidate, 'min_profit_buffer_pct', 0.0) or 0.0), 0.0)
    )
    edge_margin = realizable_edge - total_cost_floor
    edge_margin_r = edge_margin / stop_distance_pct if stop_distance_pct > 0 else 0.0
    if edge_margin <= 0:
        score_multiplier = 0.82
    elif edge_margin_r < 0.20:
        score_multiplier = 0.92
    elif edge_margin_r >= 0.90:
        score_multiplier = 1.06
    elif edge_margin_r >= 0.55:
        score_multiplier = 1.03
    else:
        score_multiplier = 1.0
    candidate.realizable_edge_margin_pct = round(edge_margin, 4)
    candidate.realizable_edge_margin_r = round(edge_margin_r, 4)
    candidate.realizable_edge_score_multiplier = round(score_multiplier, 4)
    return score_multiplier


def apply_market_direction_bias(candidate: Any, payload: Dict[str, Any] | None, *, max_score_tilt: float = 0.04) -> Any:
    """Apply soft market tilt, then a bounded economics-aware ranking adjustment."""
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
    candidate.market_directional_conviction = float(data.get('directional_conviction', 0.0) or 0.0)
    candidate.reasons = list(getattr(candidate, 'reasons', []) or [])
    candidate.reasons.append(
        f"market_direction_bias={bias}:strength={strength:.2f}:breadth={candidate.market_breadth_ratio:.2f}:"
        f"weighted={candidate.market_weighted_breadth_ratio:.2f}:median={candidate.market_median_change_pct:.2f}:"
        f"conviction={candidate.market_directional_conviction:.2f}:multiplier={multiplier:.4f}"
    )

    edge_multiplier = _apply_realizable_edge_adjustment(candidate)
    if edge_multiplier != 1.0:
        candidate.score = round(float(candidate.score or 0.0) * edge_multiplier, 4)
    if hasattr(candidate, 'realizable_reward_r'):
        candidate.reasons.append(
            f"realizable_edge=reward_r={candidate.realizable_reward_r:.3f}:"
            f"edge_pct={candidate.realizable_expected_edge:.3f}:"
            f"margin_r={candidate.realizable_edge_margin_r:.3f}:"
            f"score_multiplier={candidate.realizable_edge_score_multiplier:.3f}"
        )
    return candidate
