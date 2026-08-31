from __future__ import annotations

from typing import Any


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _normalized_side(value: Any) -> str:
    text = str(value or '').strip().upper()
    return 'SHORT' if text in {'SHORT', 'SELL'} else 'LONG'


def _trend_alignment(candidate: Any) -> float:
    side = _normalized_side(getattr(candidate, 'side', getattr(candidate, 'position_side', 'LONG')))
    bias = str(getattr(candidate, 'higher_timeframe_bias', '') or '').strip().upper()
    long_tokens = {'LONG', 'BULL', 'BULLISH', 'UP', 'UPTREND', 'BULL_TREND'}
    short_tokens = {'SHORT', 'BEAR', 'BEARISH', 'DOWN', 'DOWNTREND', 'BEAR_TREND'}
    if bias in long_tokens:
        return 1.0 if side == 'LONG' else -1.0
    if bias in short_tokens:
        return 1.0 if side == 'SHORT' else -1.0

    summary = getattr(candidate, 'higher_tf_summary', None)
    if not isinstance(summary, dict):
        return 0.0
    votes = 0
    known = 0
    for timeframe in ('1h', '4h'):
        payload = summary.get(timeframe)
        if not isinstance(payload, dict):
            continue
        text = str(
            payload.get('bias')
            or payload.get('trend')
            or payload.get('direction')
            or payload.get('label')
            or ''
        ).strip().upper()
        if text in long_tokens:
            known += 1
            votes += 1 if side == 'LONG' else -1
        elif text in short_tokens:
            known += 1
            votes += 1 if side == 'SHORT' else -1
    if known <= 0:
        return 0.0
    return max(-1.0, min(votes / known, 1.0))


def compute_selection_quality_multiplier(candidate: Any) -> float:
    """Return a bounded soft-ranking multiplier for symbol selection quality.

    The multiplier deliberately remains soft: execution/risk hard vetoes stay in
    their existing layers.  Positive adjustments require several independent
    quality signals, while obvious illiquidity or stale/chasing setups are
    deweighted so raw momentum cannot dominate the shortlist by itself.
    """
    multiplier = 1.0

    tradeability = max(0.0, min(_number(getattr(candidate, 'tradeability_score', 0.0)), 1.0))
    if tradeability > 0:
        if tradeability >= 0.85:
            multiplier *= 1.03
        elif tradeability < 0.50:
            multiplier *= 0.86
        elif tradeability < 0.65:
            multiplier *= 0.94

    grade = str(getattr(candidate, 'liquidity_grade', '') or '').strip().upper()
    fill_ratio = max(0.0, min(_number(getattr(candidate, 'book_depth_fill_ratio', 0.0)), 1.0))
    spread_bps = max(_number(getattr(candidate, 'spread_bps', 0.0)), 0.0)
    impact_pct = max(_number(getattr(candidate, 'estimated_impact_pct', 0.0)), 0.0)
    has_liquidity_data = bool(grade) or fill_ratio > 0 or spread_bps > 0 or impact_pct > 0
    if has_liquidity_data:
        weak_liquidity = (
            grade in {'C', 'D', 'E', 'F'}
            or (fill_ratio > 0 and fill_ratio < 0.65)
            or spread_bps > 15.0
            or impact_pct > 0.20
        )
        strong_liquidity = (
            grade in {'A', 'A+', 'S'}
            and fill_ratio >= 0.90
            and (spread_bps <= 5.0 or spread_bps <= 0)
            and (impact_pct <= 0.08 or impact_pct <= 0)
        )
        if weak_liquidity:
            multiplier *= 0.84
        elif strong_liquidity:
            multiplier *= 1.04

    alignment = _trend_alignment(candidate)
    if alignment >= 0.75:
        multiplier *= 1.04
    elif alignment <= -0.75:
        multiplier *= 0.88
    candidate.selection_htf_alignment = round(alignment, 4)

    volume_multiple = max(_number(getattr(candidate, 'volume_multiple', 0.0)), 0.0)
    if volume_multiple > 0:
        if volume_multiple >= 1.5:
            multiplier *= 1.025
        elif volume_multiple < 0.85:
            multiplier *= 0.92

    side = _normalized_side(getattr(candidate, 'side', getattr(candidate, 'position_side', 'LONG')))
    recent_5m = _number(getattr(candidate, 'recent_5m_change_pct', 0.0))
    directional_5m = recent_5m if side == 'LONG' else -recent_5m
    acceleration = max(_number(getattr(candidate, 'acceleration_ratio_5m_vs_15m', 0.0)), 0.0)
    move_24h = abs(_number(getattr(candidate, 'price_change_pct_24h', 0.0)))
    stale_extreme_move = move_24h >= 20.0 and directional_5m <= 0.10 and acceleration < 1.0
    if stale_extreme_move:
        multiplier *= 0.88

    if bool(getattr(candidate, 'overextension_flag', False)):
        multiplier *= 0.94

    multiplier = max(0.70, min(multiplier, 1.12))
    return round(multiplier, 4)


def apply_selection_quality(candidate: Any) -> Any:
    multiplier = compute_selection_quality_multiplier(candidate)
    candidate.selection_quality_multiplier = multiplier
    candidate.selection_quality_score = round(float(getattr(candidate, 'score', 0.0) or 0.0) * multiplier, 4)
    candidate.score = candidate.selection_quality_score

    reasons = [
        reason for reason in list(getattr(candidate, 'reasons', []) or [])
        if not str(reason).startswith('selection_quality=')
    ]
    reasons.append(
        'selection_quality='
        f'multiplier={multiplier:.4f}:'
        f'tradeability={_number(getattr(candidate, "tradeability_score", 0.0)):.3f}:'
        f'liquidity={str(getattr(candidate, "liquidity_grade", "") or "-")}:'
        f'htf_alignment={getattr(candidate, "selection_htf_alignment", 0.0):.2f}'
    )
    candidate.reasons = reasons
    return candidate


def install_selection_quality_hook(strategy_module: Any) -> None:
    original = getattr(strategy_module, 'apply_market_direction_bias', None)
    if not callable(original) or getattr(original, '_selection_quality_hook', False):
        return

    def apply_market_direction_bias_with_selection_quality(*args: Any, **kwargs: Any):
        candidate = original(*args, **kwargs)
        return apply_selection_quality(candidate)

    apply_market_direction_bias_with_selection_quality._selection_quality_hook = True  # type: ignore[attr-defined]
    strategy_module.apply_market_direction_bias = apply_market_direction_bias_with_selection_quality
