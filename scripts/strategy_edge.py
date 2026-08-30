"""Estimate a conservative, realizable reward multiple for strategy candidates."""
from __future__ import annotations

from typing import Any, Dict, Optional


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(float(value), high))


def estimate_realizable_reward_r(
    *,
    base_reward_r: float = 1.0,
    trigger_type: str = 'breakout',
    state: str = 'watch',
    candidate_stage: str = '',
    setup_ready: bool = True,
    trigger_fired: bool = True,
    overextension_flag: bool = False,
    breakout_quality: Optional[Dict[str, Any]] = None,
    volume_multiple: float = 1.0,
    min_volume_multiple: float = 1.0,
    stop_distance_pct: float = 0.0,
    expected_slippage_pct: float = 0.0,
    book_depth_fill_ratio: Optional[float] = None,
    has_orderbook_depth: bool = False,
) -> Dict[str, Any]:
    """Return a quality-adjusted reward multiple and diagnostics.

    The model deliberately estimates *realizable* reward instead of assuming every
    candidate can achieve the same configured R multiple. Missing optional orderbook
    data is neutral; observed poor depth/slippage is penalized. Unconfirmed/watch
    candidates are also discounted so raw heat cannot outrank fully fired setups.
    """
    base = _clamp(float(base_reward_r or 1.0), 0.25, 4.0)
    trigger = str(trigger_type or 'breakout').strip().lower()
    state_name = str(state or 'watch').strip().lower()
    stage_name = str(candidate_stage or '').strip().lower()

    trigger_multiplier = 1.08 if trigger == 'pullback' else 1.0
    state_multiplier = {
        'launch': 1.15,
        'squeeze': 1.10,
        'watch': 0.88,
        'accumulation': 1.02,
        'overheated': 0.68,
        'momentum_extension': 0.72,
        'distribution': 0.58,
        'none': 0.82,
    }.get(state_name, 0.90)
    extension_multiplier = 0.70 if bool(overextension_flag) else 1.0

    if not setup_ready:
        stage_multiplier = 0.62
    elif not trigger_fired:
        stage_multiplier = 0.78 if stage_name in {'pre_trigger_watch', 'watch_candidate', ''} else 0.82
    elif stage_name in {'pre_trigger_watch', 'watch_candidate'}:
        stage_multiplier = 0.90
    else:
        stage_multiplier = 1.0

    required_volume = max(float(min_volume_multiple or 0.0), 1.0)
    volume_ratio = max(float(volume_multiple or 0.0), 0.0) / required_volume
    if volume_ratio >= 1.8:
        volume_multiplier = 1.08
    elif volume_ratio >= 1.2:
        volume_multiplier = 1.04
    elif volume_ratio >= 1.0:
        volume_multiplier = 1.0
    elif volume_ratio >= 0.8:
        volume_multiplier = 0.88
    else:
        volume_multiplier = 0.75

    quality = dict(breakout_quality or {})
    flow_count = max(int(quality.get('confirmation_count', 0) or 0), 0)
    if trigger == 'breakout' and quality:
        if bool(quality.get('hard_reject')):
            breakout_multiplier = 0.45
        elif not bool(quality.get('quality_pass', False)):
            breakout_multiplier = 0.72
        else:
            breakout_multiplier = min(1.0 + 0.025 * flow_count, 1.10)
    else:
        breakout_multiplier = 1.0

    if has_orderbook_depth and book_depth_fill_ratio is not None:
        fill_ratio = _clamp(float(book_depth_fill_ratio or 0.0), 0.0, 1.0)
        if fill_ratio >= 0.90:
            depth_multiplier = 1.04
        elif fill_ratio >= 0.75:
            depth_multiplier = 1.0
        elif fill_ratio >= 0.60:
            depth_multiplier = 0.88
        else:
            depth_multiplier = 0.68
    else:
        fill_ratio = None
        depth_multiplier = 1.0

    stop_pct = max(float(stop_distance_pct or 0.0), 0.0)
    slippage_pct = max(float(expected_slippage_pct or 0.0), 0.0)
    if stop_pct > 1e-12:
        slippage_r = slippage_pct / stop_pct
        slippage_multiplier = _clamp(1.0 - 0.55 * slippage_r, 0.65, 1.0)
    else:
        slippage_r = 0.0
        slippage_multiplier = 1.0

    raw = base
    for multiplier in (
        trigger_multiplier,
        state_multiplier,
        stage_multiplier,
        extension_multiplier,
        volume_multiplier,
        breakout_multiplier,
        depth_multiplier,
        slippage_multiplier,
    ):
        raw *= multiplier
    reward_r = _clamp(raw, 0.35, 3.0)

    return {
        'reward_r': round(reward_r, 4),
        'base_reward_r': round(base, 4),
        'trigger_multiplier': round(trigger_multiplier, 4),
        'state_multiplier': round(state_multiplier, 4),
        'stage_multiplier': round(stage_multiplier, 4),
        'extension_multiplier': round(extension_multiplier, 4),
        'volume_multiplier': round(volume_multiplier, 4),
        'breakout_multiplier': round(breakout_multiplier, 4),
        'depth_multiplier': round(depth_multiplier, 4),
        'slippage_multiplier': round(slippage_multiplier, 4),
        'slippage_r': round(slippage_r, 4),
        'flow_confirmation_count': flow_count,
        'candidate_stage': stage_name,
        'setup_ready': bool(setup_ready),
        'trigger_fired': bool(trigger_fired),
        'has_orderbook_depth': bool(has_orderbook_depth),
        'book_depth_fill_ratio': None if fill_ratio is None else round(fill_ratio, 4),
    }
