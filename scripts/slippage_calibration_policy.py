from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable

from trade_bucket_analysis import build_trade_bucket_analysis_payload, load_events


DEFAULT_HERMES_HOME = Path(os.path.expanduser(os.getenv('HERMES_HOME', str(Path.home() / '.hermes'))))
DEFAULT_RUNTIME_STATE_DIR = DEFAULT_HERMES_HOME / 'binance-futures-momentum-long' / 'runtime-state'
DEFAULT_MIN_BUCKET_SAMPLES = 20
DEFAULT_MIN_GLOBAL_SAMPLES = 30
DEFAULT_MIN_UNDERPREDICTION_RATE_PCT = 55.0
DEFAULT_MIN_RATIO = 1.05
DEFAULT_MAX_MULTIPLIER = 1.50
DEFAULT_REFRESH_SECONDS = 300.0
DEFAULT_EVENT_LIMIT = 5000
DEFAULT_LOOKBACK_DAYS = 30

_POLICY_CACHE: Dict[str, Any] = {
    'loaded_at_monotonic': 0.0,
    'events_mtime_ns': None,
    'payload': None,
    'last_error': '',
}


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_side(value: Any) -> str:
    side = str(value or '').strip().upper()
    if side in {'BUY', 'LONG'}:
        return 'LONG'
    if side in {'SELL', 'SHORT'}:
        return 'SHORT'
    return side


def _normalize_execution_mode(value: Any) -> str:
    text = str(value or '').strip().lower()
    if text in {'maker', 'maker_only', 'post_only'}:
        return 'maker'
    if text in {'taker', 'market'}:
        return 'taker'
    return text


def _runtime_events_path(runtime_state_dir: Path | None = None) -> Path:
    base = Path(runtime_state_dir) if runtime_state_dir is not None else DEFAULT_RUNTIME_STATE_DIR
    return base / 'events.jsonl'


def load_calibration_payload(
    *,
    runtime_state_dir: Path | None = None,
    refresh_seconds: float = DEFAULT_REFRESH_SECONDS,
    event_limit: int = DEFAULT_EVENT_LIMIT,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    now_monotonic: float | None = None,
) -> Dict[str, Any]:
    events_path = _runtime_events_path(runtime_state_dir)
    current_monotonic = time.monotonic() if now_monotonic is None else float(now_monotonic)
    cached_payload = _POLICY_CACHE.get('payload')
    cache_age = current_monotonic - _number(_POLICY_CACHE.get('loaded_at_monotonic'))
    refresh_window = max(float(refresh_seconds or 0.0), 0.0)
    if isinstance(cached_payload, dict) and cache_age >= 0 and cache_age < refresh_window:
        return cached_payload

    try:
        mtime_ns = events_path.stat().st_mtime_ns
    except OSError:
        mtime_ns = None
    if isinstance(cached_payload, dict) and _POLICY_CACHE.get('events_mtime_ns') == mtime_ns:
        _POLICY_CACHE['loaded_at_monotonic'] = current_monotonic
        return cached_payload

    try:
        if not events_path.exists():
            payload: Dict[str, Any] = {}
        else:
            rows = load_events(events_path, limit=max(int(event_limit or 0), 1))
            payload = build_trade_bucket_analysis_payload(
                rows,
                lookback_days=max(int(lookback_days or 0), 0),
            )
    except Exception as exc:
        _POLICY_CACHE['loaded_at_monotonic'] = current_monotonic
        _POLICY_CACHE['events_mtime_ns'] = mtime_ns
        _POLICY_CACHE['last_error'] = f'{exc.__class__.__name__}: {exc}'
        if isinstance(cached_payload, dict):
            return cached_payload
        _POLICY_CACHE['payload'] = {}
        return {}

    _POLICY_CACHE.update({
        'loaded_at_monotonic': current_monotonic,
        'events_mtime_ns': mtime_ns,
        'payload': payload,
        'last_error': '',
    })
    return payload


def reset_calibration_cache() -> None:
    _POLICY_CACHE.update({
        'loaded_at_monotonic': 0.0,
        'events_mtime_ns': None,
        'payload': None,
        'last_error': '',
    })


def _eligible_multiplier(
    row: Dict[str, Any],
    *,
    min_samples: int,
    min_underprediction_rate_pct: float,
    min_ratio: float,
    max_multiplier: float,
) -> float | None:
    sample_count = int(_number(row.get('sample_count')))
    ratio = _number(row.get('actual_to_predicted_ratio'))
    underprediction_rate = _number(row.get('underprediction_rate_pct'))
    if sample_count < max(int(min_samples), 1):
        return None
    if ratio < max(float(min_ratio), 1.0):
        return None
    if underprediction_rate < max(float(min_underprediction_rate_pct), 50.0):
        return None
    return min(max(ratio, 1.0), max(float(max_multiplier), 1.0))


def _dimension_rows(payload: Dict[str, Any], dimension: str) -> Iterable[Dict[str, Any]]:
    segmented = payload.get('slippage_calibration_by_dimension', {})
    if not isinstance(segmented, dict):
        return []
    rows = segmented.get(dimension, [])
    return rows if isinstance(rows, list) else []


def resolve_slippage_calibration(
    candidate: Any,
    execution_quality: Dict[str, Any] | None = None,
    *,
    payload: Dict[str, Any] | None = None,
    min_bucket_samples: int = DEFAULT_MIN_BUCKET_SAMPLES,
    min_global_samples: int = DEFAULT_MIN_GLOBAL_SAMPLES,
    min_underprediction_rate_pct: float = DEFAULT_MIN_UNDERPREDICTION_RATE_PCT,
    min_ratio: float = DEFAULT_MIN_RATIO,
    max_multiplier: float = DEFAULT_MAX_MULTIPLIER,
) -> Dict[str, Any]:
    data = payload if isinstance(payload, dict) else load_calibration_payload()
    quality = execution_quality if isinstance(execution_quality, dict) else {}
    dimension_values = {
        'side': _normalize_side(getattr(candidate, 'side', getattr(candidate, 'position_side', ''))),
        'maker_or_taker': _normalize_execution_mode(quality.get('maker_or_taker') or quality.get('execution_mode') or getattr(candidate, 'maker_or_taker', '')),
        'liquidity_grade': str(
            quality.get('execution_liquidity_grade')
            or quality.get('liquidity_grade_at_entry')
            or getattr(candidate, 'execution_liquidity_grade', '')
            or getattr(candidate, 'liquidity_grade', '')
            or ''
        ).strip(),
        'market_regime_label': str(
            getattr(candidate, 'market_regime_label', '')
            or getattr(candidate, 'regime_label', '')
            or ''
        ).strip(),
    }
    matched: list[Dict[str, Any]] = []
    for dimension, value in dimension_values.items():
        if not value:
            continue
        normalized_value = value.lower() if dimension == 'maker_or_taker' else value.upper() if dimension == 'side' else value
        for row in _dimension_rows(data, dimension):
            row_value = str(row.get(dimension) or '').strip()
            normalized_row_value = row_value.lower() if dimension == 'maker_or_taker' else row_value.upper() if dimension == 'side' else row_value
            if normalized_row_value != normalized_value:
                continue
            multiplier = _eligible_multiplier(
                row,
                min_samples=min_bucket_samples,
                min_underprediction_rate_pct=min_underprediction_rate_pct,
                min_ratio=min_ratio,
                max_multiplier=max_multiplier,
            )
            if multiplier is not None:
                matched.append({
                    'dimension': dimension,
                    'value': row_value,
                    'sample_count': int(_number(row.get('sample_count'))),
                    'actual_to_predicted_ratio': _number(row.get('actual_to_predicted_ratio')),
                    'underprediction_rate_pct': _number(row.get('underprediction_rate_pct')),
                    'multiplier': multiplier,
                })
            break

    source = 'none'
    multiplier = 1.0
    if matched:
        source = 'segmented'
        multiplier = max(item['multiplier'] for item in matched)
    else:
        global_row = data.get('slippage_calibration', {})
        if isinstance(global_row, dict):
            global_multiplier = _eligible_multiplier(
                global_row,
                min_samples=min_global_samples,
                min_underprediction_rate_pct=min_underprediction_rate_pct,
                min_ratio=min_ratio,
                max_multiplier=max_multiplier,
            )
            if global_multiplier is not None:
                source = 'global'
                multiplier = global_multiplier
                matched.append({
                    'dimension': 'global',
                    'value': 'all',
                    'sample_count': int(_number(global_row.get('sample_count'))),
                    'actual_to_predicted_ratio': _number(global_row.get('actual_to_predicted_ratio')),
                    'underprediction_rate_pct': _number(global_row.get('underprediction_rate_pct')),
                    'multiplier': global_multiplier,
                })

    return {
        'active': multiplier > 1.0,
        'multiplier': round(min(max(multiplier, 1.0), max(float(max_multiplier), 1.0)), 4),
        'source': source,
        'matched': matched,
        'min_bucket_samples': int(min_bucket_samples),
        'min_global_samples': int(min_global_samples),
        'min_underprediction_rate_pct': float(min_underprediction_rate_pct),
        'min_ratio': float(min_ratio),
        'max_multiplier': float(max_multiplier),
    }


def apply_candidate_slippage_calibration(candidate: Any, *, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    calibration = resolve_slippage_calibration(candidate, payload=payload)
    multiplier = _number(calibration.get('multiplier'), 1.0)
    raw_expected_slippage_pct = max(_number(getattr(candidate, 'expected_slippage_pct', 0.0)), 0.0)
    candidate.base_expected_slippage_pct = _number(
        getattr(candidate, 'base_expected_slippage_pct', raw_expected_slippage_pct),
        raw_expected_slippage_pct,
    )
    candidate.expected_slippage_pct = round(candidate.base_expected_slippage_pct * multiplier, 6)
    candidate.slippage_calibration_multiplier = round(multiplier, 4)
    candidate.slippage_calibration_source = str(calibration.get('source') or 'none')
    candidate.slippage_calibration_matches = list(calibration.get('matched') or [])
    candidate.slippage_calibration_active = bool(calibration.get('active'))
    return calibration


def apply_execution_slippage_calibration(
    candidate: Any,
    execution_quality: Dict[str, Any],
    *,
    payload: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    calibrated = dict(execution_quality)
    calibration = resolve_slippage_calibration(candidate, calibrated, payload=payload)
    multiplier = _number(calibration.get('multiplier'), 1.0)
    base_bps = max(_number(calibrated.get('absolute_slippage_bps')), 0.0)
    base_slippage_r = max(_number(calibrated.get('expected_slippage_r')), 0.0)
    base_size_multiplier = max(_number(calibrated.get('size_multiplier'), 1.0), 0.0)
    calibrated['base_absolute_slippage_bps'] = base_bps
    calibrated['base_expected_slippage_r'] = base_slippage_r
    calibrated['base_size_multiplier'] = base_size_multiplier
    calibrated['absolute_slippage_bps'] = round(base_bps * multiplier, 4)
    calibrated['expected_slippage_r'] = round(base_slippage_r * multiplier, 6)
    calibrated['size_multiplier'] = round(base_size_multiplier / multiplier, 6) if multiplier > 1.0 else base_size_multiplier
    calibrated['slippage_calibration_multiplier'] = round(multiplier, 4)
    calibrated['slippage_calibration_source'] = str(calibration.get('source') or 'none')
    calibrated['slippage_calibration_matches'] = list(calibration.get('matched') or [])
    calibrated['slippage_calibration_active'] = bool(calibration.get('active'))
    return calibrated


def install_slippage_calibration_hooks(strategy_module: Any) -> None:
    original_market_bias = getattr(strategy_module, 'apply_market_direction_bias', None)
    if callable(original_market_bias) and not getattr(original_market_bias, '_slippage_calibration_hook', False):
        def market_bias_with_calibration(candidate: Any, payload: Dict[str, Any] | None, *, max_score_tilt: float = 0.04):
            apply_candidate_slippage_calibration(candidate)
            return original_market_bias(candidate, payload, max_score_tilt=max_score_tilt)

        market_bias_with_calibration._slippage_calibration_hook = True  # type: ignore[attr-defined]
        strategy_module.apply_market_direction_bias = market_bias_with_calibration

    original_execution_quality = getattr(strategy_module, 'compute_execution_quality_size_adjustment', None)
    if callable(original_execution_quality) and not getattr(original_execution_quality, '_slippage_calibration_hook', False):
        def execution_quality_with_calibration(candidate: Any):
            quality = original_execution_quality(candidate)
            return apply_execution_slippage_calibration(candidate, quality)

        execution_quality_with_calibration._slippage_calibration_hook = True  # type: ignore[attr-defined]
        strategy_module.compute_execution_quality_size_adjustment = execution_quality_with_calibration
