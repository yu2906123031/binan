from __future__ import annotations

import datetime
import math
from collections import defaultdict
from typing import Any, Iterable

MIN_FINE_SAMPLES = 20
MIN_PARENT_SAMPLES = 30
MIN_GLOBAL_SAMPLES = 40
DEFAULT_QUANTILE = 0.80
MAX_RELAXATION = 0.08


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return float(default)
        number = float(value)
        return number if math.isfinite(number) else float(default)
    except (TypeError, ValueError):
        return float(default)


def _text(value: Any) -> str:
    return str(value or '').strip().upper()


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    q = max(0.0, min(float(q), 1.0))
    if len(ordered) == 1:
        return ordered[0]
    pos = q * (len(ordered) - 1)
    low = int(math.floor(pos))
    high = int(math.ceil(pos))
    if low == high:
        return ordered[low]
    frac = pos - low
    return ordered[low] * (1.0 - frac) + ordered[high] * frac


def _session_bucket(recorded_at: Any) -> str:
    text = str(recorded_at or '').strip()
    if not text:
        return 'UNKNOWN'
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return 'UNKNOWN'
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    hour = parsed.astimezone(datetime.timezone.utc).hour
    if 0 <= hour < 8:
        return 'ASIA'
    if 8 <= hour < 13:
        return 'EUROPE'
    if 13 <= hour < 21:
        return 'US'
    return 'OFF_HOURS'


def _notional_bucket(value: Any) -> str:
    notional = abs(_num(value))
    if notional <= 0:
        return 'UNKNOWN'
    if notional < 100:
        return 'XS'
    if notional < 500:
        return 'S'
    if notional < 2_000:
        return 'M'
    if notional < 10_000:
        return 'L'
    return 'XL'


def _snapshot(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get('entry_prediction_snapshot')
    return value if isinstance(value, dict) else {}


def _slippage_bps(row: dict[str, Any]) -> float | None:
    value = row.get('actual_fill_slippage_bps')
    if value in (None, ''):
        value = _snapshot(row).get('actual_fill_slippage_bps')
    if value in (None, ''):
        return None
    return _num(value)


def _dimension(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    snapshot: dict[str, Any] = _snapshot(row)
    liquidity = _text(
        row.get('liquidity_grade_at_entry')
        or row.get('execution_liquidity_grade')
        or snapshot.get('liquidity_grade_at_entry')
        or snapshot.get('execution_liquidity_grade')
        or row.get('liquidity_grade')
        or snapshot.get('liquidity_grade')
        or 'UNKNOWN'
    )
    regime = _text(
        row.get('market_regime_label')
        or snapshot.get('market_regime_label')
        or row.get('regime_label')
        or snapshot.get('regime_label')
        or 'UNKNOWN'
    )
    order_type = _text(
        row.get('maker_or_taker')
        or row.get('execution_mode')
        or snapshot.get('maker_or_taker')
        or snapshot.get('execution_mode')
        or 'UNKNOWN'
    )
    notional = _notional_bucket(
        row.get('notional_usdt')
        or row.get('planned_notional_usdt')
        or snapshot.get('notional_usdt')
        or snapshot.get('planned_notional_usdt')
    )
    session = _session_bucket(row.get('recorded_at'))
    return liquidity, regime, order_type, notional, session


def build_hierarchical_cost_model(events: Iterable[dict[str, Any]], *, quantile: float = DEFAULT_QUANTILE) -> dict[str, Any]:
    fine: dict[str, list[float]] = defaultdict(list)
    parent: dict[str, list[float]] = defaultdict(list)
    coarse: dict[str, list[float]] = defaultdict(list)
    global_values: list[float] = []
    for row in events:
        if not isinstance(row, dict) or str(row.get('event_type') or '') != 'entry_filled':
            continue
        slippage = _slippage_bps(row)
        if slippage is None:
            continue
        adverse = max(slippage, 0.0)
        liquidity, regime, order_type, notional, session = _dimension(row)
        fine[f'{liquidity}|{regime}|{order_type}|{notional}|{session}'].append(adverse)
        parent[f'{liquidity}|{regime}|{notional}'].append(adverse)
        coarse[f'{liquidity}|{regime}'].append(adverse)
        global_values.append(adverse)

    def summarize(source: dict[str, list[float]]) -> dict[str, dict[str, float | int]]:
        return {
            key: {
                'sample_count': len(values),
                'p50_bps': round(_quantile(values, 0.50), 4),
                'conservative_bps': round(_quantile(values, quantile), 4),
                'stress_bps': round(_quantile(values, 0.95), 4),
            }
            for key, values in source.items()
        }

    return {
        'quantile': quantile,
        'sample_count': len(global_values),
        'global': {
            'sample_count': len(global_values),
            'p50_bps': round(_quantile(global_values, 0.50), 4),
            'conservative_bps': round(_quantile(global_values, quantile), 4),
            'stress_bps': round(_quantile(global_values, 0.95), 4),
        },
        'fine': summarize(fine),
        'parent': summarize(parent),
        'coarse': summarize(coarse),
    }


def resolve_hierarchical_cost(candidate: Any, model: dict[str, Any], *, current_bps: float | None = None) -> dict[str, Any]:
    liquidity = _text(getattr(candidate, 'execution_liquidity_grade', '') or getattr(candidate, 'liquidity_grade', '') or 'UNKNOWN')
    regime = _text(getattr(candidate, 'market_regime_label', '') or getattr(candidate, 'regime_label', '') or 'UNKNOWN')
    order_type = _text(getattr(candidate, 'maker_or_taker', '') or getattr(candidate, 'execution_mode', '') or 'UNKNOWN')
    notional = _notional_bucket(
        getattr(candidate, 'planned_notional_usdt', 0.0)
        or getattr(candidate, 'planned_notional', 0.0)
        or getattr(candidate, 'notional', 0.0)
    )
    session = _session_bucket(getattr(candidate, 'decision_at', None) or getattr(candidate, 'recorded_at', None))
    lookups = [
        ('fine', f'{liquidity}|{regime}|{order_type}|{notional}|{session}', MIN_FINE_SAMPLES),
        ('parent', f'{liquidity}|{regime}|{notional}', MIN_PARENT_SAMPLES),
        ('coarse', f'{liquidity}|{regime}', MIN_PARENT_SAMPLES),
        ('global', '', MIN_GLOBAL_SAMPLES),
    ]
    selected: dict[str, Any] | None = None
    source = 'none'
    key = ''
    for level, lookup_key, minimum in lookups:
        if level == 'global':
            row = model.get('global')
        else:
            rows = model.get(level)
            row = rows.get(lookup_key) if isinstance(rows, dict) else None
        if isinstance(row, dict) and int(row.get('sample_count') or 0) >= minimum:
            selected = row
            source = level
            key = lookup_key
            break
    baseline = max(_num(current_bps if current_bps is not None else getattr(candidate, 'expected_slippage_pct', 0.0) * 100.0), 0.0)
    if selected is None:
        estimate = baseline
        stress = baseline
        samples = 0
    else:
        observed = max(_num(selected.get('conservative_bps')), 0.0)
        stress = max(_num(selected.get('stress_bps')), observed)
        samples = int(selected.get('sample_count') or 0)
        floor = baseline * (1.0 - MAX_RELAXATION)
        estimate = max(observed, floor) if baseline > 0 else observed
    return {
        'source': source,
        'key': key,
        'sample_count': samples,
        'estimated_adverse_slippage_bps': round(estimate, 4),
        'stress_adverse_slippage_bps': round(stress, 4),
        'baseline_bps': round(baseline, 4),
        'relaxation_cap': MAX_RELAXATION,
    }
