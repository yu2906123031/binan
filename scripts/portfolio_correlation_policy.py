from __future__ import annotations

import math
from typing import Any, Iterable, Sequence


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return float(default)
        number = float(value)
        return number if math.isfinite(number) else float(default)
    except (TypeError, ValueError):
        return float(default)


def _series(value: Any) -> list[float]:
    if not isinstance(value, (list, tuple)):
        return []
    result: list[float] = []
    for item in value:
        number = _num(item, math.nan)
        if math.isfinite(number):
            result.append(number)
    return result


def robust_correlation(left: Sequence[float], right: Sequence[float]) -> float | None:
    n = min(len(left), len(right))
    if n < 8:
        return None
    x = list(left[-n:])
    y = list(right[-n:])
    mx = sum(x) / n
    my = sum(y) / n
    vx = sum((v - mx) ** 2 for v in x)
    vy = sum((v - my) ** 2 for v in y)
    if vx <= 1e-12 or vy <= 1e-12:
        return None
    cov = sum((a - mx) * (b - my) for a, b in zip(x, y))
    corr = cov / math.sqrt(vx * vy)
    return max(-1.0, min(corr, 1.0))


def _group(candidate: Any) -> str:
    for name in (
        'portfolio_correlation_group', 'correlation_group', 'portfolio_narrative_bucket',
        'narrative_bucket', 'sector', 'sector_name', 'category', 'theme', 'market_segment',
    ):
        value = str(getattr(candidate, name, '') or '').strip().upper()
        if value and value not in {'UNKNOWN', 'NONE', 'N/A', 'OTHER'}:
            return value
    return ''


def evaluate_portfolio_incremental_risk(candidate: Any, open_positions: Iterable[Any]) -> dict[str, Any]:
    candidate_returns = _series(
        getattr(candidate, 'recent_returns_1h', None)
        or getattr(candidate, 'recent_returns_4h', None)
        or getattr(candidate, 'recent_returns', None)
    )
    candidate_group = _group(candidate)
    same_group = 0
    correlations: list[float] = []
    for position in open_positions:
        group = _group(position)
        if candidate_group and group and group == candidate_group:
            same_group += 1
        position_returns = _series(
            getattr(position, 'recent_returns_1h', None) if not isinstance(position, dict) else position.get('recent_returns_1h')
        )
        if not position_returns and isinstance(position, dict):
            position_returns = _series(position.get('recent_returns_4h') or position.get('recent_returns'))
        elif not position_returns:
            position_returns = _series(getattr(position, 'recent_returns_4h', None) or getattr(position, 'recent_returns', None))
        corr = robust_correlation(candidate_returns, position_returns)
        if corr is not None:
            correlations.append(corr)

    max_positive_corr = max(correlations, default=0.0)
    multiplier = 1.0
    if same_group >= 2:
        multiplier = min(multiplier, 0.60)
    elif same_group == 1:
        multiplier = min(multiplier, 0.80)
    if max_positive_corr >= 0.90:
        multiplier = min(multiplier, 0.50)
    elif max_positive_corr >= 0.80:
        multiplier = min(multiplier, 0.65)
    elif max_positive_corr >= 0.70:
        multiplier = min(multiplier, 0.80)

    return {
        'size_multiplier': round(multiplier, 4),
        'same_group_open_count': same_group,
        'max_positive_correlation': round(max_positive_corr, 4),
        'correlation_sample_count': len(correlations),
        'dynamic_signal_available': bool(correlations),
        'group_signal_available': bool(candidate_group),
    }
