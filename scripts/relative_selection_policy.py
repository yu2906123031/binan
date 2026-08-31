from __future__ import annotations

import statistics
import threading
from typing import Any, Iterable

_STATE = threading.local()


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _side(candidate: Any) -> str:
    text = str(getattr(candidate, 'side', getattr(candidate, 'position_side', 'LONG')) or '').upper()
    return 'SHORT' if text in {'SHORT', 'SELL'} else 'LONG'


def _freshness(candidate: Any) -> float | None:
    change = _num(getattr(candidate, 'recent_5m_change_pct', None))
    acceleration = _num(getattr(candidate, 'acceleration_ratio_5m_vs_15m', None))
    if change is None or acceleration is None:
        return None
    directional = change if _side(candidate) == 'LONG' else -change
    return directional * max(acceleration, 0.0)


def _percentiles(candidates: list[Any], getter, *, higher_is_better: bool = True) -> dict[int, float]:
    rows: list[tuple[int, float]] = []
    for item in candidates:
        value = getter(item)
        if value is not None:
            rows.append((id(item), float(value)))
    if len(rows) < 3:
        return {}
    ordered = sorted(rows, key=lambda row: row[1], reverse=not higher_is_better)
    n = len(ordered)
    result: dict[int, float] = {}
    i = 0
    while i < n:
        j = i + 1
        while j < n and ordered[j][1] == ordered[i][1]:
            j += 1
        avg_rank = (i + j - 1) / 2.0
        percentile = avg_rank / (n - 1) if n > 1 else 0.5
        for k in range(i, j):
            result[ordered[k][0]] = percentile
        i = j
    return result


def _cohort_confidence(cohort: list[Any]) -> float:
    """Shrink cross-sectional tilts when the cohort is small or nearly tied."""
    if len(cohort) < 3:
        return 0.0
    count_confidence = min(1.0, 0.45 + 0.11 * (len(cohort) - 3))
    bases = [float(getattr(item, 'score', 0.0) or 0.0) for item in cohort]
    median = statistics.median(bases) if bases else 0.0
    if abs(median) <= 1e-9:
        dispersion_confidence = 0.65
    else:
        relative_range = (max(bases) - min(bases)) / max(abs(median), 1e-9)
        dispersion_confidence = max(0.35, min(relative_range / 0.20, 1.0))
    return round(max(0.0, min(count_confidence * dispersion_confidence, 1.0)), 4)


def rerank_candidate_cohort(candidates: Iterable[Any]) -> list[Any]:
    cohort = list(candidates)
    if len(cohort) < 3:
        return cohort

    metrics = [
        (0.30, _percentiles(cohort, lambda c: _num(getattr(c, 'realizable_edge_margin_r', getattr(c, 'expected_edge', None))), higher_is_better=True)),
        (0.20, _percentiles(cohort, lambda c: _num(getattr(c, 'book_depth_fill_ratio', None)), higher_is_better=True)),
        (0.15, _percentiles(cohort, lambda c: _num(getattr(c, 'spread_bps', None)), higher_is_better=False)),
        (0.10, _percentiles(cohort, lambda c: _num(getattr(c, 'estimated_impact_pct', None)), higher_is_better=False)),
        (0.10, _percentiles(cohort, lambda c: _num(getattr(c, 'quote_volume_24h', None)), higher_is_better=True)),
        (0.15, _percentiles(cohort, _freshness, higher_is_better=True)),
    ]
    cohort_confidence = _cohort_confidence(cohort)

    for candidate in cohort:
        if not hasattr(candidate, 'relative_selection_base_score'):
            candidate.relative_selection_base_score = float(getattr(candidate, 'score', 0.0) or 0.0)
        weighted = 0.0
        total_weight = 0.0
        metric_count = 0
        for weight, percentile_map in metrics:
            if id(candidate) in percentile_map:
                weighted += weight * percentile_map[id(candidate)]
                total_weight += weight
                metric_count += 1
        percentile_score = weighted / total_weight if total_weight > 0 else 0.5
        raw_tilt = (percentile_score - 0.5) * 0.12
        multiplier = 1.0 if metric_count < 2 else max(0.94, min(1.06, 1.0 + raw_tilt * cohort_confidence))
        candidate.relative_selection_percentile = round(percentile_score, 4)
        candidate.relative_selection_metric_count = metric_count
        candidate.relative_selection_cohort_size = len(cohort)
        candidate.relative_selection_confidence = cohort_confidence
        candidate.relative_selection_multiplier = round(multiplier, 4)
        candidate.score = round(float(candidate.relative_selection_base_score) * multiplier, 4)
        reasons = [r for r in list(getattr(candidate, 'reasons', []) or []) if not str(r).startswith('relative_selection=')]
        reasons.append(
            f'relative_selection=percentile={percentile_score:.3f}:metrics={metric_count}:'
            f'cohort={len(cohort)}:confidence={cohort_confidence:.3f}:multiplier={multiplier:.4f}'
        )
        candidate.reasons = reasons
    return cohort


def install_relative_selection_hook(strategy_module: Any) -> None:
    run_scan = getattr(strategy_module, 'run_scan_once', None)
    build_alert = getattr(strategy_module, 'build_standardized_alert', None)
    if not callable(run_scan) or not callable(build_alert):
        return
    if getattr(run_scan, '_relative_selection_hook', False):
        return

    def build_alert_with_cohort(candidate: Any, *args: Any, **kwargs: Any):
        if getattr(_STATE, 'active', False) and hasattr(candidate, 'score'):
            cohort = getattr(_STATE, 'cohort', None)
            if cohort is None:
                cohort = []
                _STATE.cohort = cohort
            if all(existing is not candidate for existing in cohort):
                cohort.append(candidate)
            rerank_candidate_cohort(cohort)
        return build_alert(candidate, *args, **kwargs)

    def run_scan_with_relative_selection(*args: Any, **kwargs: Any):
        previous_active = getattr(_STATE, 'active', False)
        previous_cohort = getattr(_STATE, 'cohort', None)
        _STATE.active = True
        _STATE.cohort = []
        try:
            return run_scan(*args, **kwargs)
        finally:
            _STATE.active = previous_active
            _STATE.cohort = previous_cohort

    build_alert_with_cohort._relative_selection_hook = True  # type: ignore[attr-defined]
    run_scan_with_relative_selection._relative_selection_hook = True  # type: ignore[attr-defined]
    strategy_module.build_standardized_alert = build_alert_with_cohort
    strategy_module.run_scan_once = run_scan_with_relative_selection
