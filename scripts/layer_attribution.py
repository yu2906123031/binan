from __future__ import annotations

import threading
import uuid
from typing import Any, Iterable

_STATE = threading.local()
ATTRIBUTION_EVENT_TYPE = 'layer_attribution_scan'
ATTRIBUTION_VERSION = 1


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _side(candidate: Any) -> str:
    text = str(getattr(candidate, 'position_side', None) or getattr(candidate, 'side', 'LONG') or 'LONG').strip().upper()
    return 'SHORT' if text in {'SHORT', 'SELL'} else 'LONG'


def _triggered(candidate: Any) -> bool:
    return bool(getattr(candidate, 'trigger_fired', False)) and not bool(getattr(candidate, 'pretrigger_watch', False))


def _score(candidate: Any, base_attr: str, multiplier_attr: str, fallback: float) -> float:
    base = getattr(candidate, base_attr, None)
    multiplier = getattr(candidate, multiplier_attr, None)
    if base is None or multiplier is None:
        return fallback
    return _num(base, fallback) * _num(multiplier, 1.0)


def reconstruct_stage_scores(candidate: Any) -> dict[str, float]:
    """Reconstruct the score after each existing ranking layer without rerunning policy logic."""
    final_score = _num(getattr(candidate, 'score', 0.0))
    raw = _num(getattr(candidate, 'base_ranking_score', final_score), final_score)
    market = raw * _num(getattr(candidate, 'market_direction_score_multiplier', 1.0), 1.0)
    edge = market * _num(getattr(candidate, 'realizable_edge_score_multiplier', 1.0), 1.0)

    quality = getattr(candidate, 'selection_quality_score', None)
    if quality is None:
        quality = edge * _num(getattr(candidate, 'selection_quality_multiplier', 1.0), 1.0)
    quality_score = _num(quality, edge)

    relative = _score(candidate, 'relative_selection_base_score', 'relative_selection_multiplier', quality_score)
    diversified = _score(candidate, 'diversification_base_score', 'selection_diversification_multiplier', relative)
    stable = _score(candidate, 'stability_base_score', 'selection_stability_multiplier', diversified)
    outcome = _score(candidate, 'selection_outcome_base_score', 'selection_outcome_multiplier', stable)
    if hasattr(candidate, 'selection_outcome_multiplier'):
        outcome = final_score

    return {
        'raw': round(raw, 4),
        'market': round(market, 4),
        'trigger_priority': round(market, 4),
        'realizable_edge': round(edge, 4),
        'selection_quality': round(quality_score, 4),
        'relative_selection': round(relative, 4),
        'diversification': round(diversified, 4),
        'stability': round(stable, 4),
        'outcome_calibration': round(outcome, 4),
    }


def build_candidate_attribution_snapshot(candidate: Any) -> dict[str, Any]:
    scores = reconstruct_stage_scores(candidate)
    return {
        'symbol': str(getattr(candidate, 'symbol', '') or '').strip().upper(),
        'side': _side(candidate),
        'trigger_fired': _triggered(candidate),
        'candidate_stage': str(getattr(candidate, 'candidate_stage', '') or ''),
        'state': str(getattr(candidate, 'state', '') or ''),
        'alert_tier': str(getattr(candidate, 'alert_tier', '') or ''),
        'trigger_class': str(getattr(candidate, 'trigger_class', '') or ''),
        'market_regime_label': str(
            getattr(candidate, 'market_regime_label', '') or getattr(candidate, 'regime_label', '') or ''
        ),
        'entry_reference_price': round(_num(getattr(candidate, 'last_price', 0.0)), 8),
        'expected_edge': round(_num(getattr(candidate, 'expected_edge', 0.0)), 6),
        'realizable_edge_margin_r': round(_num(getattr(candidate, 'realizable_edge_margin_r', 0.0)), 6),
        'expected_slippage_pct': round(_num(getattr(candidate, 'expected_slippage_pct', 0.0)), 6),
        'liquidity_grade': str(getattr(candidate, 'liquidity_grade', '') or ''),
        'relative_selection_percentile': round(_num(getattr(candidate, 'relative_selection_percentile', 0.5), 0.5), 4),
        'selection_stability_streak': int(_num(getattr(candidate, 'selection_stability_streak', 0), 0)),
        'stage_scores': scores,
    }


def _winner(candidates: list[Any], stage: str, *, trigger_priority: bool) -> dict[str, Any] | None:
    if not candidates:
        return None

    def key(candidate: Any) -> tuple[int, float]:
        triggered = 1 if trigger_priority and _triggered(candidate) else 0
        score = reconstruct_stage_scores(candidate)[stage]
        return triggered, score

    selected = max(candidates, key=key)
    scores = reconstruct_stage_scores(selected)
    return {
        'symbol': str(getattr(selected, 'symbol', '') or '').strip().upper(),
        'side': _side(selected),
        'trigger_fired': _triggered(selected),
        'score': scores[stage],
    }


def build_layer_attribution_payload(candidates: Iterable[Any], scan_id: str) -> dict[str, Any]:
    cohort = list(candidates)
    stages = [
        ('raw', False),
        ('market', False),
        ('trigger_priority', True),
        ('realizable_edge', True),
        ('selection_quality', True),
        ('relative_selection', True),
        ('diversification', True),
        ('stability', True),
        ('outcome_calibration', True),
    ]
    return {
        'attribution_version': ATTRIBUTION_VERSION,
        'scan_id': scan_id,
        'candidate_count': len(cohort),
        'stages': [
            {'stage': stage, 'winner': _winner(cohort, stage, trigger_priority=trigger_priority)}
            for stage, trigger_priority in stages
        ],
        'candidates': [build_candidate_attribution_snapshot(candidate) for candidate in cohort],
    }


def install_layer_attribution_hook(strategy_module: Any) -> None:
    """Persist a shadow attribution ledger without changing strategy decisions."""
    original_run_scan = getattr(strategy_module, 'run_scan_once', None)
    original_build_alert = getattr(strategy_module, 'build_standardized_alert', None)
    get_store = getattr(strategy_module, 'get_runtime_state_store', None)
    if not callable(original_run_scan) or not callable(original_build_alert) or not callable(get_store):
        return
    if getattr(original_run_scan, '_layer_attribution_hook', False):
        return

    def build_alert_with_attribution(candidate: Any, *args: Any, **kwargs: Any):
        if getattr(_STATE, 'active', False) and hasattr(candidate, 'score'):
            scan_id = str(getattr(_STATE, 'scan_id', '') or '')
            if scan_id:
                candidate.layer_attribution_scan_id = scan_id
            cohort = getattr(_STATE, 'cohort', None)
            if cohort is None:
                cohort = []
                _STATE.cohort = cohort
            if all(existing is not candidate for existing in cohort):
                cohort.append(candidate)
        return original_build_alert(candidate, *args, **kwargs)

    def run_scan_with_attribution(*args: Any, **kwargs: Any):
        scan_args = kwargs.get('args')
        if scan_args is None and len(args) >= 2:
            scan_args = args[1]
        if scan_args is None:
            return original_run_scan(*args, **kwargs)

        store = get_store(scan_args)
        previous_active = getattr(_STATE, 'active', False)
        previous_scan_id = getattr(_STATE, 'scan_id', None)
        previous_cohort = getattr(_STATE, 'cohort', None)
        _STATE.active = True
        _STATE.scan_id = uuid.uuid4().hex
        _STATE.cohort = []
        try:
            result = original_run_scan(*args, **kwargs)
            cohort = list(getattr(_STATE, 'cohort', []) or [])
            if cohort:
                try:
                    store.append_event(
                        ATTRIBUTION_EVENT_TYPE,
                        build_layer_attribution_payload(cohort, str(_STATE.scan_id)),
                    )
                except Exception:
                    pass
            return result
        finally:
            _STATE.active = previous_active
            _STATE.scan_id = previous_scan_id
            _STATE.cohort = previous_cohort

    build_alert_with_attribution._layer_attribution_hook = True  # type: ignore[attr-defined]
    run_scan_with_attribution._layer_attribution_hook = True  # type: ignore[attr-defined]
    strategy_module.build_standardized_alert = build_alert_with_attribution
    strategy_module.run_scan_once = run_scan_with_attribution
