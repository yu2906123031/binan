from __future__ import annotations

import threading
from typing import Any

from edge_position_sizing import apply_edge_size_annotation
from hierarchical_cost_policy import build_hierarchical_cost_model, resolve_hierarchical_cost
from portfolio_correlation_policy import evaluate_portfolio_incremental_risk

_STATE = threading.local()


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _apply_notional_cap(candidate: Any, multiplier: float) -> None:
    live_multiplier = max(0.0, min(float(multiplier), 1.0))
    fields = ('planned_notional_usdt', 'planned_notional', 'notional')
    for field in fields:
        value = getattr(candidate, field, None)
        if value in (None, ''):
            continue
        base_attr = f'pre_optimization_{field}'
        base = _num(getattr(candidate, base_attr, value), _num(value))
        setattr(candidate, base_attr, base)
        setattr(candidate, field, round(base * live_multiplier, 8))
        break


def apply_conservative_decision_risk(candidate: Any, *, cost_model: dict[str, Any], open_positions: list[Any]) -> Any:
    baseline_bps = max(_num(getattr(candidate, 'expected_slippage_pct', 0.0)) * 100.0, 0.0)
    cost = resolve_hierarchical_cost(candidate, cost_model, current_bps=baseline_bps)
    candidate.hierarchical_cost_estimate_bps = cost['estimated_adverse_slippage_bps']
    candidate.hierarchical_cost_stress_bps = cost['stress_adverse_slippage_bps']
    candidate.hierarchical_cost_source = cost['source']
    candidate.hierarchical_cost_sample_count = cost['sample_count']

    size = apply_edge_size_annotation(candidate)
    portfolio = evaluate_portfolio_incremental_risk(candidate, open_positions)
    candidate.portfolio_risk_size_multiplier = portfolio['size_multiplier']
    candidate.portfolio_same_group_open_count = portfolio['same_group_open_count']
    candidate.portfolio_max_positive_correlation = portfolio['max_positive_correlation']
    candidate.portfolio_correlation_sample_count = portfolio['correlation_sample_count']

    recommended = max(0.0, min(float(size['multiplier']) * float(portfolio['size_multiplier']), 1.10))
    candidate.optimized_size_multiplier_recommended = round(recommended, 4)
    candidate.optimized_size_multiplier_live = round(min(recommended, 1.0), 4)
    candidate.optimized_size_increase_shadow_only = recommended > 1.0
    _apply_notional_cap(candidate, min(recommended, 1.0))
    return candidate


def install_decision_risk_optimization_hook(strategy_module: Any) -> None:
    original_run_scan = getattr(strategy_module, 'run_scan_once', None)
    original_build_alert = getattr(strategy_module, 'build_standardized_alert', None)
    get_store = getattr(strategy_module, 'get_runtime_state_store', None)
    if not callable(original_run_scan) or not callable(original_build_alert) or not callable(get_store):
        return
    if getattr(original_run_scan, '_decision_risk_optimization_hook', False):
        return

    def build_alert_with_risk_optimization(candidate: Any, *args: Any, **kwargs: Any):
        if getattr(_STATE, 'active', False) and hasattr(candidate, 'score'):
            try:
                apply_conservative_decision_risk(
                    candidate,
                    cost_model=getattr(_STATE, 'cost_model', {}),
                    open_positions=list(getattr(_STATE, 'open_positions', []) or []),
                )
            except Exception:
                pass
        return original_build_alert(candidate, *args, **kwargs)

    def run_scan_with_risk_optimization(*args: Any, **kwargs: Any):
        scan_args = kwargs.get('args')
        if scan_args is None and len(args) >= 2:
            scan_args = args[1]
        if scan_args is None:
            return original_run_scan(*args, **kwargs)
        store = get_store(scan_args)
        try:
            events = store.read_events(limit=5000)
            cost_model = build_hierarchical_cost_model(events)
        except Exception:
            cost_model = {}
        try:
            positions = store.load_json('positions', {})
            open_positions = list(positions.values()) if isinstance(positions, dict) else []
        except Exception:
            open_positions = []

        previous_active = getattr(_STATE, 'active', False)
        previous_model = getattr(_STATE, 'cost_model', None)
        previous_positions = getattr(_STATE, 'open_positions', None)
        _STATE.active = True
        _STATE.cost_model = cost_model
        _STATE.open_positions = open_positions
        try:
            return original_run_scan(*args, **kwargs)
        finally:
            _STATE.active = previous_active
            _STATE.cost_model = previous_model
            _STATE.open_positions = previous_positions

    build_alert_with_risk_optimization._decision_risk_optimization_hook = True  # type: ignore[attr-defined]
    run_scan_with_risk_optimization._decision_risk_optimization_hook = True  # type: ignore[attr-defined]
    strategy_module.build_standardized_alert = build_alert_with_risk_optimization
    strategy_module.run_scan_once = run_scan_with_risk_optimization
