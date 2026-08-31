from __future__ import annotations

from typing import Any, Dict


CANDIDATE_SNAPSHOT_FIELDS = (
    'layer_attribution_scan_id',
    'realizable_reward_r',
    'expected_reward_r',
    'expected_edge',
    'expected_total_fee_pct',
    'execution_slippage_buffer_pct',
    'min_profit_buffer_pct',
    'expected_slippage_pct',
    'expected_slippage_r',
    'stop_distance_pct',
    'trigger_confirmation_count',
    'trigger_confirmation_flags',
    'candidate_stage',
    'setup_ready',
    'trigger_fired',
    'state',
    'alert_tier',
    'trigger_class',
    'market_regime_label',
    'regime_label',
    'regime_multiplier',
    'liquidity_grade',
    'execution_liquidity_grade',
    'score',
    'selection_quality_multiplier',
    'selection_htf_alignment',
    'relative_selection_percentile',
    'relative_selection_metric_count',
    'relative_selection_multiplier',
    'selection_diversification_group',
    'selection_diversification_duplicate_index',
    'selection_diversification_multiplier',
    'selection_stability_streak',
    'selection_stability_historical_percentile',
    'selection_stability_multiplier',
    'selection_outcome_multiplier',
)

ENTRY_FEEDBACK_FIELDS = (
    'execution_mode',
    'maker_or_taker',
    'predicted_slippage_bps',
    'actual_fill_slippage_bps',
    'actual_fill_slippage_abs_bps',
    'slippage_error_bps',
    'within_expected_slippage',
    'predicted_fill_price',
    'fill_price',
    'market_price_at_submit',
    'fill_latency_ms',
    'fill_ratio',
    'maker_fill_ratio',
    'liquidity_grade_at_entry',
    'liquidity_grade',
    'liquidity_grade_reason',
    'post_only_taker_fallback',
)


def _value_present(value: Any) -> bool:
    return value not in (None, '', [], {})


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalize_side(value: Any) -> str:
    text = str(value or '').strip().upper()
    if text in {'SELL', 'SHORT'}:
        return 'SHORT'
    return 'LONG'


def _directional_slippage_bps(side: Any, fill_price: float, reference_price: float) -> float | None:
    if fill_price <= 0 or reference_price <= 0:
        return None
    if _normalize_side(side) == 'SHORT':
        return (reference_price - fill_price) / reference_price * 10000.0
    return (fill_price - reference_price) / reference_price * 10000.0


def build_entry_prediction_snapshot(candidate: Any, live_execution: Dict[str, Any]) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {}
    for field in CANDIDATE_SNAPSHOT_FIELDS:
        value = getattr(candidate, field, None)
        if _value_present(value):
            snapshot[field] = value

    if not _value_present(snapshot.get('market_regime_label')):
        regime_label = getattr(candidate, 'regime_label', None)
        if _value_present(regime_label):
            snapshot['market_regime_label'] = regime_label
    if not _value_present(snapshot.get('market_regime_multiplier')):
        regime_multiplier = getattr(candidate, 'regime_multiplier', None)
        if _value_present(regime_multiplier):
            snapshot['market_regime_multiplier'] = regime_multiplier

    entry_feedback = live_execution.get('entry_order_feedback', {})
    if not isinstance(entry_feedback, dict):
        entry_feedback = {}
    for field in ENTRY_FEEDBACK_FIELDS:
        value = entry_feedback.get(field)
        if _value_present(value):
            snapshot[field] = value

    if not _value_present(snapshot.get('liquidity_grade_at_entry')):
        feedback_grade = entry_feedback.get('liquidity_grade')
        if _value_present(feedback_grade):
            snapshot['liquidity_grade_at_entry'] = feedback_grade

    entry_price = _to_float(
        snapshot.get('fill_price')
        or live_execution.get('entry_price')
        or live_execution.get('average_price')
        or live_execution.get('avg_price')
    )
    reference_price = _to_float(
        snapshot.get('market_price_at_submit')
        or snapshot.get('predicted_fill_price')
        or getattr(candidate, 'last_price', 0.0)
    )
    if entry_price > 0:
        snapshot['actual_entry_price'] = entry_price
    if reference_price > 0:
        snapshot['entry_reference_price'] = reference_price
    if 'actual_fill_slippage_bps' not in snapshot and entry_price > 0 and reference_price > 0:
        directional = _directional_slippage_bps(
            getattr(candidate, 'side', getattr(candidate, 'position_side', 'LONG')),
            entry_price,
            reference_price,
        )
        if directional is not None:
            snapshot['actual_fill_slippage_bps'] = round(directional, 4)
            snapshot['actual_fill_slippage_abs_bps'] = round(abs(directional), 4)
    return snapshot


def install_lifecycle_snapshot_hooks(strategy_module: Any) -> None:
    original_execute = getattr(strategy_module, 'execute_candidate', None)
    if not callable(original_execute) or getattr(original_execute, '_lifecycle_snapshot_hook', False):
        return

    def execute_with_snapshot(candidate: Any, *args: Any, **kwargs: Any):
        result = original_execute(candidate, *args, **kwargs)
        if not isinstance(result, dict):
            return result
        try:
            snapshot = build_entry_prediction_snapshot(candidate, result)
        except Exception:
            return result
        if snapshot:
            result = dict(result)
            result['entry_prediction_snapshot'] = snapshot
        return result

    execute_with_snapshot._lifecycle_snapshot_hook = True  # type: ignore[attr-defined]
    strategy_module.execute_candidate = execute_with_snapshot
