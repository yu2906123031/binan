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
        live_execution.get('entry_price')
        or entry_feedback.get('fill_price')
        or entry_feedback.get('avg_price')
    )
    reference_price = _to_float(
        entry_feedback.get('market_price_at_submit')
        or getattr(candidate, 'last_price', 0.0)
    )
    stop_price = _to_float(getattr(candidate, 'stop_price', 0.0))
    side = getattr(candidate, 'position_side', None) or getattr(candidate, 'side', None)
    if entry_price > 0:
        snapshot['entry_price_at_fill'] = entry_price
        snapshot.setdefault('fill_price', entry_price)
    if reference_price > 0:
        snapshot['entry_reference_price'] = reference_price
        snapshot['market_price_at_submit'] = reference_price
        snapshot.setdefault('shadow_entry_price', reference_price)
    if stop_price > 0:
        snapshot['initial_stop_price'] = stop_price
    if not _value_present(snapshot.get('stop_distance_pct')) and reference_price > 0 and stop_price > 0:
        snapshot['stop_distance_pct'] = abs(reference_price - stop_price) / reference_price * 100.0

    directional_bps = _directional_slippage_bps(side, entry_price, reference_price)
    if directional_bps is not None:
        snapshot['actual_fill_slippage_bps'] = round(directional_bps, 4)
        snapshot['actual_fill_slippage_abs_bps'] = round(abs(directional_bps), 4)

    predicted_bps = _to_float(snapshot.get('predicted_slippage_bps'))
    actual_bps = _to_float(snapshot.get('actual_fill_slippage_bps'))
    if _value_present(snapshot.get('predicted_slippage_bps')) or _value_present(snapshot.get('actual_fill_slippage_bps')):
        snapshot['slippage_error_bps'] = round(actual_bps - predicted_bps, 4)
        snapshot['within_expected_slippage'] = actual_bps <= predicted_bps
    snapshot['prediction_snapshot_native'] = True
    snapshot['prediction_source_event'] = 'live_entry_lifecycle'
    return snapshot


def install_lifecycle_snapshot_hooks(strategy_module: Any) -> None:
    original_persist = getattr(strategy_module, 'persist_live_open_position', None)
    if callable(original_persist) and not getattr(original_persist, '_lifecycle_snapshot_hook', False):
        def persist_with_snapshot(store: Any, candidate: Any, live_execution: Dict[str, Any]):
            positions_state, position_key = original_persist(store, candidate, live_execution)
            snapshot = build_entry_prediction_snapshot(candidate, live_execution)
            position = positions_state.get(position_key, {}) if isinstance(positions_state, dict) else {}
            if not isinstance(position, dict):
                position = {}
            position_payload = dict(position)
            position_payload.update(snapshot)
            position_payload['entry_prediction_snapshot'] = dict(snapshot)
            upsert = getattr(strategy_module, 'upsert_position_record', None)
            if callable(upsert):
                positions_state, position_key = upsert(positions_state, position_payload, key=position_key)
            else:
                positions_state[position_key] = position_payload
            store.save_json('positions', positions_state)
            return positions_state, position_key

        persist_with_snapshot._lifecycle_snapshot_hook = True  # type: ignore[attr-defined]
        strategy_module.persist_live_open_position = persist_with_snapshot

    original_append = getattr(strategy_module, 'append_buy_fill_confirmed_event', None)
    if callable(original_append) and not getattr(original_append, '_lifecycle_snapshot_hook', False):
        def append_with_snapshot(store: Any, symbol: str, positions_state: Dict[str, Any], position_key: str):
            legacy_event = original_append(store, symbol, positions_state, position_key)
            position = positions_state.get(position_key, {}) if isinstance(positions_state, dict) else {}
            if isinstance(position, dict):
                snapshot = position.get('entry_prediction_snapshot', {})
                if isinstance(snapshot, dict) and snapshot:
                    payload = {
                        'symbol': symbol,
                        'side': position.get('side') or position.get('position_side'),
                        'position_key': position.get('position_key') or position_key,
                        'entry_price': position.get('entry_price'),
                        **snapshot,
                    }
                    store.append_event('entry_filled', payload)
            return legacy_event

        append_with_snapshot._lifecycle_snapshot_hook = True  # type: ignore[attr-defined]
        strategy_module.append_buy_fill_confirmed_event = append_with_snapshot
