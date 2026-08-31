from __future__ import annotations

from typing import Any, Dict


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


def normalize_entry_order_feedback(candidate: Any, live_execution: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(live_execution or {})
    raw_feedback = normalized.get('entry_order_feedback', {})
    feedback = dict(raw_feedback) if isinstance(raw_feedback, dict) else {}

    fill_price = _to_float(
        normalized.get('entry_price')
        or feedback.get('fill_price')
        or feedback.get('avg_price')
    )
    reference_price = _to_float(
        feedback.get('market_price_at_submit')
        or getattr(candidate, 'last_price', 0.0)
    )
    side = getattr(candidate, 'position_side', None) or getattr(candidate, 'side', None)
    predicted_bps = max(_to_float(feedback.get('predicted_slippage_bps')), 0.0)

    if fill_price > 0:
        feedback['fill_price'] = fill_price
    if reference_price > 0:
        feedback['market_price_at_submit'] = reference_price
    if predicted_bps > 0 and reference_price > 0 and not feedback.get('predicted_fill_price'):
        direction = -1.0 if _normalize_side(side) == 'SHORT' else 1.0
        feedback['predicted_fill_price'] = round(
            reference_price * (1.0 + direction * predicted_bps / 10000.0),
            12,
        )

    directional_bps = _directional_slippage_bps(side, fill_price, reference_price)
    if directional_bps is not None:
        feedback['actual_fill_slippage_bps'] = round(directional_bps, 4)
        feedback['actual_fill_slippage_abs_bps'] = round(abs(directional_bps), 4)
        feedback['slippage_error_bps'] = round(directional_bps - predicted_bps, 4)
        feedback['within_expected_slippage'] = directional_bps <= predicted_bps

    liquidity_grade = feedback.get('liquidity_grade_at_entry') or feedback.get('liquidity_grade')
    if liquidity_grade not in (None, ''):
        feedback['liquidity_grade_at_entry'] = liquidity_grade

    normalized['entry_order_feedback'] = feedback
    return normalized


def install_execution_feedback_hardening(strategy_module: Any) -> None:
    original = getattr(strategy_module, 'place_live_trade', None)
    if not callable(original) or getattr(original, '_execution_feedback_hardening_hook', False):
        return

    def place_live_trade_with_feedback(*args: Any, **kwargs: Any):
        result = original(*args, **kwargs)
        if not isinstance(result, dict):
            return result
        candidate = kwargs.get('candidate')
        if candidate is None and len(args) >= 2:
            candidate = args[1]
        if candidate is None:
            return result
        return normalize_entry_order_feedback(candidate, result)

    place_live_trade_with_feedback._execution_feedback_hardening_hook = True  # type: ignore[attr-defined]
    strategy_module.place_live_trade = place_live_trade_with_feedback
