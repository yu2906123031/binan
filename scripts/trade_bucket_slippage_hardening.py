from __future__ import annotations

from typing import Any, Dict


def _to_float(value: Any) -> float:
    try:
        return float(value)
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


def install_trade_bucket_slippage_hardening(trade_bucket_module: Any) -> None:
    original = getattr(trade_bucket_module, '_with_entry_fill', None)
    if not callable(original) or getattr(original, '_directional_slippage_hardening', False):
        return

    def with_directional_slippage(snapshot: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
        enriched = original(snapshot, row)
        if row.get('actual_fill_slippage_bps') not in (None, ''):
            return enriched
        entry_price = _to_float(row.get('entry_price') or row.get('avg_price'))
        reference_price = _to_float(
            enriched.get('market_price_at_submit')
            or enriched.get('entry_reference_price')
            or enriched.get('shadow_entry_price')
        )
        side = row.get('position_side') or row.get('side')
        directional = _directional_slippage_bps(side, entry_price, reference_price)
        if directional is None:
            return enriched
        directional = round(directional, 4)
        enriched['actual_fill_slippage_bps'] = directional
        enriched['actual_fill_slippage_abs_bps'] = round(abs(directional), 4)
        predicted = enriched.get('predicted_slippage_bps')
        if predicted not in (None, ''):
            enriched['slippage_error_bps'] = round(directional - _to_float(predicted), 4)
            enriched['within_expected_slippage'] = directional <= _to_float(predicted)
        return enriched

    with_directional_slippage._directional_slippage_hardening = True  # type: ignore[attr-defined]
    trade_bucket_module._with_entry_fill = with_directional_slippage
