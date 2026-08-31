from __future__ import annotations

from typing import Any, Iterable


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def build_failed_breakout_shadow_row(event: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(event, dict):
        return None
    event_type = str(event.get('event_type') or '')
    if event_type not in {'breakout_rejected', 'rejected_breakout', 'false_breakout'}:
        return None
    side = str(event.get('side') or event.get('position_side') or 'LONG').upper()
    fade_side = 'SHORT' if side in {'LONG', 'BUY'} else 'LONG'
    return {
        'symbol': str(event.get('symbol') or '').upper(),
        'original_side': 'LONG' if side in {'LONG', 'BUY'} else 'SHORT',
        'fade_side': fade_side,
        'market_regime_label': str(event.get('market_regime_label') or event.get('regime_label') or ''),
        'liquidity_grade': str(event.get('liquidity_grade') or ''),
        'breakout_strength': _num(event.get('breakout_strength')),
        'failure_distance_pct': _num(event.get('failure_distance_pct')),
        'entry_reference_price': _num(event.get('last_price') or event.get('price')),
        'shadow_only': True,
    }


def summarize_failed_breakout_fades(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    samples = [row for row in rows if isinstance(row, dict) and row.get('shadow_only')]
    realized = [_num(row.get('net_r')) for row in samples if row.get('net_r') not in (None, '')]
    return {
        'sample_count': len(samples),
        'closed_sample_count': len(realized),
        'avg_net_r': round(sum(realized) / len(realized), 6) if realized else None,
        'positive_rate': round(sum(value > 0 for value in realized) / len(realized), 6) if realized else None,
        'live_enabled': False,
        'min_live_gate_samples': 50,
    }
