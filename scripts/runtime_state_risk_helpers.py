from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple


CanonicalOpenPositionsIter = Callable[[Any], List[Tuple[str, Dict[str, Any]]]]
NormalizePositionSide = Callable[[Any], str]
ToFloat = Callable[..., float]


def build_local_open_positions_from_state(
    positions_state: Any,
    *,
    error: Optional[Dict[str, Any]],
    normalize_position_side: NormalizePositionSide,
    to_float: ToFloat,
    iter_canonical_open_positions: CanonicalOpenPositionsIter,
) -> List[Dict[str, Any]]:
    if error:
        return []
    rows: List[Dict[str, Any]] = []
    for _key, position in iter_canonical_open_positions(positions_state):
        side = normalize_position_side(position.get('side') or position.get('position_side'))
        quantity = abs(to_float(position.get('remaining_quantity') or position.get('quantity') or position.get('filled_quantity')))
        entry_price = abs(to_float(position.get('entry_price')))
        rows.append({
            'symbol': str(position.get('symbol') or '').upper(),
            'side': side,
            'positionSide': side,
            'quantity': quantity,
            'positionAmt': quantity if side == 'LONG' else -quantity,
            'entryPrice': entry_price,
            'notional': abs(to_float(position.get('notional'))) or quantity * entry_price,
        })
    return rows
