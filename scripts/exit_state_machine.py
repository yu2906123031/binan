from __future__ import annotations

from typing import Any

STATES = {
    'INITIAL_RISK',
    'CONFIRMED_MOMENTUM',
    'PROFIT_PROTECTED',
    'RUNNER',
    'EXIT_PENDING',
    'FAILED_TRIGGER',
    'STRUCTURE_INVALIDATED',
    'CLOSED',
}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def evaluate_exit_state(position: dict[str, Any], market: dict[str, Any]) -> dict[str, Any]:
    current = str(position.get('exit_state') or 'INITIAL_RISK').upper()
    if current not in STATES:
        current = 'INITIAL_RISK'
    if current == 'CLOSED':
        return {'state': 'CLOSED', 'action': 'none', 'reason': 'already_closed', 'size_fraction': 0.0}

    mfe_r = _num(position.get('mfe_r'))
    mae_r = _num(position.get('mae_r'))
    time_minutes = _num(position.get('time_in_trade_minutes'))
    stop_distance_r = abs(_num(position.get('initial_risk_r'), 1.0)) or 1.0
    structure_valid = bool(market.get('structure_valid', True))
    trigger_valid = bool(market.get('trigger_valid', True))
    momentum_alive = bool(market.get('momentum_alive', True))
    htf_aligned = bool(market.get('htf_aligned', True))
    adverse_slippage_bps = max(_num(position.get('actual_fill_slippage_bps')), 0.0)
    slippage_limit_bps = max(_num(position.get('max_adverse_slippage_bps'), 20.0), 0.0)

    if not structure_valid:
        return {'state': 'STRUCTURE_INVALIDATED', 'action': 'close', 'reason': 'structure_invalidated', 'size_fraction': 1.0}
    if not trigger_valid and mfe_r < 0.35:
        return {'state': 'FAILED_TRIGGER', 'action': 'close', 'reason': 'trigger_failed_without_followthrough', 'size_fraction': 1.0}
    if time_minutes >= 30 and mfe_r < 0.25 and mae_r <= 0.0:
        return {'state': 'EXIT_PENDING', 'action': 'close', 'reason': 'time_stop_no_followthrough', 'size_fraction': 1.0}
    if adverse_slippage_bps > slippage_limit_bps and current == 'INITIAL_RISK':
        return {'state': 'EXIT_PENDING', 'action': 'no_add', 'reason': 'adverse_entry_slippage', 'size_fraction': 0.0}

    if current == 'INITIAL_RISK' and mfe_r >= 0.60:
        return {'state': 'CONFIRMED_MOMENTUM', 'action': 'hold', 'reason': 'followthrough_confirmed', 'size_fraction': 0.0}
    if current in {'INITIAL_RISK', 'CONFIRMED_MOMENTUM'} and mfe_r >= 1.0:
        return {'state': 'PROFIT_PROTECTED', 'action': 'reduce', 'reason': 'protect_first_r', 'size_fraction': 0.25}
    if current in {'PROFIT_PROTECTED', 'CONFIRMED_MOMENTUM'} and mfe_r >= 1.5 and momentum_alive and htf_aligned:
        return {'state': 'RUNNER', 'action': 'hold', 'reason': 'runner_conditions_met', 'size_fraction': 0.0}
    if current == 'RUNNER' and (not momentum_alive or not htf_aligned):
        return {'state': 'EXIT_PENDING', 'action': 'reduce', 'reason': 'runner_momentum_decay', 'size_fraction': 0.50}

    return {
        'state': current,
        'action': 'hold',
        'reason': 'state_unchanged',
        'size_fraction': 0.0,
        'risk_unit': round(stop_distance_r, 4),
    }
