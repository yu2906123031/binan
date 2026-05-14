from __future__ import annotations

from typing import Any, Dict, Optional, Sequence


def evaluate_portfolio_risk_guards(
    open_positions: Sequence[Dict[str, Any]],
    candidate: Any = None,
    max_long_positions: int = 0,
    max_short_positions: int = 0,
    max_net_exposure_usdt: float = 0.0,
    max_gross_exposure_usdt: float = 0.0,
    per_symbol_single_side_only: bool = True,
    opposite_side_flip_cooldown_minutes: int = 0,
    *,
    build_position_exposure_snapshot,
    normalize_position_side,
    position_side_long: str,
    position_side_short: str,
    _to_float,
) -> Dict[str, Any]:
    snapshot = build_position_exposure_snapshot(open_positions)
    reasons = []
    candidate_symbol = str(getattr(candidate, 'symbol', '') or '').upper()
    candidate_side = position_side_long
    if candidate is not None:
        candidate_side = normalize_position_side(
            getattr(candidate, 'position_side', None) or getattr(candidate, 'side', position_side_long)
        )
    candidate_notional = abs(_to_float(getattr(candidate, 'notional', 0.0) or getattr(candidate, 'planned_notional', 0.0)))
    if candidate_notional <= 0:
        candidate_notional = abs(_to_float(getattr(candidate, 'entry_price', 0.0) or getattr(candidate, 'last_price', 0.0))) * abs(_to_float(getattr(candidate, 'quantity', 0.0)))

    if candidate is not None:
        if candidate_side == position_side_long and max_long_positions > 0 and snapshot['long_count'] >= max_long_positions:
            reasons.append('max_long_positions_reached')
        if candidate_side == position_side_short and max_short_positions > 0 and snapshot['short_count'] >= max_short_positions:
            reasons.append('max_short_positions_reached')
        if per_symbol_single_side_only and candidate_symbol:
            active_sides = snapshot['symbol_sides'].get(candidate_symbol, [])
            if active_sides and candidate_side not in active_sides:
                reasons.append('per_symbol_single_side_only_violation')
            if opposite_side_flip_cooldown_minutes > 0 and active_sides and candidate_side not in active_sides:
                reasons.append('opposite_side_flip_cooldown_active')
        projected_net = snapshot['net_exposure_usdt'] + (candidate_notional if candidate_side == position_side_long else -candidate_notional)
        projected_gross = snapshot['gross_exposure_usdt'] + candidate_notional
        if max_net_exposure_usdt > 0 and abs(projected_net) >= max_net_exposure_usdt:
            reasons.append('max_net_exposure_reached')
        if max_gross_exposure_usdt > 0 and projected_gross >= max_gross_exposure_usdt:
            reasons.append('max_gross_exposure_reached')
    snapshot['candidate_symbol'] = candidate_symbol
    snapshot['candidate_side'] = candidate_side
    snapshot['candidate_notional_usdt'] = candidate_notional
    return {'allowed': not reasons, 'reasons': reasons, 'snapshot': snapshot}


def evaluate_risk_guards(
    symbol: Optional[str] = None,
    risk_state: Optional[Dict[str, Any]] = None,
    candidate: Any = None,
    now_ts: Optional[int] = None,
    daily_max_loss_usdt: float = 0.0,
    max_consecutive_losses: int = 0,
    symbol_cooldown_minutes: int = 0,
    *,
    default_risk_state,
    _to_float,
    compute_expected_slippage_r,
    classify_execution_liquidity_grade,
    estimate_candidate_heat_r,
    time_module,
    **kwargs: Any,
) -> Dict[str, Any]:
    normalized = default_risk_state()
    if isinstance(risk_state, dict):
        normalized.update(risk_state)
    if not isinstance(normalized.get('symbol_cooldowns'), dict):
        normalized['symbol_cooldowns'] = {}
    if not isinstance(normalized.get('portfolio_exposure_pct_by_theme'), dict):
        normalized['portfolio_exposure_pct_by_theme'] = {}
    if not isinstance(normalized.get('portfolio_exposure_pct_by_correlation'), dict):
        normalized['portfolio_exposure_pct_by_correlation'] = {}
    if not isinstance(normalized.get('portfolio_heat_r_by_theme'), dict):
        normalized['portfolio_heat_r_by_theme'] = {}
    if not isinstance(normalized.get('portfolio_heat_r_by_correlation'), dict):
        normalized['portfolio_heat_r_by_correlation'] = {}

    reasons = []
    if normalized.get('halted'):
        reasons.append('strategy_halted')
    pnl = abs(_to_float(normalized.get('daily_realized_pnl_usdt')))
    if daily_max_loss_usdt > 0 and pnl >= daily_max_loss_usdt:
        reasons.append('daily_max_loss_reached')
    if max_consecutive_losses > 0 and int(normalized.get('consecutive_losses', 0) or 0) >= max_consecutive_losses:
        reasons.append('max_consecutive_losses_reached')

    cooldown_until = None
    if symbol:
        cooldown_until = normalized['symbol_cooldowns'].get(symbol)
        ts = int(time_module.time()) if now_ts is None else int(now_ts)
        if cooldown_until and ts < int(cooldown_until):
            reasons.append('symbol_cooldown_active')

    if candidate is not None:
        state = getattr(candidate, 'state', '')
        if not bool(getattr(candidate, 'setup_ready', False)):
            reasons.append('candidate_setup_not_ready')
        elif not bool(getattr(candidate, 'trigger_fired', False)):
            if not bool(getattr(candidate, 'probe_entry', False)):
                reasons.append('candidate_trigger_not_fired')
        execution_slippage_r = compute_expected_slippage_r(candidate)
        spread_bps = _to_float(getattr(candidate, 'spread_bps', 0.0))
        orderbook_slope = _to_float(getattr(candidate, 'orderbook_slope', 0.0))
        cancel_rate = _to_float(getattr(candidate, 'cancel_rate', 0.0))
        execution_liquidity_grade = classify_execution_liquidity_grade(
            getattr(candidate, 'book_depth_fill_ratio', 0.0),
            execution_slippage_r,
            spread_bps=spread_bps,
            orderbook_slope=orderbook_slope,
            cancel_rate=cancel_rate,
        )
        if state == 'distribution':
            reasons.append('candidate_distribution_risk')
        if _to_float(getattr(candidate, 'cvd_delta', 0.0)) < 0 and _to_float(getattr(candidate, 'cvd_zscore', 0.0)) <= -2.0:
            reasons.append('candidate_cvd_divergence')
        oi_hard_reversal_threshold = abs(_to_float(getattr(candidate, 'oi_hard_reversal_threshold_pct', 0.8), default=0.8))
        if _to_float(getattr(candidate, 'oi_change_pct_5m', 0.0)) <= -oi_hard_reversal_threshold:
            reasons.append('candidate_oi_reversal')
        risk_slippage_r = max(_to_float(getattr(candidate, 'execution_slippage_risk_threshold_r', 0.15), default=0.15), 0.0)
        if execution_slippage_r > risk_slippage_r:
            reasons.append('candidate_execution_slippage_risk')
        liquidity_penalty_present = spread_bps > 0 or orderbook_slope > 0 or cancel_rate > 0
        if execution_liquidity_grade == 'D':
            reasons.append('candidate_execution_liquidity_poor')
        elif execution_liquidity_grade == 'C' and (
            _to_float(getattr(candidate, 'book_depth_fill_ratio', 0.0)) < 0.5 or liquidity_penalty_present
        ):
            reasons.append('candidate_execution_liquidity_poor')
        elif execution_liquidity_grade == 'B' and liquidity_penalty_present:
            reasons.append('candidate_execution_liquidity_poor')
        position_size_pct = max(_to_float(getattr(candidate, 'position_size_pct', 0.0)), 0.0)
        portfolio_narrative_bucket = str(kwargs.get('portfolio_narrative_bucket') or getattr(candidate, 'portfolio_narrative_bucket', '') or '').strip()
        portfolio_correlation_group = str(kwargs.get('portfolio_correlation_group') or getattr(candidate, 'portfolio_correlation_group', '') or '').strip()
        max_theme = max(_to_float(kwargs.get('max_portfolio_exposure_pct_per_theme', 0.0)), 0.0)
        max_corr = max(_to_float(kwargs.get('max_portfolio_exposure_pct_per_correlation_group', 0.0)), 0.0)
        base_risk_usdt = max(_to_float(kwargs.get('base_risk_usdt', 0.0)), 0.0)
        candidate_heat_r = estimate_candidate_heat_r(candidate, base_risk_usdt=base_risk_usdt)
        current_open_heat_r = max(_to_float(normalized.get('portfolio_heat_open_r', 0.0)), 0.0)
        current_pending_heat_r = max(_to_float(normalized.get('portfolio_heat_pending_r', 0.0)), 0.0)
        gross_heat_cap_r = max(_to_float(kwargs.get('gross_heat_cap_r', 0.0)), 0.0)
        same_theme_heat_cap_r = max(_to_float(kwargs.get('same_theme_heat_cap_r', 0.0)), 0.0)
        same_correlation_heat_cap_r = max(_to_float(kwargs.get('same_correlation_heat_cap_r', 0.0)), 0.0)
        if gross_heat_cap_r > 0 and (current_open_heat_r + current_pending_heat_r + candidate_heat_r) >= gross_heat_cap_r:
            reasons.append('candidate_portfolio_heat_overexposure')
        if portfolio_narrative_bucket and max_theme > 0 and position_size_pct > 0:
            current_theme = _to_float(normalized['portfolio_exposure_pct_by_theme'].get(portfolio_narrative_bucket))
            if current_theme + position_size_pct >= max_theme:
                reasons.append('candidate_portfolio_theme_overexposure')
        if portfolio_correlation_group and max_corr > 0 and position_size_pct > 0:
            current_corr = _to_float(normalized['portfolio_exposure_pct_by_correlation'].get(portfolio_correlation_group))
            if current_corr + position_size_pct >= max_corr:
                reasons.append('candidate_portfolio_correlation_overexposure')
        if portfolio_narrative_bucket and same_theme_heat_cap_r > 0 and candidate_heat_r > 0:
            current_theme_heat = _to_float(normalized['portfolio_heat_r_by_theme'].get(portfolio_narrative_bucket))
            if current_theme_heat + candidate_heat_r >= same_theme_heat_cap_r:
                reasons.append('candidate_same_theme_heat_overexposure')
        if portfolio_correlation_group and same_correlation_heat_cap_r > 0 and candidate_heat_r > 0:
            current_corr_heat = _to_float(normalized['portfolio_heat_r_by_correlation'].get(portfolio_correlation_group))
            if current_corr_heat + candidate_heat_r >= same_correlation_heat_cap_r:
                reasons.append('candidate_same_correlation_heat_overexposure')

    return {'allowed': not reasons, 'reasons': reasons, 'cooldown_until': cooldown_until, 'normalized_risk_state': normalized}
