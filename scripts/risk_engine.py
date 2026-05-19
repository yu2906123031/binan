from __future__ import annotations

import datetime
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
    if not isinstance(normalized.get('daily_symbol_trade_counts'), dict):
        normalized['daily_symbol_trade_counts'] = {}
    if not isinstance(normalized.get('recent_closed_trades'), list):
        normalized['recent_closed_trades'] = []

    def _normalize_candidate_side(value: Any) -> str:
        text = str(value or '').strip().upper()
        if text in {'SHORT', 'SELL'}:
            return 'SHORT'
        return 'LONG'

    reasons = []
    ts = int(time_module.time()) if now_ts is None else int(now_ts)
    normalized_symbol = str(symbol or '').strip().upper()
    candidate_side = _normalize_candidate_side(
        getattr(candidate, 'position_side', None) or getattr(candidate, 'side', None)
    ) if candidate is not None else 'LONG'
    if normalized.get('halted'):
        reasons.append('strategy_halted')
    allowed_session_utc_hours = kwargs.get('allowed_session_utc_hours')
    if allowed_session_utc_hours:
        try:
            allowed_hours = {int(hour) % 24 for hour in allowed_session_utc_hours}
        except (TypeError, ValueError):
            allowed_hours = set()
        current_utc_hour = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).hour
        if allowed_hours and current_utc_hour not in allowed_hours:
            reasons.append('session_filter_blocked')
    pnl = abs(_to_float(normalized.get('daily_realized_pnl_usdt')))
    if daily_max_loss_usdt > 0 and pnl >= daily_max_loss_usdt:
        reasons.append('daily_max_loss_reached')
    if max_consecutive_losses > 0 and int(normalized.get('consecutive_losses', 0) or 0) >= max_consecutive_losses:
        reasons.append('max_consecutive_losses_reached')

    cooldown_until = None
    if normalized_symbol:
        cooldown_until = normalized['symbol_cooldowns'].get(normalized_symbol)
        if cooldown_until and ts < int(cooldown_until):
            reasons.append('symbol_cooldown_active')
        daily_symbol_trade_limit = max(int(kwargs.get('daily_symbol_trade_limit', 0) or 0), 0)
        if daily_symbol_trade_limit > 0:
            current_trade_count = int(normalized['daily_symbol_trade_counts'].get(normalized_symbol, 0) or 0)
            if current_trade_count >= daily_symbol_trade_limit:
                reasons.append('daily_symbol_trade_limit_reached')
        aggressive_flip_cooldown_minutes = max(int(kwargs.get('aggressive_flip_cooldown_minutes', 0) or 0), 0)
        if aggressive_flip_cooldown_minutes > 0:
            cooldown_seconds = aggressive_flip_cooldown_minutes * 60
            for closed_trade in reversed(normalized['recent_closed_trades']):
                if not isinstance(closed_trade, dict):
                    continue
                closed_symbol = str(closed_trade.get('symbol') or '').strip().upper()
                if closed_symbol != normalized_symbol:
                    continue
                closed_side = _normalize_candidate_side(closed_trade.get('position_side') or closed_trade.get('side'))
                closed_at = closed_trade.get('closed_at')
                if closed_side != candidate_side and closed_at is not None and ts - int(closed_at) < cooldown_seconds:
                    reasons.append('aggressive_flip_cooldown_active')
                break

    if candidate is not None:
        state = getattr(candidate, 'state', '')
        must_pass_flags = getattr(candidate, 'must_pass_flags', None)
        if not isinstance(must_pass_flags, dict):
            must_pass_flags = {}

        effective_setup_ready = bool(must_pass_flags.get('setup_ready', getattr(candidate, 'setup_ready', False)))
        effective_trigger_fired = bool(must_pass_flags.get('trigger_fired', getattr(candidate, 'trigger_fired', False)))
        effective_probe_entry = bool(must_pass_flags.get('probe_entry', getattr(candidate, 'probe_entry', False)))
        effective_high_vol_alt_mode = bool(must_pass_flags.get('high_vol_alt_mode', getattr(candidate, 'high_vol_alt_mode', False)))

        if not effective_setup_ready:
            reasons.append('candidate_setup_not_ready')
        elif not effective_trigger_fired:
            if not effective_probe_entry:
                reasons.append('candidate_trigger_not_fired')
        trigger_min_confirmations = max(int(_to_float(getattr(candidate, 'trigger_min_confirmations', 0))), 0)
        trigger_confirmation_count = max(int(_to_float(getattr(candidate, 'trigger_confirmation_count', 0))), 0)
        trigger_confirmation_flags = getattr(candidate, 'trigger_confirmation_flags', None)
        trigger_confirmation_gate_active = trigger_confirmation_count > 0 or bool(trigger_confirmation_flags)
        if trigger_confirmation_gate_active and effective_trigger_fired and not effective_probe_entry and trigger_min_confirmations > 0 and trigger_confirmation_count < trigger_min_confirmations:
            reasons.append('candidate_trigger_confirmations_insufficient')
        execution_slippage_r = compute_expected_slippage_r(candidate)
        spread_bps = _to_float(getattr(candidate, 'spread_bps', 0.0))
        orderbook_slope = _to_float(getattr(candidate, 'orderbook_slope', 0.0))
        cancel_rate = _to_float(getattr(candidate, 'cancel_rate', 0.0))
        top_depth_usdt = _to_float(getattr(candidate, 'top_depth_usdt', 0.0))
        estimated_impact_pct = _to_float(getattr(candidate, 'estimated_impact_pct', getattr(candidate, 'orderbook_impact_pct', 0.0)))
        absolute_slippage_bps = max(_to_float(getattr(candidate, 'expected_slippage_pct', 0.0)), 0.0) * 100.0
        execution_liquidity_grade = classify_execution_liquidity_grade(
            getattr(candidate, 'book_depth_fill_ratio', 0.0),
            execution_slippage_r,
            spread_bps=spread_bps,
            orderbook_slope=orderbook_slope,
            cancel_rate=cancel_rate,
            top_depth_usdt=top_depth_usdt,
            estimated_impact_pct=estimated_impact_pct,
        )
        if state == 'distribution':
            reasons.append('candidate_distribution_risk')
        if _to_float(getattr(candidate, 'cvd_delta', 0.0)) < 0 and _to_float(getattr(candidate, 'cvd_zscore', 0.0)) <= -2.0:
            reasons.append('candidate_cvd_divergence')
        oi_hard_reversal_threshold = abs(_to_float(getattr(candidate, 'oi_hard_reversal_threshold_pct', 0.8), default=0.8))
        if _to_float(getattr(candidate, 'oi_change_pct_5m', 0.0)) <= -oi_hard_reversal_threshold:
            reasons.append('candidate_oi_reversal')
        risk_slippage_r = max(_to_float(getattr(candidate, 'execution_slippage_risk_threshold_r', 0.15), default=0.15), 0.0)
        hard_slippage_r = max(_to_float(getattr(candidate, 'execution_slippage_hard_veto_r', 0.25), default=0.25), risk_slippage_r)
        severe_absolute_slippage = absolute_slippage_bps > 25.0
        severe_book_impact = estimated_impact_pct >= 0.25
        severe_spread = spread_bps >= 18.0 and absolute_slippage_bps > 25.0
        severe_depth_gap = top_depth_usdt > 0 and top_depth_usdt < 10.0 and absolute_slippage_bps > 25.0
        if execution_slippage_r > hard_slippage_r and (severe_absolute_slippage or severe_book_impact or severe_spread or severe_depth_gap):
            reasons.append('candidate_execution_slippage_risk')
        has_explicit_edge_cost_contract = any(
            hasattr(candidate, field_name)
            for field_name in (
                'expected_edge',
                'expected_total_fee_pct',
                'execution_slippage_buffer_pct',
                'min_profit_buffer_pct',
            )
        )
        expected_edge = max(_to_float(getattr(candidate, 'expected_edge', 0.0)), 0.0)
        expected_total_fee_pct = max(_to_float(getattr(candidate, 'expected_total_fee_pct', 0.0)), 0.0)
        execution_slippage_buffer_pct = max(
            _to_float(
                getattr(candidate, 'execution_slippage_buffer_pct', getattr(candidate, 'expected_slippage_pct', 0.0)),
            ),
            0.0,
        )
        min_profit_buffer_pct = max(_to_float(getattr(candidate, 'min_profit_buffer_pct', 0.0)), 0.0)
        total_cost_floor_pct = expected_total_fee_pct + execution_slippage_buffer_pct + min_profit_buffer_pct
        if has_explicit_edge_cost_contract and total_cost_floor_pct > 0 and expected_edge <= total_cost_floor_pct:
            reasons.append('candidate_edge_after_costs_insufficient')
        liquidity_penalty_present = spread_bps > 0 or orderbook_slope > 0 or cancel_rate > 0
        explicit_liquidity_grade = str(getattr(candidate, 'liquidity_grade', '') or '').strip().upper()
        if execution_liquidity_grade == 'E':
            reasons.append('candidate_execution_liquidity_poor')
        elif execution_liquidity_grade == 'D' and explicit_liquidity_grade == 'C':
            reasons.append('candidate_execution_liquidity_poor')
        elif execution_liquidity_grade == 'B' and explicit_liquidity_grade != 'B' and (spread_bps >= 15.0 or cancel_rate >= 0.35):
            reasons.append('candidate_execution_liquidity_poor')
        elif execution_liquidity_grade == 'C' and explicit_liquidity_grade != 'B' and (
            _to_float(getattr(candidate, 'book_depth_fill_ratio', 0.0)) < 0.5 and (spread_bps >= 12.0 or top_depth_usdt < 10.0 or estimated_impact_pct >= 0.25)
        ):
            reasons.append('candidate_execution_liquidity_poor')
        breakout_level = _to_float(getattr(candidate, 'breakout_level', 0.0))
        last_price = _to_float(getattr(candidate, 'last_price', 0.0))
        trigger_fired = bool(getattr(candidate, 'trigger_fired', False))
        volume_multiple = _to_float(getattr(candidate, 'volume_multiple', 0.0))
        cvd_delta = _to_float(getattr(candidate, 'cvd_delta', 0.0))
        oi_change_5m = _to_float(getattr(candidate, 'oi_change_pct_5m', 0.0))
        fake_breakout_buffer_pct = max(_to_float(kwargs.get('fake_breakout_buffer_pct', 0.001)), 0.0)
        if trigger_fired and breakout_level > 0 and last_price > 0:
            has_flow_context = cvd_delta != 0 or oi_change_5m != 0
            if candidate_side == 'LONG':
                reclaimed = last_price >= breakout_level * (1.0 + fake_breakout_buffer_pct)
                flow_confirmed = (cvd_delta > 0 and oi_change_5m >= 0 and volume_multiple >= 1.0) if has_flow_context else True
            else:
                reclaimed = last_price <= breakout_level * (1.0 - fake_breakout_buffer_pct)
                flow_confirmed = (cvd_delta < 0 and oi_change_5m >= 0 and volume_multiple >= 1.0) if has_flow_context else True
            if not reclaimed or not flow_confirmed:
                reasons.append('candidate_fake_breakout_risk')
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
        dynamic_risk_multiplier = max(_to_float(kwargs.get('dynamic_risk_multiplier', getattr(candidate, 'dynamic_risk_multiplier', normalized.get('dynamic_risk_multiplier', 1.0))), default=1.0), 0.0)
        if dynamic_risk_multiplier > 0:
            gross_heat_cap_r *= dynamic_risk_multiplier
            same_theme_heat_cap_r *= dynamic_risk_multiplier
            same_correlation_heat_cap_r *= dynamic_risk_multiplier
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
