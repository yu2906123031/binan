from types import SimpleNamespace

from scripts.risk_engine import evaluate_portfolio_risk_guards, evaluate_risk_guards


def _to_float(value, default=0.0):
    try:
        if value in (None, ''):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _risk_state(**overrides):
    state = {
        'halted': False,
        'daily_realized_pnl_usdt': 0.0,
        'consecutive_losses': 0,
        'symbol_cooldowns': {},
        'portfolio_exposure_pct_by_theme': {},
        'portfolio_exposure_pct_by_correlation': {},
        'portfolio_heat_r_by_theme': {},
        'portfolio_heat_r_by_correlation': {},
        'daily_symbol_trade_counts': {},
        'daily_symbol_loss_usdt': {},
        'rolling_symbol_loss_usdt': {},
        'symbol_consecutive_losses': {},
        'recent_symbol_entries': {},
        'limit_cancel_replace_count_by_symbol': {},
        'loss_deweighted_symbols': {},
        'recent_closed_trades': [],
    }
    state.update(overrides)
    return state


def _candidate(**overrides):
    payload = dict(
        symbol='XYZUSDT',
        side='LONG',
        position_side='LONG',
        state='launch',
        score=100.0,
        setup_ready=True,
        trigger_fired=True,
        must_pass_flags={'setup_ready': True, 'trigger_fired': True},
        trigger_confirmation_count=0,
        trigger_min_confirmations=0,
        trigger_confirmation_flags={},
        book_depth_fill_ratio=1.0,
        expected_slippage_pct=0.0,
        spread_bps=0.0,
        orderbook_slope=0.0,
        cancel_rate=0.0,
        top_depth_usdt=10000.0,
        estimated_impact_pct=0.0,
        oi_change_pct_5m=0.0,
        cvd_delta=0.0,
        cvd_zscore=0.0,
        position_size_pct=0.0,
    )
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _evaluate(symbol='XYZUSDT', state=None, candidate=None, **kwargs):
    return evaluate_risk_guards(
        symbol=symbol,
        risk_state=state or _risk_state(),
        candidate=candidate,
        now_ts=1_800_000_000,
        default_risk_state=lambda: _risk_state(),
        _to_float=_to_float,
        compute_expected_slippage_r=lambda _candidate: 0.0,
        classify_execution_liquidity_grade=lambda *args, **kwargs: 'A',
        estimate_candidate_heat_r=lambda _candidate, base_risk_usdt=0.0: 0.0,
        time_module=SimpleNamespace(time=lambda: 1_800_000_000),
        **kwargs,
    )


def test_invalid_session_configuration_fails_closed_for_entries():
    result = _evaluate(candidate=None, allowed_session_utc_hours=['8', 'bad'])
    assert 'session_filter_config_invalid' in result['reasons']
    assert result['allowed'] is False


def test_reduce_only_is_not_blocked_by_entry_session_filter():
    result = _evaluate(candidate=None, allowed_session_utc_hours=['bad'], reduce_only=True)
    assert 'session_filter_config_invalid' not in result['reasons']


def test_cancel_replace_limit_blocks_at_configured_limit_not_one_later():
    state = _risk_state(limit_cancel_replace_count_by_symbol={'XYZUSDT': 2})
    result = _evaluate(state=state, candidate=None, max_limit_cancel_replace_per_symbol=2)
    assert 'symbol_limit_cancel_replace_limit_reached' in result['reasons']


def test_any_losing_symbol_is_temporarily_deweighted_without_name_hardcoding():
    state = _risk_state(rolling_symbol_loss_usdt={'XYZUSDT': -0.5})
    result = _evaluate(state=state, candidate=_candidate(), score_threshold=95.0, after_symbol_loss_cooldown_minutes=60)
    assert 'symbol_loss_deweighted_score_below_threshold' in result['reasons']
    payload = result['normalized_risk_state']['loss_deweighted_symbols']['XYZUSDT']
    assert payload['rolling_loss_usdt'] == -0.5


def test_loss_deweight_recovers_immediately_when_rolling_loss_recovers():
    state = _risk_state(
        rolling_symbol_loss_usdt={'XYZUSDT': 0.25},
        loss_deweighted_symbols={'XYZUSDT': {'cooldown_until': 1_900_000_000, 'score_penalty': 12.0}},
    )
    result = _evaluate(state=state, candidate=_candidate(), score_threshold=95.0)
    assert 'symbol_loss_deweighted_score_below_threshold' not in result['reasons']
    assert 'XYZUSDT' not in result['normalized_risk_state']['loss_deweighted_symbols']


def test_symbol_cooldown_minutes_is_used_as_default_loss_cooldown():
    state = _risk_state(rolling_symbol_loss_usdt={'XYZUSDT': -0.5})
    result = evaluate_risk_guards(
        symbol='XYZUSDT',
        risk_state=state,
        candidate=_candidate(),
        now_ts=1_800_000_000,
        symbol_cooldown_minutes=7,
        default_risk_state=lambda: _risk_state(),
        _to_float=_to_float,
        compute_expected_slippage_r=lambda _candidate: 0.0,
        classify_execution_liquidity_grade=lambda *args, **kwargs: 'A',
        estimate_candidate_heat_r=lambda _candidate, base_risk_usdt=0.0: 0.0,
        time_module=SimpleNamespace(time=lambda: 1_800_000_000),
        score_threshold=95.0,
    )
    assert result['normalized_risk_state']['loss_deweighted_symbols']['XYZUSDT']['cooldown_until'] == 1_800_000_000 + 7 * 60


def _snapshot(_positions):
    return {
        'long_count': 0,
        'short_count': 0,
        'net_exposure_usdt': 0.0,
        'gross_exposure_usdt': 0.0,
        'symbol_sides': {},
    }


def _normalize_side(value):
    return 'SHORT' if str(value).upper() in {'SHORT', 'SELL'} else 'LONG'


def test_portfolio_guard_reads_planned_notional_usdt_contract():
    candidate = SimpleNamespace(symbol='XYZUSDT', side='LONG', planned_notional_usdt=100.0, quantity=0.0)
    result = evaluate_portfolio_risk_guards(
        [], candidate, max_gross_exposure_usdt=50.0,
        build_position_exposure_snapshot=_snapshot,
        normalize_position_side=_normalize_side,
        position_side_long='LONG', position_side_short='SHORT', _to_float=_to_float,
    )
    assert result['snapshot']['candidate_notional_usdt'] == 100.0
    assert 'max_gross_exposure_reached' in result['reasons']


def test_portfolio_flip_cooldown_parameter_is_effective():
    candidate = SimpleNamespace(
        symbol='XYZUSDT', side='LONG', planned_notional_usdt=10.0,
        last_closed_symbol='XYZUSDT', last_closed_position_side='SHORT',
        last_closed_at=1_800_000_000 - 60, risk_now_ts=1_800_000_000,
    )
    result = evaluate_portfolio_risk_guards(
        [], candidate, opposite_side_flip_cooldown_minutes=5,
        build_position_exposure_snapshot=_snapshot,
        normalize_position_side=_normalize_side,
        position_side_long='LONG', position_side_short='SHORT', _to_float=_to_float,
    )
    assert 'opposite_side_flip_cooldown_active' in result['reasons']
