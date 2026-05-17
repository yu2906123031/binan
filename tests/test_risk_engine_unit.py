import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace


MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'risk_engine.py'
SCRIPTS_DIR = MODULE_PATH.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

MAIN_SCRIPT_PATH = SCRIPTS_DIR / 'binance_futures_momentum_long.py'
main_spec = importlib.util.spec_from_file_location('binance_futures_momentum_long', MAIN_SCRIPT_PATH)
assert main_spec is not None
main_mod = importlib.util.module_from_spec(main_spec)
sys.modules[main_spec.name] = main_mod
assert main_spec.loader is not None
main_spec.loader.exec_module(main_mod)

spec = importlib.util.spec_from_file_location('risk_engine', MODULE_PATH)
assert spec is not None
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)


POSITION_SIDE_LONG = 'LONG'
POSITION_SIDE_SHORT = 'SHORT'


def _to_float(value, default=0.0):
    try:
        if value in (None, ''):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def default_risk_state():
    return main_mod.default_risk_state()


def compute_expected_slippage_r(candidate):
    return main_mod.compute_expected_slippage_r(candidate)


def classify_execution_liquidity_grade(book_depth_fill_ratio, expected_slippage_r, spread_bps=0.0, orderbook_slope=0.0, cancel_rate=0.0):
    return main_mod.classify_execution_liquidity_grade(
        book_depth_fill_ratio,
        expected_slippage_r,
        spread_bps=spread_bps,
        orderbook_slope=orderbook_slope,
        cancel_rate=cancel_rate,
    )


def estimate_candidate_heat_r(candidate, base_risk_usdt=0.0):
    return main_mod.estimate_candidate_heat_r(candidate, base_risk_usdt=base_risk_usdt)


def normalize_position_side(side, default=POSITION_SIDE_LONG):
    return main_mod.normalize_position_side(side, default=default)


def build_position_exposure_snapshot(open_positions):
    return main_mod.build_position_exposure_snapshot(open_positions)


def make_candidate(**overrides):
    base = {
        'symbol': 'DOGEUSDT',
        'state': 'launch',
        'setup_ready': True,
        'trigger_fired': True,
        'position_size_pct': 0.0,
        'book_depth_fill_ratio': 0.9,
        'expected_slippage_pct': 0.1,
        'risk_per_unit': 2.0,
        'quantity': 10.0,
        'cvd_delta': 50_000.0,
        'cvd_zscore': 1.2,
        'oi_change_pct_5m': 0.4,
        'oi_hard_reversal_threshold_pct': 0.8,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def evaluate_risk_guards(**kwargs):
    return mod.evaluate_risk_guards(
        default_risk_state=default_risk_state,
        _to_float=_to_float,
        compute_expected_slippage_r=compute_expected_slippage_r,
        classify_execution_liquidity_grade=classify_execution_liquidity_grade,
        estimate_candidate_heat_r=estimate_candidate_heat_r,
        time_module=main_mod.time,
        **kwargs,
    )


def evaluate_portfolio_risk_guards(**kwargs):
    return mod.evaluate_portfolio_risk_guards(
        build_position_exposure_snapshot=build_position_exposure_snapshot,
        normalize_position_side=normalize_position_side,
        position_side_long=POSITION_SIDE_LONG,
        position_side_short=POSITION_SIDE_SHORT,
        _to_float=_to_float,
        **kwargs,
    )


def test_evaluate_risk_guards_normalizes_malformed_state_and_keeps_candidate_allowed():
    candidate = make_candidate()

    payload = evaluate_risk_guards(
        symbol='DOGEUSDT',
        risk_state={
            'symbol_cooldowns': [],
            'portfolio_exposure_pct_by_theme': 'broken',
            'portfolio_exposure_pct_by_correlation': 1,
            'portfolio_heat_r_by_theme': None,
            'portfolio_heat_r_by_correlation': 'bad',
        },
        candidate=candidate,
    )

    assert payload['allowed'] is True
    assert payload['reasons'] == []
    assert payload['normalized_risk_state']['symbol_cooldowns'] == {}
    assert payload['normalized_risk_state']['portfolio_exposure_pct_by_theme'] == {}
    assert payload['normalized_risk_state']['portfolio_exposure_pct_by_correlation'] == {}
    assert payload['normalized_risk_state']['portfolio_heat_r_by_theme'] == {}
    assert payload['normalized_risk_state']['portfolio_heat_r_by_correlation'] == {}


def test_evaluate_risk_guards_blocks_liquidity_grade_c_when_depth_is_too_thin():
    candidate = make_candidate(
        risk_per_unit=1.0,
        expected_slippage_pct=0.25,
        book_depth_fill_ratio=0.49,
        execution_slippage_risk_threshold_r=0.3,
    )

    payload = evaluate_risk_guards(symbol='THINUSDT', risk_state=default_risk_state(), candidate=candidate)

    assert payload['allowed'] is False
    assert payload['reasons'] == ['candidate_execution_liquidity_poor']


def test_evaluate_risk_guards_allows_grade_c_when_depth_threshold_is_met():
    candidate = make_candidate(
        risk_per_unit=1.5,
        expected_slippage_pct=0.18,
        book_depth_fill_ratio=0.5,
    )

    payload = evaluate_risk_guards(symbol='CAUTIONUSDT', risk_state=default_risk_state(), candidate=candidate)

    assert payload['allowed'] is True
    assert payload['reasons'] == []


def test_evaluate_risk_guards_blocks_same_bucket_heat_caps_using_dynamic_thresholds():
    candidate = make_candidate(
        position_size_pct=1.4,
        quantity=12.0,
        risk_per_unit=2.0,
        portfolio_narrative_bucket='meme',
        portfolio_correlation_group='dog-beta',
    )

    payload = evaluate_risk_guards(
        symbol='DOGEUSDT',
        candidate=candidate,
        risk_state={
            'portfolio_heat_open_r': 0.8,
            'portfolio_heat_pending_r': 0.3,
            'portfolio_heat_r_by_theme': {'meme': 0.7},
            'portfolio_heat_r_by_correlation': {'dog-beta': 0.6},
        },
        base_risk_usdt=10.0,
        gross_heat_cap_r=3.0,
        same_theme_heat_cap_r=3.0,
        same_correlation_heat_cap_r=3.0,
    )

    assert payload['allowed'] is False
    assert payload['reasons'] == [
        'candidate_portfolio_heat_overexposure',
        'candidate_same_theme_heat_overexposure',
        'candidate_same_correlation_heat_overexposure',
    ]


def test_evaluate_risk_guards_uses_must_pass_flags_for_setup_and_trigger_gates():
    candidate = make_candidate(
        setup_ready=True,
        trigger_fired=True,
        must_pass_flags={'setup_ready': False, 'trigger_fired': False},
    )

    payload = evaluate_risk_guards(symbol='DOGEUSDT', risk_state=default_risk_state(), candidate=candidate)

    assert payload['allowed'] is False
    assert payload['reasons'] == ['candidate_setup_not_ready']


def test_evaluate_risk_guards_uses_must_pass_flags_probe_entry_override_for_trigger_gate():
    candidate = make_candidate(
        setup_ready=True,
        trigger_fired=True,
        probe_entry=False,
        must_pass_flags={'setup_ready': True, 'trigger_fired': False, 'probe_entry': True},
    )

    payload = evaluate_risk_guards(symbol='DOGEUSDT', risk_state=default_risk_state(), candidate=candidate)

    assert payload['allowed'] is True
    assert payload['reasons'] == []


def test_evaluate_portfolio_risk_guards_blocks_short_count_and_net_exposure_from_fallback_notional():
    candidate = SimpleNamespace(symbol='SUIUSDT', side='SHORT', entry_price=50.0, quantity=2.0)
    open_positions = [
        {'symbol': 'BTCUSDT', 'side': 'LONG', 'notional': 120.0},
        {'symbol': 'ETHUSDT', 'side': 'SHORT', 'notional': 80.0},
    ]

    payload = evaluate_portfolio_risk_guards(
        open_positions=open_positions,
        candidate=candidate,
        max_short_positions=1,
        max_net_exposure_usdt=10.0,
    )

    assert payload['allowed'] is False
    assert payload['reasons'] == ['max_short_positions_reached', 'max_net_exposure_reached']
    assert payload['snapshot']['candidate_notional_usdt'] == 100.0
    assert payload['snapshot']['candidate_side'] == POSITION_SIDE_SHORT


def test_evaluate_portfolio_risk_guards_marks_flip_cooldown_when_single_side_guard_is_enabled():
    candidate = SimpleNamespace(symbol='DOGEUSDT', side='SHORT', notional=40.0)
    open_positions = [
        {'symbol': 'DOGEUSDT', 'positionSide': 'LONG', 'notional': 75.0},
    ]

    payload = evaluate_portfolio_risk_guards(
        open_positions=open_positions,
        candidate=candidate,
        per_symbol_single_side_only=True,
        opposite_side_flip_cooldown_minutes=15,
    )

    assert payload['allowed'] is False
    assert payload['reasons'] == [
        'per_symbol_single_side_only_violation',
        'opposite_side_flip_cooldown_active',
    ]
    assert payload['snapshot']['symbol_sides']['DOGEUSDT'] == [POSITION_SIDE_LONG]



def test_evaluate_risk_guards_uses_orderbook_penalties_in_execution_liquidity_grade():
    candidate = make_candidate(
        risk_per_unit=1.0,
        expected_slippage_pct=0.05,
        book_depth_fill_ratio=0.9,
        spread_bps=18.0,
        orderbook_slope=0.2,
        cancel_rate=0.4,
    )

    payload = evaluate_risk_guards(symbol='DOGEUSDT', risk_state=default_risk_state(), candidate=candidate)

    assert payload['allowed'] is False
    assert payload['reasons'] == ['candidate_execution_liquidity_poor']



def test_evaluate_portfolio_risk_guards_prefers_candidate_position_side_for_short_limits():
    candidate = SimpleNamespace(symbol='SUIUSDT', position_side='SHORT', entry_price=50.0, quantity=2.0)
    open_positions = [
        {'symbol': 'ETHUSDT', 'positionSide': 'SHORT', 'notional': 80.0},
    ]

    payload = evaluate_portfolio_risk_guards(
        open_positions=open_positions,
        candidate=candidate,
        max_short_positions=1,
    )

    assert payload['allowed'] is False
    assert payload['reasons'] == ['max_short_positions_reached']
    assert payload['snapshot']['candidate_side'] == POSITION_SIDE_SHORT



def test_evaluate_risk_guards_blocks_trigger_until_min_confirmations_are_met():
    candidate = make_candidate(
        setup_ready=True,
        trigger_fired=True,
        trigger_confirmation_count=1,
        trigger_min_confirmations=2,
    )

    payload = evaluate_risk_guards(symbol='DOGEUSDT', risk_state=default_risk_state(), candidate=candidate)

    assert payload['allowed'] is False
    assert 'candidate_trigger_confirmations_insufficient' in payload['reasons']



def test_evaluate_risk_guards_blocks_entries_outside_allowed_utc_hours():
    candidate = make_candidate(expected_edge=1.0, expected_total_fee_pct=0.02, execution_slippage_buffer_pct=0.02, min_profit_buffer_pct=0.02)

    payload = evaluate_risk_guards(
        symbol='DOGEUSDT',
        risk_state=default_risk_state(),
        candidate=candidate,
        now_ts=1_710_000_000,  # 2024-03-09 16:00:00 UTC
        allowed_session_utc_hours=[1, 2, 3],
    )

    assert payload['allowed'] is False
    assert 'session_filter_blocked' in payload['reasons']



def test_evaluate_risk_guards_scales_heat_caps_with_dynamic_risk_multiplier():
    candidate = make_candidate(
        position_size_pct=1.0,
        quantity=10.0,
        risk_per_unit=1.0,
        expected_edge=1.0,
        expected_total_fee_pct=0.02,
        execution_slippage_buffer_pct=0.02,
        min_profit_buffer_pct=0.02,
        dynamic_risk_multiplier=0.5,
    )

    payload = evaluate_risk_guards(
        symbol='DOGEUSDT',
        risk_state={'portfolio_heat_open_r': 1.2},
        candidate=candidate,
        base_risk_usdt=10.0,
        gross_heat_cap_r=3.0,
    )

    assert payload['allowed'] is False
    assert 'candidate_portfolio_heat_overexposure' in payload['reasons']



def test_evaluate_risk_guards_blocks_daily_symbol_trade_limit():

    candidate = make_candidate(
        expected_edge=1.0,
        expected_total_fee_pct=0.02,
        execution_slippage_buffer_pct=0.02,
        min_profit_buffer_pct=0.02,
    )
    risk_state = default_risk_state()
    risk_state['daily_symbol_trade_counts'] = {'DOGEUSDT': 3}

    payload = evaluate_risk_guards(
        symbol='DOGEUSDT',
        risk_state=risk_state,
        candidate=candidate,
        daily_symbol_trade_limit=3,
    )

    assert payload['allowed'] is False
    assert 'daily_symbol_trade_limit_reached' in payload['reasons']



def test_evaluate_risk_guards_blocks_fee_aware_edge_shortfall_when_cost_floor_exceeds_edge():
    candidate = make_candidate(
        expected_edge=0.05,
        expected_total_fee_pct=0.02,
        execution_slippage_buffer_pct=0.02,
        min_profit_buffer_pct=0.02,
    )

    payload = evaluate_risk_guards(
        symbol='DOGEUSDT',
        risk_state=default_risk_state(),
        candidate=candidate,
    )

    assert payload['allowed'] is False
    assert 'candidate_edge_after_costs_insufficient' in payload['reasons']



def test_evaluate_risk_guards_blocks_aggressive_flip_reentry_inside_cooldown_window():
    candidate = make_candidate(
        side='SHORT',
        expected_edge=1.0,
        expected_total_fee_pct=0.02,
        execution_slippage_buffer_pct=0.02,
        min_profit_buffer_pct=0.02,
    )
    risk_state = default_risk_state()
    risk_state['recent_closed_trades'] = [
        {
            'symbol': 'DOGEUSDT',
            'position_side': 'LONG',
            'closed_at': 1_710_000_000,
        }
    ]

    payload = evaluate_risk_guards(
        symbol='DOGEUSDT',
        risk_state=risk_state,
        candidate=candidate,
        now_ts=1_710_000_000 + 60,
        aggressive_flip_cooldown_minutes=5,
    )

    assert payload['allowed'] is False
    assert 'aggressive_flip_cooldown_active' in payload['reasons']
