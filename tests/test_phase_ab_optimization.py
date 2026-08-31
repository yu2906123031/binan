import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def test_attribution_analysis_measures_incremental_stage_value():
    mod = importlib.import_module('attribution_analysis')
    scan_id = 'scan-1'
    events = [
        {
            'event_type': 'layer_attribution_scan',
            'scan_id': scan_id,
            'stages': [
                {'stage': 'raw', 'winner': {'symbol': 'A', 'side': 'LONG'}},
                {'stage': 'outcome_calibration', 'winner': {'symbol': 'B', 'side': 'LONG'}},
            ],
        },
        {'event_type': 'trade_closed', 'layer_attribution_scan_id': scan_id, 'symbol': 'A', 'side': 'LONG', 'realized_r': -0.5},
        {'event_type': 'trade_closed', 'layer_attribution_scan_id': scan_id, 'symbol': 'B', 'side': 'LONG', 'realized_r': 1.0},
    ]
    payload = mod.build_layer_attribution_analysis(events)
    assert payload['evaluated_scan_count'] == 1
    transition = payload['transitions'][0]
    assert transition['avg_incremental_net_r'] == 1.5
    assert transition['positive_contribution_rate'] == 1.0


def test_hierarchical_cost_uses_fine_bucket_then_fallback():
    mod = importlib.import_module('hierarchical_cost_policy')
    rows = []
    for idx in range(25):
        rows.append({
            'event_type': 'entry_filled',
            'recorded_at': f'2026-08-{(idx % 20) + 1:02d}T14:00:00Z',
            'actual_fill_slippage_bps': 4 + idx % 3,
            'liquidity_grade': 'A',
            'market_regime_label': 'TREND',
            'maker_or_taker': 'TAKER',
            'planned_notional_usdt': 250,
        })
    model = mod.build_hierarchical_cost_model(rows)
    candidate = SimpleNamespace(
        liquidity_grade='A', market_regime_label='TREND', maker_or_taker='TAKER',
        planned_notional_usdt=250, decision_at='2026-08-31T14:00:00Z', expected_slippage_pct=0.04,
    )
    resolved = mod.resolve_hierarchical_cost(candidate, model, current_bps=4.0)
    assert resolved['source'] == 'fine'
    assert resolved['sample_count'] == 25
    assert resolved['estimated_adverse_slippage_bps'] >= 4.0


def test_edge_sizing_is_bounded_and_can_disable_negative_edge():
    mod = importlib.import_module('edge_position_sizing')
    weak = SimpleNamespace(realizable_edge_margin_r=-0.1, liquidity_grade='A', book_depth_fill_ratio=1.0, relative_selection_percentile=1.0)
    strong = SimpleNamespace(realizable_edge_margin_r=1.2, liquidity_grade='A', book_depth_fill_ratio=1.0, relative_selection_percentile=0.9)
    assert mod.compute_edge_size_multiplier(weak)['multiplier'] == 0.0
    assert 1.0 < mod.compute_edge_size_multiplier(strong)['multiplier'] <= 1.10


def test_portfolio_correlation_reduces_duplicate_dynamic_exposure():
    mod = importlib.import_module('portfolio_correlation_policy')
    candidate = SimpleNamespace(correlation_group='L1', recent_returns=[1, 2, 3, 4, 5, 6, 7, 8])
    positions = [
        {'correlation_group': 'L1', 'recent_returns': [1, 2, 3, 4, 5, 6, 7, 8]},
    ]
    risk = mod.evaluate_portfolio_incremental_risk(candidate, positions)
    assert risk['same_group_open_count'] == 1
    assert risk['max_positive_correlation'] > 0.99
    assert risk['size_multiplier'] <= 0.5


def test_exit_state_machine_time_stop_and_runner_decay():
    mod = importlib.import_module('exit_state_machine')
    no_follow = mod.evaluate_exit_state(
        {'exit_state': 'INITIAL_RISK', 'mfe_r': 0.1, 'mae_r': -0.1, 'time_in_trade_minutes': 31},
        {'structure_valid': True, 'trigger_valid': True, 'momentum_alive': True, 'htf_aligned': True},
    )
    assert no_follow['action'] == 'close'
    runner = mod.evaluate_exit_state(
        {'exit_state': 'RUNNER', 'mfe_r': 2.0, 'time_in_trade_minutes': 20},
        {'structure_valid': True, 'trigger_valid': True, 'momentum_alive': False, 'htf_aligned': True},
    )
    assert runner['action'] == 'reduce'


def test_research_gate_requires_samples_robustness_and_side_effect_control():
    mod = importlib.import_module('research_gate')
    approved = mod.evaluate_research_gate({
        'sample_count': 80,
        'oos_net_expectancy_delta_r': 0.08,
        'max_drawdown_delta_r': -0.1,
        'adverse_selection_delta': -0.02,
        'capacity_delta': 0.1,
        'turnover_delta_pct': 5,
        'tail_loss_delta_r': -0.01,
        'cost_stress_expectancy_delta_r': 0.02,
        'latency_stress_expectancy_delta_r': 0.01,
    })
    assert approved['approved_for_live'] is True
    rejected = mod.evaluate_research_gate({'sample_count': 10, 'oos_net_expectancy_delta_r': 1.0})
    assert rejected['approved_for_live'] is False


def test_execution_router_counts_missed_alpha_not_just_slippage():
    mod = importlib.import_module('execution_router_shadow')
    result = mod.evaluate_execution_routes({
        'market': {'sample_count': 40, 'slippage_bps': 6, 'fill_ratio': 1.0, 'missed_alpha_bps': 0},
        'passive_limit': {'sample_count': 40, 'slippage_bps': 1, 'fill_ratio': 0.4, 'missed_alpha_bps': 20},
    })
    assert result['shadow_winner']['route'] == 'market'
    assert result['live_route_change_allowed'] is False


def test_decision_optimizer_never_auto_increases_live_notional():
    mod = importlib.import_module('decision_risk_optimization')
    candidate = SimpleNamespace(
        symbol='XUSDT', side='LONG', score=100, realizable_edge_margin_r=1.2,
        liquidity_grade='A', book_depth_fill_ratio=1.0, relative_selection_percentile=0.9,
        planned_notional_usdt=1000.0, expected_slippage_pct=0.03,
    )
    mod.apply_conservative_decision_risk(candidate, cost_model={}, open_positions=[])
    assert candidate.optimized_size_multiplier_recommended >= 1.0
    assert candidate.optimized_size_multiplier_live == 1.0
    assert candidate.planned_notional_usdt == 1000.0
    assert candidate.optimized_size_increase_shadow_only is True
