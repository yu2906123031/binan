import importlib.util
import sys
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'binance_futures_momentum_long.py'


def load_module():
    spec = importlib.util.spec_from_file_location('binance_futures_momentum_long', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def make_candidate(symbol: str = 'DOGEUSDT', side: str = 'LONG', usdt_size: float = 300.0):
    return type(
        'Candidate',
        (),
        {
            'symbol': symbol,
            'side': side,
            'usdt_size': usdt_size,
            'notional': usdt_size,
            'planned_notional': usdt_size,
        },
    )()


def test_evaluate_portfolio_risk_guards_blocks_long_count_limit():
    mod = load_module()
    candidate = make_candidate(side='LONG', usdt_size=120.0)
    open_positions = [
        {'symbol': 'BTCUSDT', 'side': 'LONG', 'notional': 100.0},
        {'symbol': 'ETHUSDT', 'side': 'LONG', 'notional': 80.0},
    ]
    guard = mod.evaluate_portfolio_risk_guards(
        open_positions=open_positions,
        candidate=candidate,
        max_long_positions=2,
    )
    assert guard['allowed'] is False
    assert 'max_long_positions_reached' in guard['reasons']


def test_evaluate_portfolio_risk_guards_blocks_gross_exposure_limit():
    mod = load_module()
    candidate = make_candidate(side='LONG', usdt_size=150.0)
    open_positions = [
        {'symbol': 'BTCUSDT', 'side': 'LONG', 'notional': 200.0},
        {'symbol': 'ETHUSDT', 'side': 'SHORT', 'notional': 100.0},
    ]
    guard = mod.evaluate_portfolio_risk_guards(
        open_positions=open_positions,
        candidate=candidate,
        max_gross_exposure_usdt=400.0,
    )
    assert guard['allowed'] is False
    assert 'max_gross_exposure_reached' in guard['reasons']


def test_evaluate_portfolio_risk_guards_blocks_symbol_hedge_when_single_side_only():
    mod = load_module()
    candidate = make_candidate(symbol='DOGEUSDT', side='SHORT', usdt_size=50.0)
    open_positions = [
        {'symbol': 'DOGEUSDT', 'side': 'LONG', 'notional': 75.0},
    ]
    guard = mod.evaluate_portfolio_risk_guards(
        open_positions=open_positions,
        candidate=candidate,
        per_symbol_single_side_only=True,
    )
    assert guard['allowed'] is False
    assert 'per_symbol_single_side_only_violation' in guard['reasons']


def test_evaluate_risk_guards_blocks_theme_and_correlation_overexposure():
    mod = load_module()
    candidate = type(
        'Candidate',
        (),
        {
            'symbol': 'DOGEUSDT',
            'state': 'launch',
            'position_size_pct': 2.5,
            'book_depth_fill_ratio': 0.9,
            'expected_slippage_pct': 0.02,
            'cvd_delta': 1.0,
            'cvd_zscore': 0.5,
            'oi_change_pct_5m': 1.0,
        },
    )()
    guard = mod.evaluate_risk_guards(
        symbol='DOGEUSDT',
        risk_state={
            'portfolio_exposure_pct_by_theme': {'meme': 8.0},
            'portfolio_exposure_pct_by_correlation': {'dog-beta': 6.0},
        },
        candidate=candidate,
        portfolio_narrative_bucket='meme',
        portfolio_correlation_group='dog-beta',
        max_portfolio_exposure_pct_per_theme=10.0,
        max_portfolio_exposure_pct_per_correlation_group=8.0,
    )
    assert guard['allowed'] is False
    assert 'candidate_portfolio_theme_overexposure' in guard['reasons']
    assert 'candidate_portfolio_correlation_overexposure' in guard['reasons']


def test_evaluate_portfolio_risk_guards_allows_when_within_limits():
    mod = load_module()
    candidate = make_candidate(symbol='SUIUSDT', side='SHORT', usdt_size=40.0)
    open_positions = [
        {'symbol': 'DOGEUSDT', 'side': 'LONG', 'notional': 110.0},
    ]
    guard = mod.evaluate_portfolio_risk_guards(
        open_positions=open_positions,
        candidate=candidate,
        max_short_positions=2,
        max_net_exposure_usdt=80.0,
        max_gross_exposure_usdt=250.0,
        per_symbol_single_side_only=True,
    )
    assert guard['allowed'] is True
    assert guard['reasons'] == []
    assert guard['snapshot']['gross_exposure_usdt'] == 110.0
    assert guard['snapshot']['candidate_notional_usdt'] == 40.0
    assert guard['snapshot']['net_exposure_usdt'] == 110.0
