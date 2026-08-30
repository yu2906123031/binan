import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_module():
    spec = importlib.util.spec_from_file_location('market_direction_bias_test', SCRIPTS_DIR / 'market_direction_bias.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def tickers(changes):
    return [
        {'symbol': f'COIN{i}USDT', 'priceChangePercent': str(change), 'quoteVolume': '10000000'}
        for i, change in enumerate(changes)
    ]


def test_broad_rising_market_produces_long_bias():
    mod = load_module()
    result = mod.compute_market_direction_bias(
        tickers([5, 4, 3, 2, 1, 0.5, -0.2, -0.5]),
        {'structural_label': 'BULL_TREND'},
    )
    assert result['bias'] == 'LONG'
    assert result['breadth_ratio'] > 0.6
    assert result['weighted_breadth_ratio'] > 0.6
    assert result['median_change_pct'] > 0
    assert 0 < result['strength'] <= 1


def test_broad_falling_market_produces_short_bias():
    mod = load_module()
    result = mod.compute_market_direction_bias(
        tickers([-5, -4, -3, -2, -1, -0.5, 0.2, 0.5]),
        {'structural_label': 'BEAR_TREND'},
    )
    assert result['bias'] == 'SHORT'
    assert result['breadth_ratio'] < 0.4
    assert result['weighted_breadth_ratio'] < 0.4
    assert result['median_change_pct'] < 0


def test_mixed_or_insufficient_market_is_neutral():
    mod = load_module()
    assert mod.compute_market_direction_bias(tickers([2, 1, -1, -2]), {'structural_label': 'RANGE'})['bias'] == 'NEUTRAL'
    assert mod.compute_market_direction_bias(tickers([2, 1]), {'structural_label': 'BULL_TREND'})['bias'] == 'NEUTRAL'


def test_low_liquidity_alt_noise_is_ignored():
    mod = load_module()
    rows = [
        {'symbol': f'TINY{i}USDT', 'priceChangePercent': '15', 'quoteVolume': '10000'}
        for i in range(20)
    ]
    rows += [
        {'symbol': f'LIQUID{i}USDT', 'priceChangePercent': '-3', 'quoteVolume': '50000000'}
        for i in range(6)
    ]
    result = mod.compute_market_direction_bias(rows, {'structural_label': 'BEAR_TREND'})
    assert result['ignored_low_liquidity'] == 20
    assert result['sample_size'] == 6
    assert result['bias'] == 'SHORT'
    assert result['median_change_pct'] < 0


def test_single_huge_contract_cannot_overrule_broad_market():
    mod = load_module()
    rows = [
        {'symbol': 'MEGAUSDT', 'priceChangePercent': '8', 'quoteVolume': '100000000000'},
    ]
    rows += [
        {'symbol': f'COIN{i}USDT', 'priceChangePercent': '-2', 'quoteVolume': '10000000'}
        for i in range(9)
    ]
    result = mod.compute_market_direction_bias(rows, {'structural_label': 'BEAR_TREND'})
    assert result['bias'] == 'SHORT'
    assert result['breadth_ratio'] < 0.45
    assert result['weighted_breadth_ratio'] < 0.40


def test_regime_conflict_keeps_bias_neutral_even_with_breadth():
    mod = load_module()
    result = mod.compute_market_direction_bias(
        tickers([5, 4, 3, 2, 1, 0.5, -0.2, -0.5]),
        {'structural_label': 'BEAR_TREND'},
    )
    assert result['bias'] == 'NEUTRAL'


def test_bias_is_soft_observable_and_preserves_both_sides():
    mod = load_module()
    long = SimpleNamespace(side='LONG', score=80.0, reasons=[])
    short = SimpleNamespace(side='SHORT', score=80.0, reasons=[])
    payload = {
        'bias': 'LONG',
        'strength': 0.8,
        'breadth_ratio': 0.75,
        'weighted_breadth_ratio': 0.72,
        'median_change_pct': 1.2,
        'sample_size': 100,
    }

    mod.apply_market_direction_bias(long, payload)
    mod.apply_market_direction_bias(short, payload)

    assert long.score > 80.0
    assert 75.0 < short.score < 80.0
    assert long.market_direction_bias == short.market_direction_bias == 'LONG'
    assert long.market_weighted_breadth_ratio == 0.72
    assert long.market_median_change_pct == 1.2
    assert any('market_direction_bias=LONG' in reason for reason in short.reasons)


def test_neutral_bias_does_not_change_score():
    mod = load_module()
    candidate = SimpleNamespace(side='SHORT', score=77.0, reasons=[])
    mod.apply_market_direction_bias(candidate, {'bias': 'NEUTRAL', 'strength': 0.0, 'breadth_ratio': 0.5, 'sample_size': 50})
    assert candidate.score == 77.0
    assert candidate.market_direction_score_multiplier == 1.0
