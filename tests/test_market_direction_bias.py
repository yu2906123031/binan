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
    assert 0 < result['strength'] <= 1


def test_broad_falling_market_produces_short_bias():
    mod = load_module()
    result = mod.compute_market_direction_bias(
        tickers([-5, -4, -3, -2, -1, -0.5, 0.2, 0.5]),
        {'structural_label': 'BEAR_TREND'},
    )
    assert result['bias'] == 'SHORT'
    assert result['breadth_ratio'] < 0.4


def test_mixed_or_insufficient_market_is_neutral():
    mod = load_module()
    assert mod.compute_market_direction_bias(tickers([2, 1, -1, -2]), {'structural_label': 'RANGE'})['bias'] == 'NEUTRAL'
    assert mod.compute_market_direction_bias(tickers([2, 1]), {'structural_label': 'BULL_TREND'})['bias'] == 'NEUTRAL'


def test_bias_is_soft_observable_and_preserves_both_sides():
    mod = load_module()
    long = SimpleNamespace(side='LONG', score=80.0, reasons=[])
    short = SimpleNamespace(side='SHORT', score=80.0, reasons=[])
    payload = {'bias': 'LONG', 'strength': 0.8, 'breadth_ratio': 0.75, 'sample_size': 100}

    mod.apply_market_direction_bias(long, payload)
    mod.apply_market_direction_bias(short, payload)

    assert long.score > 80.0
    assert 75.0 < short.score < 80.0
    assert long.market_direction_bias == short.market_direction_bias == 'LONG'
    assert any('market_direction_bias=LONG' in reason for reason in short.reasons)


def test_neutral_bias_does_not_change_score():
    mod = load_module()
    candidate = SimpleNamespace(side='SHORT', score=77.0, reasons=[])
    mod.apply_market_direction_bias(candidate, {'bias': 'NEUTRAL', 'strength': 0.0, 'breadth_ratio': 0.5, 'sample_size': 50})
    assert candidate.score == 77.0
    assert candidate.market_direction_score_multiplier == 1.0
