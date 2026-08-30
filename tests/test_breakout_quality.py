import importlib.util
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_module():
    spec = importlib.util.spec_from_file_location('breakout_quality_test', SCRIPTS_DIR / 'breakout_quality.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def base_kwargs(**overrides):
    values = {
        'side': 'long',
        'last_price': 100.25,
        'breakout_level': 100.0,
        'current_open': 99.90,
        'current_high': 100.30,
        'current_low': 99.80,
        'volume_multiple': 1.5,
        'min_volume_multiple': 1.1,
        'oi_change_pct_5m': 0.25,
        'cvd_delta': 10.0,
        'cvd_zscore': 0.8,
        'taker_buy_ratio': 0.57,
    }
    values.update(overrides)
    return values


def test_clean_long_breakout_passes():
    mod = load_module()
    result = mod.evaluate_breakout_quality(**base_kwargs())
    assert result['quality_pass'] is True
    assert result['hard_reject'] is False
    assert result['confirmation_count'] >= 3
    assert result['flags']['breakout_volume_ok'] is True


def test_clean_short_breakout_passes():
    mod = load_module()
    result = mod.evaluate_breakout_quality(**base_kwargs(
        side='short',
        last_price=99.70,
        breakout_level=100.0,
        current_open=100.10,
        current_high=100.20,
        current_low=99.65,
        cvd_delta=-12.0,
        cvd_zscore=-0.9,
        taker_buy_ratio=0.43,
    ))
    assert result['quality_pass'] is True
    assert result['hard_reject'] is False


def test_tiny_breakout_is_not_accepted():
    mod = load_module()
    result = mod.evaluate_breakout_quality(**base_kwargs(last_price=100.03, current_high=100.08))
    assert result['quality_pass'] is False
    assert result['flags']['breakout_distance_ok'] is False


def test_large_rejection_wick_is_not_accepted():
    mod = load_module()
    result = mod.evaluate_breakout_quality(**base_kwargs(
        last_price=100.20,
        current_open=99.95,
        current_high=101.50,
        current_low=99.90,
    ))
    assert result['quality_pass'] is False
    assert result['flags']['breakout_wick_ok'] is False


def test_weak_volume_is_not_accepted():
    mod = load_module()
    result = mod.evaluate_breakout_quality(**base_kwargs(volume_multiple=0.95, min_volume_multiple=0.8))
    assert result['quality_pass'] is False
    assert result['flags']['breakout_volume_ok'] is False


def test_oi_and_cvd_divergence_hard_vetoes_price_breakout():
    mod = load_module()
    result = mod.evaluate_breakout_quality(**base_kwargs(
        oi_change_pct_5m=-0.7,
        cvd_delta=-15.0,
        cvd_zscore=-1.2,
        taker_buy_ratio=0.45,
    ))
    assert result['quality_pass'] is False
    assert result['hard_reject'] is True
    assert 'breakout_flow_divergence_veto' in result['reasons']


def test_missing_optional_flow_does_not_fail_clean_price_volume():
    mod = load_module()
    result = mod.evaluate_breakout_quality(**base_kwargs(
        oi_change_pct_5m=None,
        cvd_delta=None,
        cvd_zscore=None,
        taker_buy_ratio=None,
    ))
    assert result['quality_pass'] is True
    assert result['hard_reject'] is False
    assert result['confirmation_count'] == 0
