import importlib.util
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_candidate_builder():
    spec = importlib.util.spec_from_file_location(
        'candidate_builder_pullback_long_test', SCRIPTS_DIR / 'candidate_builder.py'
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def detect(mod, **overrides):
    base = dict(
        trade_side='long',
        last_price=0.99,
        breakout_level=1.0,
        higher_tf_allowed=True,
        macd_hist=0.001,
        macd_prev_hist=0.0,
        distance_from_ema20_5m_pct=-0.5,
        distance_from_vwap_15m_pct=-2.5,
        current_open=0.988,
    )
    base.update(overrides)
    return mod.detect_pullback_long_setup(**base)


def test_strong_coin_pullback_to_support_with_macd_turn_is_long_signal():
    mod = load_candidate_builder()
    assert detect(mod) is True


def test_strong_coin_pullback_to_support_with_bullish_candle_is_long_signal():
    mod = load_candidate_builder()
    assert detect(mod, macd_hist=-0.001, macd_prev_hist=0.0, last_price=0.99, current_open=0.988) is True


def test_short_side_is_not_pullback_long():
    mod = load_candidate_builder()
    assert detect(mod, trade_side='short') is False


def test_countertrend_coin_is_not_pullback_long():
    mod = load_candidate_builder()
    assert detect(mod, higher_tf_allowed=False) is False


def test_no_meaningful_pullback_is_not_pullback_long():
    mod = load_candidate_builder()
    assert detect(mod, last_price=0.998, breakout_level=1.0) is False


def test_pullback_not_near_support_is_not_pullback_long():
    mod = load_candidate_builder()
    assert detect(mod, distance_from_ema20_5m_pct=-3.0, distance_from_vwap_15m_pct=-4.0) is False


def test_pullback_near_support_without_reacceleration_is_not_pullback_long():
    mod = load_candidate_builder()
    assert detect(mod, macd_hist=-0.002, macd_prev_hist=-0.001, last_price=0.988, current_open=0.99) is False
