import importlib.util
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_candidate_builder():
    spec = importlib.util.spec_from_file_location(
        'candidate_builder_pullback_test', SCRIPTS_DIR / 'candidate_builder.py'
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def detect(mod, **overrides):
    base = dict(
        trade_side='short',
        last_price=1.0,
        breakout_level=0.99,
        higher_tf_allowed=True,
        macd_hist=-0.001,
        macd_prev_hist=0.0,
        distance_from_ema20_5m_pct=0.5,
        distance_from_vwap_15m_pct=2.5,
        current_open=1.002,
    )
    base.update(overrides)
    return mod.detect_pullback_short_setup(**base)


def test_weak_coin_pullback_to_resistance_with_macd_turn_is_short_signal():
    mod = load_candidate_builder()
    assert detect(mod) is True


def test_weak_coin_pullback_to_resistance_with_bearish_candle_is_short_signal():
    mod = load_candidate_builder()
    assert detect(mod, macd_hist=0.0, macd_prev_hist=-0.001, last_price=1.0, current_open=1.002) is True


def test_long_side_is_not_pullback_short():
    mod = load_candidate_builder()
    assert detect(mod, trade_side='long') is False


def test_not_weak_coin_is_not_pullback_short():
    mod = load_candidate_builder()
    assert detect(mod, higher_tf_allowed=False) is False


def test_no_rebound_is_not_pullback_short():
    mod = load_candidate_builder()
    # 价格没有高于近期低点 0.5% 以上（仍在低点附近）
    assert detect(mod, last_price=0.991, breakout_level=0.99) is False


def test_rebound_not_near_resistance_is_not_pullback_short():
    mod = load_candidate_builder()
    # 距离 EMA 和 VWAP 都超过 tolerance
    assert detect(mod, distance_from_ema20_5m_pct=3.0, distance_from_vwap_15m_pct=4.0) is False


def test_rebound_near_resistance_without_exhaustion_is_not_pullback_short():
    mod = load_candidate_builder()
    # MACD 未转弱且未收阴（动能仍在）
    assert detect(mod, macd_hist=0.001, macd_prev_hist=-0.001, last_price=1.004, current_open=1.0) is False
