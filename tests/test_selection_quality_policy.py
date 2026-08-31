import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_module():
    spec = importlib.util.spec_from_file_location('selection_quality_policy_test', SCRIPTS_DIR / 'selection_quality_policy.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def candidate(**overrides):
    payload = dict(
        side='LONG',
        position_side='LONG',
        score=100.0,
        reasons=[],
        tradeability_score=0.75,
        liquidity_grade='B',
        book_depth_fill_ratio=0.80,
        spread_bps=8.0,
        estimated_impact_pct=0.10,
        higher_timeframe_bias='NEUTRAL',
        higher_tf_summary={},
        volume_multiple=1.1,
        recent_5m_change_pct=0.4,
        acceleration_ratio_5m_vs_15m=1.2,
        price_change_pct_24h=6.0,
        overextension_flag=False,
    )
    payload.update(overrides)
    return SimpleNamespace(**payload)


def test_clean_liquid_aligned_symbol_gets_soft_boost():
    mod = load_module()
    row = candidate(
        tradeability_score=0.92,
        liquidity_grade='A',
        book_depth_fill_ratio=0.96,
        spread_bps=3.0,
        estimated_impact_pct=0.04,
        higher_timeframe_bias='BULLISH',
        volume_multiple=1.8,
    )
    multiplier = mod.compute_selection_quality_multiplier(row)
    assert 1.05 < multiplier <= 1.12


def test_illiquid_symbol_is_deweighted_even_with_signal_score():
    mod = load_module()
    row = candidate(
        tradeability_score=0.42,
        liquidity_grade='D',
        book_depth_fill_ratio=0.45,
        spread_bps=28.0,
        estimated_impact_pct=0.35,
    )
    mod.apply_selection_quality(row)
    assert row.score < 80.0
    assert row.selection_quality_multiplier < 0.80


def test_opposed_higher_timeframe_trend_is_deweighted():
    mod = load_module()
    aligned = candidate(higher_timeframe_bias='BULLISH')
    opposed = candidate(higher_timeframe_bias='BEARISH')
    assert mod.compute_selection_quality_multiplier(aligned) > mod.compute_selection_quality_multiplier(opposed)
    assert opposed.selection_htf_alignment == -1.0


def test_short_side_uses_directional_higher_timeframe_alignment():
    mod = load_module()
    short = candidate(side='SHORT', position_side='SHORT', higher_timeframe_bias='BEAR_TREND')
    assert mod.compute_selection_quality_multiplier(short) > 1.0
    assert short.selection_htf_alignment == 1.0


def test_extreme_24h_move_without_fresh_momentum_is_deweighted():
    mod = load_module()
    fresh = candidate(price_change_pct_24h=28.0, recent_5m_change_pct=0.8, acceleration_ratio_5m_vs_15m=1.3)
    stale = candidate(price_change_pct_24h=28.0, recent_5m_change_pct=0.0, acceleration_ratio_5m_vs_15m=0.7)
    assert mod.compute_selection_quality_multiplier(stale) < mod.compute_selection_quality_multiplier(fresh)


def test_missing_optional_quality_data_is_neutral():
    mod = load_module()
    row = candidate(
        tradeability_score=0.0,
        liquidity_grade='',
        book_depth_fill_ratio=0.0,
        spread_bps=0.0,
        estimated_impact_pct=0.0,
        higher_timeframe_bias='',
        volume_multiple=0.0,
    )
    assert mod.compute_selection_quality_multiplier(row) == 1.0


def test_apply_is_idempotent_when_wrapped_after_base_ranker():
    mod = load_module()

    def base_ranker(row, _payload):
        row.score = 100.0
        row.reasons = [reason for reason in row.reasons if not str(reason).startswith('base_rank=')]
        row.reasons.append('base_rank=100')
        return row

    strategy = SimpleNamespace(apply_market_direction_bias=base_ranker)
    mod.install_selection_quality_hook(strategy)
    row = candidate(
        tradeability_score=0.9,
        liquidity_grade='A',
        book_depth_fill_ratio=0.95,
        spread_bps=2.0,
        higher_timeframe_bias='BULLISH',
        volume_multiple=1.7,
    )
    strategy.apply_market_direction_bias(row, {})
    first = (row.score, list(row.reasons))
    strategy.apply_market_direction_bias(row, {})
    second = (row.score, list(row.reasons))
    assert second == first
    assert sum(str(reason).startswith('selection_quality=') for reason in row.reasons) == 1


def test_hook_installation_is_idempotent():
    mod = load_module()

    def base_ranker(row, _payload):
        return row

    strategy = SimpleNamespace(apply_market_direction_bias=base_ranker)
    mod.install_selection_quality_hook(strategy)
    first = strategy.apply_market_direction_bias
    mod.install_selection_quality_hook(strategy)
    assert strategy.apply_market_direction_bias is first
