from scripts.strategy_edge import estimate_realizable_reward_r


def test_launch_pullback_with_good_depth_gets_higher_realizable_reward():
    result = estimate_realizable_reward_r(
        base_reward_r=1.0,
        trigger_type='pullback',
        state='launch',
        volume_multiple=2.0,
        min_volume_multiple=1.0,
        stop_distance_pct=1.0,
        expected_slippage_pct=0.05,
        book_depth_fill_ratio=0.95,
        has_orderbook_depth=True,
    )
    assert result['reward_r'] > 1.2


def test_overheated_extended_breakout_is_heavily_discounted():
    result = estimate_realizable_reward_r(
        base_reward_r=1.0,
        trigger_type='breakout',
        state='overheated',
        overextension_flag=True,
        breakout_quality={'quality_pass': True, 'confirmation_count': 1},
        volume_multiple=1.0,
        min_volume_multiple=1.0,
        stop_distance_pct=1.0,
        expected_slippage_pct=0.20,
        book_depth_fill_ratio=0.65,
        has_orderbook_depth=True,
    )
    assert result['reward_r'] < 0.6


def test_hard_flow_divergence_does_not_receive_optimistic_edge():
    result = estimate_realizable_reward_r(
        base_reward_r=1.5,
        trigger_type='breakout',
        state='launch',
        breakout_quality={'quality_pass': False, 'hard_reject': True, 'confirmation_count': 0},
        volume_multiple=2.0,
        min_volume_multiple=1.0,
    )
    assert result['breakout_multiplier'] == 0.45
    assert result['reward_r'] < 1.0


def test_missing_orderbook_depth_is_neutral_not_punished():
    missing = estimate_realizable_reward_r(
        base_reward_r=1.0,
        trigger_type='breakout',
        state='launch',
        volume_multiple=1.5,
        min_volume_multiple=1.0,
        book_depth_fill_ratio=0.0,
        has_orderbook_depth=False,
    )
    observed_bad = estimate_realizable_reward_r(
        base_reward_r=1.0,
        trigger_type='breakout',
        state='launch',
        volume_multiple=1.5,
        min_volume_multiple=1.0,
        book_depth_fill_ratio=0.2,
        has_orderbook_depth=True,
    )
    assert missing['depth_multiplier'] == 1.0
    assert observed_bad['depth_multiplier'] == 0.68
    assert missing['reward_r'] > observed_bad['reward_r']


def test_high_slippage_relative_to_stop_reduces_reward():
    cheap = estimate_realizable_reward_r(
        base_reward_r=1.0,
        state='launch',
        stop_distance_pct=2.0,
        expected_slippage_pct=0.05,
        volume_multiple=1.2,
    )
    expensive = estimate_realizable_reward_r(
        base_reward_r=1.0,
        state='launch',
        stop_distance_pct=0.5,
        expected_slippage_pct=0.25,
        volume_multiple=1.2,
    )
    assert expensive['slippage_r'] > cheap['slippage_r']
    assert expensive['reward_r'] < cheap['reward_r']
