import argparse
import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path('/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py')
spec = importlib.util.spec_from_file_location('binance_futures_momentum_long', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def make_kline(open_price, high, low, close, volume=1000, quote_volume=100000, taker_buy_quote_volume=None):
    if taker_buy_quote_volume is None:
        taker_buy_quote_volume = quote_volume * 0.5
    return [
        0,
        str(open_price),
        str(high),
        str(low),
        str(close),
        str(volume),
        0,
        str(quote_volume),
        0,
        0,
        str(taker_buy_quote_volume),
        0,
    ]


def make_meta():
    return mod.SymbolMeta(
        symbol='TESTUSDT',
        price_precision=4,
        quantity_precision=3,
        tick_size=0.0001,
        step_size=0.001,
        min_qty=0.001,
        quote_asset='USDT',
        status='TRADING',
        contract_type='PERPETUAL',
    )


def make_breakout_klines():
    klines_5m = [make_kline(100 + i, 101 + i, 99 + i, 100.7 + i, volume=1000 + i * 10, quote_volume=100000 + i * 1000) for i in range(29)]
    klines_5m.append(make_kline(129, 133, 128, 132, volume=5000, quote_volume=700000, taker_buy_quote_volume=520000))
    klines_15m = [make_kline(90 + i, 91 + i, 89 + i, 90.6 + i, volume=2000, quote_volume=200000) for i in range(30)]
    klines_1h = [make_kline(80 + i, 81 + i, 79 + i, 80.5 + i, volume=3000, quote_volume=300000) for i in range(30)]
    klines_4h = [make_kline(70 + i, 71 + i, 69 + i, 70.5 + i, volume=4000, quote_volume=400000) for i in range(30)]
    return klines_5m, klines_15m, klines_1h, klines_4h


def make_ticker():
    return {
        'symbol': 'TESTUSDT',
        'priceChangePercent': '12',
        'quoteVolume': '80000000',
        'lastPrice': '132',
    }


def test_compute_leading_sentiment_signal_rewards_early_turn_and_penalizes_overheated_sentiment():
    payload = mod.compute_leading_sentiment_signal(okx_sentiment_score=0.2, okx_sentiment_acceleration=0.4)
    assert payload['score'] > 8
    assert 'sentiment_early_turn_zone' in payload['reasons']
    assert 'sentiment_acceleration_turn' in payload['reasons']

    overheated = mod.compute_leading_sentiment_signal(okx_sentiment_score=0.82, okx_sentiment_acceleration=0.1)
    assert overheated['score'] < 0
    assert 'sentiment_too_hot' in overheated['reasons']


def test_compute_squeeze_and_control_risk_scores_capture_short_squeeze_and_distribution_risk():
    squeeze = mod.compute_squeeze_signal(
        funding_rate=-0.0014,
        funding_rate_avg=-0.0008,
        short_bias=0.76,
        oi_zscore_5m=4.0,
        cvd_delta=260000.0,
        cvd_zscore=3.5,
        recent_5m_change_pct=2.4,
    )
    assert squeeze['score'] >= 25
    assert 'negative_funding_crowded_shorts' in squeeze['reasons']
    assert 'retail_short_bias' in squeeze['reasons']

    risk = mod.compute_control_risk_score(
        short_bias=0.12,
        oi_notional_percentile=0.985,
        smart_money_flow_score=-0.45,
    )
    assert risk['score'] >= 20
    assert risk['veto'] is True
    assert risk['veto_reason'] == 'smart_money_distribution_veto'
    assert 'oi_at_extreme_percentile' in risk['reasons']
    assert 'weak_short_fuel' in risk['reasons']
    assert 'smart_money_distribution_risk' in risk['reasons']


def test_merge_smart_money_scores_triggers_veto_for_persistent_outflow():
    payload = mod.merge_smart_money_scores(exchange_score=-0.4, onchain_score=-0.7)
    assert round(payload['score'], 2) == -0.55
    assert payload['veto'] is True
    assert payload['veto_reason'] == 'smart_money_outflow_veto'
    assert payload['sources'] == ['exchange', 'onchain']


def test_compute_sentiment_resonance_bonus_rewards_alignment_and_penalizes_smart_money_outflow():
    payload = mod.compute_sentiment_resonance_bonus(
        okx_sentiment_score=0.8,
        okx_sentiment_acceleration=0.45,
        sector_resonance_score=0.7,
        smart_money_flow_score=-0.6,
    )

    assert payload['bonus'] < 7.5
    assert payload['penalty'] >= 8.0
    assert 'okx_sentiment_positive' in payload['reasons']
    assert 'sector_resonance_positive' in payload['reasons']
    assert 'smart_money_outflow' in payload['reasons']
    assert 'smart_money_veto_zone' in payload['reasons']


def test_compute_market_regime_filter_blocks_when_btc_and_sol_both_break_down():
    btc = [make_kline(100 + i, 101 + i, 99 + i, 100 + i) for i in range(29)]
    btc.append(make_kline(130, 131, 100, 102))
    sol = [make_kline(50 + i, 51 + i, 49 + i, 50 + i) for i in range(29)]
    sol.append(make_kline(80, 81, 60, 62))

    payload = mod.compute_market_regime_filter(btc_klines=btc, sol_klines=sol)

    assert payload['risk_on'] is False
    assert payload['score_multiplier'] == 0.55
    assert payload['label'] == 'risk_off'
    assert 'btc_trend_down' in payload['reasons']
    assert 'btc_momentum_breakdown' in payload['reasons']
    assert 'sol_trend_down' in payload['reasons']
    assert 'sol_momentum_breakdown' in payload['reasons']


def test_build_candidate_labels_short_squeeze_launch_state_when_funding_negative_and_retail_short_heavy():
    klines_5m, klines_15m, klines_1h, klines_4h = make_breakout_klines()
    ticker = make_ticker()
    meta = make_meta()

    candidate = mod.build_candidate(
        symbol='TESTUSDT',
        ticker=ticker,
        klines_5m=klines_5m,
        klines_15m=klines_15m,
        klines_1h=klines_1h,
        klines_4h=klines_4h,
        meta=meta,
        hot_rank=1,
        gainer_rank=2,
        risk_usdt=10.0,
        lookback_bars=12,
        swing_bars=6,
        min_5m_change_pct=0.5,
        min_quote_volume=1000.0,
        stop_buffer_pct=0.01,
        max_rsi_5m=85.0,
        min_volume_multiple=1.0,
        max_distance_from_ema_pct=12.0,
        funding_rate=-0.0014,
        funding_rate_threshold=0.0005,
        funding_rate_avg=-0.0008,
        funding_rate_avg_threshold=0.0003,
        max_distance_from_vwap_pct=12.0,
        max_leverage=5,
        short_bias=0.78,
        oi_now=1_450_000.0,
        oi_5m_ago=1_200_000.0,
        oi_15m_ago=1_050_000.0,
        cvd_delta=240000.0,
        cvd_zscore=3.2,
        oi_notional_percentile=0.82,
        oi_zscore_5m=4.1,
        okx_sentiment_score=0.28,
        okx_sentiment_acceleration=0.42,
        sector_resonance_score=0.64,
        smart_money_flow_score=0.45,
        onchain_smart_money_score=0.25,
        market_regime={'risk_on': True, 'score_multiplier': 1.1, 'reasons': ['btc_above_ema20'], 'label': 'risk_on'},
    )

    assert candidate is not None
    assert candidate.state == 'launch'
    assert candidate.alert_tier == 'critical'
    assert candidate.position_size_pct == 3.3
    assert candidate.smart_money_veto is False
    assert 'launch_short_squeeze' in candidate.state_reasons
    assert any(reason.startswith('position_size_pct=') for reason in candidate.reasons)


def test_build_candidate_blocks_when_smart_money_veto_and_distribution_risk_present():
    klines_5m, klines_15m, klines_1h, klines_4h = make_breakout_klines()
    ticker = make_ticker()

    candidate = mod.build_candidate(
        symbol='TESTUSDT',
        ticker=ticker,
        klines_5m=klines_5m,
        klines_15m=klines_15m,
        klines_1h=klines_1h,
        klines_4h=klines_4h,
        meta=make_meta(),
        hot_rank=1,
        gainer_rank=1,
        risk_usdt=10.0,
        lookback_bars=12,
        swing_bars=6,
        min_5m_change_pct=0.5,
        min_quote_volume=1000.0,
        stop_buffer_pct=0.01,
        max_rsi_5m=85.0,
        min_volume_multiple=1.0,
        max_distance_from_ema_pct=12.0,
        funding_rate=0.0001,
        funding_rate_threshold=0.0005,
        funding_rate_avg=0.0001,
        funding_rate_avg_threshold=0.0003,
        max_distance_from_vwap_pct=12.0,
        max_leverage=5,
        short_bias=0.1,
        oi_now=1_600_000.0,
        oi_5m_ago=1_640_000.0,
        oi_15m_ago=1_680_000.0,
        cvd_delta=-260000.0,
        cvd_zscore=-3.4,
        oi_notional_percentile=0.992,
        oi_zscore_5m=2.8,
        okx_sentiment_score=0.75,
        okx_sentiment_acceleration=-0.05,
        sector_resonance_score=0.15,
        smart_money_flow_score=-0.72,
        onchain_smart_money_score=-0.64,
        market_regime={'risk_on': True, 'score_multiplier': 1.0, 'reasons': [], 'label': 'risk_on'},
    )

    assert candidate is not None
    assert candidate.smart_money_veto is True
    assert mod.apply_hard_veto_filters(candidate) == 'smart_money_outflow_veto'


def test_build_candidate_sets_staged_entry_and_slippage_fields():
    klines_5m, klines_15m, klines_1h, klines_4h = make_breakout_klines()
    candidate = mod.build_candidate(
        symbol='TESTUSDT',
        ticker=make_ticker(),
        klines_5m=klines_5m,
        klines_15m=klines_15m,
        klines_1h=klines_1h,
        klines_4h=klines_4h,
        meta=make_meta(),
        hot_rank=1,
        gainer_rank=2,
        risk_usdt=10.0,
        lookback_bars=12,
        swing_bars=6,
        min_5m_change_pct=0.5,
        min_quote_volume=1000.0,
        stop_buffer_pct=0.01,
        max_rsi_5m=85.0,
        min_volume_multiple=1.0,
        max_distance_from_ema_pct=12.0,
        funding_rate=-0.0014,
        funding_rate_threshold=0.0005,
        funding_rate_avg=-0.0008,
        funding_rate_avg_threshold=0.0003,
        max_distance_from_vwap_pct=12.0,
        max_leverage=5,
        short_bias=0.78,
        oi_now=1_450_000.0,
        oi_5m_ago=1_200_000.0,
        oi_15m_ago=1_050_000.0,
        cvd_delta=240000.0,
        cvd_zscore=3.2,
        oi_notional_percentile=0.82,
        oi_zscore_5m=4.1,
        okx_sentiment_score=0.28,
        okx_sentiment_acceleration=0.42,
        sector_resonance_score=0.64,
        smart_money_flow_score=0.45,
        onchain_smart_money_score=0.25,
        market_regime={'risk_on': True, 'score_multiplier': 1.1, 'reasons': ['btc_above_ema20'], 'label': 'risk_on'},
    )

    assert candidate is not None
    assert candidate.setup_ready is True
    assert candidate.trigger_fired is True
    assert candidate.overextension_flag is False
    assert candidate.entry_distance_from_breakout_pct > 0
    assert candidate.expected_slippage_pct == round(candidate.entry_distance_from_breakout_pct * 0.35, 4)
    assert 0.0 <= candidate.book_depth_fill_ratio <= 1.0


def test_build_candidate_blocks_when_expected_slippage_and_overextension_stay_high():
    klines_5m, klines_15m, klines_1h, klines_4h = make_breakout_klines()
    stretched_5m = [row[:] for row in klines_5m]
    stretched_15m = [row[:] for row in klines_15m]
    stretched_5m[-1][4] = '141.0'
    stretched_5m[-1][2] = '141.5'
    stretched_15m[-1][4] = '141.0'
    stretched_15m[-1][2] = '141.5'

    candidate = mod.build_candidate(
        symbol='TESTUSDT',
        ticker={'symbol': 'TESTUSDT', 'priceChangePercent': '12', 'quoteVolume': '80000000', 'lastPrice': '141.0'},
        klines_5m=stretched_5m,
        klines_15m=stretched_15m,
        klines_1h=klines_1h,
        klines_4h=klines_4h,
        meta=make_meta(),
        hot_rank=1,
        gainer_rank=1,
        risk_usdt=10.0,
        lookback_bars=12,
        swing_bars=6,
        min_5m_change_pct=0.5,
        min_quote_volume=1000.0,
        stop_buffer_pct=0.01,
        max_rsi_5m=95.0,
        min_volume_multiple=1.0,
        max_distance_from_ema_pct=12.0,
        funding_rate=0.0001,
        funding_rate_threshold=0.0005,
        funding_rate_avg=0.0001,
        funding_rate_avg_threshold=0.0003,
        max_distance_from_vwap_pct=12.0,
        max_leverage=5,
        short_bias=0.2,
        oi_now=1_300_000.0,
        oi_5m_ago=1_240_000.0,
        oi_15m_ago=1_180_000.0,
        cvd_delta=160000.0,
        cvd_zscore=2.4,
        oi_notional_percentile=0.7,
        oi_zscore_5m=2.5,
        okx_sentiment_score=0.12,
        okx_sentiment_acceleration=0.1,
        sector_resonance_score=0.15,
        smart_money_flow_score=0.18,
        onchain_smart_money_score=0.0,
        market_regime={'risk_on': True, 'score_multiplier': 1.0, 'reasons': [], 'label': 'risk_on'},
    )

    assert candidate is not None
    assert candidate.overextension_flag is False
    assert candidate.setup_ready is True
    assert candidate.trigger_fired is True
    assert candidate.expected_slippage_pct > 3.0


def test_run_scan_once_enriches_candidate_with_okx_sentiment_and_hard_veto(monkeypatch):
    args = argparse.Namespace(
        risk_usdt=10.0,
        max_notional_usdt=0.0,
        lookback_bars=12,
        swing_bars=6,
        top_gainers=5,
        max_candidates=4,
        min_5m_change_pct=0.5,
        min_quote_volume=1000,
        stop_buffer_pct=0.01,
        max_rsi_5m=90.0,
        min_volume_multiple=1.0,
        max_distance_from_ema_pct=12.0,
        max_distance_from_vwap_pct=12.0,
        leverage=5,
        max_funding_rate=0.01,
        max_funding_rate_avg=0.01,
        okx_sentiment_file='',
        okx_sentiment_inline='TESTUSDT|0.82|0.31|0.74|0.58',
        okx_sentiment_command='',
        okx_mcp_command='',
        okx_sentiment_timeout=15,
        okx_auto=False,
        smart_money_inline='TESTUSDT|-0.92',
        smart_money_file='',
    )

    monkeypatch.setattr(mod, 'load_manual_square_symbols', lambda _args: [])
    monkeypatch.setattr(mod, 'fetch_exchange_meta', lambda client: {'TESTUSDT': make_meta()})
    monkeypatch.setattr(mod, 'fetch_tickers', lambda client: [make_ticker()])
    monkeypatch.setattr(mod, 'fetch_klines', lambda client, symbol, interval, limit: [make_kline(1, 1, 1, 1) for _ in range(limit)])
    monkeypatch.setattr(mod, 'merged_candidate_symbols', lambda **kwargs: (['TESTUSDT'], {'TESTUSDT': 1}, {'TESTUSDT': 1}))
    monkeypatch.setattr(mod, 'fetch_funding_rates', lambda client, symbol, limit=3: [-0.001, -0.0005, -0.0003])
    monkeypatch.setattr(mod, 'fetch_open_interest_hist', lambda client, symbol, period='5m', limit=30: [])
    monkeypatch.setattr(mod, 'fetch_top_account_long_short_ratio', lambda client, symbol, period='5m', limit=10: [])
    monkeypatch.setattr(mod, 'derive_microstructure_inputs', lambda **kwargs: {})
    monkeypatch.setattr(mod, 'compute_market_regime_filter', lambda **kwargs: {
        'risk_on': True,
        'score_multiplier': 1.0,
        'reasons': [],
        'label': 'risk_on',
        'momentum_flags': {'btc': False, 'sol': False},
    })

    def fake_build_candidate(*args, **kwargs):
        assert kwargs['okx_sentiment_score'] == 0.82
        assert kwargs['okx_sentiment_acceleration'] == 0.31
        assert kwargs['sector_resonance_score'] == 0.74
        assert kwargs['smart_money_flow_score'] == 0.58
        assert kwargs['onchain_smart_money_score'] == -0.92
        return mod.Candidate(
            symbol='TESTUSDT',
            last_price=132.0,
            price_change_pct_24h=18.0,
            quote_volume_24h=80000000.0,
            hot_rank=1,
            gainer_rank=1,
            funding_rate=-0.001,
            funding_rate_avg=-0.0005,
            recent_5m_change_pct=2.6,
            acceleration_ratio_5m_vs_15m=1.7,
            breakout_level=130.0,
            recent_swing_low=126.0,
            stop_price=124.0,
            quantity=1.0,
            risk_per_unit=8.0,
            recommended_leverage=3,
            rsi_5m=72.0,
            volume_multiple=2.0,
            distance_from_ema20_5m_pct=4.0,
            distance_from_vwap_15m_pct=3.0,
            score=74.0,
            reasons=['okx_sentiment_positive'],
            higher_tf_summary={'allowed': True},
            okx_sentiment_score=0.82,
            okx_sentiment_acceleration=0.31,
            sector_resonance_score=0.74,
            smart_money_flow_score=-0.17,
            onchain_smart_money_score=-0.92,
            smart_money_veto=True,
            smart_money_veto_reason='smart_money_outflow_veto',
            smart_money_sources=['exchange', 'onchain'],
            state='launch',
            state_reasons=['short_squeeze_setup'],
            cvd_delta=-220000.0,
            cvd_zscore=-3.1,
            oi_change_pct_5m=-4.2,
        )

    monkeypatch.setattr(mod, 'build_candidate', fake_build_candidate)

    payload, best, meta = mod.run_scan_once(client=object(), args=args)

    assert payload['ok'] is True
    assert best is None
    assert payload['candidate_count'] == 0
    assert payload['candidates'] == []
    assert payload['selected_alert'] is None
    assert meta['TESTUSDT'].symbol == 'TESTUSDT'


def test_build_standardized_alert_exposes_entry_distance_and_overheat_fields():
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=132.0,
        price_change_pct_24h=18.0,
        quote_volume_24h=80_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0003,
        funding_rate_avg=0.0002,
        recent_5m_change_pct=2.4,
        acceleration_ratio_5m_vs_15m=1.6,
        breakout_level=128.0,
        recent_swing_low=124.0,
        stop_price=123.5,
        quantity=1.25,
        risk_per_unit=8.5,
        recommended_leverage=3,
        rsi_5m=74.0,
        volume_multiple=2.1,
        distance_from_ema20_5m_pct=5.2,
        distance_from_vwap_15m_pct=4.4,
        higher_tf_summary={'1h': 'up', '4h': 'up'},
        score=81.0,
        reasons=['alert_tier=critical'],
        state='overheated',
        state_reasons=['overheated_extension'],
        alert_tier='critical',
        position_size_pct=2.5,
        atr_stop_distance=6.0,
    )
    candidate.entry_distance_from_breakout_pct = round((candidate.last_price - candidate.breakout_level) / candidate.breakout_level * 100, 4)
    candidate.entry_distance_from_vwap_pct = candidate.distance_from_vwap_15m_pct
    candidate.overextension_flag = True

    alert = mod.build_standardized_alert(candidate)

    assert alert['symbol'] == 'TESTUSDT'
    assert alert['state'] == 'overheated'
    assert alert['alert_tier'] == 'critical'
    assert alert['entry_distance_from_breakout_pct'] == candidate.entry_distance_from_breakout_pct
    assert alert['entry_distance_from_vwap_pct'] == candidate.entry_distance_from_vwap_pct
    assert alert['overextension_flag'] is True


def test_build_standardized_alert_exposes_candidate_three_layer_fields():
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=132.0,
        price_change_pct_24h=18.0,
        quote_volume_24h=80_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0003,
        funding_rate_avg=0.0002,
        recent_5m_change_pct=2.4,
        acceleration_ratio_5m_vs_15m=1.6,
        breakout_level=128.0,
        recent_swing_low=124.0,
        stop_price=123.5,
        quantity=1.25,
        risk_per_unit=8.5,
        recommended_leverage=3,
        rsi_5m=74.0,
        volume_multiple=2.1,
        distance_from_ema20_5m_pct=5.2,
        distance_from_vwap_15m_pct=4.4,
        higher_tf_summary={'1h': 'up', '4h': 'up'},
        score=81.0,
        reasons=['alert_tier=critical'],
        state='launch',
        state_reasons=['launch_short_squeeze'],
        alert_tier='critical',
        position_size_pct=2.5,
        atr_stop_distance=6.0,
        must_pass_flags={'state_ok': True, 'liquidity_ok': True},
        quality_score=73.5,
        execution_priority_score=67.2,
        candle_extension_pct=4.12,
        recent_3bar_runup_pct=6.48,
        overextension_flag='warn',
        entry_pattern='squeeze_launch',
        trend_regime='risk_on_trend',
        liquidity_grade='A',
        setup_ready=True,
        trigger_fired=True,
        expected_slippage_pct=1.44,
        book_depth_fill_ratio=0.83,
    )
    candidate.entry_distance_from_breakout_pct = round((candidate.last_price - candidate.breakout_level) / candidate.breakout_level * 100, 4)
    candidate.entry_distance_from_vwap_pct = candidate.distance_from_vwap_15m_pct

    alert = mod.build_standardized_alert(candidate)

    assert alert['must_pass_flags'] == {'state_ok': True, 'liquidity_ok': True}
    assert alert['quality_score'] == 73.5
    assert alert['execution_priority_score'] == 67.2
    assert alert['candle_extension_pct'] == 4.12
    assert alert['recent_3bar_runup_pct'] == 6.48
    assert alert['overextension_flag'] == 'warn'
    assert alert['entry_pattern'] == 'squeeze_launch'
    assert alert['trend_regime'] == 'risk_on_trend'
    assert alert['liquidity_grade'] == 'A'
    assert alert['setup_ready'] is True
    assert alert['trigger_fired'] is True


def test_run_loop_records_rejection_event_when_risk_guard_blocks_live_trade(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    args = argparse.Namespace(
        reconcile_only=False,
        halt_on_orphan_position=False,
        daily_max_loss_usdt=0.0,
        max_consecutive_losses=0,
        symbol_cooldown_minutes=0,
        live=True,
        max_open_positions=1,
        profile='test',
        auto_loop=False,
        disable_notify=True,
        notify_target='',
    )
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=132.0,
        price_change_pct_24h=10.0,
        quote_volume_24h=80_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=2.0,
        acceleration_ratio_5m_vs_15m=1.4,
        breakout_level=130.0,
        recent_swing_low=126.0,
        stop_price=124.0,
        quantity=1.0,
        risk_per_unit=8.0,
        recommended_leverage=3,
        rsi_5m=67.0,
        volume_multiple=1.8,
        distance_from_ema20_5m_pct=3.0,
        distance_from_vwap_15m_pct=2.4,
        higher_tf_summary={'1h': 'up'},
        score=72.0,
        reasons=['candidate_selected'],
        state='launch',
        state_reasons=['impulse_ready'],
    )

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda _args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda *a, **k: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *a, **k: ({'ok': True, 'candidate_count': 1, 'candidates': [{'symbol': 'TESTUSDT'}]}, candidate, {'TESTUSDT': make_meta()}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda _store: mod.default_risk_state())
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': False, 'reasons': ['candidate_distribution_risk'], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])

    result = mod.run_loop(client=object(), args=args)
    events_path = tmp_path / 'events.jsonl'
    rows = [mod.json.loads(line) for line in events_path.read_text(encoding='utf-8').splitlines() if line.strip()]

    assert result['cycles'][0]['live_skipped_due_to_risk_guard'] == ['candidate_distribution_risk']
    assert rows[-1]['event_type'] == 'candidate_rejected'
    assert rows[-1]['symbol'] == 'TESTUSDT'
    assert rows[-1]['reasons'] == ['candidate_distribution_risk']


def test_run_loop_records_rejection_event_when_max_open_positions_blocks_trade(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    args = argparse.Namespace(
        reconcile_only=False,
        halt_on_orphan_position=False,
        daily_max_loss_usdt=0.0,
        max_consecutive_losses=0,
        symbol_cooldown_minutes=0,
        live=True,
        max_open_positions=1,
        profile='test',
        auto_loop=False,
        disable_notify=True,
        notify_target='',
    )
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=132.0,
        price_change_pct_24h=10.0,
        quote_volume_24h=80_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=2.0,
        acceleration_ratio_5m_vs_15m=1.4,
        breakout_level=130.0,
        recent_swing_low=126.0,
        stop_price=124.0,
        quantity=1.0,
        risk_per_unit=8.0,
        recommended_leverage=3,
        rsi_5m=67.0,
        volume_multiple=1.8,
        distance_from_ema20_5m_pct=3.0,
        distance_from_vwap_15m_pct=2.4,
        higher_tf_summary={'1h': 'up'},
        score=72.0,
        reasons=['candidate_selected'],
        state='launch',
        state_reasons=['impulse_ready'],
    )

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda _args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda *a, **k: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *a, **k: ({'ok': True, 'candidate_count': 1, 'candidates': [{'symbol': 'TESTUSDT'}]}, candidate, {'TESTUSDT': make_meta()}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda _store: mod.default_risk_state())
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'BTCUSDT', 'positionAmt': '0.01'}])

    result = mod.run_loop(client=object(), args=args)
    events_path = tmp_path / 'events.jsonl'
    rows = [mod.json.loads(line) for line in events_path.read_text(encoding='utf-8').splitlines() if line.strip()]

    assert len(result['cycles'][0]['live_skipped_due_to_existing_positions']) == 1
    assert rows[-1]['event_type'] == 'candidate_rejected'
    assert rows[-1]['symbol'] == 'TESTUSDT'
    assert rows[-1]['reasons'] == ['max_open_positions_reached']


def test_apply_management_action_confirms_breakeven_stop_replacement(monkeypatch):
    state = mod.TradeManagementState(symbol='TESTUSDT', initial_quantity=1.0, remaining_quantity=1.0, current_stop_price=95.0)
    meta = make_meta()
    calls = []

    monkeypatch.setattr(mod, 'cancel_order', lambda client, symbol, order_id=None, client_order_id=None: calls.append(('cancel', symbol, order_id)) or {'ok': True})
    monkeypatch.setattr(mod, 'place_stop_market_order', lambda client, symbol, stop_price, quantity, meta, side=None: calls.append(('stop', symbol, stop_price, quantity, side)) or {'orderId': 99, 'triggerPrice': stop_price})

    new_state, active_stop, payload = mod.apply_management_action(
        client=object(),
        symbol='TESTUSDT',
        meta=meta,
        state=state,
        action={'type': 'move_stop_to_breakeven', 'new_stop_price': 100.0, 'confirmation_mode': 'ema_support'},
        active_stop_order={'orderId': 77},
    )

    assert active_stop['orderId'] == 99
    assert payload['new_stop_order']['orderId'] == 99
    assert calls == [('cancel', 'TESTUSDT', 77), ('stop', 'TESTUSDT', 100.0, 1.0, 'LONG')]


def test_evaluate_management_actions_requires_breakeven_confirmation_buffer():
    state = mod.TradeManagementState(symbol='TESTUSDT', initial_quantity=1.0, remaining_quantity=1.0)
    plan = mod.TradeManagementPlan(
        entry_price=100.0,
        stop_price=95.0,
        quantity=1.0,
        initial_risk_per_unit=5.0,
        breakeven_trigger_price=101.0,
        tp1_trigger_price=105.0,
        tp1_close_qty=0.5,
        tp2_trigger_price=110.0,
        tp2_close_qty=0.3,
        runner_qty=0.2,
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.02,
    )

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=101.2,
        ema5m=100.5,
        trailing_reference=101.5,
        trailing_buffer_pct=0.02,
    )
    assert actions == []

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=102.5,
        ema5m=101.2,
        trailing_reference=103.0,
        trailing_buffer_pct=0.02,
    )
    assert actions[0]['type'] == 'move_stop_to_breakeven'
    assert actions[0]['confirmation_mode'] == 'ema_support'


def test_monitor_live_trade_persists_exit_reason_and_trade_invalidated(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    symbol = 'TESTUSDT'
    store.save_json('positions', {
        symbol: {
            'symbol': symbol,
            'status': 'monitoring',
            'quantity': 1.0,
            'remaining_quantity': 1.0,
            '_debug_current_price': 111.0,
            '_debug_ema5m': 110.0,
            '_debug_trailing_reference': 112.0,
        }
    })
    args = argparse.Namespace(
        profile='test',
        max_monitor_cycles=1,
        monitor_poll_interval_sec=0,
        trailing_buffer_pct=0.01,
        disable_notify=True,
        notify_target='',
    )
    trade = {
        'entry_price': 100.0,
        'quantity': 1.0,
        'stop_order': {'orderId': 77},
        'protection_check': {'status': 'protected'},
        'trade_management_plan': {
            'entry_price': 100.0,
            'stop_price': 95.0,
            'quantity': 1.0,
            'initial_risk_per_unit': 5.0,
            'breakeven_trigger_price': 101.0,
            'tp1_trigger_price': 105.0,
            'tp1_close_qty': 0.5,
            'tp2_trigger_price': 110.0,
            'tp2_close_qty': 0.3,
            'runner_qty': 0.2,
            'breakeven_confirmation_mode': 'price_only',
            'breakeven_min_buffer_pct': 0.0,
            'exit_reason': None,
        },
    }

    monkeypatch.setattr(mod, 'fetch_klines', lambda *a, **k: [make_kline(100, 112, 99, 111) for _ in range(21)])
    monkeypatch.setattr(mod, 'log_runtime_event', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'place_reduce_only_market', lambda *a, **k: {'orderId': 888})

    def fake_apply(client, symbol, meta, state, action, active_stop_order):
        action['exit_reason'] = 'tp1'
        state.remaining_quantity = 0.0
        state.tp1_hit = True
        return state, None, {'reduce_order': {'orderId': 888}}

    monkeypatch.setattr(mod, 'apply_management_action', fake_apply)

    result = mod.monitor_live_trade(client=object(), symbol=symbol, meta=make_meta(), args=args, trade=trade, store=store)
    rows = [mod.json.loads(line) for line in (tmp_path / 'events.jsonl').read_text(encoding='utf-8').splitlines() if line.strip()]
    positions = store.load_json('positions', {})

    assert result['status'] == 'closed'
    assert result['exit_reason'] == 'tp1'
    assert rows[-1]['event_type'] == 'trade_invalidated'
    assert rows[-1]['exit_reason'] == 'tp1'
    assert symbol not in positions


def test_sync_tracked_positions_with_exchange_marks_missing_exchange_position_closed(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT': {
            'symbol': 'DOGEUSDT',
            'status': 'monitoring',
            'quantity': 5.0,
            'remaining_quantity': 5.0,
            'stop_order_id': 123,
            'protection_status': 'protected',
        },
        'BTCUSDT': {
            'symbol': 'BTCUSDT',
            'status': 'monitoring',
            'quantity': 1.0,
            'remaining_quantity': 1.0,
            'stop_order_id': 456,
            'protection_status': 'missing',
        },
    })

    result = mod.sync_tracked_positions_with_exchange(
        store,
        exchange_positions=[{'symbol': 'BTCUSDT', 'positionAmt': '2.5'}],
        protected_symbols=['BTCUSDT'],
    )

    positions = store.load_json('positions', {})
    assert result['closed_symbols'] == ['DOGEUSDT']
    assert 'BTCUSDT' in result['refreshed_symbols']
    assert positions['DOGEUSDT']['status'] == 'closed'
    assert positions['DOGEUSDT']['remaining_quantity'] == 0.0
    assert positions['DOGEUSDT']['stop_order_id'] is None
    assert positions['DOGEUSDT']['protection_status'] == 'flat'
    assert positions['BTCUSDT']['quantity'] == 2.5
    assert positions['BTCUSDT']['remaining_quantity'] == 2.5
    assert positions['BTCUSDT']['protection_status'] == 'protected'


def test_reconcile_runtime_state_reports_closed_tracked_positions(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT': {
            'symbol': 'DOGEUSDT',
            'status': 'monitoring',
            'quantity': 5.0,
            'remaining_quantity': 5.0,
            'stop_order_id': 123,
            'protection_status': 'protected',
        },
    })

    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])

    result = mod.reconcile_runtime_state(client=object(), store=store, halt_on_orphan_position=False)
    positions = store.load_json('positions', {})

    assert result['closed_tracked_positions'] == ['DOGEUSDT']
    assert result['exchange_position_count'] == 0
    assert positions['DOGEUSDT']['status'] == 'closed'
    assert positions['DOGEUSDT']['remaining_quantity'] == 0.0
    assert positions['DOGEUSDT']['protection_status'] == 'flat'


def test_reconcile_runtime_state_auto_repairs_missing_protection(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT': {
            'symbol': 'DOGEUSDT',
            'status': 'monitoring',
            'quantity': 5.0,
            'remaining_quantity': 5.0,
            'stop_price': 0.1234,
            'stop_order_id': 321,
            'protection_status': 'missing',
        },
    })

    repair_calls = []

    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'DOGEUSDT', 'positionAmt': '5'}])
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *args, **kwargs: {
        'status': 'missing',
        'active_position': {'symbol': 'DOGEUSDT', 'positionAmt': '5'},
        'expected_order_id': 321,
        'open_orders': [],
    })
    monkeypatch.setattr(mod, 'repair_missing_protection', lambda **kwargs: repair_calls.append(kwargs) or {
        'ok': True,
        'symbol': 'DOGEUSDT',
        'status': 'protected',
        'stop_order': {'orderId': 999},
        'stop_price': 0.1234,
        'quantity': 5.0,
        'repair_attempted': True,
    })

    result = mod.reconcile_runtime_state(
        client=object(),
        store=store,
        halt_on_orphan_position=False,
        repair_missing_protection_enabled=True,
    )
    positions = store.load_json('positions', {})

    assert len(repair_calls) == 1
    assert result['positions_missing_protection'] == []
    assert result['protection_repairs'][0]['status'] == 'protected'
    assert positions['DOGEUSDT']['protection_status'] == 'protected'
    assert positions['DOGEUSDT']['stop_order_id'] == 999


def test_reconcile_runtime_state_reports_missing_protection_when_auto_repair_disabled(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'status': 'monitoring',
            'quantity': 5.0,
            'remaining_quantity': 5.0,
            'stop_price': 0.1234,
            'stop_order_id': 321,
            'protection_status': 'missing',
        },
    })

    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'DOGEUSDT', 'positionAmt': '5'}])
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *args, **kwargs: {
        'status': 'missing',
        'active_position': {'symbol': 'DOGEUSDT', 'positionAmt': '5'},
        'expected_order_id': 321,
        'open_orders': [],
    })

    result = mod.reconcile_runtime_state(
        client=object(),
        store=store,
        halt_on_orphan_position=False,
        repair_missing_protection_enabled=False,
    )

    assert result['positions_missing_protection'] == ['DOGEUSDT']
    assert result['protection_repairs'] == []


def test_reconcile_runtime_state_reports_missing_protection_for_short_position_when_auto_repair_disabled(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT:SHORT': {
            'symbol': 'DOGEUSDT',
            'side': 'SHORT',
            'status': 'monitoring',
            'quantity': 5.0,
            'remaining_quantity': 5.0,
            'stop_price': 0.1234,
            'stop_order_id': 321,
            'protection_status': 'missing',
        },
    })

    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'DOGEUSDT', 'positionAmt': '-5', 'positionSide': 'SHORT'}])
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *args, **kwargs: {
        'status': 'missing',
        'active_position': {'symbol': 'DOGEUSDT', 'positionAmt': '-5', 'positionSide': 'SHORT'},
        'expected_order_id': 321,
        'open_orders': [],
    })

    result = mod.reconcile_runtime_state(
        client=object(),
        store=store,
        halt_on_orphan_position=False,
        repair_missing_protection_enabled=False,
    )

    positions = store.load_json('positions', {})
    assert result['positions_missing_protection'] == ['DOGEUSDT:SHORT']
    assert result['protection_repairs'] == []
    assert positions['DOGEUSDT:SHORT']['protection_status'] == 'missing'


def test_sync_tracked_positions_with_exchange_uses_side_aware_position_keys(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'status': 'monitoring',
            'quantity': 5.0,
            'remaining_quantity': 5.0,
            'stop_order_id': 123,
            'protection_status': 'protected',
        },
        'DOGEUSDT:SHORT': {
            'symbol': 'DOGEUSDT',
            'side': 'SHORT',
            'status': 'monitoring',
            'quantity': 2.0,
            'remaining_quantity': 2.0,
            'stop_order_id': 456,
            'protection_status': 'protected',
        },
    })

    result = mod.sync_tracked_positions_with_exchange(
        store,
        exchange_positions=[{'symbol': 'DOGEUSDT', 'positionAmt': '3.5', 'positionSide': 'LONG'}],
        protected_symbols=['DOGEUSDT:LONG'],
    )

    positions = store.load_json('positions', {})
    assert result['closed_symbols'] == ['DOGEUSDT:SHORT']
    assert result['refreshed_symbols'] == ['DOGEUSDT']
    assert positions['DOGEUSDT:LONG']['quantity'] == 3.5
    assert positions['DOGEUSDT:LONG']['remaining_quantity'] == 3.5
    assert positions['DOGEUSDT:LONG']['protection_status'] == 'protected'
    assert positions['DOGEUSDT:SHORT']['status'] == 'closed'
    assert positions['DOGEUSDT:SHORT']['remaining_quantity'] == 0.0
    assert positions['DOGEUSDT:SHORT']['protection_status'] == 'flat'


def test_place_live_trade_recovers_entry_order_via_query_when_post_timeout_unknown(monkeypatch):
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=132.0,
        price_change_pct_24h=18.0,
        quote_volume_24h=80_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0003,
        funding_rate_avg=0.0002,
        recent_5m_change_pct=2.4,
        acceleration_ratio_5m_vs_15m=1.6,
        breakout_level=128.0,
        recent_swing_low=124.0,
        stop_price=126.0,
        quantity=1.25,
        risk_per_unit=6.0,
        recommended_leverage=3,
        rsi_5m=74.0,
        volume_multiple=2.1,
        distance_from_ema20_5m_pct=5.2,
        distance_from_vwap_15m_pct=4.4,
        higher_tf_summary={'1h': 'up', '4h': 'up'},
        score=90.0,
        reasons=['test'],
        side='LONG',
        state='launch',
        state_reasons=['launch_short_squeeze'],
        alert_tier='critical',
        position_size_pct=3.3,
        smart_money_veto=False,
        atr_stop_distance=6.0,
    )
    args = argparse.Namespace(
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        breakeven_r=1.0,
        profile='test',
    )
    meta = make_meta()
    events = []

    class FakeClient:
        def __init__(self):
            self.order_attempts = 0
            self.calls = []

        def signed_post(self, path, params):
            self.calls.append((path, dict(params)))
            if path == '/fapi/v1/leverage':
                return {'leverage': params['leverage']}
            if path == '/fapi/v1/order':
                self.order_attempts += 1
                if self.order_attempts == 1:
                    raise mod.BinanceAPIError('APIError(code=-1007): Timeout waiting for response from backend server. Send status unknown; execution status unknown.')
                return {
                    'orderId': 12345,
                    'clientOrderId': 'entry-1',
                    'status': 'FILLED',
                    'avgPrice': '132.5',
                    'executedQty': '1.25',
                    'cumQuote': '165.625',
                    'updateTime': 1710000000123,
                }
            raise AssertionError(path)

    client = FakeClient()

    monkeypatch.setattr(mod, 'log_runtime_event', lambda event_type, payload: events.append((event_type, dict(payload))))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'place_stop_market_order', lambda *a, **k: {'orderId': 999, 'clientOrderId': 'stop-1'})
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *a, **k: {'status': 'protected', 'expected_order_id': 999})
    monkeypatch.setattr(mod, 'query_order', lambda client, symbol, order_id=None, client_order_id=None: {
        'orderId': 12345,
        'clientOrderId': client_order_id or 'entry-1',
        'status': 'FILLED',
        'avgPrice': '132.5',
        'executedQty': '1.25',
        'cumQuote': '165.625',
        'updateTime': 1710000000123,
        'symbol': symbol,
        'positionSide': 'LONG',
    })

    result = mod.place_live_trade(client, candidate, leverage=3, meta=meta, args=args)

    assert result['entry_order_feedback']['order_id'] == 12345
    assert result['entry_order_feedback']['status'] == 'FILLED'
    assert result['entry_price'] == 132.5
    assert result['filled_quantity'] == 1.25
    assert any(event_type == 'entry_order_recovered' for event_type, _ in events)


def test_place_live_trade_raises_when_timeout_unknown_cannot_be_confirmed(monkeypatch):
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=132.0,
        price_change_pct_24h=18.0,
        quote_volume_24h=80_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0003,
        funding_rate_avg=0.0002,
        recent_5m_change_pct=2.4,
        acceleration_ratio_5m_vs_15m=1.6,
        breakout_level=128.0,
        recent_swing_low=124.0,
        stop_price=126.0,
        quantity=1.25,
        risk_per_unit=6.0,
        recommended_leverage=3,
        rsi_5m=74.0,
        volume_multiple=2.1,
        distance_from_ema20_5m_pct=5.2,
        distance_from_vwap_15m_pct=4.4,
        higher_tf_summary={'1h': 'up', '4h': 'up'},
        score=90.0,
        reasons=['test'],
        side='LONG',
        state='launch',
        state_reasons=['launch_short_squeeze'],
        alert_tier='critical',
        position_size_pct=3.3,
        smart_money_veto=False,
        atr_stop_distance=6.0,
    )
    args = argparse.Namespace(
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        breakeven_r=1.0,
        profile='test',
    )
    meta = make_meta()
    events = []

    class FakeClient:
        def signed_post(self, path, params):
            if path == '/fapi/v1/leverage':
                return {'leverage': params['leverage']}
            if path == '/fapi/v1/order':
                raise mod.BinanceAPIError('APIError(code=-1007): Timeout waiting for response from backend server. Send status unknown; execution status unknown.')
            raise AssertionError(path)

    monkeypatch.setattr(mod, 'log_runtime_event', lambda event_type, payload: events.append((event_type, dict(payload))))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'query_order', lambda *a, **k: (_ for _ in ()).throw(mod.BinanceAPIError('APIError(code=-2013): Unknown order sent.')))

    try:
        mod.place_live_trade(FakeClient(), candidate, leverage=3, meta=meta, args=args)
    except mod.BinanceAPIError as exc:
        assert 'entry order status remained unknown' in str(exc)
    else:
        raise AssertionError('expected BinanceAPIError')

    assert any(event_type == 'error' and 'entry order status remained unknown' in payload.get('message', '') for event_type, payload in events)




def test_place_live_trade_hard_gates_when_existing_position_is_open(monkeypatch):
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=132.0,
        price_change_pct_24h=18.0,
        quote_volume_24h=80_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0003,
        funding_rate_avg=0.0002,
        recent_5m_change_pct=2.4,
        acceleration_ratio_5m_vs_15m=1.6,
        breakout_level=128.0,
        recent_swing_low=124.0,
        stop_price=126.0,
        quantity=1.25,
        risk_per_unit=6.0,
        recommended_leverage=3,
        rsi_5m=74.0,
        volume_multiple=2.1,
        distance_from_ema20_5m_pct=5.2,
        distance_from_vwap_15m_pct=4.4,
        higher_tf_summary={'1h': 'up', '4h': 'up'},
        score=90.0,
        reasons=['test'],
        side='LONG',
        state='launch',
        state_reasons=['launch_short_squeeze'],
        alert_tier='critical',
        position_size_pct=3.3,
        smart_money_veto=False,
        atr_stop_distance=6.0,
    )
    args = argparse.Namespace(tp1_r=1.5, tp1_close_pct=0.3, tp2_r=2.0, tp2_close_pct=0.4, breakeven_r=1.0, profile='test')
    meta = make_meta()
    events = []

    monkeypatch.setattr(mod, 'log_runtime_event', lambda event_type, payload: events.append((event_type, dict(payload))))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'TESTUSDT', 'positionSide': 'LONG', 'positionAmt': '1.0'}])
    monkeypatch.setattr(mod, 'fetch_open_orders', lambda client, symbol=None: [])

    class FakeClient:
        def signed_post(self, path, params):
            if path == '/fapi/v1/leverage':
                return {'leverage': params['leverage']}
            raise AssertionError(f'unexpected signed_post: {path}')

    try:
        mod.place_live_trade(FakeClient(), candidate, leverage=3, meta=meta, args=args)
    except mod.BinanceAPIError as exc:
        assert 'preflight hard gate' in str(exc)
        assert 'existing_position_open' in str(exc)
    else:
        raise AssertionError('expected BinanceAPIError')

    assert any(event_type == 'error' and payload.get('preflight_reason') == 'existing_position_open' for event_type, payload in events)



def test_place_live_trade_hard_gates_when_existing_open_order_present(monkeypatch):
    candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=132.0,
        price_change_pct_24h=18.0,
        quote_volume_24h=80_000_000.0,
        hot_rank=1,
        gainer_rank=1,
        funding_rate=0.0003,
        funding_rate_avg=0.0002,
        recent_5m_change_pct=2.4,
        acceleration_ratio_5m_vs_15m=1.6,
        breakout_level=128.0,
        recent_swing_low=124.0,
        stop_price=126.0,
        quantity=1.25,
        risk_per_unit=6.0,
        recommended_leverage=3,
        rsi_5m=74.0,
        volume_multiple=2.1,
        distance_from_ema20_5m_pct=5.2,
        distance_from_vwap_15m_pct=4.4,
        higher_tf_summary={'1h': 'up', '4h': 'up'},
        score=90.0,
        reasons=['test'],
        side='LONG',
        state='launch',
        state_reasons=['launch_short_squeeze'],
        alert_tier='critical',
        position_size_pct=3.3,
        smart_money_veto=False,
        atr_stop_distance=6.0,
    )
    args = argparse.Namespace(tp1_r=1.5, tp1_close_pct=0.3, tp2_r=2.0, tp2_close_pct=0.4, breakeven_r=1.0, profile='test')
    meta = make_meta()
    events = []

    monkeypatch.setattr(mod, 'log_runtime_event', lambda event_type, payload: events.append((event_type, dict(payload))))
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'fetch_open_orders', lambda client, symbol=None: [{'symbol': 'TESTUSDT', 'orderId': 321, 'type': 'STOP_MARKET'}])

    class FakeClient:
        def signed_post(self, path, params):
            if path == '/fapi/v1/leverage':
                return {'leverage': params['leverage']}
            raise AssertionError(f'unexpected signed_post: {path}')

    try:
        mod.place_live_trade(FakeClient(), candidate, leverage=3, meta=meta, args=args)
    except mod.BinanceAPIError as exc:
        assert 'preflight hard gate' in str(exc)
        assert 'existing_open_orders' in str(exc)
    else:
        raise AssertionError('expected BinanceAPIError')

    assert any(event_type == 'error' and payload.get('preflight_reason') == 'existing_open_orders' for event_type, payload in events)


def test_runtime_store_load_json_exposes_canonical_and_legacy_long_aliases(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'BTCUSDT': {
            'symbol': 'btcusdt',
            'status': 'monitoring',
            'quantity': 1.25,
        },
        'ETHUSDT:SHORT': {
            'symbol': 'ethusdt',
            'side': 'SHORT',
            'status': 'monitoring',
            'quantity': 2.0,
            'lowest_price_seen': 1725.0,
        },
    })

    positions = store.load_json('positions', {})

    assert 'BTCUSDT:LONG' in positions
    assert 'BTCUSDT' in positions
    assert positions['BTCUSDT'] is positions['BTCUSDT:LONG']
    assert positions['BTCUSDT:LONG']['symbol'] == 'BTCUSDT'
    assert positions['BTCUSDT:LONG']['side'] == 'LONG'
    assert positions['BTCUSDT:LONG']['position_key'] == 'BTCUSDT:LONG'
    assert positions['BTCUSDT:LONG']['remaining_quantity'] == 1.25
    assert positions['BTCUSDT:LONG']['current_stop_price'] is None
    assert positions['BTCUSDT:LONG']['lowest_price_seen'] is None
    assert positions['ETHUSDT:SHORT']['symbol'] == 'ETHUSDT'
    assert positions['ETHUSDT:SHORT']['side'] == 'SHORT'
    assert positions['ETHUSDT:SHORT']['position_key'] == 'ETHUSDT:SHORT'
    assert positions['ETHUSDT:SHORT']['lowest_price_seen'] == 1725.0


def test_runtime_store_append_event_backfills_side_and_position_key_from_payload(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))

    row = store.append_event('entry_filled', {
        'symbol': 'btcusdt',
        'quantity': 1.0,
    })

    assert row['symbol'] == 'BTCUSDT'
    assert row['side'] == 'LONG'
    assert row['position_key'] == 'BTCUSDT:LONG'
    assert row['quantity'] == 1.0

    rows = [mod.json.loads(line) for line in (tmp_path / 'events.jsonl').read_text(encoding='utf-8').splitlines() if line.strip()]
    assert rows[-1]['symbol'] == 'BTCUSDT'
    assert rows[-1]['side'] == 'LONG'
    assert rows[-1]['position_key'] == 'BTCUSDT:LONG'


def test_monitor_live_trade_reads_and_persists_side_aware_position_key(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    symbol = 'TESTUSDT'
    position_key = 'TESTUSDT:LONG'
    store.save_json('positions', {
        position_key: {
            'symbol': symbol,
            'side': 'LONG',
            'status': 'monitoring',
            'quantity': 1.0,
            'remaining_quantity': 1.0,
            '_debug_current_price': 111.0,
            '_debug_ema5m': 110.0,
            '_debug_trailing_reference': 112.0,
        }
    })
    args = argparse.Namespace(
        profile='test',
        max_monitor_cycles=1,
        monitor_poll_interval_sec=0,
        trailing_buffer_pct=0.01,
        disable_notify=True,
        notify_target='',
    )
    trade = {
        'symbol': symbol,
        'side': 'LONG',
        'entry_price': 100.0,
        'quantity': 1.0,
        'stop_order': {'orderId': 77},
        'protection_check': {'status': 'protected'},
        'trade_management_plan': {
            'entry_price': 100.0,
            'stop_price': 95.0,
            'quantity': 1.0,
            'initial_risk_per_unit': 5.0,
            'breakeven_trigger_price': 101.0,
            'tp1_trigger_price': 105.0,
            'tp1_close_qty': 0.5,
            'tp2_trigger_price': 110.0,
            'tp2_close_qty': 0.3,
            'runner_qty': 0.2,
            'breakeven_confirmation_mode': 'price_only',
            'breakeven_min_buffer_pct': 0.0,
            'exit_reason': None,
        },
    }

    monkeypatch.setattr(mod, 'fetch_klines', lambda *a, **k: [make_kline(100, 112, 99, 111) for _ in range(21)])
    monkeypatch.setattr(mod, 'log_runtime_event', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'place_reduce_only_market', lambda *a, **k: {'orderId': 888})

    def fake_apply(client, symbol, meta, state, action, active_stop_order):
        action['exit_reason'] = 'tp1'
        state.remaining_quantity = 0.0
        state.tp1_hit = True
        return state, None, {'reduce_order': {'orderId': 888}}

    monkeypatch.setattr(mod, 'apply_management_action', fake_apply)

    result = mod.monitor_live_trade(client=object(), symbol=symbol, meta=make_meta(), args=args, trade=trade, store=store)
    positions = store.load_json('positions', {})

    assert result['status'] == 'closed'
    assert result['exit_reason'] == 'tp1'
    assert position_key not in positions


def test_monitor_live_trade_reads_and_persists_short_side_aware_position_key(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    symbol = 'TESTUSDT'
    position_key = 'TESTUSDT:SHORT'
    store.save_json('positions', {
        position_key: {
            'symbol': symbol,
            'side': 'SHORT',
            'position_key': position_key,
            'status': 'monitoring',
            'quantity': 1.0,
            'remaining_quantity': 1.0,
            '_debug_current_price': 89.0,
            '_debug_ema5m': 90.0,
            '_debug_trailing_reference': 88.0,
        }
    })
    args = argparse.Namespace(
        profile='test',
        max_monitor_cycles=1,
        monitor_poll_interval_sec=0,
        trailing_buffer_pct=0.01,
        disable_notify=True,
        notify_target='',
    )
    trade = {
        'symbol': symbol,
        'side': 'SHORT',
        'entry_price': 100.0,
        'quantity': 1.0,
        'stop_order': {'orderId': 77},
        'protection_check': {'status': 'protected'},
        'trade_management_plan': {
            'side': 'SHORT',
            'entry_price': 100.0,
            'stop_price': 105.0,
            'quantity': 1.0,
            'initial_risk_per_unit': 5.0,
            'breakeven_trigger_price': 99.0,
            'tp1_trigger_price': 95.0,
            'tp1_close_qty': 0.5,
            'tp2_trigger_price': 90.0,
            'tp2_close_qty': 0.3,
            'runner_qty': 0.2,
            'breakeven_confirmation_mode': 'price_only',
            'breakeven_min_buffer_pct': 0.0,
            'exit_reason': None,
        },
    }

    monkeypatch.setattr(mod, 'fetch_klines', lambda *a, **k: [make_kline(100, 101, 88, 89) for _ in range(21)])
    monkeypatch.setattr(mod, 'evaluate_management_actions', lambda *a, **k: [{'type': 'take_profit_1', 'close_qty': 1.0, 'exit_reason': 'tp1'}])
    monkeypatch.setattr(mod, 'extract_lows', lambda rows: [88.0 for _ in rows])
    monkeypatch.setattr(mod, 'log_runtime_event', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'place_reduce_only_market', lambda *a, **k: {'orderId': 889})

    def fake_apply(client, symbol, meta, state, action, active_stop_order):
        action['exit_reason'] = 'tp1'
        state.remaining_quantity = 0.0
        state.tp1_hit = True
        return state, None, {'reduce_order': {'orderId': 889}}

    monkeypatch.setattr(mod, 'apply_management_action', fake_apply)

    result = mod.monitor_live_trade(client=object(), symbol=symbol, meta=make_meta(), args=args, trade=trade, store=store)
    rows = [mod.json.loads(line) for line in (tmp_path / 'events.jsonl').read_text(encoding='utf-8').splitlines() if line.strip()]
    positions = store.load_json('positions', {})
    debug_state = store.load_json('monitor_debug', {})

    assert result['status'] == 'closed'
    assert result['exit_reason'] == 'tp1'
    assert rows[-1]['event_type'] == 'trade_invalidated'
    assert rows[-1]['side'] == 'SHORT'
    assert rows[-1]['position_key'] == position_key
    assert debug_state['actions'][0]['type'] == 'take_profit_1'
    assert debug_state['current_price'] == 101.0
