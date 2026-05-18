import argparse
import datetime
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

SCRIPT_PATH = SCRIPTS_DIR / 'binance_futures_momentum_long.py'
spec = importlib.util.spec_from_file_location('binance_futures_momentum_long', SCRIPT_PATH)
assert spec is not None
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)

RUNTIME_STORE_PATH = SCRIPTS_DIR / 'runtime_store.py'
runtime_store_spec = importlib.util.spec_from_file_location('runtime_store', RUNTIME_STORE_PATH)
assert runtime_store_spec is not None
runtime_store = importlib.util.module_from_spec(runtime_store_spec)
sys.modules[runtime_store_spec.name] = runtime_store
assert runtime_store_spec.loader is not None
runtime_store_spec.loader.exec_module(runtime_store)


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


def make_candidate_request(**overrides):
    klines_5m, klines_15m, klines_1h, klines_4h = make_breakout_klines()
    base = dict(
        symbol='TESTUSDT',
        ticker=make_ticker(),
        klines_5m=klines_5m,
        klines_15m=klines_15m,
        klines_1h=klines_1h,
        klines_4h=klines_4h,
        meta=make_meta(),
        hot_rank=1,
        gainer_rank=2,
        funding_rate=-0.0014,
        funding_rate_avg=-0.0008,
        market_regime={'risk_on': True, 'score_multiplier': 1.1, 'reasons': ['btc_above_ema20'], 'label': 'risk_on'},
        legacy_kwargs={
            'risk_usdt': 10.0,
            'min_notional_usdt': 20.0,
            'max_notional_usdt': 30.0,
            'lookback_bars': 12,
            'swing_bars': 6,
            'min_5m_change_pct': 0.5,
            'min_quote_volume': 1000.0,
            'stop_buffer_pct': 0.01,
            'max_rsi_5m': 85.0,
            'min_volume_multiple': 1.0,
            'max_distance_from_ema_pct': 12.0,
            'funding_rate_threshold': 0.0005,
            'funding_rate_avg_threshold': 0.0003,
            'max_distance_from_vwap_pct': 12.0,
            'max_leverage': 5,
            'short_bias': 0.78,
            'oi_now': 1_450_000.0,
            'oi_5m_ago': 1_200_000.0,
            'oi_15m_ago': 1_050_000.0,
            'cvd_delta': 240000.0,
            'cvd_zscore': 3.2,
            'oi_notional_percentile': 0.82,
            'oi_zscore_5m': 4.1,
            'okx_sentiment_score': 0.28,
            'okx_sentiment_acceleration': 0.42,
            'sector_resonance_score': 0.64,
            'smart_money_flow_score': 0.45,
            'onchain_smart_money_score': 0.25,
        },
    )
    legacy_overrides = overrides.pop('legacy_kwargs', {})
    if legacy_overrides:
        base['legacy_kwargs'].update(legacy_overrides)
    base.update(overrides)
    return mod.BuildCandidateRequest(**base)


def build_candidate_from_request(**overrides):
    return mod._build_candidate_from_request(make_candidate_request(**overrides))


def test_prepare_build_candidate_request_inputs_merges_legacy_fields_without_mutating_request():
    request = make_candidate_request(
        okx_sentiment=None,
        smart_money_context=None,
        legacy_kwargs={
            'microstructure_inputs': {'short_bias': 0.66, 'cvd_delta': 180000.0},
            'short_bias': 0.78,
            'oi_now': 1_450_000.0,
            'oi_5m_ago': 1_200_000.0,
            'oi_15m_ago': 1_050_000.0,
            'cvd_delta': 240000.0,
            'cvd_zscore': 3.2,
            'okx_sentiment_score': 0.28,
            'okx_sentiment_acceleration': 0.42,
            'sector_resonance_score': 0.64,
            'smart_money_flow_score': 0.45,
            'risk_usdt': 10.0,
        },
    )
    original_legacy = dict(request.legacy_kwargs)

    prepared = mod.prepare_build_candidate_request_inputs(request)

    assert prepared['microstructure_inputs'] == {
        'short_bias': 0.66,
        'cvd_delta': 180000.0,
        'oi_now': 1_450_000.0,
        'oi_5m_ago': 1_200_000.0,
        'oi_15m_ago': 1_050_000.0,
        'cvd_zscore': 3.2,
    }
    assert prepared['okx_sentiment'] == {
        'okx_sentiment_score': 0.28,
        'okx_sentiment_acceleration': 0.42,
        'sector_resonance_score': 0.64,
    }
    assert prepared['smart_money_context'] == {'smart_money_flow_score': 0.45}
    assert prepared['legacy_kwargs']['risk_usdt'] == 10.0
    assert prepared['legacy_kwargs']['funding_rate_threshold'] == original_legacy['funding_rate_threshold']
    assert prepared['legacy_kwargs']['funding_rate_avg_threshold'] == original_legacy['funding_rate_avg_threshold']
    for extracted_key in (
        'microstructure_inputs',
        'okx_sentiment_score',
        'okx_sentiment_acceleration',
        'sector_resonance_score',
        'smart_money_flow_score',
    ):
        assert extracted_key not in prepared['legacy_kwargs']
    assert request.legacy_kwargs == original_legacy


def test_build_candidate_runtime_inputs_copies_sequences_and_injects_dependencies(monkeypatch):
    request = make_candidate_request(
        klines_5m=[{'close': '1'}],
        klines_15m=[{'close': '2'}],
        klines_1h=[{'close': '3'}],
        klines_4h=[{'close': '4'}],
        open_interest_rows=[{'sumOpenInterestValue': '10'}],
        taker_long_short_ratio_rows=[{'buySellRatio': '1.1'}],
        top_long_short_position_ratio_rows=[{'longShortRatio': '1.2'}],
        top_long_short_account_ratio_rows=[{'longShortRatio': '1.3'}],
        symbol_open_interest_rows_5m=[{'sumOpenInterestValue': '11'}],
        symbol_open_interest_rows_15m=[{'sumOpenInterestValue': '12'}],
    )
    prepared = {
        'microstructure_inputs': {'short_bias': 0.66},
        'okx_sentiment': {'okx_sentiment_score': 0.28},
        'smart_money_context': {'smart_money_flow_score': 0.45},
        'legacy_kwargs': {'risk_usdt': 10.0},
    }
    captured = {}

    monkeypatch.setattr(mod, 'prepare_build_candidate_request_inputs', lambda request_arg: prepared)
    monkeypatch.setattr(mod, 'build_candidate_impl', lambda **kwargs: captured.update(kwargs) or 'candidate')

    result = mod._build_candidate_from_request(request)

    assert result == 'candidate'
    assert captured['klines_5m'] == [{'close': '1'}]
    assert captured['klines_15m'] == [{'close': '2'}]
    assert captured['klines_1h'] == [{'close': '3'}]
    assert captured['klines_4h'] == [{'close': '4'}]
    assert captured['open_interest_rows'] == [{'sumOpenInterestValue': '10'}]
    assert captured['taker_long_short_ratio_rows'] == [{'buySellRatio': '1.1'}]
    assert captured['top_long_short_position_ratio_rows'] == [{'longShortRatio': '1.2'}]
    assert captured['top_long_short_account_ratio_rows'] == [{'longShortRatio': '1.3'}]
    assert captured['symbol_open_interest_rows_5m'] == [{'sumOpenInterestValue': '11'}]
    assert captured['symbol_open_interest_rows_15m'] == [{'sumOpenInterestValue': '12'}]
    assert captured['microstructure_inputs'] == {'short_bias': 0.66}
    assert captured['okx_sentiment'] == {'okx_sentiment_score': 0.28}
    assert captured['smart_money_context'] == {'smart_money_flow_score': 0.45}
    assert captured['risk_usdt'] == 10.0
    assert captured['min_notional_usdt'] == 20.0
    assert captured['max_notional_usdt'] == 30.0
    assert captured['Candidate'] is mod.Candidate
    assert captured['TRADE_SIDE_LONG'] == mod.TRADE_SIDE_LONG
    assert captured['TRADE_SIDE_SHORT'] == mod.TRADE_SIDE_SHORT
    assert captured['normalize_trade_side'] is mod.normalize_trade_side
    assert captured['trade_side_to_position_side'] is mod.trade_side_to_position_side
    assert captured['compute_control_risk_score'] is mod.compute_control_risk_score
    assert captured['classify_candidate_state'] is mod.classify_candidate_state
    assert captured['build_trade_management_plan'] is mod.build_trade_management_plan

    request.klines_5m.append({'close': 'mutated'})
    request.symbol_open_interest_rows_5m.append({'sumOpenInterestValue': '99'})
    assert captured['klines_5m'] == [{'close': '1'}]
    assert captured['symbol_open_interest_rows_5m'] == [{'sumOpenInterestValue': '11'}]


def test_build_candidate_runtime_inputs_injects_candidate_construction_tail_dependencies(monkeypatch):
    request = make_candidate_request()
    prepared = {
        'microstructure_inputs': {'short_bias': 0.66},
        'okx_sentiment': {'okx_sentiment_score': 0.28},
        'smart_money_context': {'smart_money_flow_score': 0.45},
        'legacy_kwargs': {'risk_usdt': 10.0},
    }
    captured = {}

    monkeypatch.setattr(mod, 'prepare_build_candidate_request_inputs', lambda request_arg: prepared)
    monkeypatch.setattr(mod, 'build_candidate_impl', lambda **kwargs: captured.update(kwargs) or 'candidate')

    mod._build_candidate_from_request(request)

    assert captured['merge_smart_money_scores'] is mod.merge_smart_money_scores
    assert captured['compute_relative_oi_features'] is mod.compute_relative_oi_features
    assert captured['compute_squeeze_signal'] is mod.compute_squeeze_signal
    assert captured['compute_control_risk_score'] is mod.compute_control_risk_score
    assert captured['classify_candidate_state'] is mod.classify_candidate_state
    assert captured['recommend_leverage'] is mod.recommend_leverage
    assert captured['evaluate_trigger_confirmation'] is mod.evaluate_trigger_confirmation
    assert captured['clamp'] is mod.clamp
    assert captured['classify_alert_tier'] is mod.classify_alert_tier
    assert captured['recommended_position_size_pct'] is mod.recommended_position_size_pct
    assert captured['build_trade_management_plan'] is mod.build_trade_management_plan


def test_build_candidate_request_path_preserves_trade_management_plan_fields(monkeypatch):
    sentinel = object()
    captured = {}

    def fake_build_candidate_impl(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(mod, 'build_candidate_impl', fake_build_candidate_impl)

    candidate = build_candidate_from_request()

    assert candidate is sentinel
    assert captured['build_trade_management_plan'] is mod.build_trade_management_plan
    assert captured['risk_usdt'] == 10.0
    assert captured['min_notional_usdt'] == 20.0
    assert captured['max_notional_usdt'] == 30.0
    assert captured['lookback_bars'] == 12
    assert captured['swing_bars'] == 6
    assert captured['funding_rate_threshold'] == 0.0005
    assert captured['funding_rate_avg_threshold'] == 0.0003
    assert captured['max_distance_from_vwap_pct'] == 12.0
    assert captured['max_leverage'] == 5
    assert captured['market_regime']['label'] == 'risk_on'
    assert captured['build_trade_management_plan'] is mod.build_trade_management_plan


def test_finalize_candidate_construction_appends_tail_fields_and_flags():
    candidate = mod.candidate_builder_mod.finalize_candidate_construction(
        Candidate=mod.Candidate,
        symbol='TESTUSDT',
        last_price=132.0,
        price_change_pct_24h=12.0,
        quote_volume_24h=80_000_000.0,
        hot_rank=1,
        gainer_rank=2,
        funding_rate=-0.0014,
        funding_rate_avg=-0.0008,
        recent_5m_change_pct=2.3,
        acceleration_ratio=1.8,
        breakout_level=131.0,
        recent_swing_low=127.0,
        stop_price=129.5,
        quantity=1.25,
        risk_per_unit=2.0,
        recommended_leverage=4,
        rsi_5m=68.0,
        volume_multiple=2.4,
        distance_from_ema20_5m_pct=1.2,
        distance_from_vwap_15m_pct=0.8,
        trend_1h={'allowed': True},
        trend_4h={'allowed': True},
        score=187.4,
        reasons=['tail_refactor_sentinel'],
        trade_side=mod.TRADE_SIDE_LONG,
        position_side='LONG',
        higher_timeframe_bias='long',
        oi_features={
            'oi_change_pct_5m': 8.0,
            'oi_change_pct_15m': 15.0,
            'oi_acceleration_ratio': 1.9,
            'taker_buy_ratio': 0.62,
            'oi_zscore_5m': 4.1,
            'volume_zscore_5m': 3.7,
            'bollinger_bandwidth_pct': 2.2,
            'price_above_vwap': True,
            'funding_rate_percentile_hint': 'low',
            'cvd_delta': 240000.0,
            'cvd_zscore': 3.2,
        },
        microstructure_inputs={
            'long_short_ratio': 0.88,
            'short_bias': 0.78,
        },
        atr_stop_distance=1.4,
        stop_model='atr',
        stop_distance_pct=1.06,
        stop_too_tight_flag=False,
        stop_too_wide_flag=False,
        state_payload={
            'state': 'launch',
            'state_reasons': ['launch_short_squeeze'],
            'setup_score': 18.5,
            'exhaustion_score': 1.2,
        },
        okx_sentiment_score=0.28,
        okx_sentiment_acceleration=0.42,
        sector_resonance_score=0.64,
        smart_money_effective=0.45,
        leading_payload={'score': 10.0},
        squeeze_payload={'score': 14.0},
        control_risk_payload={'score': 0.0, 'veto': False, 'veto_reason': ''},
        initial_alert_tier='critical',
        initial_position_size_pct=3.3,
        regime_label='risk_on',
        regime_multiplier=1.1,
        onchain_smart_money_score=0.25,
        smart_money_merge={'veto': False, 'veto_reason': '', 'sources': ['exchange_flow']},
        entry_distance_from_breakout_pct=0.7634,
        entry_distance_from_vwap_pct=0.8,
        overextension_flag=False,
        setup_ready=False,
        trigger_fired=False,
        expected_slippage_pct=0.2672,
        book_depth_fill_ratio=0.8664,
        liquidity_grade='B',
        funding_rate_threshold=0.0005,
        tradeability_score=0.635,
        loser_rank=None,
        trigger_confirmation={
            'flags': {
                'breakout_close_confirmed': True,
                'high_elastic_long_pullback_confirmed': False,
                'long_crowding_ok': True,
            },
            'confirmation_count': 3,
            'min_confirmations': 2,
        },
        legacy_kwargs={'oi_hard_reversal_threshold_pct': 0.8},
        waiting_breakout=True,
    )

    assert candidate.state == 'launch'
    assert candidate.state_reasons == ['launch_short_squeeze']
    assert candidate.alert_tier == 'critical'
    assert candidate.position_size_pct == 3.3
    assert candidate.squeeze_score == 14.0
    assert candidate.control_risk_score == 0.0
    assert candidate.expected_slippage_pct == 0.2672
    assert candidate.book_depth_fill_ratio == 0.8664
    assert candidate.trigger_confirmation_count == 3
    assert candidate.trigger_confirmation_flags['breakout_close_confirmed'] is True
    assert candidate.must_pass_flags['setup_ready'] is False
    assert candidate.must_pass_flags['trigger_fired'] is False
    assert candidate.must_pass_flags['long_crowding_ok'] is True
    assert candidate.reasons[-4:] == [
        'trigger_confirmation_count=3',
        'trigger_min_confirmations=2',
        'waiting_breakout',
        'alert_tier=critical',
    ] or candidate.reasons[-5:] == [
        'trigger_confirmation_count=3',
        'trigger_min_confirmations=2',
        'waiting_breakout',
        'alert_tier=critical',
        'position_size_pct=3.3',
    ]
    assert 'position_size_pct=3.3' in candidate.reasons


def test_finalize_candidate_construction_writes_probe_entry_into_must_pass_flags():
    candidate = mod.candidate_builder_mod.finalize_candidate_construction(
        Candidate=mod.Candidate,
        symbol='TESTUSDT',
        last_price=132.0,
        price_change_pct_24h=12.0,
        quote_volume_24h=80_000_000.0,
        hot_rank=1,
        gainer_rank=2,
        funding_rate=-0.0014,
        funding_rate_avg=-0.0008,
        recent_5m_change_pct=2.3,
        acceleration_ratio=1.8,
        breakout_level=131.0,
        recent_swing_low=127.0,
        stop_price=129.5,
        quantity=1.25,
        risk_per_unit=2.0,
        recommended_leverage=4,
        rsi_5m=68.0,
        volume_multiple=2.4,
        distance_from_ema20_5m_pct=1.2,
        distance_from_vwap_15m_pct=0.8,
        trend_1h={'allowed': True},
        trend_4h={'allowed': True},
        score=187.4,
        reasons=['probe_entry_flag_sentinel'],
        trade_side=mod.TRADE_SIDE_LONG,
        position_side='LONG',
        higher_timeframe_bias='long',
        oi_features={
            'oi_change_pct_5m': 8.0,
            'oi_change_pct_15m': 15.0,
            'oi_acceleration_ratio': 1.9,
            'taker_buy_ratio': 0.62,
            'oi_zscore_5m': 4.1,
            'volume_zscore_5m': 3.7,
            'bollinger_bandwidth_pct': 2.2,
            'price_above_vwap': True,
            'funding_rate_percentile_hint': 'low',
            'cvd_delta': 240000.0,
            'cvd_zscore': 3.2,
        },
        microstructure_inputs={
            'long_short_ratio': 0.88,
            'short_bias': 0.78,
        },
        atr_stop_distance=1.4,
        stop_model='atr',
        stop_distance_pct=1.06,
        stop_too_tight_flag=False,
        stop_too_wide_flag=False,
        state_payload={
            'state': 'launch',
            'state_reasons': ['launch_short_squeeze'],
            'setup_score': 18.5,
            'exhaustion_score': 1.2,
        },
        okx_sentiment_score=0.28,
        okx_sentiment_acceleration=0.42,
        sector_resonance_score=0.64,
        smart_money_effective=0.45,
        leading_payload={'score': 10.0},
        squeeze_payload={'score': 14.0},
        control_risk_payload={'score': 0.0, 'veto': False, 'veto_reason': ''},
        initial_alert_tier='critical',
        initial_position_size_pct=3.3,
        regime_label='risk_on',
        regime_multiplier=1.1,
        onchain_smart_money_score=0.25,
        smart_money_merge={'veto': False, 'veto_reason': '', 'sources': ['exchange_flow']},
        entry_distance_from_breakout_pct=0.7634,
        entry_distance_from_vwap_pct=0.8,
        overextension_flag=False,
        setup_ready=True,
        trigger_fired=True,
        expected_slippage_pct=0.2672,
        book_depth_fill_ratio=0.8664,
        liquidity_grade='B',
        funding_rate_threshold=0.0005,
        tradeability_score=0.635,
        loser_rank=None,
        trigger_confirmation={
            'flags': {
                'breakout_close_confirmed': True,
                'probe_entry': True,
                'long_crowding_ok': True,
            },
            'confirmation_count': 3,
            'min_confirmations': 2,
        },
        legacy_kwargs={'oi_hard_reversal_threshold_pct': 0.8},
        waiting_breakout=False,
    )

    assert candidate.must_pass_flags['probe_entry'] is True


def test_build_candidate_request_path_preserves_tail_outputs_after_candidate_construction_refactor(monkeypatch):
    sentinel_candidate = mod.Candidate(
        symbol='TESTUSDT',
        last_price=132.0,
        price_change_pct_24h=12.0,
        quote_volume_24h=80_000_000.0,
        hot_rank=1,
        gainer_rank=2,
        funding_rate=-0.0014,
        funding_rate_avg=-0.0008,
        recent_5m_change_pct=2.3,
        acceleration_ratio_5m_vs_15m=1.8,
        breakout_level=131.0,
        recent_swing_low=127.0,
        stop_price=129.5,
        quantity=1.25,
        risk_per_unit=2.0,
        recommended_leverage=4,
        rsi_5m=68.0,
        volume_multiple=2.4,
        distance_from_ema20_5m_pct=1.2,
        distance_from_vwap_15m_pct=0.8,
        higher_tf_summary={'1h': {'allowed': True}, '4h': {'allowed': True}},
        score=187.4,
        reasons=['tail_refactor_sentinel'],
        side=mod.TRADE_SIDE_LONG,
        position_side='LONG',
        trigger_type='breakout',
        higher_timeframe_bias='long',
        oi_change_pct_5m=8.0,
        oi_change_pct_15m=15.0,
        oi_acceleration_ratio=1.9,
        taker_buy_ratio=0.62,
        long_short_ratio=0.88,
        short_bias=0.78,
        oi_zscore_5m=4.1,
        volume_zscore_5m=3.7,
        bollinger_bandwidth_pct=2.2,
        price_above_vwap=True,
        funding_rate_percentile_hint='low',
        cvd_delta=240000.0,
        cvd_zscore=3.2,
        atr_stop_distance=1.4,
        stop_model='atr',
        stop_distance_pct=1.06,
        stop_too_tight_flag=False,
        stop_too_wide_flag=False,
        state='launch',
        state_reasons=['launch_short_squeeze'],
        setup_score=18.5,
        exhaustion_score=1.2,
        okx_sentiment_score=0.28,
        okx_sentiment_acceleration=0.42,
        sector_resonance_score=0.64,
        smart_money_flow_score=0.45,
        leading_sentiment_delta=10.0,
        squeeze_score=14.0,
        control_risk_score=0.0,
        alert_tier='critical',
        position_size_pct=3.3,
        regime_label='risk_on',
        regime_multiplier=1.1,
        onchain_smart_money_score=0.25,
        smart_money_veto=False,
        smart_money_veto_reason='',
        smart_money_sources=['exchange_flow'],
        entry_distance_from_breakout_pct=0.7634,
        entry_distance_from_vwap_pct=0.8,
        overextension_flag=False,
        setup_ready=False,
        trigger_fired=False,
        expected_slippage_pct=0.2672,
        book_depth_fill_ratio=0.8664,
        loser_rank=None,
        trigger_confirmation_flags={
            'breakout_close_confirmed': True,
            'high_elastic_long_pullback_confirmed': False,
            'long_crowding_ok': True,
        },
        trigger_confirmation_count=3,
        trigger_min_confirmations=2,
        oi_hard_reversal_threshold_pct=0.8,
        portfolio_narrative_bucket='',
        portfolio_correlation_group='',
    )
    sentinel_candidate.must_pass_flags = {
        'breakout_close_confirmed': True,
        'setup_ready': False,
        'trigger_fired': False,
    }

    monkeypatch.setattr(mod, 'build_candidate_impl', lambda **kwargs: sentinel_candidate)

    candidate = build_candidate_from_request()

    assert candidate is sentinel_candidate
    assert candidate.state == 'launch'
    assert candidate.state_reasons == ['launch_short_squeeze']
    assert candidate.alert_tier == 'critical'
    assert candidate.position_size_pct == 3.3
    assert candidate.squeeze_score == 14.0
    assert candidate.control_risk_score == 0.0
    assert candidate.expected_slippage_pct == 0.2672
    assert candidate.book_depth_fill_ratio == 0.8664
    assert candidate.trigger_confirmation_count == 3
    assert candidate.trigger_confirmation_flags['breakout_close_confirmed'] is True
    assert candidate.must_pass_flags['setup_ready'] is False


def test_compute_leading_sentiment_signal_rewards_early_turn_and_penalizes_overheated_sentiment():
    payload = mod.compute_leading_sentiment_signal(okx_sentiment_score=0.2, okx_sentiment_acceleration=0.4)
    assert payload['score'] > 8
    assert 'sentiment_early_turn_zone' in payload['reasons']
    assert 'sentiment_acceleration_turn' in payload['reasons']

    overheated = mod.compute_leading_sentiment_signal(okx_sentiment_score=0.82, okx_sentiment_acceleration=0.1)
    assert overheated['score'] < 0
    assert 'sentiment_too_hot' in overheated['reasons']


def test_compute_leading_sentiment_signal_is_side_aware_for_shorts():
    payload = mod.compute_leading_sentiment_signal(okx_sentiment_score=-0.2, okx_sentiment_acceleration=-0.4, side=mod.TRADE_SIDE_SHORT)
    assert payload['score'] > 8
    assert 'sentiment_early_turn_zone_short' in payload['reasons']
    assert 'sentiment_acceleration_turn_short' in payload['reasons']

    overheated = mod.compute_leading_sentiment_signal(okx_sentiment_score=-0.82, okx_sentiment_acceleration=-0.1, side=mod.TRADE_SIDE_SHORT)
    assert overheated['score'] < 0
    assert 'sentiment_too_hot_short' in overheated['reasons']


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


def test_compute_squeeze_and_control_risk_scores_are_side_aware_for_shorts():
    squeeze = mod.compute_squeeze_signal(
        funding_rate=0.0016,
        funding_rate_avg=0.0008,
        short_bias=0.12,
        oi_zscore_5m=4.0,
        cvd_delta=-260000.0,
        cvd_zscore=-3.5,
        recent_5m_change_pct=2.2,
        side=mod.TRADE_SIDE_SHORT,
    )
    assert squeeze['score'] >= 25
    assert 'positive_funding_crowded_longs' in squeeze['reasons']
    assert 'retail_long_bias' in squeeze['reasons']
    assert 'negative_cvd_confirmation' in squeeze['reasons']

    risk = mod.compute_control_risk_score(
        short_bias=0.76,
        oi_notional_percentile=0.985,
        smart_money_flow_score=0.45,
        side=mod.TRADE_SIDE_SHORT,
    )
    assert risk['score'] >= 20
    assert risk['veto'] is True
    assert risk['veto_reason'] == 'smart_money_long_pressure_veto'
    assert 'oi_at_extreme_percentile' in risk['reasons']
    assert 'crowded_short_side' in risk['reasons']
    assert 'smart_money_long_pressure_risk' in risk['reasons']


def test_merge_smart_money_scores_triggers_veto_for_persistent_outflow():
    payload = mod.merge_smart_money_scores(exchange_score=-0.4, onchain_score=-0.7)
    assert round(payload['score'], 2) == -0.55
    assert payload['veto'] is True
    assert payload['veto_reason'] == 'smart_money_outflow_veto'
    assert payload['sources'] == ['exchange', 'onchain']


def test_merge_smart_money_scores_triggers_veto_for_persistent_long_pressure_on_shorts():
    payload = mod.merge_smart_money_scores(exchange_score=0.55, onchain_score=0.42, side=mod.TRADE_SIDE_SHORT)
    assert abs(payload['score'] - 0.485) < 1e-9
    assert payload['veto'] is True
    assert payload['veto_reason'] == 'smart_money_long_pressure_veto'
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


def test_build_candidate_request_path_labels_short_squeeze_launch_state_when_funding_negative_and_retail_short_heavy():
    candidate = build_candidate_from_request()

    assert candidate is not None
    assert candidate.state == 'launch'
    assert candidate.alert_tier == 'critical'
    assert candidate.position_size_pct == 3.3
    assert candidate.smart_money_veto is False
    assert 'launch_short_squeeze' in candidate.state_reasons
    assert any(reason.startswith('position_size_pct=') for reason in candidate.reasons)


def test_build_candidate_request_path_blocks_when_smart_money_veto_and_distribution_risk_present():
    candidate = build_candidate_from_request(
        gainer_rank=1,
        funding_rate=0.0001,
        funding_rate_avg=0.0001,
        market_regime={'risk_on': True, 'score_multiplier': 1.0, 'reasons': [], 'label': 'risk_on'},
        legacy_kwargs={
            'short_bias': 0.1,
            'oi_now': 1_600_000.0,
            'oi_5m_ago': 1_640_000.0,
            'oi_15m_ago': 1_680_000.0,
            'cvd_delta': -260000.0,
            'cvd_zscore': -3.4,
            'oi_notional_percentile': 0.992,
            'oi_zscore_5m': 2.8,
            'okx_sentiment_score': 0.75,
            'okx_sentiment_acceleration': -0.05,
            'sector_resonance_score': 0.15,
            'smart_money_flow_score': -0.72,
            'onchain_smart_money_score': -0.64,
        },
    )

    assert candidate is not None
    assert candidate.smart_money_veto is True
    assert mod.apply_hard_veto_filters(candidate) == 'smart_money_outflow_veto'


def test_build_candidate_request_path_sets_staged_entry_and_slippage_fields():
    candidate = build_candidate_from_request()

    assert candidate is not None
    assert candidate.setup_ready is False
    assert candidate.trigger_fired is False
    assert candidate.trigger_confirmation_count >= 2
    assert candidate.trigger_confirmation_flags['breakout_close_confirmed'] is True
    assert candidate.trigger_confirmation_flags['high_elastic_long_pullback_confirmed'] is False
    assert candidate.trigger_confirmation_flags['long_crowding_ok'] is True
    assert candidate.overextension_flag is False
    assert candidate.stop_model in {'structure', 'atr', 'blended'}
    assert candidate.stop_distance_pct > 0
    assert candidate.stop_too_tight_flag is False
    assert candidate.entry_distance_from_breakout_pct > 0
    assert candidate.expected_slippage_pct == round(candidate.entry_distance_from_breakout_pct * 0.35, 4)
    assert 0.0 <= candidate.book_depth_fill_ratio <= 1.0


def test_apply_candidate_diagnostics_surfaces_pullback_stage_requirements_for_high_elastic_long():
    candidate = build_candidate_from_request()
    candidate = mod.apply_candidate_diagnostics(candidate)

    assert candidate.candidate_stage == 'watch_candidate'
    assert 'elastic_pullback_not_confirmed' in candidate.setup_missing
    assert 'candidate_setup_not_ready' in candidate.trade_missing
    assert candidate.trigger_confirmation_flags['breakout_close_confirmed'] is True
    assert candidate.trigger_confirmation_flags['high_elastic_long_pullback_confirmed'] is False


def test_build_candidate_request_path_records_atr_stop_and_fee_aware_edge_contract():
    candidate = build_candidate_from_request()

    assert candidate is not None
    assert candidate.atr_stop_distance > 0
    assert candidate.stop_model in {'atr', 'blended'}
    assert candidate.expected_edge > 0
    assert candidate.expected_total_fee_pct > 0
    assert candidate.execution_slippage_buffer_pct > 0
    assert candidate.min_profit_buffer_pct > 0
    assert candidate.expected_edge > (
        candidate.expected_total_fee_pct
        + candidate.execution_slippage_buffer_pct
        + candidate.min_profit_buffer_pct
    )


def test_build_candidate_request_path_blocks_when_expected_slippage_and_overextension_stay_high():
    request = make_candidate_request(
        ticker={'symbol': 'TESTUSDT', 'priceChangePercent': '12', 'quoteVolume': '80000000', 'lastPrice': '141.0'},
        market_regime={'risk_on': True, 'score_multiplier': 1.0, 'reasons': [], 'label': 'risk_on'},
        legacy_kwargs={
            'max_rsi_5m': 95.0,
            'funding_rate_threshold': 0.0005,
            'funding_rate_avg_threshold': 0.0003,
            'short_bias': 0.2,
            'oi_now': 1_300_000.0,
            'oi_5m_ago': 1_240_000.0,
            'oi_15m_ago': 1_180_000.0,
            'cvd_delta': 160000.0,
            'cvd_zscore': 2.4,
            'oi_notional_percentile': 0.7,
            'oi_zscore_5m': 2.5,
            'okx_sentiment_score': 0.12,
            'okx_sentiment_acceleration': 0.1,
            'sector_resonance_score': 0.15,
            'smart_money_flow_score': 0.18,
            'onchain_smart_money_score': 0.0,
        },
    )
    request.klines_5m[-1][4] = '141.0'
    request.klines_5m[-1][2] = '141.5'
    request.klines_15m[-1][4] = '141.0'
    request.klines_15m[-1][2] = '141.5'

    candidate = mod._build_candidate_from_request(request)

    assert candidate is not None
    assert candidate.overextension_flag is False
    assert candidate.setup_ready is False
    assert candidate.trigger_fired is False
    assert candidate.trigger_confirmation_flags['high_elastic_long_pullback_confirmed'] is False
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
        stop_model='blended',
        stop_distance_pct=6.44,
        stop_too_tight_flag=False,
        stop_too_wide_flag=True,
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
    assert alert['stop_model'] == 'blended'
    assert alert['stop_distance_pct'] == 6.44
    assert alert['stop_too_tight_flag'] is False
    assert alert['stop_too_wide_flag'] is True


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
        tradeability_score=67.8,
        expected_edge=2.34,
        expected_total_fee_pct=0.28,
        execution_slippage_buffer_pct=0.41,
        min_profit_buffer_pct=0.55,
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
    assert alert['tradeability_score'] == 67.8
    assert alert['expected_edge'] == 2.34
    assert alert['expected_total_fee_pct'] == 0.28
    assert alert['execution_slippage_buffer_pct'] == 0.41
    assert alert['min_profit_buffer_pct'] == 0.55


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
        require_book_ticker_ws=False,
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
        require_book_ticker_ws=False,
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


def test_run_loop_okx_simulated_reconcile_clears_stale_position_before_max_open_check(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'status': 'monitoring',
            'quantity': 5.0,
            'remaining_quantity': 5.0,
            'stop_order_id': 321,
            'protection_status': 'simulated',
            'trade_management_plan': {'quantity': 5.0},
        },
    })
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
        repair_missing_protection=False,
        require_book_ticker_ws=False,
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
    placed = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda _args: store)
    monkeypatch.setattr(mod, 'run_scan_once', lambda *a, **k: ({'ok': True, 'candidate_count': 1, 'candidates': [{'symbol': 'TESTUSDT'}]}, candidate, {'TESTUSDT': make_meta()}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda _store: mod.default_risk_state())
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, requested_leverage, meta, passed_args: placed.append((client, best_candidate.symbol, requested_leverage)) or {
        'symbol': best_candidate.symbol,
        'side': 'LONG',
        'filled_quantity': 1.0,
        'entry_price': 132.0,
        'entry_order_feedback': {'orderId': 'abc'},
        'trade_management_plan': {'quantity': 1.0},
        'stop_order': {},
        'protection_check': {'status': 'protected'},
    })

    result = mod.run_loop(client=object(), args=args)
    positions = store.load_json('positions', {})

    assert len(placed) == 1
    assert placed[0][1:] == ('TESTUSDT', 3)
    assert result['cycles'][0].get('live_skipped_due_to_existing_positions', []) == []
    assert positions['DOGEUSDT:LONG']['status'] == 'closed'
    assert positions['TESTUSDT:LONG']['status'] in {'open', 'recovery_pending'}


def test_run_loop_reconcile_closed_symbol_enters_cooldown_before_same_symbol_reentry(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'COSUSDT:LONG': {
            'symbol': 'COSUSDT',
            'side': 'LONG',
            'status': 'monitoring',
            'quantity': 12.0,
            'remaining_quantity': 12.0,
            'stop_order_id': 654,
            'protection_status': 'simulated',
            'trade_management_plan': {'quantity': 12.0},
        },
    })
    args = argparse.Namespace(
        reconcile_only=False,
        halt_on_orphan_position=False,
        daily_max_loss_usdt=0.0,
        max_consecutive_losses=0,
        symbol_cooldown_minutes=15,
        live=True,
        max_open_positions=1,
        profile='test',
        auto_loop=False,
        disable_notify=True,
        notify_target='',
        repair_missing_protection=False,
        require_book_ticker_ws=False,
    )
    candidate = mod.Candidate(
        symbol='COSUSDT',
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
        setup_ready=True,
        trigger_fired=True,
        book_depth_fill_ratio=1.0,
        expected_slippage_pct=0.0,
        spread_bps=0.0,
        orderbook_slope=0.0,
        cancel_rate=0.0,
        must_pass_flags={'setup_ready': True, 'trigger_fired': True},
    )
    placed = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda _args: store)
    monkeypatch.setattr(mod, 'run_scan_once', lambda *a, **k: ({'ok': True, 'candidate_count': 1, 'candidates': [{'symbol': 'COSUSDT'}]}, candidate, {'COSUSDT': make_meta()}))
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'place_live_trade', lambda *a, **k: placed.append(True) or {'symbol': 'COSUSDT'})

    result = mod.run_loop(client=object(), args=args)
    positions = store.load_json('positions', {})
    risk_state = mod.load_risk_state(store)

    assert placed == []
    assert positions['COSUSDT:LONG']['status'] == 'closed'
    assert result['cycles'][0]['risk_guard']['allowed'] is False
    assert 'symbol_cooldown_active' in result['cycles'][0]['risk_guard']['reasons']
    assert int(risk_state['symbol_cooldowns']['COSUSDT']) > int(mod.time.time())


def test_evaluate_risk_guards_blocks_candidate_when_expected_edge_fails_total_cost_floor():
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
        setup_ready=True,
        trigger_fired=True,
        book_depth_fill_ratio=0.91,
        expected_slippage_pct=0.2,
        tradeability_score=0.71,
        must_pass_flags={'setup_ready': True, 'trigger_fired': True},
    )
    candidate.expected_edge = 0.45
    candidate.expected_total_fee_pct = 0.18
    candidate.execution_slippage_buffer_pct = 0.17
    candidate.min_profit_buffer_pct = 0.15

    result = mod.evaluate_risk_guards(candidate=candidate)

    assert result['allowed'] is False
    assert 'candidate_edge_after_costs_insufficient' in result['reasons']


def test_evaluate_risk_guards_blocks_candidate_when_expected_edge_is_zero_against_positive_cost_floor():
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
        setup_ready=True,
        trigger_fired=True,
        book_depth_fill_ratio=0.91,
        expected_slippage_pct=0.2,
        tradeability_score=0.71,
        must_pass_flags={'setup_ready': True, 'trigger_fired': True},
    )
    candidate.expected_edge = 0.0
    candidate.expected_total_fee_pct = 0.18
    candidate.execution_slippage_buffer_pct = 0.17
    candidate.min_profit_buffer_pct = 0.15

    result = mod.evaluate_risk_guards(candidate=candidate)

    assert result['allowed'] is False
    assert 'candidate_edge_after_costs_insufficient' in result['reasons']


def test_run_auto_loop_user_data_stream_monitor_cycles_existing_listen_key_and_emits_alert(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('user_data_stream', {'listen_key': 'lk-1', 'symbol': 'TESTUSDT'})
    store.save_json('positions', {
        'TESTUSDT:LONG': {
            'symbol': 'TESTUSDT',
            'position_side': 'LONG',
        }
    })
    args = argparse.Namespace(
        user_stream_refresh_interval_minutes=12.5,
        user_stream_disconnect_timeout_minutes=34.0,
        disable_notify=False,
        notify_target='',
    )
    fake_monitor = {
        'status': 'refresh_failed',
        'action': 'refresh_failed',
        'listen_key': 'lk-1',
        'error': 'boom',
        'health': {
            'symbol': 'TESTUSDT',
            'detail': 'boom',
            'disconnect_count': 2,
            'refresh_failure_count': 3,
            'reconnect_count': 4,
            'started_at': '2026-05-10T11:00:00+00:00',
            'last_refresh_at': '2026-05-10T11:30:00+00:00',
            'updated_at': '2026-05-10T11:31:00+00:00',
        },
        'now_utc': '2026-05-10T11:31:00+00:00',
    }
    recorded_notifications = []
    client = object()

    def fake_cycle(**kwargs):
        assert kwargs['client'] is client
        assert kwargs['store'] is store
        assert kwargs['symbol'] == 'TESTUSDT'
        assert kwargs['refresh_interval_minutes'] == 12.5
        assert kwargs['disconnect_timeout_minutes'] == 34.0
        return dict(fake_monitor)

    def fake_emit_notification(notify_args, event_type, payload):
        recorded_notifications.append((notify_args, event_type, payload))

    monkeypatch.setattr(mod, 'run_user_data_stream_monitor_cycle', fake_cycle)
    monkeypatch.setattr(mod, 'emit_notification', fake_emit_notification)

    result = mod.run_auto_loop_user_data_stream_monitor(client=client, store=store, args=args)
    positions_state = store.load_json('positions', {})

    assert result['monitor'] == fake_monitor
    assert result['alert']['status'] == 'refresh_failed'
    assert result['alert']['symbol'] == 'TESTUSDT'
    assert positions_state['TESTUSDT:LONG']['user_data_stream']['listen_key'] == 'lk-1'
    assert recorded_notifications == [(args, 'user_data_stream_alert', result['alert'])]


def test_run_auto_loop_user_data_stream_monitor_returns_empty_payload_without_existing_listen_key(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    args = argparse.Namespace()

    result = mod.run_auto_loop_user_data_stream_monitor(client=object(), store=store, args=args)

    assert result == {'monitor': None, 'alert': None}


def test_run_auto_loop_book_ticker_websocket_monitor_starts_background_supervisor_and_reads_health(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    args = argparse.Namespace(auto_loop=True)
    ws_module = object()
    ensure_calls = []

    monkeypatch.setattr(mod, 'websocket', ws_module)
    monkeypatch.setattr(mod, 'resolve_auto_loop_book_ticker_symbols', lambda client, args: ['BTCUSDT', 'ETHUSDT'])

    def fake_ensure(*, store, symbol_provider, ws_module):
        ensure_calls.append({
            'store': store,
            'ws_module': ws_module,
            'provider_symbols': list(symbol_provider()),
        })
        store.save_json('book_ticker_ws_status', {'status': 'healthy', 'messages_processed': 7})
        return {
            'mode': 'background_thread',
            'thread_name': 'book-ticker-ws-supervisor',
            'symbols': ['BTCUSDT', 'ETHUSDT'],
            'running': True,
        }

    monkeypatch.setattr(mod, 'ensure_auto_loop_book_ticker_websocket_supervisor_running', fake_ensure)

    result = mod.run_auto_loop_book_ticker_websocket_monitor(client=object(), store=store, args=args)

    assert ensure_calls == [{
        'store': store,
        'ws_module': ws_module,
        'provider_symbols': ['BTCUSDT', 'ETHUSDT'],
    }]
    assert result == {
        'status': 'available',
        'summary': {
            'mode': 'background_thread',
            'thread_name': 'book-ticker-ws-supervisor',
            'symbols': ['BTCUSDT', 'ETHUSDT'],
            'running': True,
        },
        'health': {'status': 'healthy', 'messages_processed': 7},
    }


def test_run_auto_loop_book_ticker_websocket_monitor_marks_unavailable_without_websocket(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    args = argparse.Namespace(auto_loop=True)
    events = []

    monkeypatch.setattr(mod, 'websocket', None)
    monkeypatch.setattr(
        mod,
        'append_rate_limited_runtime_event',
        lambda store_arg, event_type, payload, **kwargs: events.append({
            'store': store_arg,
            'event_type': event_type,
            'payload': payload,
            'kwargs': kwargs,
        }),
    )

    result = mod.run_auto_loop_book_ticker_websocket_monitor(client=object(), store=store, args=args)

    assert result == {
        'status': 'unavailable',
        'summary': {'status': 'unavailable', 'reason': 'websocket_client_missing'},
        'health': {},
    }
    assert events == [{
        'store': store,
        'event_type': 'book_ticker_ws_unavailable',
        'payload': {
            'event_source': 'book_ticker_websocket',
            'reason': 'websocket_client_missing',
        },
        'kwargs': {'key': 'global', 'min_interval_seconds': 3600.0},
    }]


def test_run_loop_allows_live_trade_when_book_ticker_ws_unavailable_and_gate_disabled(monkeypatch, tmp_path):
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
        repair_missing_protection=False,
        require_book_ticker_ws=False,
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
    placed = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda _args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda *a, **k: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *a, **k: ({'ok': True, 'candidate_count': 1, 'candidates': [{'symbol': 'TESTUSDT'}]}, candidate, {'TESTUSDT': make_meta()}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda _store: mod.default_risk_state())
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'run_auto_loop_book_ticker_websocket_monitor', lambda client, store, args: {'status': 'unavailable', 'summary': {'status': 'unavailable', 'reason': 'websocket_client_missing'}, 'health': {}})
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, requested_leverage, meta, passed_args: placed.append((best_candidate.symbol, requested_leverage)) or {
        'symbol': best_candidate.symbol,
        'side': 'LONG',
        'filled_quantity': 1.0,
        'entry_price': 132.0,
        'entry_order_feedback': {'orderId': 'abc'},
        'trade_management_plan': {'quantity': 1.0},
        'stop_order': {},
        'protection_check': {'status': 'protected'},
    })
    monkeypatch.setattr(mod, 'monitor_live_trade', lambda **kwargs: {'status': 'closed', 'exit_reason': 'test'})

    result = mod.run_loop(client=object(), args=args)

    assert placed == [('TESTUSDT', 3)]
    assert result['cycles'][0].get('live_skipped_due_to_websocket_gate', []) == []


def test_run_loop_allows_live_probe_entry_for_10u_aggressive_waiting_breakout(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    args = argparse.Namespace(
        reconcile_only=False,
        halt_on_orphan_position=False,
        daily_max_loss_usdt=0.0,
        max_consecutive_losses=0,
        symbol_cooldown_minutes=0,
        live=True,
        max_open_positions=1,
        profile='10u-aggressive',
        auto_loop=False,
        disable_notify=True,
        notify_target='',
        repair_missing_protection=False,
        require_book_ticker_ws=False,
        leverage=5,
        sim_probe_entry_enabled=True,
        sim_probe_size_ratio=0.3,
        sim_probe_min_score=58.0,
        sim_probe_max_breakout_distance_pct=0.6,
        binance_simulated_trading=False,
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
        setup_ready=True,
        trigger_fired=False,
        entry_distance_from_breakout_pct=-0.2,
        position_size_pct=1.5,
    )
    placed = []

    monkeypatch.setattr(mod, 'compute_execution_quality_size_adjustment', lambda _candidate: {'execution_liquidity_grade': 'A', 'expected_slippage_r': 0.05})
    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda _args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda *a, **k: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *a, **k: ({'ok': True, 'candidate_count': 1, 'candidates': [{'symbol': 'TESTUSDT'}]}, candidate, {'TESTUSDT': make_meta()}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda _store: mod.default_risk_state())
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': False, 'reasons': ['candidate_trigger_not_fired'], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'place_live_trade', lambda client, best_candidate, requested_leverage, meta, passed_args: placed.append((best_candidate.symbol, requested_leverage, best_candidate.quantity, getattr(best_candidate, 'probe_entry', False))) or {
        'symbol': best_candidate.symbol,
        'side': 'LONG',
        'filled_quantity': best_candidate.quantity,
        'entry_price': 132.0,
        'entry_order_feedback': {'orderId': 'abc'},
        'trade_management_plan': {'quantity': best_candidate.quantity},
        'stop_order': {},
        'protection_check': {'status': 'protected'},
    })
    monkeypatch.setattr(mod, 'monitor_live_trade', lambda **kwargs: {'status': 'closed', 'exit_reason': 'test'})

    result = mod.run_loop(client=object(), args=args)

    assert placed == [('TESTUSDT', 5, 0.3, True)]
    assert result['cycles'][0]['sim_probe_entry']['allowed'] is True
    assert result['cycles'][0]['risk_guard']['probe_override'] is True


def test_run_loop_skips_live_trade_when_book_ticker_ws_unavailable_and_gate_required(monkeypatch, tmp_path):
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
        repair_missing_protection=False,
        require_book_ticker_ws=True,
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
    placed = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda _args: store)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda *a, **k: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': []})
    monkeypatch.setattr(mod, 'run_scan_once', lambda *a, **k: ({'ok': True, 'candidate_count': 1, 'candidates': [{'symbol': 'TESTUSDT'}]}, candidate, {'TESTUSDT': make_meta()}))
    monkeypatch.setattr(mod, 'load_risk_state', lambda _store: mod.default_risk_state())
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': [], 'cooldown_until': None, 'normalized_risk_state': mod.default_risk_state()})
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])
    monkeypatch.setattr(mod, 'run_auto_loop_book_ticker_websocket_monitor', lambda client, store, args: {'status': 'unavailable', 'summary': {'status': 'unavailable', 'reason': 'websocket_client_missing'}, 'health': {}})
    monkeypatch.setattr(mod, 'place_live_trade', lambda *a, **k: placed.append('called'))

    result = mod.run_loop(client=object(), args=args)

    assert placed == []
    assert result['cycles'][0]['live_skipped_due_to_websocket_gate'] == ['book_ticker_websocket_unavailable:websocket_client_missing']


def test_build_auto_loop_user_data_stream_monitor_config_reads_explicit_fields_only():
    args = argparse.Namespace(
        user_stream_refresh_interval_minutes=12.5,
        user_stream_disconnect_timeout_minutes=34.0,
        unrelated_field='ignored',
    )

    config = mod.build_auto_loop_user_data_stream_monitor_config(args)

    assert config == mod.AutoLoopUserDataStreamMonitorConfig(
        refresh_interval_minutes=12.5,
        disconnect_timeout_minutes=34.0,
    )


def test_run_auto_loop_user_data_stream_monitor_uses_explicit_config_without_args_namespace(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('user_data_stream', {'listen_key': 'lk-1', 'symbol': 'TESTUSDT'})
    recorded = {}

    def fake_cycle(**kwargs):
        recorded.update(kwargs)
        return {
            'status': 'healthy',
            'listen_key': 'lk-1',
            'symbol': 'TESTUSDT',
            'refresh_failure_count': 0,
            'disconnect_count': 0,
        }

    monkeypatch.setattr(mod, 'run_user_data_stream_monitor_cycle', fake_cycle)
    monkeypatch.setattr(mod, 'emit_user_data_stream_alert_if_needed', lambda args, symbol, monitor: None)

    config = mod.AutoLoopUserDataStreamMonitorConfig(
        refresh_interval_minutes=22.0,
        disconnect_timeout_minutes=44.0,
    )

    result = mod.run_auto_loop_user_data_stream_monitor(
        client=object(),
        store=store,
        args=object(),
        config=config,
    )

    assert recorded['refresh_interval_minutes'] == 22.0
    assert recorded['disconnect_timeout_minutes'] == 44.0
    assert result['monitor']['symbol'] == 'TESTUSDT'
    assert result['alert'] is None


def test_build_user_data_stream_position_payload_keeps_monitor_contract():
    payload = mod.build_user_data_stream_position_payload({
        'status': 'refresh_failed',
        'listen_key': 'lk-1',
        'health': {'symbol': 'TESTUSDT', 'disconnect_count': 2},
        'action': 'refresh_failed',
        'now_utc': '2026-05-10T11:31:00+00:00',
        'ignored': 'value',
    })

    assert payload == {
        'status': 'refresh_failed',
        'listen_key': 'lk-1',
        'health': {'symbol': 'TESTUSDT', 'disconnect_count': 2},
        'action': 'refresh_failed',
        'now_utc': '2026-05-10T11:31:00+00:00',
    }


def test_build_user_data_stream_position_payload_masks_listen_key_and_previous_listen_key():
    payload = mod.build_user_data_stream_position_payload({
        'status': 'refresh_failed',
        'listen_key': 'listen-key-123456',
        'previous_listen_key': 'previous-key-654321',
        'health': {
            'symbol': 'TESTUSDT',
            'listen_key': 'listen-key-123456',
            'previous_listen_key': 'previous-key-654321',
            'disconnect_count': 2,
        },
        'action': 'restarted_after_missing_listen_key',
        'now_utc': '2026-05-10T11:31:00+00:00',
    })

    assert payload == {
        'status': 'refresh_failed',
        'listen_key': 'list***3456',
        'previous_listen_key': 'prev***4321',
        'health': {
            'symbol': 'TESTUSDT',
            'listen_key': 'list***3456',
            'previous_listen_key': 'prev***4321',
            'disconnect_count': 2,
        },
        'action': 'restarted_after_missing_listen_key',
        'now_utc': '2026-05-10T11:31:00+00:00',
    }


def test_persist_user_data_stream_monitor_to_positions_updates_only_matching_symbol_records(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'TESTUSDT:LONG': {
            'symbol': 'TESTUSDT',
            'position_side': 'LONG',
            'status': 'monitoring',
        },
        'BTCUSDT:LONG': {
            'symbol': 'BTCUSDT',
            'position_side': 'LONG',
            'status': 'monitoring',
        },
    })
    monitor = {
        'status': 'refresh_failed',
        'listen_key': 'lk-1',
        'action': 'refresh_failed',
        'now_utc': '2026-05-10T11:31:00+00:00',
        'health': {
            'symbol': 'TESTUSDT',
            'disconnect_count': 2,
        },
    }

    result = mod.persist_user_data_stream_monitor_to_positions(store, monitor)

    assert result['TESTUSDT:LONG']['user_data_stream'] == mod.build_user_data_stream_position_payload(monitor)
    assert 'user_data_stream' not in result['BTCUSDT:LONG']
    assert store.load_json('positions', {}) == result


def test_emit_user_data_stream_alert_if_needed_builds_notification_payload(monkeypatch):
    notifications = []
    args = argparse.Namespace(disable_notify=False, notify_target='telegram:test')

    monkeypatch.setattr(
        mod,
        'emit_notification',
        lambda args, event_type, payload: notifications.append((event_type, payload)) or {'ok': True},
    )

    payload = mod.emit_user_data_stream_alert_if_needed(
        args=args,
        symbol='TESTUSDT',
        monitor={
            'status': 'refresh_failed',
            'action': 'refresh_failed',
            'listen_key': 'lk-1',
            'error': 'boom',
            'now_utc': '2026-05-10T11:31:00+00:00',
            'health': {
                'symbol': 'TESTUSDT',
                'detail': 'boom',
                'disconnect_count': 2,
                'refresh_failure_count': 3,
                'reconnect_count': 4,
                'started_at': '2026-05-10T11:00:00+00:00',
                'last_refresh_at': '2026-05-10T11:30:00+00:00',
                'updated_at': '2026-05-10T11:31:00+00:00',
            },
        },
    )

    assert payload == {
        'symbol': 'TESTUSDT',
        'status': 'refresh_failed',
        'action': 'refresh_failed',
        'error': 'boom',
        'detail': 'boom',
        'listen_key': 'lk-1',
        'disconnect_count': 2,
        'refresh_failure_count': 3,
        'reconnect_count': 4,
        'started_at': '2026-05-10T11:00:00+00:00',
        'last_refresh_at': '2026-05-10T11:30:00+00:00',
        'updated_at': '2026-05-10T11:31:00+00:00',
    }
    assert notifications == [('user_data_stream_alert', payload)]


def test_emit_user_data_stream_alert_if_needed_masks_listen_keys(monkeypatch):
    notifications = []
    args = argparse.Namespace(disable_notify=False, notify_target='telegram:test')

    monkeypatch.setattr(
        mod,
        'emit_notification',
        lambda args, event_type, payload: notifications.append((event_type, payload)) or {'ok': True},
    )

    payload = mod.emit_user_data_stream_alert_if_needed(
        args=args,
        symbol='TESTUSDT',
        monitor={
            'status': 'refresh_failed',
            'action': 'restarted_after_missing_listen_key',
            'listen_key': 'listen-key-123456',
            'previous_listen_key': 'previous-key-654321',
            'error': 'boom',
            'now_utc': '2026-05-10T11:31:00+00:00',
            'health': {
                'symbol': 'TESTUSDT',
                'detail': 'boom',
                'listen_key': 'listen-key-123456',
                'previous_listen_key': 'previous-key-654321',
                'disconnect_count': 2,
                'refresh_failure_count': 3,
                'reconnect_count': 4,
                'started_at': '2026-05-10T11:00:00+00:00',
                'last_refresh_at': '2026-05-10T11:30:00+00:00',
                'updated_at': '2026-05-10T11:31:00+00:00',
            },
        },
    )

    assert payload['listen_key'] == 'list***3456'
    assert payload['previous_listen_key'] == 'prev***4321'
    assert notifications == [('user_data_stream_alert', payload)]


def test_run_auto_loop_user_data_stream_monitor_core_runs_cycle_persist_and_alert(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    existing_state = {'listen_key': 'lk-1', 'symbol': 'TESTUSDT'}
    monitor = {'status': 'healthy', 'health': {'symbol': 'TESTUSDT'}}
    calls = []

    monkeypatch.setattr(
        mod,
        'run_user_data_stream_monitor_cycle',
        lambda **kwargs: calls.append(('cycle', kwargs)) or monitor,
    )
    monkeypatch.setattr(
        mod,
        'persist_user_data_stream_monitor_to_positions',
        lambda store_arg, monitor_arg: calls.append(('persist', store_arg, monitor_arg)),
    )
    monkeypatch.setattr(
        mod,
        'emit_user_data_stream_alert_if_needed',
        lambda args, symbol, monitor_arg: calls.append(('alert', args, symbol, monitor_arg)) or {'sent': True},
    )

    result = mod.run_auto_loop_user_data_stream_monitor_core(
        client='client',
        store=store,
        args='args',
        existing_uds_state=existing_state,
        config=mod.AutoLoopUserDataStreamMonitorConfig(
            refresh_interval_minutes=22.0,
            disconnect_timeout_minutes=44.0,
        ),
    )

    assert result == {'monitor': monitor, 'alert': {'sent': True}}
    assert calls == [
        ('cycle', {
            'client': 'client',
            'store': store,
            'symbol': 'TESTUSDT',
            'refresh_interval_minutes': 22.0,
            'disconnect_timeout_minutes': 44.0,
        }),
        ('persist', store, monitor),
        ('alert', 'args', 'TESTUSDT', monitor),
    ]


def test_run_auto_loop_user_data_stream_monitor_core_returns_empty_result_without_listen_key(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))

    result = mod.run_auto_loop_user_data_stream_monitor_core(
        client='client',
        store=store,
        args='args',
        existing_uds_state={'symbol': 'TESTUSDT'},
        config=mod.AutoLoopUserDataStreamMonitorConfig(
            refresh_interval_minutes=22.0,
            disconnect_timeout_minutes=44.0,
        ),
    )

    assert result == {'monitor': None, 'alert': None}


def test_run_auto_loop_user_data_stream_monitor_wrapper_builds_state_and_delegates(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('user_data_stream', {'listen_key': 'lk-1', 'symbol': 'TESTUSDT'})
    captured = {}
    config = mod.AutoLoopUserDataStreamMonitorConfig(
        refresh_interval_minutes=22.0,
        disconnect_timeout_minutes=44.0,
    )

    monkeypatch.setattr(mod, 'build_auto_loop_user_data_stream_monitor_config', lambda args: config)

    def fake_core(**kwargs):
        captured.update(kwargs)
        return {'monitor': {'status': 'healthy'}, 'alert': None}

    monkeypatch.setattr(mod, 'run_auto_loop_user_data_stream_monitor_core', fake_core)

    result = mod.run_auto_loop_user_data_stream_monitor(
        client='client',
        store=store,
        args='args',
    )

    assert result == {'monitor': {'status': 'healthy'}, 'alert': None}
    assert captured == {
        'client': 'client',
        'store': store,
        'args': 'args',
        'existing_uds_state': {'listen_key': 'lk-1', 'symbol': 'TESTUSDT'},
        'config': config,
    }


def test_build_auto_loop_book_ticker_monitor_optional_store_seams_wires_loader_and_emitter(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    expected_loader = object()
    expected_emitter = object()

    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_health_loader',
        lambda store, health_store_key='book_ticker_ws_status': expected_loader,
    )
    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_unavailable_event_emitter',
        lambda store: expected_emitter,
    )

    seams = mod.build_auto_loop_book_ticker_monitor_optional_store_seams(
        store=store,
        health_store_key='book_ticker_ws_status',
    )

    assert seams == {
        'health_loader': expected_loader,
        'unavailable_event_emitter': expected_emitter,
    }


def test_build_auto_loop_book_ticker_monitor_default_seams_wires_probe_and_summary_builder(monkeypatch):
    expected_probe = object()
    expected_summary_builder = object()

    monkeypatch.setattr(mod, 'make_auto_loop_book_ticker_websocket_capability_probe', lambda: expected_probe)
    monkeypatch.setattr(mod, 'make_auto_loop_book_ticker_unavailable_summary_builder', lambda: expected_summary_builder)

    seams = mod.build_auto_loop_book_ticker_monitor_default_seams()

    assert seams == {
        'websocket_capability_probe': expected_probe,
        'unavailable_summary_builder': expected_summary_builder,
    }


def test_build_auto_loop_book_ticker_websocket_monitor_config_is_explicit_marker(monkeypatch, tmp_path):
    args = argparse.Namespace(auto_loop=True, another_field='ignored')
    expected_provider = object()
    expected_store_seams = {
        'health_loader': object(),
        'unavailable_event_emitter': object(),
    }
    expected_default_seams = {
        'websocket_capability_probe': object(),
        'unavailable_summary_builder': object(),
    }
    store = mod.RuntimeStateStore(str(tmp_path))

    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_symbol_provider',
        lambda client, args: expected_provider,
    )
    monkeypatch.setattr(
        mod,
        'build_auto_loop_book_ticker_monitor_optional_store_seams',
        lambda store, health_store_key='book_ticker_ws_status': expected_store_seams,
    )
    monkeypatch.setattr(
        mod,
        'build_auto_loop_book_ticker_monitor_default_seams',
        lambda: expected_default_seams,
    )

    config = mod.build_auto_loop_book_ticker_websocket_monitor_config(client=object(), args=args, store=store)

    assert config == mod.AutoLoopBookTickerWebsocketMonitorConfig(
        symbol_provider=expected_provider,
        health_loader=expected_store_seams['health_loader'],
        health_store_key='book_ticker_ws_status',
        websocket_capability_probe=expected_default_seams['websocket_capability_probe'],
        unavailable_event_emitter=expected_store_seams['unavailable_event_emitter'],
        unavailable_summary_builder=expected_default_seams['unavailable_summary_builder'],
        unavailable_reason='websocket_client_missing',
        max_supervisor_cycles=0,
    )


def test_run_auto_loop_book_ticker_websocket_monitor_uses_explicit_config_without_args_namespace(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    supervisor_calls = []
    ws_module = object()
    provider_calls = []
    health_loader_calls = []

    monkeypatch.setattr(mod, 'websocket', ws_module)

    def fake_provider():
        provider_calls.append('called')
        return ['BTCUSDT']

    def fake_health_loader():
        health_loader_calls.append('called')
        return {'status': 'healthy', 'messages_processed': 9}

    def fake_supervisor(store_arg, initial_symbols, symbol_provider, ws_module=None, **kwargs):
        supervisor_calls.append({
            'store': store_arg,
            'initial_symbols': list(initial_symbols),
            'provider_symbols': list(symbol_provider()),
            'ws_module': ws_module,
            'max_supervisor_cycles': kwargs.get('max_supervisor_cycles'),
        })
        return {'cycles_completed': 1}

    monkeypatch.setattr(mod, 'run_book_ticker_websocket_supervisor', fake_supervisor)

    result = mod.run_auto_loop_book_ticker_websocket_monitor(
        client=object(),
        store=store,
        args=object(),
        config=mod.AutoLoopBookTickerWebsocketMonitorConfig(
            symbol_provider=fake_provider,
            health_loader=fake_health_loader,
            health_store_key='custom_health_key',
            max_supervisor_cycles=7,
        ),
    )

    assert provider_calls == ['called', 'called']
    assert health_loader_calls == ['called']
    assert supervisor_calls == [{
        'store': store,
        'initial_symbols': ['BTCUSDT'],
        'provider_symbols': ['BTCUSDT'],
        'ws_module': ws_module,
        'max_supervisor_cycles': 7,
    }]
    assert result == {
        'status': 'available',
        'summary': {'cycles_completed': 1},
        'health': {'status': 'healthy', 'messages_processed': 9},
    }


def test_build_auto_loop_book_ticker_supervisor_summary_scales_message_budget_to_symbol_count(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    ws_module = object()
    supervisor_calls = []

    def fake_provider():
        return ['BTCUSDT', 'ETHUSDT', 'DOGEUSDT']

    def fake_supervisor(store_arg, initial_symbols, symbol_provider, ws_module=None, **kwargs):
        supervisor_calls.append({
            'store': store_arg,
            'initial_symbols': list(initial_symbols),
            'provider_symbols': list(symbol_provider()),
            'ws_module': ws_module,
            'max_supervisor_cycles': kwargs.get('max_supervisor_cycles'),
            'max_messages_per_cycle': kwargs.get('max_messages_per_cycle'),
        })
        return {'cycles_completed': 1, 'messages_processed_total': kwargs.get('max_messages_per_cycle')}

    monkeypatch.setattr(mod, 'run_book_ticker_websocket_supervisor', fake_supervisor)

    result = mod.build_auto_loop_book_ticker_supervisor_summary(
        store=store,
        symbol_provider=fake_provider,
        ws_module=ws_module,
        max_supervisor_cycles=7,
    )

    assert supervisor_calls == [{
        'store': store,
        'initial_symbols': ['BTCUSDT', 'ETHUSDT', 'DOGEUSDT'],
        'provider_symbols': ['BTCUSDT', 'ETHUSDT', 'DOGEUSDT'],
        'ws_module': ws_module,
        'max_supervisor_cycles': 7,
        'max_messages_per_cycle': 100,
    }]
    assert result == {'cycles_completed': 1, 'messages_processed_total': 100}



def test_build_auto_loop_book_ticker_supervisor_summary_raises_message_budget_for_large_symbol_set(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    ws_module = object()
    supervisor_calls = []
    symbols = [f'SYM{idx}USDT' for idx in range(112)]

    def fake_provider():
        return list(symbols)

    def fake_supervisor(store_arg, initial_symbols, symbol_provider, ws_module=None, **kwargs):
        supervisor_calls.append({
            'initial_count': len(initial_symbols),
            'provider_count': len(symbol_provider()),
            'max_messages_per_cycle': kwargs.get('max_messages_per_cycle'),
        })
        return {'cycles_completed': 1}

    monkeypatch.setattr(mod, 'run_book_ticker_websocket_supervisor', fake_supervisor)

    mod.build_auto_loop_book_ticker_supervisor_summary(
        store=store,
        symbol_provider=fake_provider,
        ws_module=ws_module,
        max_supervisor_cycles=1,
    )

    assert supervisor_calls == [{
        'initial_count': 112,
        'provider_count': 112,
        'max_messages_per_cycle': 672,
    }]



def test_build_auto_loop_book_ticker_websocket_monitor_config_wires_explicit_probe_and_unavailable_emitter(monkeypatch, tmp_path):
    args = argparse.Namespace(auto_loop=True)
    store = mod.RuntimeStateStore(str(tmp_path))
    expected_provider = object()
    expected_loader = object()
    expected_probe = object()
    expected_emitter = object()
    expected_summary_builder = object()

    monkeypatch.setattr(mod, 'make_auto_loop_book_ticker_symbol_provider', lambda client, args: expected_provider)
    monkeypatch.setattr(mod, 'make_auto_loop_book_ticker_health_loader', lambda store, health_store_key='book_ticker_ws_status': expected_loader)
    monkeypatch.setattr(mod, 'make_auto_loop_book_ticker_websocket_capability_probe', lambda: expected_probe)
    monkeypatch.setattr(mod, 'make_auto_loop_book_ticker_unavailable_event_emitter', lambda store: expected_emitter)
    monkeypatch.setattr(mod, 'make_auto_loop_book_ticker_unavailable_summary_builder', lambda: expected_summary_builder)

    config = mod.build_auto_loop_book_ticker_websocket_monitor_config(client=object(), args=args, store=store)

    assert config == mod.AutoLoopBookTickerWebsocketMonitorConfig(
        symbol_provider=expected_provider,
        health_loader=expected_loader,
        health_store_key='book_ticker_ws_status',
        websocket_capability_probe=expected_probe,
        unavailable_event_emitter=expected_emitter,
        unavailable_summary_builder=expected_summary_builder,
        unavailable_reason='websocket_client_missing',
        max_supervisor_cycles=0,
    )


def test_run_auto_loop_book_ticker_websocket_monitor_complete_config_path_avoids_fallback_builders(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    supervisor_calls = []
    ws_module = object()

    monkeypatch.setattr(mod, 'websocket', ws_module)
    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_symbol_provider',
        lambda client, args: (_ for _ in ()).throw(AssertionError('symbol fallback builder should stay unused')),
    )
    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_health_loader',
        lambda store, health_store_key='book_ticker_ws_status': (_ for _ in ()).throw(AssertionError('health fallback builder should stay unused')),
    )
    monkeypatch.setattr(
        mod,
        'build_auto_loop_book_ticker_websocket_monitor_config',
        lambda client, args, store=None: (_ for _ in ()).throw(AssertionError('config builder should stay unused')),
    )

    def fake_supervisor(store_arg, initial_symbols, symbol_provider, ws_module=None, **kwargs):
        supervisor_calls.append({
            'store': store_arg,
            'initial_symbols': list(initial_symbols),
            'provider_symbols': list(symbol_provider()),
            'ws_module': ws_module,
        })
        return {'cycles_completed': 3}

    monkeypatch.setattr(mod, 'run_book_ticker_websocket_supervisor', fake_supervisor)

    result = mod.run_auto_loop_book_ticker_websocket_monitor(
        client=object(),
        store=store,
        args=object(),
        config=mod.AutoLoopBookTickerWebsocketMonitorConfig(
            symbol_provider=lambda: ['SOLUSDT'],
            health_loader=lambda: {'status': 'healthy', 'messages_processed': 11},
        ),
    )

    assert supervisor_calls == [{
        'store': store,
        'initial_symbols': ['SOLUSDT'],
        'provider_symbols': ['SOLUSDT'],
        'ws_module': ws_module,
    }]
    assert result == {
        'status': 'available',
        'summary': {'cycles_completed': 3},
        'health': {'status': 'healthy', 'messages_processed': 11},
    }


def test_resolve_auto_loop_book_ticker_websocket_capability_probe_prefers_config_probe(monkeypatch):
    expected_probe = object()

    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_websocket_capability_probe',
        lambda: (_ for _ in ()).throw(AssertionError('fallback probe factory should stay unused')),
    )

    result = mod.resolve_auto_loop_book_ticker_websocket_capability_probe(
        mod.AutoLoopBookTickerWebsocketMonitorConfig(
            websocket_capability_probe=expected_probe,
        )
    )

    assert result is expected_probe


def test_resolve_auto_loop_book_ticker_unavailable_summary_builder_prefers_config_builder(monkeypatch):
    expected_builder = object()

    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_unavailable_summary_builder',
        lambda: (_ for _ in ()).throw(AssertionError('fallback summary builder factory should stay unused')),
    )

    result = mod.resolve_auto_loop_book_ticker_unavailable_summary_builder(
        mod.AutoLoopBookTickerWebsocketMonitorConfig(
            unavailable_summary_builder=expected_builder,
        )
    )

    assert result is expected_builder


def test_resolve_auto_loop_book_ticker_unavailable_event_emitter_prefers_config_emitter(tmp_path, monkeypatch):
    store = mod.RuntimeStateStore(str(tmp_path))
    expected_emitter = object()

    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_unavailable_event_emitter',
        lambda store: (_ for _ in ()).throw(AssertionError('fallback event emitter factory should stay unused')),
    )

    result = mod.resolve_auto_loop_book_ticker_unavailable_event_emitter(
        store=store,
        config=mod.AutoLoopBookTickerWebsocketMonitorConfig(
            unavailable_event_emitter=expected_emitter,
        ),
    )

    assert result is expected_emitter


def test_resolve_auto_loop_book_ticker_health_loader_prefers_config_loader(tmp_path, monkeypatch):
    store = mod.RuntimeStateStore(str(tmp_path))
    expected_loader = object()

    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_health_loader',
        lambda store, health_store_key='book_ticker_ws_status': (_ for _ in ()).throw(AssertionError('fallback health loader factory should stay unused')),
    )

    result = mod.resolve_auto_loop_book_ticker_health_loader(
        store=store,
        config=mod.AutoLoopBookTickerWebsocketMonitorConfig(
            health_loader=expected_loader,
            health_store_key='custom_health_key',
        ),
    )

    assert result is expected_loader


def test_run_auto_loop_book_ticker_websocket_monitor_core_uses_minimal_orchestration_dependencies(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    ws_module = object()
    branch_calls = []

    def fake_probe():
        return ws_module

    def fake_available_branch(*, store, config, ws_module):
        branch_calls.append({
            'kind': 'available',
            'store': store,
            'config': config,
            'ws_module': ws_module,
        })
        return {'status': 'available', 'summary': {'cycles_completed': 5}, 'health': {'status': 'healthy'}}

    monkeypatch.setattr(mod, 'resolve_auto_loop_book_ticker_websocket_capability_probe', lambda config: fake_probe)
    monkeypatch.setattr(mod, 'run_auto_loop_book_ticker_websocket_monitor_available_branch', fake_available_branch)
    monkeypatch.setattr(
        mod,
        'run_auto_loop_book_ticker_websocket_monitor_unavailable_branch',
        lambda **kwargs: (_ for _ in ()).throw(AssertionError('core should dispatch to available branch')),
    )
    monkeypatch.setattr(
        mod,
        'run_book_ticker_websocket_supervisor',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('core should stay at orchestration level')),
    )

    config = mod.AutoLoopBookTickerWebsocketMonitorConfig(
        symbol_provider=lambda: ['BNBUSDT'],
        health_loader=lambda: {'status': 'healthy', 'messages_processed': 17},
        max_supervisor_cycles=9,
    )

    result = mod.run_auto_loop_book_ticker_websocket_monitor_core(
        store=store,
        config=config,
    )

    assert branch_calls == [{
        'kind': 'available',
        'store': store,
        'config': config,
        'ws_module': ws_module,
    }]
    assert result == {'status': 'available', 'summary': {'cycles_completed': 5}, 'health': {'status': 'healthy'}}


def test_run_auto_loop_book_ticker_websocket_monitor_unavailable_branch_uses_summary_and_emitter(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    events = []
    summary_calls = []
    result_calls = []

    def fake_summary_builder(reason):
        summary_calls.append(reason)
        return {'status': 'unavailable', 'reason': reason, 'source': 'test'}

    def fake_emitter(summary):
        events.append(summary)

    def fake_result_builder(*, summary):
        result_calls.append(summary)
        return {
            'status': 'unavailable',
            'summary': summary,
            'health': {'source': 'result-builder'},
        }

    monkeypatch.setattr(mod, 'build_auto_loop_book_ticker_unavailable_result', fake_result_builder)

    config = mod.AutoLoopBookTickerWebsocketMonitorConfig(
        unavailable_summary_builder=fake_summary_builder,
        unavailable_event_emitter=fake_emitter,
        unavailable_reason='missing_ws',
    )

    result = mod.run_auto_loop_book_ticker_websocket_monitor_unavailable_branch(
        store=store,
        config=config,
    )

    assert summary_calls == ['missing_ws']
    assert events == [{'status': 'unavailable', 'reason': 'missing_ws', 'source': 'test'}]
    assert result_calls == [{'status': 'unavailable', 'reason': 'missing_ws', 'source': 'test'}]
    assert result == {
        'status': 'unavailable',
        'summary': {'status': 'unavailable', 'reason': 'missing_ws', 'source': 'test'},
        'health': {'source': 'result-builder'},
    }


def test_build_auto_loop_book_ticker_unavailable_result_returns_default_payload():
    summary = {'status': 'unavailable', 'reason': 'missing_ws', 'source': 'test'}

    result = mod.build_auto_loop_book_ticker_unavailable_result(summary=summary)

    assert result == {
        'status': 'unavailable',
        'summary': summary,
        'health': {},
    }


def test_run_auto_loop_book_ticker_websocket_monitor_available_branch_uses_supervisor_and_health(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    ws_module = object()
    summary_calls = []
    health_reader_calls = []

    def fake_provider():
        return ['BTCUSDT', 'ETHUSDT']

    def fake_summary_builder(*, store, symbol_provider, ws_module, max_supervisor_cycles):
        summary_calls.append({
            'store': store,
            'symbols': list(symbol_provider()),
            'ws_module': ws_module,
            'max_supervisor_cycles': max_supervisor_cycles,
        })
        return {'cycles_completed': 3, 'subscription_version': 4}

    def fake_health_reader(*, store, config):
        health_reader_calls.append({'store': store, 'config': config})
        return {'status': 'healthy', 'messages_processed': 11}

    monkeypatch.setattr(mod, 'build_auto_loop_book_ticker_supervisor_summary', fake_summary_builder)
    monkeypatch.setattr(mod, 'read_auto_loop_book_ticker_health', fake_health_reader)
    monkeypatch.setattr(
        mod,
        'run_book_ticker_websocket_supervisor',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('available branch should use extracted supervisor summary seam')),
    )

    config = mod.AutoLoopBookTickerWebsocketMonitorConfig(
        symbol_provider=fake_provider,
        health_loader=lambda: (_ for _ in ()).throw(AssertionError('available branch should use extracted health seam')),
        max_supervisor_cycles=7,
    )

    result = mod.run_auto_loop_book_ticker_websocket_monitor_available_branch(
        store=store,
        config=config,
        ws_module=ws_module,
    )

    assert summary_calls == [{
        'store': store,
        'symbols': ['BTCUSDT', 'ETHUSDT'],
        'ws_module': ws_module,
        'max_supervisor_cycles': 7,
    }]
    assert health_reader_calls == [{'store': store, 'config': config}]
    assert result == {
        'status': 'available',
        'summary': {'cycles_completed': 3, 'subscription_version': 4},
        'health': {'status': 'healthy', 'messages_processed': 11},
    }


def test_resolve_auto_loop_book_ticker_symbol_provider_prefers_config_provider(monkeypatch):
    expected_provider = object()

    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_symbol_provider',
        lambda client, args: (_ for _ in ()).throw(AssertionError('symbol provider builder should stay unused')),
    )

    config = mod.AutoLoopBookTickerWebsocketMonitorConfig(symbol_provider=expected_provider)

    result = mod.resolve_auto_loop_book_ticker_symbol_provider(client=object(), args=object(), config=config)

    assert result is expected_provider


def test_resolve_auto_loop_book_ticker_symbol_provider_builds_fallback_provider(monkeypatch):
    fallback_provider = object()
    builder_calls = []
    client = object()
    args = object()

    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_symbol_provider',
        lambda client_arg, args_arg: builder_calls.append({'client': client_arg, 'args': args_arg}) or fallback_provider,
    )

    result = mod.resolve_auto_loop_book_ticker_symbol_provider(
        client=client,
        args=args,
        config=mod.AutoLoopBookTickerWebsocketMonitorConfig(),
    )

    assert builder_calls == [{'client': client, 'args': args}]
    assert result is fallback_provider


def test_build_auto_loop_book_ticker_available_result_returns_default_payload():
    summary = {'cycles_completed': 2, 'symbols': ['BTCUSDT']}
    health = {'status': 'healthy', 'messages_processed': 17}

    result = mod.build_auto_loop_book_ticker_available_result(summary=summary, health=health)

    assert result == {
        'status': 'available',
        'summary': summary,
        'health': health,
    }


def test_resolve_auto_loop_book_ticker_max_supervisor_cycles_reads_config_value():
    config = mod.AutoLoopBookTickerWebsocketMonitorConfig(max_supervisor_cycles=9)

    result = mod.resolve_auto_loop_book_ticker_max_supervisor_cycles(config)

    assert result == 9


def test_build_auto_loop_book_ticker_supervisor_summary_runs_supervisor(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    ws_module = object()
    supervisor_calls = []
    provider_calls = []

    def fake_provider():
        provider_calls.append('called')
        return ['SOLUSDT']

    def fake_supervisor(store_arg, initial_symbols, symbol_provider, ws_module=None, **kwargs):
        supervisor_calls.append({
            'store': store_arg,
            'initial_symbols': list(initial_symbols),
            'provider_symbols': list(symbol_provider()),
            'ws_module': ws_module,
            'max_supervisor_cycles': kwargs.get('max_supervisor_cycles'),
        })
        return {'cycles_completed': 8}

    monkeypatch.setattr(mod, 'run_book_ticker_websocket_supervisor', fake_supervisor)

    result = mod.build_auto_loop_book_ticker_supervisor_summary(
        store=store,
        symbol_provider=fake_provider,
        ws_module=ws_module,
        max_supervisor_cycles=5,
    )

    assert supervisor_calls == [{
        'store': store,
        'initial_symbols': ['SOLUSDT'],
        'provider_symbols': ['SOLUSDT'],
        'ws_module': ws_module,
        'max_supervisor_cycles': 5,
    }]
    assert provider_calls == ['called', 'called']
    assert result == {'cycles_completed': 8}


def test_read_auto_loop_book_ticker_health_uses_config_loader(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    builder_calls = []

    monkeypatch.setattr(
        mod,
        'make_auto_loop_book_ticker_health_loader',
        lambda store_arg, health_store_key='book_ticker_ws_status': builder_calls.append({'store': store_arg, 'health_store_key': health_store_key}) or (lambda: {'status': 'fallback'}),
    )

    config = mod.AutoLoopBookTickerWebsocketMonitorConfig(
        symbol_provider=lambda: ['BTCUSDT'],
        health_loader=lambda: {'status': 'healthy', 'messages_processed': 13},
        health_store_key='custom_key',
    )

    result = mod.read_auto_loop_book_ticker_health(store=store, config=config)

    assert builder_calls == []
    assert result == {'status': 'healthy', 'messages_processed': 13}


def test_run_auto_loop_book_ticker_websocket_monitor_builds_config_then_delegates_to_core(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    client = object()
    args = argparse.Namespace(auto_loop=True)
    expected_config = mod.AutoLoopBookTickerWebsocketMonitorConfig(symbol_provider=lambda: ['BTCUSDT'])
    calls = []

    def fake_build_config(client_arg, args_arg, store=None):
        calls.append({'step': 'build', 'client': client_arg, 'args': args_arg, 'store': store})
        return expected_config

    def fake_core(*, store, config):
        calls.append({'step': 'core', 'store': store, 'config': config})
        return {'status': 'available', 'summary': {'cycles_completed': 1}, 'health': {'status': 'ok'}}

    monkeypatch.setattr(mod, 'build_auto_loop_book_ticker_websocket_monitor_config', fake_build_config)
    monkeypatch.setattr(mod, 'run_auto_loop_book_ticker_websocket_monitor_core', fake_core)

    result = mod.run_auto_loop_book_ticker_websocket_monitor(client=client, store=store, args=args)

    assert mod.run_auto_loop_book_ticker_websocket_monitor.__doc__ == 'Compatibility wrapper: build fallback config then delegate to core helper.'
    assert calls == [
        {'step': 'build', 'client': client, 'args': args, 'store': store},
        {'step': 'core', 'store': store, 'config': expected_config},
    ]
    assert result == {'status': 'available', 'summary': {'cycles_completed': 1}, 'health': {'status': 'ok'}}


def test_run_cycle_auto_loop_builds_book_ticker_config_then_calls_core_helper(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    client = object()
    args = argparse.Namespace(auto_loop=True, reconcile_only=False, live=False, scan_only=False)
    expected_book_ticker_config = mod.AutoLoopBookTickerWebsocketMonitorConfig(symbol_provider=lambda: ['BTCUSDT'])
    expected_user_stream_config = mod.AutoLoopUserDataStreamMonitorConfig(
        refresh_interval_minutes=30.0,
        disconnect_timeout_minutes=65.0,
    )
    calls = []

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda args_arg: store)
    monkeypatch.setattr(mod, 'execution_exchange_label', lambda args_arg: 'binance')
    monkeypatch.setattr(mod, 'is_binance_simulated_trading', lambda args_arg: False)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda *a, **k: {'ok': True})
    monkeypatch.setattr(
        mod,
        'build_auto_loop_book_ticker_websocket_monitor_config',
        lambda client_arg, args_arg, store=None: calls.append({'step': 'build-book', 'client': client_arg, 'args': args_arg, 'store': store}) or expected_book_ticker_config,
    )
    monkeypatch.setattr(
        mod,
        'build_auto_loop_user_data_stream_monitor_config',
        lambda args_arg: calls.append({'step': 'build-uds', 'args': args_arg}) or expected_user_stream_config,
    )
    monkeypatch.setattr(
        mod,
        'run_auto_loop_book_ticker_websocket_monitor',
        lambda **kwargs: (_ for _ in ()).throw(AssertionError('wrapper should stay unused in outer auto-loop orchestration')),
    )
    monkeypatch.setattr(
        mod,
        'run_auto_loop_book_ticker_websocket_monitor_core',
        lambda *, store, config: calls.append({'step': 'core-book', 'store': store, 'config': config}) or {'status': 'available', 'summary': {'cycles_completed': 2}, 'health': {'status': 'healthy'}},
    )
    monkeypatch.setattr(
        mod,
        'run_auto_loop_user_data_stream_monitor',
        lambda **kwargs: calls.append({'step': 'uds-run', 'kwargs': kwargs}) or {'monitor': None, 'alert': None},
    )
    monkeypatch.setattr(mod, 'load_risk_state', lambda store: {'halted': False})
    monkeypatch.setattr(mod, 'run_scan_once', lambda client_arg, args_arg: ({'funnel': {}}, None, {}))
    monkeypatch.setattr(mod, 'evaluate_risk_guards', lambda **kwargs: {'allowed': True, 'reasons': []})

    result = mod.run_loop(client=client, args=args)

    assert calls[:4] == [
        {'step': 'build-book', 'client': client, 'args': args, 'store': store},
        {'step': 'build-uds', 'args': args},
        {'step': 'core-book', 'store': store, 'config': expected_book_ticker_config},
        {
            'step': 'uds-run',
            'kwargs': {
                'client': client,
                'store': store,
                'args': args,
                'config': expected_user_stream_config,
            },
        },
    ]
    assert result['cycles'][0]['book_ticker_websocket'] == {
        'cycles_completed': 2,
        'health': {'status': 'healthy'},
    }


def test_run_auto_loop_book_ticker_websocket_monitor_uses_explicit_unavailable_adapters_without_store_event_side_effects(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    capability_probe_calls = []
    unavailable_emitter_calls = []
    unavailable_summary_calls = []
    append_calls = []

    monkeypatch.setattr(mod, 'websocket', object())
    monkeypatch.setattr(
        mod,
        'append_rate_limited_runtime_event',
        lambda *args, **kwargs: append_calls.append({'args': args, 'kwargs': kwargs}),
    )

    def fake_probe():
        capability_probe_calls.append('called')
        return None

    def fake_summary_builder(reason):
        unavailable_summary_calls.append(reason)
        return {
            'status': 'custom-unavailable',
            'reason': reason,
            'source': 'explicit-builder',
        }

    def fake_unavailable_emitter(summary):
        unavailable_emitter_calls.append(summary)

    result = mod.run_auto_loop_book_ticker_websocket_monitor(
        client=object(),
        store=store,
        args=object(),
        config=mod.AutoLoopBookTickerWebsocketMonitorConfig(
            websocket_capability_probe=fake_probe,
            unavailable_event_emitter=fake_unavailable_emitter,
            unavailable_summary_builder=fake_summary_builder,
            unavailable_reason='custom_reason',
        ),
    )

    assert capability_probe_calls == ['called']
    assert unavailable_summary_calls == ['custom_reason']
    assert unavailable_emitter_calls == [{
        'status': 'custom-unavailable',
        'reason': 'custom_reason',
        'source': 'explicit-builder',
    }]
    assert append_calls == []
    assert result == {
        'status': 'unavailable',
        'summary': {
            'status': 'custom-unavailable',
            'reason': 'custom_reason',
            'source': 'explicit-builder',
        },
        'health': {},
    }


def test_build_local_open_positions_for_risk_emits_rate_limited_event_on_malformed_positions_json(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    (tmp_path / 'positions.json').write_text('{bad json', encoding='utf-8')

    rows_first = mod.build_local_open_positions_for_risk(store)
    rows_second = mod.build_local_open_positions_for_risk(store)
    events_path = tmp_path / 'events.jsonl'
    event_rows = [mod.json.loads(line) for line in events_path.read_text(encoding='utf-8').splitlines() if line.strip()]

    assert rows_first == []
    assert rows_second == []
    assert len(event_rows) == 1
    assert event_rows[0]['event_type'] == 'runtime_state_degraded'
    assert event_rows[0]['state_file'] == 'positions.json'
    assert event_rows[0]['state_key'] == 'positions'
    assert event_rows[0]['fallback_used'] == 'empty_positions'
    assert event_rows[0]['consumer'] == 'build_local_open_positions_for_risk'
    assert event_rows[0]['error_type'] == 'JSONDecodeError'


def test_build_local_open_positions_for_risk_matches_runtime_state_helper(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'BTCUSDT': {
            'symbol': 'btcusdt',
            'side': 'long',
            'remaining_quantity': '0.015',
            'entry_price': '64000.5',
            'notional': '960.0075',
        },
        'ETHUSDT:SHORT': {
            'symbol': 'ethusdt',
            'side': 'short',
            'quantity': '0.25',
            'entry_price': '3500',
        },
        'XRPUSDT:LONG': {
            'symbol': 'xrpusdt',
            'side': 'long',
            'remaining_quantity': '0',
            'entry_price': '0.52',
        },
    })
    expected = mod.build_local_open_positions_for_risk(store)

    helper_path = SCRIPTS_DIR / 'runtime_state_risk_helpers.py'
    helper_spec = importlib.util.spec_from_file_location('runtime_state_risk_helpers', helper_path)
    assert helper_spec is not None
    helper_module = importlib.util.module_from_spec(helper_spec)
    sys.modules[helper_spec.name] = helper_module
    assert helper_spec.loader is not None
    helper_spec.loader.exec_module(helper_module)

    positions_state, error = store.load_json_with_error('positions', {})
    actual = helper_module.build_local_open_positions_from_state(
        positions_state,
        error=error,
        normalize_position_side=mod.normalize_position_side,
        to_float=mod._to_float,
        iter_canonical_open_positions=mod.iter_canonical_open_positions,
    )

    assert actual == expected
    assert actual == [
        {
            'symbol': 'BTCUSDT',
            'side': 'LONG',
            'positionSide': 'LONG',
            'quantity': 0.015,
            'positionAmt': 0.015,
            'entryPrice': 64000.5,
            'notional': 960.0075,
        },
        {
            'symbol': 'ETHUSDT',
            'side': 'SHORT',
            'positionSide': 'SHORT',
            'quantity': 0.25,
            'positionAmt': -0.25,
            'entryPrice': 3500.0,
            'notional': 875.0,
        },
    ]


def test_build_local_open_positions_for_risk_matches_runtime_state_consumer_helper_on_malformed_positions_json(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    (tmp_path / 'positions.json').write_text('{bad json', encoding='utf-8')

    expected_rows = mod.build_local_open_positions_for_risk(store)
    expected_events = store.read_events(limit=10)

    helper_path = SCRIPTS_DIR / 'runtime_state_risk_helpers.py'
    helper_spec = importlib.util.spec_from_file_location('runtime_state_risk_helpers', helper_path)
    assert helper_spec is not None
    helper_module = importlib.util.module_from_spec(helper_spec)
    sys.modules[helper_spec.name] = helper_module
    assert helper_spec.loader is not None
    helper_spec.loader.exec_module(helper_module)

    helper_root = tmp_path / 'helper-store'
    helper_root.mkdir(parents=True, exist_ok=True)
    (helper_root / 'positions.json').write_text('{bad json', encoding='utf-8')
    helper_store = mod.RuntimeStateStore(str(helper_root))

    actual_rows = helper_module.load_local_open_positions_for_risk(
        helper_store,
        should_emit_runtime_state_degraded=mod._should_emit_runtime_state_degraded,
        append_runtime_state_degraded_event=mod.append_rate_limited_runtime_event,
        build_local_open_positions_from_state=helper_module.build_local_open_positions_from_state,
        normalize_position_side=mod.normalize_position_side,
        to_float=mod._to_float,
        iter_canonical_open_positions=mod.iter_canonical_open_positions,
    )
    actual_events = helper_store.read_events(limit=10)

    assert actual_rows == expected_rows == []
    assert len(expected_events) == 1
    assert len(actual_events) == 1
    assert expected_events[0]['event_type'] == actual_events[0]['event_type'] == 'runtime_state_degraded'
    assert expected_events[0]['state_key'] == actual_events[0]['state_key'] == 'positions'
    assert expected_events[0]['state_file'] == actual_events[0]['state_file'] == 'positions.json'
    assert expected_events[0]['fallback_used'] == actual_events[0]['fallback_used'] == 'empty_positions'
    assert expected_events[0]['consumer'] == actual_events[0]['consumer'] == 'build_local_open_positions_for_risk'


def test_load_risk_state_emits_rate_limited_event_on_malformed_risk_state_json(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    (tmp_path / 'risk_state.json').write_text('{bad json', encoding='utf-8')

    state_first = mod.load_risk_state(store)
    state_second = mod.load_risk_state(store)
    events_path = tmp_path / 'events.jsonl'
    event_rows = [mod.json.loads(line) for line in events_path.read_text(encoding='utf-8').splitlines() if line.strip()]

    assert state_first == mod.default_risk_state()
    assert state_second == mod.default_risk_state()
    assert len(event_rows) == 1
    assert event_rows[0]['event_type'] == 'runtime_state_degraded'
    assert event_rows[0]['state_file'] == 'risk_state.json'
    assert event_rows[0]['state_key'] == 'risk_state'
    assert event_rows[0]['fallback_used'] == 'default_risk_state'
    assert event_rows[0]['consumer'] == 'load_risk_state'
    assert event_rows[0]['error_type'] == 'JSONDecodeError'


def test_load_risk_state_merges_defaults_and_refreshes_heat_snapshot(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('risk_state', {
        'daily_realized_pnl': -12.5,
        'portfolio_heat_open_r': 0.4,
        'symbol_cooldowns': ['corrupted'],
        'portfolio_exposure_pct_by_theme': ['bad'],
        'portfolio_heat_r_by_theme': {'old_theme': 0.4},
        'portfolio_heat_r_by_correlation': {'old_corr': 0.6},
    })

    monkeypatch.setattr(mod, 'compute_positions_heat_snapshot', lambda _positions: {
        'tracked_positions': 2,
        'open_heat_r': 1.8,
        'heat_r_by_theme': {'ai': 1.1},
        'heat_r_by_correlation': {'meme-beta': 0.7},
    })

    state = mod.load_risk_state(store)

    assert state['daily_realized_pnl'] == -12.5
    assert state['halted'] is False
    assert state['symbol_cooldowns'] == {}
    assert state['portfolio_exposure_pct_by_theme'] == {}
    assert state['portfolio_exposure_pct_by_correlation'] == {}
    assert state['portfolio_heat_open_r'] == 1.8
    assert state['portfolio_heat_r_by_theme'] == {'ai': 1.1}
    assert state['portfolio_heat_r_by_correlation'] == {'meme-beta': 0.7}


def test_load_risk_state_preserves_existing_heat_when_snapshot_has_no_open_positions(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('risk_state', {
        'portfolio_heat_open_r': 0.9,
        'portfolio_heat_r_by_theme': {'defi': 0.4},
        'portfolio_heat_r_by_correlation': {'alts': 0.5},
    })

    monkeypatch.setattr(mod, 'compute_positions_heat_snapshot', lambda _positions: {
        'tracked_positions': 0,
        'open_heat_r': 3.2,
        'heat_r_by_theme': {'ignored': 9.9},
        'heat_r_by_correlation': {'ignored': 8.8},
    })

    state = mod.load_risk_state(store)

    assert state['portfolio_heat_open_r'] == 0.9
    assert state['portfolio_heat_r_by_theme'] == {'defi': 0.4}
    assert state['portfolio_heat_r_by_correlation'] == {'alts': 0.5}


def test_normalize_loaded_risk_state_matches_risk_state_module(tmp_path):
    runtime_risk_state = {
        'daily_realized_pnl': -12.5,
        'portfolio_heat_open_r': 0.4,
        'symbol_cooldowns': ['corrupted'],
        'portfolio_exposure_pct_by_theme': ['bad'],
        'portfolio_heat_r_by_theme': {'old_theme': 0.4},
        'portfolio_heat_r_by_correlation': {'old_corr': 0.6},
    }

    expected = mod.normalize_loaded_risk_state(runtime_risk_state)

    helper_path = SCRIPTS_DIR / 'risk_state_helpers.py'
    helper_spec = importlib.util.spec_from_file_location('risk_state_helpers', helper_path)
    assert helper_spec is not None
    helper_mod = importlib.util.module_from_spec(helper_spec)
    sys.modules[helper_spec.name] = helper_mod
    assert helper_spec.loader is not None
    helper_spec.loader.exec_module(helper_mod)

    actual = helper_mod.normalize_loaded_risk_state(runtime_risk_state, mod.default_risk_state)

    assert actual == expected


def test_refresh_risk_state_heat_snapshot_matches_risk_state_module(monkeypatch):
    risk_state = {
        'portfolio_heat_open_r': 0.9,
        'portfolio_heat_r_by_theme': {'defi': 0.4},
        'portfolio_heat_r_by_correlation': {'alts': 0.5},
    }
    positions_state = {'BTCUSDT:LONG': {'symbol': 'BTCUSDT', 'position_side': 'LONG'}}
    heat_snapshot = {
        'tracked_positions': 2,
        'open_heat_r': 1.8,
        'heat_r_by_theme': {'ai': 1.1},
        'heat_r_by_correlation': {'meme-beta': 0.7},
    }

    monkeypatch.setattr(mod, 'compute_positions_heat_snapshot', lambda _positions: dict(heat_snapshot))
    expected = mod.refresh_risk_state_heat_snapshot(risk_state, positions_state)

    helper_path = SCRIPTS_DIR / 'risk_state_helpers.py'
    helper_spec = importlib.util.spec_from_file_location('risk_state_helpers', helper_path)
    assert helper_spec is not None
    helper_mod = importlib.util.module_from_spec(helper_spec)
    sys.modules[helper_spec.name] = helper_mod
    assert helper_spec.loader is not None
    helper_spec.loader.exec_module(helper_mod)

    actual = helper_mod.refresh_risk_state_heat_snapshot(
        risk_state,
        positions_state,
        lambda _positions: dict(heat_snapshot),
    )

    assert actual == expected


def test_load_risk_state_matches_runtime_state_consumer_helper(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('risk_state', {
        'daily_realized_pnl': -12.5,
        'portfolio_heat_open_r': 0.4,
        'symbol_cooldowns': ['corrupted'],
        'portfolio_exposure_pct_by_theme': ['bad'],
        'portfolio_heat_r_by_theme': {'old_theme': 0.4},
        'portfolio_heat_r_by_correlation': {'old_corr': 0.6},
    })
    store.save_json('positions', {'BTCUSDT:LONG': {'symbol': 'BTCUSDT', 'position_side': 'LONG'}})

    heat_snapshot = {
        'tracked_positions': 2,
        'open_heat_r': 1.8,
        'heat_r_by_theme': {'ai': 1.1},
        'heat_r_by_correlation': {'meme-beta': 0.7},
    }
    monkeypatch.setattr(mod, 'compute_positions_heat_snapshot', lambda _positions: dict(heat_snapshot))
    expected = mod.load_risk_state(store)

    helper_path = SCRIPTS_DIR / 'runtime_state_risk_helpers.py'
    helper_spec = importlib.util.spec_from_file_location('runtime_state_risk_helpers', helper_path)
    assert helper_spec is not None
    helper_mod = importlib.util.module_from_spec(helper_spec)
    sys.modules[helper_spec.name] = helper_mod
    assert helper_spec.loader is not None
    helper_spec.loader.exec_module(helper_mod)

    helper_root = tmp_path / 'helper-store'
    helper_root.mkdir(parents=True, exist_ok=True)
    helper_store = mod.RuntimeStateStore(str(helper_root))
    helper_store.save_json('risk_state', {
        'daily_realized_pnl': -12.5,
        'portfolio_heat_open_r': 0.4,
        'symbol_cooldowns': ['corrupted'],
        'portfolio_exposure_pct_by_theme': ['bad'],
        'portfolio_heat_r_by_theme': {'old_theme': 0.4},
        'portfolio_heat_r_by_correlation': {'old_corr': 0.6},
    })
    helper_store.save_json('positions', {'BTCUSDT:LONG': {'symbol': 'BTCUSDT', 'position_side': 'LONG'}})

    actual = helper_mod.load_runtime_risk_state(
        helper_store,
        should_emit_runtime_state_degraded=mod._should_emit_runtime_state_degraded,
        append_runtime_state_degraded_event=mod.append_rate_limited_runtime_event,
        default_risk_state=mod.default_risk_state,
        normalize_loaded_risk_state=mod.normalize_loaded_risk_state,
        refresh_risk_state_heat_snapshot=mod.refresh_risk_state_heat_snapshot,
        compute_positions_heat_snapshot=lambda _positions: dict(heat_snapshot),
    )

    assert actual == expected


def test_load_risk_state_matches_runtime_state_consumer_helper_on_malformed_risk_state_json(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    (tmp_path / 'risk_state.json').write_text('{bad json', encoding='utf-8')
    store.save_json('positions', {'BTCUSDT:LONG': {'symbol': 'BTCUSDT', 'position_side': 'LONG'}})

    heat_snapshot = {
        'tracked_positions': 2,
        'open_heat_r': 1.8,
        'heat_r_by_theme': {'ai': 1.1},
        'heat_r_by_correlation': {'meme-beta': 0.7},
    }
    monkeypatch.setattr(mod, 'compute_positions_heat_snapshot', lambda _positions: dict(heat_snapshot))
    expected = mod.load_risk_state(store)
    expected_events = store.read_events(limit=10)

    helper_path = SCRIPTS_DIR / 'runtime_state_risk_helpers.py'
    helper_spec = importlib.util.spec_from_file_location('runtime_state_risk_helpers', helper_path)
    assert helper_spec is not None
    helper_mod = importlib.util.module_from_spec(helper_spec)
    sys.modules[helper_spec.name] = helper_mod
    assert helper_spec.loader is not None
    helper_spec.loader.exec_module(helper_mod)

    helper_root = tmp_path / 'helper-risk-store'
    helper_root.mkdir(parents=True, exist_ok=True)
    (helper_root / 'risk_state.json').write_text('{bad json', encoding='utf-8')
    helper_store = mod.RuntimeStateStore(str(helper_root))
    helper_store.save_json('positions', {'BTCUSDT:LONG': {'symbol': 'BTCUSDT', 'position_side': 'LONG'}})

    actual = helper_mod.load_runtime_risk_state(
        helper_store,
        should_emit_runtime_state_degraded=mod._should_emit_runtime_state_degraded,
        append_runtime_state_degraded_event=mod.append_rate_limited_runtime_event,
        default_risk_state=mod.default_risk_state,
        normalize_loaded_risk_state=mod.normalize_loaded_risk_state,
        refresh_risk_state_heat_snapshot=mod.refresh_risk_state_heat_snapshot,
        compute_positions_heat_snapshot=lambda _positions: dict(heat_snapshot),
    )
    actual_events = helper_store.read_events(limit=10)

    assert actual == expected
    assert len(expected_events) == 1
    assert len(actual_events) == 1
    assert expected_events[0]['event_type'] == actual_events[0]['event_type'] == 'runtime_state_degraded'
    assert expected_events[0]['state_key'] == actual_events[0]['state_key'] == 'risk_state'
    assert expected_events[0]['state_file'] == actual_events[0]['state_file'] == 'risk_state.json'
    assert expected_events[0]['fallback_used'] == actual_events[0]['fallback_used'] == 'default_risk_state'
    assert expected_events[0]['consumer'] == actual_events[0]['consumer'] == 'load_risk_state'


def test_runtime_store_load_json_returns_default_for_malformed_last_cycle_json_without_rewrite(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    raw_path = tmp_path / 'last_cycle.json'
    raw_path.write_text('{bad json', encoding='utf-8')

    loaded = store.load_json('last_cycle', {'cycle': {}})

    assert loaded == {'cycle': {}}
    assert raw_path.read_text(encoding='utf-8') == '{bad json'


def test_runtime_store_load_json_with_error_reports_malformed_last_cycle_json(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    raw_path = tmp_path / 'last_cycle.json'
    raw_path.write_text('{bad json', encoding='utf-8')

    loaded, load_error = store.load_json_with_error('last_cycle', {'cycle': {}})

    assert loaded == {'cycle': {}}
    assert load_error is not None
    assert load_error['state_key'] == 'last_cycle'
    assert load_error['state_file'] == 'last_cycle.json'
    assert load_error['error_type'] == 'JSONDecodeError'
    assert raw_path.read_text(encoding='utf-8') == '{bad json'


def test_runtime_store_load_json_returns_default_for_malformed_user_data_stream_json_without_rewrite(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    raw_path = tmp_path / 'user_data_stream.json'
    raw_path.write_text('{bad json', encoding='utf-8')

    loaded = store.load_json('user_data_stream', {})

    assert loaded == {}
    assert raw_path.read_text(encoding='utf-8') == '{bad json'


def test_runtime_store_load_json_with_error_reports_malformed_user_data_stream_json(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    raw_path = tmp_path / 'user_data_stream.json'
    raw_path.write_text('{bad json', encoding='utf-8')

    loaded, load_error = store.load_json_with_error('user_data_stream', {})

    assert loaded == {}
    assert load_error is not None
    assert load_error['state_key'] == 'user_data_stream'
    assert load_error['state_file'] == 'user_data_stream.json'
    assert load_error['error_type'] == 'JSONDecodeError'
    assert raw_path.read_text(encoding='utf-8') == '{bad json'


def test_apply_management_action_uses_position_side_for_short_reduce_and_stop(monkeypatch):
    state = mod.TradeManagementState(
        symbol='TESTUSDT',
        side='long',
        position_side='SHORT',
        initial_quantity=1.0,
        remaining_quantity=1.0,
        current_stop_price=105.0,
    )
    meta = make_meta()
    calls = []

    monkeypatch.setattr(mod, 'cancel_order', lambda *args, **kwargs: calls.append(('cancel', kwargs.get('order_id'))))
    monkeypatch.setattr(mod, 'place_reduce_only_market', lambda client, symbol, quantity, meta, side=None: calls.append(('reduce', symbol, quantity, side)) or {'orderId': 88})
    monkeypatch.setattr(mod, 'place_stop_market_order', lambda client, symbol, stop_price, quantity, meta, side=None: calls.append(('stop', symbol, stop_price, quantity, side)) or {'orderId': 99, 'triggerPrice': stop_price})

    new_state, active_stop, payload = mod.apply_management_action(
        client=object(),
        symbol='TESTUSDT',
        meta=meta,
        state=state,
        action={'type': 'take_profit_1', 'close_qty': 0.4, 'new_stop_price': 101.0},
        active_stop_order={'orderId': 77},
    )

    assert ('reduce', 'TESTUSDT', 0.4, 'SHORT') in calls
    assert ('stop', 'TESTUSDT', 101.0, 0.6, 'SHORT') in calls
    assert new_state.remaining_quantity == 0.6
    assert new_state.tp1_hit is True
    assert active_stop['orderId'] == 99
    assert payload['reduce_order']['orderId'] == 88


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


def test_evaluate_management_actions_requires_breakeven_confirmation_buffer_after_tp1():
    state = mod.TradeManagementState(symbol='TESTUSDT', initial_quantity=1.0, remaining_quantity=0.5, tp1_hit=True)
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
    assert actions[0]['new_stop_price'] == 102.0


def test_evaluate_management_actions_triggers_micro_scalp_time_stop_after_min_profit_window():
    state = mod.TradeManagementState(
        symbol='TESTUSDT',
        initial_quantity=1.0,
        remaining_quantity=0.55,
        realized_r=0.35,
        opened_at='2026-05-15T12:00:00Z',
    )
    plan = mod.TradeManagementPlan(
        entry_price=100.0,
        stop_price=95.0,
        quantity=1.0,
        initial_risk_per_unit=5.0,
        breakeven_trigger_price=101.0,
        tp1_trigger_price=105.0,
        tp1_close_qty=0.45,
        tp2_trigger_price=110.0,
        tp2_close_qty=0.30,
        runner_qty=0.25,
        micro_scalp_time_stop_sec=300,
        micro_scalp_min_profit_r=0.3,
    )

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=101.0,
        ema5m=100.8,
        trailing_reference=101.2,
        trailing_buffer_pct=0.02,
        now=datetime.datetime(2026, 5, 15, 12, 6, 0, tzinfo=datetime.timezone.utc),
    )

    assert [action['type'] for action in actions] == ['move_stop_to_breakeven', 'micro_scalp_time_stop']
    assert actions[1]['close_qty'] == 0.55
    assert actions[1]['exit_reason'] == 'micro_scalp_time_stop'


def test_build_trade_management_state_from_position_restores_tp_runner_checkpoint():
    position = {
        'symbol': 'TESTUSDT',
        'position_side': 'LONG',
        'entry_price': 100.0,
        'quantity': 1.2,
        'remaining_quantity': 0.42,
        'current_stop_price': 100.2,
        'moved_to_breakeven': True,
        'tp1_hit': True,
        'tp2_hit': True,
        'highest_price_seen': 113.4,
        'lowest_price_seen': 98.7,
        'opened_at': '2026-05-15T12:00:00Z',
        'first_1r_at': '2026-05-15T12:03:00Z',
        'realized_r': 1.18,
    }

    state = mod.build_trade_management_state_from_position(position)

    assert state.symbol == 'TESTUSDT'
    assert state.position_key == 'TESTUSDT:LONG'
    assert state.side == 'long'
    assert state.position_side == 'LONG'
    assert state.initial_quantity == 1.2
    assert state.remaining_quantity == 0.42
    assert state.current_stop_price == 100.2
    assert state.moved_to_breakeven is True
    assert state.tp1_hit is True
    assert state.tp2_hit is True
    assert state.highest_price_seen == 113.4
    assert state.lowest_price_seen == 98.7
    assert state.opened_at == '2026-05-15T12:00:00Z'
    assert state.first_1r_at == '2026-05-15T12:03:00Z'
    assert state.realized_r == 1.18


def test_build_trade_management_state_from_position_infers_runner_checkpoint_from_remaining_quantity():
    position = {
        'symbol': 'TESTUSDT',
        'position_side': 'LONG',
        'entry_price': 100.0,
        'quantity': 1.0,
        'remaining_quantity': 0.25,
        'current_stop_price': 101.0,
        'trade_management_plan': {
            'entry_price': 100.0,
            'stop_price': 95.0,
            'quantity': 1.0,
            'initial_risk_per_unit': 5.0,
            'breakeven_trigger_price': 105.0,
            'tp1_trigger_price': 107.5,
            'tp1_close_qty': 0.3,
            'tp2_trigger_price': 110.0,
            'tp2_close_qty': 0.45,
            'runner_qty': 0.25,
            'side': 'long',
            'position_side': 'LONG',
        },
    }

    state = mod.build_trade_management_state_from_position(position)

    assert state.remaining_quantity == 0.25
    assert state.moved_to_breakeven is True
    assert state.tp1_hit is True
    assert state.tp2_hit is True


def test_build_trade_management_plan_from_position_inherits_micro_scalp_time_stop_args():
    position = {
        'symbol': 'TESTUSDT',
        'position_side': 'LONG',
        'entry_price': 100.0,
        'current_stop_price': 95.0,
        'quantity': 1.2,
    }
    args = argparse.Namespace(
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp1_profit_usdt=0.0,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        tp2_profit_usdt=0.0,
        breakeven_r=1.0,
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.001,
        micro_scalp_time_stop_sec=420,
        micro_scalp_min_profit_r=0.25,
    )

    plan = mod.build_trade_management_plan_from_position(position, args)

    assert plan.micro_scalp_time_stop_sec == 420
    assert plan.micro_scalp_min_profit_r == 0.25


def test_evaluate_management_actions_hits_tp1_before_breakeven_for_long():
    state = mod.TradeManagementState(symbol='TESTUSDT', initial_quantity=1.0, remaining_quantity=1.0)
    plan = mod.TradeManagementPlan(
        entry_price=100.0,
        stop_price=95.0,
        quantity=1.0,
        initial_risk_per_unit=5.0,
        breakeven_trigger_price=106.75,
        tp1_trigger_price=105.0,
        tp1_close_qty=0.45,
        tp2_trigger_price=111.0,
        tp2_close_qty=0.30,
        runner_qty=0.25,
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.0015,
    )

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=105.1,
        ema5m=104.8,
        trailing_reference=105.4,
        trailing_buffer_pct=0.025,
    )

    assert [action['type'] for action in actions] == ['take_profit_1']
    assert actions[0]['close_qty'] == 0.45


def test_evaluate_management_actions_moves_long_stop_to_buffered_breakeven_after_tp1():
    state = mod.TradeManagementState(
        symbol='TESTUSDT',
        initial_quantity=1.0,
        remaining_quantity=0.55,
        tp1_hit=True,
    )
    plan = mod.TradeManagementPlan(
        entry_price=100.0,
        stop_price=95.0,
        quantity=1.0,
        initial_risk_per_unit=5.0,
        breakeven_trigger_price=106.75,
        tp1_trigger_price=105.0,
        tp1_close_qty=0.45,
        tp2_trigger_price=111.0,
        tp2_close_qty=0.30,
        runner_qty=0.25,
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.0015,
    )

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=106.9,
        ema5m=100.2,
        trailing_reference=107.2,
        trailing_buffer_pct=0.025,
    )

    assert actions[0]['type'] == 'move_stop_to_breakeven'
    assert actions[0]['new_stop_price'] == 100.15


def test_evaluate_management_actions_moves_short_stop_to_buffered_breakeven_after_tp1():
    state = mod.TradeManagementState(
        symbol='TESTUSDT',
        initial_quantity=1.0,
        remaining_quantity=0.55,
        side='short',
        position_side='SHORT',
        tp1_hit=True,
    )
    plan = mod.TradeManagementPlan(
        entry_price=100.0,
        stop_price=105.0,
        quantity=1.0,
        initial_risk_per_unit=5.0,
        breakeven_trigger_price=93.25,
        tp1_trigger_price=95.0,
        tp1_close_qty=0.45,
        tp2_trigger_price=89.0,
        tp2_close_qty=0.30,
        runner_qty=0.25,
        side='short',
        position_side='SHORT',
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.0015,
    )

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=93.1,
        ema5m=99.8,
        trailing_reference=92.7,
        trailing_buffer_pct=0.025,
    )

    assert actions[0]['type'] == 'move_stop_to_breakeven'
    assert actions[0]['new_stop_price'] == 99.85


def test_evaluate_management_actions_requires_tp1_before_breakeven_for_long():
    state = mod.TradeManagementState(symbol='TESTUSDT', initial_quantity=1.0, remaining_quantity=1.0)
    plan = mod.TradeManagementPlan(
        entry_price=100.0,
        stop_price=95.0,
        quantity=1.0,
        initial_risk_per_unit=5.0,
        breakeven_trigger_price=106.75,
        tp1_trigger_price=105.0,
        tp1_close_qty=0.45,
        tp2_trigger_price=111.0,
        tp2_close_qty=0.30,
        runner_qty=0.25,
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.0015,
    )

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=106.9,
        ema5m=100.2,
        trailing_reference=107.2,
        trailing_buffer_pct=0.025,
    )

    assert [action['type'] for action in actions] == ['take_profit_1', 'move_stop_to_breakeven']
    assert actions[1]['new_stop_price'] == 100.15


    state = mod.TradeManagementState(
        symbol='TESTUSDT',
        initial_quantity=1.0,
        remaining_quantity=1.0,
        moved_to_breakeven=True,
        current_stop_price=103.5,
    )
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
    )

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=105.2,
        ema5m=101.0,
        trailing_reference=105.6,
        trailing_buffer_pct=0.02,
    )

    assert actions[0]['type'] == 'take_profit_1'
    assert actions[0]['new_stop_price'] == 103.5


def test_evaluate_management_actions_tp2_tightens_short_stop_downward_from_current_stop():
    state = mod.TradeManagementState(
        symbol='TESTUSDT',
        initial_quantity=1.0,
        remaining_quantity=0.5,
        side='short',
        position_side='SHORT',
        moved_to_breakeven=True,
        tp1_hit=True,
        current_stop_price=96.0,
    )
    plan = mod.TradeManagementPlan(
        entry_price=100.0,
        stop_price=105.0,
        quantity=1.0,
        initial_risk_per_unit=5.0,
        breakeven_trigger_price=99.0,
        tp1_trigger_price=95.0,
        tp1_close_qty=0.5,
        tp2_trigger_price=90.0,
        tp2_close_qty=0.3,
        runner_qty=0.2,
        side='short',
        position_side='SHORT',
    )

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=89.8,
        ema5m=97.0,
        trailing_reference=89.0,
        trailing_buffer_pct=0.02,
    )

    assert actions[0]['type'] == 'take_profit_2'
    assert actions[0]['new_stop_price'] == 95.0


def test_evaluate_management_actions_tp1_tightens_short_stop_downward_from_wider_stop():
    state = mod.TradeManagementState(
        symbol='TESTUSDT',
        initial_quantity=1.0,
        remaining_quantity=1.0,
        side='short',
        position_side='SHORT',
        moved_to_breakeven=True,
        current_stop_price=104.0,
    )
    plan = mod.TradeManagementPlan(
        entry_price=100.0,
        stop_price=105.0,
        quantity=1.0,
        initial_risk_per_unit=5.0,
        breakeven_trigger_price=99.0,
        tp1_trigger_price=95.0,
        tp1_close_qty=0.5,
        tp2_trigger_price=90.0,
        tp2_close_qty=0.3,
        runner_qty=0.2,
        side='short',
        position_side='SHORT',
    )

    actions = mod.evaluate_management_actions(
        state,
        plan,
        current_price=94.8,
        ema5m=98.0,
        trailing_reference=94.0,
        trailing_buffer_pct=0.02,
    )

    assert actions[0]['type'] == 'take_profit_1'
    assert actions[0]['new_stop_price'] == 98.0


def test_monitor_live_trade_persists_exit_reason_and_trade_invalidated(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    symbol = 'TESTUSDT'
    store.save_json('positions', {
        symbol: {
            'symbol': symbol,
            'status': 'monitoring',
            'quantity': 1.0,
            'remaining_quantity': 1.0,
            'opened_at': '2026-04-29T00:00:00Z',
            'selected_score': 82.6,
            'selected_state': 'launch',
            'selected_alert_tier': 'critical',
            'candidate_stage': 'launch',
            'trigger_class': 'breakout',
                'score_decile': '80-89',
                'market_regime_label': 'risk_on',
                'market_regime_multiplier': 1.1,
                'setup_ready': True,
                'trigger_fired': True,
                'highest_price_seen': 111.0,
                'lowest_price_seen': 100.0,
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
    monkeypatch.setattr(mod, '_utc_now', lambda: mod.datetime.datetime(2026, 4, 29, 0, 5, tzinfo=mod.datetime.timezone.utc))
    monkeypatch.setattr(mod, 'evaluate_management_actions', lambda *a, **k: [{'type': 'take_profit_1', 'close_qty': 1.0, 'exit_reason': 'tp1'}])

    def fake_apply(client, symbol, meta, state, action, active_stop_order):
        action['exit_reason'] = 'tp1'
        state.remaining_quantity = 0.0
        state.tp1_hit = True
        return state, None, {'reduce_order': {'orderId': 888, 'avgPrice': '111.0'}}

    monkeypatch.setattr(mod, 'apply_management_action', fake_apply)

    result = mod.monitor_live_trade(client=object(), symbol=symbol, meta=make_meta(), args=args, trade=trade, store=store)
    rows = [mod.json.loads(line) for line in (tmp_path / 'events.jsonl').read_text(encoding='utf-8').splitlines() if line.strip()]
    positions = store.load_json('positions', {})

    assert result['status'] == 'closed'
    assert result['exit_reason'] == 'tp1'
    assert result['realized_r'] == 2.2
    assert rows[-1]['event_type'] == 'trade_invalidated'
    assert rows[-1]['exit_reason'] == 'tp1'
    assert rows[-1]['realized_r'] == 2.2
    assert rows[-1]['mfe_r'] == 2.2
    assert rows[-1]['mae_r'] == 0.0
    assert rows[-1]['time_to_1r'] == 5.0
    assert rows[-1]['time_in_trade_minutes'] == 5.0
    assert rows[-1]['trigger_class'] == 'breakout'
    assert rows[-1]['score_decile'] == '80-89'
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
    assert positions['DOGEUSDT:LONG']['status'] == 'closed'
    assert positions['DOGEUSDT:LONG']['remaining_quantity'] == 0.0
    assert positions['DOGEUSDT:LONG']['stop_order_id'] is None
    assert positions['DOGEUSDT:LONG']['protection_status'] == 'flat'
    assert positions['BTCUSDT:LONG']['quantity'] == 2.5
    assert positions['BTCUSDT:LONG']['remaining_quantity'] == 2.5
    assert positions['BTCUSDT:LONG']['protection_status'] == 'protected'


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
    assert positions['DOGEUSDT:LONG']['status'] == 'closed'
    assert positions['DOGEUSDT:LONG']['remaining_quantity'] == 0.0
    assert positions['DOGEUSDT:LONG']['protection_status'] == 'flat'


def test_sync_tracked_positions_with_exchange_clears_recovery_flags_when_position_closes(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT': {
            'symbol': 'DOGEUSDT',
            'status': 'monitoring',
            'quantity': 5.0,
            'remaining_quantity': 5.0,
            'stop_order_id': 123,
            'protection_status': 'protected',
            'recovery_incomplete': True,
            'protected_recovery_pending': True,
            'opened_at': '2026-04-29T00:00:00Z',
            'selected_score': 82.6,
            'selected_state': 'launch',
            'selected_alert_tier': 'critical',
            'candidate_stage': 'launch',
            'trigger_class': 'breakout_retest',
            'market_regime_label': 'expansion',
            'market_regime_multiplier': 1.25,
            'setup_ready': True,
            'trigger_fired': True,
            'trade_management_plan': {
                'position_side': 'LONG',
                'side': 'BUY',
                'quantity': 5.0,
                'stop_price': 0.12,
                'initial_stop_price': 0.12,
                'initial_risk_per_unit': 0.01,
                'tp1': 0.12,
            },
        },
    })

    mod.sync_tracked_positions_with_exchange(store, exchange_positions=[])
    positions = store.load_json('positions', {})

    assert positions['DOGEUSDT:LONG']['status'] == 'closed'
    assert positions['DOGEUSDT:LONG']['recovery_incomplete'] is False
    assert positions['DOGEUSDT:LONG']['protected_recovery_pending'] is False
    assert positions['DOGEUSDT:LONG']['trade_management_plan'] == {}
    assert positions['DOGEUSDT:LONG']['exchange_reconcile_reason'] == 'exchange_position_missing'
    assert positions['DOGEUSDT:LONG']['opened_at'] == '2026-04-29T00:00:00Z'
    assert positions['DOGEUSDT:LONG']['selected_score'] == 82.6
    assert positions['DOGEUSDT:LONG']['selected_state'] == 'launch'

    rows = [mod.json.loads(line) for line in store._events_path().read_text(encoding='utf-8').splitlines() if line.strip()]
    assert rows[-1]['event_type'] == 'trade_invalidated'
    assert rows[-1]['symbol'] == 'DOGEUSDT'
    assert rows[-1]['position_side'] == 'LONG'
    assert rows[-1]['exit_reason'] == 'exchange_position_missing'
    assert rows[-1]['opened_at'] == '2026-04-29T00:00:00Z'
    assert rows[-1]['score'] == 82.6
    assert rows[-1]['state'] == 'launch'
    assert rows[-1]['alert_tier'] == 'critical'


def test_sync_tracked_positions_with_exchange_does_not_repeat_reconcile_close_events_for_already_closed_position(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_side': 'LONG',
            'status': 'monitoring',
            'quantity': 0.0,
            'filled_quantity': 0.005,
            'remaining_quantity': 0.0,
            'exchange_position_amt': 0.0,
            'protection_status': 'flat',
            'exchange_reconcile_reason': 'exchange_position_missing',
            'closed_at': '2026-05-16T10:01:20.513347Z',
            'exit_reason': 'exchange_position_missing',
            'opened_at': '2026-05-16T09:49:37.968235Z',
            'selected_score': 43.2272,
            'selected_state': 'launch',
            'selected_alert_tier': 'blocked',
        },
    })

    first = mod.sync_tracked_positions_with_exchange(store, exchange_positions=[])
    second = mod.sync_tracked_positions_with_exchange(store, exchange_positions=[])
    events_path = store._events_path()
    rows = [mod.json.loads(line) for line in events_path.read_text(encoding='utf-8').splitlines() if line.strip()] if events_path.exists() else []

    assert first['closed_symbols'] == []
    assert second['closed_symbols'] == []
    assert rows == []


def test_sync_tracked_positions_with_exchange_clears_orphan_close_fields_when_position_closes(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_side': 'LONG',
            'status': 'orphan',
            'monitor_mode': 'background_thread',
            'quantity': 5.0,
            'remaining_quantity': 5.0,
            'exchange_position_amt': 5.0,
            'notional': 600.0,
            'unrealized_pnl': 12.0,
            'mark_price': 0.1234,
            'stop_order_id': 123,
            'protection_status': 'missing',
            'user_data_stream': {'status': 'healthy', 'listen_key': 'lk-1'},
            'book_ticker_websocket': {'status': 'healthy'},
            'monitor_thread_name': 'trade-monitor-1',
            'active_stop_order': {'orderId': 123},
        },
    })

    mod.sync_tracked_positions_with_exchange(store, exchange_positions=[])
    positions = store.load_json('positions', {})

    assert positions['DOGEUSDT:LONG']['status'] == 'closed'
    assert positions['DOGEUSDT:LONG']['closed_at']
    assert positions['DOGEUSDT:LONG']['exit_reason'] == 'exchange_position_missing'
    assert positions['DOGEUSDT:LONG']['monitor_mode'] == 'closed'
    assert positions['DOGEUSDT:LONG']['user_data_stream'] == {}
    assert positions['DOGEUSDT:LONG']['book_ticker_websocket'] == {}
    assert positions['DOGEUSDT:LONG']['monitor_thread_name'] == ''
    assert positions['DOGEUSDT:LONG']['active_stop_order'] == {}


def test_reconcile_runtime_state_okx_simulated_closes_stale_runtime_positions(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'status': 'monitoring',
            'quantity': 5.0,
            'remaining_quantity': 5.0,
            'stop_order_id': 123,
            'protection_status': 'simulated',
            'trade_management_plan': {'quantity': 5.0},
        },
    })
    args = argparse.Namespace()

    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [])

    result = mod.reconcile_runtime_state(client=object(), store=store, halt_on_orphan_position=False, args=args)
    positions = store.load_json('positions', {})

    assert result['closed_tracked_positions'] == ['DOGEUSDT']
    assert result['position_count'] == 0
    assert positions['DOGEUSDT:LONG']['status'] == 'closed'
    assert positions['DOGEUSDT:LONG']['remaining_quantity'] == 0.0
    assert positions['DOGEUSDT:LONG']['exchange_reconcile_reason'] == 'exchange_position_missing'


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
    assert positions['DOGEUSDT:LONG']['protection_status'] == 'protected'
    assert positions['DOGEUSDT:LONG']['stop_order_id'] == 999


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


def test_sync_tracked_positions_with_exchange_zeroes_runtime_fields_for_closed_records(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'ZECUSDT:LONG': {
            'symbol': 'ZECUSDT',
            'side': 'LONG',
            'position_side': 'LONG',
            'status': 'closed',
            'quantity': 0.354,
            'remaining_quantity': 0.0,
            'exchange_position_amt': 0.354,
            'notional': 200.0454,
            'unrealized_pnl': 1.23,
            'mark_price': 565.1,
            'protection_status': 'flat',
            'trade_management_plan': None,
            'recovery_incomplete': True,
            'recovery_reason': 'missing_valid_stop_distance',
        },
        'ZECUSDT': {
            'symbol': 'ZECUSDT',
            'side': 'LONG',
            'position_side': 'LONG',
            'status': 'closed',
            'quantity': 0.354,
            'remaining_quantity': 0.0,
            'exchange_position_amt': 0.354,
            'notional': 200.0454,
            'unrealized_pnl': 1.23,
            'mark_price': 565.1,
            'protection_status': 'flat',
            'trade_management_plan': None,
            'recovery_incomplete': True,
            'recovery_reason': 'missing_valid_stop_distance',
        },
    })

    result = mod.sync_tracked_positions_with_exchange(store, [], protected_symbols=[])

    positions = store.load_json('positions', {})
    assert result['closed_symbols'] == ['ZECUSDT']
    assert set(positions.keys()) == {'ZECUSDT:LONG'}
    tracked = positions['ZECUSDT:LONG']
    assert tracked['status'] == 'closed'
    assert tracked['remaining_quantity'] == 0.0
    assert tracked['quantity'] == 0.0
    assert tracked['exchange_position_amt'] == 0.0
    assert tracked['notional'] == 0.0
    assert tracked['unrealized_pnl'] == 0.0
    assert tracked['mark_price'] == 0.0
    assert tracked['protection_status'] == 'flat'


def test_run_loop_forwards_args_into_reconcile_runtime_state(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    captured = {}
    args = argparse.Namespace(
        halt_on_orphan_position=False,
        repair_missing_protection=True,
        reconcile_only=True,
        profile='unit-test',
        live=True,
        scan_only=False,
        auto_loop=False,
    )

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda _args: store)
    monkeypatch.setattr(mod, 'is_binance_simulated_trading', lambda _args: False)
    monkeypatch.setattr(mod, 'execution_exchange_label', lambda _args: 'binance')

    def fake_reconcile_runtime_state(client, store, halt_on_orphan_position=False, repair_missing_protection_enabled=True, args=None):
        captured['client'] = client
        captured['store'] = store
        captured['halt_on_orphan_position'] = halt_on_orphan_position
        captured['repair_missing_protection_enabled'] = repair_missing_protection_enabled
        captured['args'] = args
        return {'ok': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []}

    monkeypatch.setattr(mod, 'reconcile_runtime_state', fake_reconcile_runtime_state)

    result = mod.run_loop(client=object(), args=args)

    assert result['mode'] == 'reconcile_only'
    assert captured['store'] is store
    assert captured['halt_on_orphan_position'] is False
    assert captured['repair_missing_protection_enabled'] is True
    assert captured['args'] is args



def test_reconcile_runtime_state_keeps_protected_recovery_pending_without_valid_plan(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'GIGGLEUSDT:SHORT': {
            'symbol': 'GIGGLEUSDT',
            'side': 'SHORT',
            'status': 'protected_recovery_pending',
            'quantity': 1.28,
            'remaining_quantity': 1.28,
            'entry_price': 37.4,
            'stop_price': 37.4,
            'current_stop_price': 37.4,
            'protection_status': 'protected',
            'trade_management_plan': None,
            'recovery_incomplete': True,
            'recovery_reason': 'missing_valid_stop_distance',
        },
    })

    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'GIGGLEUSDT', 'positionAmt': '-1.28', 'positionSide': 'SHORT'}])
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *args, **kwargs: {
        'status': 'protected',
        'active_position': {'symbol': 'GIGGLEUSDT', 'positionAmt': '-1.28', 'positionSide': 'SHORT'},
        'expected_order_id': None,
        'open_orders': [{'orderId': 654, 'type': 'STOP_MARKET'}],
    })

    result = mod.reconcile_runtime_state(
        client=object(),
        store=store,
        halt_on_orphan_position=False,
        repair_missing_protection_enabled=True,
        args=argparse.Namespace(
            tp1_r=1.5,
            tp1_close_pct=0.3,
            tp2_r=2.0,
            tp2_close_pct=0.4,
            breakeven_r=1.0,
            breakeven_confirmation_mode='ema_support',
            breakeven_min_buffer_pct=0.001,
        ),
    )

    positions = store.load_json('positions', {})
    assert result['positions_missing_protection'] == []
    assert positions['GIGGLEUSDT:SHORT']['protection_status'] == 'protected'
    assert positions['GIGGLEUSDT:SHORT']['status'] == 'protected_recovery_pending'
    assert positions['GIGGLEUSDT:SHORT']['protected_recovery_pending'] is True
    assert positions['GIGGLEUSDT:SHORT']['trade_management_plan'] is None
    assert positions['GIGGLEUSDT:SHORT']['recovery_reason'] == 'missing_valid_stop_distance'


def test_reconcile_runtime_state_rebuilds_plan_for_protected_recovered_short(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'GIGGLEUSDT:SHORT': {
            'symbol': 'GIGGLEUSDT',
            'side': 'SHORT',
            'status': 'protected_recovery_pending',
            'quantity': 1.28,
            'remaining_quantity': 1.28,
            'entry_price': 37.4,
            'stop_price': 38.1,
            'current_stop_price': 38.1,
            'protection_status': 'protected',
            'trade_management_plan': None,
            'recovery_incomplete': True,
            'recovery_reason': 'missing_valid_stop_distance',
        },
    })

    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'GIGGLEUSDT', 'positionAmt': '-1.28', 'positionSide': 'SHORT'}])
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *args, **kwargs: {
        'status': 'protected',
        'active_position': {'symbol': 'GIGGLEUSDT', 'positionAmt': '-1.28', 'positionSide': 'SHORT'},
        'expected_order_id': None,
        'open_orders': [{'orderId': 654, 'type': 'STOP_MARKET', 'stopPrice': '38.1'}],
    })

    result = mod.reconcile_runtime_state(
        client=object(),
        store=store,
        halt_on_orphan_position=False,
        repair_missing_protection_enabled=True,
        args=argparse.Namespace(
            tp1_r=1.5,
            tp1_close_pct=0.3,
            tp2_r=2.0,
            tp2_close_pct=0.4,
            breakeven_r=1.0,
            breakeven_confirmation_mode='ema_support',
            breakeven_min_buffer_pct=0.001,
        ),
    )

    positions = store.load_json('positions', {})
    tracked = positions['GIGGLEUSDT:SHORT']
    assert result['positions_missing_protection'] == []
    assert tracked['protection_status'] == 'protected'
    assert tracked['status'] == 'monitoring'
    assert tracked['protected_recovery_pending'] is False
    assert tracked['trade_management_plan']['side'] == 'short'
    assert tracked['trade_management_plan']['position_side'] == 'SHORT'
    assert tracked['trade_management_plan']['stop_price'] == 38.1
    assert tracked['trade_management_plan']['breakeven_confirmation_mode'] == 'ema_support'
    assert tracked['trade_management_plan']['breakeven_min_buffer_pct'] == 0.001
    assert 'recovery_incomplete' not in tracked
    assert 'recovery_reason' not in tracked


def test_reconcile_runtime_state_recovers_plan_from_protection_open_order_stop(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'ZECUSDT:SHORT': {
            'symbol': 'ZECUSDT',
            'side': 'SHORT',
            'status': 'protected_recovery_pending',
            'quantity': 0.034,
            'remaining_quantity': 0.034,
            'entry_price': 584.7605882353,
            'stop_price': 584.7605882353,
            'current_stop_price': 584.7605882353,
            'protection_status': 'protected',
            'trade_management_plan': None,
            'recovery_incomplete': True,
            'recovery_reason': 'missing_valid_stop_distance',
        },
    })

    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{
        'symbol': 'ZECUSDT',
        'positionAmt': '-0.034',
        'positionSide': 'SHORT',
        'entryPrice': '584.7605882353',
        'currentPrice': '569.01',
        'markPrice': '569.01',
        'unRealizedProfit': '0.53551999',
        'notional': '-19.34634',
        'leverage': '5',
        'isolatedMargin': '3.869268',
    }])
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *args, **kwargs: {
        'status': 'protected',
        'active_position': {'symbol': 'ZECUSDT', 'positionAmt': '-0.034', 'positionSide': 'SHORT'},
        'expected_order_id': None,
        'open_orders': [{'orderId': 654, 'type': 'STOP_MARKET', 'stopPrice': '589.8'}],
    })

    result = mod.reconcile_runtime_state(
        client=object(),
        store=store,
        halt_on_orphan_position=False,
        repair_missing_protection_enabled=True,
        args=argparse.Namespace(
            tp1_r=1.5,
            tp1_close_pct=0.3,
            tp2_r=2.0,
            tp2_close_pct=0.4,
            breakeven_r=1.0,
            breakeven_confirmation_mode='ema_support',
            breakeven_min_buffer_pct=0.001,
        ),
    )

    positions = store.load_json('positions', {})
    tracked = positions['ZECUSDT:SHORT']
    assert result['positions_missing_protection'] == []
    assert tracked['protection_status'] == 'protected'
    assert tracked['status'] == 'monitoring'
    assert tracked['protected_recovery_pending'] is False
    assert tracked['current_stop_price'] == 589.8
    assert tracked['stop_price'] == 589.8
    assert tracked['trade_management_plan']['stop_price'] == 589.8
    assert tracked['trade_management_plan']['initial_risk_per_unit'] > 0
    assert 'recovery_incomplete' not in tracked
    assert 'recovery_reason' not in tracked


def test_resolve_position_protection_status_requires_matching_algo_identity(monkeypatch):
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'DOGEUSDT', 'positionAmt': '5', 'positionSide': 'LONG'}])
    monkeypatch.setattr(mod, 'fetch_open_orders', lambda client, symbol: [])
    monkeypatch.setattr(mod, 'fetch_open_algo_orders', lambda client, symbol: [
        {
            'clientAlgoId': 'other-stop',
            'orderType': 'STOP_MARKET',
            'triggerPrice': '0.1210',
            'quantity': '5',
            'positionSide': 'LONG',
            'side': 'SELL',
            'symbol': symbol,
        }
    ])

    result = mod.resolve_position_protection_status(
        client=object(),
        symbol='DOGEUSDT',
        expected_stop_order={
            'clientAlgoId': 'expected-stop',
            'triggerPrice': '0.1234',
            'quantity': '5',
            'positionSide': 'LONG',
            'side': 'SELL',
        },
        side='LONG',
    )

    assert result['status'] == 'missing'
    assert result['matched_via'] == 'unmatched'
    assert result['expected_client_algo_id'] == 'expected-stop'


def test_resolve_position_protection_status_rejects_algo_stop_with_wrong_trigger_price(monkeypatch):
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'DOGEUSDT', 'positionAmt': '5', 'positionSide': 'LONG'}])
    monkeypatch.setattr(mod, 'fetch_open_orders', lambda client, symbol: [])
    monkeypatch.setattr(mod, 'fetch_open_algo_orders', lambda client, symbol: [
        {
            'clientAlgoId': 'expected-stop',
            'orderType': 'STOP_MARKET',
            'triggerPrice': '0.1210',
            'quantity': '5',
            'positionSide': 'LONG',
            'side': 'SELL',
            'symbol': symbol,
        }
    ])

    result = mod.resolve_position_protection_status(
        client=object(),
        symbol='DOGEUSDT',
        expected_stop_order={
            'clientAlgoId': 'expected-stop',
            'triggerPrice': '0.1234',
            'quantity': '5',
            'positionSide': 'LONG',
            'side': 'SELL',
        },
        side='LONG',
    )

    assert result['status'] == 'missing'
    assert result['matched_via'] == 'unmatched'


def test_resolve_position_protection_status_accepts_exact_algo_stop_match(monkeypatch):
    monkeypatch.setattr(mod, 'fetch_open_positions', lambda client: [{'symbol': 'DOGEUSDT', 'positionAmt': '5', 'positionSide': 'LONG'}])
    monkeypatch.setattr(mod, 'fetch_open_orders', lambda client, symbol: [])
    monkeypatch.setattr(mod, 'fetch_open_algo_orders', lambda client, symbol: [
        {
            'clientAlgoId': 'expected-stop',
            'orderType': 'STOP_MARKET',
            'triggerPrice': '0.1234',
            'quantity': '5',
            'positionSide': 'LONG',
            'side': 'SELL',
            'symbol': symbol,
        }
    ])

    result = mod.resolve_position_protection_status(
        client=object(),
        symbol='DOGEUSDT',
        expected_stop_order={
            'clientAlgoId': 'expected-stop',
            'triggerPrice': '0.1234',
            'quantity': '5',
            'positionSide': 'LONG',
            'side': 'SELL',
        },
        side='LONG',
    )

    assert result['status'] == 'protected'
    assert result['matched_via'] == 'open_algo_orders'
    assert result['matched_trigger_price'] == 0.1234
    assert result['matched_quantity'] == 5.0


def test_monitor_live_trade_prefers_current_stop_price_on_restart(tmp_path, monkeypatch):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'DOGEUSDT:LONG': {
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_key': 'DOGEUSDT:LONG',
            'status': 'monitoring',
            'quantity': 5.0,
            'remaining_quantity': 5.0,
            'entry_price': 100.0,
            'stop_price': 95.0,
            'current_stop_price': 101.0,
            'trade_management_plan': {
                'entry_price': 100.0,
                'stop_price': 95.0,
                'quantity': 5.0,
                'initial_risk_per_unit': 5.0,
                'breakeven_trigger_price': 105.0,
                'tp1_trigger_price': 107.5,
                'tp1_close_qty': 2.0,
                'tp2_trigger_price': 110.0,
                'tp2_close_qty': 1.5,
                'runner_qty': 1.5,
                'side': 'long',
                'position_side': 'LONG',
            },
        },
    })

    monkeypatch.setattr(mod, 'fetch_klines', lambda *args, **kwargs: [[0, 0, 103.0, 99.0, 102.0]] * 21)
    captured = {}

    def fake_evaluate_management_actions(state, *args, **kwargs):
        captured['current_stop_price'] = state.current_stop_price
        captured['stop_price'] = state.stop_price if hasattr(state, 'stop_price') else None
        return []

    monkeypatch.setattr(mod, 'evaluate_management_actions', fake_evaluate_management_actions)
    monkeypatch.setattr(mod.time, 'sleep', lambda *args, **kwargs: None)

    trade = {
        'entry_price': 100.0,
        'quantity': 5.0,
        'stop_order': {'orderId': 123},
        'protection_check': {'status': 'protected'},
        'trade_management_plan': {
            'entry_price': 100.0,
            'stop_price': 95.0,
            'quantity': 5.0,
            'initial_risk_per_unit': 5.0,
            'breakeven_trigger_price': 105.0,
            'tp1_trigger_price': 107.5,
            'tp1_close_qty': 2.0,
            'tp2_trigger_price': 110.0,
            'tp2_close_qty': 1.5,
            'runner_qty': 1.5,
            'side': 'long',
            'position_side': 'LONG',
        },
        'side': 'LONG',
    }
    args = argparse.Namespace(
        profile='10u-active',
        max_monitor_cycles=1,
        monitor_poll_interval_sec=0,
        trailing_buffer_pct=0.02,
        trailing_trigger_r=2.0,
        notification_cooldown_sec=0.0,
        disable_notify=True,
        notify_target='',
    )

    result = mod.monitor_live_trade(client=object(), symbol='DOGEUSDT', meta=make_meta(), args=args, trade=trade, store=store)
    state = store.load_json('monitor_debug', {})

    assert result['status'] == 'monitoring'
    assert result['protection_status'] == 'protected'
    assert result['stop_order_id'] == 123
    assert result['remaining_quantity'] == 5.0
    assert captured['current_stop_price'] == 101.0
    assert state['current_price'] == 102.0


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


def test_sync_tracked_positions_with_exchange_persists_realtime_exchange_fields(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('positions', {
        'APEUSDT:LONG': {
            'symbol': 'APEUSDT',
            'side': 'LONG',
            'status': 'monitoring',
            'quantity': 100.0,
            'remaining_quantity': 100.0,
            'entry_price': 0.1386,
            'leverage': 5,
            'protection_status': 'protected',
        },
    })

    mod.sync_tracked_positions_with_exchange(
        store,
        exchange_positions=[{
            'symbol': 'APEUSDT',
            'positionSide': 'LONG',
            'positionAmt': '1000',
            'entryPrice': '0.138600',
            'markPrice': '0.190300',
            'notional': '190.300000',
            'unRealizedProfit': '51.700000',
            'isolatedMargin': '38.060000',
            'leverage': '5',
        }],
        protected_symbols=['APEUSDT:LONG'],
    )

    positions = store.load_json('positions', {})
    tracked = positions['APEUSDT:LONG']
    assert tracked['quantity'] == 1000.0
    assert tracked['remaining_quantity'] == 1000.0
    assert tracked['entry_price'] == 0.1386
    assert tracked['mark_price'] == 0.1903
    assert tracked['current_price'] == 0.1903
    assert tracked['position_notional'] == 190.3
    assert tracked['unrealized_pnl_usdt'] == 51.7
    assert tracked['position_margin_usdt'] == 38.06
    assert tracked['unrealized_pnl_pct'] == pytest.approx(135.8381503)
    assert tracked['leverage'] == 5


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
        tp1_profit_usdt=5.0,
        tp2_profit_usdt=10.0,
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
            if path == '/fapi/v1/marginType':
                return {'code': 200, 'msg': 'success'}
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
    monkeypatch.setattr(mod, 'place_take_profit_market_order', lambda *a, **k: {'orderId': 1001, 'clientOrderId': 'tp-1'})
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


def test_place_live_trade_keeps_10u_aggressive_probe_entry_at_profile_leverage(monkeypatch):
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
        recommended_leverage=5,
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
        probe_entry=True,
    )
    args = argparse.Namespace(
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        tp1_profit_usdt=5.0,
        tp2_profit_usdt=10.0,
        breakeven_r=1.0,
        profile='10u-aggressive',
        leverage=5,
    )
    args = mod.apply_runtime_profile(args)
    meta = make_meta()

    class FakeClient:
        def __init__(self):
            self.calls = []

        def signed_post(self, path, params):
            self.calls.append((path, dict(params)))
            if path == '/fapi/v1/marginType':
                return {'code': 200, 'msg': 'success'}
            if path == '/fapi/v1/leverage':
                return {'leverage': params['leverage']}
            if path == '/fapi/v1/order':
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

    monkeypatch.setattr(mod, 'log_runtime_event', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'emit_notification', lambda *a, **k: None)
    monkeypatch.setattr(mod, 'place_stop_market_order', lambda *a, **k: {'orderId': 999, 'clientOrderId': 'stop-1'})
    monkeypatch.setattr(mod, 'place_take_profit_market_order', lambda *a, **k: {'orderId': 1001, 'clientOrderId': 'tp-1'})
    monkeypatch.setattr(mod, 'resolve_position_protection_status', lambda *a, **k: {'status': 'protected', 'expected_order_id': 999})

    result = mod.place_live_trade(client, candidate, leverage=5, meta=meta, args=args)

    leverage_calls = [params['leverage'] for path, params in client.calls if path == '/fapi/v1/leverage']
    assert result['filled_quantity'] == 1.25
    assert leverage_calls == [5]


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
            if path == '/fapi/v1/marginType':
                return {'code': 200, 'msg': 'success'}
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
            if path == '/fapi/v1/marginType':
                return {'code': 200, 'msg': 'success'}
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
            if path == '/fapi/v1/marginType':
                return {'code': 200, 'msg': 'success'}
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


def test_persist_live_open_position_keeps_nonzero_stop_distance_for_restarts(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    candidate = SimpleNamespace(
        symbol='GIGGLEUSDT',
        side='SHORT',
        stop_price=38.1,
        quantity=1.28,
        score=118.6,
        state='watch',
        alert_tier='critical',
        candidate_stage='confirmed',
        market_regime_label='trend',
        regime_multiplier=1.0,
        setup_ready=True,
        trigger_fired=True,
        portfolio_narrative_bucket='',
        portfolio_correlation_group='',
    )
    live_execution = {
        'symbol': 'GIGGLEUSDT',
        'side': 'SHORT',
        'entry_price': 37.4,
        'filled_quantity': 1.28,
        'entry_order_feedback': {'order_id': 123, 'client_order_id': 'abc', 'status': 'FILLED', 'cum_quote': '47.872', 'update_time': 1},
        'stop_order': {'orderId': 456},
        'protection_check': {'status': 'protected'},
        'trade_management_plan': {
            'entry_price': 37.4,
            'stop_price': 38.1,
            'quantity': 1.28,
            'initial_risk_per_unit': 0.7,
            'breakeven_trigger_price': 36.7,
            'tp1_trigger_price': 36.35,
            'tp1_close_qty': 0.384,
            'tp2_trigger_price': 36.0,
            'tp2_close_qty': 0.512,
            'runner_qty': 0.384,
            'side': 'short',
            'position_side': 'SHORT',
        },
        'margin_type': 'isolated',
        'margin_type_check': {},
        'leverage': 5,
        'leverage_check': {},
    }

    positions_state, position_key = mod.persist_live_open_position(store, candidate, live_execution)
    tracked = positions_state[position_key]

    assert tracked['position_key'] == 'GIGGLEUSDT:SHORT'
    assert tracked['stop_price'] == 38.1
    assert tracked['current_stop_price'] == 38.1
    assert tracked['trade_management_plan']['stop_price'] == 38.1

    reloaded = store.load_json('positions', {})['GIGGLEUSDT:SHORT']
    assert reloaded['stop_price'] == 38.1
    assert reloaded['current_stop_price'] == 38.1
    assert reloaded['trade_management_plan']['stop_price'] == 38.1


def test_runtime_store_load_json_marks_flat_stop_as_recovery_incomplete_after_restart(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    raw_path = tmp_path / 'positions.json'
    raw_path.write_text(mod.json.dumps({
        'GIGGLEUSDT:SHORT': {
            'symbol': 'GIGGLEUSDT',
            'side': 'SHORT',
            'status': 'monitoring',
            'quantity': 1.28,
            'remaining_quantity': 1.28,
            'entry_price': 37.4,
            'stop_price': 37.4,
            'current_stop_price': 37.4,
            'protection_status': 'protected',
            'trade_management_plan': {
                'entry_price': 37.4,
                'stop_price': 37.4,
                'quantity': 1.28,
                'initial_risk_per_unit': 0.0,
                'breakeven_trigger_price': 37.4,
                'tp1_trigger_price': 37.4,
                'tp1_close_qty': 0.384,
                'tp2_trigger_price': 37.4,
                'tp2_close_qty': 0.512,
                'runner_qty': 0.384,
                'side': 'short',
                'position_side': 'SHORT',
            },
        },
    }, ensure_ascii=False, indent=2), encoding='utf-8')

    tracked = store.load_json('positions', {})['GIGGLEUSDT:SHORT']

    assert tracked['recovery_incomplete'] is True
    assert tracked['recovery_reason'] == 'missing_valid_stop_distance'
    assert tracked['trade_management_plan'] is None
    assert tracked['stop_price'] == 37.4
    assert tracked['current_stop_price'] == 37.4


def test_runtime_store_load_json_repairs_corrupted_short_plan_side(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    raw_path = tmp_path / 'positions.json'
    raw_path.write_text(mod.json.dumps({
        'GIGGLEUSDT:SHORT': {
            'symbol': 'GIGGLEUSDT',
            'side': 'SHORT',
            'status': 'monitoring',
            'quantity': 1.28,
            'remaining_quantity': 1.28,
            'trade_management_plan': {
                'entry_price': 37.4,
                'stop_price': 38.6,
                'quantity': 1.28,
                'initial_risk_per_unit': 1.2,
                'breakeven_trigger_price': 36.2,
                'tp1_trigger_price': 36.0,
                'tp1_close_qty': 0.4,
                'tp2_trigger_price': 35.5,
                'tp2_close_qty': 0.5,
                'runner_qty': 0.38,
                'side': 'LONG',
                'position_side': 'LONG',
            },
        },
    }, ensure_ascii=False, indent=2), encoding='utf-8')

    positions = store.load_json('positions', {})

    assert sorted(positions.keys()) == ['GIGGLEUSDT:SHORT']
    tracked = positions['GIGGLEUSDT:SHORT']
    assert tracked['side'] == 'short'
    assert tracked['position_side'] == 'SHORT'
    assert tracked['position_key'] == 'GIGGLEUSDT:SHORT'
    assert tracked['trade_management_plan']['side'] == 'short'
    assert tracked['trade_management_plan']['position_side'] == 'SHORT'

    persisted = mod.save_positions_state(store, positions)
    assert sorted(persisted.keys()) == ['GIGGLEUSDT:SHORT']
    assert persisted['GIGGLEUSDT:SHORT']['trade_management_plan']['side'] == 'short'
    assert persisted['GIGGLEUSDT:SHORT']['trade_management_plan']['position_side'] == 'SHORT'


def test_build_trade_management_plan_from_position_preserves_short_side_from_position_side():
    args = argparse.Namespace(
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        tp1_profit_usdt=5.0,
        tp2_profit_usdt=10.0,
        breakeven_r=1.0,
    )

    plan = mod.build_trade_management_plan_from_position({
        'symbol': 'GIGGLEUSDT',
        'side': 'long',
        'position_side': 'SHORT',
        'entry_price': 37.4,
        'stop_price': 38.6,
        'quantity': 1.28,
        'trade_management_plan': {
            'entry_price': 37.4,
            'stop_price': 38.6,
            'quantity': 1.28,
            'initial_risk_per_unit': 1.2,
            'breakeven_trigger_price': 36.2,
            'tp1_trigger_price': 36.0,
            'tp1_close_qty': 0.4,
            'tp1_profit_usdt': 5.0,
            'tp2_trigger_price': 35.5,
            'tp2_close_qty': 0.5,
            'tp2_profit_usdt': 10.0,
            'runner_qty': 0.38,
            'side': 'long',
            'position_side': 'LONG',
        },
    }, args)

    assert plan.side == mod.TRADE_SIDE_SHORT
    assert plan.position_side == mod.POSITION_SIDE_SHORT
    assert plan.tp1_profit_usdt == 5.0
    assert plan.tp2_profit_usdt == 10.0


def test_build_trade_management_plan_from_position_derives_short_side_without_position_side():
    args = argparse.Namespace(
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        breakeven_r=1.0,
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.001,
    )

    plan = mod.build_trade_management_plan_from_position({
        'symbol': 'GIGGLEUSDT',
        'side': 'short',
        'entry_price': 37.4,
        'stop_price': 38.6,
        'quantity': 1.28,
        'trade_management_plan': 'corrupted',
    }, args)

    assert plan.side == mod.TRADE_SIDE_SHORT
    assert plan.position_side == mod.POSITION_SIDE_SHORT


def test_build_trade_management_plan_from_position_raises_for_zero_stop_distance():
    args = argparse.Namespace(
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        breakeven_r=1.0,
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.001,
    )

    try:
        mod.build_trade_management_plan_from_position({
            'symbol': 'ZECUSDT',
            'position_side': 'LONG',
            'entry_price': 565.1,
            'stop_price': 565.1,
            'current_stop_price': 565.1,
            'quantity': 0.354,
            'trade_management_plan': 'corrupted',
        }, args)
    except ValueError as exc:
        assert 'missing valid stop distance' in str(exc)
        assert 'ZECUSDT:LONG' in str(exc)
    else:
        raise AssertionError('expected ValueError for zero stop distance')


def test_build_trade_management_plan_from_position_falls_back_when_persisted_plan_is_non_mapping():
    args = argparse.Namespace(
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        breakeven_r=1.0,
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.001,
    )

    plan = mod.build_trade_management_plan_from_position({
        'symbol': 'GIGGLEUSDT',
        'position_side': 'SHORT',
        'entry_price': 37.4,
        'stop_price': 38.6,
        'quantity': 1.28,
        'trade_management_plan': 'corrupted',
    }, args)

    assert plan.side == mod.TRADE_SIDE_SHORT
    assert plan.position_side == mod.POSITION_SIDE_SHORT
    assert plan.stop_price == 38.6
    assert plan.breakeven_confirmation_mode == 'ema_support'
    assert plan.breakeven_min_buffer_pct == 0.001


def test_build_trade_management_plan_from_position_uses_profit_targets_from_args_when_rebuilding():
    args = argparse.Namespace(
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        tp1_profit_usdt=5.0,
        tp2_profit_usdt=10.0,
        breakeven_r=1.0,
        breakeven_confirmation_mode='ema_support',
        breakeven_min_buffer_pct=0.001,
    )

    plan = mod.build_trade_management_plan_from_position({
        'symbol': 'GIGGLEUSDT',
        'position_side': 'SHORT',
        'entry_price': 37.4,
        'stop_price': 38.6,
        'quantity': 1.28,
        'trade_management_plan': None,
    }, args)

    assert plan.tp1_profit_usdt == 5.0
    assert plan.tp2_profit_usdt == 10.0
    assert plan.tp1_close_qty == pytest.approx(0.384)
    assert plan.tp2_close_qty == pytest.approx(0.512)


def test_build_parser_accepts_absolute_profit_targets():
    parser = mod.build_parser()

    args = parser.parse_args(['--tp1-profit-usdt', '5', '--tp2-profit-usdt', '10'])

    assert args.tp1_profit_usdt == 5.0
    assert args.tp2_profit_usdt == 10.0


def test_restore_position_lifecycle_fields_marks_zero_risk_plan_as_recovery_incomplete():
    restored = mod.restore_position_lifecycle_fields({
        'symbol': 'ZECUSDT',
        'side': 'long',
        'position_side': 'LONG',
        'entry_price': 565.1,
        'quantity': 0.354,
        'current_stop_price': 565.1,
        'stop_price': 565.1,
        'status': 'monitoring',
        'protection_status': 'protected',
        'trade_management_plan': {
            'entry_price': 565.1,
            'stop_price': 565.1,
            'quantity': 0.354,
            'initial_risk_per_unit': 0.0,
            'breakeven_trigger_price': 565.1,
            'tp1_trigger_price': 565.1,
            'tp1_close_qty': 0.1062,
            'tp2_trigger_price': 565.1,
            'tp2_close_qty': 0.1416,
            'runner_qty': 0.1062,
            'side': 'long',
            'position_side': 'LONG',
        },
    })

    assert restored['recovery_incomplete'] is True
    assert restored['recovery_reason'] == 'missing_valid_stop_distance'
    assert restored['trade_management_plan'] is None
    assert restored['status'] == 'protected_recovery_pending'


def test_restore_position_lifecycle_fields_normalizes_valid_plan_side():
    restored = mod.restore_position_lifecycle_fields({
        'symbol': 'GIGGLEUSDT',
        'side': 'short',
        'position_side': 'SHORT',
        'entry_price': 37.4,
        'quantity': 1.28,
        'current_stop_price': 38.1,
        'stop_price': 38.1,
        'trade_management_plan': {
            'entry_price': 37.4,
            'stop_price': 38.1,
            'quantity': 1.28,
            'initial_risk_per_unit': 0.7,
            'breakeven_trigger_price': 36.7,
            'tp1_trigger_price': 36.35,
            'tp1_close_qty': 0.384,
            'tp2_trigger_price': 36.0,
            'tp2_close_qty': 0.512,
            'runner_qty': 0.384,
            'side': 'short',
            'position_side': 'SHORT',
        },
    })

    assert restored['trade_management_plan']['side'] == 'short'
    assert restored['trade_management_plan']['position_side'] == 'SHORT'
    assert 'recovery_incomplete' not in restored


def test_runtime_store_save_positions_state_canonicalizes_before_return(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))

    persisted = mod.save_positions_state(store, {
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
        },
    })

    assert sorted(persisted.keys()) == ['BTCUSDT:LONG', 'ETHUSDT:SHORT']
    assert persisted['BTCUSDT:LONG']['symbol'] == 'BTCUSDT'
    assert persisted['BTCUSDT:LONG']['position_key'] == 'BTCUSDT:LONG'
    assert persisted['ETHUSDT:SHORT']['position_key'] == 'ETHUSDT:SHORT'



def test_runtime_store_load_json_exposes_canonical_positions_only(tmp_path):
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

    assert sorted(positions.keys()) == ['BTCUSDT:LONG', 'ETHUSDT:SHORT']
    assert positions['BTCUSDT:LONG']['symbol'] == 'BTCUSDT'
    assert positions['BTCUSDT:LONG']['side'] == 'long'
    assert positions['BTCUSDT:LONG']['position_side'] == 'LONG'
    assert positions['BTCUSDT:LONG']['position_key'] == 'BTCUSDT:LONG'
    assert positions['BTCUSDT:LONG']['remaining_quantity'] == 1.25
    assert positions['BTCUSDT:LONG']['current_stop_price'] is None
    assert positions['BTCUSDT:LONG']['lowest_price_seen'] is None
    assert positions['ETHUSDT:SHORT']['symbol'] == 'ETHUSDT'
    assert positions['ETHUSDT:SHORT']['side'] == 'short'
    assert positions['ETHUSDT:SHORT']['position_side'] == 'SHORT'
    assert positions['ETHUSDT:SHORT']['position_key'] == 'ETHUSDT:SHORT'
    assert positions['ETHUSDT:SHORT']['lowest_price_seen'] == 1725.0


def test_runtime_store_load_json_rewrites_duplicate_legacy_keys(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    raw_path = tmp_path / 'positions.json'
    raw_path.write_text(mod.json.dumps({
        'BTCUSDT': {
            'symbol': 'btcusdt',
            'status': 'monitoring',
            'quantity': 1.25,
        },
        'BTCUSDT:LONG': {
            'symbol': 'BTCUSDT',
            'side': 'LONG',
            'status': 'monitoring',
            'quantity': 1.25,
            'remaining_quantity': 0.9,
        },
    }, ensure_ascii=False, indent=2), encoding='utf-8')

    positions = store.load_json('positions', {})

    assert sorted(positions.keys()) == ['BTCUSDT:LONG']
    tracked = positions['BTCUSDT:LONG']
    assert tracked['symbol'] == 'BTCUSDT'
    assert tracked['side'] == 'long'
    assert tracked['position_side'] == 'LONG'
    assert tracked['remaining_quantity'] == 0.9

    persisted = mod.save_positions_state(store, positions)
    assert sorted(persisted.keys()) == ['BTCUSDT:LONG']
    assert persisted['BTCUSDT:LONG']['position_key'] == 'BTCUSDT:LONG'


def test_runtime_store_load_json_positions_does_not_rewrite_canonicalized_payload_on_read(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    raw_path = tmp_path / 'positions.json'
    raw_path.write_text(mod.json.dumps({
        'BTCUSDT': {
            'symbol': 'btcusdt',
            'status': 'monitoring',
            'quantity': 1.25,
        },
        'BTCUSDT:LONG': {
            'symbol': 'BTCUSDT',
            'side': 'LONG',
            'status': 'monitoring',
            'quantity': 1.25,
            'remaining_quantity': 0.9,
        },
    }, ensure_ascii=False, indent=2), encoding='utf-8')
    before = raw_path.read_text(encoding='utf-8')

    positions = store.load_json('positions', {})

    assert sorted(positions.keys()) == ['BTCUSDT:LONG']
    assert positions['BTCUSDT:LONG']['remaining_quantity'] == 0.9
    assert raw_path.read_text(encoding='utf-8') == before


def test_runtime_store_save_json_writes_via_atomic_replace(tmp_path, monkeypatch):
    store = mod.RuntimeStateStore(str(tmp_path))
    target_path = tmp_path / 'positions.json'
    replace_calls = []

    original_replace = mod.os.replace

    def spy_replace(src, dst):
        replace_calls.append((mod.Path(src).name, mod.Path(dst).name))
        return original_replace(src, dst)

    monkeypatch.setattr(mod.os, 'replace', spy_replace)

    persisted = store.save_json('positions', {
        'BTCUSDT': {
            'symbol': 'btcusdt',
            'status': 'monitoring',
            'quantity': 1.25,
        },
    })

    assert persisted['BTCUSDT:LONG']['position_key'] == 'BTCUSDT:LONG'
    positions_replace_calls = [call for call in replace_calls if call[1] == 'positions.json']
    assert len(positions_replace_calls) == 1
    assert positions_replace_calls[0][0].startswith('.positions.json.')
    assert target_path.exists()
    assert not (tmp_path / positions_replace_calls[0][0]).exists()


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


def test_runtime_store_append_event_fsyncs_and_terminates_each_jsonl_row(tmp_path, monkeypatch):
    store = mod.RuntimeStateStore(str(tmp_path))
    flush_calls = []
    fsync_calls = []
    event_fds = []
    original_open = mod.Path.open

    class RecordingHandle:
        def __init__(self, fh):
            self._fh = fh

        def write(self, data):
            return self._fh.write(data)

        def flush(self):
            flush_calls.append(True)
            return self._fh.flush()

        def fileno(self):
            fd = self._fh.fileno()
            event_fds.append(fd)
            return fd

        def __enter__(self):
            self._fh.__enter__()
            return self

        def __exit__(self, exc_type, exc, tb):
            return self._fh.__exit__(exc_type, exc, tb)

    def recording_open(self, *args, **kwargs):
        fh = original_open(self, *args, **kwargs)
        if self == tmp_path / 'events.jsonl' and args and args[0] == 'a':
            return RecordingHandle(fh)
        return fh

    monkeypatch.setattr(mod.Path, 'open', recording_open)
    monkeypatch.setattr(mod.os, 'fsync', lambda fd: fsync_calls.append(fd))

    row = store.append_event('entry_filled', {'symbol': 'btcusdt'})

    assert row['event_type'] == 'entry_filled'
    assert len(flush_calls) == 1
    assert any(fd in event_fds for fd in fsync_calls)
    raw_text = (tmp_path / 'events.jsonl').read_text(encoding='utf-8')
    assert raw_text.endswith('\n')
    parsed = [mod.json.loads(line) for line in raw_text.splitlines() if line.strip()]
    assert parsed[-1]['event_type'] == 'entry_filled'


def test_runtime_store_read_events_skips_malformed_trailing_jsonl_row(tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    events_path = tmp_path / 'events.jsonl'
    valid_first = {'event_type': 'candidate_selected', 'symbol': 'BTCUSDT'}
    valid_second = {'event_type': 'entry_filled', 'symbol': 'ETHUSDT'}
    events_path.write_text(
        mod.json.dumps(valid_first, ensure_ascii=False) + '\n' +
        mod.json.dumps(valid_second, ensure_ascii=False) + '\n' +
        '{"event_type": "truncated"',
        encoding='utf-8',
    )

    rows = store.read_events(limit=10)

    assert rows == [valid_first, valid_second]


def test_runtime_store_module_matches_script_materialize_positions_state_behavior():
    raw_positions = {
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
    }

    script_materialized = mod.materialize_positions_state(raw_positions)
    extracted_materialized = runtime_store.materialize_positions_state(raw_positions)

    assert extracted_materialized == script_materialized


def test_runtime_store_module_matches_script_event_normalization_behavior():
    payload = {
        'symbol': 'btcusdt',
        'quantity': 1.0,
    }

    script_row = mod.normalize_runtime_event_payload(payload)
    extracted_row = runtime_store.normalize_runtime_event_payload(payload)

    assert extracted_row == script_row


def test_monitor_live_trade_persists_canonical_positions_without_legacy_alias(tmp_path, monkeypatch):
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
        'stop_order': {'orderId': 12345},
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
            'side': 'long',
            'position_side': 'LONG',
        },
        'quantity': 1.0,
        'side': 'LONG',
    }
    meta = mod.SymbolMeta(
        symbol=symbol,
        price_precision=4,
        quantity_precision=3,
        tick_size=0.1,
        step_size=0.001,
        min_qty=0.001,
        quote_asset='USDT',
        status='TRADING',
        contract_type='PERPETUAL',
    )

    monkeypatch.setattr(mod, 'fetch_klines', lambda *args, **kwargs: [[0, 0, 0, 0, 100.0]] * 21)
    monkeypatch.setattr(mod, 'extract_closes', lambda klines: [100.0] * len(klines))
    monkeypatch.setattr(mod, 'extract_highs', lambda klines: [100.0] * len(klines))
    monkeypatch.setattr(mod, 'extract_lows', lambda klines: [100.0] * len(klines))
    monkeypatch.setattr(mod, 'evaluate_management_actions', lambda *args, **kwargs: [])
    monkeypatch.setattr(mod.time, 'sleep', lambda *_args, **_kwargs: None)

    result = mod.monitor_live_trade(client=object(), symbol=symbol, meta=meta, args=args, trade=trade, store=store)

    persisted = mod.json.loads((tmp_path / 'positions.json').read_text(encoding='utf-8'))
    assert persisted == {}
    reloaded = mod.RuntimeStateStore(str(tmp_path)).load_json('positions', {})
    assert reloaded == {}
    assert result['ok'] is True


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
    store.load_json('positions', {})
    debug_state = store.load_json('monitor_debug', {})

    assert result['status'] == 'closed'
    assert result['exit_reason'] == 'tp1'
    assert rows[-1]['event_type'] == 'trade_invalidated'
    assert rows[-1]['side'] == 'SHORT'
    assert rows[-1]['position_key'] == position_key
    assert debug_state['actions'][0]['type'] == 'take_profit_1'
    assert debug_state['current_price'] == 89.0
    assert debug_state['current_price_source'] == 'kline_close_fallback'



def test_scan_only_cycle_converts_binance_ip_ban_to_blocked_manager_update(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    args = argparse.Namespace(
        auto_loop=True,
        reconcile_only=False,
        live=True,
        scan_only=False,
        scanner_timeout_seconds=180.0,
        runtime_ttl_seconds=900.0,
    )

    monkeypatch.setattr(mod, 'get_runtime_state_store', lambda _args: store)
    monkeypatch.setattr(mod, 'is_binance_simulated_trading', lambda _args: False)
    monkeypatch.setattr(mod, 'reconcile_runtime_state', lambda *a, **k: {'ok': True, 'orphan_positions': [], 'positions_missing_protection': []})
    monkeypatch.setattr(
        mod,
        'run_with_deadman_timeout',
        lambda *a, **k: (_ for _ in ()).throw(mod.BinanceAPIError("Binance API error 418: {'code': -1003, 'msg': 'Way too much request weight used; IP banned until 1234567890'}")),
    )

    result = mod.scan_only_cycle(client=object(), args=args, store=store, cycle_no=7)

    assert result['ok'] is True
    assert result['cycle']['blocked_reason'] == 'binance_ip_ban'
    assert result['manager_update']['blocked_reason'] == 'binance_ip_ban'
    heartbeat = store.load_json('runtime_heartbeat', {})
    assert heartbeat['components']['scanner']['status'] == 'blocked'
    assert heartbeat['components']['scanner']['blocked_reason'] == 'binance_ip_ban'


def test_book_ticker_websocket_supervisor_target_records_error_health_when_startup_crashes(monkeypatch, tmp_path):
    store = mod.RuntimeStateStore(str(tmp_path))
    events = []

    with mod._BOOK_TICKER_WS_SUPERVISOR_LOCK:
        mod._BOOK_TICKER_WS_SUPERVISOR_STATE.clear()
        mod._BOOK_TICKER_WS_SUPERVISOR_STATE.update({
            'generation_id': 7,
            'symbols': ['BTCUSDT', 'ETHUSDT'],
        })

    def fake_summary(**kwargs):
        raise RuntimeError('ws dns unavailable')

    monkeypatch.setattr(mod, 'build_auto_loop_book_ticker_supervisor_summary', fake_summary)
    monkeypatch.setattr(
        mod,
        'append_runtime_event',
        lambda store_arg, event_type, payload: events.append((store_arg, event_type, payload)),
    )

    mod._book_ticker_websocket_supervisor_target(
        store=store,
        symbol_provider=lambda: ['BTCUSDT'],
        ws_module=object(),
        generation_id=7,
    )

    health = store.load_json('book_ticker_ws_status', {})
    assert health['status'] == 'error'
    assert health['symbols'] == ['BTCUSDT', 'ETHUSDT']
    assert health['reconnect_count'] == 0
    assert health['subscription_version'] == 0
    assert health['last_error'] == 'ws dns unavailable'
    assert events == [(
        store,
        'book_ticker_ws_supervisor_crashed',
        {'event_source': 'book_ticker_websocket', 'error': 'ws dns unavailable'},
    )]



def _ticker_args(**overrides):
    data = {
        'scanner_rest_fallback': True,
        'scanner_rest_fallback_min_interval_seconds': 180.0,
        'scanner_rest_fallback_max_used_weight_1m': 900,
        'ticker_24hr_cache_max_age_seconds': 300.0,
        'scanner_order_book_cache_max_age_seconds': 30.0,
        'runtime_state_dir': 'unit-test-runtime-state',
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def test_ticker_24hr_cache_fresh_scanner_skips_rest_fallback(tmp_path, monkeypatch):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('ticker_24hr_cache', {
        'updated_at_ms': int(mod.time.time() * 1000),
        'source': 'unit_test',
        'row_count': 1,
        'rows_by_symbol': {'BTCUSDT': {'symbol': 'BTCUSDT', 'priceChangePercent': '1', 'quoteVolume': '1000000'}},
        'rest_used_weight_1m': 100,
    })
    calls = {'fetch': 0}
    monkeypatch.setattr(mod, 'fetch_tickers', lambda client: calls.__setitem__('fetch', calls['fetch'] + 1) or [])
    monkeypatch.setattr(mod, '_runtime_store_rest_guard_snapshot', lambda _store: {'state': 'CLOSED', 'rest_used_weight_1m': 100})

    rows, diag = mod.resolve_scan_tickers(object(), store, _ticker_args(), fallback_symbols=['BTCUSDT'], return_diagnostics=True)

    assert calls['fetch'] == 0
    assert rows[0]['symbol'] == 'BTCUSDT'
    assert diag['ticker_24hr_cache_available'] is True
    assert diag['scanner_rest_fallback_used'] is False
    assert 'ticker_24hr_cache_row_count' in diag


def test_ticker_24hr_cache_missing_high_weight_skips_rest_fallback(tmp_path, monkeypatch):
    store = mod.RuntimeStateStore(str(tmp_path))
    calls = {'fetch': 0}
    monkeypatch.setattr(mod, 'fetch_tickers', lambda client: calls.__setitem__('fetch', calls['fetch'] + 1) or [])
    monkeypatch.setattr(mod, '_runtime_store_rest_guard_snapshot', lambda _store: {'state': 'CLOSED', 'rest_used_weight_1m': 901})

    rows, diag = mod.resolve_scan_tickers(object(), store, _ticker_args(), fallback_symbols=['BTCUSDT'], return_diagnostics=True)

    assert calls['fetch'] == 0
    assert rows == []
    assert diag['scanner_rest_fallback_skipped_reason'] == 'rest_used_weight_1m_exceeds_limit'
    assert diag['rest_used_weight_1m'] == 901


def test_ticker_24hr_cache_missing_degraded_rows_from_book_ticker_cache(tmp_path, monkeypatch):
    store = mod.RuntimeStateStore(str(tmp_path))
    store.save_json('book_ticker_cache', {
        'BTCUSDT': {
            'updated_at': mod._isoformat_utc(mod._utc_now()),
            'samples': [{'bidPrice': '100.0', 'askPrice': '101.0', 'bidQty': '1', 'askQty': '1'}],
            'event_count': 1,
        }
    })
    monkeypatch.setattr(mod, 'fetch_tickers', lambda client: pytest.fail('REST fallback should be skipped'))
    monkeypatch.setattr(mod, '_runtime_store_rest_guard_snapshot', lambda _store: {'state': 'CLOSED', 'rest_used_weight_1m': 901})

    rows, diag = mod.resolve_scan_tickers(object(), store, _ticker_args(), fallback_symbols=['BTCUSDT'], return_diagnostics=True)

    assert rows and rows[0]['symbol'] == 'BTCUSDT'
    assert rows[0]['degraded_ticker_24hr'] is True
    assert diag['degraded'] is True
    assert diag['degraded_reason'] == 'ticker_24hr_cache_missing'


def test_scanner_rest_fallback_min_interval_enforced(tmp_path, monkeypatch):
    store = mod.RuntimeStateStore(str(tmp_path))
    monkeypatch.setattr(mod, '_runtime_store_rest_guard_snapshot', lambda _store: {'state': 'CLOSED', 'rest_used_weight_1m': 100})
    monkeypatch.setattr(mod, 'fetch_tickers', lambda client: [{'symbol': 'BTCUSDT'}])

    first_rows, first_diag = mod.resolve_scan_tickers(object(), store, _ticker_args(), fallback_symbols=['BTCUSDT'], return_diagnostics=True)
    second_rows, second_diag = mod.resolve_scan_tickers(object(), store, _ticker_args(), fallback_symbols=['BTCUSDT'], return_diagnostics=True)

    assert first_rows == [{'symbol': 'BTCUSDT'}]
    assert first_diag['scanner_rest_fallback_used'] is True
    assert second_rows == []
    assert second_diag['scanner_rest_fallback_skipped_reason'] == 'scanner_rest_fallback_min_interval'


def test_ticker_24hr_cache_refresher_loop_failure_returns_cleanly(tmp_path, monkeypatch):
    store = mod.RuntimeStateStore(str(tmp_path))
    attempts = {'count': 0}
    def boom(_client):
        attempts['count'] += 1
        raise RuntimeError('temporary ticker failure')
    class OneShotSleep:
        def __init__(self):
            self.stopped = False
        def is_set(self):
            return self.stopped
        def wait(self, _interval):
            self.stopped = True
            return True
    monkeypatch.setattr(mod, 'fetch_tickers', boom)

    mod.ticker_24hr_cache_refresher_loop(object(), _ticker_args(ticker_24hr_cache_refresh_seconds=1), store, stop_event=OneShotSleep())

    assert attempts['count'] == 1
    assert store.load_json('ticker_24hr_cache', {}) == {}


def test_ticker_24hr_cache_diagnostics_fields_present(tmp_path, monkeypatch):
    store = mod.RuntimeStateStore(str(tmp_path))
    monkeypatch.setattr(mod, '_runtime_store_rest_guard_snapshot', lambda _store: {'state': 'CLOSED', 'rest_used_weight_1m': 901})

    _rows, diag = mod.resolve_scan_tickers(object(), store, _ticker_args(), fallback_symbols=['BTCUSDT'], return_diagnostics=True)

    for key in [
        'ticker_24hr_cache_available',
        'ticker_24hr_cache_age_seconds',
        'ticker_24hr_cache_row_count',
        'scanner_rest_fallback_used',
        'scanner_rest_fallback_skipped_reason',
        'rest_used_weight_1m',
        'symbols_skipped_due_to_missing_ticker_24hr',
    ]:
        assert key in diag
