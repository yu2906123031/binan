import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'binance_futures_momentum_long.py'
EXEC_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'execution_engine.py'
SCRIPTS_DIR = SCRIPT_PATH.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_strategy():
    spec = importlib.util.spec_from_file_location('bfml_five_usdt_test', SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_execution():
    spec = importlib.util.spec_from_file_location('exec_five_usdt_test', EXEC_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def make_candidate(mod, symbol='1000BONKUSDT', **kwargs):
    base = dict(
        symbol=symbol,
        last_price=1.0,
        price_change_pct_24h=5.0,
        quote_volume_24h=10_000_000.0,
        hot_rank=None,
        gainer_rank=None,
        funding_rate=0.0,
        funding_rate_avg=0.0,
        recent_5m_change_pct=1.0,
        acceleration_ratio_5m_vs_15m=1.0,
        breakout_level=1.01,
        recent_swing_low=0.98,
        stop_price=0.98,
        quantity=1.0,
        risk_per_unit=0.02,
        recommended_leverage=3,
        rsi_5m=55.0,
        volume_multiple=2.0,
        distance_from_ema20_5m_pct=1.0,
        distance_from_vwap_15m_pct=1.0,
        higher_tf_summary={},
        score=70.0,
        reasons=[],
        side='LONG',
        expected_net_profit_usdt=5.1,
        expected_rr=2.2,
        expected_loss_usdt=2.0,
        spread_bps=1.0,
    )
    base.update(kwargs)
    return mod.Candidate(**base)


def test_long_target_5u_quantity_reverse_sizing():
    mod = load_strategy()
    plan = mod.plan_five_usdt_target_trade('LONG', 100.0, 105.0, 99.2, target_net_profit_usdt=5.0)
    assert plan['planned_quantity'] > 1.0
    assert abs(plan['expected_net_profit_usdt'] - 5.0) < 0.08
    assert plan['target_profit_reject_reason'] == ''


def test_short_target_5u_quantity_reverse_sizing():
    mod = load_strategy()
    plan = mod.plan_five_usdt_target_trade('SHORT', 100.0, 95.0, 100.8, target_net_profit_usdt=5.0)
    assert plan['planned_quantity'] > 1.0
    assert abs(plan['expected_net_profit_usdt'] - 5.0) < 0.08
    assert plan['target_profit_reject_reason'] == ''


def test_stop_loss_over_2_5_shrinks_or_rejects():
    mod = load_strategy()
    plan = mod.plan_five_usdt_target_trade('LONG', 100.0, 105.0, 90.0, target_net_profit_usdt=5.0, max_loss_usdt=2.5)
    assert plan['expected_loss_usdt'] <= 2.55 or plan['target_profit_reject_reason'] in {'expected_net_profit_below_min', 'expected_rr_below_min', 'expected_loss_above_max'}


def test_expected_rr_below_min_rejected():
    mod = load_strategy()
    plan = mod.plan_five_usdt_target_trade('LONG', 100.0, 101.0, 99.0, min_expected_rr=1.7)
    assert plan['target_profit_reject_reason'] in {'expected_rr_below_min', 'expected_net_profit_below_min', 'tiny_profit_trade_rejected'}


def test_expected_net_profit_below_min_rejected():
    mod = load_strategy()
    plan = mod.plan_five_usdt_target_trade('LONG', 100.0, 100.8, 99.8, target_net_profit_usdt=3.0, min_target_net_profit_usdt=4.2, taker_fee_rate=0.0, slippage_buffer_pct=0.0)
    assert plan['target_profit_reject_reason'] in {'expected_net_profit_below_min', 'tiny_profit_trade_rejected'}


def test_fee_ratio_over_12_percent_rejected():
    mod = load_strategy()
    plan = mod.plan_five_usdt_target_trade('LONG', 100.0, 110.0, 98.0, taker_fee_rate=0.01)
    assert plan['target_profit_reject_reason'] == 'fee_ratio_too_high'


def test_tiny_profit_trade_rejected():
    mod = load_strategy()
    plan = mod.plan_five_usdt_target_trade('LONG', 100.0, 100.01, 99.9, disable_tiny_tp=True)
    assert plan['target_profit_reject_reason'] in {'tiny_profit_trade_rejected', 'invalid_take_profit_distance'}


def test_stale_reduce_only_orders_cancelled_after_flat():
    exe = load_execution()
    cancelled = []
    orders = [
        {'symbol': 'ZECUSDT', 'orderId': 1, 'type': 'STOP_MARKET', 'side': 'BUY', 'positionSide': 'SHORT', 'reduceOnly': True},
        {'symbol': 'ZECUSDT', 'orderId': 2, 'type': 'TAKE_PROFIT_MARKET', 'side': 'BUY', 'positionSide': 'SHORT', 'reduceOnly': 'true'},
        {'symbol': 'ZECUSDT', 'orderId': 3, 'type': 'LIMIT', 'side': 'SELL', 'positionSide': 'SHORT', 'reduceOnly': False},
    ]
    events = exe.cancel_stale_protection_orders_after_flat(
        client=object(), symbol='ZECUSDT', position_side='SHORT',
        fetch_open_orders=lambda c, s: orders,
        cancel_order=lambda c, symbol, order_id=None, **kw: cancelled.append(order_id) or {'orderId': order_id},
        emit_event=lambda name, payload: None,
    )
    assert cancelled == [1, 2]
    assert events['cancelled_count'] == 2


def test_step_size_tick_size_constraints_and_profile_defaults():
    mod = load_strategy()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'five-usdt-scalp-v2']))
    assert args.risk_usdt == 1.5
    assert args.max_loss_usdt <= 2.5
    plan = mod.plan_five_usdt_target_trade('LONG', 100.03, 105.07, 97.01, step_size=0.001, tick_size=0.01)
    assert round(plan['planned_quantity'], 3) == plan['planned_quantity']
    assert round(plan['planned_take_profit_price'], 2) == plan['planned_take_profit_price']

class DummyStore:
    def __init__(self, rows):
        self.rows = rows

    def read_events(self, limit=5000):
        return self.rows[-limit:]


def test_five_usdt_profile_classifies_symbol_quality_tiers():
    mod = load_strategy()
    assert mod.classify_five_usdt_symbol_quality_tier('BTCUSDT')['symbol_quality_tier'] == 'A'
    assert mod.classify_five_usdt_symbol_quality_tier('NEARUSDT')['symbol_quality_tier'] == 'B'
    tier_c = mod.classify_five_usdt_symbol_quality_tier('1000BONKUSDT')
    assert tier_c['symbol_quality_tier'] == 'C'
    assert tier_c['symbol_tier_min_expected_net_profit_usdt'] == 5.8
    assert tier_c['symbol_tier_min_rr'] == 2.5
    underperformer = mod.classify_five_usdt_symbol_quality_tier('NOTUSDT')
    assert underperformer['symbol_quality_tier'] == 'C'
    assert underperformer['symbol_quality_reason'] == 'recent_30d_underperformer_deweighted'


def test_five_usdt_symbol_loss_cooldown_map_uses_recent_closed_losses():
    mod = load_strategy()
    now = mod._utc_now()
    store = DummyStore([
        {'event_type': 'trade_closed', 'symbol': 'ZECUSDT', 'net_pnl_usdt': -2.8, 'closed_at': mod._isoformat_utc(now - mod.datetime.timedelta(hours=2))},
    ])
    cooldowns = mod.build_symbol_loss_cooldown_map(store)
    assert 'ZECUSDT' in cooldowns
    assert cooldowns['ZECUSDT']['reasons'] == ['symbol_recent_loss_cooldown']


def test_five_usdt_symbol_loss_cooldown_map_blocks_two_consecutive_losses_for_six_hours():
    mod = load_strategy()
    now = mod._utc_now()
    store = DummyStore([
        {'event_type': 'trade_closed', 'symbol': 'HYPEUSDT', 'net_pnl_usdt': -0.4, 'closed_at': mod._isoformat_utc(now - mod.datetime.timedelta(hours=2))},
        {'event_type': 'trade_closed', 'symbol': 'HYPEUSDT', 'net_pnl_usdt': -0.5, 'closed_at': mod._isoformat_utc(now - mod.datetime.timedelta(hours=1))},
    ])
    cooldown = mod.build_symbol_loss_cooldown_map(store)['HYPEUSDT']
    assert 'symbol_consecutive_loss_cooldown' in cooldown['reasons']
    assert cooldown['consecutive_losses'] == 2
    assert mod._parse_iso8601_utc(cooldown['cooldown_until']) >= now + mod.datetime.timedelta(hours=4, minutes=59)


def test_five_usdt_symbol_loss_cooldown_map_blocks_recent_profit_factor_below_point_seven():
    mod = load_strategy()
    now = mod._utc_now()
    rows = []
    pnls = [0.2, -1.0, 0.2, -1.0, 0.2, -1.0, 0.2, -1.0, 0.2, -1.0]
    for index, pnl in enumerate(pnls):
        rows.append({'event_type': 'trade_closed', 'symbol': 'WLDUSDT', 'net_pnl_usdt': pnl, 'closed_at': mod._isoformat_utc(now - mod.datetime.timedelta(hours=10 - index))})
    cooldown = mod.build_symbol_loss_cooldown_map(DummyStore(rows))['WLDUSDT']
    assert 'symbol_low_profit_factor_cooldown' in cooldown['reasons']
    assert cooldown['recent10_profit_factor'] == 0.2


def test_five_usdt_candidate_selection_filter_applies_tier_thresholds_and_fields():
    mod = load_strategy()
    candidate = make_candidate(mod, expected_net_profit_usdt=4.8, oi_change_pct_5m=1.0, cvd_delta=1.0)
    reason = mod.apply_five_usdt_candidate_selection_filter(candidate, mod.parse_args([]), None, {})
    assert reason == 'symbol_tier_expected_profit_too_low'
    assert candidate.symbol_quality_tier == 'C'
    assert candidate.symbol_tier_min_expected_net_profit_usdt == 5.8


def test_five_usdt_candidate_selection_filter_blocks_recent_underperformers():
    mod = load_strategy()
    candidate = make_candidate(
        mod,
        symbol='NOTUSDT',
        expected_net_profit_usdt=99.0,
        expected_rr=99.0,
        expected_loss_usdt=0.1,
        oi_change_pct_5m=1.0,
        cvd_delta=1.0,
    )

    reason = mod.apply_five_usdt_candidate_selection_filter(candidate, mod.parse_args([]), None, {})

    assert reason == 'recent_30d_underperformer_blacklist'
    assert 'recent_30d_underperformer_deweighted' in candidate.reasons


def test_five_usdt_candidate_selection_filter_rejects_c_tier_without_orderflow_confirmation():
    mod = load_strategy()
    candidate = make_candidate(mod, expected_net_profit_usdt=6.2, expected_rr=2.8, expected_loss_usdt=1.0)
    reason = mod.apply_five_usdt_candidate_selection_filter(candidate, mod.parse_args([]), None, {})
    assert reason == 'c_tier_missing_orderflow_confirmation'


def test_five_usdt_profile_defaults_candidate_source_caps_and_symbol_quality():
    mod = load_strategy()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'five-usdt-scalp-v2']))
    assert args.top_gainers == 25
    assert args.top_losers == 25
    assert args.max_candidates >= 10
    assert args.scan_prefilter_multiplier == 3
    assert args.enable_symbol_quality_tier is True
    assert args.five_usdt_watchlist_scoring is False


def test_five_usdt_score_breakdown_watchlist_and_risk_off_weight():
    mod = load_strategy()
    candidate = make_candidate(
        mod,
        symbol='NEARUSDT',
        score=62.0,
        expected_net_profit_usdt=5.4,
        expected_rr=2.1,
        expected_loss_usdt=2.0,
        volume_multiple=2.4,
        oi_change_pct_5m=0.8,
        cvd_delta=1.3,
        entry_distance_from_breakout_pct=-0.22,
        trigger_confirmation_flags={'breakout_close_confirmed': False, 'oi_taker_alignment_confirmed': True},
        setup_ready=True,
        trigger_fired=False,
    )

    mod.apply_five_usdt_candidate_selection_filter(candidate, mod.parse_args([]), None, {})
    weighted = mod.apply_five_usdt_watchlist_scoring(candidate, {'label': 'risk_off', 'score_multiplier': 0.55})

    assert weighted.five_usdt_score_breakdown['expected_profit_score'] > 0
    assert weighted.five_usdt_score_breakdown['orderflow_score'] > 0
    assert weighted.watchlist_priority_score > weighted.score
    assert weighted.risk_off_score_weight == 0.85
    assert any(reason.startswith('watchlist_priority_score=') for reason in weighted.reasons)


def test_five_usdt_alert_exposes_watchlist_score_fields():
    mod = load_strategy()
    candidate = make_candidate(mod)
    candidate.five_usdt_score_breakdown = {'expected_profit_score': 10.0}
    candidate.watchlist_priority_score = 81.5
    candidate.risk_off_score_weight = 1.15

    alert = mod.build_standardized_alert(candidate)

    assert alert['five_usdt_score_breakdown'] == {'expected_profit_score': 10.0}
    assert alert['watchlist_priority_score'] == 81.5
    assert alert['risk_off_score_weight'] == 1.15


def test_five_usdt_profile_defaults_expand_watchlist_scan_depth():
    mod = load_strategy()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'five-usdt-scalp-v2']))

    assert args.top_gainers >= 20
    assert args.top_losers >= 20
    assert args.max_candidates >= 8
    assert args.scan_prefilter_multiplier >= 3
    assert args.five_usdt_watchlist_scoring is False


def test_micro_scalp_profile_defaults_are_safe_and_small():
    mod = load_strategy()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'five-usdt-scalp-v2']))

    assert args.risk_usdt == 1.5
    assert args.min_notional_usdt == 60.0
    assert args.max_notional_usdt == 90.0
    assert args.target_notional_usdt == 80.0
    assert args.probe_min_notional_usdt == 20.0
    assert args.probe_max_notional_usdt == 30.0
    assert args.leverage == 10
    assert args.max_open_positions == 1
    assert args.symbol_cooldown_minutes == 5
    assert args.opposite_side_flip_cooldown_minutes == 15
    assert args.daily_max_loss_usdt == 10.0
    assert args.max_consecutive_losses == 2
    assert args.consecutive_loss_pause_minutes == 120
    assert args.daily_circuit_breaker is True
    assert args.take_profit_pct == 0.012
    assert 0.008 <= args.stop_loss_pct <= 0.012
    assert args.breakeven_after_pct == 0.01
    assert args.trailing_after_pct == 0.018
    assert args.max_holding_minutes >= 30
    assert args.timeout_exit_enabled is True
    assert args.micro_scalp_time_stop_sec == 2700
    assert args.micro_scalp_min_profit_r == 0.0
    assert args.execution_preflight_enabled is True
    assert args.repair_missing_protection is True


def test_micro_scalp_score_breakdown_thresholds_and_breakout_soft_score():
    mod = load_strategy()
    candidate = make_candidate(
        mod,
        recent_5m_change_pct=0.18,
        volume_multiple=1.8,
        entry_distance_from_breakout_pct=-0.35,
        distance_from_ema20_5m_pct=0.08,
        distance_from_vwap_15m_pct=0.06,
        oi_change_pct_5m=0.35,
        cvd_delta=1.2,
        funding_rate=0.0001,
        spread_bps=1.2,
        trigger_confirmation_flags={'breakout_close_confirmed': False},
    )

    breakdown = mod.compute_micro_scalp_score_breakdown(candidate)

    assert set(breakdown) == {
        'volume_spike_score',
        'price_acceleration_score',
        'book_imbalance_score',
        'vwap_ema_reclaim_score',
        'oi_delta_score',
        'cvd_alignment_score',
        'breakout_proximity_score',
        'funding_crowding_score',
    }
    assert sum(breakdown.values()) >= 55
    assert breakdown['breakout_proximity_score'] > 0
    assert mod.classify_micro_scalp_score(sum(breakdown.values()))['candidate_pool'] is True


def test_micro_scalp_alert_exposes_score_breakdown_and_status():
    mod = load_strategy()
    candidate = make_candidate(mod, score=0.0)
    candidate.scalp_score_breakdown = {
        'volume_spike_score': 20,
        'price_acceleration_score': 15,
        'book_imbalance_score': 10,
        'vwap_ema_reclaim_score': 10,
        'oi_delta_score': 5,
        'cvd_alignment_score': 5,
        'breakout_proximity_score': 5,
        'funding_crowding_score': 5,
    }
    candidate.scalp_score = 75
    candidate.strong_trade_candidate = True
    alert = mod.build_standardized_alert(candidate)

    assert alert['scalp_score'] == 75
    assert alert['strong_trade_candidate'] is True
    assert alert['scalp_score_breakdown']['volume_spike_score'] == 20


def test_micro_scalp_last_cycle_payload_has_required_diagnostics_shape():
    mod = load_strategy()
    payload = mod.build_micro_scalp_cycle_diagnostics(
        early_filter_pass_count=3,
        early_filter_reject_reasons={'quote_volume_below_gate': 2},
        candidate_alerts=[{'symbol': 'SOLUSDT', 'scalp_score': 72, 'scalp_score_breakdown': {'volume_spike_score': 20}}],
        blocked_reasons={'trigger_missing': 1, 'risk_blocked': 1},
    )

    assert payload['early_filter_pass_count'] == 3
    assert payload['early_filter_reject_reasons']['quote_volume_below_gate'] == 2
    assert payload['scalp_score_top_symbols'][0]['symbol'] == 'SOLUSDT'
    assert payload['top_10_candidates'][0]['scalp_score_breakdown']['volume_spike_score'] == 20
    for key in ['liquidity_failed', 'spread_failed', 'score_too_low', 'trigger_missing', 'risk_blocked', 'execution_blocked']:
        assert key in payload['why_no_order']



def test_expensive_scan_symbol_sets_prioritize_watchlist_and_apply_independent_limits():
    mod = load_strategy()
    plan = mod.resolve_expensive_scan_symbol_sets(
        ['AAAUSDT', 'BBBUSDT', 'CCCUSDT', 'DDDUSDT'],
        watchlist_symbols=['CCCUSDT', 'ZZZUSDT'],
        microstructure_limit=3,
        funding_limit=2,
        open_interest_limit=1,
        cvd_limit=2,
    )

    assert plan['priority_symbols'][:3] == ['CCCUSDT', 'AAAUSDT', 'BBBUSDT']
    assert plan['microstructure_symbols'] == {'CCCUSDT', 'AAAUSDT', 'BBBUSDT'}
    assert plan['funding_symbols'] == {'CCCUSDT', 'AAAUSDT'}
    assert plan['open_interest_symbols'] == {'CCCUSDT'}
    assert plan['cvd_symbols'] == {'CCCUSDT', 'AAAUSDT'}
    assert plan['diagnostics']['funding_symbol_limit'] == 2
    assert plan['diagnostics']['open_interest_symbol_limit'] == 1


def test_five_usdt_profile_defaults_rest_budgeted_scan_depth_and_watchlist():
    mod = load_strategy()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'five-usdt-scalp-v2']))

    assert args.scan_microstructure_symbol_limit <= args.max_candidates
    assert args.scan_funding_symbol_limit <= args.scan_microstructure_symbol_limit
    assert args.scan_open_interest_symbol_limit <= args.scan_microstructure_symbol_limit
    assert args.scan_cvd_symbol_limit <= args.scan_microstructure_symbol_limit
    assert 'BTCUSDT' in args.scan_watchlist_symbols
    assert 'SOLUSDT' in args.scan_watchlist_symbols


def test_build_scan_budget_diagnostics_exposes_profile_and_limits():
    mod = load_strategy()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'five-usdt-scalp-v2']))
    plan = mod.resolve_expensive_scan_symbol_sets(
        ['1000BONKUSDT', 'SOLUSDT', 'ETHUSDT'],
        watchlist_symbols=mod.parse_symbol_list(args.scan_watchlist_symbols),
        microstructure_limit=args.scan_microstructure_symbol_limit,
        funding_limit=args.scan_funding_symbol_limit,
        open_interest_limit=args.scan_open_interest_symbol_limit,
        cvd_limit=args.scan_cvd_symbol_limit,
    )
    diagnostics = mod.build_scan_budget_diagnostics(args, plan)

    assert diagnostics['profile'] == 'five-usdt-scalp-v2'
    assert diagnostics['watchlist_symbol_count'] >= 2
    assert diagnostics['expensive_fetch_budget']['funding'] == args.scan_funding_symbol_limit
    assert diagnostics['expensive_fetch_budget']['open_interest'] == args.scan_open_interest_symbol_limit
    assert diagnostics['expensive_fetch_budget']['cvd'] == args.scan_cvd_symbol_limit
    assert diagnostics['microstructure_symbol_count'] == len(plan['microstructure_symbols'])


def test_market_regime_engine_uses_named_states_and_dynamic_side_weights():
    mod = load_strategy()
    up = [[0, 0, 0, 0, str(100 + i), 0] for i in range(30)]
    down = [[0, 0, 0, 0, str(130 - i), 0] for i in range(30)]

    bull = mod.compute_market_regime_filter(up, up)
    bear = mod.compute_market_regime_filter(down, down)

    assert bull['label'] == 'BULL_TREND'
    assert bull['score_multiplier'] == 1.0
    assert bull['side_weights'] == {'LONG': 0.7, 'SHORT': 0.3}
    assert bear['label'] == 'BEAR_TREND'
    assert bear['score_multiplier'] == 0.8
    assert bear['side_weights'] == {'LONG': 0.2, 'SHORT': 0.8}
    assert mod.derive_directional_score_multiplier('LONG', 'BULL_TREND', 1.0) > mod.derive_directional_score_multiplier('SHORT', 'BULL_TREND', 1.0)
    assert mod.derive_directional_score_multiplier('SHORT', 'BEAR_TREND', 0.8) > mod.derive_directional_score_multiplier('LONG', 'BEAR_TREND', 0.8)


def test_sector_engine_classifies_symbols_and_scores_resonance():
    mod = load_strategy()
    assert mod.classify_symbol_sector('DOGEUSDT') == 'MEME'
    assert mod.classify_symbol_sector('FETUSDT') == 'AI'
    assert mod.classify_symbol_sector('ARBUSDT') == 'L2'
    score = mod.compute_sector_score('DOGEUSDT', ['PEPEUSDT', 'BONKUSDT', 'FETUSDT'])
    assert score['sector'] == 'MEME'
    assert score['same_sector_count'] == 2
    assert score['sector_score'] > 0


def test_candidate_pool_grade_abc_keeps_watch_candidates():
    mod = load_strategy()
    assert mod.classify_candidate_pool_grade(82, setup_ready=True, trigger_fired=True)['grade'] == 'A'
    assert mod.classify_candidate_pool_grade(66, setup_ready=True, trigger_fired=False)['grade'] == 'B'
    c = mod.classify_candidate_pool_grade(48, setup_ready=False, trigger_fired=False)
    assert c['grade'] == 'C'
    assert c['candidate_pool'] is True


def test_persistent_watchlist_db_keeps_rows_and_prunes_after_ttl(tmp_path):
    mod = load_strategy()
    db_path = tmp_path / 'watchlist.db'
    now = 1_700_000_000.0
    mod.upsert_watchlist_db(db_path, [{'symbol': 'DOGEUSDT', 'score': 61.0, 'sector': 'MEME'}], now_ts=now)
    rows = mod.load_watchlist_db(db_path, now_ts=now + 3600, ttl_hours=72)
    assert rows[0]['symbol'] == 'DOGEUSDT'
    assert rows[0]['max_score'] == 61.0
    assert rows[0]['sector'] == 'MEME'
    mod.upsert_watchlist_db(db_path, [{'symbol': 'DOGEUSDT', 'score': 72.0, 'sector': 'MEME'}], now_ts=now + 7200)
    rows = mod.load_watchlist_db(db_path, now_ts=now + 7200, ttl_hours=72)
    assert rows[0]['max_score'] == 72.0
    assert rows[0]['last_seen'] >= rows[0]['first_seen']
    assert mod.load_watchlist_db(db_path, now_ts=now + 75 * 3600, ttl_hours=72) == []


def test_micro_scalp_diagnostics_exposes_why_no_trade_top5_and_runtime_stats():
    mod = load_strategy()
    payload = mod.build_micro_scalp_cycle_diagnostics(
        early_filter_pass_count=4,
        early_filter_reject_reasons={},
        candidate_alerts=[
            {'symbol': 'AUSDT', 'scalp_score': 80, 'candidate_pool_grade': 'B', 'trade_missing': ['candidate_trigger_not_fired']},
            {'symbol': 'BUSDT', 'scalp_score': 70, 'candidate_pool_grade': 'C', 'trade_missing': ['candidate_setup_not_ready']},
        ],
        blocked_reasons={'trigger_missing': 2},
    )
    assert len(payload['why_no_trade_top5']) == 2
    assert payload['why_no_trade_top5'][0]['symbol'] == 'AUSDT'
    assert payload['runtime_stats']['candidate_count'] == 2
    assert payload['runtime_stats']['near_trade_count'] == 1


def test_five_usdt_scalp_v2_profile_uses_trade_size_defaults():
    mod = load_strategy()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'five-usdt-scalp-v2']))
    assert args.max_open_positions == 1
    assert args.max_long_positions == 1
    assert args.max_short_positions == 1
    assert args.min_notional_usdt == 60.0
    assert args.max_notional_usdt == 90.0
    assert args.target_notional_usdt == 80.0
    assert args.leverage == 10
    assert args.daily_max_loss_usdt == 10.0
    assert args.max_consecutive_losses == 2
    assert args.consecutive_loss_pause_minutes == 120
    assert args.take_profit_pct == 0.012
    assert args.tp1_close_pct == 0.4
    assert args.tp2_r == 2.5
    assert args.tp2_close_pct == 0.4
    assert args.tp3_r == 4.0
    assert args.tp3_close_pct == 0.2
    assert args.breakeven_after_pct == 0.01
    assert args.trailing_after_pct == 0.018
    assert args.max_holding_minutes >= 30


def test_position_tier_trade_requires_a_grade_full_confirmation_and_liquidity():
    mod = load_strategy()
    candidate = make_candidate(
        mod,
        score=80.0,
        candidate_pool_grade='A',
        trigger_fired=True,
        trigger_confirmation_count=2,
        setup_missing=[],
        trade_missing=[],
        liquidity_grade='A',
        execution_liquidity_grade_override='A',
        spread_bps=2.0,
        book_depth_fill_ratio=0.9,
        expected_edge=1.3,
    )
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'five-usdt-scalp-v2']))
    plan = mod.classify_five_usdt_position_tier(candidate, args, account_balance_usdt=87.0)
    assert plan['position_tier'] == 'TRADE'
    assert plan['target_notional_usdt'] == 80.0
    assert plan['planned_notional'] == 80.0
    assert plan['max_allowed_notional_usdt'] <= 95.7
    assert plan['submit_block_reason'] == ''
    assert plan['expected_profit_at_tp2'] >= 1.9
    assert plan['expected_profit_at_tp3'] >= 3.1


def test_position_tier_b_grade_is_probe_only():
    mod = load_strategy()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'five-usdt-scalp-v2']))
    candidate = make_candidate(
        mod,
        score=77.0,
        candidate_pool_grade='B',
        trigger_fired=True,
        trigger_confirmation_count=2,
        setup_missing=[],
        trade_missing=[],
        liquidity_grade='A',
        execution_liquidity_grade_override='A',
        spread_bps=2.0,
        book_depth_fill_ratio=0.9,
        expected_edge=1.4,
    )
    plan = mod.classify_five_usdt_position_tier(candidate, args, account_balance_usdt=200.0)
    assert plan['position_tier'] == 'PROBE'
    assert 20.0 <= plan['planned_notional'] <= 30.0
    assert plan['submit_block_reason'] == 'trade_tier_requires_a_grade'


def test_candidate_selected_payload_includes_position_tier_profit_and_risk_fields():
    mod = load_strategy()
    candidate = make_candidate(mod)
    candidate.position_tier = 'TRADE'
    candidate.target_notional_usdt = 80.0
    candidate.planned_notional_usdt = 80.0
    candidate.expected_profit_at_tp1 = 0.96
    candidate.expected_profit_at_tp2 = 3.04
    candidate.expected_profit_at_tp3 = 4.96
    candidate.max_loss_to_stop = 0.8
    candidate.risk_reward_ratio = 3.8
    candidate.submit_block_reason = ''
    payload = mod.build_candidate_selected_event_payload(candidate)
    for key in [
        'position_tier', 'target_notional_usdt', 'planned_notional',
        'expected_profit_at_tp1', 'expected_profit_at_tp2', 'expected_profit_at_tp3',
        'max_loss_to_stop', 'risk_reward_ratio', 'submit_block_reason',
    ]:
        assert key in payload
    assert payload['position_tier'] == 'TRADE'
    assert payload['planned_notional'] == 80.0


def test_orphan_entry_order_detected_for_untracked_reduce_false_limit():
    mod = load_strategy()
    events = []
    cancelled = []
    orders = [
        {'symbol': 'GALAUSDT', 'orderId': 77, 'type': 'LIMIT', 'side': 'BUY', 'reduceOnly': False, 'clientOrderId': 'legacy-entry'},
    ]
    result = mod.detect_and_cancel_orphan_entry_orders(
        client=object(),
        open_orders=orders,
        active_entry_client_order_ids=set(),
        emit_event=lambda name, payload: events.append((name, payload)),
        cancel_order_fn=lambda client, order: cancelled.append(order['orderId']) or {'orderId': order['orderId']},
    )
    assert result['orphan_entry_order_detected'] == 1
    assert result['orphan_entry_order_cancelled'] == 1
    assert events[0][0] == 'orphan_entry_order_detected'
    assert cancelled == [77]
