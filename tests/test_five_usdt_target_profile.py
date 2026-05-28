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
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'five-usdt-target-v1']))
    assert args.target_net_profit_usdt == 5.0
    assert args.max_loss_usdt == 2.5
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
    assert tier_c['symbol_tier_min_expected_net_profit_usdt'] == 5.0
    assert tier_c['symbol_tier_min_rr'] == 2.0


def test_five_usdt_symbol_loss_cooldown_map_uses_recent_closed_losses():
    mod = load_strategy()
    now = mod._utc_now()
    store = DummyStore([
        {'event_type': 'trade_closed', 'symbol': 'ZECUSDT', 'net_pnl_usdt': -2.8, 'closed_at': mod._isoformat_utc(now - mod.datetime.timedelta(hours=2))},
    ])
    cooldowns = mod.build_symbol_loss_cooldown_map(store)
    assert 'ZECUSDT' in cooldowns
    assert cooldowns['ZECUSDT']['reasons'] == ['symbol_recent_loss_cooldown']


def test_five_usdt_candidate_selection_filter_applies_tier_thresholds_and_fields():
    mod = load_strategy()
    candidate = make_candidate(mod, expected_net_profit_usdt=4.8, oi_change_pct_5m=1.0, cvd_delta=1.0)
    reason = mod.apply_five_usdt_candidate_selection_filter(candidate, mod.parse_args([]), None, {})
    assert reason == 'symbol_tier_expected_profit_too_low'
    assert candidate.symbol_quality_tier == 'C'
    assert candidate.symbol_tier_min_expected_net_profit_usdt == 5.0


def test_five_usdt_candidate_selection_filter_rejects_c_tier_without_orderflow_confirmation():
    mod = load_strategy()
    candidate = make_candidate(mod)
    reason = mod.apply_five_usdt_candidate_selection_filter(candidate, mod.parse_args([]), None, {})
    assert reason == 'c_tier_missing_orderflow_confirmation'


def test_five_usdt_profile_defaults_candidate_source_caps_and_symbol_quality():
    mod = load_strategy()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'five-usdt-target-v1']))
    assert args.top_gainers == 10
    assert args.top_losers == 10
    assert args.max_candidates == 5
    assert args.scan_prefilter_multiplier == 2
    assert args.enable_symbol_quality_tier is True

