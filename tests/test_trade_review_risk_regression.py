import importlib.util
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]


def load_module(name, rel):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


risk_engine = load_module('risk_engine_for_trade_review', 'scripts/risk_engine.py')
exporter = load_module('export_binance_last_5d_trades_for_test', 'scripts/export_binance_last_5d_trades.py')


def default_risk_state():
    return {
        'halted': False,
        'symbol_cooldowns': {},
        'daily_realized_pnl_usdt': 0.0,
        'consecutive_losses': 0,
        'daily_symbol_loss_usdt': {},
        'rolling_symbol_loss_usdt': {},
        'symbol_consecutive_losses': {},
        'recent_symbol_entries': {},
        'recent_closed_trades': [],
        'limit_cancel_replace_count_by_symbol': {},
        'loss_deweighted_symbols': {},
    }


def eval_guard(symbol='NEARUSDT', risk_state=None, **kwargs):
    candidate = SimpleNamespace(
        symbol=symbol,
        side='LONG',
        setup_ready=True,
        trigger_fired=True,
        score=70,
        cancel_rate=0,
        book_depth_fill_ratio=1,
    )
    return risk_engine.evaluate_risk_guards(
        symbol=symbol,
        risk_state=risk_state or default_risk_state(),
        candidate=candidate,
        now_ts=1_700_000_000,
        default_risk_state=default_risk_state,
        _to_float=lambda value, default=0.0: float(value if value is not None else default),
        compute_expected_slippage_r=lambda c: 0.0,
        classify_execution_liquidity_grade=lambda *a, **k: 'A',
        estimate_candidate_heat_r=lambda c, base_risk_usdt=0.0: 0.1,
        time_module=SimpleNamespace(time=lambda: 1_700_000_000),
        **kwargs,
    )


def test_symbol_loss_limit_blocks_new_entry_and_allows_reduce_only_context():
    state = default_risk_state()
    state['daily_symbol_loss_usdt'] = {'NEARUSDT': -1.21}
    blocked = eval_guard(risk_state=state, symbol_daily_loss_limit_usdt=1.2)
    assert blocked['allowed'] is False
    assert 'symbol_daily_loss_limit_reached' in blocked['reasons']
    assert int(blocked['normalized_risk_state']['symbol_cooldowns']['NEARUSDT']) > 1_700_000_000

    close_allowed = eval_guard(risk_state=state, symbol_daily_loss_limit_usdt=1.2, reduce_only=True)
    assert close_allowed['allowed'] is True


def test_recent_symbol_loss_cooldown_blocks_reentry():
    state = default_risk_state()
    state['recent_closed_trades'] = [{'symbol': 'NEARUSDT', 'pnl_usdt': -0.2, 'closed_at': 1_699_999_990}]
    result = eval_guard(risk_state=state, after_symbol_loss_cooldown_minutes=180)
    assert result['allowed'] is False
    assert 'symbol_recent_loss_cooldown_active' in result['reasons']


def test_cancel_replace_limit_blocks_chasing():
    state = default_risk_state()
    state['limit_cancel_replace_count_by_symbol'] = {'NEARUSDT': 3}
    result = eval_guard(risk_state=state, max_limit_cancel_replace_per_symbol=2)
    assert result['allowed'] is False
    assert 'symbol_limit_cancel_replace_limit_reached' in result['reasons']


def test_open_position_and_total_pnl_summary_includes_unrealized():
    income = [
        {'incomeType': 'REALIZED_PNL', 'symbol': 'AUSDT', 'income': '2.0'},
        {'incomeType': 'COMMISSION', 'symbol': 'AUSDT', 'income': '-0.5'},
        {'incomeType': 'FUNDING_FEE', 'symbol': 'AUSDT', 'income': '-0.1'},
    ]
    positions = [{'symbol': 'AUSDT', 'positionAmt': '3', 'unRealizedProfit': '1.25'}]
    summary = exporter.build_summary(
        now=exporter.datetime.fromtimestamp(1000, tz=exporter.timezone.utc),
        start=exporter.datetime.fromtimestamp(0, tz=exporter.timezone.utc),
        records=[], income=income, positions=positions,
        trades_by_symbol={'AUSDT': []}, orders_by_symbol={'AUSDT': []}, symbols=['AUSDT'],
    )
    assert summary['net_realized_after_fee_usdt'] == 1.4
    assert summary['open_position_symbols'] == ['AUSDT']
    assert summary['unrealized_pnl_by_symbol_usdt'] == {'AUSDT': 1.25}
    assert summary['estimated_total_pnl_usdt'] == 2.65


def test_fast_stop_count_detects_loss_close_within_five_minutes():
    trades = {'AUSDT': [
        {'time': 1000, 'buyer': True, 'positionSide': 'LONG', 'realizedPnl': '0', 'commission': '0.01', 'maker': False, 'orderId': 1},
        {'time': 1000 + 240_000, 'buyer': False, 'positionSide': 'LONG', 'realizedPnl': '-0.4', 'commission': '0.01', 'maker': False, 'orderId': 2},
    ]}
    quality = exporter.compute_order_quality(trades, {'AUSDT': []})
    assert quality['fast_stop_count'] == 1
    assert quality['fast_stop_count_by_symbol']['AUSDT'] == 1
