from __future__ import annotations

from tests_support import strategy_v2_restore_regression_impl as _impl

# Preserve the large legacy regression suite without rewriting it. Pytest
# collects the imported test callables from this canonical module; only the
# execution-slippage assertion below is updated for the new signed convention.
for _name in dir(_impl):
    if _name.startswith('test_') and _name != 'test_place_live_trade_uses_gtx_limit_for_liquidity_grade_d_and_records_fill_metrics':
        globals()[_name] = getattr(_impl, _name)


def test_place_live_trade_uses_gtx_limit_for_liquidity_grade_d_and_records_fill_metrics(monkeypatch):
    candidate = _impl.mod.Candidate(
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
        expected_slippage_pct=0.0022,
        book_depth_fill_ratio=0.35,
        spread_bps=7.5,
    )
    candidate.top_depth_usdt = 180.0
    candidate.estimated_impact_pct = 0.05
    candidate.best_bid_price = 131.9
    args = _impl.argparse.Namespace(
        tp1_r=1.5,
        tp1_close_pct=0.3,
        tp2_r=2.0,
        tp2_close_pct=0.4,
        breakeven_r=1.0,
        profile='test',
        maker_only_timeout_seconds=1.0,
        maker_only_max_retries=0,
    )
    meta = _impl.make_meta()

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
                assert params['type'] == 'LIMIT'
                assert params['timeInForce'] == 'GTX'
                assert params['price'] == '131.9000'
                return {
                    'orderId': 12345,
                    'clientOrderId': 'entry-1',
                    'status': 'NEW',
                    'avgPrice': '0',
                    'executedQty': '0',
                    'cumQuote': '0',
                    'updateTime': 1710000000123,
                }
            raise AssertionError(path)

    client = FakeClient()

    monkeypatch.setattr(_impl.mod, 'log_runtime_event', lambda *a, **k: None)
    monkeypatch.setattr(_impl.mod, 'emit_notification', lambda *a, **k: None)
    monkeypatch.setattr(_impl.mod, 'place_stop_market_order', lambda *a, **k: {'orderId': 999, 'clientOrderId': 'stop-1'})
    monkeypatch.setattr(_impl.mod, 'place_take_profit_market_order', lambda *a, **k: {'orderId': 1001, 'clientOrderId': 'tp-1'})
    monkeypatch.setattr(_impl.mod, 'resolve_position_protection_status', lambda *a, **k: {'status': 'protected', 'expected_order_id': 999})
    monkeypatch.setattr(_impl.mod, 'query_order', lambda client, symbol, order_id=None, client_order_id=None: {
        'orderId': 12345,
        'clientOrderId': client_order_id or 'entry-1',
        'status': 'FILLED',
        'avgPrice': '131.9',
        'executedQty': '0.937',
        'cumQuote': '123.5903',
        'updateTime': 1710000000123,
        'symbol': symbol,
        'positionSide': 'LONG',
    })

    result = _impl.mod.place_live_trade(client, candidate, leverage=3, meta=meta, args=args)

    entry_order_calls = [params for path, params in client.calls if path == '/fapi/v1/order' and params.get('type') == 'LIMIT']
    assert entry_order_calls
    feedback = result['entry_order_feedback']
    assert feedback['execution_mode'] == 'maker_only'
    assert feedback['maker_or_taker'] == 'maker'
    assert feedback['predicted_slippage_bps'] == 0.22
    # 131.9 filled below the 132.0 reference for a LONG, so the fill was
    # favorable and must be negative under the signed slippage convention.
    assert feedback['actual_fill_slippage_bps'] < 0
    assert feedback['actual_fill_slippage_abs_bps'] > 0
    assert feedback['fill_ratio'] == 1.0
    assert feedback['liquidity_grade_at_entry'] == 'D'
    assert 'grade=D' in feedback['liquidity_grade_reason']
