import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace


SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'execution_feedback_hardening.py'
spec = importlib.util.spec_from_file_location('execution_feedback_hardening_test_mod', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_long_feedback_uses_directional_slippage_and_expected_fill_price():
    candidate = SimpleNamespace(side='LONG', last_price=100.0)
    live_execution = {
        'entry_price': 100.1,
        'entry_order_feedback': {
            'predicted_slippage_bps': 8.0,
            'actual_fill_slippage_bps': 10.0,
            'liquidity_grade': 'A',
        },
    }
    result = mod.normalize_entry_order_feedback(candidate, live_execution)
    feedback = result['entry_order_feedback']
    assert feedback['market_price_at_submit'] == 100.0
    assert feedback['fill_price'] == 100.1
    assert feedback['predicted_fill_price'] == 100.08
    assert feedback['actual_fill_slippage_bps'] == 10.0
    assert feedback['actual_fill_slippage_abs_bps'] == 10.0
    assert feedback['slippage_error_bps'] == 2.0
    assert feedback['within_expected_slippage'] is False
    assert feedback['liquidity_grade_at_entry'] == 'A'


def test_short_favorable_fill_is_negative_slippage():
    candidate = SimpleNamespace(side='SHORT', last_price=100.0)
    result = mod.normalize_entry_order_feedback(
        candidate,
        {
            'entry_price': 100.1,
            'entry_order_feedback': {'predicted_slippage_bps': 8.0},
        },
    )
    feedback = result['entry_order_feedback']
    assert feedback['predicted_fill_price'] == 99.92
    assert feedback['actual_fill_slippage_bps'] == -10.0
    assert feedback['actual_fill_slippage_abs_bps'] == 10.0
    assert feedback['slippage_error_bps'] == -18.0
    assert feedback['within_expected_slippage'] is True


def test_explicit_submit_reference_is_preserved():
    candidate = SimpleNamespace(side='LONG', last_price=100.0)
    result = mod.normalize_entry_order_feedback(
        candidate,
        {
            'entry_price': 100.0,
            'entry_order_feedback': {
                'market_price_at_submit': 99.9,
                'predicted_slippage_bps': 4.0,
            },
        },
    )
    feedback = result['entry_order_feedback']
    assert feedback['market_price_at_submit'] == 99.9
    assert feedback['actual_fill_slippage_bps'] == 10.01


def test_install_hook_normalizes_returned_live_execution_and_is_idempotent():
    calls = {'count': 0}

    def place_live_trade(client, candidate, **kwargs):
        calls['count'] += 1
        return {
            'entry_price': 99.9,
            'entry_order_feedback': {
                'predicted_slippage_bps': 5.0,
                'actual_fill_slippage_bps': 10.0,
            },
        }

    strategy = SimpleNamespace(place_live_trade=place_live_trade)
    mod.install_execution_feedback_hardening(strategy)
    first_wrapper = strategy.place_live_trade
    mod.install_execution_feedback_hardening(strategy)
    assert strategy.place_live_trade is first_wrapper

    candidate = SimpleNamespace(side='LONG', last_price=100.0)
    result = strategy.place_live_trade(object(), candidate)
    assert calls['count'] == 1
    assert result['entry_order_feedback']['actual_fill_slippage_bps'] == -10.0
    assert result['entry_order_feedback']['within_expected_slippage'] is True


def test_submit_time_book_ticker_ask_is_used_for_long_entry():
    class Client:
        def __init__(self):
            self.orders = []

        def get(self, path, params=None, timeout=15, purpose='market_data'):
            assert path == '/fapi/v1/ticker/bookTicker'
            assert params == {'symbol': 'BTCUSDT'}
            assert purpose == 'execution'
            return {'bidPrice': '100.10', 'askPrice': '100.20'}

        def signed_post(self, path, params):
            self.orders.append((path, dict(params)))
            return {'status': 'FILLED'}

    def place_live_trade(client, candidate, **kwargs):
        client.signed_post('/fapi/v1/order', {'symbol': candidate.symbol, 'side': 'BUY', 'type': 'MARKET'})
        return {
            'entry_price': 100.25,
            'entry_order_feedback': {'predicted_slippage_bps': 8.0},
        }

    strategy = SimpleNamespace(place_live_trade=place_live_trade)
    mod.install_execution_feedback_hardening(strategy)
    candidate = SimpleNamespace(symbol='BTCUSDT', side='LONG', last_price=99.0)
    result = strategy.place_live_trade(Client(), candidate)
    feedback = result['entry_order_feedback']
    assert feedback['market_price_at_submit'] == 100.2
    assert feedback['market_price_source'] == 'book_ticker_ask_at_submit'
    assert feedback['actual_fill_slippage_bps'] == 4.99


def test_submit_time_book_ticker_bid_is_used_for_short_entry():
    class Client:
        def get(self, path, params=None, timeout=15, purpose='market_data'):
            return {'bidPrice': '99.80', 'askPrice': '99.90'}

        def signed_post(self, path, params):
            return {'status': 'FILLED'}

    def place_live_trade(client, candidate, **kwargs):
        client.signed_post('/fapi/v1/order', {'symbol': candidate.symbol, 'side': 'SELL', 'type': 'LIMIT', 'price': '99.8'})
        return {'entry_price': 99.75, 'entry_order_feedback': {'predicted_slippage_bps': 8.0}}

    strategy = SimpleNamespace(place_live_trade=place_live_trade)
    mod.install_execution_feedback_hardening(strategy)
    candidate = SimpleNamespace(symbol='ETHUSDT', side='SHORT', last_price=101.0)
    feedback = strategy.place_live_trade(Client(), candidate)['entry_order_feedback']
    assert feedback['market_price_at_submit'] == 99.8
    assert feedback['market_price_source'] == 'book_ticker_bid_at_submit'
    assert feedback['actual_fill_slippage_bps'] == 5.01


def test_protection_order_does_not_replace_entry_submit_reference():
    class Client:
        def get(self, path, params=None, timeout=15, purpose='market_data'):
            return {'bidPrice': '100.0', 'askPrice': '100.1'}

        def signed_post(self, path, params):
            return {'status': 'FILLED'}

    def place_live_trade(client, candidate, **kwargs):
        client.signed_post('/fapi/v1/order', {'symbol': candidate.symbol, 'side': 'BUY', 'type': 'MARKET'})
        client.signed_post(
            '/fapi/v1/order',
            {'symbol': candidate.symbol, 'side': 'SELL', 'type': 'STOP_MARKET', 'stopPrice': '98', 'reduceOnly': True},
        )
        return {'entry_price': 100.2, 'entry_order_feedback': {'predicted_slippage_bps': 8.0}}

    strategy = SimpleNamespace(place_live_trade=place_live_trade)
    mod.install_execution_feedback_hardening(strategy)
    candidate = SimpleNamespace(symbol='BTCUSDT', side='LONG', last_price=99.0)
    feedback = strategy.place_live_trade(Client(), candidate)['entry_order_feedback']
    assert feedback['market_price_at_submit'] == 100.1
