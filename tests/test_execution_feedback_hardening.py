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
