import importlib.util
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
SCRIPT_PATH = SCRIPTS_DIR / 'trade_bucket_analysis.py'
HARDENING_PATH = SCRIPTS_DIR / 'trade_bucket_slippage_hardening.py'

spec = importlib.util.spec_from_file_location('trade_bucket_analysis_direction_test', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)

hardening_spec = importlib.util.spec_from_file_location('trade_bucket_slippage_hardening_test', HARDENING_PATH)
hardening = importlib.util.module_from_spec(hardening_spec)
sys.modules[hardening_spec.name] = hardening
hardening_spec.loader.exec_module(hardening)
hardening.install_trade_bucket_slippage_hardening(mod)


def _closed_fill(side: str, entry_price: float):
    rows = [
        {
            'event_type': 'candidate_selected',
            'symbol': 'TESTUSDT',
            'side': side,
            'predicted_slippage_bps': 5.0,
            'shadow_entry_price': 100.0,
        },
        {
            'event_type': 'entry_filled',
            'symbol': 'TESTUSDT',
            'side': side,
            'entry_price': entry_price,
        },
        {
            'event_type': 'trade_invalidated',
            'symbol': 'TESTUSDT',
            'side': side,
            'realized_r': 0.0,
        },
    ]
    return mod.filter_closed_trade_events(rows)[0]


def test_long_adverse_fallback_slippage_is_positive():
    closed = _closed_fill('LONG', 100.10)
    assert closed['actual_fill_slippage_bps'] == 10.0


def test_long_favorable_fallback_slippage_is_negative():
    closed = _closed_fill('LONG', 99.90)
    assert closed['actual_fill_slippage_bps'] == -10.0
    assert closed['actual_fill_slippage_abs_bps'] == 10.0
    assert closed['slippage_error_bps'] == -15.0
    assert closed['within_expected_slippage'] is True


def test_short_adverse_fallback_slippage_is_positive():
    closed = _closed_fill('SHORT', 99.90)
    assert closed['actual_fill_slippage_bps'] == 10.0


def test_short_favorable_fallback_slippage_is_negative():
    closed = _closed_fill('SHORT', 100.10)
    assert closed['actual_fill_slippage_bps'] == -10.0
    assert closed['actual_fill_slippage_abs_bps'] == 10.0
    assert closed['slippage_error_bps'] == -15.0
    assert closed['within_expected_slippage'] is True


def test_direct_execution_feedback_remains_authoritative():
    rows = [
        {'event_type': 'candidate_selected', 'symbol': 'TESTUSDT', 'side': 'LONG', 'predicted_slippage_bps': 5.0, 'shadow_entry_price': 100.0},
        {'event_type': 'entry_filled', 'symbol': 'TESTUSDT', 'side': 'LONG', 'entry_price': 99.90, 'actual_fill_slippage_bps': -7.5},
        {'event_type': 'trade_invalidated', 'symbol': 'TESTUSDT', 'side': 'LONG', 'realized_r': 0.0},
    ]
    closed = mod.filter_closed_trade_events(rows)[0]
    assert closed['actual_fill_slippage_bps'] == -7.5


def test_installation_is_idempotent():
    first = mod._with_entry_fill
    hardening.install_trade_bucket_slippage_hardening(mod)
    assert mod._with_entry_fill is first
