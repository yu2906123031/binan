import importlib.util
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'trade_bucket_analysis.py'
spec = importlib.util.spec_from_file_location('trade_bucket_analysis_direction_test', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


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
    assert closed['slippage_error_bps'] == -15.0


def test_short_adverse_fallback_slippage_is_positive():
    closed = _closed_fill('SHORT', 99.90)
    assert closed['actual_fill_slippage_bps'] == 10.0


def test_short_favorable_fallback_slippage_is_negative():
    closed = _closed_fill('SHORT', 100.10)
    assert closed['actual_fill_slippage_bps'] == -10.0
    assert closed['slippage_error_bps'] == -15.0
