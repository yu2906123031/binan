import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest


MODULE_PATH = Path(os.environ.get(
    'BINANCE_RUNTIME_CTL_PATH',
    str(Path(__file__).resolve().parents[1] / 'scripts' / 'binance_runtime_ctl.py'),
))
if not MODULE_PATH.exists():
    pytest.skip('external runtime control integration script is not installed', allow_module_level=True)
spec = importlib.util.spec_from_file_location('binance_runtime_ctl', MODULE_PATH)
assert spec is not None
assert spec.loader is not None
ctl = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = ctl
spec.loader.exec_module(ctl)


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding='utf-8')


def test_process_rows_counts_live_owned_scanner_as_active(monkeypatch):
    class FakeResult:
        stdout = '\n'.join([
            '101 1 100 /venv/bin/python3 -u /root/.hermes/scripts/binance_momentum_supervisor.py',
            '202 101 90 /venv/bin/python -u /root/binan/scripts/binance_futures_momentum_long.py --live --auto-loop',
            '303 202 10 /venv/bin/python -u /root/binan/scripts/binance_futures_momentum_long.py --live --auto-loop',
        ])

    monkeypatch.setattr(ctl.subprocess, 'run', lambda *args, **kwargs: FakeResult())

    rows = ctl.process_rows()
    assert [r['kind'] for r in rows] == ['supervisor', 'live_child', 'scanner_active']
    assert ctl.pids('scanner_deadman') == []


def test_doctor_accepts_single_websocket_owner_with_multiple_symbol_streams(tmp_path):
    runtime_state_dir = tmp_path / 'state'
    _write_json(runtime_state_dir / 'book_ticker_ws_status.json', {
        'active': True,
        'pid': 101,
        'owner_pid': 101,
        'thread_count': 1,
        'stream_count': 120,
        'symbol_count': 120,
    })
    _write_json(runtime_state_dir / 'ticker_24hr_cache_refresher_heartbeat.json', {
        'active': True,
        'pid': 101,
        'thread_count': 1,
    })

    report = ctl.build_doctor_report(
        runtime_state_dir=runtime_state_dir,
        supervisor_pids=[101],
        live_child_pids=[202],
        scanner_deadman_pids=[],
        account_state={'positions': [], 'open_orders': []},
    )

    assert report['websocket_supervisor_thread_count'] == 1
    assert report['websocket_stream_count'] == 120
    assert report['websocket_symbol_count'] == 120
    assert report['status'] == 'healthy'
    assert 'websocket_supervisor_thread_count_not_one' not in report.get('reasons', [])


def test_doctor_marks_stale_inactive_websocket_status_degraded(tmp_path):
    runtime_state_dir = tmp_path / 'state'
    _write_json(runtime_state_dir / 'book_ticker_ws_status.json', {
        'active': False,
        'pid': 999,
        'thread_count': 1,
        'stream_count': 0,
        'symbol_count': 0,
    })
    _write_json(runtime_state_dir / 'ticker_24hr_cache_refresher_heartbeat.json', {
        'active': True,
        'pid': 101,
        'thread_count': 1,
    })

    report = ctl.build_doctor_report(
        runtime_state_dir=runtime_state_dir,
        supervisor_pids=[101],
        live_child_pids=[202],
        scanner_deadman_pids=[],
        account_state={'positions': [], 'open_orders': []},
    )

    assert report['websocket_supervisor_thread_count'] == 0
    assert report['status'] == 'degraded'
    assert 'websocket_supervisor_thread_count_not_one' in report.get('reasons', [])


def test_doctor_accepts_runtime_websocket_status_without_active_flag(tmp_path):
    runtime_state_dir = tmp_path / 'state'
    _write_json(runtime_state_dir / 'book_ticker_ws_status.json', {
        'status': 'healthy',
        'symbols': ['BTCUSDT', 'ETHUSDT'],
        'symbol_count': 2,
        'active_streams': ['btcusdt@bookTicker', 'ethusdt@bookTicker'],
        'messages_processed': 42,
        'samples_written': 42,
        'last_error': '',
    })
    _write_json(runtime_state_dir / 'ticker_24hr_cache_refresher_heartbeat.json', {
        'active': True,
        'pid': 101,
        'thread_count': 1,
    })

    report = ctl.build_doctor_report(
        runtime_state_dir=runtime_state_dir,
        supervisor_pids=[101],
        live_child_pids=[202],
        scanner_deadman_pids=[],
        account_state={'positions': [], 'open_orders': []},
    )

    assert report['websocket_supervisor_thread_count'] == 1
    assert report['websocket_stream_count'] == 2
    assert report['websocket_symbol_count'] == 2
    assert report['status'] == 'healthy'
    assert 'websocket_supervisor_thread_count_not_one' not in report.get('reasons', [])

def test_status_report_combines_exchange_process_runtime_and_funnel_without_mutating_state(tmp_path):
    runtime_state_dir = tmp_path / 'state'
    _write_json(runtime_state_dir / 'last_cycle.json', {
        'updated_at': '2026-05-20T11:58:05Z',
        'cycle': {
            'funnel': {
                'raw_scan_symbol_count': 10,
                'evaluated_symbol_count': 8,
                'evaluated_side_count': 16,
                'early_filter_passed_count': 0,
                'candidate_pool_count': 0,
                'setup_ready_count': 0,
                'trigger_fired_count': 0,
                'risk_passed_count': 0,
                'candidate_count': 0,
                'ticker_24hr_cache_available': True,
                'rest_circuit_state': 'CLOSED',
                'rest_used_weight_1m': 3,
            },
            'summary_counters': {'selected_count': 0},
            'degraded': True,
            'degraded_reason': 'rest_fallback',
        },
    })
    _write_json(runtime_state_dir / 'resident_last_result.json', {
        'cycles': [{'updated_at': '2026-05-20T11:58:03Z'}],
    })
    _write_json(runtime_state_dir / 'book_ticker_ws_status.json', {
        'status': 'healthy',
        'symbol_count': 2,
        'messages_processed': 42,
        'symbols': ['BTCUSDT', 'ETHUSDT'],
    })
    _write_json(runtime_state_dir / 'ticker_24hr_cache.json', {
        'updated_at_ms': 1779278200000,
        'row_count': 597,
    })
    _write_json(runtime_state_dir / 'binance_rest_guard.json', {
        'rest_circuit_state': 'CLOSED',
        'rest_used_weight_1m': 7,
    })
    before_files = sorted(p.name for p in runtime_state_dir.iterdir())

    exchange_state = {
        'account_status_code': 200,
        'balance_status_code': 200,
        'position_risk_status_code': 200,
        'open_orders_status_code': 200,
        'wallet_balance_usdt': 83.5,
        'available_balance_usdt': 80.25,
        'position_count': 1,
        'open_order_count': 2,
        'unrealized_pnl': -0.75,
    }

    report = ctl.build_status_report(
        runtime_state_dir=runtime_state_dir,
        now=1779278285.0,
        supervisor_pids=[101],
        live_child_pids=[202],
        scanner_deadman_pids=[],
        exchange_state=exchange_state,
    )

    assert report['exchange']['account_http_200'] is True
    assert report['exchange']['wallet_balance_usdt'] == 83.5
    assert report['processes']['supervisor_count'] == 1
    assert report['processes']['websocket_supervisor_count'] == 1
    assert report['runtime_state']['websocket_status'] == 'healthy'
    assert report['runtime_state']['websocket_messages_processed'] == 42
    assert report['runtime_state']['ticker_24hr_cache_row_count'] == 597
    assert report['runtime_state']['rest_used_weight_1m'] == 7
    assert report['scan_funnel']['raw_scan_symbol_count'] == 10
    assert report['scan_funnel']['early_filter_passed_count'] == 0
    assert report['scan_funnel']['selected'] == 0
    assert report['scan_funnel']['degraded'] is True
    assert report['open_block_reason'] == 'early_filter_all_rejected'
    assert sorted(p.name for p in runtime_state_dir.iterdir()) == before_files


def test_status_report_reads_nested_cycle_scan_funnel(tmp_path):
    runtime_state_dir = tmp_path / 'state'
    _write_json(runtime_state_dir / 'last_cycle.json', {
        'updated_at': '2026-05-20T13:38:17Z',
        'cycle': {
            'scan': {
                'funnel': {
                    'raw_scan_symbol_count': 118,
                    'evaluated_symbol_count': 48,
                    'evaluated_side_count': 96,
                    'early_filter_passed_count': 64,
                    'candidate_pool_count': 52,
                    'setup_ready_count': 27,
                    'trigger_fired_count': 7,
                    'risk_passed_count': 0,
                    'selected_risk_allowed_count': 0,
                    'ticker_24hr_cache_available': True,
                },
                'summary_counters': {
                    'raw_scan_symbol_count': 118,
                    'selected_count': 1,
                    'candidate_count': 52,
                },
            },
        },
    })
    _write_json(runtime_state_dir / 'book_ticker_ws_status.json', {'status': 'healthy', 'symbol_count': 113})
    _write_json(runtime_state_dir / 'ticker_24hr_cache.json', {'row_count': 597})

    report = ctl.build_status_report(
        runtime_state_dir=runtime_state_dir,
        now=1779278285.0,
        supervisor_pids=[101],
        live_child_pids=[202],
        scanner_deadman_pids=[],
        exchange_state={'account_status_code': 200, 'balance_status_code': 200, 'position_risk_status_code': 200, 'open_orders_status_code': 200},
    )

    assert report['scan_funnel']['raw_scan_symbol_count'] == 118
    assert report['scan_funnel']['candidate_count'] == 52
    assert report['scan_funnel']['selected'] == 1
    assert report['open_block_reason'] == 'risk_rejected'


def test_status_json_and_chinese_summary_output(tmp_path, capsys):
    runtime_state_dir = tmp_path / 'state'
    _write_json(runtime_state_dir / 'last_cycle.json', {'updated_at': '2026-05-20T11:58:05Z', 'cycle': {'funnel': {'raw_scan_symbol_count': 0}}})
    _write_json(runtime_state_dir / 'book_ticker_ws_status.json', {'status': 'healthy', 'symbol_count': 0})
    _write_json(runtime_state_dir / 'ticker_24hr_cache.json', {'row_count': 0})
    report = ctl.build_status_report(
        runtime_state_dir=runtime_state_dir,
        now=1779278285.0,
        supervisor_pids=[101],
        live_child_pids=[202],
        scanner_deadman_pids=[],
        exchange_state={'account_status_code': 200, 'balance_status_code': 200, 'position_risk_status_code': 200, 'open_orders_status_code': 200},
    )

    ctl.print_status_report(report, json_output=True)
    parsed = json.loads(capsys.readouterr().out)
    assert parsed['open_block_reason'] == 'no_seed_symbols'

    ctl.print_status_report(report, json_output=False)
    out = capsys.readouterr().out
    assert '币安 U 本位合约运行状态' in out
    assert '交易所真实状态' in out
    assert '不开单原因: no_seed_symbols' in out
