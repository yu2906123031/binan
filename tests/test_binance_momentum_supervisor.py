import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest


MODULE_PATH = Path(os.environ.get(
    'BINANCE_MOMENTUM_SUPERVISOR_PATH',
    str(Path(__file__).resolve().parents[1] / 'scripts' / 'binance_momentum_supervisor.py'),
))
if not MODULE_PATH.exists():
    pytest.skip('external supervisor integration script is not installed', allow_module_level=True)
spec = importlib.util.spec_from_file_location('binance_momentum_supervisor', MODULE_PATH)
assert spec is not None
assert spec.loader is not None
supervisor = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = supervisor
spec.loader.exec_module(supervisor)


class Spawned(RuntimeError):
    pass


@pytest.fixture
def isolated_supervisor_files(tmp_path, monkeypatch):
    bot_script = tmp_path / 'main.py'
    bot_script.write_text('print("ok")\n', encoding='utf-8')
    square_symbols = tmp_path / 'square-symbols.txt'
    square_symbols.write_text('ZROUSDT\n', encoding='utf-8')
    external_signal = tmp_path / 'external-signal.json'
    external_signal.write_text(json.dumps({'signal_map': {'ZROUSDT': {'score': 0.0}}}), encoding='utf-8')
    runtime_state_dir = tmp_path / 'runtime-state'
    runtime_state_dir.mkdir()
    positions_json = runtime_state_dir / 'positions.json'
    monkeypatch.setattr(supervisor, 'BOT_SCRIPT', bot_script)
    monkeypatch.setattr(supervisor, 'SQUARE_SYMBOLS_FILE', square_symbols)
    monkeypatch.setattr(supervisor, 'EXTERNAL_SIGNAL_JSON', external_signal)
    monkeypatch.setattr(supervisor, 'RUNTIME_STATE_DIR', runtime_state_dir, raising=False)
    monkeypatch.setattr(supervisor, 'POSITIONS_STATE_FILE', positions_json, raising=False)
    monkeypatch.setattr(supervisor, 'HALT_MARKER_FILE', runtime_state_dir / 'supervisor_halt.json', raising=False)
    monkeypatch.setattr(supervisor, 'SINGLE_INSTANCE_LOCK_FILE', runtime_state_dir / 'supervisor.lock', raising=False)
    monkeypatch.setattr(supervisor, 'SINGLE_INSTANCE_LOCK_HANDLE', None, raising=False)
    monkeypatch.setattr(supervisor, 'SHUTDOWN_REQUESTED', False, raising=False)
    monkeypatch.setattr(supervisor, 'POLL_INTERVAL', 0)
    monkeypatch.setattr(supervisor, 'RESTART_DELAY', 0)
    monkeypatch.setattr(supervisor, 'load_env', lambda _path: None)
    return positions_json


def test_main_starts_child_for_recoverable_runtime_position(monkeypatch, isolated_supervisor_files):
    positions_json = isolated_supervisor_files
    positions_json.write_text(
        json.dumps(
            {
                'ZROUSDT:SHORT': {
                    'symbol': 'ZROUSDT',
                    'position_side': 'SHORT',
                    'status': 'recovery_pending',
                    'remaining_quantity': 14.9,
                    'trade_management_plan': {'entry_price': 1.33, 'stop_price': 1.34},
                }
            }
        ),
        encoding='utf-8',
    )

    monkeypatch.setattr(
        supervisor,
        'fetch_account_state',
        lambda: {
            'positions': [{'symbol': 'ZROUSDT', 'positionAmt': '-14.9', 'entryPrice': '1.33'}],
            'risk_positions': [{'symbol': 'ZROUSDT', 'positionAmt': '-14.9', 'entryPrice': '1.33'}],
            'position_mismatch': False,
            'open_orders': [],
            'ignored_open_orders': [],
        },
    )

    recorded = {}

    def fake_popen(cmd, **kwargs):
        recorded['cmd'] = cmd
        recorded['kwargs'] = kwargs
        raise Spawned('spawned child')

    monkeypatch.setattr(supervisor.subprocess, 'Popen', fake_popen)
    monkeypatch.setattr(supervisor.time, 'sleep', lambda _seconds: (_ for _ in ()).throw(AssertionError('should spawn child before sleeping')))

    with pytest.raises(Spawned):
        supervisor.main()

    assert recorded['cmd'][0] == supervisor.resolve_strategy_python()
    assert recorded['cmd'][2] == str(supervisor.BOT_SCRIPT)


def test_main_starts_child_for_reconciled_runtime_position(monkeypatch, isolated_supervisor_files):
    positions_json = isolated_supervisor_files
    positions_json.write_text(
        json.dumps(
            {
                'IOUSDT:SHORT': {
                    'symbol': 'IOUSDT',
                    'position_side': 'SHORT',
                    'status': 'reconciled',
                    'remaining_quantity': 49.0,
                    'entry_price': 0.1497,
                }
            }
        ),
        encoding='utf-8',
    )

    monkeypatch.setattr(
        supervisor,
        'fetch_account_state',
        lambda: {
            'positions': [{'symbol': 'IOUSDT', 'positionAmt': '-49', 'entryPrice': '0.1497'}],
            'risk_positions': [{'symbol': 'IOUSDT', 'positionAmt': '-49', 'entryPrice': '0.1497'}],
            'position_mismatch': False,
            'open_orders': [],
            'ignored_open_orders': [],
        },
    )

    recorded = {}

    def fake_popen(cmd, **kwargs):
        recorded['cmd'] = cmd
        raise Spawned('spawned child')

    monkeypatch.setattr(supervisor.subprocess, 'Popen', fake_popen)
    monkeypatch.setattr(supervisor.time, 'sleep', lambda _seconds: (_ for _ in ()).throw(AssertionError('should spawn child before sleeping')))

    with pytest.raises(Spawned):
        supervisor.main()

    assert recorded['cmd'][0] == supervisor.resolve_strategy_python()


def test_main_blocks_when_runtime_state_cannot_recover_account_position(monkeypatch, isolated_supervisor_files, capsys):
    positions_json = isolated_supervisor_files
    positions_json.write_text(json.dumps({'positions': []}), encoding='utf-8')

    monkeypatch.setattr(
        supervisor,
        'fetch_account_state',
        lambda: {
            'positions': [{'symbol': 'ZROUSDT', 'positionAmt': '-14.9', 'entryPrice': '1.33'}],
            'risk_positions': [{'symbol': 'ZROUSDT', 'positionAmt': '-14.9', 'entryPrice': '1.33'}],
            'position_mismatch': False,
            'open_orders': [],
            'ignored_open_orders': [],
        },
    )

    monkeypatch.setattr(supervisor.subprocess, 'Popen', lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError('should stay blocked')))

    def stop_sleep(_seconds):
        raise KeyboardInterrupt('stop after first blocked poll')

    monkeypatch.setattr(supervisor.time, 'sleep', stop_sleep)

    with pytest.raises(KeyboardInterrupt):
        supervisor.main()

    captured = capsys.readouterr()
    assert '"reason": "account_not_flat"' in captured.out


def test_build_command_uses_live_entry_relaxation_thresholds(monkeypatch, isolated_supervisor_files):
    cmd = supervisor.build_command()

    assert cmd[cmd.index('--trigger-relax-min-score') + 1] == '78'
    assert cmd[cmd.index('--trigger-relax-min-points') + 1] == '3'
    assert cmd[cmd.index('--execution-slippage-hard-veto-r') + 1] == '450'
    assert cmd[cmd.index('--execution-slippage-risk-threshold-r') + 1] == '450'


def test_build_command_forwards_bounded_post_only_taker_fallback(monkeypatch, isolated_supervisor_files):
    config_path = isolated_supervisor_files.parent / 'runtime-config.json'
    config_path.write_text(json.dumps({
        'allow_post_only_taker_fallback': True,
        'maker_post_only_fallback_min_score': 92,
        'maker_post_only_fallback_min_liquidity_grade': 'A',
        'maker_post_only_fallback_max_spread_bps': 6,
    }), encoding='utf-8')
    monkeypatch.setattr(supervisor, 'RUNTIME_CONFIG_FILE', config_path)

    cmd = supervisor.build_command()

    assert '--allow-post-only-taker-fallback' in cmd
    assert cmd[cmd.index('--maker-post-only-fallback-min-score') + 1] == '92'
    assert cmd[cmd.index('--maker-post-only-fallback-min-liquidity-grade') + 1] == 'A'
    assert cmd[cmd.index('--maker-post-only-fallback-max-spread-bps') + 1] == '6'


def test_build_command_uses_current_five_usdt_runtime_defaults(monkeypatch, isolated_supervisor_files):
    cmd = supervisor.build_command()

    assert cmd[cmd.index('--profile') + 1] == 'five-usdt-scalp-v2'
    assert cmd[cmd.index('--target-notional-usdt') + 1] == '80.0'
    assert cmd[cmd.index('--min-notional-usdt') + 1] == '60.0'
    assert cmd[cmd.index('--max-notional-usdt') + 1] == '90.0'
    assert cmd[cmd.index('--tp1-profit-usdt') + 1] == '1.5'
    assert cmd[cmd.index('--tp2-profit-usdt') + 1] == '3.0'
    assert cmd[cmd.index('--tp1-close-pct') + 1] == '0.4'
    assert cmd[cmd.index('--tp2-close-pct') + 1] == '0.4'
    assert cmd[cmd.index('--max-open-positions') + 1] == '3'
    assert cmd[cmd.index('--max-long-positions') + 1] == '3'
    assert cmd[cmd.index('--max-short-positions') + 1] == '3'
    assert cmd[cmd.index('--min-target-net-profit-usdt') + 1] == '0.1'
    assert cmd[cmd.index('--max-loss-usdt') + 1] == '1.5'
    assert cmd[cmd.index('--min-expected-rr') + 1] == '0.7'
    assert cmd[cmd.index('--trigger-min-confirmations') + 1] == '1'
    assert cmd[cmd.index('--micro-scalp-time-stop-sec') + 1] == '1800'
    assert cmd[cmd.index('--daily-max-loss-usdt') + 1] == '4.0'
    assert cmd[cmd.index('--max-consecutive-losses') + 1] == '2'
    assert cmd[cmd.index('--consecutive-loss-pause-minutes') + 1] == '180'
    assert cmd[cmd.index('--leverage') + 1] == '10'
    assert cmd[cmd.index('--margin-type') + 1] == 'ISOLATED'


def test_build_command_allows_runtime_risk_and_exit_overrides(monkeypatch, isolated_supervisor_files):
    cmd = supervisor.build_command({
        'risk_usdt': 1.2,
        'target_notional_usdt': 75.0,
        'min_notional_usdt': 55.0,
        'max_notional_usdt': 85.0,
        'tp1_profit_usdt': 1.2,
        'tp2_profit_usdt': 2.8,
        'tp1_close_pct': 0.5,
        'tp2_close_pct': 0.3,
        'leverage': 8,
        'margin_type': 'ISOLATED',
        'trigger_relax_min_score': 78,
        'trigger_relax_min_points': 3,
    })

    expected = {
        '--risk-usdt': '1.2',
        '--target-notional-usdt': '75.0',
        '--min-notional-usdt': '55.0',
        '--max-notional-usdt': '85.0',
        '--tp1-profit-usdt': '1.2',
        '--tp2-profit-usdt': '2.8',
        '--tp1-close-pct': '0.5',
        '--tp2-close-pct': '0.3',
        '--leverage': '8',
        '--margin-type': 'ISOLATED',
        '--trigger-relax-min-score': '78',
        '--trigger-relax-min-points': '3',
    }
    for flag, value in expected.items():
        assert cmd[cmd.index(flag) + 1] == value


def test_build_command_gives_live_execution_enough_deadman_budget(monkeypatch, isolated_supervisor_files):
    cmd = supervisor.build_command()

    assert float(cmd[cmd.index('--execution-timeout-seconds') + 1]) >= 90.0


def test_build_command_enables_kline_rest_fallback_when_scanner_rest_fallback_is_enabled(monkeypatch, isolated_supervisor_files):
    cmd = supervisor.build_command({'scanner_rest_fallback': True})

    assert '--scanner-rest-fallback' in cmd
    assert '--scanner-kline-rest-fallback' in cmd
    assert cmd[cmd.index('--scanner-kline-rest-fallback-min-interval-seconds') + 1] == '0'


def test_classify_child_exit_treats_sigterm_as_clean_stop(monkeypatch):
    monkeypatch.setattr(supervisor, 'SHUTDOWN_REQUESTED', False, raising=False)

    decision = supervisor.classify_child_exit(return_code=-supervisor.signal.SIGTERM, consecutive_failures=1)

    assert decision['action'] == 'exit'
    assert decision['reason'] == 'shutdown_requested'


def test_temporary_ban_writes_halt_marker_and_backoff(monkeypatch, isolated_supervisor_files, capsys):
    positions_json = isolated_supervisor_files
    ban_until_ms = int(supervisor.time.time() * 1000) + 120_000
    positions_json.write_text('{}', encoding='utf-8')
    monkeypatch.setattr(
        supervisor,
        'fetch_account_state',
        lambda: (_ for _ in ()).throw(supervisor.BinanceTemporaryBan('banned until 1779008201172', ban_until_ms=ban_until_ms)),
    )
    monkeypatch.setattr(supervisor.subprocess, 'Popen', lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError('should stay blocked')))

    def stop_sleep(seconds):
        assert seconds >= 60
        raise KeyboardInterrupt('stop after ban backoff')

    monkeypatch.setattr(supervisor.time, 'sleep', stop_sleep)

    with pytest.raises(KeyboardInterrupt):
        supervisor.main()

    marker = supervisor.HALT_MARKER_FILE
    payload = json.loads(marker.read_text(encoding='utf-8'))
    assert payload['reason'] == 'binance_temporary_ban'
    assert payload['ban_until_ms'] == ban_until_ms
    captured = capsys.readouterr()
    assert '"reason": "binance_temporary_ban"' in captured.out
