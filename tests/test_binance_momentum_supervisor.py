import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


MODULE_PATH = Path('/root/.hermes/scripts/binance_momentum_supervisor.py')
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
