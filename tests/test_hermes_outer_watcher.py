import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'hermes_outer_watcher.py'
spec = importlib.util.spec_from_file_location('hermes_outer_watcher', MODULE_PATH)
watcher = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = watcher
spec.loader.exec_module(watcher)


def test_build_command_preserves_forwarded_max_open_positions(tmp_path):
    runner = tmp_path / 'main.py'
    runtime_state_dir = tmp_path / 'state'
    cmd = watcher._build_command(runner, runtime_state_dir, ['--live', '--profile', 'default', '--max-open-positions', '3'])
    assert cmd.count('--max-open-positions') == 1
    idx = cmd.index('--max-open-positions')
    assert cmd[idx + 1] == '3'
    assert '--max-scan-cycles' in cmd
    assert cmd[cmd.index('--max-scan-cycles') + 1] == '1'


def test_parse_args_accepts_runner_timeout_sec():
    args = watcher.parse_args(['--runner-timeout-sec', '12'])
    assert args.runner_timeout_sec == 12.0


def test_run_once_passes_timeout_to_subprocess(monkeypatch):
    recorded = {}

    class Completed:
        returncode = 0
        stdout = ''
        stderr = ''

    def fake_run(cmd, capture_output, text, timeout):
        recorded['cmd'] = cmd
        recorded['capture_output'] = capture_output
        recorded['text'] = text
        recorded['timeout'] = timeout
        return Completed()

    monkeypatch.setattr(watcher.subprocess, 'run', fake_run)
    completed = watcher._run_once(['python', 'main.py'], print_command=False, timeout_sec=9.5)

    assert completed.returncode == 0
    assert recorded == {
        'cmd': ['python', 'main.py'],
        'capture_output': True,
        'text': True,
        'timeout': 9.5,
    }


def test_main_emits_timeout_event_and_returns_timeout_exit_code(tmp_path, monkeypatch, capsys):
    runtime_state_dir = tmp_path / 'state'
    runtime_state_dir.mkdir()
    runner = tmp_path / 'main.py'
    runner.write_text('')

    def fake_run_once(cmd, print_command, timeout_sec):
        raise subprocess.TimeoutExpired(cmd=list(cmd), timeout=timeout_sec)

    monkeypatch.setattr(watcher, '_run_once', fake_run_once)

    exit_code = watcher.main([
        '--runner', str(runner),
        '--runtime-state-dir', str(runtime_state_dir),
        '--poll-interval-sec', '0',
        '--runner-timeout-sec', '7',
        '--', '--live', '--profile', 'default'
    ])

    assert exit_code == watcher.EXIT_RUNNER_TIMEOUT
    captured = capsys.readouterr()
    assert '"event_type": "watcher_runner_timeout"' in captured.out
    assert '"timeout_sec": 7.0' in captured.out
    assert '"status": "strategy_run_timeout"' in captured.err
    assert '"timeout_sec": 7.0' in captured.err


def test_main_returns_missing_runner_exit_code_without_spawning(tmp_path, monkeypatch, capsys):
    runtime_state_dir = tmp_path / 'state'
    runtime_state_dir.mkdir()
    missing_runner = tmp_path / 'missing_main.py'
    spawned = {'called': False}

    def fake_run_once(*args, **kwargs):
        spawned['called'] = True
        raise AssertionError('should not spawn runner when file is missing')

    monkeypatch.setattr(watcher, '_run_once', fake_run_once)

    exit_code = watcher.main([
        '--runner', str(missing_runner),
        '--runtime-state-dir', str(runtime_state_dir),
        '--poll-interval-sec', '0',
        '--', '--live', '--profile', 'default'
    ])

    assert exit_code == watcher.EXIT_MISSING_RUNNER
    assert spawned['called'] is False
    captured = capsys.readouterr()
    assert '"event_type": "watcher_missing_runner"' in captured.out
    assert '"status": "missing_runner"' in captured.err


def test_main_returns_interrupted_exit_code_when_runner_is_cancelled(tmp_path, monkeypatch, capsys):
    runtime_state_dir = tmp_path / 'state'
    runtime_state_dir.mkdir()
    runner = tmp_path / 'main.py'
    runner.write_text('')

    def fake_run_once(cmd, print_command, timeout_sec):
        raise KeyboardInterrupt()

    monkeypatch.setattr(watcher, '_run_once', fake_run_once)

    exit_code = watcher.main([
        '--runner', str(runner),
        '--runtime-state-dir', str(runtime_state_dir),
        '--poll-interval-sec', '0',
        '--runner-timeout-sec', '7',
        '--', '--live', '--profile', 'default'
    ])

    assert exit_code == watcher.EXIT_INTERRUPTED
    captured = capsys.readouterr()
    assert '"event_type": "watcher_interrupted"' in captured.out
    assert '"status": "interrupted"' in captured.err


def test_main_emits_state_read_error_for_malformed_positions_json(tmp_path, monkeypatch, capsys):
    runtime_state_dir = tmp_path / 'state'
    runtime_state_dir.mkdir()
    positions_path = runtime_state_dir / 'positions.json'
    positions_path.write_text('{bad json', encoding='utf-8')
    runner = tmp_path / 'main.py'
    runner.write_text('')

    def fake_run_once(*_args, **_kwargs):
        raise AssertionError('watcher should fail before spawning runner when positions.json is malformed')

    monkeypatch.setattr(watcher, '_run_once', fake_run_once)

    exit_code = watcher.main([
        '--runner', str(runner),
        '--runtime-state-dir', str(runtime_state_dir),
        '--poll-interval-sec', '0',
        '--', '--live', '--profile', 'default'
    ])

    assert exit_code == watcher.EXIT_STATE_READ_ERROR
    captured = capsys.readouterr()
    assert '"event_type": "watcher_state_read_error"' in captured.out
    assert '"status": "state_read_error"' in captured.err
    assert 'positions.json' in captured.out
    assert 'positions.json' in captured.err


def test_main_emits_events_read_error_for_malformed_events_jsonl(tmp_path, monkeypatch, capsys):
    runtime_state_dir = tmp_path / 'state'
    runtime_state_dir.mkdir()
    positions_path = runtime_state_dir / 'positions.json'
    events_path = runtime_state_dir / 'events.jsonl'
    positions_path.write_text(json.dumps({'positions': []}), encoding='utf-8')
    events_path.write_text('{bad jsonl\n', encoding='utf-8')
    runner = tmp_path / 'main.py'
    runner.write_text('')

    def fake_run_once(*_args, **_kwargs):
        raise AssertionError('watcher should fail before spawning runner when events.jsonl is malformed')

    monkeypatch.setattr(watcher, '_run_once', fake_run_once)

    exit_code = watcher.main([
        '--runner', str(runner),
        '--runtime-state-dir', str(runtime_state_dir),
        '--poll-interval-sec', '0',
        '--', '--live', '--profile', 'default'
    ])

    assert exit_code == watcher.EXIT_EVENTS_READ_ERROR
    captured = capsys.readouterr()
    assert '"event_type": "watcher_events_read_error"' in captured.out
    assert '"status": "events_read_error"' in captured.err
    assert 'events.jsonl' in captured.out
    assert 'events.jsonl' in captured.err


def test_watcher_stays_pre_entry_until_reaching_max_open_positions(tmp_path, monkeypatch, capsys):
    runtime_state_dir = tmp_path / 'state'
    runtime_state_dir.mkdir()
    events_path = runtime_state_dir / 'events.jsonl'
    positions_path = runtime_state_dir / 'positions.json'
    runner = tmp_path / 'main.py'
    runner.write_text('')

    run_calls = []
    snapshots = [
        {'positions': [{'symbol': 'AAAUSDT', 'status': 'monitoring'}]},
        {'positions': [
            {'symbol': 'AAAUSDT', 'status': 'monitoring'},
            {'symbol': 'BBBUSDT', 'status': 'monitoring'},
            {'symbol': 'CCCUSDT', 'status': 'monitoring'},
        ]},
        {'positions': []},
    ]
    event_batches = [
        [{'event_type': 'buy_fill_confirmed', 'symbol': 'AAAUSDT'}],
        [
            {'event_type': 'buy_fill_confirmed', 'symbol': 'AAAUSDT'},
            {'event_type': 'buy_fill_confirmed', 'symbol': 'BBBUSDT'},
            {'event_type': 'buy_fill_confirmed', 'symbol': 'CCCUSDT'},
        ],
        [
            {'event_type': 'buy_fill_confirmed', 'symbol': 'AAAUSDT'},
            {'event_type': 'buy_fill_confirmed', 'symbol': 'BBBUSDT'},
            {'event_type': 'buy_fill_confirmed', 'symbol': 'CCCUSDT'},
            {'event_type': 'trade_invalidated', 'symbol': 'AAAUSDT'},
        ],
    ]

    def write_events(rows):
        events_path.write_text('\n'.join(json.dumps(row) for row in rows) + ('\n' if rows else ''), encoding='utf-8')

    positions_path.write_text(json.dumps({'positions': []}), encoding='utf-8')
    write_events([])

    class Completed:
        def __init__(self):
            self.returncode = 0
            self.stdout = ''
            self.stderr = ''

    def fake_run_once(cmd, print_command, timeout_sec=0.0):
        call_index = len(run_calls)
        run_calls.append(cmd)
        positions_path.write_text(json.dumps(snapshots[call_index]), encoding='utf-8')
        write_events(event_batches[call_index])
        return Completed()

    sleep_calls = []

    def fake_sleep(seconds):
        sleep_calls.append(seconds)
        if len(run_calls) == 2 and positions_path.exists():
            positions_path.write_text(json.dumps(snapshots[2]), encoding='utf-8')
            write_events(event_batches[2])

    monkeypatch.setattr(watcher, '_run_once', fake_run_once)
    monkeypatch.setattr(watcher.time, 'sleep', fake_sleep)

    exit_code = watcher.main([
        '--runner', str(runner),
        '--runtime-state-dir', str(runtime_state_dir),
        '--poll-interval-sec', '0',
        '--post-entry-poll-sec', '0',
        '--', '--live', '--profile', 'default', '--max-open-positions', '3'
    ])

    assert exit_code == 0
    assert len(run_calls) == 2
    output = capsys.readouterr().out
    assert '"phase": "pre_entry"' in output
    assert '"tracked_positions": 1' in output
    assert '"status": "entry_progress"' in output
    assert '"status": "entry_confirmed"' in output
    assert '"tracked_positions": 3' in output
    assert '"status": "exit_confirmed_and_positions_cleared"' in output


def test_watcher_promotes_orphan_positions_to_entry_confirmed_when_target_already_reached(tmp_path, monkeypatch, capsys):
    runtime_state_dir = tmp_path / 'state'
    runtime_state_dir.mkdir()
    events_path = runtime_state_dir / 'events.jsonl'
    positions_path = runtime_state_dir / 'positions.json'
    runner = tmp_path / 'main.py'
    runner.write_text('')

    positions_path.write_text(json.dumps({
        'AAAUSDT:LONG': {'symbol': 'AAAUSDT', 'status': 'orphan'},
        'BBBUSDT:LONG': {'symbol': 'BBBUSDT', 'status': 'orphan'},
    }), encoding='utf-8')
    events_path.write_text('', encoding='utf-8')

    class Completed:
        def __init__(self):
            self.returncode = 0
            self.stdout = ''
            self.stderr = ''

    run_calls = []

    def fake_run_once(cmd, print_command, timeout_sec=0.0):
        run_calls.append(cmd)
        events = [
            {'event_type': 'buy_fill_confirmed', 'symbol': 'AAAUSDT'},
        ]
        events_path.write_text('\n'.join(json.dumps(row) for row in events) + '\n', encoding='utf-8')
        return Completed()

    def fake_sleep(_seconds):
        positions_path.write_text(json.dumps({'positions': []}), encoding='utf-8')
        events = [
            {'event_type': 'buy_fill_confirmed', 'symbol': 'AAAUSDT'},
            {'event_type': 'trade_invalidated', 'symbol': 'AAAUSDT'},
        ]
        events_path.write_text('\n'.join(json.dumps(row) for row in events) + '\n', encoding='utf-8')

    monkeypatch.setattr(watcher, '_run_once', fake_run_once)
    monkeypatch.setattr(watcher.time, 'sleep', fake_sleep)

    exit_code = watcher.main([
        '--runner', str(runner),
        '--runtime-state-dir', str(runtime_state_dir),
        '--poll-interval-sec', '0',
        '--post-entry-poll-sec', '0',
        '--', '--live', '--profile', 'default', '--max-open-positions', '2'
    ])

    assert exit_code == 0
    assert len(run_calls) == 1
    output = capsys.readouterr().out
    assert '"status": "entry_confirmed"' in output
    assert '"tracked_positions": 2' in output
    assert '"phase": "post_entry"' in output
    assert '"status": "exit_confirmed_and_positions_cleared"' in output
