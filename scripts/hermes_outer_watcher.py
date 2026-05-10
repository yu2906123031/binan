#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, cast

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNNER = ROOT / 'main.py'
DEFAULT_RUNTIME_STATE_DIR = Path.home() / '.hermes' / 'binance-futures-momentum-long' / 'runtime-state'
ENTRY_EVENTS = {'buy_fill_confirmed'}
EXIT_EVENTS = {'trade_invalidated', 'okx_position_reconciled_closed'}
EXIT_OK = 0
EXIT_MISSING_RUNNER = 2
EXIT_RUNNER_TIMEOUT = 3
EXIT_EVENTS_READ_ERROR = 4
EXIT_STATE_READ_ERROR = 5
EXIT_INTERRUPTED = 130


def _read_failure_payload(status: str, path: Path, exc: Exception, phase: str, seen_events: Optional[int] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        'ok': False,
        'status': status,
        'phase': phase,
        'path': str(path),
        'error_type': type(exc).__name__,
        'error': str(exc),
    }
    if seen_events is not None:
        payload['seen_events'] = seen_events
    return payload


def _emit_read_failure(event_type: str, status: str, path: Path, exc: Exception, phase: str, seen_events: Optional[int] = None) -> None:
    payload = _read_failure_payload(status, path, exc, phase, seen_events=seen_events)
    _emit(event_type, **payload)
    print(json.dumps(payload, ensure_ascii=False), file=sys.stderr, flush=True)


def _emit(event_type: str, **payload: Any) -> None:
    event = {
        'event_type': event_type,
        'ts': int(time.time()),
        **payload,
    }
    print(json.dumps(event, ensure_ascii=False), flush=True)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Hermes-managed outer watcher for Binance momentum live scan.')
    parser.add_argument('--runner', default=str(DEFAULT_RUNNER), help='Path to main.py entrypoint.')
    parser.add_argument('--runtime-state-dir', default=str(DEFAULT_RUNTIME_STATE_DIR), help='Runtime state directory shared with the strategy.')
    parser.add_argument('--poll-interval-sec', type=float, default=20.0, help='Seconds between live scan attempts before entry fill is confirmed.')
    parser.add_argument('--post-entry-poll-sec', type=float, default=10.0, help='Seconds between checks after entry fill while waiting for exit and cleanup.')
    parser.add_argument('--idle-exit-sec', type=float, default=180.0, help='Exit after this many seconds with no tracked positions and no fresh entry signal.')
    parser.add_argument('--max-pre-entry-cycles', type=int, default=0, help='Maximum scan cycles before entry fill. 0 keeps watching.')
    parser.add_argument('--max-post-entry-checks', type=int, default=0, help='Maximum post-entry checks while waiting for exit/cleanup. 0 keeps watching.')
    parser.add_argument('--runner-timeout-sec', type=float, default=0.0, help='Timeout for each spawned strategy run. 0 waits without a timeout.')
    parser.add_argument('--reset-events', action='store_true', help='Archive existing runtime events before starting a new watcher session.')
    parser.add_argument('--print-command', action='store_true', help='Print each spawned strategy command.')
    parser.add_argument('strategy_args', nargs=argparse.REMAINDER, help='Arguments forwarded to main.py. Include --live and any profile flags here.')
    return parser.parse_args(argv)


def _normalize_strategy_args(strategy_args: Sequence[str]) -> List[str]:
    args = list(strategy_args or [])
    if args and args[0] == '--':
        args = args[1:]
    return args


def _events_path(runtime_state_dir: Path) -> Path:
    return runtime_state_dir.expanduser() / 'events.jsonl'


def _positions_path(runtime_state_dir: Path) -> Path:
    return runtime_state_dir.expanduser() / 'positions.json'


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding='utf-8'))


def _read_events(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line:
            continue
        row = json.loads(line)
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _archive_events(path: Path) -> Optional[Path]:
    if not path.exists():
        return None
    archive = path.with_name(f"events.{int(time.time())}.jsonl")
    archive.write_text(path.read_text(encoding='utf-8'), encoding='utf-8')
    path.unlink()
    return archive


def _tracked_positions(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    positions = payload.get('positions') if isinstance(payload.get('positions'), list) else None
    if positions is None:
        positions = [value for value in payload.values() if isinstance(value, dict)]
    result: List[Dict[str, Any]] = []
    for item in positions:
        if not isinstance(item, dict):
            continue
        status = str(item.get('status') or '').lower()
        if status and status != 'closed':
            result.append(item)
    return result


def _latest_matching_event(events: Sequence[Dict[str, Any]], event_types: set[str], after_index: int = 0) -> Tuple[Optional[Dict[str, Any]], int]:
    for idx in range(max(after_index, 0), len(events)):
        row = events[idx]
        if str(row.get('event_type') or '') in event_types:
            return row, idx + 1
    return None, len(events)


def _extract_max_open_positions(forwarded: Sequence[str]) -> int:
    for idx, token in enumerate(forwarded):
        if token != '--max-open-positions':
            continue
        if idx + 1 >= len(forwarded):
            return 1
        try:
            value = int(str(forwarded[idx + 1]).strip())
        except Exception:
            return 1
        return max(value, 1)
    return 1


def _build_command(runner: Path, runtime_state_dir: Path, forwarded: Sequence[str]) -> List[str]:
    cmd = [sys.executable, str(runner), '--runtime-state-dir', str(runtime_state_dir)]
    if '--auto-loop' not in forwarded:
        cmd.append('--auto-loop')
    if '--max-scan-cycles' not in forwarded:
        cmd.extend(['--max-scan-cycles', '1'])
    cmd.extend(forwarded)
    return cmd


def _run_once(cmd: Sequence[str], print_command: bool, timeout_sec: float = 0.0) -> subprocess.CompletedProcess[str]:
    if print_command:
        _emit('watcher_run_start', command=' '.join(cmd), timeout_sec=max(timeout_sec, 0.0))
    run_kwargs: Dict[str, Any] = {'capture_output': True, 'text': True}
    if timeout_sec > 0:
        run_kwargs['timeout'] = timeout_sec
    return cast(subprocess.CompletedProcess[str], subprocess.run(cmd, **run_kwargs))


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    strategy_args = _normalize_strategy_args(args.strategy_args)
    runner = Path(args.runner).expanduser().resolve()
    runtime_state_dir = Path(args.runtime_state_dir).expanduser().resolve()
    runtime_state_dir.mkdir(parents=True, exist_ok=True)
    events_path = _events_path(runtime_state_dir)
    positions_path = _positions_path(runtime_state_dir)

    if args.reset_events:
        archived = _archive_events(events_path)
        if archived is not None:
            _emit('watcher_events_archived', archive_path=str(archived))

    forwarded = list(strategy_args)
    if not runner.exists():
        _emit('watcher_missing_runner', ok=False, status='missing_runner', runner=str(runner))
        print(json.dumps({'ok': False, 'status': 'missing_runner', 'runner': str(runner)}, ensure_ascii=False), file=sys.stderr, flush=True)
        return EXIT_MISSING_RUNNER
    cmd = _build_command(runner, runtime_state_dir, forwarded)
    try:
        seen_index = len(_read_events(events_path))
    except Exception as exc:
        _emit_read_failure('watcher_events_read_error', 'events_read_error', events_path, exc, phase='startup')
        return EXIT_EVENTS_READ_ERROR
    entry_event: Optional[Dict[str, Any]] = None
    exit_event: Optional[Dict[str, Any]] = None
    pre_entry_cycles = 0
    post_entry_checks = 0
    idle_started_at: Optional[float] = None
    target_open_positions = _extract_max_open_positions(forwarded)

    _emit(
        'watcher_started',
        runner=str(runner),
        runtime_state_dir=str(runtime_state_dir),
        poll_interval_sec=args.poll_interval_sec,
        post_entry_poll_sec=args.post_entry_poll_sec,
        idle_exit_sec=args.idle_exit_sec,
        max_pre_entry_cycles=args.max_pre_entry_cycles,
        max_post_entry_checks=args.max_post_entry_checks,
        strategy_args=forwarded,
        target_open_positions=target_open_positions,
    )

    while True:
        try:
            tracked_positions = _tracked_positions(_load_json(positions_path, {}))
        except Exception as exc:
            _emit_read_failure('watcher_state_read_error', 'state_read_error', positions_path, exc, phase='pre_entry' if entry_event is None else 'post_entry', seen_events=seen_index)
            return EXIT_STATE_READ_ERROR
        if entry_event is None:
            if args.max_pre_entry_cycles and pre_entry_cycles >= args.max_pre_entry_cycles:
                _emit('watcher_pre_entry_cycle_limit_reached', ok=True, status='pre_entry_cycle_limit_reached', pre_entry_cycles=pre_entry_cycles)
                return 0
            try:
                completed = _run_once(cmd, args.print_command, args.runner_timeout_sec)
            except KeyboardInterrupt:
                _emit(
                    'watcher_interrupted',
                    ok=False,
                    status='interrupted',
                    phase='pre_entry',
                    pre_entry_cycles=pre_entry_cycles,
                    command=' '.join(cmd),
                )
                print(
                    json.dumps(
                        {
                            'ok': False,
                            'status': 'interrupted',
                            'phase': 'pre_entry',
                            'pre_entry_cycles': pre_entry_cycles,
                            'command': ' '.join(cmd),
                        },
                        ensure_ascii=False,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
                return EXIT_INTERRUPTED
            except subprocess.TimeoutExpired as exc:
                _emit(
                    'watcher_runner_timeout',
                    ok=False,
                    status='strategy_run_timeout',
                    timeout_sec=max(args.runner_timeout_sec, 0.0),
                    pre_entry_cycles=pre_entry_cycles,
                    command=' '.join(cmd),
                )
                print(
                    json.dumps(
                        {
                            'ok': False,
                            'status': 'strategy_run_timeout',
                            'timeout_sec': max(args.runner_timeout_sec, 0.0),
                            'pre_entry_cycles': pre_entry_cycles,
                            'command': exc.cmd,
                        },
                        ensure_ascii=False,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
                return EXIT_RUNNER_TIMEOUT
            pre_entry_cycles += 1
            print(completed.stdout, end='')
            if completed.stderr:
                print(completed.stderr, file=sys.stderr, end='')
            _emit('watcher_run_finished', returncode=completed.returncode, pre_entry_cycles=pre_entry_cycles)
            if completed.returncode != 0:
                print(json.dumps({'ok': False, 'status': 'strategy_run_failed', 'returncode': completed.returncode, 'pre_entry_cycles': pre_entry_cycles}, ensure_ascii=False), file=sys.stderr, flush=True)
                return completed.returncode
            try:
                events = _read_events(events_path)
            except Exception as exc:
                _emit_read_failure('watcher_events_read_error', 'events_read_error', events_path, exc, phase='pre_entry', seen_events=seen_index)
                return EXIT_EVENTS_READ_ERROR
            entry_event, seen_index = _latest_matching_event(events, ENTRY_EVENTS, after_index=seen_index)
            try:
                tracked_positions = _tracked_positions(_load_json(positions_path, {}))
            except Exception as exc:
                _emit_read_failure('watcher_state_read_error', 'state_read_error', positions_path, exc, phase='pre_entry', seen_events=seen_index)
                return EXIT_STATE_READ_ERROR
            if entry_event is not None and len(tracked_positions) >= target_open_positions:
                _emit('watcher_entry_confirmed', ok=True, status='entry_confirmed', pre_entry_cycles=pre_entry_cycles, tracked_positions=len(tracked_positions), target_open_positions=target_open_positions, event=entry_event)
                idle_started_at = None
                continue
            if entry_event is not None:
                _emit('watcher_entry_progress', ok=True, status='entry_progress', pre_entry_cycles=pre_entry_cycles, tracked_positions=len(tracked_positions), target_open_positions=target_open_positions, event=entry_event)
                if len(tracked_positions) >= target_open_positions:
                    entry_event = dict(entry_event)
                    _emit('watcher_entry_confirmed', ok=True, status='entry_confirmed', pre_entry_cycles=pre_entry_cycles, tracked_positions=len(tracked_positions), target_open_positions=target_open_positions, event=entry_event)
                    idle_started_at = None
                    continue
                entry_event = None
                idle_started_at = None
            elif tracked_positions:
                _emit('watcher_position_open_without_entry_event', ok=True, status='position_open_without_entry_event', pre_entry_cycles=pre_entry_cycles, positions=tracked_positions)
            _emit('watcher_heartbeat', phase='pre_entry', pre_entry_cycles=pre_entry_cycles, tracked_positions=len(tracked_positions), seen_events=seen_index, target_open_positions=target_open_positions)
            time.sleep(max(args.poll_interval_sec, 0.0))
            continue

        try:
            events = _read_events(events_path)
        except Exception as exc:
            _emit_read_failure('watcher_events_read_error', 'events_read_error', events_path, exc, phase='post_entry', seen_events=seen_index)
            return EXIT_EVENTS_READ_ERROR
        exit_event, seen_index = _latest_matching_event(events, EXIT_EVENTS, after_index=seen_index)
        try:
            tracked_positions = _tracked_positions(_load_json(positions_path, {}))
        except Exception as exc:
            _emit_read_failure('watcher_state_read_error', 'state_read_error', positions_path, exc, phase='post_entry', seen_events=seen_index)
            return EXIT_STATE_READ_ERROR
        if exit_event is not None and not tracked_positions:
            _emit('watcher_exit_confirmed_and_positions_cleared', ok=True, status='exit_confirmed_and_positions_cleared', post_entry_checks=post_entry_checks, event=exit_event)
            return 0
        if tracked_positions:
            idle_started_at = None
        else:
            now = time.time()
            if idle_started_at is None:
                idle_started_at = now
            elif args.idle_exit_sec > 0 and now - idle_started_at >= args.idle_exit_sec:
                _emit('watcher_idle_cleanup_confirmed', ok=True, status='idle_cleanup_confirmed', post_entry_checks=post_entry_checks, last_exit_event=exit_event)
                return 0
        post_entry_checks += 1
        if args.max_post_entry_checks and post_entry_checks >= args.max_post_entry_checks:
            _emit('watcher_post_entry_check_limit_reached', ok=True, status='post_entry_check_limit_reached', post_entry_checks=post_entry_checks, tracked_positions=tracked_positions, last_exit_event=exit_event)
            return 0
        idle_seconds = None if idle_started_at is None else max(0.0, time.time() - idle_started_at)
        _emit('watcher_heartbeat', phase='post_entry', post_entry_checks=post_entry_checks, tracked_positions=len(tracked_positions), seen_events=seen_index, idle_seconds=idle_seconds, has_exit_event=exit_event is not None)
        time.sleep(max(args.post_entry_poll_sec, 0.0))


if __name__ == '__main__':
    raise SystemExit(main())
