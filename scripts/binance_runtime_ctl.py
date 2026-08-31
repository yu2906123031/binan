from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Iterable


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


def _parse_time(value: Any) -> dt.datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def process_rows() -> list[dict[str, Any]]:
    try:
        proc = subprocess.run(
            ['ps', '-eo', 'pid=,ppid=,etimes=,args='],
            check=False,
            capture_output=True,
            text=True,
        )
        lines = proc.stdout.splitlines()
    except Exception:
        return []
    raw: list[dict[str, Any]] = []
    for line in lines:
        parts = line.strip().split(None, 3)
        if len(parts) < 4:
            continue
        try:
            pid, ppid, etimes = map(int, parts[:3])
        except ValueError:
            continue
        cmd = parts[3]
        kind = ''
        if 'binance_momentum_supervisor.py' in cmd:
            kind = 'supervisor'
        elif 'binance_futures_momentum_long.py' in cmd and '--live' in cmd and '--auto-loop' in cmd:
            kind = 'live_child'
        if kind:
            raw.append({'pid': pid, 'ppid': ppid, 'etimes': etimes, 'cmd': cmd, 'kind': kind})
    live_pids = {row['pid'] for row in raw if row['kind'] == 'live_child'}
    for row in raw:
        if row['kind'] == 'live_child' and row['ppid'] in live_pids:
            row['kind'] = 'scanner_active'
    return raw


def pids(kind: str) -> list[int]:
    rows = process_rows()
    if kind == 'scanner_deadman':
        active = {row['pid'] for row in rows if row['kind'] == 'scanner_active'}
        return [row['pid'] for row in rows if 'scanner_deadman' in row['cmd'] and row['pid'] not in active]
    return [row['pid'] for row in rows if row['kind'] == kind]


def _ws_counts(payload: dict[str, Any], *, now: dt.datetime | None = None, max_age_seconds: float = 90.0) -> tuple[int, int, int, str]:
    now = now or dt.datetime.now(dt.timezone.utc)
    status = str(payload.get('status') or '').lower()
    updated = _parse_time(payload.get('last_message_at') or payload.get('last_sample_at') or payload.get('updated_at'))
    stale = updated is not None and (now - updated).total_seconds() > max_age_seconds
    explicit_active = payload.get('active')
    inferred_active = status == 'healthy' and bool(payload.get('symbols') or payload.get('active_streams') or payload.get('symbol_count'))
    active = bool(explicit_active) if explicit_active is not None else inferred_active
    if stale:
        active = False
    threads = int(payload.get('thread_count') or (1 if active else 0)) if active else 0
    streams = int(payload.get('stream_count') or len(payload.get('active_streams') or []))
    symbols = int(payload.get('symbol_count') or len(payload.get('symbols') or []))
    return threads, streams, symbols, ('stale' if stale else status or ('healthy' if active else 'unknown'))


def build_doctor_report(
    *,
    runtime_state_dir: Path,
    supervisor_pids: Iterable[int],
    live_child_pids: Iterable[int],
    scanner_deadman_pids: Iterable[int],
    account_state: dict[str, Any],
) -> dict[str, Any]:
    state_dir = Path(runtime_state_dir)
    ws = _read_json(state_dir / 'book_ticker_ws_status.json', {}) or {}
    threads, streams, symbols, ws_status = _ws_counts(ws)
    reasons: list[str] = []
    if len(list(supervisor_pids)) != 1:
        reasons.append('supervisor_count_not_one')
    if len(list(live_child_pids)) != 1:
        reasons.append('live_child_count_not_one')
    if list(scanner_deadman_pids):
        reasons.append('scanner_deadman_present')
    if threads != 1:
        reasons.append('websocket_supervisor_thread_count_not_one')
    positions = account_state.get('positions') or []
    open_orders = account_state.get('open_orders') or []
    return {
        'status': 'healthy' if not reasons else 'degraded',
        'reasons': reasons,
        'websocket_supervisor_thread_count': threads,
        'websocket_stream_count': streams,
        'websocket_symbol_count': symbols,
        'websocket_status': ws_status,
        'position_count': len(positions) if isinstance(positions, list) else 0,
        'open_order_count': len(open_orders) if isinstance(open_orders, list) else 0,
    }


def _scan_payload(last_cycle: dict[str, Any]) -> dict[str, Any]:
    cycle = last_cycle.get('cycle') if isinstance(last_cycle, dict) else {}
    cycle = cycle if isinstance(cycle, dict) else {}
    scan = cycle.get('scan') if isinstance(cycle.get('scan'), dict) else cycle
    funnel = scan.get('funnel') if isinstance(scan, dict) and isinstance(scan.get('funnel'), dict) else {}
    summary = scan.get('summary_counters') if isinstance(scan, dict) and isinstance(scan.get('summary_counters'), dict) else {}
    result = dict(funnel)
    result['candidate_count'] = int(funnel.get('candidate_count') or funnel.get('candidate_pool_count') or summary.get('candidate_count') or 0)
    result['selected'] = int(funnel.get('selected_risk_allowed_count') or summary.get('selected_count') or 0)
    result['degraded'] = bool(scan.get('degraded') if isinstance(scan, dict) else cycle.get('degraded'))
    return result


def _open_block_reason(funnel: dict[str, Any]) -> str:
    if int(funnel.get('raw_scan_symbol_count') or 0) <= 0:
        return 'no_seed_symbols'
    if int(funnel.get('early_filter_passed_count') or 0) <= 0:
        return 'early_filter_all_rejected'
    if int(funnel.get('trigger_fired_count') or 0) > 0 and int(funnel.get('risk_passed_count') or funnel.get('selected_risk_allowed_count') or 0) <= 0:
        return 'risk_rejected'
    if int(funnel.get('candidate_count') or 0) <= 0:
        return 'no_candidates'
    return ''


def build_status_report(
    *,
    runtime_state_dir: Path,
    now: float | None = None,
    supervisor_pids: Iterable[int],
    live_child_pids: Iterable[int],
    scanner_deadman_pids: Iterable[int],
    exchange_state: dict[str, Any],
) -> dict[str, Any]:
    state_dir = Path(runtime_state_dir)
    last_cycle = _read_json(state_dir / 'last_cycle.json', {}) or {}
    ws = _read_json(state_dir / 'book_ticker_ws_status.json', {}) or {}
    ticker = _read_json(state_dir / 'ticker_24hr_cache.json', {}) or {}
    rest = _read_json(state_dir / 'binance_rest_guard.json', {}) or {}
    current = dt.datetime.fromtimestamp(now, dt.timezone.utc) if now is not None else dt.datetime.now(dt.timezone.utc)
    threads, streams, symbols, ws_status = _ws_counts(ws, now=current)
    funnel = _scan_payload(last_cycle)
    exchange = dict(exchange_state or {})
    exchange['account_http_200'] = all(int(exchange.get(key) or 0) == 200 for key in (
        'account_status_code', 'balance_status_code', 'position_risk_status_code', 'open_orders_status_code'
    ))
    return {
        'exchange': exchange,
        'processes': {
            'supervisor_count': len(list(supervisor_pids)),
            'live_child_count': len(list(live_child_pids)),
            'scanner_deadman_count': len(list(scanner_deadman_pids)),
            'websocket_supervisor_count': threads,
        },
        'runtime_state': {
            'websocket_status': ws_status,
            'websocket_messages_processed': int(ws.get('messages_processed') or 0),
            'websocket_stream_count': streams,
            'websocket_symbol_count': symbols,
            'ticker_24hr_cache_row_count': int(ticker.get('row_count') or 0),
            'rest_used_weight_1m': int(rest.get('rest_used_weight_1m') or 0),
        },
        'scan_funnel': funnel,
        'open_block_reason': _open_block_reason(funnel),
    }


def print_status_report(report: dict[str, Any], *, json_output: bool = False) -> None:
    if json_output:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print('币安 U 本位合约运行状态')
    print('交易所真实状态')
    print(f"进程: supervisor={report.get('processes', {}).get('supervisor_count', 0)} live={report.get('processes', {}).get('live_child_count', 0)}")
    print(f"不开单原因: {report.get('open_block_reason') or '-'}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('command', nargs='?', default='status', choices=['status', 'doctor'])
    parser.add_argument('--runtime-state-dir', default=os.path.expanduser('~/.hermes/binance-futures-momentum-long/runtime-state'))
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()
    rows = process_rows()
    state_dir = Path(args.runtime_state_dir)
    if args.command == 'doctor':
        report = build_doctor_report(
            runtime_state_dir=state_dir,
            supervisor_pids=[r['pid'] for r in rows if r['kind'] == 'supervisor'],
            live_child_pids=[r['pid'] for r in rows if r['kind'] == 'live_child'],
            scanner_deadman_pids=pids('scanner_deadman'),
            account_state={'positions': [], 'open_orders': []},
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report['status'] == 'healthy' else 1
    report = build_status_report(
        runtime_state_dir=state_dir,
        supervisor_pids=[r['pid'] for r in rows if r['kind'] == 'supervisor'],
        live_child_pids=[r['pid'] for r in rows if r['kind'] == 'live_child'],
        scanner_deadman_pids=pids('scanner_deadman'),
        exchange_state={},
    )
    print_status_report(report, json_output=args.json)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
