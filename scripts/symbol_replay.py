#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


DEFAULT_HERMES_HOME = Path(os.path.expanduser(os.getenv('HERMES_HOME', str(Path.home() / '.hermes'))))
DEFAULT_APP_HOME = DEFAULT_HERMES_HOME / 'binance-futures-momentum-long'
DEFAULT_RUNTIME_STATE_DIR = DEFAULT_APP_HOME / 'runtime-state'

ENTRY_START_EVENTS = {'entry_filled', 'okx_simulated_order_submitted'}
TERMINAL_EVENTS = {'candidate_rejected', 'trade_invalidated'}
MANAGEMENT_EVENTS = {
    'initial_stop_placed',
    'protection_confirmed',
    'breakeven_moved',
    'tp1_hit',
    'tp2_hit',
    'runner_exited',
    'management_action_failed',
    'okx_breakeven_moved',
    'okx_management_action_failed',
}


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _round(value: Any, digits: int = 4) -> float:
    return round(_to_float(value), digits)


def _normalize_text(value: Any, default: str = '') -> str:
    text = str(value or '').strip()
    return text or default


def load_events(events_path: Path, limit: int = 5000) -> List[Dict[str, Any]]:
    if not events_path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    max_rows = max(int(limit or 0), 1)
    with events_path.open('r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    if len(rows) <= max_rows:
        return rows
    return rows[-max_rows:]


def _normalize_position_key(row: Dict[str, Any]) -> str:
    key = _normalize_text(row.get('position_key')).upper()
    if key:
        return key
    symbol = _normalize_text(row.get('symbol')).upper()
    side = _normalize_text(row.get('side') or row.get('position_side')).upper()
    if symbol and side:
        return f'{symbol}:{side}'
    return symbol


def filter_symbol_events(rows: Iterable[Dict[str, Any]], symbol: str, side: str = '') -> List[Dict[str, Any]]:
    target_symbol = _normalize_text(symbol).upper()
    target_side = _normalize_text(side).upper()
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_symbol = _normalize_text(row.get('symbol')).upper()
        row_side = _normalize_text(row.get('side') or row.get('position_side')).upper()
        if target_symbol and row_symbol != target_symbol:
            continue
        if target_side and row_side != target_side:
            continue
        filtered.append(row)
    return filtered


def _count_table(counter: Counter, key_name: str) -> List[Dict[str, Any]]:
    ordered = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return [{key_name: key, 'count': count} for key, count in ordered]


def _new_session(position_key: str, row: Dict[str, Any]) -> Dict[str, Any]:
    symbol = _normalize_text(row.get('symbol')).upper()
    side = _normalize_text(row.get('side') or row.get('position_side')).upper()
    return {
        'position_key': position_key,
        'symbol': symbol,
        'side': side,
        'status': 'pending',
        'started_at': _normalize_text(row.get('recorded_at')) or None,
        'selected_at': None,
        'submitted_at': None,
        'entry_filled_at': None,
        'buy_fill_confirmed_at': None,
        'closed_at': None,
        'rejected_at': None,
        'selected_score': None,
        'selected_state': '',
        'selected_alert_tier': '',
        'selected_entry_price': None,
        'selected_stop_price': None,
        'entry_price': None,
        'stop_price': None,
        'quantity': None,
        'filled_quantity': None,
        'execution_exchange': '',
        'entry_order_id': None,
        'exit_reason': '',
        'reject_reason': '',
        'reject_reason_label': '',
        'management_actions': [],
        'event_sequence': [],
        'events': [],
    }


def _apply_row_to_session(session: Dict[str, Any], row: Dict[str, Any]) -> None:
    event_type = _normalize_text(row.get('event_type'), 'unknown')
    recorded_at = _normalize_text(row.get('recorded_at')) or None
    if not session.get('started_at'):
        session['started_at'] = recorded_at
    if not session.get('symbol'):
        session['symbol'] = _normalize_text(row.get('symbol')).upper()
    if not session.get('side'):
        session['side'] = _normalize_text(row.get('side') or row.get('position_side')).upper()
    if not session.get('position_key'):
        session['position_key'] = _normalize_position_key(row)

    session['event_sequence'].append(event_type)
    session['events'].append(dict(row))

    if event_type == 'candidate_selected':
        session['status'] = 'selected'
        session['selected_at'] = recorded_at
        session['selected_score'] = _round(row.get('score'), 4)
        session['selected_state'] = _normalize_text(row.get('state'))
        session['selected_alert_tier'] = _normalize_text(row.get('alert_tier'))
        session['selected_entry_price'] = _round(row.get('entry_price'), 10)
        session['selected_stop_price'] = _round(row.get('stop_price'), 10)
        session['quantity'] = _round(row.get('quantity'), 10)
        session['execution_exchange'] = _normalize_text(row.get('execution_exchange'))
        return

    if event_type == 'okx_simulated_order_submitted':
        session['status'] = 'submitted'
        session['submitted_at'] = recorded_at
        session['entry_price'] = _round(row.get('entry_price'), 10)
        session['filled_quantity'] = _round(row.get('quantity') or row.get('filled_quantity'), 10)
        session['execution_exchange'] = _normalize_text(row.get('exchange') or 'OKX_SIMULATED')
        session['entry_order_id'] = row.get('order_id')
        return

    if event_type == 'entry_filled':
        session['status'] = 'entered'
        session['entry_filled_at'] = recorded_at
        session['entry_price'] = _round(row.get('entry_price'), 10)
        session['stop_price'] = _round(row.get('stop_price'), 10)
        session['quantity'] = _round(row.get('quantity'), 10)
        session['filled_quantity'] = _round(row.get('filled_quantity') or row.get('quantity'), 10)
        session['entry_order_id'] = row.get('entry_order_id')
        if not session.get('execution_exchange'):
            session['execution_exchange'] = _normalize_text(row.get('execution_exchange') or 'BINANCE')
        return

    if event_type == 'buy_fill_confirmed':
        if session.get('status') not in {'closed', 'rejected'}:
            session['status'] = 'entered'
        session['buy_fill_confirmed_at'] = recorded_at
        if session.get('entry_price') in (None, 0.0):
            session['entry_price'] = _round(row.get('entry_price'), 10)
        if session.get('stop_price') in (None, 0.0):
            session['stop_price'] = _round(row.get('stop_price'), 10)
        if session.get('quantity') in (None, 0.0):
            session['quantity'] = _round(row.get('quantity'), 10)
        session['filled_quantity'] = _round(row.get('filled_quantity') or row.get('quantity'), 10)
        session['entry_order_id'] = row.get('entry_order_id')
        return

    if event_type in MANAGEMENT_EVENTS:
        session['management_actions'].append({
            'event_type': event_type,
            'recorded_at': recorded_at,
            'close_qty': _round(row.get('close_qty'), 10) if row.get('close_qty') is not None else None,
            'new_stop_price': _round(row.get('new_stop_price'), 10) if row.get('new_stop_price') is not None else None,
            'protection_status': row.get('protection_status'),
        })
        if event_type == 'protection_confirmed' and session.get('status') == 'entered':
            session['status'] = 'protected'
        return

    if event_type == 'candidate_rejected':
        session['status'] = 'rejected'
        session['rejected_at'] = recorded_at
        session['reject_reason'] = _normalize_text(row.get('reject_reason'))
        session['reject_reason_label'] = _normalize_text(row.get('reject_reason_label'))
        return

    if event_type == 'trade_invalidated':
        session['status'] = 'closed'
        session['closed_at'] = recorded_at
        session['exit_reason'] = _normalize_text(row.get('exit_reason'))
        return


def build_symbol_replay_payload(rows: Iterable[Dict[str, Any]], symbol: str, side: str = '') -> Dict[str, Any]:
    filtered = filter_symbol_events(rows, symbol=symbol, side=side)
    sessions: List[Dict[str, Any]] = []
    active_by_key: Dict[str, Dict[str, Any]] = {}

    for row in filtered:
        event_type = _normalize_text(row.get('event_type'))
        position_key = _normalize_position_key(row)
        session: Optional[Dict[str, Any]] = active_by_key.get(position_key) if position_key else None

        if event_type == 'candidate_selected':
            if session and session.get('status') == 'selected' and not session.get('entry_filled_at') and not session.get('submitted_at'):
                session['status'] = 'superseded'
            session = _new_session(position_key, row)
            sessions.append(session)
            if position_key:
                active_by_key[position_key] = session
        elif event_type in ENTRY_START_EVENTS:
            if session is None or session.get('status') in {'closed', 'rejected', 'superseded'} or bool(session.get('entry_filled_at') or session.get('submitted_at')):
                session = _new_session(position_key, row)
                sessions.append(session)
                if position_key:
                    active_by_key[position_key] = session
        elif session is None:
            session = _new_session(position_key, row)
            sessions.append(session)
            if position_key:
                active_by_key[position_key] = session

        _apply_row_to_session(session, row)
        if event_type in TERMINAL_EVENTS and position_key:
            active_by_key.pop(position_key, None)

    event_type_counter: Counter = Counter()
    exit_reason_counter: Counter = Counter()
    reject_reason_counter: Counter = Counter()

    for row in filtered:
        event_type_counter[_normalize_text(row.get('event_type'), 'unknown')] += 1
    for session in sessions:
        if session.get('exit_reason'):
            exit_reason_counter[session['exit_reason']] += 1
        if session.get('reject_reason'):
            reject_reason_counter[session['reject_reason']] += 1

    selected_count = sum(1 for session in sessions if session.get('selected_at'))
    entered_count = sum(1 for session in sessions if session.get('entry_filled_at') or session.get('buy_fill_confirmed_at'))
    submitted_count = sum(1 for session in sessions if session.get('submitted_at'))
    rejected_count = sum(1 for session in sessions if session.get('status') == 'rejected')
    closed_count = sum(1 for session in sessions if session.get('status') == 'closed')
    active_count = sum(1 for session in sessions if session.get('status') in {'selected', 'submitted', 'entered', 'protected'})
    superseded_count = sum(1 for session in sessions if session.get('status') == 'superseded')

    return {
        'summary': {
            'symbol': _normalize_text(symbol).upper(),
            'side': _normalize_text(side).upper(),
            'total_events': len(filtered),
            'session_count': len(sessions),
            'selected_count': selected_count,
            'entered_count': entered_count,
            'submitted_count': submitted_count,
            'rejected_count': rejected_count,
            'closed_count': closed_count,
            'active_count': active_count,
            'superseded_count': superseded_count,
        },
        'by_event_type': _count_table(event_type_counter, 'event_type'),
        'by_exit_reason': _count_table(exit_reason_counter, 'exit_reason'),
        'by_reject_reason': _count_table(reject_reason_counter, 'reject_reason'),
        'sessions': sessions,
    }


def render_markdown_report(payload: Dict[str, Any]) -> str:
    lines = ['# Symbol Replay Report', '']
    summary = payload.get('summary', {})
    lines.append(f"- symbol: {summary.get('symbol') or 'ALL'}")
    lines.append(f"- side: {summary.get('side') or 'ALL'}")
    lines.append(f"- total_events: {summary.get('total_events', 0)}")
    lines.append(f"- session_count: {summary.get('session_count', 0)}")
    lines.append(f"- selected_count: {summary.get('selected_count', 0)}")
    lines.append(f"- entered_count: {summary.get('entered_count', 0)}")
    lines.append(f"- submitted_count: {summary.get('submitted_count', 0)}")
    lines.append(f"- rejected_count: {summary.get('rejected_count', 0)}")
    lines.append(f"- closed_count: {summary.get('closed_count', 0)}")
    lines.append(f"- active_count: {summary.get('active_count', 0)}")
    lines.append('')

    def append_table(title: str, rows: List[Dict[str, Any]], columns: List[str]) -> None:
        lines.append(f'## {title}')
        lines.append('')
        if not rows:
            lines.append('_no rows_')
            lines.append('')
            return
        lines.append('| ' + ' | '.join(columns) + ' |')
        lines.append('| ' + ' | '.join(['---'] * len(columns)) + ' |')
        for row in rows:
            lines.append('| ' + ' | '.join(str(row.get(column, '')) for column in columns) + ' |')
        lines.append('')

    session_rows = []
    for item in payload.get('sessions', []):
        session_rows.append({
            'position_key': item.get('position_key'),
            'status': item.get('status'),
            'selected_at': item.get('selected_at') or '',
            'entry_filled_at': item.get('entry_filled_at') or item.get('buy_fill_confirmed_at') or item.get('submitted_at') or '',
            'closed_at': item.get('closed_at') or item.get('rejected_at') or '',
            'exit_reason': item.get('exit_reason') or '',
            'reject_reason': item.get('reject_reason') or '',
            'selected_score': item.get('selected_score') if item.get('selected_score') is not None else '',
            'entry_price': item.get('entry_price') if item.get('entry_price') is not None else '',
            'filled_quantity': item.get('filled_quantity') if item.get('filled_quantity') is not None else '',
        })

    append_table(
        'Sessions',
        session_rows,
        ['position_key', 'status', 'selected_at', 'entry_filled_at', 'closed_at', 'exit_reason', 'reject_reason', 'selected_score', 'entry_price', 'filled_quantity'],
    )
    append_table('By event type', payload.get('by_event_type', []), ['event_type', 'count'])
    append_table('By exit reason', payload.get('by_exit_reason', []), ['exit_reason', 'count'])
    append_table('By reject reason', payload.get('by_reject_reason', []), ['reject_reason', 'count'])
    return '\n'.join(lines).rstrip() + '\n'


def _default_output_paths(symbol: str, side: str = '') -> tuple[Path, Path]:
    symbol_part = _normalize_text(symbol, 'all').lower()
    side_part = _normalize_text(side).lower()
    suffix = f'{symbol_part}-{side_part}' if side_part else symbol_part
    return (
        DEFAULT_APP_HOME / f'symbol-replay-{suffix}.json',
        DEFAULT_APP_HOME / f'symbol-replay-{suffix}.md',
    )


def run(
    runtime_state_dir: Path,
    symbol: str,
    output_json_path: Optional[Path] = None,
    output_markdown_path: Optional[Path] = None,
    limit: int = 5000,
    side: str = '',
) -> Dict[str, Any]:
    if output_json_path is None or output_markdown_path is None:
        default_json, default_markdown = _default_output_paths(symbol, side)
        output_json_path = output_json_path or default_json
        output_markdown_path = output_markdown_path or default_markdown
    runtime_state_dir = Path(runtime_state_dir)
    payload = build_symbol_replay_payload(load_events(runtime_state_dir / 'events.jsonl', limit=limit), symbol=symbol, side=side)
    output_json_path = Path(output_json_path)
    output_markdown_path = Path(output_markdown_path)
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    output_markdown_path.write_text(render_markdown_report(payload), encoding='utf-8')
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description='Replay a single symbol from runtime events into JSON and markdown reports.')
    parser.add_argument('--runtime-state-dir', default=str(DEFAULT_RUNTIME_STATE_DIR))
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--side', default='')
    parser.add_argument('--output-json', default='')
    parser.add_argument('--output-markdown', default='')
    parser.add_argument('--limit', type=int, default=5000)
    args = parser.parse_args()
    payload = run(
        runtime_state_dir=Path(args.runtime_state_dir),
        symbol=args.symbol,
        side=args.side,
        output_json_path=Path(args.output_json) if args.output_json else None,
        output_markdown_path=Path(args.output_markdown) if args.output_markdown else None,
        limit=args.limit,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
