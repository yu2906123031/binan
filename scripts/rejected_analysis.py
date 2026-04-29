#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List


DEFAULT_HERMES_HOME = Path(os.path.expanduser(os.getenv('HERMES_HOME', str(Path.home() / '.hermes'))))
DEFAULT_APP_HOME = DEFAULT_HERMES_HOME / 'binance-futures-momentum-long'
DEFAULT_RUNTIME_STATE_DIR = DEFAULT_APP_HOME / 'runtime-state'
DEFAULT_OUTPUT_JSON = DEFAULT_APP_HOME / 'rejected-analysis.json'
DEFAULT_OUTPUT_MARKDOWN = DEFAULT_APP_HOME / 'rejected-analysis.md'


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


def _round(value: float, digits: int = 4) -> float:
    return round(float(value), digits)


def _normalize_text(value: Any, default: str = 'unknown') -> str:
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


def filter_candidate_rejected(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [row for row in rows if isinstance(row, dict) and row.get('event_type') == 'candidate_rejected']


def _count_table(counter: Counter, key_name: str) -> List[Dict[str, Any]]:
    ordered = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return [{key_name: key, 'count': count} for key, count in ordered]


def build_rejected_analysis_payload(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rejected = filter_candidate_rejected(rows)
    reason_counter: Counter = Counter()
    label_counter: Counter = Counter()
    grade_counter: Counter = Counter()
    overextension_counter: Counter = Counter()
    symbol_counter: Counter = Counter()
    aggregates: Dict[str, Dict[str, float]] = defaultdict(lambda: {'count': 0.0, 'slippage_sum': 0.0, 'depth_sum': 0.0})

    for row in rejected:
        reason = _normalize_text(row.get('reject_reason'))
        label = _normalize_text(row.get('reject_reason_label'))
        grade = _normalize_text(row.get('execution_liquidity_grade')).upper()
        overextension = _normalize_text(row.get('overextension_flag'))
        symbol = _normalize_text(row.get('symbol'))
        slippage_r = _to_float(row.get('expected_slippage_r'))
        depth_ratio = _to_float(row.get('book_depth_fill_ratio'))

        reason_counter[reason] += 1
        label_counter[label] += 1
        grade_counter[grade] += 1
        overextension_counter[overextension] += 1
        symbol_counter[symbol] += 1
        aggregate = aggregates[reason]
        aggregate['count'] += 1
        aggregate['slippage_sum'] += slippage_r
        aggregate['depth_sum'] += depth_ratio

    by_reason: List[Dict[str, Any]] = []
    for reason, count in sorted(reason_counter.items(), key=lambda item: (-item[1], item[0])):
        aggregate = aggregates[reason]
        by_reason.append({
            'reject_reason': reason,
            'count': count,
            'avg_expected_slippage_r': _round(aggregate['slippage_sum'] / count, 4),
            'avg_book_depth_fill_ratio': _round(aggregate['depth_sum'] / count, 4),
        })

    top_reason = by_reason[0] if by_reason else None
    return {
        'summary': {
            'total_rejected': len(rejected),
            'distinct_symbols': len(symbol_counter),
            'top_reject_reason': top_reason['reject_reason'] if top_reason else None,
            'top_reject_reason_count': top_reason['count'] if top_reason else 0,
        },
        'by_reason': by_reason,
        'by_label': _count_table(label_counter, 'reject_reason_label'),
        'by_grade': _count_table(grade_counter, 'execution_liquidity_grade'),
        'by_overextension': _count_table(overextension_counter, 'overextension_flag'),
        'by_symbol': _count_table(symbol_counter, 'symbol'),
    }


def render_markdown_report(payload: Dict[str, Any]) -> str:
    lines = ['# Candidate Rejected Analysis', '']
    summary = payload.get('summary', {})
    lines.append(f"- total_rejected: {summary.get('total_rejected', 0)}")
    lines.append(f"- distinct_symbols: {summary.get('distinct_symbols', 0)}")
    lines.append(f"- top_reject_reason: {summary.get('top_reject_reason') or 'n/a'}")
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

    append_table('By reason', payload.get('by_reason', []), ['reject_reason', 'count', 'avg_expected_slippage_r', 'avg_book_depth_fill_ratio'])
    append_table('By label', payload.get('by_label', []), ['reject_reason_label', 'count'])
    append_table('By liquidity grade', payload.get('by_grade', []), ['execution_liquidity_grade', 'count'])
    append_table('By overextension', payload.get('by_overextension', []), ['overextension_flag', 'count'])
    append_table('By symbol', payload.get('by_symbol', []), ['symbol', 'count'])
    return '\n'.join(lines).rstrip() + '\n'


def run(
    runtime_state_dir: Path,
    output_json_path: Path,
    output_markdown_path: Path,
    limit: int = 5000,
) -> Dict[str, Any]:
    runtime_state_dir = Path(runtime_state_dir)
    payload = build_rejected_analysis_payload(load_events(runtime_state_dir / 'events.jsonl', limit=limit))
    output_json_path = Path(output_json_path)
    output_markdown_path = Path(output_markdown_path)
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    output_markdown_path.write_text(render_markdown_report(payload), encoding='utf-8')
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description='Aggregate candidate_rejected events into JSON and markdown reports.')
    parser.add_argument('--runtime-state-dir', default=str(DEFAULT_RUNTIME_STATE_DIR))
    parser.add_argument('--output-json', default=str(DEFAULT_OUTPUT_JSON))
    parser.add_argument('--output-markdown', default=str(DEFAULT_OUTPUT_MARKDOWN))
    parser.add_argument('--limit', type=int, default=5000)
    args = parser.parse_args()
    payload = run(
        runtime_state_dir=Path(args.runtime_state_dir),
        output_json_path=Path(args.output_json),
        output_markdown_path=Path(args.output_markdown),
        limit=args.limit,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
