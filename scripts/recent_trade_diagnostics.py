#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict

import rejected_analysis
import trade_bucket_analysis

DEFAULT_HERMES_HOME = Path(os.path.expanduser(os.getenv('HERMES_HOME', str(Path.home() / '.hermes'))))
DEFAULT_APP_HOME = DEFAULT_HERMES_HOME / 'binance-futures-momentum-long'
DEFAULT_RUNTIME_STATE_DIR = DEFAULT_APP_HOME / 'runtime-state'
DEFAULT_OUTPUT_DIR = DEFAULT_APP_HOME / 'diagnostics' / 'recent-7d'


def build_summary(trade_payload: Dict[str, Any], rejected_payload: Dict[str, Any], lookback_days: int) -> Dict[str, Any]:
    trade_summary = trade_payload.get('summary', {}) if isinstance(trade_payload, dict) else {}
    rejected_summary = rejected_payload.get('summary', {}) if isinstance(rejected_payload, dict) else {}
    return {
        'lookback_days': int(lookback_days),
        'trade_bucket_summary': trade_summary,
        'rejected_summary': rejected_summary,
        'artifacts': {
            'trade_bucket_json': 'trade-bucket-analysis.json',
            'trade_bucket_markdown': 'trade-bucket-analysis.md',
            'rejected_json': 'rejected-analysis.json',
            'rejected_markdown': 'rejected-analysis.md',
        },
    }


def render_markdown(summary: Dict[str, Any]) -> str:
    trade = summary.get('trade_bucket_summary', {})
    rejected = summary.get('rejected_summary', {})
    artifacts = summary.get('artifacts', {})
    lines = [
        '# Recent 7d Trade Diagnostics',
        '',
        f"- lookback_days: {summary.get('lookback_days', 0)}",
        f"- total_closed_trades: {trade.get('total_closed_trades', 0)}",
        f"- distinct_buckets: {trade.get('distinct_buckets', 0)}",
        f"- win_rate_pct: {trade.get('win_rate_pct', 0)}",
        f"- avg_expectancy_r: {trade.get('avg_expectancy_r', 0)}",
        f"- total_rejected: {rejected.get('total_rejected', 0)}",
        f"- distinct_rejected_symbols: {rejected.get('distinct_symbols', 0)}",
        f"- top_reject_reason: {rejected.get('top_reject_reason') or 'n/a'}",
        '',
        '## Artifacts',
        '',
        f"- trade_bucket_json: {artifacts.get('trade_bucket_json', '')}",
        f"- trade_bucket_markdown: {artifacts.get('trade_bucket_markdown', '')}",
        f"- rejected_json: {artifacts.get('rejected_json', '')}",
        f"- rejected_markdown: {artifacts.get('rejected_markdown', '')}",
        '',
    ]
    return '\n'.join(lines)


def run(runtime_state_dir: Path, output_dir: Path, lookback_days: int = 7, limit: int = 20000, symbol: str = '') -> Dict[str, Any]:
    runtime_state_dir = Path(runtime_state_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    trade_payload = trade_bucket_analysis.run(
        runtime_state_dir=runtime_state_dir,
        output_json_path=output_dir / 'trade-bucket-analysis.json',
        output_markdown_path=output_dir / 'trade-bucket-analysis.md',
        limit=limit,
        symbol=symbol,
        lookback_days=lookback_days,
    )
    rejected_payload = rejected_analysis.run(
        runtime_state_dir=runtime_state_dir,
        output_json_path=output_dir / 'rejected-analysis.json',
        output_markdown_path=output_dir / 'rejected-analysis.md',
        limit=limit,
    )
    summary = build_summary(trade_payload, rejected_payload, lookback_days=lookback_days)
    (output_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    (output_dir / 'summary.md').write_text(render_markdown(summary), encoding='utf-8')
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description='Build a recent-trades diagnostics bundle for the last N days.')
    parser.add_argument('--runtime-state-dir', default=str(DEFAULT_RUNTIME_STATE_DIR))
    parser.add_argument('--output-dir', default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument('--lookback-days', type=int, default=7)
    parser.add_argument('--limit', type=int, default=20000)
    parser.add_argument('--symbol', default='')
    args = parser.parse_args()
    summary = run(
        runtime_state_dir=Path(args.runtime_state_dir),
        output_dir=Path(args.output_dir),
        lookback_days=args.lookback_days,
        limit=args.limit,
        symbol=args.symbol,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
