#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


def _load_strategy():
    strategy_path = Path(__file__).resolve().with_name('binance_futures_momentum_long.py')
    spec = importlib.util.spec_from_file_location('binance_futures_momentum_long_bridge', strategy_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


strategy = _load_strategy()


def _mcporter_executable() -> str:
    for candidate in ('mcporter', 'mcporter.cmd', 'mcporter.ps1'):
        path = shutil.which(candidate)
        if path:
            return path
    return 'mcporter'


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


def _clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _default_entry() -> Dict[str, float]:
    return {
        'okx_sentiment_score': 0.0,
        'okx_sentiment_acceleration': 0.0,
        'sector_resonance_score': 0.0,
        'smart_money_flow_score': 0.0,
    }


def normalize_symbol(raw_symbol: Any) -> str:
    return strategy.normalize_symbol(raw_symbol) or ''


def _ensure_entry(payload: Dict[str, Dict[str, float]], symbol: str) -> Dict[str, float]:
    if symbol not in payload:
        payload[symbol] = _default_entry()
    return payload[symbol]


def _extract_rows(obj: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if isinstance(obj, dict):
        if any(key in obj for key in ('symbol', 'instId', 'coin')):
            rows.append(obj)
        for value in obj.values():
            rows.extend(_extract_rows(value))
    elif isinstance(obj, list):
        for item in obj:
            rows.extend(_extract_rows(item))
    return rows


def _load_symbols_from_args(args: argparse.Namespace) -> List[str]:
    symbols: List[str] = []
    inline = getattr(args, 'symbols', '')
    if inline:
        symbols.extend(normalize_symbol(item) for item in inline.split(',') if item.strip())
    file_path = str(getattr(args, 'symbols_file', '') or '').strip()
    if file_path:
        path = Path(file_path)
        if path.exists():
            symbols.extend(normalize_symbol(line) for line in path.read_text(encoding='utf-8').splitlines() if line.strip())
    return [symbol for symbol in dict.fromkeys(symbols) if symbol]


def _run_mcporter(stdio_command: str, name: str, tool: str, tool_args: Dict[str, Any]) -> Dict[str, Any]:
    command = [
        _mcporter_executable(),
        'call',
        '--stdio',
        stdio_command,
        '--name',
        name,
        '--tool',
        tool,
        '--args',
        json.dumps(tool_args, ensure_ascii=False),
        '--output',
        'json',
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=True)
    text = '\n'.join([completed.stdout or '', completed.stderr or '']).strip()
    if not text:
        return {}
    if text.startswith('data:'):
        text = '\n'.join(line[5:].strip() if line.strip().startswith('data:') else line for line in text.splitlines())
    return json.loads(text)


def _market_tool_args(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        'symbols': getattr(args, 'symbols', ''),
        'topHot': int(getattr(args, 'top_hot', 5) or 5),
        'quoteCcy': getattr(args, 'quote_ccy', 'USDT'),
        'settleCcy': getattr(args, 'settle_ccy', 'USDT'),
        'minVolUsd24h': getattr(args, 'min_vol_usd_24h', '5000000'),
    }


def _oi_tool_args(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        'symbols': getattr(args, 'symbols', ''),
        'top': int(getattr(args, 'oi_top', 5) or 5),
        'bar': getattr(args, 'oi_bar', '5m'),
        'historyLimit': int(getattr(args, 'oi_history_limit', 6) or 6),
        'minOiUsd': getattr(args, 'min_oi_usd', '1000000'),
        'minAbsOiDeltaPct': getattr(args, 'min_abs_oi_delta_pct', '1'),
        'quoteCcy': getattr(args, 'quote_ccy', 'USDT'),
        'settleCcy': getattr(args, 'settle_ccy', 'USDT'),
    }


def _merge_market_snapshot(base: Dict[str, Dict[str, float]], payload: Dict[str, Any], requested_symbols: Iterable[str]) -> None:
    requested = set(requested_symbols)
    for row in _extract_rows(payload):
        symbol = normalize_symbol(row.get('symbol') or row.get('instId') or row.get('coin'))
        if not symbol:
            continue
        if requested and symbol not in requested:
            continue
        _ensure_entry(base, symbol)


def _merge_market_flow(base: Dict[str, Dict[str, float]], payload: Dict[str, Any]) -> None:
    for row in _extract_rows(payload):
        symbol = normalize_symbol(row.get('symbol') or row.get('instId') or row.get('coin'))
        if not symbol:
            continue
        entry = _ensure_entry(base, symbol)
        oi_delta_pct = _to_float(row.get('oiDeltaPct'))
        price_change_pct = _to_float(row.get('pxChgPct'))
        funding_rate = _to_float(row.get('fundingRate'))
        smart_money = _clamp((oi_delta_pct / 5.0) + (price_change_pct / 10.0) - (funding_rate * 250.0))
        sector_resonance = _clamp((abs(oi_delta_pct) / 10.0) + (max(price_change_pct, 0.0) / 20.0) + (max(-funding_rate, 0.0) * 50.0), 0.0, 1.0)
        entry['smart_money_flow_score'] = round(smart_money, 4)
        entry['sector_resonance_score'] = round(max(entry['sector_resonance_score'], sector_resonance), 4)


def _merge_sentiment_ranking(base: Dict[str, Dict[str, float]], payload: Dict[str, Any]) -> None:
    for row in _extract_rows(payload):
        symbol = normalize_symbol(row.get('symbol') or row.get('instId') or row.get('coin'))
        if not symbol:
            continue
        bullish_ratio = _to_float(row.get('bullishRatio'))
        bearish_ratio = _to_float(row.get('bearishRatio'))
        mention_count = max(_to_float(row.get('mentionCount')), 0.0)
        score = _clamp(bullish_ratio - bearish_ratio)
        sector_resonance = _clamp((max(score, 0.0) * 0.6) + min(mention_count / 250.0, 0.4), 0.0, 1.0)
        entry = _ensure_entry(base, symbol)
        entry['okx_sentiment_score'] = round(max(entry['okx_sentiment_score'], score), 4)
        entry['sector_resonance_score'] = round(max(entry['sector_resonance_score'], sector_resonance), 4)


def _merge_sentiment_trend(base: Dict[str, Dict[str, float]], payload: Dict[str, Any]) -> None:
    for row in _extract_rows(payload):
        symbol = normalize_symbol(row.get('symbol') or row.get('instId') or row.get('coin'))
        if not symbol:
            continue
        points = row.get('points')
        if not isinstance(points, list) or not points:
            continue
        latest = points[-1] if isinstance(points[-1], dict) else {}
        latest_score = _clamp(_to_float(latest.get('bullishRatio')) - _to_float(latest.get('bearishRatio')))
        previous_score = latest_score
        if len(points) > 1 and isinstance(points[-2], dict):
            previous = points[-2]
            previous_score = _clamp(_to_float(previous.get('bullishRatio')) - _to_float(previous.get('bearishRatio')))
        acceleration = _clamp(latest_score - previous_score)
        entry = _ensure_entry(base, symbol)
        entry['okx_sentiment_score'] = round(latest_score, 4)
        entry['okx_sentiment_acceleration'] = round(acceleration, 4)
        boosted_sector_resonance = _clamp(entry['sector_resonance_score'] + max(acceleration, 0.0) * 0.5, 0.0, 1.0)
        entry['sector_resonance_score'] = round(max(entry['sector_resonance_score'], boosted_sector_resonance), 4)


def build_bridge_payload(args: argparse.Namespace) -> Dict[str, Dict[str, float]]:
    base: Dict[str, Dict[str, float]] = {}
    requested_symbols = _load_symbols_from_args(args)
    for symbol in requested_symbols:
        _ensure_entry(base, symbol)

    market_payload = _run_mcporter(getattr(args, 'stdio_command'), getattr(args, 'name', 'okx_bridge'), 'market_filter', _market_tool_args(args))
    _merge_market_snapshot(base, market_payload, requested_symbols)

    oi_payload = _run_mcporter(getattr(args, 'stdio_command'), getattr(args, 'name', 'okx_bridge'), 'market_filter_oi_change', _oi_tool_args(args))
    _merge_market_flow(base, oi_payload)

    symbols = list(base)
    try:
        ranking_payload = _run_mcporter(
            getattr(args, 'stdio_command'),
            getattr(args, 'name', 'okx_bridge'),
            'news_get_sentiment_ranking',
            {'period': getattr(args, 'period', '1h')},
        )
        _merge_sentiment_ranking(base, ranking_payload)

        trend_payload = _run_mcporter(
            getattr(args, 'stdio_command'),
            getattr(args, 'name', 'okx_bridge'),
            'news_get_coin_sentiment',
            {
                'coins': ','.join(symbols),
                'period': getattr(args, 'period', '1h'),
                'points': int(getattr(args, 'trend_points', 24) or 24),
            },
        )
        _merge_sentiment_trend(base, trend_payload)
    except Exception:
        if getattr(args, 'news_required', False):
            raise

    return {symbol: base[symbol] for symbol in base}


def emit_lines(payload: Dict[str, Dict[str, float]]) -> str:
    lines = []
    for symbol, entry in payload.items():
        lines.append(
            f"{symbol}|{entry.get('okx_sentiment_score', 0.0):.4f}|{entry.get('okx_sentiment_acceleration', 0.0):.4f}|"
            f"{entry.get('sector_resonance_score', 0.0):.4f}|{entry.get('smart_money_flow_score', 0.0):.4f}"
        )
    return '\n'.join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Fetch OKX market and sentiment signals and emit strategy-compatible lines.')
    parser.add_argument('--stdio-command', required=True, help='Underlying MCP stdio command used by mcporter.')
    parser.add_argument('--name', default='okx_bridge')
    parser.add_argument('--symbols', default='')
    parser.add_argument('--symbols-file', default='')
    parser.add_argument('--top-hot', type=int, default=5)
    parser.add_argument('--oi-top', type=int, default=5)
    parser.add_argument('--oi-bar', default='5m')
    parser.add_argument('--oi-history-limit', type=int, default=6)
    parser.add_argument('--min-oi-usd', default='1000000')
    parser.add_argument('--min-vol-usd-24h', default='5000000')
    parser.add_argument('--min-abs-oi-delta-pct', default='1')
    parser.add_argument('--quote-ccy', default='USDT')
    parser.add_argument('--settle-ccy', default='USDT')
    parser.add_argument('--period', default='1h')
    parser.add_argument('--trend-points', type=int, default=24)
    parser.add_argument('--news-required', action='store_true')
    parser.add_argument('--output-format', choices=['lines', 'json'], default='lines')
    return parser


def main(argv: List[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = build_bridge_payload(args)
    if args.output_format == 'json':
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(emit_lines(payload))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
