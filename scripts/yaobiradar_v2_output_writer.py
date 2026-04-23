#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

SYMBOLS_PATH = Path('/root/.hermes/binance_square_symbols.txt')
EXTERNAL_JSON_PATH = Path('/root/.hermes/binance_external_signal.json')


def normalize_symbol(raw: Any) -> str:
    text = str(raw or '').strip().upper().replace('-', '').replace('_', '').replace('/', '')
    if not text:
        return ''
    if text.endswith('USDT'):
        return text
    if text[:-4].isdigit() and text[-4:].isalpha():
        return f'{text}USDT'
    if text.isalnum():
        return f'{text}USDT'
    return text


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {'1', 'true', 'yes', 'y', 'on'}:
        return True
    if text in {'0', 'false', 'no', 'n', 'off'}:
        return False
    return None


def _to_reason_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, (list, tuple, set)):
        result = []
        for item in value:
            text = str(item).strip()
            if text:
                result.append(text)
        return result
    text = str(value).strip()
    return [text] if text else []


def _coalesce(row: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    symbol = normalize_symbol(_coalesce(row, 'symbol', 'ticker', 'pair', 'instrument', 'base_symbol'))
    if not symbol:
        return {}

    score = _to_float(_coalesce(
        row,
        'external_signal_score',
        'score',
        'final_score',
        'radar_score',
        'total_score',
        'rank_score',
    ))
    tier = _coalesce(row, 'external_signal_tier', 'tier', 'alert_tier', 'signal_tier', 'level')
    if tier is not None:
        tier = str(tier).strip().lower() or None
    position_size_pct = _to_float(_coalesce(
        row,
        'external_position_size_pct',
        'position_size_pct',
        'suggested_position_size_pct',
        'size_pct',
    ))
    veto = _to_bool(_coalesce(row, 'external_veto', 'veto', 'blocked', 'blacklist', 'skip'))
    veto_reason = _coalesce(row, 'external_veto_reason', 'veto_reason', 'block_reason', 'skip_reason')
    if veto_reason is not None:
        veto_reason = str(veto_reason).strip() or None
    reasons = _to_reason_list(_coalesce(row, 'external_reasons', 'reasons', 'signals', 'tags', 'flags'))

    normalized: Dict[str, Any] = {'symbol': symbol}
    if score is not None:
        normalized['external_signal_score'] = round(score, 4)
    if tier:
        normalized['external_signal_tier'] = tier
    if position_size_pct is not None:
        normalized['external_position_size_pct'] = round(position_size_pct, 4)
    if veto is not None:
        normalized['external_veto'] = veto
    if veto_reason:
        normalized['external_veto_reason'] = veto_reason
    if reasons:
        normalized['external_reasons'] = reasons
    return normalized


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f'.{path.name}.', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass


def build_payload(rows: Iterable[Dict[str, Any]], engine: str = 'yaobiradar_v2') -> Dict[str, Any]:
    signal_map: Dict[str, Dict[str, Any]] = {}
    ordered_symbols: List[str] = []
    for raw_row in rows:
        row = normalize_row(raw_row)
        symbol = row.get('symbol', '')
        if not symbol:
            continue
        if symbol not in ordered_symbols:
            ordered_symbols.append(symbol)
        entry: Dict[str, Any] = {}
        for key in (
            'external_signal_score',
            'external_signal_tier',
            'external_position_size_pct',
            'external_veto',
            'external_veto_reason',
            'external_reasons',
        ):
            if key in row and row[key] is not None:
                entry[key] = row[key]
        signal_map[symbol] = entry
    return {
        'engine': engine,
        'generated_at': int(time.time()),
        'symbols': ordered_symbols,
        'signal_map': signal_map,
    }


def write_outputs(payload: Dict[str, Any], symbols_path: Path = SYMBOLS_PATH, external_json_path: Path = EXTERNAL_JSON_PATH) -> None:
    symbols = [normalize_symbol(symbol) for symbol in payload.get('symbols', [])]
    symbols = [symbol for symbol in dict.fromkeys(symbols) if symbol]
    signal_map = payload.get('signal_map', {})
    normalized_signal_map = {}
    for raw_symbol, raw_entry in signal_map.items():
        symbol = normalize_symbol(raw_symbol)
        if symbol and isinstance(raw_entry, dict):
            normalized_signal_map[symbol] = raw_entry
    full_payload = {
        'engine': str(payload.get('engine') or 'yaobiradar_v2'),
        'generated_at': int(payload.get('generated_at') or time.time()),
        'symbols': symbols,
        'signal_map': normalized_signal_map,
    }
    missing = [symbol for symbol in symbols if symbol not in normalized_signal_map]
    if missing:
        raise ValueError(f'missing signal_map entries for symbols: {missing}')
    atomic_write_text(symbols_path, ''.join(f'{symbol}\n' for symbol in symbols))
    atomic_write_text(external_json_path, json.dumps(full_payload, ensure_ascii=False, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    sample_rows = [
        {
            'symbol': 'DOGE',
            'score': '91.2',
            'tier': 'critical',
            'position_size_pct': 3.0,
            'reasons': ['oi_surge', 'breakout'],
        },
        {
            'ticker': 'SUIUSDT',
            'final_score': 81.5,
            'alert_tier': 'high',
            'suggested_position_size_pct': 2.0,
            'signals': ['watchlist'],
        },
    ]
    payload = build_payload(sample_rows)
    write_outputs(payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
