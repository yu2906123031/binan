#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Sequence

import yaobiradar_v2_output_writer as writer

DEFAULT_ENGINE = 'yaobiradar_v2'
DEFAULT_INPUT_PATH = writer.SYMBOLS_PATH.parent / 'yaobiradar_v2_candidates.json'


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


def _reason_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _load_symbol_lines(path: Path) -> List[str]:
    rows: List[str] = []
    for line in path.read_text(encoding='utf-8').splitlines():
        text = line.strip()
        if not text or text.startswith('#'):
            continue
        symbol = writer.normalize_symbol(text)
        if symbol:
            rows.append(symbol)
    return list(dict.fromkeys(rows))


def classify_tier(score: float, blocked: bool) -> str:
    if blocked:
        return 'blocked'
    if score >= 90.0:
        return 'critical'
    if score >= 75.0:
        return 'high'
    return 'watch'


def position_size_pct_for_tier(tier: str) -> float:
    return {
        'critical': 3.0,
        'high': 2.0,
        'watch': 1.0,
        'blocked': 0.0,
    }.get(str(tier).lower(), 1.0)


def compute_score(candidate: Dict[str, Any]) -> float:
    return round(
        _to_float(candidate.get('hot_score'))
        + _to_float(candidate.get('momentum_score'))
        + _to_float(candidate.get('liquidity_score'))
        + _to_float(candidate.get('breakout_score')),
        4,
    )


def build_candidates_from_symbols_file(path: Path) -> List[Dict[str, Any]]:
    return [
        {
            'symbol': symbol,
            'hot_score': 0.0,
            'momentum_score': 0.0,
            'liquidity_score': 0.0,
            'breakout_score': 0.0,
            'reasons': ['square_symbols_fallback'],
        }
        for symbol in _load_symbol_lines(path)
    ]


def refresh_candidates_from_square_symbols(
    square_symbols_path: Path = writer.SYMBOLS_PATH,
    candidates_path: Path = DEFAULT_INPUT_PATH,
) -> List[Dict[str, Any]]:
    rows = build_candidates_from_symbols_file(square_symbols_path)
    candidates_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    return rows


def build_rows(candidates: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    scored = []
    for candidate in candidates:
        score = compute_score(candidate)
        blocked = bool(candidate.get('blocked'))
        veto_reason = str(candidate.get('block_reason') or candidate.get('veto_reason') or '').strip()
        scored.append((score, writer.normalize_symbol(candidate.get('symbol')), blocked, veto_reason, candidate))

    ordered = sorted(scored, key=lambda item: (-item[0], item[1]))
    for index, (score, symbol, blocked, veto_reason, candidate) in enumerate(ordered, start=1):
        if not symbol:
            continue
        tier = classify_tier(score, blocked)
        reasons = _reason_list(candidate.get('reasons'))
        if blocked and veto_reason and veto_reason not in reasons:
            reasons.append(veto_reason)
        reasons.append(f'composite_rank={index}')
        row: Dict[str, Any] = {
            'symbol': symbol,
            'external_signal_score': score,
            'external_signal_tier': tier,
            'external_position_size_pct': position_size_pct_for_tier(tier),
            'external_reasons': reasons,
        }
        for source_key, output_key in (
            ('portfolio_narrative_bucket', 'portfolio_narrative_bucket'),
            ('narrative_bucket', 'portfolio_narrative_bucket'),
            ('theme_bucket', 'portfolio_narrative_bucket'),
            ('portfolio_theme', 'portfolio_narrative_bucket'),
            ('portfolio_correlation_group', 'portfolio_correlation_group'),
            ('correlation_group', 'portfolio_correlation_group'),
            ('correlation_bucket', 'portfolio_correlation_group'),
        ):
            value = str(candidate.get(source_key) or '').strip()
            if value and output_key not in row:
                row[output_key] = value
        if blocked:
            row['external_veto'] = True
            row['external_veto_reason'] = veto_reason or 'blocked'
        ranked.append(row)
    return ranked


def build_payload(candidates: Sequence[Dict[str, Any]], engine: str = DEFAULT_ENGINE) -> Dict[str, Any]:
    rows = build_rows(candidates)
    return writer.build_payload(rows, engine=engine)


def write_from_candidates(
    candidates: Sequence[Dict[str, Any]],
    engine: str = DEFAULT_ENGINE,
    symbols_path: Path = writer.SYMBOLS_PATH,
    external_json_path: Path = writer.EXTERNAL_JSON_PATH,
) -> Dict[str, Any]:
    payload = build_payload(candidates, engine=engine)
    writer.write_outputs(payload, symbols_path=symbols_path, external_json_path=external_json_path)
    return payload


def load_candidates(path: Path = DEFAULT_INPUT_PATH) -> List[Dict[str, Any]]:
    raw = json.loads(path.read_text(encoding='utf-8'))
    if isinstance(raw, dict):
        candidates = raw.get('rows') or raw.get('candidates') or raw.get('items') or []
    else:
        candidates = raw
    if not isinstance(candidates, list):
        raise ValueError('candidate payload must be a list or object containing rows/candidates/items list')
    return [item for item in candidates if isinstance(item, dict)]


def run(
    candidates: Sequence[Dict[str, Any]],
    engine: str = DEFAULT_ENGINE,
    symbols_path: Path = writer.SYMBOLS_PATH,
    external_json_path: Path = writer.EXTERNAL_JSON_PATH,
) -> Dict[str, Any]:
    return write_from_candidates(candidates, engine=engine, symbols_path=symbols_path, external_json_path=external_json_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Generate yaobiradar external signal payloads from candidate rows or square-symbol fallback input.')
    parser.add_argument('--refresh-from-square-symbols', action='store_true')
    parser.add_argument('--square-symbols-path', default=str(writer.SYMBOLS_PATH))
    parser.add_argument('--input-path', default=str(DEFAULT_INPUT_PATH))
    parser.add_argument('--symbols-output', default=str(writer.SYMBOLS_PATH))
    parser.add_argument('--external-json-output', default=str(writer.EXTERNAL_JSON_PATH))
    parser.add_argument('--print-json', action='store_true')
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    input_path = Path(args.input_path)
    if args.refresh_from_square_symbols:
        refresh_candidates_from_square_symbols(Path(args.square_symbols_path), input_path)
    candidates = load_candidates(input_path)
    payload = write_from_candidates(
        candidates,
        symbols_path=Path(args.symbols_output),
        external_json_path=Path(args.external_json_output),
    )
    if args.print_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
