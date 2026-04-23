#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Sequence

import yaobiradar_v2_output_writer as writer

DEFAULT_ENGINE = 'yaobiradar_v2'
DEFAULT_INPUT_PATH = Path('/root/.hermes/yaobiradar_v2_candidates.json')


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


def main() -> int:
    candidates = load_candidates(DEFAULT_INPUT_PATH)
    payload = write_from_candidates(candidates)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
