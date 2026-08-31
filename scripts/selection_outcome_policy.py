from __future__ import annotations

import datetime
import gzip
import json
import math
import threading
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, TextIO

_STATE = threading.local()
MIN_GLOBAL_SAMPLES = 20
MIN_BUCKET_SAMPLES = 8
MIN_EFFECTIVE_BUCKET_SAMPLES = 4.0
SHRINKAGE_SAMPLES = 12.0
MAX_EVENT_ROWS = 10000
MAX_MULTIPLIER_ADJUSTMENT = 0.04
OUTCOME_HALF_LIFE_DAYS = 14.0
MAX_OUTCOME_AGE_DAYS = 60.0


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _text(value: Any, default: str = 'unknown') -> str:
    text = str(value or '').strip().upper()
    return text or default.upper()


def _side(value: Any) -> str:
    text = _text(value)
    if text in {'SELL', 'SHORT'}:
        return 'SHORT'
    if text in {'BUY', 'LONG'}:
        return 'LONG'
    return text


def _relative_band(value: Any) -> str:
    percentile = _num(value)
    if percentile is None:
        return 'UNKNOWN'
    if percentile >= 0.80:
        return 'TOP20'
    if percentile >= 0.60:
        return 'UPPER'
    if percentile >= 0.40:
        return 'MID'
    if percentile >= 0.20:
        return 'LOWER'
    return 'BOTTOM20'


def _htf_band(value: Any) -> str:
    alignment = _num(value)
    if alignment is None:
        return 'UNKNOWN'
    if alignment >= 0.75:
        return 'ALIGNED'
    if alignment <= -0.75:
        return 'OPPOSED'
    return 'MIXED'


def _event_key(row: dict[str, Any]) -> tuple[str, str]:
    symbol = _text(row.get('symbol'), default='')
    side = _side(row.get('position_side') or row.get('side'))
    return symbol, side


def _parse_time(value: Any) -> datetime.datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)


def _outcome_weight(row: dict[str, Any], now: datetime.datetime) -> float:
    event_time = _parse_time(row.get('closed_at') or row.get('recorded_at'))
    if event_time is None:
        return 1.0
    age_days = max((now - event_time).total_seconds() / 86400.0, 0.0)
    if age_days > MAX_OUTCOME_AGE_DAYS:
        return 0.0
    return math.exp(-math.log(2.0) * age_days / OUTCOME_HALF_LIFE_DAYS)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        fh: TextIO
        if path.suffix == '.gz':
            fh = gzip.open(path, mode='rt', encoding='utf-8')
        else:
            fh = path.open(mode='r', encoding='utf-8')
        with fh:
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
    except Exception:
        return []
    return rows


def load_selection_outcome_events(store: Any, limit: int = MAX_EVENT_ROWS) -> list[dict[str, Any]]:
    """Load current and retained rotated events without making calibration day-boundary sensitive."""
    max_rows = max(int(limit or 0), 1)
    path_getter = getattr(store, '_events_path', None)
    if callable(path_getter):
        try:
            current = Path(path_getter())
            candidates = sorted(
                current.parent.glob(f'{current.name}.*.gz'),
                key=lambda path: path.stat().st_mtime,
            )
            candidates.append(current)
            rows: list[dict[str, Any]] = []
            for path in candidates:
                if path.exists():
                    rows.extend(_read_jsonl(path))
            return rows[-max_rows:]
        except Exception:
            pass
    reader = getattr(store, 'read_events', None)
    if callable(reader):
        try:
            rows = reader(limit=max_rows)
            return [dict(row) for row in rows if isinstance(row, dict)][-max_rows:]
        except Exception:
            return []
    return []


def extract_closed_selection_outcomes(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_candidate: dict[tuple[str, str], dict[str, Any]] = {}
    latest_entry: dict[tuple[str, str], dict[str, Any]] = {}
    outcomes: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        event_type = str(row.get('event_type') or '').strip()
        key = _event_key(row)
        if not key[0]:
            continue
        if event_type == 'candidate_selected':
            latest_candidate[key] = row
            continue
        if event_type in {'entry_filled', 'buy_fill_confirmed'}:
            snapshot = dict(latest_candidate.get(key, {}))
            snapshot.update(row)
            latest_entry[key] = snapshot
            continue
        if event_type != 'trade_invalidated':
            continue
        realized_r = _num(row.get('realized_r'))
        if realized_r is None:
            continue
        snapshot = dict(latest_entry.get(key) or latest_candidate.get(key) or {})
        snapshot.update(row)
        snapshot['realized_r'] = float(realized_r)
        outcomes.append(snapshot)
        latest_entry.pop(key, None)
    return outcomes


def _feature_keys(row: dict[str, Any]) -> dict[str, str]:
    regime = _text(row.get('market_regime_label') or row.get('regime_label'))
    trigger = _text(row.get('trigger_class'))
    liquidity = _text(
        row.get('liquidity_grade_at_entry')
        or row.get('execution_liquidity_grade')
        or row.get('liquidity_grade')
    )
    relative = _relative_band(row.get('relative_selection_percentile'))
    return {
        'trigger': trigger,
        'liquidity': liquidity,
        'regime': regime,
        'relative': relative,
        'htf': _htf_band(row.get('selection_htf_alignment')),
        'side': _side(row.get('position_side') or row.get('side')),
        'trigger_regime': f'{trigger}|{regime}',
        'liquidity_regime': f'{liquidity}|{regime}',
        'relative_regime': f'{relative}|{regime}',
    }


def build_selection_outcome_calibration(
    rows: Iterable[dict[str, Any]],
    *,
    now: datetime.datetime | None = None,
) -> dict[str, Any]:
    outcomes = extract_closed_selection_outcomes(rows)
    if not outcomes:
        return {'sample_count': 0, 'effective_sample_count': 0.0, 'avg_realized_r': 0.0, 'win_rate': 0.0, 'buckets': {}}
    effective_now = now or datetime.datetime.now(datetime.timezone.utc)
    weighted_outcomes = [(row, _outcome_weight(row, effective_now)) for row in outcomes]
    weighted_outcomes = [(row, weight) for row, weight in weighted_outcomes if weight > 0]
    if not weighted_outcomes:
        return {'sample_count': len(outcomes), 'effective_sample_count': 0.0, 'avg_realized_r': 0.0, 'win_rate': 0.0, 'buckets': {}}

    global_count = len(outcomes)
    total_weight = sum(weight for _row, weight in weighted_outcomes)
    global_avg = sum(float(row['realized_r']) * weight for row, weight in weighted_outcomes) / total_weight
    global_win = sum((1.0 if float(row['realized_r']) > 0 else 0.0) * weight for row, weight in weighted_outcomes) / total_weight
    grouped: dict[tuple[str, str], list[tuple[float, float]]] = defaultdict(list)
    for row, weight in weighted_outcomes:
        realized = float(row['realized_r'])
        for dimension, value in _feature_keys(row).items():
            if value not in {'', 'UNKNOWN', 'NONE', 'N/A'} and '|UNKNOWN' not in value and not value.startswith('UNKNOWN|'):
                grouped[(dimension, value)].append((realized, weight))

    buckets: dict[str, dict[str, Any]] = {}
    for (dimension, value), samples in grouped.items():
        count = len(samples)
        effective_count = sum(weight for _sample, weight in samples)
        avg_r = sum(sample * weight for sample, weight in samples) / effective_count
        win_rate = sum((1.0 if sample > 0 else 0.0) * weight for sample, weight in samples) / effective_count
        expectancy_component = math.tanh((avg_r - global_avg) / 0.5)
        win_component = max(-1.0, min((win_rate - global_win) / 0.35, 1.0))
        raw_advantage = (0.65 * expectancy_component) + (0.35 * win_component)
        reliability = effective_count / (effective_count + SHRINKAGE_SAMPLES)
        buckets[f'{dimension}:{value}'] = {
            'dimension': dimension,
            'value': value,
            'sample_count': count,
            'effective_sample_count': round(effective_count, 4),
            'avg_realized_r': round(avg_r, 4),
            'win_rate': round(win_rate, 4),
            'raw_advantage': round(raw_advantage, 4),
            'reliability': round(reliability, 4),
            'shrunk_advantage': round(raw_advantage * reliability, 4),
        }
    return {
        'sample_count': global_count,
        'effective_sample_count': round(total_weight, 4),
        'avg_realized_r': round(global_avg, 4),
        'win_rate': round(global_win, 4),
        'half_life_days': OUTCOME_HALF_LIFE_DAYS,
        'max_age_days': MAX_OUTCOME_AGE_DAYS,
        'buckets': buckets,
    }


def _candidate_feature_keys(candidate: Any) -> dict[str, str]:
    regime = _text(
        getattr(candidate, 'market_regime_label', '')
        or getattr(candidate, 'regime_label', '')
    )
    trigger = _text(getattr(candidate, 'trigger_class', ''))
    liquidity = _text(
        getattr(candidate, 'execution_liquidity_grade', '')
        or getattr(candidate, 'liquidity_grade', '')
    )
    relative = _relative_band(getattr(candidate, 'relative_selection_percentile', None))
    return {
        'trigger': trigger,
        'liquidity': liquidity,
        'regime': regime,
        'relative': relative,
        'htf': _htf_band(getattr(candidate, 'selection_htf_alignment', None)),
        'side': _side(getattr(candidate, 'position_side', None) or getattr(candidate, 'side', None)),
        'trigger_regime': f'{trigger}|{regime}',
        'liquidity_regime': f'{liquidity}|{regime}',
        'relative_regime': f'{relative}|{regime}',
    }


def compute_selection_outcome_multiplier(candidate: Any, calibration: dict[str, Any]) -> tuple[float, int, float]:
    if int(calibration.get('sample_count') or 0) < MIN_GLOBAL_SAMPLES:
        return 1.0, 0, 0.0
    buckets = calibration.get('buckets')
    if not isinstance(buckets, dict):
        return 1.0, 0, 0.0

    weights = {
        'trigger': 0.12,
        'liquidity': 0.08,
        'regime': 0.12,
        'relative': 0.08,
        'htf': 0.08,
        'side': 0.07,
        'trigger_regime': 0.20,
        'liquidity_regime': 0.12,
        'relative_regime': 0.13,
    }
    weighted = 0.0
    total_weight = 0.0
    evidence_count = 0
    for dimension, value in _candidate_feature_keys(candidate).items():
        if value in {'', 'UNKNOWN', 'NONE', 'N/A'} or '|UNKNOWN' in value or value.startswith('UNKNOWN|'):
            continue
        bucket = buckets.get(f'{dimension}:{value}')
        if not isinstance(bucket, dict) or int(bucket.get('sample_count') or 0) < MIN_BUCKET_SAMPLES:
            continue
        effective_samples = float(bucket.get('effective_sample_count', bucket.get('sample_count', 0)) or 0.0)
        if effective_samples < MIN_EFFECTIVE_BUCKET_SAMPLES:
            continue
        weight = weights[dimension]
        weighted += weight * float(bucket.get('shrunk_advantage') or 0.0)
        total_weight += weight
        evidence_count += 1
    if total_weight <= 0:
        return 1.0, 0, 0.0
    evidence_score = max(-1.0, min(weighted / total_weight, 1.0))
    multiplier = 1.0 + MAX_MULTIPLIER_ADJUSTMENT * evidence_score
    return round(max(0.96, min(multiplier, 1.04)), 4), evidence_count, round(evidence_score, 4)


def apply_selection_outcome_calibration(candidates: Iterable[Any], calibration: dict[str, Any]) -> list[Any]:
    cohort = list(candidates)
    for candidate in cohort:
        base = float(getattr(candidate, 'score', 0.0) or 0.0)
        multiplier, evidence_count, evidence_score = compute_selection_outcome_multiplier(candidate, calibration)
        candidate.selection_outcome_base_score = round(base, 4)
        candidate.selection_outcome_multiplier = multiplier
        candidate.selection_outcome_evidence_count = evidence_count
        candidate.selection_outcome_evidence_score = evidence_score
        candidate.selection_outcome_sample_count = int(calibration.get('sample_count') or 0)
        candidate.selection_outcome_effective_sample_count = round(float(calibration.get('effective_sample_count', candidate.selection_outcome_sample_count) or 0.0), 4)
        candidate.score = round(base * multiplier, 4)
        reasons = [
            reason for reason in list(getattr(candidate, 'reasons', []) or [])
            if not str(reason).startswith('selection_outcome=')
        ]
        reasons.append(
            'selection_outcome='
            f'samples={candidate.selection_outcome_sample_count}:'
            f'effective={candidate.selection_outcome_effective_sample_count:.2f}:'
            f'evidence={evidence_count}:score={evidence_score:.4f}:multiplier={multiplier:.4f}'
        )
        candidate.reasons = reasons
    return cohort


def install_selection_outcome_hook(relative_selection_module: Any, strategy_module: Any) -> None:
    original_rerank = getattr(relative_selection_module, 'rerank_candidate_cohort', None)
    original_run_scan = getattr(strategy_module, 'run_scan_once', None)
    get_store = getattr(strategy_module, 'get_runtime_state_store', None)
    if not callable(original_rerank) or not callable(original_run_scan) or not callable(get_store):
        return
    if getattr(original_run_scan, '_selection_outcome_hook', False):
        return

    def rerank_with_outcomes(candidates: Iterable[Any]):
        cohort = original_rerank(candidates)
        if getattr(_STATE, 'active', False):
            calibration = getattr(_STATE, 'calibration', {})
            cohort = apply_selection_outcome_calibration(cohort, calibration)
        return cohort

    def run_scan_with_outcomes(*args: Any, **kwargs: Any):
        scan_args = kwargs.get('args')
        if scan_args is None and len(args) >= 2:
            scan_args = args[1]
        if scan_args is None:
            return original_run_scan(*args, **kwargs)
        store = get_store(scan_args)
        try:
            events = load_selection_outcome_events(store)
            calibration = build_selection_outcome_calibration(events)
        except Exception:
            calibration = {'sample_count': 0, 'effective_sample_count': 0.0, 'avg_realized_r': 0.0, 'win_rate': 0.0, 'buckets': {}}

        previous_active = getattr(_STATE, 'active', False)
        previous_calibration = getattr(_STATE, 'calibration', None)
        _STATE.active = True
        _STATE.calibration = calibration
        try:
            return original_run_scan(*args, **kwargs)
        finally:
            _STATE.active = previous_active
            _STATE.calibration = previous_calibration

    rerank_with_outcomes._selection_outcome_hook = True  # type: ignore[attr-defined]
    run_scan_with_outcomes._selection_outcome_hook = True  # type: ignore[attr-defined]
    relative_selection_module.rerank_candidate_cohort = rerank_with_outcomes
    strategy_module.run_scan_once = run_scan_with_outcomes
