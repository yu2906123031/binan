from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any, Iterable

ATTRIBUTION_EVENT_TYPE = 'layer_attribution_scan'
CLOSED_EVENT_TYPES = {'trade_invalidated', 'trade_closed', 'position_closed', 'exit_filled'}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _side(value: Any) -> str:
    text = str(value or '').strip().upper()
    return 'SHORT' if text in {'SHORT', 'SELL'} else 'LONG'


def _key(symbol: Any, side: Any) -> tuple[str, str]:
    return str(symbol or '').strip().upper(), _side(side)


def _outcome_scan_id(row: dict[str, Any]) -> str:
    direct = str(row.get('layer_attribution_scan_id') or '').strip()
    if direct:
        return direct
    snapshot = row.get('entry_prediction_snapshot')
    if isinstance(snapshot, dict):
        return str(snapshot.get('layer_attribution_scan_id') or '').strip()
    return ''


def index_attribution_scans(events: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    scans: dict[str, dict[str, Any]] = {}
    for row in events:
        if not isinstance(row, dict) or row.get('event_type') != ATTRIBUTION_EVENT_TYPE:
            continue
        scan_id = str(row.get('scan_id') or '').strip()
        if scan_id:
            scans[scan_id] = row
    return scans


def index_closed_outcomes(events: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    linked: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        if not isinstance(row, dict) or str(row.get('event_type') or '') not in CLOSED_EVENT_TYPES:
            continue
        scan_id = _outcome_scan_id(row)
        if scan_id:
            linked[scan_id].append(row)
    return dict(linked)


def _realized_r(row: dict[str, Any]) -> float | None:
    for field in ('net_realized_r', 'realized_r', 'pnl_r', 'net_r'):
        if row.get(field) not in (None, ''):
            return _num(row.get(field))
    return None


def _winner_by_stage(scan: dict[str, Any]) -> dict[str, tuple[str, str]]:
    result: dict[str, tuple[str, str]] = {}
    stages = scan.get('stages')
    if not isinstance(stages, list):
        return result
    for stage_row in stages:
        if not isinstance(stage_row, dict):
            continue
        winner = stage_row.get('winner')
        if not isinstance(winner, dict):
            continue
        stage = str(stage_row.get('stage') or '')
        symbol = winner.get('symbol')
        if stage and symbol:
            result[stage] = _key(symbol, winner.get('side'))
    return result


def build_layer_attribution_analysis(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in events if isinstance(row, dict)]
    scans = index_attribution_scans(rows)
    outcomes = index_closed_outcomes(rows)
    stage_values: dict[str, list[float]] = defaultdict(list)
    stage_hits: dict[str, int] = defaultdict(int)
    stage_selected: dict[str, int] = defaultdict(int)
    transition_delta: dict[tuple[str, str], list[float]] = defaultdict(list)
    opportunity_cost: dict[str, list[float]] = defaultdict(list)
    false_promotion: dict[str, int] = defaultdict(int)
    evaluated_scans = 0

    for scan_id, scan in scans.items():
        closed = outcomes.get(scan_id, [])
        by_candidate: dict[tuple[str, str], float] = {}
        for outcome in closed:
            value = _realized_r(outcome)
            symbol = outcome.get('symbol')
            if value is None or not symbol:
                continue
            by_candidate[_key(symbol, outcome.get('position_side') or outcome.get('side'))] = value
        if not by_candidate:
            continue
        evaluated_scans += 1
        winners = _winner_by_stage(scan)
        ordered_stages = [str(row.get('stage') or '') for row in scan.get('stages', []) if isinstance(row, dict)]
        previous_stage = ''
        previous_value: float | None = None
        for stage in ordered_stages:
            winner_key = winners.get(stage)
            if winner_key is None:
                continue
            stage_selected[stage] += 1
            value = by_candidate.get(winner_key)
            if value is not None:
                stage_values[stage].append(value)
                if value > 0:
                    stage_hits[stage] += 1
            if previous_stage and previous_value is not None and value is not None:
                transition_delta[(previous_stage, stage)].append(value - previous_value)
                if winner_key != winners.get(previous_stage):
                    if previous_value > value:
                        opportunity_cost[stage].append(previous_value - value)
                    if value < 0 <= previous_value:
                        false_promotion[stage] += 1
            previous_stage = stage
            previous_value = value

    stage_summary = []
    for stage in sorted(stage_selected):
        values = stage_values.get(stage, [])
        stage_summary.append({
            'stage': stage,
            'selected_count': stage_selected[stage],
            'closed_count': len(values),
            'avg_net_r': round(mean(values), 6) if values else None,
            'hit_rate': round(stage_hits[stage] / len(values), 6) if values else None,
            'false_promotion_count': false_promotion.get(stage, 0),
            'opportunity_cost_r': round(sum(opportunity_cost.get(stage, [])), 6),
        })

    transitions = []
    for (before, after), values in sorted(transition_delta.items()):
        transitions.append({
            'before': before,
            'after': after,
            'sample_count': len(values),
            'avg_incremental_net_r': round(mean(values), 6) if values else None,
            'positive_contribution_rate': round(sum(value > 0 for value in values) / len(values), 6) if values else None,
        })

    return {
        'attribution_scan_count': len(scans),
        'evaluated_scan_count': evaluated_scans,
        'closed_scan_count': len(outcomes),
        'stage_summary': stage_summary,
        'transitions': transitions,
    }
