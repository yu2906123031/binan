#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime
import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


DEFAULT_HERMES_HOME = Path(os.path.expanduser(os.getenv('HERMES_HOME', str(Path.home() / '.hermes'))))
DEFAULT_APP_HOME = DEFAULT_HERMES_HOME / 'binance-futures-momentum-long'
DEFAULT_RUNTIME_STATE_DIR = DEFAULT_APP_HOME / 'runtime-state'
DEFAULT_OUTPUT_JSON = DEFAULT_APP_HOME / 'trade-bucket-analysis.json'
DEFAULT_OUTPUT_MARKDOWN = DEFAULT_APP_HOME / 'trade-bucket-analysis.md'

PREDICTION_FIELDS = (
    'realizable_reward_r',
    'expected_reward_r',
    'expected_edge',
    'expected_total_fee_pct',
    'execution_slippage_buffer_pct',
    'min_profit_buffer_pct',
    'expected_slippage_pct',
    'expected_slippage_r',
    'predicted_slippage_bps',
    'stop_distance_pct',
    'trigger_confirmation_count',
    'trigger_confirmation_flags',
    'candidate_stage',
    'setup_ready',
    'trigger_fired',
    'score',
    'score_decile',
    'state',
    'alert_tier',
    'trigger_class',
    'market_regime_label',
    'market_regime_multiplier',
    'shadow_entry_price',
    'maker_or_taker',
    'execution_mode',
    'liquidity_grade_at_entry',
    'execution_liquidity_grade',
)


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


def _normalize_text(value: Any, default: str = 'unknown') -> str:
    text = str(value or '').strip()
    return text or default


def _parse_iso8601_utc(value: Any) -> Optional[datetime.datetime]:
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


def _side_key(value: Any) -> str:
    side = str(value or '').strip().upper()
    if side in {'BUY', 'LONG'}:
        return 'LONG'
    if side in {'SELL', 'SHORT'}:
        return 'SHORT'
    return side


def _prediction_snapshot(row: Dict[str, Any]) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {}
    for field in PREDICTION_FIELDS:
        value = row.get(field)
        if value not in (None, '', [], {}):
            snapshot[field] = value
    flags = row.get('trigger_confirmation_flags')
    if isinstance(flags, dict):
        flow_count = flags.get('breakout_flow_confirmation_count')
        if flow_count not in (None, ''):
            snapshot['breakout_flow_confirmation_count'] = flow_count
        min_volume = flags.get('breakout_min_volume_multiple')
        if min_volume not in (None, ''):
            snapshot['breakout_min_volume_multiple'] = min_volume
    if 'breakout_flow_confirmation_count' not in snapshot:
        flow_count = row.get('breakout_flow_confirmation_count')
        if flow_count not in (None, ''):
            snapshot['breakout_flow_confirmation_count'] = flow_count
    return snapshot


def _with_entry_fill(snapshot: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(snapshot)
    entry_price = _optional_float(row, 'entry_price', 'avg_price')
    if entry_price is not None and entry_price > 0:
        enriched['entry_price_at_fill'] = entry_price
    for field in ('maker_or_taker', 'execution_mode', 'liquidity_grade_at_entry', 'execution_liquidity_grade'):
        value = row.get(field)
        if value not in (None, ''):
            enriched[field] = value
    predicted_bps = _optional_float(enriched, 'predicted_slippage_bps')
    if predicted_bps is None:
        predicted_bps = _optional_float(row, 'predicted_slippage_bps')
    if predicted_bps is None:
        expected_slippage_pct = _optional_float(enriched, 'expected_slippage_pct')
        if expected_slippage_pct is not None:
            predicted_bps = expected_slippage_pct * 100.0
    if predicted_bps is not None:
        enriched['predicted_slippage_bps'] = _round(predicted_bps, 4)
    actual_bps = _optional_float(row, 'actual_fill_slippage_bps')
    if actual_bps is None and entry_price is not None and entry_price > 0:
        shadow_entry_price = _optional_float(enriched, 'shadow_entry_price')
        if shadow_entry_price is not None and shadow_entry_price > 0:
            actual_bps = abs(entry_price - shadow_entry_price) / shadow_entry_price * 10000.0
    if actual_bps is not None:
        enriched['actual_fill_slippage_bps'] = _round(actual_bps, 4)
    if predicted_bps is not None and actual_bps is not None:
        enriched['slippage_error_bps'] = _round(actual_bps - predicted_bps, 4)
    enriched['entry_fill_recorded_at'] = row.get('recorded_at')
    enriched['prediction_source_event'] = 'entry_filled'
    return enriched


def enrich_closed_trade_events(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest_by_symbol: Dict[str, Dict[str, Any]] = {}
    latest_by_symbol_side: Dict[tuple[str, str], Dict[str, Any]] = {}
    latest_entry_by_symbol: Dict[str, Dict[str, Any]] = {}
    latest_entry_by_symbol_side: Dict[tuple[str, str], Dict[str, Any]] = {}
    enriched: List[Dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        event_type = str(row.get('event_type') or '')
        symbol = _normalize_text(row.get('symbol'), default='').upper()
        side = _side_key(row.get('position_side') or row.get('side'))
        if event_type == 'candidate_selected' and symbol:
            snapshot = _prediction_snapshot(row)
            if snapshot:
                snapshot['prediction_source_event'] = 'candidate_selected'
                snapshot['prediction_recorded_at'] = row.get('recorded_at')
                latest_by_symbol[symbol] = snapshot
                if side:
                    latest_by_symbol_side[(symbol, side)] = snapshot
            continue
        if event_type in {'entry_filled', 'buy_fill_confirmed'} and symbol:
            snapshot = latest_by_symbol_side.get((symbol, side)) if side else None
            if snapshot is None:
                snapshot = latest_by_symbol.get(symbol, {})
            entry_snapshot = _with_entry_fill(snapshot or {}, row)
            latest_entry_by_symbol[symbol] = entry_snapshot
            if side:
                latest_entry_by_symbol_side[(symbol, side)] = entry_snapshot
            continue
        if event_type != 'trade_invalidated':
            continue
        snapshot = latest_entry_by_symbol_side.get((symbol, side)) if symbol and side else None
        if snapshot is None and symbol:
            snapshot = latest_entry_by_symbol.get(symbol)
        if snapshot is None:
            snapshot = latest_by_symbol_side.get((symbol, side)) if symbol and side else None
        if snapshot is None and symbol:
            snapshot = latest_by_symbol.get(symbol)
        if snapshot:
            for key, value in snapshot.items():
                if row.get(key) in (None, '', [], {}):
                    row[key] = value
            row['prediction_snapshot_backfilled'] = True
        else:
            row.setdefault('prediction_snapshot_backfilled', False)
        enriched.append(row)
        if symbol and side:
            latest_entry_by_symbol_side.pop((symbol, side), None)
        if symbol:
            latest_entry_by_symbol.pop(symbol, None)
    return enriched


def filter_closed_trade_events(
    rows: Iterable[Dict[str, Any]],
    symbol: str = '',
    lookback_days: int = 0,
    now: Optional[datetime.datetime] = None,
) -> List[Dict[str, Any]]:
    target_symbol = _normalize_text(symbol, default='').upper()
    effective_now = now or datetime.datetime.now(datetime.timezone.utc)
    min_time = None
    if int(lookback_days or 0) > 0:
        min_time = effective_now - datetime.timedelta(days=int(lookback_days))
    filtered: List[Dict[str, Any]] = []
    for row in enrich_closed_trade_events(rows):
        row_symbol = _normalize_text(row.get('symbol'), default='').upper()
        if target_symbol and row_symbol != target_symbol:
            continue
        event_time = _parse_iso8601_utc(row.get('closed_at') or row.get('recorded_at'))
        if min_time is not None and event_time is not None and event_time < min_time:
            continue
        filtered.append(row)
    return filtered


def _count_table(counter: Counter, key_name: str) -> List[Dict[str, Any]]:
    ordered = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return [{key_name: key, 'count': count} for key, count in ordered]


def _optional_float(row: Dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        value = row.get(key)
        if value not in (None, ''):
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None


def _edge_calibration(closed_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    samples: List[Dict[str, float]] = []
    backfilled_samples = 0
    for row in closed_rows:
        predicted = _optional_float(row, 'realizable_reward_r', 'expected_reward_r')
        if predicted is None:
            expected_edge = _optional_float(row, 'expected_edge')
            stop_pct = _optional_float(row, 'stop_distance_pct', 'initial_stop_distance_pct')
            if expected_edge is not None and stop_pct is not None and stop_pct > 0:
                predicted = expected_edge / stop_pct
        if predicted is None or predicted <= 0:
            continue
        if row.get('prediction_snapshot_backfilled'):
            backfilled_samples += 1
        samples.append({'predicted': predicted, 'realized': _to_float(row.get('realized_r'))})

    if not samples:
        return {
            'sample_count': 0,
            'backfilled_sample_count': 0,
            'avg_predicted_reward_r': None,
            'avg_realized_r': None,
            'calibration_ratio': None,
            'mean_error_r': None,
            'mean_absolute_error_r': None,
        }
    count = len(samples)
    predicted_sum = sum(item['predicted'] for item in samples)
    realized_sum = sum(item['realized'] for item in samples)
    errors = [item['realized'] - item['predicted'] for item in samples]
    return {
        'sample_count': count,
        'backfilled_sample_count': backfilled_samples,
        'avg_predicted_reward_r': _round(predicted_sum / count, 4),
        'avg_realized_r': _round(realized_sum / count, 4),
        'calibration_ratio': _round(realized_sum / predicted_sum if predicted_sum else 0.0, 4),
        'mean_error_r': _round(sum(errors) / count, 4),
        'mean_absolute_error_r': _round(sum(abs(error) for error in errors) / count, 4),
    }


def _slippage_calibration(closed_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    samples: List[Dict[str, float]] = []
    for row in closed_rows:
        predicted = _optional_float(row, 'predicted_slippage_bps')
        if predicted is None:
            expected_slippage_pct = _optional_float(row, 'expected_slippage_pct')
            if expected_slippage_pct is not None:
                predicted = expected_slippage_pct * 100.0
        actual = _optional_float(row, 'actual_fill_slippage_bps')
        if predicted is None or actual is None:
            continue
        samples.append({'predicted': predicted, 'actual': actual})
    if not samples:
        return {
            'sample_count': 0,
            'avg_predicted_slippage_bps': None,
            'avg_actual_slippage_bps': None,
            'actual_to_predicted_ratio': None,
            'mean_error_bps': None,
            'mean_absolute_error_bps': None,
            'underprediction_rate_pct': None,
        }
    count = len(samples)
    predicted_sum = sum(item['predicted'] for item in samples)
    actual_sum = sum(item['actual'] for item in samples)
    errors = [item['actual'] - item['predicted'] for item in samples]
    underpredicted = sum(1 for item in samples if item['actual'] > item['predicted'])
    return {
        'sample_count': count,
        'avg_predicted_slippage_bps': _round(predicted_sum / count, 4),
        'avg_actual_slippage_bps': _round(actual_sum / count, 4),
        'actual_to_predicted_ratio': _round(actual_sum / predicted_sum, 4) if predicted_sum else None,
        'mean_error_bps': _round(sum(errors) / count, 4),
        'mean_absolute_error_bps': _round(sum(abs(error) for error in errors) / count, 4),
        'underprediction_rate_pct': _round((underpredicted / count) * 100.0, 2),
    }


def _slippage_calibration_by_dimension(closed_rows: List[Dict[str, Any]], dimension: str) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in closed_rows:
        if dimension == 'side':
            key = _side_key(row.get('position_side') or row.get('side')) or 'unknown'
        elif dimension == 'maker_or_taker':
            key = _normalize_text(row.get('maker_or_taker') or row.get('execution_mode'))
        elif dimension == 'liquidity_grade':
            key = _normalize_text(row.get('liquidity_grade_at_entry') or row.get('execution_liquidity_grade') or row.get('liquidity_grade'))
        elif dimension == 'market_regime_label':
            key = _normalize_text(row.get('market_regime_label'))
        else:
            key = _normalize_text(row.get(dimension))
        grouped[key].append(row)
    output: List[Dict[str, Any]] = []
    for key, group_rows in grouped.items():
        calibration = _slippage_calibration(group_rows)
        if calibration['sample_count'] <= 0:
            continue
        output.append({dimension: key, **calibration})
    return sorted(output, key=lambda item: (-int(item['sample_count']), str(item.get(dimension, ''))))


def _prediction_coverage(closed_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(closed_rows)
    with_prediction = 0
    with_slippage = 0
    with_actual_slippage = 0
    with_flow = 0
    backfilled = 0
    for row in closed_rows:
        if _optional_float(row, 'realizable_reward_r', 'expected_reward_r', 'expected_edge') is not None:
            with_prediction += 1
        if _optional_float(row, 'predicted_slippage_bps', 'expected_slippage_pct', 'expected_slippage_r', 'execution_slippage_buffer_pct') is not None:
            with_slippage += 1
        if _optional_float(row, 'actual_fill_slippage_bps') is not None:
            with_actual_slippage += 1
        if row.get('breakout_flow_confirmation_count') not in (None, '') or row.get('trigger_confirmation_count') not in (None, ''):
            with_flow += 1
        if row.get('prediction_snapshot_backfilled'):
            backfilled += 1
    return {
        'closed_trade_count': total,
        'with_edge_prediction': with_prediction,
        'with_slippage_prediction': with_slippage,
        'with_actual_slippage': with_actual_slippage,
        'with_flow_confirmation': with_flow,
        'backfilled_from_candidate_selected': backfilled,
        'edge_prediction_coverage_pct': _round((with_prediction / total) * 100.0 if total else 0.0, 2),
        'slippage_prediction_coverage_pct': _round((with_slippage / total) * 100.0 if total else 0.0, 2),
        'actual_slippage_coverage_pct': _round((with_actual_slippage / total) * 100.0 if total else 0.0, 2),
        'flow_confirmation_coverage_pct': _round((with_flow / total) * 100.0 if total else 0.0, 2),
    }


def build_trade_bucket_analysis_payload(
    rows: Iterable[Dict[str, Any]],
    symbol: str = '',
    lookback_days: int = 0,
    now: Optional[datetime.datetime] = None,
) -> Dict[str, Any]:
    closed_rows = filter_closed_trade_events(rows, symbol=symbol, lookback_days=lookback_days, now=now)
    aggregates: Dict[tuple[str, str, str, str, str], Dict[str, float]] = defaultdict(lambda: {
        'count': 0.0, 'wins': 0.0, 'expectancy_sum': 0.0, 'mfe_sum': 0.0, 'mae_sum': 0.0,
        'time_to_1r_sum': 0.0, 'time_to_1r_count': 0.0, 'time_in_trade_sum': 0.0, 'time_in_trade_count': 0.0,
    })
    exit_reason_counter: Counter = Counter()
    symbol_counter: Counter = Counter()
    trigger_counter: Counter = Counter()
    state_counter: Counter = Counter()

    for row in closed_rows:
        regime = _normalize_text(row.get('market_regime_label'))
        side = _normalize_text(row.get('side')).upper()
        state = _normalize_text(row.get('state'))
        trigger_class = _normalize_text(row.get('trigger_class'))
        score_decile = _normalize_text(row.get('score_decile'))
        realized_r = _to_float(row.get('realized_r'))
        mfe_r = _to_float(row.get('mfe_r'))
        mae_r = _to_float(row.get('mae_r'))
        time_to_1r = row.get('time_to_1r_minutes', row.get('time_to_1r'))
        time_in_trade = row.get('time_in_trade_minutes')
        exit_reason = _normalize_text(row.get('exit_reason'))
        event_symbol = _normalize_text(row.get('symbol'))
        bucket = (regime, side, state, trigger_class, score_decile)
        aggregate = aggregates[bucket]
        aggregate['count'] += 1
        aggregate['wins'] += 1 if realized_r > 0 else 0
        aggregate['expectancy_sum'] += realized_r
        aggregate['mfe_sum'] += mfe_r
        aggregate['mae_sum'] += mae_r
        if time_to_1r not in (None, ''):
            aggregate['time_to_1r_sum'] += _to_float(time_to_1r)
            aggregate['time_to_1r_count'] += 1
        if time_in_trade not in (None, ''):
            aggregate['time_in_trade_sum'] += _to_float(time_in_trade)
            aggregate['time_in_trade_count'] += 1
        exit_reason_counter[exit_reason] += 1
        symbol_counter[event_symbol] += 1
        trigger_counter[trigger_class] += 1
        state_counter[state] += 1

    by_bucket: List[Dict[str, Any]] = []
    for bucket, aggregate in sorted(aggregates.items(), key=lambda item: (-item[1]['count'], -item[1]['expectancy_sum'], item[0])):
        regime, side, state, trigger_class, score_decile = bucket
        count = int(aggregate['count'])
        wins = int(aggregate['wins'])
        by_bucket.append({
            'market_regime_label': regime, 'side': side, 'state': state, 'trigger_class': trigger_class,
            'score_decile': score_decile, 'count': count,
            'win_rate_pct': _round((wins / count) * 100.0 if count else 0.0, 2),
            'avg_expectancy_r': _round(aggregate['expectancy_sum'] / count if count else 0.0, 4),
            'avg_mfe_r': _round(aggregate['mfe_sum'] / count if count else 0.0, 4),
            'avg_mae_r': _round(aggregate['mae_sum'] / count if count else 0.0, 4),
            'avg_time_to_1r_minutes': _round(aggregate['time_to_1r_sum'] / aggregate['time_to_1r_count'], 4) if aggregate['time_to_1r_count'] else None,
            'avg_time_in_trade_minutes': _round(aggregate['time_in_trade_sum'] / aggregate['time_in_trade_count'], 4) if aggregate['time_in_trade_count'] else None,
        })

    total_closed = len(closed_rows)
    total_wins = sum(1 for row in closed_rows if _to_float(row.get('realized_r')) > 0)
    total_expectancy = sum(_to_float(row.get('realized_r')) for row in closed_rows)
    total_mfe = sum(_to_float(row.get('mfe_r')) for row in closed_rows)
    total_mae = sum(_to_float(row.get('mae_r')) for row in closed_rows)
    return {
        'summary': {
            'symbol': _normalize_text(symbol, default='').upper(), 'lookback_days': int(lookback_days or 0),
            'total_closed_trades': total_closed, 'distinct_buckets': len(by_bucket),
            'win_rate_pct': _round((total_wins / total_closed) * 100.0 if total_closed else 0.0, 2),
            'avg_expectancy_r': _round(total_expectancy / total_closed if total_closed else 0.0, 4),
            'avg_mfe_r': _round(total_mfe / total_closed if total_closed else 0.0, 4),
            'avg_mae_r': _round(total_mae / total_closed if total_closed else 0.0, 4),
        },
        'edge_calibration': _edge_calibration(closed_rows),
        'slippage_calibration': _slippage_calibration(closed_rows),
        'slippage_calibration_by_side': _slippage_calibration_by_dimension(closed_rows, 'side'),
        'slippage_calibration_by_maker_or_taker': _slippage_calibration_by_dimension(closed_rows, 'maker_or_taker'),
        'slippage_calibration_by_liquidity_grade': _slippage_calibration_by_dimension(closed_rows, 'liquidity_grade'),
        'slippage_calibration_by_market_regime': _slippage_calibration_by_dimension(closed_rows, 'market_regime_label'),
        'prediction_coverage': _prediction_coverage(closed_rows),
        'by_bucket': by_bucket,
        'by_exit_reason': _count_table(exit_reason_counter, 'exit_reason'),
        'by_symbol': _count_table(symbol_counter, 'symbol'),
        'by_trigger_class': _count_table(trigger_counter, 'trigger_class'),
        'by_state': _count_table(state_counter, 'state'),
    }


def render_markdown_report(payload: Dict[str, Any]) -> str:
    lines = ['# Trade Bucket Analysis', '']
    summary = payload.get('summary', {})
    calibration = payload.get('edge_calibration', {})
    slippage = payload.get('slippage_calibration', {})
    coverage = payload.get('prediction_coverage', {})
    lines.extend([
        f"- symbol: {summary.get('symbol') or 'ALL'}",
        f"- lookback_days: {summary.get('lookback_days', 0)}",
        f"- total_closed_trades: {summary.get('total_closed_trades', 0)}",
        f"- distinct_buckets: {summary.get('distinct_buckets', 0)}",
        f"- win_rate_pct: {summary.get('win_rate_pct', 0)}",
        f"- avg_expectancy_r: {summary.get('avg_expectancy_r', 0)}", '',
        '## Edge calibration', '',
        f"- sample_count: {calibration.get('sample_count', 0)}",
        f"- backfilled_sample_count: {calibration.get('backfilled_sample_count', 0)}",
        f"- avg_predicted_reward_r: {calibration.get('avg_predicted_reward_r')}",
        f"- avg_realized_r: {calibration.get('avg_realized_r')}",
        f"- calibration_ratio: {calibration.get('calibration_ratio')}",
        f"- mean_error_r: {calibration.get('mean_error_r')}",
        f"- mean_absolute_error_r: {calibration.get('mean_absolute_error_r')}", '',
        '## Slippage calibration', '',
        f"- sample_count: {slippage.get('sample_count', 0)}",
        f"- avg_predicted_slippage_bps: {slippage.get('avg_predicted_slippage_bps')}",
        f"- avg_actual_slippage_bps: {slippage.get('avg_actual_slippage_bps')}",
        f"- actual_to_predicted_ratio: {slippage.get('actual_to_predicted_ratio')}",
        f"- mean_error_bps: {slippage.get('mean_error_bps')}",
        f"- mean_absolute_error_bps: {slippage.get('mean_absolute_error_bps')}",
        f"- underprediction_rate_pct: {slippage.get('underprediction_rate_pct')}", '',
        '## Prediction coverage', '',
        f"- edge_prediction_coverage_pct: {coverage.get('edge_prediction_coverage_pct', 0)}",
        f"- slippage_prediction_coverage_pct: {coverage.get('slippage_prediction_coverage_pct', 0)}",
        f"- actual_slippage_coverage_pct: {coverage.get('actual_slippage_coverage_pct', 0)}",
        f"- flow_confirmation_coverage_pct: {coverage.get('flow_confirmation_coverage_pct', 0)}",
        f"- backfilled_from_candidate_selected: {coverage.get('backfilled_from_candidate_selected', 0)}", '',
    ])

    def append_table(title: str, rows: List[Dict[str, Any]], columns: List[str]) -> None:
        lines.extend([f'## {title}', ''])
        if not rows:
            lines.extend(['_no rows_', ''])
            return
        lines.append('| ' + ' | '.join(columns) + ' |')
        lines.append('| ' + ' | '.join(['---'] * len(columns)) + ' |')
        for row in rows:
            lines.append('| ' + ' | '.join(str(row.get(column, '')) for column in columns) + ' |')
        lines.append('')

    slippage_columns = ['sample_count', 'avg_predicted_slippage_bps', 'avg_actual_slippage_bps', 'actual_to_predicted_ratio', 'mean_error_bps', 'mean_absolute_error_bps', 'underprediction_rate_pct']
    append_table('Slippage by side', payload.get('slippage_calibration_by_side', []), ['side', *slippage_columns])
    append_table('Slippage by maker or taker', payload.get('slippage_calibration_by_maker_or_taker', []), ['maker_or_taker', *slippage_columns])
    append_table('Slippage by liquidity grade', payload.get('slippage_calibration_by_liquidity_grade', []), ['liquidity_grade', *slippage_columns])
    append_table('Slippage by market regime', payload.get('slippage_calibration_by_market_regime', []), ['market_regime_label', *slippage_columns])
    append_table('By bucket', payload.get('by_bucket', []), ['market_regime_label', 'side', 'state', 'trigger_class', 'score_decile', 'count', 'win_rate_pct', 'avg_expectancy_r', 'avg_mfe_r', 'avg_mae_r', 'avg_time_to_1r_minutes', 'avg_time_in_trade_minutes'])
    append_table('By exit reason', payload.get('by_exit_reason', []), ['exit_reason', 'count'])
    append_table('By symbol', payload.get('by_symbol', []), ['symbol', 'count'])
    append_table('By trigger class', payload.get('by_trigger_class', []), ['trigger_class', 'count'])
    append_table('By state', payload.get('by_state', []), ['state', 'count'])
    return '\n'.join(lines).rstrip() + '\n'


def run(runtime_state_dir: Path, output_json_path: Path, output_markdown_path: Path, limit: int = 5000, symbol: str = '', lookback_days: int = 0) -> Dict[str, Any]:
    runtime_state_dir = Path(runtime_state_dir)
    payload = build_trade_bucket_analysis_payload(load_events(runtime_state_dir / 'events.jsonl', limit=limit), symbol=symbol, lookback_days=lookback_days)
    output_json_path = Path(output_json_path)
    output_markdown_path = Path(output_markdown_path)
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    output_markdown_path.write_text(render_markdown_report(payload), encoding='utf-8')
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description='Aggregate closed-trade analytics into bucketed expectancy tables.')
    parser.add_argument('--runtime-state-dir', default=str(DEFAULT_RUNTIME_STATE_DIR))
    parser.add_argument('--output-json', default=str(DEFAULT_OUTPUT_JSON))
    parser.add_argument('--output-markdown', default=str(DEFAULT_OUTPUT_MARKDOWN))
    parser.add_argument('--symbol', default='')
    parser.add_argument('--lookback-days', type=int, default=0)
    parser.add_argument('--limit', type=int, default=5000)
    args = parser.parse_args()
    payload = run(runtime_state_dir=Path(args.runtime_state_dir), output_json_path=Path(args.output_json), output_markdown_path=Path(args.output_markdown), limit=args.limit, symbol=args.symbol, lookback_days=args.lookback_days)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
