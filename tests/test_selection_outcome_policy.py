from __future__ import annotations

import datetime
import gzip
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_module():
    spec = importlib.util.spec_from_file_location(
        'selection_outcome_policy_test',
        SCRIPTS_DIR / 'selection_outcome_policy.py',
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def trade_events(index: int, realized_r: float, trigger: str) -> list[dict]:
    symbol = f'C{index}USDT'
    return [
        {
            'event_type': 'entry_filled',
            'symbol': symbol,
            'side': 'LONG',
            'trigger_class': trigger,
            'liquidity_grade': 'A',
            'market_regime_label': 'TREND',
            'relative_selection_percentile': 0.9 if trigger == 'BREAKOUT' else 0.1,
            'selection_htf_alignment': 1.0 if trigger == 'BREAKOUT' else -1.0,
        },
        {
            'event_type': 'trade_invalidated',
            'symbol': symbol,
            'side': 'LONG',
            'realized_r': realized_r,
        },
    ]


def candidate(trigger: str):
    return SimpleNamespace(
        symbol='TESTUSDT',
        side='LONG',
        score=100.0,
        reasons=[],
        trigger_class=trigger,
        liquidity_grade='A',
        market_regime_label='TREND',
        relative_selection_percentile=0.9 if trigger == 'BREAKOUT' else 0.1,
        selection_htf_alignment=1.0 if trigger == 'BREAKOUT' else -1.0,
    )


def test_extract_closed_outcomes_backfills_entry_features():
    mod = load_module()
    rows = trade_events(1, 1.25, 'BREAKOUT')
    outcomes = mod.extract_closed_selection_outcomes(rows)
    assert len(outcomes) == 1
    assert outcomes[0]['realized_r'] == 1.25
    assert outcomes[0]['trigger_class'] == 'BREAKOUT'
    assert outcomes[0]['relative_selection_percentile'] == 0.9


def test_profitable_historical_bucket_is_boosted_and_losing_bucket_is_deweighted():
    mod = load_module()
    rows = []
    for index in range(10):
        rows.extend(trade_events(index, 1.0, 'BREAKOUT'))
    for index in range(10, 20):
        rows.extend(trade_events(index, -1.0, 'PULLBACK'))
    calibration = mod.build_selection_outcome_calibration(rows)

    good_multiplier, good_evidence, good_score = mod.compute_selection_outcome_multiplier(
        candidate('BREAKOUT'), calibration
    )
    bad_multiplier, bad_evidence, bad_score = mod.compute_selection_outcome_multiplier(
        candidate('PULLBACK'), calibration
    )
    assert calibration['sample_count'] == 20
    assert good_evidence >= 1
    assert bad_evidence >= 1
    assert good_score > 0
    assert bad_score < 0
    assert 1.0 < good_multiplier <= 1.04
    assert 0.96 <= bad_multiplier < 1.0


def test_insufficient_global_samples_are_neutral():
    mod = load_module()
    rows = []
    for index in range(12):
        rows.extend(trade_events(index, 1.0, 'BREAKOUT'))
    calibration = mod.build_selection_outcome_calibration(rows)
    multiplier, evidence, score = mod.compute_selection_outcome_multiplier(candidate('BREAKOUT'), calibration)
    assert multiplier == 1.0
    assert evidence == 0
    assert score == 0.0


def test_bucket_below_minimum_samples_is_not_used():
    mod = load_module()
    rows = []
    for index in range(20):
        rows.extend(trade_events(index, 0.0, 'BASE'))
    for index in range(20, 27):
        rows.extend(trade_events(index, 2.0, 'RARE'))
    calibration = mod.build_selection_outcome_calibration(rows)
    rare = candidate('RARE')
    rare.liquidity_grade = 'UNKNOWN'
    rare.market_regime_label = 'UNKNOWN'
    rare.relative_selection_percentile = None
    rare.selection_htf_alignment = None
    rare.side = 'UNKNOWN'
    multiplier, evidence, _score = mod.compute_selection_outcome_multiplier(rare, calibration)
    assert evidence == 0
    assert multiplier == 1.0


def test_apply_is_recomputed_from_current_base_score_not_stale_base():
    mod = load_module()
    rows = []
    for index in range(10):
        rows.extend(trade_events(index, 1.0, 'BREAKOUT'))
    for index in range(10, 20):
        rows.extend(trade_events(index, -1.0, 'PULLBACK'))
    calibration = mod.build_selection_outcome_calibration(rows)
    row = candidate('BREAKOUT')
    mod.apply_selection_outcome_calibration([row], calibration)
    first_multiplier = row.selection_outcome_multiplier
    row.score = 200.0
    mod.apply_selection_outcome_calibration([row], calibration)
    assert row.selection_outcome_base_score == 200.0
    assert row.score == round(200.0 * first_multiplier, 4)
    assert len([reason for reason in row.reasons if reason.startswith('selection_outcome=')]) == 1


def test_loader_reads_rotated_gzip_and_current_events(tmp_path):
    mod = load_module()
    current = tmp_path / 'events.jsonl'
    rotated = tmp_path / 'events.jsonl.20260830T000000Z.gz'
    with gzip.open(rotated, 'wt', encoding='utf-8') as fh:
        fh.write(json.dumps({'event_type': 'entry_filled', 'symbol': 'OLDUSDT', 'side': 'LONG'}) + '\n')
    current.write_text(
        json.dumps({'event_type': 'trade_invalidated', 'symbol': 'NEWUSDT', 'side': 'LONG', 'realized_r': 1}) + '\n',
        encoding='utf-8',
    )

    class Store:
        def _events_path(self):
            return current

    rows = mod.load_selection_outcome_events(Store())
    assert [row['symbol'] for row in rows] == ['OLDUSDT', 'NEWUSDT']


def test_multiplier_is_bounded():
    mod = load_module()
    calibration = {
        'sample_count': 100,
        'buckets': {
            'trigger:BREAKOUT': {
                'sample_count': 100,
                'shrunk_advantage': 10.0,
            },
        },
    }
    row = candidate('BREAKOUT')
    row.liquidity_grade = 'UNKNOWN'
    row.market_regime_label = 'UNKNOWN'
    row.relative_selection_percentile = None
    row.selection_htf_alignment = None
    row.side = 'UNKNOWN'
    multiplier, evidence, _score = mod.compute_selection_outcome_multiplier(row, calibration)
    assert evidence == 1
    assert multiplier == 1.04


def test_old_timestamped_outcomes_expire_from_effective_evidence():
    mod = load_module()
    now = datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc)
    rows = []
    for index in range(10):
        pair = trade_events(index, 2.0, 'BREAKOUT')
        pair[-1]['closed_at'] = '2026-06-01T00:00:00Z'
        rows.extend(pair)
    for index in range(10, 20):
        pair = trade_events(index, -1.0, 'BREAKOUT')
        pair[-1]['closed_at'] = '2026-08-30T00:00:00Z'
        rows.extend(pair)
    calibration = mod.build_selection_outcome_calibration(rows, now=now)
    trigger_bucket = calibration['buckets']['trigger:BREAKOUT']
    assert calibration['sample_count'] == 20
    assert calibration['effective_sample_count'] < 11.0
    assert trigger_bucket['effective_sample_count'] < 11.0
    assert trigger_bucket['avg_realized_r'] < 0


def test_regime_conditioned_evidence_separates_same_trigger_across_market_states():
    mod = load_module()
    rows = []
    for index in range(10):
        rows.extend(trade_events(index, 1.2, 'BREAKOUT'))
    for index in range(10, 20):
        pair = trade_events(index, -1.2, 'BREAKOUT')
        pair[0]['market_regime_label'] = 'RANGE'
        rows.extend(pair)
    calibration = mod.build_selection_outcome_calibration(rows)
    trend = candidate('BREAKOUT')
    range_candidate = candidate('BREAKOUT')
    range_candidate.market_regime_label = 'RANGE'
    trend_multiplier, trend_evidence, trend_score = mod.compute_selection_outcome_multiplier(trend, calibration)
    range_multiplier, range_evidence, range_score = mod.compute_selection_outcome_multiplier(range_candidate, calibration)
    assert trend_evidence >= 2
    assert range_evidence >= 2
    assert trend_score > range_score
    assert trend_multiplier > range_multiplier


def test_hook_is_idempotent():
    mod = load_module()

    def rerank(rows):
        return list(rows)

    def run_scan(*args, **kwargs):
        return 'ok'

    class Store:
        def read_events(self, limit=1000):
            return []

    strategy = SimpleNamespace(
        run_scan_once=run_scan,
        get_runtime_state_store=lambda args: Store(),
    )
    relative = SimpleNamespace(rerank_candidate_cohort=rerank)
    mod.install_selection_outcome_hook(relative, strategy)
    wrapped_run = strategy.run_scan_once
    wrapped_rerank = relative.rerank_candidate_cohort
    mod.install_selection_outcome_hook(relative, strategy)
    assert strategy.run_scan_once is wrapped_run
    assert relative.rerank_candidate_cohort is wrapped_rerank
