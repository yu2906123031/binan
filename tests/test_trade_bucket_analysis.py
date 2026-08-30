import importlib.util
import json
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'trade_bucket_analysis.py'
spec = importlib.util.spec_from_file_location('trade_bucket_analysis', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_build_trade_bucket_analysis_payload_aggregates_expectancy_and_mfe_mae_by_bucket():
    rows = [
        {'event_type': 'trade_invalidated', 'symbol': 'DOGEUSDT', 'market_regime_label': 'risk_on', 'side': 'LONG', 'state': 'launch', 'trigger_class': 'breakout', 'score_decile': '80-89', 'realized_r': 1.2, 'mfe_r': 1.8, 'mae_r': 0.4, 'time_to_1r': 12.0, 'time_in_trade_minutes': 35.0, 'exit_reason': 'tp2'},
        {'event_type': 'trade_invalidated', 'symbol': 'DOGEUSDT', 'market_regime_label': 'risk_on', 'side': 'LONG', 'state': 'launch', 'trigger_class': 'breakout', 'score_decile': '80-89', 'realized_r': -0.4, 'mfe_r': 0.9, 'mae_r': 1.1, 'time_to_1r': None, 'time_in_trade_minutes': 18.0, 'exit_reason': 'stop'},
        {'event_type': 'trade_invalidated', 'symbol': 'SUIUSDT', 'market_regime_label': 'risk_off', 'side': 'SHORT', 'state': 'watch', 'trigger_class': 'breakdown', 'score_decile': '60-69', 'realized_r': 0.8, 'mfe_r': 1.1, 'mae_r': 0.3, 'time_to_1r': 8.0, 'time_in_trade_minutes': 22.0, 'exit_reason': 'runner'},
        {'event_type': 'candidate_selected', 'symbol': 'BTCUSDT'},
    ]
    payload = mod.build_trade_bucket_analysis_payload(rows)
    assert payload['summary']['total_closed_trades'] == 3
    assert payload['summary']['distinct_buckets'] == 2
    assert payload['summary']['win_rate_pct'] == 66.67
    assert payload['summary']['avg_expectancy_r'] == 0.5333
    first_bucket = payload['by_bucket'][0]
    assert first_bucket['market_regime_label'] == 'risk_on'
    assert first_bucket['side'] == 'LONG'
    assert first_bucket['trigger_class'] == 'breakout'
    assert first_bucket['count'] == 2
    assert first_bucket['win_rate_pct'] == 50.0
    assert first_bucket['avg_expectancy_r'] == 0.4
    assert first_bucket['avg_mfe_r'] == 1.35
    assert first_bucket['avg_mae_r'] == 0.75
    assert first_bucket['avg_time_to_1r_minutes'] == 12.0
    assert first_bucket['avg_time_in_trade_minutes'] == 26.5
    assert payload['edge_calibration']['sample_count'] == 0
    assert payload['slippage_calibration']['sample_count'] == 0
    assert payload['prediction_coverage']['edge_prediction_coverage_pct'] == 0.0
    assert payload['by_exit_reason'] == [{'exit_reason': 'runner', 'count': 1}, {'exit_reason': 'stop', 'count': 1}, {'exit_reason': 'tp2', 'count': 1}]
    assert {'symbol': 'DOGEUSDT', 'count': 2} in payload['by_symbol']
    assert {'symbol': 'SUIUSDT', 'count': 1} in payload['by_symbol']
    assert {'trigger_class': 'breakout', 'count': 2} in payload['by_trigger_class']
    assert {'state': 'launch', 'count': 2} in payload['by_state']


def test_edge_calibration_compares_predicted_reward_with_realized_r():
    rows = [
        {'event_type': 'trade_invalidated', 'symbol': 'AUSDT', 'realized_r': 1.0, 'realizable_reward_r': 2.0},
        {'event_type': 'trade_invalidated', 'symbol': 'BUSDT', 'realized_r': -0.5, 'expected_reward_r': 1.0},
        {'event_type': 'trade_invalidated', 'symbol': 'CUSDT', 'realized_r': 0.5, 'expected_edge': 0.01, 'stop_distance_pct': 0.005},
        {'event_type': 'trade_invalidated', 'symbol': 'DUSDT', 'realized_r': 5.0},
    ]
    calibration = mod.build_trade_bucket_analysis_payload(rows)['edge_calibration']
    assert calibration['sample_count'] == 3
    assert calibration['avg_predicted_reward_r'] == 1.6667
    assert calibration['avg_realized_r'] == 0.3333
    assert calibration['calibration_ratio'] == 0.2
    assert calibration['mean_error_r'] == -1.3333
    assert calibration['mean_absolute_error_r'] == 1.3333


def test_candidate_selected_prediction_is_backfilled_into_closed_trade():
    rows = [
        {
            'event_type': 'candidate_selected',
            'recorded_at': '2026-08-30T01:00:00Z',
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'expected_edge': 1.2,
            'stop_distance_pct': 0.6,
            'expected_slippage_pct': 0.08,
            'expected_slippage_r': 0.12,
            'trigger_confirmation_count': 4,
            'trigger_confirmation_flags': {
                'breakout_flow_confirmation_count': 3,
                'breakout_min_volume_multiple': 1.35,
            },
            'candidate_stage': 'trade_candidate',
            'state': 'launch',
            'trigger_class': 'breakout',
        },
        {
            'event_type': 'trade_invalidated',
            'recorded_at': '2026-08-30T01:20:00Z',
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'realized_r': 1.0,
            'mfe_r': 1.4,
            'mae_r': 0.3,
            'exit_reason': 'tp1',
        },
    ]
    closed = mod.filter_closed_trade_events(rows)
    assert len(closed) == 1
    assert closed[0]['expected_edge'] == 1.2
    assert closed[0]['stop_distance_pct'] == 0.6
    assert closed[0]['expected_slippage_pct'] == 0.08
    assert closed[0]['breakout_flow_confirmation_count'] == 3
    assert closed[0]['prediction_snapshot_backfilled'] is True
    payload = mod.build_trade_bucket_analysis_payload(rows)
    assert payload['edge_calibration']['sample_count'] == 1
    assert payload['edge_calibration']['backfilled_sample_count'] == 1
    assert payload['edge_calibration']['avg_predicted_reward_r'] == 2.0
    assert payload['edge_calibration']['avg_realized_r'] == 1.0
    assert payload['edge_calibration']['calibration_ratio'] == 0.5
    coverage = payload['prediction_coverage']
    assert coverage['edge_prediction_coverage_pct'] == 100.0
    assert coverage['slippage_prediction_coverage_pct'] == 100.0
    assert coverage['flow_confirmation_coverage_pct'] == 100.0


def test_entry_fill_calibrates_predicted_vs_actual_slippage():
    rows = [
        {
            'event_type': 'candidate_selected',
            'recorded_at': '2026-08-30T01:00:00Z',
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'expected_edge': 1.2,
            'stop_distance_pct': 0.6,
            'expected_slippage_pct': 0.08,
            'shadow_entry_price': 100.0,
        },
        {
            'event_type': 'entry_filled',
            'recorded_at': '2026-08-30T01:01:00Z',
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'entry_price': 100.1,
        },
        {
            'event_type': 'trade_invalidated',
            'recorded_at': '2026-08-30T01:20:00Z',
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'realized_r': 0.5,
        },
    ]
    closed = mod.filter_closed_trade_events(rows)
    assert closed[0]['predicted_slippage_bps'] == 8.0
    assert closed[0]['actual_fill_slippage_bps'] == 10.0
    assert closed[0]['slippage_error_bps'] == 2.0
    calibration = mod.build_trade_bucket_analysis_payload(rows)['slippage_calibration']
    assert calibration['sample_count'] == 1
    assert calibration['avg_predicted_slippage_bps'] == 8.0
    assert calibration['avg_actual_slippage_bps'] == 10.0
    assert calibration['actual_to_predicted_ratio'] == 1.25
    assert calibration['mean_error_bps'] == 2.0
    assert calibration['mean_absolute_error_bps'] == 2.0
    assert calibration['underprediction_rate_pct'] == 100.0
    coverage = mod.build_trade_bucket_analysis_payload(rows)['prediction_coverage']
    assert coverage['actual_slippage_coverage_pct'] == 100.0


def test_slippage_calibration_is_segmented_by_execution_context():
    rows = [
        {
            'event_type': 'candidate_selected', 'symbol': 'AUSDT', 'side': 'LONG',
            'expected_slippage_pct': 0.05, 'shadow_entry_price': 100.0,
            'market_regime_label': 'risk_on', 'execution_liquidity_grade': 'A',
        },
        {
            'event_type': 'entry_filled', 'symbol': 'AUSDT', 'side': 'LONG', 'entry_price': 100.08,
            'maker_or_taker': 'maker', 'liquidity_grade_at_entry': 'A',
        },
        {'event_type': 'trade_invalidated', 'symbol': 'AUSDT', 'side': 'LONG', 'realized_r': 0.2},
        {
            'event_type': 'candidate_selected', 'symbol': 'BUSDT', 'side': 'SHORT',
            'expected_slippage_pct': 0.10, 'shadow_entry_price': 200.0,
            'market_regime_label': 'risk_off', 'execution_liquidity_grade': 'B',
        },
        {
            'event_type': 'entry_filled', 'symbol': 'BUSDT', 'side': 'SHORT', 'entry_price': 199.6,
            'maker_or_taker': 'taker', 'liquidity_grade_at_entry': 'B',
        },
        {'event_type': 'trade_invalidated', 'symbol': 'BUSDT', 'side': 'SHORT', 'realized_r': -0.1},
    ]
    payload = mod.build_trade_bucket_analysis_payload(rows)
    assert payload['slippage_calibration_by_side'][0]['sample_count'] == 1
    assert {row['side'] for row in payload['slippage_calibration_by_side']} == {'LONG', 'SHORT'}
    maker = next(row for row in payload['slippage_calibration_by_maker_or_taker'] if row['maker_or_taker'] == 'maker')
    taker = next(row for row in payload['slippage_calibration_by_maker_or_taker'] if row['maker_or_taker'] == 'taker')
    assert maker['avg_predicted_slippage_bps'] == 5.0
    assert maker['avg_actual_slippage_bps'] == 8.0
    assert taker['avg_predicted_slippage_bps'] == 10.0
    assert taker['avg_actual_slippage_bps'] == 20.0
    assert {row['liquidity_grade'] for row in payload['slippage_calibration_by_liquidity_grade']} == {'A', 'B'}
    assert {row['market_regime_label'] for row in payload['slippage_calibration_by_market_regime']} == {'risk_on', 'risk_off'}


def test_entry_fill_slippage_backfill_prefers_same_side_and_clears_after_close():
    rows = [
        {'event_type': 'candidate_selected', 'symbol': 'SUIUSDT', 'side': 'LONG', 'expected_slippage_pct': 0.05, 'shadow_entry_price': 100.0},
        {'event_type': 'candidate_selected', 'symbol': 'SUIUSDT', 'side': 'SHORT', 'expected_slippage_pct': 0.20, 'shadow_entry_price': 100.0},
        {'event_type': 'entry_filled', 'symbol': 'SUIUSDT', 'side': 'LONG', 'entry_price': 100.1},
        {'event_type': 'trade_invalidated', 'symbol': 'SUIUSDT', 'side': 'LONG', 'realized_r': 0.2},
        {'event_type': 'trade_invalidated', 'symbol': 'SUIUSDT', 'side': 'LONG', 'realized_r': -0.2},
    ]
    closed = mod.filter_closed_trade_events(rows)
    assert closed[0]['predicted_slippage_bps'] == 5.0
    assert closed[0]['actual_fill_slippage_bps'] == 10.0
    assert closed[1].get('actual_fill_slippage_bps') is None


def test_candidate_prediction_backfill_prefers_same_side():
    rows = [
        {'event_type': 'candidate_selected', 'symbol': 'SUIUSDT', 'side': 'LONG', 'expected_edge': 0.5, 'stop_distance_pct': 1.0},
        {'event_type': 'candidate_selected', 'symbol': 'SUIUSDT', 'side': 'SHORT', 'expected_edge': 2.0, 'stop_distance_pct': 1.0},
        {'event_type': 'trade_invalidated', 'symbol': 'SUIUSDT', 'side': 'LONG', 'realized_r': 0.25},
    ]
    closed = mod.filter_closed_trade_events(rows)
    assert closed[0]['expected_edge'] == 0.5
    assert mod.build_trade_bucket_analysis_payload(rows)['edge_calibration']['avg_predicted_reward_r'] == 0.5


def test_run_filters_symbol_and_writes_report_files(tmp_path):
    runtime_dir = tmp_path / 'runtime'
    runtime_dir.mkdir()
    events_path = runtime_dir / 'events.jsonl'
    events_path.write_text('\n'.join([
        json.dumps({'event_type': 'trade_invalidated', 'recorded_at': '2026-04-29T01:05:00Z', 'symbol': 'DOGEUSDT', 'market_regime_label': 'risk_on', 'side': 'LONG', 'state': 'launch', 'trigger_class': 'breakout', 'score_decile': '80-89', 'realized_r': 1.0, 'mfe_r': 1.4, 'mae_r': 0.3, 'time_to_1r': 10.0, 'time_in_trade_minutes': 30.0, 'exit_reason': 'tp1', 'realizable_reward_r': 1.5}),
        json.dumps({'event_type': 'trade_invalidated', 'recorded_at': '2026-04-29T01:15:00Z', 'symbol': 'SUIUSDT', 'market_regime_label': 'risk_off', 'side': 'SHORT', 'state': 'watch', 'trigger_class': 'breakdown', 'score_decile': '60-69', 'realized_r': -0.2, 'mfe_r': 0.7, 'mae_r': 0.8, 'time_to_1r': None, 'time_in_trade_minutes': 16.0, 'exit_reason': 'stop'}),
    ]) + '\n', encoding='utf-8')
    json_path = tmp_path / 'report.json'
    md_path = tmp_path / 'report.md'
    payload = mod.run(runtime_state_dir=runtime_dir, output_json_path=json_path, output_markdown_path=md_path, limit=100, symbol='DOGEUSDT', lookback_days=0)
    assert payload['summary']['symbol'] == 'DOGEUSDT'
    assert payload['summary']['total_closed_trades'] == 1
    assert payload['edge_calibration']['sample_count'] == 1
    written = json.loads(json_path.read_text(encoding='utf-8'))
    assert written['summary']['avg_expectancy_r'] == 1.0
    markdown = md_path.read_text(encoding='utf-8')
    assert '# Trade Bucket Analysis' in markdown
    assert '## Edge calibration' in markdown
    assert '## Slippage calibration' in markdown
    assert '## Slippage by side' in markdown
    assert '## Slippage by maker or taker' in markdown
    assert '## Prediction coverage' in markdown
    assert 'DOGEUSDT' in markdown
    assert 'SUIUSDT' not in markdown
