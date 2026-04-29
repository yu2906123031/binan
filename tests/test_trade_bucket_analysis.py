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
        {
            'event_type': 'trade_invalidated',
            'symbol': 'DOGEUSDT',
            'market_regime_label': 'risk_on',
            'side': 'LONG',
            'state': 'launch',
            'trigger_class': 'breakout',
            'score_decile': '80-89',
            'realized_r': 1.2,
            'mfe_r': 1.8,
            'mae_r': 0.4,
            'time_to_1r': 12.0,
            'time_in_trade_minutes': 35.0,
            'exit_reason': 'tp2',
        },
        {
            'event_type': 'trade_invalidated',
            'symbol': 'DOGEUSDT',
            'market_regime_label': 'risk_on',
            'side': 'LONG',
            'state': 'launch',
            'trigger_class': 'breakout',
            'score_decile': '80-89',
            'realized_r': -0.4,
            'mfe_r': 0.9,
            'mae_r': 1.1,
            'time_to_1r': None,
            'time_in_trade_minutes': 18.0,
            'exit_reason': 'stop',
        },
        {
            'event_type': 'trade_invalidated',
            'symbol': 'SUIUSDT',
            'market_regime_label': 'risk_off',
            'side': 'SHORT',
            'state': 'watch',
            'trigger_class': 'breakdown',
            'score_decile': '60-69',
            'realized_r': 0.8,
            'mfe_r': 1.1,
            'mae_r': 0.3,
            'time_to_1r': 8.0,
            'time_in_trade_minutes': 22.0,
            'exit_reason': 'runner',
        },
        {
            'event_type': 'candidate_selected',
            'symbol': 'BTCUSDT',
        },
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

    assert payload['by_exit_reason'] == [
        {'exit_reason': 'runner', 'count': 1},
        {'exit_reason': 'stop', 'count': 1},
        {'exit_reason': 'tp2', 'count': 1},
    ]
    assert {'symbol': 'DOGEUSDT', 'count': 2} in payload['by_symbol']
    assert {'symbol': 'SUIUSDT', 'count': 1} in payload['by_symbol']


def test_run_filters_symbol_and_writes_report_files(tmp_path):
    runtime_dir = tmp_path / 'runtime'
    runtime_dir.mkdir()
    events_path = runtime_dir / 'events.jsonl'
    events_path.write_text(
        '\n'.join([
            json.dumps({
                'event_type': 'trade_invalidated',
                'recorded_at': '2026-04-29T01:05:00Z',
                'symbol': 'DOGEUSDT',
                'market_regime_label': 'risk_on',
                'side': 'LONG',
                'state': 'launch',
                'trigger_class': 'breakout',
                'score_decile': '80-89',
                'realized_r': 1.0,
                'mfe_r': 1.4,
                'mae_r': 0.3,
                'time_to_1r': 10.0,
                'time_in_trade_minutes': 30.0,
                'exit_reason': 'tp1',
            }),
            json.dumps({
                'event_type': 'trade_invalidated',
                'recorded_at': '2026-04-29T01:15:00Z',
                'symbol': 'SUIUSDT',
                'market_regime_label': 'risk_off',
                'side': 'SHORT',
                'state': 'watch',
                'trigger_class': 'breakdown',
                'score_decile': '60-69',
                'realized_r': -0.2,
                'mfe_r': 0.7,
                'mae_r': 0.8,
                'time_to_1r': None,
                'time_in_trade_minutes': 16.0,
                'exit_reason': 'stop',
            }),
        ]) + '\n',
        encoding='utf-8',
    )
    json_path = tmp_path / 'report.json'
    md_path = tmp_path / 'report.md'

    payload = mod.run(
        runtime_state_dir=runtime_dir,
        output_json_path=json_path,
        output_markdown_path=md_path,
        limit=100,
        symbol='DOGEUSDT',
        lookback_days=0,
    )

    assert payload['summary']['symbol'] == 'DOGEUSDT'
    assert payload['summary']['total_closed_trades'] == 1
    written = json.loads(json_path.read_text(encoding='utf-8'))
    assert written['summary']['avg_expectancy_r'] == 1.0
    markdown = md_path.read_text(encoding='utf-8')
    assert '# Trade Bucket Analysis' in markdown
    assert 'DOGEUSDT' in markdown
    assert 'SUIUSDT' not in markdown
