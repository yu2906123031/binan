import importlib.util
import json
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'rejected_analysis.py'
spec = importlib.util.spec_from_file_location('rejected_analysis', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_build_rejected_analysis_payload_aggregates_reason_label_grade_and_overextension():
    rows = [
        {
            'event_type': 'candidate_rejected',
            'symbol': 'DOGEUSDT',
            'reject_reason': 'candidate_distribution_risk',
            'reject_reason_label': 'distribution',
            'execution_liquidity_grade': 'C',
            'overextension_flag': 'high',
            'expected_slippage_r': 0.22,
            'book_depth_fill_ratio': 0.41,
        },
        {
            'event_type': 'candidate_rejected',
            'symbol': 'DOGEUSDT',
            'reject_reason': 'candidate_distribution_risk',
            'reject_reason_label': 'distribution',
            'execution_liquidity_grade': 'C',
            'overextension_flag': 'high',
            'expected_slippage_r': 0.12,
            'book_depth_fill_ratio': 0.52,
        },
        {
            'event_type': 'candidate_rejected',
            'symbol': 'SUIUSDT',
            'reject_reason': 'candidate_execution_slippage_risk',
            'reject_reason_label': 'slippage_risk',
            'execution_liquidity_grade': 'D',
            'overextension_flag': 'mild',
            'expected_slippage_r': 0.31,
            'book_depth_fill_ratio': 0.18,
        },
        {
            'event_type': 'entry_filled',
            'symbol': 'BTCUSDT',
        },
    ]

    payload = mod.build_rejected_analysis_payload(rows)

    assert payload['summary']['total_rejected'] == 3
    assert payload['summary']['distinct_symbols'] == 2
    assert payload['summary']['top_reject_reason'] == 'candidate_distribution_risk'
    assert payload['summary']['top_reject_reason_count'] == 2

    assert payload['by_reason'][0]['reject_reason'] == 'candidate_distribution_risk'
    assert payload['by_reason'][0]['count'] == 2
    assert payload['by_reason'][0]['avg_expected_slippage_r'] == 0.17
    assert payload['by_reason'][0]['avg_book_depth_fill_ratio'] == 0.465

    assert payload['by_label'][0] == {'reject_reason_label': 'distribution', 'count': 2}
    assert payload['by_grade'][0] == {'execution_liquidity_grade': 'C', 'count': 2}
    assert payload['by_overextension'][0] == {'overextension_flag': 'high', 'count': 2}


def test_run_reads_events_and_writes_report_files(tmp_path):
    runtime_dir = tmp_path / 'runtime'
    runtime_dir.mkdir()
    events_path = runtime_dir / 'events.jsonl'
    events_path.write_text(
        '\n'.join([
            json.dumps({
                'event_type': 'candidate_rejected',
                'symbol': 'DOGEUSDT',
                'reject_reason': 'candidate_distribution_risk',
                'reject_reason_label': 'distribution',
                'execution_liquidity_grade': 'C',
                'overextension_flag': 'high',
                'expected_slippage_r': 0.2,
                'book_depth_fill_ratio': 0.4,
            }),
            json.dumps({
                'event_type': 'candidate_rejected',
                'symbol': 'SUIUSDT',
                'reject_reason': 'candidate_execution_slippage_risk',
                'reject_reason_label': 'slippage_risk',
                'execution_liquidity_grade': 'D',
                'overextension_flag': 'mild',
                'expected_slippage_r': 0.3,
                'book_depth_fill_ratio': 0.2,
            }),
        ]) + '\n',
        encoding='utf-8',
    )
    json_path = tmp_path / 'report.json'
    md_path = tmp_path / 'report.md'

    payload = mod.run(runtime_state_dir=runtime_dir, output_json_path=json_path, output_markdown_path=md_path, limit=100)

    assert payload['summary']['total_rejected'] == 2
    assert json.loads(json_path.read_text(encoding='utf-8'))['summary']['distinct_symbols'] == 2
    markdown = md_path.read_text(encoding='utf-8')
    assert '# Candidate Rejected Analysis' in markdown
    assert 'candidate_distribution_risk' in markdown
    assert 'slippage_risk' in markdown
