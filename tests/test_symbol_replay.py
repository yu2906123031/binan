import importlib.util
import json
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'symbol_replay.py'
spec = importlib.util.spec_from_file_location('symbol_replay', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_build_symbol_replay_payload_groups_rejected_and_closed_sessions_by_position_key():
    rows = [
        {
            'event_type': 'candidate_selected',
            'recorded_at': '2026-04-29T01:00:00+00:00',
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_key': 'DOGEUSDT:LONG',
            'score': 81.2,
            'state': 'launch',
            'alert_tier': 'critical',
            'entry_price': 0.142,
            'stop_price': 0.137,
            'quantity': 1000.0,
            'execution_exchange': 'BINANCE',
        },
        {
            'event_type': 'candidate_rejected',
            'recorded_at': '2026-04-29T01:00:15+00:00',
            'symbol': 'DOGEUSDT',
            'side': 'LONG',
            'position_key': 'DOGEUSDT:LONG',
            'reject_reason': 'max_open_positions_reached',
            'reject_reason_label': 'position_limit',
        },
        {
            'event_type': 'candidate_selected',
            'recorded_at': '2026-04-29T01:05:00+00:00',
            'symbol': 'DOGEUSDT',
            'side': 'SHORT',
            'position_key': 'DOGEUSDT:SHORT',
            'score': 78.5,
            'state': 'launch',
            'alert_tier': 'high',
            'entry_price': 0.1418,
            'stop_price': 0.145,
            'quantity': 900.0,
            'execution_exchange': 'BINANCE',
        },
        {
            'event_type': 'entry_filled',
            'recorded_at': '2026-04-29T01:05:05+00:00',
            'symbol': 'DOGEUSDT',
            'side': 'SHORT',
            'position_key': 'DOGEUSDT:SHORT',
            'entry_price': 0.1412,
            'stop_price': 0.145,
            'quantity': 900.0,
            'filled_quantity': 900.0,
            'entry_order_id': 12345,
        },
        {
            'event_type': 'tp1_hit',
            'recorded_at': '2026-04-29T01:10:00+00:00',
            'symbol': 'DOGEUSDT',
            'side': 'SHORT',
            'position_key': 'DOGEUSDT:SHORT',
            'close_qty': 300.0,
            'new_stop_price': 0.14,
        },
        {
            'event_type': 'trade_invalidated',
            'recorded_at': '2026-04-29T01:12:00+00:00',
            'symbol': 'DOGEUSDT',
            'side': 'SHORT',
            'position_key': 'DOGEUSDT:SHORT',
            'exit_reason': 'tp1',
        },
        {
            'event_type': 'candidate_selected',
            'recorded_at': '2026-04-29T01:15:00+00:00',
            'symbol': 'SUIUSDT',
            'side': 'LONG',
            'position_key': 'SUIUSDT:LONG',
            'score': 70.0,
        },
    ]

    payload = mod.build_symbol_replay_payload(rows, symbol='DOGEUSDT')

    assert payload['summary']['symbol'] == 'DOGEUSDT'
    assert payload['summary']['session_count'] == 2
    assert payload['summary']['selected_count'] == 2
    assert payload['summary']['entered_count'] == 1
    assert payload['summary']['rejected_count'] == 1
    assert payload['summary']['closed_count'] == 1

    assert payload['by_exit_reason'][0] == {'exit_reason': 'tp1', 'count': 1}
    assert payload['by_reject_reason'][0] == {'reject_reason': 'max_open_positions_reached', 'count': 1}

    rejected_session = payload['sessions'][0]
    assert rejected_session['position_key'] == 'DOGEUSDT:LONG'
    assert rejected_session['status'] == 'rejected'
    assert rejected_session['reject_reason'] == 'max_open_positions_reached'
    assert rejected_session['event_sequence'] == ['candidate_selected', 'candidate_rejected']

    closed_session = payload['sessions'][1]
    assert closed_session['position_key'] == 'DOGEUSDT:SHORT'
    assert closed_session['status'] == 'closed'
    assert closed_session['entry_order_id'] == 12345
    assert closed_session['entry_price'] == 0.1412
    assert closed_session['exit_reason'] == 'tp1'
    assert closed_session['management_actions'][0]['event_type'] == 'tp1_hit'


def test_run_filters_symbol_side_and_writes_report_files(tmp_path):
    runtime_dir = tmp_path / 'runtime'
    runtime_dir.mkdir()
    events_path = runtime_dir / 'events.jsonl'
    events_path.write_text(
        '\n'.join([
            json.dumps({
                'event_type': 'candidate_selected',
                'recorded_at': '2026-04-29T01:00:00+00:00',
                'symbol': 'DOGEUSDT',
                'side': 'LONG',
                'position_key': 'DOGEUSDT:LONG',
                'score': 81.2,
            }),
            json.dumps({
                'event_type': 'candidate_rejected',
                'recorded_at': '2026-04-29T01:00:15+00:00',
                'symbol': 'DOGEUSDT',
                'side': 'LONG',
                'position_key': 'DOGEUSDT:LONG',
                'reject_reason': 'risk_guard_blocked',
            }),
            json.dumps({
                'event_type': 'candidate_selected',
                'recorded_at': '2026-04-29T01:05:00+00:00',
                'symbol': 'DOGEUSDT',
                'side': 'SHORT',
                'position_key': 'DOGEUSDT:SHORT',
                'score': 78.5,
            }),
            json.dumps({
                'event_type': 'entry_filled',
                'recorded_at': '2026-04-29T01:05:05+00:00',
                'symbol': 'DOGEUSDT',
                'side': 'SHORT',
                'position_key': 'DOGEUSDT:SHORT',
                'entry_price': 0.1412,
                'quantity': 900.0,
            }),
            json.dumps({
                'event_type': 'trade_invalidated',
                'recorded_at': '2026-04-29T01:12:00+00:00',
                'symbol': 'DOGEUSDT',
                'side': 'SHORT',
                'position_key': 'DOGEUSDT:SHORT',
                'exit_reason': 'tp1',
            }),
            '{not-json}',
        ]) + '\n',
        encoding='utf-8',
    )
    json_path = tmp_path / 'replay.json'
    md_path = tmp_path / 'replay.md'

    payload = mod.run(
        runtime_state_dir=runtime_dir,
        symbol='DOGEUSDT',
        side='SHORT',
        output_json_path=json_path,
        output_markdown_path=md_path,
        limit=100,
    )

    assert payload['summary']['symbol'] == 'DOGEUSDT'
    assert payload['summary']['side'] == 'SHORT'
    assert payload['summary']['session_count'] == 1
    assert payload['summary']['closed_count'] == 1

    written = json.loads(json_path.read_text(encoding='utf-8'))
    assert written['summary']['entered_count'] == 1
    markdown = md_path.read_text(encoding='utf-8')
    assert '# Symbol Replay Report' in markdown
    assert 'DOGEUSDT:SHORT' in markdown
    assert 'DOGEUSDT:LONG' not in markdown
