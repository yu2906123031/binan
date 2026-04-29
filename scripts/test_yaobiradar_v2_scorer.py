import importlib.util
import sys
from pathlib import Path

SKILL_SCRIPTS_DIR = Path(__file__).resolve().parent
MODULE_PATH = SKILL_SCRIPTS_DIR / 'yaobiradar_v2_scorer.py'


def load_module():
    sys.path.insert(0, str(SKILL_SCRIPTS_DIR))
    try:
        spec = importlib.util.spec_from_file_location('yaobiradar_v2_scorer', MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        if sys.path and sys.path[0] == str(SKILL_SCRIPTS_DIR):
            sys.path.pop(0)


def test_build_rows_ranks_and_normalizes_symbols():
    mod = load_module()
    rows = mod.build_rows([
        {
            'symbol': 'doge',
            'hot_score': 32,
            'momentum_score': 28,
            'liquidity_score': 16,
            'breakout_score': 18,
            'reasons': ['hot_board', 'breakout'],
        },
        {
            'symbol': 'SUIUSDT',
            'hot_score': 25,
            'momentum_score': 24,
            'liquidity_score': 20,
            'breakout_score': 10,
            'reasons': ['steady_trend'],
        },
    ])

    assert [row['symbol'] for row in rows] == ['DOGEUSDT', 'SUIUSDT']
    assert rows[0]['external_signal_score'] == 94.0
    assert rows[0]['external_signal_tier'] == 'critical'
    assert rows[0]['external_position_size_pct'] == 3.0
    assert rows[0]['external_reasons'] == ['hot_board', 'breakout', 'composite_rank=1']
    assert rows[1]['external_signal_score'] == 79.0
    assert rows[1]['external_signal_tier'] == 'high'
    assert rows[1]['external_position_size_pct'] == 2.0


def test_build_rows_marks_blocked_entries_with_veto_reason():
    mod = load_module()
    rows = mod.build_rows([
        {
            'symbol': '1000PEPE',
            'hot_score': 10,
            'momentum_score': 8,
            'liquidity_score': 5,
            'breakout_score': 4,
            'blocked': True,
            'block_reason': 'manual_blacklist',
        }
    ])

    assert rows == [{
        'symbol': '1000PEPEUSDT',
        'external_signal_score': 27.0,
        'external_signal_tier': 'blocked',
        'external_position_size_pct': 0.0,
        'external_veto': True,
        'external_veto_reason': 'manual_blacklist',
        'external_reasons': ['manual_blacklist', 'composite_rank=1'],
    }]


def test_run_writes_payload_files(tmp_path):
    mod = load_module()
    symbols_path = tmp_path / 'symbols.txt'
    external_json_path = tmp_path / 'external.json'

    payload = mod.run([
        {
            'symbol': 'DOGE',
            'hot_score': 30,
            'momentum_score': 25,
            'liquidity_score': 15,
            'breakout_score': 15,
            'reasons': ['oi_surge'],
            'portfolio_narrative_bucket': 'accumulation',
            'portfolio_correlation_group': 'accumulation',
        }
    ], symbols_path=symbols_path, external_json_path=external_json_path)

    assert payload['symbols'] == ['DOGEUSDT']
    assert symbols_path.read_text(encoding='utf-8') == 'DOGEUSDT\n'
    written = external_json_path.read_text(encoding='utf-8')
    assert 'DOGEUSDT' in written
    assert 'yaobiradar_v2' in written
    assert 'portfolio_narrative_bucket' in written
