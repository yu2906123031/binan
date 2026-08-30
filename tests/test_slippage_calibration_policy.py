import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
SCRIPT_PATH = SCRIPTS_DIR / 'slippage_calibration_policy.py'
spec = importlib.util.spec_from_file_location('slippage_calibration_policy', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def _payload(*, side_ratio=1.25, side_samples=25, underprediction=72.0, global_ratio=1.1, global_samples=40):
    return {
        'slippage_calibration': {
            'sample_count': global_samples,
            'actual_to_predicted_ratio': global_ratio,
            'underprediction_rate_pct': underprediction,
        },
        'slippage_calibration_by_dimension': {
            'side': [
                {
                    'side': 'LONG',
                    'sample_count': side_samples,
                    'actual_to_predicted_ratio': side_ratio,
                    'underprediction_rate_pct': underprediction,
                }
            ],
            'maker_or_taker': [],
            'liquidity_grade': [],
            'market_regime_label': [],
        },
    }


def test_segmented_calibration_requires_minimum_sample_count():
    candidate = SimpleNamespace(side='LONG')
    result = mod.resolve_slippage_calibration(
        candidate,
        payload=_payload(side_ratio=1.4, side_samples=19, global_samples=0),
    )
    assert result['active'] is False
    assert result['multiplier'] == 1.0
    assert result['source'] == 'none'


def test_segmented_calibration_only_tightens_when_underprediction_is_confirmed():
    candidate = SimpleNamespace(side='LONG')
    result = mod.resolve_slippage_calibration(
        candidate,
        payload=_payload(side_ratio=1.3, side_samples=30, underprediction=70.0),
    )
    assert result['active'] is True
    assert result['source'] == 'segmented'
    assert result['multiplier'] == 1.3
    assert result['matched'][0]['dimension'] == 'side'

    overpredicted = mod.resolve_slippage_calibration(
        candidate,
        payload=_payload(side_ratio=0.8, side_samples=30, underprediction=30.0, global_samples=0),
    )
    assert overpredicted['active'] is False
    assert overpredicted['multiplier'] == 1.0


def test_calibration_multiplier_is_capped():
    candidate = SimpleNamespace(side='LONG')
    result = mod.resolve_slippage_calibration(candidate, payload=_payload(side_ratio=3.0, side_samples=100))
    assert result['multiplier'] == 1.5


def test_global_calibration_is_used_only_when_no_segment_is_eligible():
    candidate = SimpleNamespace(side='SHORT')
    result = mod.resolve_slippage_calibration(
        candidate,
        payload=_payload(side_ratio=1.4, side_samples=50, global_ratio=1.2, global_samples=50),
    )
    assert result['active'] is True
    assert result['source'] == 'global'
    assert result['multiplier'] == 1.2


def test_candidate_slippage_calibration_is_idempotent_and_increases_cost_assumption():
    candidate = SimpleNamespace(side='LONG', expected_slippage_pct=0.08)
    payload = _payload(side_ratio=1.25, side_samples=30)
    first = mod.apply_candidate_slippage_calibration(candidate, payload=payload)
    second = mod.apply_candidate_slippage_calibration(candidate, payload=payload)
    assert first['multiplier'] == 1.25
    assert second['multiplier'] == 1.25
    assert candidate.base_expected_slippage_pct == 0.08
    assert candidate.expected_slippage_pct == 0.1
    assert candidate.slippage_calibration_active is True


def test_execution_calibration_increases_slippage_and_reduces_size():
    candidate = SimpleNamespace(side='LONG')
    quality = {
        'absolute_slippage_bps': 8.0,
        'expected_slippage_r': 0.04,
        'size_multiplier': 0.9,
        'execution_mode': 'maker_only',
        'execution_liquidity_grade': 'A',
    }
    result = mod.apply_execution_slippage_calibration(
        candidate,
        quality,
        payload=_payload(side_ratio=1.25, side_samples=30),
    )
    assert result['base_absolute_slippage_bps'] == 8.0
    assert result['absolute_slippage_bps'] == 10.0
    assert result['expected_slippage_r'] == 0.05
    assert result['base_size_multiplier'] == 0.9
    assert result['size_multiplier'] == 0.72
    assert result['slippage_calibration_active'] is True


def test_execution_mode_normalization_matches_maker_bucket():
    candidate = SimpleNamespace(side='SHORT')
    payload = {
        'slippage_calibration': {'sample_count': 0},
        'slippage_calibration_by_dimension': {
            'side': [],
            'maker_or_taker': [
                {
                    'maker_or_taker': 'maker',
                    'sample_count': 40,
                    'actual_to_predicted_ratio': 1.15,
                    'underprediction_rate_pct': 65.0,
                }
            ],
            'liquidity_grade': [],
            'market_regime_label': [],
        },
    }
    result = mod.resolve_slippage_calibration(candidate, {'execution_mode': 'maker_only'}, payload=payload)
    assert result['source'] == 'segmented'
    assert result['multiplier'] == 1.15
    assert result['matched'][0]['dimension'] == 'maker_or_taker'
