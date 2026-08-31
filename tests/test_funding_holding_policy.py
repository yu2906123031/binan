import importlib
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def test_positive_funding_costs_long_and_can_consume_edge():
    mod = importlib.import_module('funding_holding_policy')
    result = mod.evaluate_funding_hold({
        'side': 'LONG',
        'funding_rate': 0.01,
        'stop_distance_pct': 0.5,
        'remaining_expected_edge_r': 0.1,
    }, projected_hours=8)
    assert result['funding_cost_r'] > 0
    assert result['action'] == 'reduce'


def test_positive_funding_is_credit_for_short():
    mod = importlib.import_module('funding_holding_policy')
    result = mod.estimate_funding_cost_r({
        'side': 'SHORT',
        'funding_rate': 0.001,
        'stop_distance_pct': 1.0,
    }, projected_hours=8)
    assert result['funding_cost_r'] == 0
    assert result['funding_credit_r'] > 0
