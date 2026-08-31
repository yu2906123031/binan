import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
SCRIPT_PATH = SCRIPTS_DIR / 'risk_exit_guard.py'
spec = importlib.util.spec_from_file_location('risk_exit_guard', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_reduce_only_exit_overrides_entry_risk_reasons():
    calls = []

    def original(*args, **kwargs):
        calls.append((args, kwargs))
        return {
            'allowed': False,
            'reasons': [
                'strategy_halted',
                'daily_max_loss_reached',
                'max_consecutive_losses_reached',
                'candidate_execution_liquidity_poor',
            ],
            'normalized_risk_state': {'halted': True},
            'execution_mode': 'taker',
        }

    strategy = SimpleNamespace(evaluate_risk_guards=original)
    mod.install_reduce_only_risk_guard(strategy)
    result = strategy.evaluate_risk_guards('BTCUSDT', reduce_only=True)

    assert len(calls) == 1
    assert result['allowed'] is True
    assert result['reasons'] == []
    assert result['reduce_only_override'] is True
    assert result['reduce_only_original_reasons'] == [
        'strategy_halted',
        'daily_max_loss_reached',
        'max_consecutive_losses_reached',
        'candidate_execution_liquidity_poor',
    ]
    assert result['normalized_risk_state'] == {'halted': True}


def test_close_position_and_allow_reduce_only_are_treated_as_risk_reducing():
    for flag in ('close_position', 'allow_reduce_only'):
        strategy = SimpleNamespace(
            evaluate_risk_guards=lambda *args, **kwargs: {'allowed': False, 'reasons': ['strategy_halted']}
        )
        mod.install_reduce_only_risk_guard(strategy)
        result = strategy.evaluate_risk_guards(**{flag: True})
        assert result['allowed'] is True
        assert result['reasons'] == []
        assert result['reduce_only_override'] is True


def test_normal_entry_keeps_original_risk_decision():
    original_result = {'allowed': False, 'reasons': ['daily_max_loss_reached']}
    strategy = SimpleNamespace(evaluate_risk_guards=lambda *args, **kwargs: original_result)
    mod.install_reduce_only_risk_guard(strategy)
    result = strategy.evaluate_risk_guards('ETHUSDT')
    assert result is original_result


def test_hook_installation_is_idempotent():
    strategy = SimpleNamespace(evaluate_risk_guards=lambda *args, **kwargs: {'allowed': True, 'reasons': []})
    mod.install_reduce_only_risk_guard(strategy)
    first = strategy.evaluate_risk_guards
    mod.install_reduce_only_risk_guard(strategy)
    assert strategy.evaluate_risk_guards is first
