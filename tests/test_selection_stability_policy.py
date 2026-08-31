from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_module():
    spec = importlib.util.spec_from_file_location(
        'selection_stability_policy_test',
        SCRIPTS_DIR / 'selection_stability_policy.py',
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def candidate(symbol='AAAUSDT', score=100.0, percentile=0.8):
    return SimpleNamespace(
        symbol=symbol,
        score=score,
        relative_selection_percentile=percentile,
        reasons=[],
    )


def history(*, last_seen_scan=1, streak=1, ema_percentile=0.8, appearances=1):
    return {
        'last_seen_scan': last_seen_scan,
        'streak': streak,
        'ema_percentile': ema_percentile,
        'appearances': appearances,
    }


def test_new_candidate_is_neutral():
    mod = load_module()
    row = candidate()
    multiplier, streak = mod.compute_stability_multiplier(row, None, 5)
    assert multiplier == 1.0
    assert streak == 1


def test_consecutive_high_quality_candidate_gets_soft_boost():
    mod = load_module()
    row = candidate(percentile=0.85)
    multiplier, streak = mod.compute_stability_multiplier(
        row,
        history(last_seen_scan=4, streak=3, ema_percentile=0.82, appearances=3),
        5,
    )
    assert streak == 4
    assert 1.03 < multiplier <= 1.05


def test_gap_resets_streak_and_old_quality_boost():
    mod = load_module()
    row = candidate(percentile=0.9)
    multiplier, streak = mod.compute_stability_multiplier(
        row,
        history(last_seen_scan=2, streak=7, ema_percentile=0.95, appearances=10),
        5,
    )
    assert streak == 1
    assert multiplier == 1.0


def test_sudden_jump_after_weak_previous_scan_is_not_overrewarded():
    mod = load_module()
    row = candidate(percentile=0.95)
    multiplier, streak = mod.compute_stability_multiplier(
        row,
        history(last_seen_scan=4, streak=1, ema_percentile=0.20, appearances=1),
        5,
    )
    assert streak == 2
    assert multiplier < 1.0
    assert multiplier >= 0.98


def test_apply_uses_current_upstream_score_without_compounding():
    mod = load_module()
    state = {'scan_id': 4, 'symbols': {'AAAUSDT': history(last_seen_scan=4, streak=3, ema_percentile=0.8)}}
    row = candidate(score=100.0, percentile=0.8)
    mod.apply_selection_stability([row], state, 5)
    first_multiplier = row.selection_stability_multiplier
    assert row.score == round(100.0 * first_multiplier, 4)

    row.score = 110.0
    mod.apply_selection_stability([row], state, 5)
    assert row.stability_base_score == 110.0
    assert row.score == round(110.0 * first_multiplier, 4)


def test_state_update_preserves_zero_percentile_and_updates_ema():
    mod = load_module()
    state = {
        'scan_id': 1,
        'symbols': {'AAAUSDT': history(last_seen_scan=1, streak=1, ema_percentile=1.0, appearances=1)},
    }
    row = candidate(percentile=0.0)
    updated = mod.update_stability_state(state, [row], 2)
    payload = updated['symbols']['AAAUSDT']
    assert payload['streak'] == 2
    assert payload['appearances'] == 2
    assert payload['ema_percentile'] == 0.65


def test_old_symbols_are_pruned():
    mod = load_module()
    state = {
        'scan_id': 20,
        'symbols': {
            'OLDUSDT': history(last_seen_scan=1, streak=1, ema_percentile=0.9),
            'RECENTUSDT': history(last_seen_scan=19, streak=2, ema_percentile=0.8),
        },
    }
    updated = mod.update_stability_state(state, [], 20)
    assert 'OLDUSDT' not in updated['symbols']
    assert 'RECENTUSDT' in updated['symbols']


def test_hook_loads_and_persists_runtime_history_once_per_scan():
    mod = load_module()

    class Store:
        def __init__(self):
            self.saved = []
            self.state = {
                'scan_id': 3,
                'symbols': {'AAAUSDT': history(last_seen_scan=3, streak=2, ema_percentile=0.8, appearances=2)},
            }

        def load_json(self, name, default=None):
            assert name == mod.STATE_NAME
            return self.state

        def save_json(self, name, payload):
            assert name == mod.STATE_NAME
            self.saved.append(payload)
            self.state = payload

    store = Store()
    relative = SimpleNamespace(rerank_candidate_cohort=lambda rows: list(rows))
    rows = [candidate('AAAUSDT', 100.0, 0.85), candidate('BBBUSDT', 99.0, 0.6)]

    def run_scan(_client, _args):
        relative.rerank_candidate_cohort(rows)
        return {'ok': True}, rows[0], {}

    strategy = SimpleNamespace(
        run_scan_once=run_scan,
        get_runtime_state_store=lambda _args: store,
    )
    mod.install_selection_stability_hook(relative, strategy)
    wrapped = strategy.run_scan_once
    mod.install_selection_stability_hook(relative, strategy)
    assert strategy.run_scan_once is wrapped

    result = strategy.run_scan_once(None, SimpleNamespace())
    assert result[0]['ok'] is True
    assert len(store.saved) == 1
    assert store.saved[0]['scan_id'] == 4
    assert store.saved[0]['symbols']['AAAUSDT']['streak'] == 3
    assert rows[0].selection_stability_multiplier > 1.0
