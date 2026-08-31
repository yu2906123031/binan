import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_module():
    spec = importlib.util.spec_from_file_location('relative_selection_policy_test', SCRIPTS_DIR / 'relative_selection_policy.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def candidate(symbol, *, score=100.0, edge=0.5, fill=0.8, spread=8.0, impact=0.10, volume=1_000_000, change=0.5, accel=1.2):
    return SimpleNamespace(
        symbol=symbol,
        side='LONG',
        score=score,
        reasons=[],
        realizable_edge_margin_r=edge,
        book_depth_fill_ratio=fill,
        spread_bps=spread,
        estimated_impact_pct=impact,
        quote_volume_24h=volume,
        recent_5m_change_pct=change,
        acceleration_ratio_5m_vs_15m=accel,
        trigger_fired=True,
    )


def test_relative_ranking_promotes_best_peer_and_deweights_worst():
    mod = load_module()
    best = candidate('BEST', edge=1.2, fill=0.98, spread=2, impact=0.02, volume=50_000_000, change=1.2, accel=1.8)
    mid = candidate('MID')
    worst = candidate('WORST', edge=0.1, fill=0.45, spread=25, impact=0.4, volume=100_000, change=0.05, accel=0.7)
    mod.rerank_candidate_cohort([best, mid, worst])
    assert best.score > mid.score > worst.score
    assert best.relative_selection_multiplier > 1.0
    assert worst.relative_selection_multiplier < 1.0


def test_relative_ranking_is_idempotent():
    mod = load_module()
    rows = [candidate('A', edge=1.0), candidate('B', edge=0.5), candidate('C', edge=0.1)]
    mod.rerank_candidate_cohort(rows)
    first = [(row.score, row.relative_selection_multiplier) for row in rows]
    mod.rerank_candidate_cohort(rows)
    second = [(row.score, row.relative_selection_multiplier) for row in rows]
    assert second == first


def test_small_cohort_is_left_unchanged():
    mod = load_module()
    rows = [candidate('A'), candidate('B')]
    before = [row.score for row in rows]
    mod.rerank_candidate_cohort(rows)
    assert [row.score for row in rows] == before


def test_missing_peer_metrics_remain_neutral():
    mod = load_module()
    rows = [SimpleNamespace(symbol=str(i), score=80.0, reasons=[], trigger_fired=True) for i in range(3)]
    mod.rerank_candidate_cohort(rows)
    assert all(row.score == 80.0 for row in rows)
    assert all(row.relative_selection_multiplier == 1.0 for row in rows)


def test_hook_updates_full_cohort_before_best_selection_and_resets_between_scans():
    mod = load_module()
    batches = [
        [candidate('BEST', edge=1.2, spread=2), candidate('MID', edge=0.5), candidate('WORST', edge=0.1, spread=25)],
        [candidate('X', edge=0.2), candidate('Y', edge=0.6), candidate('Z', edge=1.0)],
    ]
    state = {'index': 0}

    def build_alert(item, *_args, **_kwargs):
        return {'symbol': item.symbol, 'score': item.score}

    def run_scan_once(*_args, **_kwargs):
        rows = batches[state['index']]
        state['index'] += 1
        alerts = [module.build_standardized_alert(item, {}) for item in rows]
        best = sorted(rows, key=lambda item: item.score, reverse=True)[0]
        return {'alerts': alerts}, best, {}

    module = SimpleNamespace(run_scan_once=run_scan_once, build_standardized_alert=build_alert)
    mod.install_relative_selection_hook(module)
    _, best1, _ = module.run_scan_once()
    _, best2, _ = module.run_scan_once()
    assert best1.symbol == 'BEST'
    assert best2.symbol == 'Z'
    assert len({row.relative_selection_base_score for row in batches[0]}) == 1


def test_installer_is_idempotent():
    mod = load_module()
    module = SimpleNamespace(
        run_scan_once=lambda: None,
        build_standardized_alert=lambda item: item,
    )
    mod.install_relative_selection_hook(module)
    first = module.run_scan_once
    mod.install_relative_selection_hook(module)
    assert module.run_scan_once is first
