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
        'selection_diversification_policy_test',
        SCRIPTS_DIR / 'selection_diversification_policy.py',
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def candidate(symbol: str, score: float, group: str = '', *, triggered: bool = True):
    return SimpleNamespace(
        symbol=symbol,
        score=score,
        reasons=[],
        trigger_fired=triggered,
        pretrigger_watch=False,
        portfolio_correlation_group=group,
        portfolio_narrative_bucket='',
    )


def test_first_candidate_in_group_is_not_penalized():
    mod = load_module()
    first = candidate('AAAUSDT', 100.0, 'L1')
    second = candidate('BBBUSDT', 99.0, 'L1')
    mod.apply_selection_diversification([first, second])
    assert first.selection_diversification_multiplier == 1.0
    assert first.score == 100.0
    assert second.selection_diversification_multiplier == 0.97
    assert second.score < 99.0


def test_duplicate_penalty_increases_for_crowded_group():
    mod = load_module()
    rows = [candidate(f'C{i}USDT', 100.0 - i, 'MEME') for i in range(4)]
    mod.apply_selection_diversification(rows)
    assert [row.selection_diversification_multiplier for row in rows] == [1.0, 0.97, 0.93, 0.89]


def test_distinct_groups_are_not_penalized():
    mod = load_module()
    rows = [
        candidate('AAAUSDT', 100.0, 'L1'),
        candidate('BBBUSDT', 99.0, 'L2'),
        candidate('CCCUSDT', 98.0, 'DEFI'),
    ]
    mod.apply_selection_diversification(rows)
    assert all(row.selection_diversification_multiplier == 1.0 for row in rows)


def test_missing_group_metadata_is_neutral():
    mod = load_module()
    rows = [candidate('AAAUSDT', 100.0), candidate('BBBUSDT', 99.0), candidate('CCCUSDT', 98.0)]
    mod.apply_selection_diversification(rows)
    assert all(row.score == base for row, base in zip(rows, [100.0, 99.0, 98.0]))
    assert all(row.selection_diversification_group == '' for row in rows)


def test_narrative_bucket_is_used_when_correlation_group_missing():
    mod = load_module()
    first = candidate('AAAUSDT', 100.0)
    second = candidate('BBBUSDT', 99.0)
    first.portfolio_narrative_bucket = 'AI'
    second.portfolio_narrative_bucket = 'AI'
    mod.apply_selection_diversification([first, second])
    assert first.selection_diversification_group == 'NARR:AI'
    assert second.selection_diversification_multiplier == 0.97


def test_triggered_and_untriggered_candidates_do_not_share_duplicate_count():
    mod = load_module()
    fired = candidate('AAAUSDT', 90.0, 'L1', triggered=True)
    waiting = candidate('BBBUSDT', 120.0, 'L1', triggered=False)
    mod.apply_selection_diversification([fired, waiting])
    assert fired.selection_diversification_multiplier == 1.0
    assert waiting.selection_diversification_multiplier == 1.0


def test_diversification_is_idempotent():
    mod = load_module()
    first = candidate('AAAUSDT', 100.0, 'L1')
    second = candidate('BBBUSDT', 99.0, 'L1')
    mod.apply_selection_diversification([first, second])
    first_result = (first.score, second.score, list(second.reasons))
    mod.apply_selection_diversification([first, second])
    second_result = (first.score, second.score, list(second.reasons))
    assert second_result == first_result


def test_hook_wraps_relative_reranking_once():
    mod = load_module()
    calls = []

    def rerank(rows):
        calls.append('rerank')
        return list(rows)

    relative = SimpleNamespace(rerank_candidate_cohort=rerank)
    mod.install_selection_diversification_hook(relative)
    wrapped = relative.rerank_candidate_cohort
    mod.install_selection_diversification_hook(relative)
    assert relative.rerank_candidate_cohort is wrapped

    rows = [candidate('AAAUSDT', 100.0, 'L1'), candidate('BBBUSDT', 99.0, 'L1')]
    result = relative.rerank_candidate_cohort(rows)
    assert calls == ['rerank']
    assert result[1].selection_diversification_multiplier == 0.97
