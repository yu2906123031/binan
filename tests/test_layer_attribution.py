from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def load_module():
    spec = importlib.util.spec_from_file_location('layer_attribution_test', SCRIPTS_DIR / 'layer_attribution.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def candidate(symbol: str, *, raw: float, triggered: bool, final: float):
    row = SimpleNamespace(
        symbol=symbol,
        side='LONG',
        score=final,
        base_ranking_score=raw,
        market_direction_score_multiplier=1.02,
        realizable_edge_score_multiplier=1.03,
        selection_quality_multiplier=1.01,
        selection_quality_score=raw * 1.02 * 1.03 * 1.01,
        relative_selection_base_score=raw * 1.02 * 1.03 * 1.01,
        relative_selection_multiplier=1.04,
        diversification_base_score=raw * 1.02 * 1.03 * 1.01 * 1.04,
        selection_diversification_multiplier=0.97,
        stability_base_score=raw * 1.02 * 1.03 * 1.01 * 1.04 * 0.97,
        selection_stability_multiplier=1.02,
        selection_outcome_base_score=raw * 1.02 * 1.03 * 1.01 * 1.04 * 0.97 * 1.02,
        selection_outcome_multiplier=1.01,
        trigger_fired=triggered,
        pretrigger_watch=False,
        candidate_stage='triggered' if triggered else 'watch',
        state='active',
        alert_tier='A',
        trigger_class='BREAKOUT',
        market_regime_label='TREND',
        last_price=100.0,
        expected_edge=1.2,
        realizable_edge_margin_r=0.8,
        expected_slippage_pct=0.03,
        liquidity_grade='A',
        relative_selection_percentile=0.9,
        selection_stability_streak=3,
    )
    return row


def test_reconstruct_stage_scores_uses_existing_layer_baselines():
    mod = load_module()
    row = candidate('BTCUSDT', raw=100.0, triggered=True, final=109.0)
    scores = mod.reconstruct_stage_scores(row)
    assert scores['raw'] == 100.0
    assert scores['market'] == 102.0
    assert scores['realizable_edge'] == 105.06
    assert scores['selection_quality'] == round(100.0 * 1.02 * 1.03 * 1.01, 4)
    assert scores['relative_selection'] == round(row.relative_selection_base_score * 1.04, 4)
    assert scores['diversification'] == round(row.diversification_base_score * 0.97, 4)
    assert scores['stability'] == round(row.stability_base_score * 1.02, 4)
    assert scores['outcome_calibration'] == 109.0


def test_trigger_priority_can_change_winner_without_changing_score():
    mod = load_module()
    high_watch = candidate('WATCHUSDT', raw=120.0, triggered=False, final=120.0)
    lower_trigger = candidate('FIREUSDT', raw=100.0, triggered=True, final=100.0)
    payload = mod.build_layer_attribution_payload([high_watch, lower_trigger], 'scan-1')
    winners = {row['stage']: row['winner']['symbol'] for row in payload['stages']}
    assert winners['raw'] == 'WATCHUSDT'
    assert winners['market'] == 'WATCHUSDT'
    assert winners['trigger_priority'] == 'FIREUSDT'
    assert winners['outcome_calibration'] == 'FIREUSDT'


def test_payload_contains_counterfactual_candidates_and_scan_id():
    mod = load_module()
    row = candidate('BTCUSDT', raw=100.0, triggered=True, final=109.0)
    payload = mod.build_layer_attribution_payload([row], 'scan-abc')
    assert payload['attribution_version'] == 1
    assert payload['scan_id'] == 'scan-abc'
    assert payload['candidate_count'] == 1
    snapshot = payload['candidates'][0]
    assert snapshot['symbol'] == 'BTCUSDT'
    assert snapshot['stage_scores']['raw'] == 100.0
    assert snapshot['entry_reference_price'] == 100.0


def test_hook_persists_one_scan_event_and_tags_candidate():
    mod = load_module()
    events = []

    class Store:
        def append_event(self, event_type, payload):
            events.append((event_type, payload))

    store = Store()
    strategy = SimpleNamespace()

    def build_alert(row, *args, **kwargs):
        return {'symbol': row.symbol}

    row = candidate('BTCUSDT', raw=100.0, triggered=True, final=109.0)

    def run_scan(client, args):
        strategy.build_standardized_alert(row)
        return 'ok'

    strategy.build_standardized_alert = build_alert
    strategy.run_scan_once = run_scan
    strategy.get_runtime_state_store = lambda args: store

    mod.install_layer_attribution_hook(strategy)
    assert strategy.run_scan_once(None, SimpleNamespace()) == 'ok'
    assert getattr(row, 'layer_attribution_scan_id', '')
    assert len(events) == 1
    assert events[0][0] == mod.ATTRIBUTION_EVENT_TYPE
    assert events[0][1]['scan_id'] == row.layer_attribution_scan_id


def test_hook_is_idempotent():
    mod = load_module()
    strategy = SimpleNamespace(
        build_standardized_alert=lambda row: row,
        run_scan_once=lambda client, args: 'ok',
        get_runtime_state_store=lambda args: SimpleNamespace(append_event=lambda *a, **k: None),
    )
    mod.install_layer_attribution_hook(strategy)
    first_run = strategy.run_scan_once
    first_build = strategy.build_standardized_alert
    mod.install_layer_attribution_hook(strategy)
    assert strategy.run_scan_once is first_run
    assert strategy.build_standardized_alert is first_build
