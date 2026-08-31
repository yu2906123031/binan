from __future__ import annotations

from typing import Any


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def evaluate_research_gate(metrics: dict[str, Any], *, min_samples: int = 50) -> dict[str, Any]:
    samples = int(_num(metrics.get('sample_count')))
    net_expectancy_delta = _num(metrics.get('oos_net_expectancy_delta_r'))
    drawdown_delta = _num(metrics.get('max_drawdown_delta_r'))
    adverse_selection_delta = _num(metrics.get('adverse_selection_delta'))
    capacity_delta = _num(metrics.get('capacity_delta'))
    turnover_delta = _num(metrics.get('turnover_delta_pct'))
    tail_loss_delta = _num(metrics.get('tail_loss_delta_r'))
    cost_stress_delta = _num(metrics.get('cost_stress_expectancy_delta_r'))
    latency_stress_delta = _num(metrics.get('latency_stress_expectancy_delta_r'))

    enough_samples = samples >= max(int(min_samples), 1)
    improves_primary = (
        net_expectancy_delta > 0
        or drawdown_delta < 0
        or adverse_selection_delta < 0
        or capacity_delta > 0
    )
    robustness_ok = cost_stress_delta >= -0.02 and latency_stress_delta >= -0.02
    side_effects_ok = turnover_delta <= 15.0 and tail_loss_delta <= 0.02
    approved = enough_samples and improves_primary and robustness_ok and side_effects_ok
    reasons: list[str] = []
    if not enough_samples:
        reasons.append('insufficient_samples')
    if not improves_primary:
        reasons.append('no_primary_improvement')
    if not robustness_ok:
        reasons.append('stress_test_regression')
    if not side_effects_ok:
        reasons.append('side_effect_regression')
    return {
        'approved_for_live': approved,
        'sample_count': samples,
        'min_samples': int(min_samples),
        'reasons': reasons,
        'requirements': {
            'walk_forward_oos': True,
            'embargo': True,
            'cost_stress': True,
            'latency_stress': True,
            'capacity_stress': True,
        },
    }
