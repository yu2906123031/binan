from __future__ import annotations

from typing import Any

from lifecycle_snapshot import install_lifecycle_snapshot_hooks
from request_throttle_hardening import install_request_throttle_hardening
from rest_guard_retry_after_hardening import install_rest_guard_retry_after_hardening
from risk_exit_guard import install_reduce_only_risk_guard
from selection_diversification_policy import install_selection_diversification_hook
from selection_outcome_policy import install_selection_outcome_hook
from selection_quality_policy import install_selection_quality_hook
from selection_stability_policy import install_selection_stability_hook
from slippage_cache_staleness_hardening import install_slippage_cache_staleness_hardening


def install_startup_hardening(
    *,
    strategy_module: Any,
    request_manager_module: Any,
    slippage_calibration_policy_module: Any,
    relative_selection_policy_module: Any,
) -> None:
    """Install the remaining compatibility layers once, in deterministic order.

    Runtime-state durability, execution feedback normalization, and trade-bucket
    directional slippage are now installed by their canonical modules directly.
    """
    install_request_throttle_hardening(request_manager_module)

    install_rest_guard_retry_after_hardening(strategy_module)
    install_lifecycle_snapshot_hooks(strategy_module)
    install_reduce_only_risk_guard(strategy_module)

    install_slippage_cache_staleness_hardening(slippage_calibration_policy_module)
    install_selection_quality_hook(strategy_module)
    slippage_calibration_policy_module.install_slippage_calibration_hooks(strategy_module)

    # The relative-selection wrapper chain must be composed before the strategy
    # scan hook captures rerank_candidate_cohort.
    install_selection_diversification_hook(relative_selection_policy_module)
    install_selection_stability_hook(relative_selection_policy_module, strategy_module)
    install_selection_outcome_hook(relative_selection_policy_module, strategy_module)
    relative_selection_policy_module.install_relative_selection_hook(strategy_module)
