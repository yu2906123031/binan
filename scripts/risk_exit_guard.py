from __future__ import annotations

from typing import Any, Dict


def _is_reduce_only_context(kwargs: Dict[str, Any]) -> bool:
    return bool(kwargs.get('reduce_only') or kwargs.get('close_position') or kwargs.get('allow_reduce_only'))


def install_reduce_only_risk_guard(strategy_module: Any) -> None:
    """Ensure risk-reducing exits cannot be blocked by entry-only risk guards."""
    original = getattr(strategy_module, 'evaluate_risk_guards', None)
    if not callable(original) or getattr(original, '_reduce_only_risk_guard', False):
        return

    def evaluate_risk_guards_with_reduce_only(*args: Any, **kwargs: Any):
        result = original(*args, **kwargs)
        if not _is_reduce_only_context(kwargs) or not isinstance(result, dict):
            return result
        normalized = dict(result)
        normalized['allowed'] = True
        normalized['reasons'] = []
        normalized['reduce_only_override'] = True
        normalized['reduce_only_original_reasons'] = list(result.get('reasons') or [])
        return normalized

    evaluate_risk_guards_with_reduce_only._reduce_only_risk_guard = True  # type: ignore[attr-defined]
    strategy_module.evaluate_risk_guards = evaluate_risk_guards_with_reduce_only
