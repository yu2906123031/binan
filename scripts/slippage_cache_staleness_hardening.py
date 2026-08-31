from __future__ import annotations

from typing import Any, Dict


DEFAULT_MAX_STALE_SECONDS = 6 * 60 * 60


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def install_slippage_cache_staleness_hardening(policy_module: Any) -> None:
    original = getattr(policy_module, 'load_calibration_payload', None)
    cache = getattr(policy_module, '_POLICY_CACHE', None)
    if not callable(original) or not isinstance(cache, dict) or getattr(original, '_slippage_cache_staleness_hardening', False):
        return

    cache.setdefault('successful_at_monotonic', 0.0)
    original_reset = getattr(policy_module, 'reset_calibration_cache', None)

    def load_calibration_payload_with_staleness(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        current = kwargs.get('now_monotonic')
        if current is None:
            current = policy_module.time.monotonic()
        current = float(current)
        max_stale_seconds = max(_number(kwargs.pop('max_stale_seconds', DEFAULT_MAX_STALE_SECONDS)), 0.0)

        cached_payload = cache.get('payload')
        successful_at = _number(cache.get('successful_at_monotonic'))
        if successful_at <= 0 and isinstance(cached_payload, dict) and not cache.get('last_error'):
            successful_at = _number(cache.get('loaded_at_monotonic'))
            cache['successful_at_monotonic'] = successful_at
        successful_age = current - successful_at if successful_at > 0 else None

        if successful_age is not None and successful_age >= max_stale_seconds:
            cache['events_mtime_ns'] = object()

        previous_payload = cached_payload if isinstance(cached_payload, dict) else None
        previous_successful_at = successful_at
        result = original(*args, **kwargs)

        if not cache.get('last_error'):
            rebuilt_payload = cache.get('payload')
            if previous_payload is None or rebuilt_payload is not previous_payload:
                cache['successful_at_monotonic'] = current
            elif previous_successful_at > 0:
                cache['successful_at_monotonic'] = previous_successful_at
            return result

        if previous_payload is not None and previous_successful_at > 0:
            failure_age = current - previous_successful_at
            if 0 <= failure_age < max_stale_seconds:
                cache['successful_at_monotonic'] = previous_successful_at
                return previous_payload

        cache['payload'] = {}
        cache['successful_at_monotonic'] = 0.0
        return {}

    load_calibration_payload_with_staleness._slippage_cache_staleness_hardening = True  # type: ignore[attr-defined]
    policy_module.load_calibration_payload = load_calibration_payload_with_staleness

    if callable(original_reset) and not getattr(original_reset, '_slippage_cache_staleness_hardening', False):
        def reset_calibration_cache_with_staleness() -> None:
            original_reset()
            cache['successful_at_monotonic'] = 0.0

        reset_calibration_cache_with_staleness._slippage_cache_staleness_hardening = True  # type: ignore[attr-defined]
        policy_module.reset_calibration_cache = reset_calibration_cache_with_staleness
