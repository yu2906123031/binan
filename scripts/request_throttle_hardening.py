from __future__ import annotations

import time
from typing import Any


DEFAULT_429_COOLDOWN_SECONDS = 5.0


def install_request_throttle_hardening(request_manager_module: Any, *, ordinary_429_cooldown_seconds: float = DEFAULT_429_COOLDOWN_SECONDS) -> None:
    breaker_cls = getattr(request_manager_module, 'CircuitBreaker', None)
    if breaker_cls is None:
        return
    original_open = getattr(breaker_cls, 'open', None)
    if not callable(original_open) or getattr(original_open, '_throttle_hardened', False):
        return

    short_cooldown = max(float(ordinary_429_cooldown_seconds or 0.0), 0.1)

    def open_with_status_aware_cooldown(self: Any, *, reason: str, retry_after_ms: int | None = None) -> None:
        effective_retry_after = retry_after_ms
        if effective_retry_after is None and str(reason or '') == 'binance_429_rate_limit':
            effective_retry_after = int((time.time() + short_cooldown) * 1000)
        original_open(self, reason=reason, retry_after_ms=effective_retry_after)

    open_with_status_aware_cooldown._throttle_hardened = True  # type: ignore[attr-defined]
    breaker_cls.open = open_with_status_aware_cooldown
