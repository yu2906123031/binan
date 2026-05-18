import asyncio
import sys
import time
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.binance_request_manager import (
    BinanceAPIThrottled,
    BinanceCircuitOpen,
    BinanceRequest,
    BinanceRequestManager,
    CircuitBreaker,
    GlobalRateLimiter,
    RetryManager,
)


@pytest.mark.asyncio
async def test_rate_limiter_enforces_request_spacing_without_busy_loop():
    limiter = GlobalRateLimiter(max_requests_per_second=2, max_weight_per_minute=100)
    started = time.monotonic()
    await limiter.acquire(weight=1)
    await limiter.acquire(weight=1)
    await limiter.acquire(weight=1)
    elapsed = time.monotonic() - started
    assert elapsed >= 0.45


@pytest.mark.asyncio
async def test_request_manager_serializes_requests_through_priority_queue_and_tracks_weight():
    calls = []

    async def transport(req: BinanceRequest):
        calls.append(req.path)
        return {"path": req.path, "headers": {"X-MBX-USED-WEIGHT-1M": "12"}}

    manager = BinanceRequestManager(transport=transport, limiter=GlobalRateLimiter(max_requests_per_second=50, max_weight_per_minute=100))
    await manager.start()
    low = asyncio.create_task(manager.request("GET", "/low", priority=50, weight=1))
    high = asyncio.create_task(manager.request("GET", "/high", priority=1, weight=1))
    assert await high == {"path": "/high", "headers": {"X-MBX-USED-WEIGHT-1M": "12"}}
    assert await low == {"path": "/low", "headers": {"X-MBX-USED-WEIGHT-1M": "12"}}
    await manager.shutdown()
    assert calls == ["/high", "/low"]
    assert manager.metrics.used_weight_1m == 12
    assert manager.metrics.completed_requests == 2


@pytest.mark.asyncio
async def test_retry_manager_has_bounded_budget_and_jittered_backoff():
    attempts = 0

    async def op():
        nonlocal attempts
        attempts += 1
        raise TimeoutError("network")

    retry = RetryManager(max_attempts=3, base_delay=0.001, max_delay=0.01, jitter=0.0)
    with pytest.raises(TimeoutError):
        await retry.run(op, is_retryable=lambda exc: True)
    assert attempts == 3


@pytest.mark.asyncio
async def test_418_opens_global_circuit_and_blocks_followup_requests():
    async def transport(req: BinanceRequest):
        raise BinanceAPIThrottled(status_code=418, message="IP banned until 9999999999999")

    breaker = CircuitBreaker(default_cooldown_seconds=60)
    manager = BinanceRequestManager(
        transport=transport,
        limiter=GlobalRateLimiter(max_requests_per_second=50, max_weight_per_minute=100),
        circuit_breaker=breaker,
        retry_manager=RetryManager(max_attempts=1, base_delay=0.001, jitter=0.0),
    )
    await manager.start()
    with pytest.raises(BinanceAPIThrottled):
        await manager.request("GET", "/fapi/v1/klines", priority=10, weight=1)
    assert breaker.state == "open"
    with pytest.raises(BinanceCircuitOpen):
        await manager.request("GET", "/fapi/v1/ticker/24hr", priority=10, weight=1)
    await manager.shutdown()


@pytest.mark.asyncio
async def test_shutdown_rejects_new_requests_and_drains_worker():
    async def transport(req: BinanceRequest):
        return {"ok": True, "headers": {}}

    manager = BinanceRequestManager(transport=transport)
    await manager.start()
    assert await manager.request("GET", "/ok") == {"ok": True, "headers": {}}
    await manager.shutdown()
    with pytest.raises(RuntimeError):
        await manager.request("GET", "/after-shutdown")
