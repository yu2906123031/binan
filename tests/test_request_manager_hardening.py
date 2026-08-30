import asyncio

import pytest

from scripts.binance_request_manager import BinanceRequest, BinanceRequestManager, GlobalRateLimiter, RetryManager


@pytest.mark.asyncio
async def test_hung_transport_is_cancelled_and_worker_recovers():
    calls = []
    cancelled = asyncio.Event()

    async def transport(req: BinanceRequest):
        calls.append(req.path)
        if req.path == '/hung':
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
        return {'path': req.path, 'headers': {}}

    manager = BinanceRequestManager(
        transport=transport,
        limiter=GlobalRateLimiter(max_requests_per_second=50, max_weight_per_minute=100),
        retry_manager=RetryManager(max_attempts=1),
    )
    with pytest.raises(TimeoutError):
        await manager.request('GET', '/hung', timeout=0.03)
    await asyncio.wait_for(cancelled.wait(), timeout=0.5)
    assert await manager.request('GET', '/after', timeout=0.5) == {'path': '/after', 'headers': {}}
    await manager.shutdown()
    assert calls == ['/hung', '/after']


@pytest.mark.asyncio
async def test_non_idempotent_hung_write_is_cancelled_without_retry():
    attempts = 0

    async def transport(req: BinanceRequest):
        nonlocal attempts
        attempts += 1
        await asyncio.Event().wait()

    manager = BinanceRequestManager(
        transport=transport,
        limiter=GlobalRateLimiter(max_requests_per_second=50, max_weight_per_minute=100),
        retry_manager=RetryManager(max_attempts=3, base_delay=0.001, jitter=0.0),
    )
    with pytest.raises(TimeoutError):
        await manager.request('POST', '/fapi/v1/order', timeout=0.03)
    await asyncio.sleep(0.02)
    await manager.shutdown()
    assert attempts == 1
    assert manager.metrics.retry_count == 0


@pytest.mark.asyncio
async def test_shutdown_is_bounded_even_if_transport_is_hung():
    started = asyncio.Event()

    async def transport(req: BinanceRequest):
        started.set()
        await asyncio.Event().wait()

    manager = BinanceRequestManager(
        transport=transport,
        limiter=GlobalRateLimiter(max_requests_per_second=50, max_weight_per_minute=100),
        retry_manager=RetryManager(max_attempts=1),
        shutdown_timeout=0.03,
    )
    task = asyncio.create_task(manager.request('GET', '/hung', timeout=10.0))
    await asyncio.wait_for(started.wait(), timeout=0.5)
    await asyncio.wait_for(manager.shutdown(), timeout=0.5)
    assert manager._worker is None
    if not task.done():
        task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
