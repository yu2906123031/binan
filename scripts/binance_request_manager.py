from __future__ import annotations

import asyncio
import random
import re
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple


class BinanceCircuitOpen(RuntimeError):
    pass


class BinanceAPIThrottled(RuntimeError):
    def __init__(self, *, status_code: int, message: str, retry_after_ms: Optional[int] = None):
        super().__init__(message)
        self.status_code = int(status_code)
        self.retry_after_ms = retry_after_ms or extract_retry_after_ms(message)


@dataclass(order=True)
class _QueuedRequest:
    priority: int
    sequence: int
    request: "BinanceRequest" = field(compare=False)
    future: asyncio.Future = field(compare=False)


@dataclass(frozen=True)
class BinanceRequest:
    method: str
    path: str
    params: Dict[str, Any] = field(default_factory=dict)
    signed: bool = False
    weight: int = 1
    priority: int = 10
    timeout: float = 15.0


@dataclass
class RequestMetrics:
    enqueued_requests: int = 0
    completed_requests: int = 0
    failed_requests: int = 0
    dropped_requests: int = 0
    retry_count: int = 0
    queue_size: int = 0
    used_weight_1m: int = 0
    last_latency_ms: float = 0.0
    last_error: str = ""


def extract_retry_after_ms(message: str) -> Optional[int]:
    match = re.search(r"banned until\s+(\d{10,})", str(message), flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


class GlobalRateLimiter:
    def __init__(self, *, max_requests_per_second: int = 5, max_weight_per_minute: int = 1200):
        self.max_requests_per_second = max(1, int(max_requests_per_second))
        self.max_weight_per_minute = max(1, int(max_weight_per_minute))
        self._second_tokens = float(self.max_requests_per_second)
        self._minute_tokens = float(self.max_weight_per_minute)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(self.max_requests_per_second)

    async def acquire(self, *, weight: int = 1) -> None:
        weight = max(1, int(weight))
        async with self._semaphore:
            while True:
                async with self._lock:
                    now = time.monotonic()
                    elapsed = max(0.0, now - self._updated)
                    self._updated = now
                    self._second_tokens = min(self.max_requests_per_second, self._second_tokens + elapsed * self.max_requests_per_second)
                    self._minute_tokens = min(self.max_weight_per_minute, self._minute_tokens + elapsed * (self.max_weight_per_minute / 60.0))
                    if self._second_tokens >= 1.0 and self._minute_tokens >= weight:
                        self._second_tokens -= 1.0
                        self._minute_tokens -= weight
                        return
                    wait_second = (1.0 - self._second_tokens) / self.max_requests_per_second if self._second_tokens < 1.0 else 0.0
                    wait_minute = (weight - self._minute_tokens) / (self.max_weight_per_minute / 60.0) if self._minute_tokens < weight else 0.0
                    delay = max(wait_second, wait_minute, 0.001)
                await asyncio.sleep(delay)

    def observe_used_weight_1m(self, used_weight: int) -> None:
        remaining = max(0, self.max_weight_per_minute - int(used_weight))
        self._minute_tokens = min(self._minute_tokens, float(remaining))


class CircuitBreaker:
    def __init__(self, *, default_cooldown_seconds: float = 900.0):
        self.default_cooldown_seconds = float(default_cooldown_seconds)
        self.state = "closed"
        self.reason = ""
        self.retry_after_ms: Optional[int] = None

    def ensure_allows(self) -> None:
        if self.state != "open":
            return
        now_ms = int(time.time() * 1000)
        if self.retry_after_ms and now_ms >= self.retry_after_ms:
            self.close()
            return
        raise BinanceCircuitOpen(f"Binance circuit open: {self.reason}; retry_after_ms={self.retry_after_ms}")

    def open(self, *, reason: str, retry_after_ms: Optional[int] = None) -> None:
        self.state = "open"
        self.reason = reason
        self.retry_after_ms = retry_after_ms or int((time.time() + self.default_cooldown_seconds) * 1000)

    def close(self) -> None:
        self.state = "closed"
        self.reason = ""
        self.retry_after_ms = None


class RetryManager:
    def __init__(self, *, max_attempts: int = 3, base_delay: float = 0.25, max_delay: float = 8.0, jitter: float = 0.25):
        self.max_attempts = max(1, int(max_attempts))
        self.base_delay = max(0.0, float(base_delay))
        self.max_delay = max(self.base_delay, float(max_delay))
        self.jitter = max(0.0, float(jitter))

    async def run(self, op: Callable[[], Awaitable[Any]], *, is_retryable: Callable[[BaseException], bool]) -> Any:
        attempt = 0
        while True:
            attempt += 1
            try:
                return await op()
            except BaseException as exc:
                if attempt >= self.max_attempts or not is_retryable(exc):
                    raise
                delay = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
                if self.jitter:
                    delay += random.uniform(0.0, self.jitter * delay)
                await asyncio.sleep(delay)


class BinanceRequestManager:
    def __init__(
        self,
        *,
        transport: Callable[[BinanceRequest], Awaitable[Any]],
        limiter: Optional[GlobalRateLimiter] = None,
        retry_manager: Optional[RetryManager] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        max_queue_size: int = 512,
    ):
        self.transport = transport
        self.limiter = limiter or GlobalRateLimiter()
        self.retry_manager = retry_manager or RetryManager()
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self.queue: asyncio.PriorityQueue[_QueuedRequest] = asyncio.PriorityQueue(maxsize=max_queue_size)
        self.metrics = RequestMetrics()
        self._sequence = 0
        self._worker: Optional[asyncio.Task] = None
        self._stopping = False

    async def start(self) -> None:
        if self._worker is None or self._worker.done():
            self._stopping = False
            self._worker = asyncio.create_task(self._run_worker(), name="binance-request-manager")

    async def shutdown(self) -> None:
        self._stopping = True
        if self._worker:
            await self.queue.join()
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass

    async def request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, *, signed: bool = False, weight: int = 1, priority: int = 10, timeout: float = 15.0) -> Any:
        if self._stopping:
            raise RuntimeError("BinanceRequestManager is shutting down")
        self.circuit_breaker.ensure_allows()
        await self.start()
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._sequence += 1
        request = BinanceRequest(method=method.upper(), path=path, params=dict(params or {}), signed=signed, weight=weight, priority=priority, timeout=timeout)
        item = _QueuedRequest(priority=int(priority), sequence=self._sequence, request=request, future=future)
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            self.metrics.dropped_requests += 1
            raise RuntimeError("Binance request queue full")
        self.metrics.enqueued_requests += 1
        self.metrics.queue_size = self.queue.qsize()
        return await future

    async def _run_worker(self) -> None:
        while True:
            item = await self.queue.get()
            try:
                if item.future.cancelled():
                    continue
                self.circuit_breaker.ensure_allows()
                await self.limiter.acquire(weight=item.request.weight)
                started = time.monotonic()
                result = await self.retry_manager.run(lambda: self.transport(item.request), is_retryable=self._is_retryable)
                self.metrics.last_latency_ms = (time.monotonic() - started) * 1000.0
                self._observe_headers(result)
                self.metrics.completed_requests += 1
                if not item.future.done():
                    item.future.set_result(result)
            except BinanceAPIThrottled as exc:
                self.metrics.failed_requests += 1
                self.metrics.last_error = str(exc)
                if exc.status_code == 418:
                    self.circuit_breaker.open(reason="binance_418_ip_ban", retry_after_ms=exc.retry_after_ms)
                elif exc.status_code == 429:
                    self.circuit_breaker.open(reason="binance_429_rate_limit", retry_after_ms=exc.retry_after_ms)
                if not item.future.done():
                    item.future.set_exception(exc)
            except BaseException as exc:
                self.metrics.failed_requests += 1
                self.metrics.last_error = str(exc)
                if not item.future.done():
                    item.future.set_exception(exc)
            finally:
                self.metrics.queue_size = self.queue.qsize()
                self.queue.task_done()

    @staticmethod
    def _is_retryable(exc: BaseException) -> bool:
        if isinstance(exc, BinanceAPIThrottled):
            return exc.status_code == 429
        return isinstance(exc, (TimeoutError, ConnectionError, OSError))

    def _observe_headers(self, result: Any) -> None:
        headers: Dict[str, Any] = {}
        if isinstance(result, dict):
            maybe = result.get("headers")
            if isinstance(maybe, dict):
                headers = maybe
        value = headers.get("X-MBX-USED-WEIGHT-1M") or headers.get("x-mbx-used-weight-1m")
        if value is not None:
            try:
                self.metrics.used_weight_1m = int(value)
                self.limiter.observe_used_weight_1m(int(value))
            except (TypeError, ValueError):
                pass
