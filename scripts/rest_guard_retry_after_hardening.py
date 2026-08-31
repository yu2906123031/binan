from __future__ import annotations

import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Mapping, Optional


DEFAULT_429_COOLDOWN_SECONDS = 5.0


def _header_value(headers: Any, name: str) -> str:
    if not isinstance(headers, Mapping):
        try:
            return str(headers.get(name) or headers.get(name.lower()) or '')
        except Exception:
            return ''
    target = name.lower()
    for key, value in headers.items():
        if str(key).lower() == target:
            return str(value or '').strip()
    return ''


def extract_retry_after_header_ms(headers: Any, *, now_ms: int) -> Optional[int]:
    raw = _header_value(headers, 'Retry-After').strip()
    if not raw:
        return None
    try:
        seconds = float(raw)
    except (TypeError, ValueError):
        seconds = -1.0
    if seconds >= 0:
        target = int(now_ms + seconds * 1000.0)
        return target if target > now_ms else None
    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    target = int(parsed.timestamp() * 1000.0)
    return target if target > now_ms else None


def extract_response_retry_after_ms(strategy_module: Any, response: Any, *, now_ms: int) -> Optional[int]:
    candidates = []
    header_ms = extract_retry_after_header_ms(getattr(response, 'headers', {}), now_ms=now_ms)
    if header_ms is not None:
        candidates.append(header_ms)
    extract_message = getattr(strategy_module, '_extract_retry_after_ms_from_message', None)
    if callable(extract_message):
        try:
            payload = response.json()
        except Exception:
            payload = getattr(response, 'text', '')
        try:
            body_ms = extract_message(payload)
        except Exception:
            body_ms = None
        if body_ms is not None and int(body_ms) > now_ms:
            candidates.append(int(body_ms))
    return max(candidates) if candidates else None


def _set_guard_open(strategy_module: Any, *, status_code: int, retry_after_ms: Optional[int], now_ms: int) -> None:
    mutate_state = getattr(strategy_module, '_with_binance_rest_guard_state', None)
    if not callable(mutate_state):
        return
    if status_code == 418:
        fallback_seconds = float(getattr(strategy_module, 'REST_418_COOLDOWN_SECONDS', 3600.0) or 3600.0)
        reason = 'http_418_ip_ban'
    else:
        fallback_seconds = float(getattr(strategy_module, 'REST_429_COOLDOWN_SECONDS', DEFAULT_429_COOLDOWN_SECONDS) or DEFAULT_429_COOLDOWN_SECONDS)
        reason = 'http_429_rate_limit'
    open_until_ms = int(retry_after_ms) if retry_after_ms is not None else int(now_ms + max(fallback_seconds, 0.0) * 1000.0)

    def mutate(guard: dict[str, Any]) -> None:
        guard['rest_circuit_state'] = 'OPEN'
        guard['rest_circuit_reason'] = reason
        guard['circuit_open_until_ms'] = open_until_ms
        guard['next_rest_probe_at_ms'] = open_until_ms
        guard['half_open_probe_used'] = False

    mutate_state(mutate)


def install_rest_guard_retry_after_hardening(
    strategy_module: Any,
    *,
    ordinary_429_cooldown_seconds: float = DEFAULT_429_COOLDOWN_SECONDS,
) -> None:
    strategy_module.REST_429_COOLDOWN_SECONDS = max(float(ordinary_429_cooldown_seconds), 0.0)

    original_after_response = getattr(strategy_module, '_binance_rest_guard_after_response', None)
    if callable(original_after_response) and not getattr(original_after_response, '_retry_after_hardening', False):
        def after_response_with_retry_after(response: Any, *, purpose: str = '', path: str = '', request_latency_ms: int = 0) -> None:
            original_after_response(response, purpose=purpose, path=path, request_latency_ms=request_latency_ms)
            status_code = int(getattr(response, 'status_code', 0) or 0)
            if status_code not in {418, 429}:
                return
            now_fn = getattr(strategy_module, '_rest_now_ms', None)
            now_ms = int(now_fn()) if callable(now_fn) else int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000.0)
            explicit_retry_after_ms = extract_response_retry_after_ms(strategy_module, response, now_ms=now_ms)
            _set_guard_open(
                strategy_module,
                status_code=status_code,
                retry_after_ms=explicit_retry_after_ms,
                now_ms=now_ms,
            )

        after_response_with_retry_after._retry_after_hardening = True  # type: ignore[attr-defined]
        strategy_module._binance_rest_guard_after_response = after_response_with_retry_after

    original_after_error = getattr(strategy_module, '_binance_rest_guard_after_error', None)
    if callable(original_after_error) and not getattr(original_after_error, '_retry_after_hardening', False):
        def after_error_preserving_response_hint(error: Any, fallback_cooldown_seconds: float = 900.0) -> None:
            message = str(error)
            lowered = message.lower()
            status_code = 418 if ('binance api error 418' in lowered or 'ip banned' in lowered) else 429 if ('binance api error 429' in lowered or 'too many requests' in lowered) else 0
            if status_code:
                extract_message = getattr(strategy_module, '_extract_retry_after_ms_from_message', None)
                explicit_from_message = extract_message(message) if callable(extract_message) else None
                snapshot_fn = getattr(strategy_module, '_binance_rest_guard_snapshot', None)
                now_fn = getattr(strategy_module, '_rest_now_ms', None)
                if explicit_from_message is None and callable(snapshot_fn) and callable(now_fn):
                    snapshot = snapshot_fn()
                    reason = str(snapshot.get('rest_circuit_reason') or snapshot.get('reason') or '').lower()
                    open_until_ms = int(snapshot.get('circuit_open_until_ms') or 0)
                    expected_marker = '418' if status_code == 418 else '429'
                    if expected_marker in reason and open_until_ms > int(now_fn()):
                        return
            original_after_error(error, fallback_cooldown_seconds=fallback_cooldown_seconds)

        after_error_preserving_response_hint._retry_after_hardening = True  # type: ignore[attr-defined]
        strategy_module._binance_rest_guard_after_error = after_error_preserving_response_hint
