from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import datetime
import hashlib
import hmac
import json
import math
import multiprocessing
import os
import pickle
import random
import re
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field, replace

from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import quote, urlencode

import requests
import portalocker

_REAL_TIME_TIME = time.time

from execution_engine import cancel_stale_protection_orders_after_flat as execution_cancel_stale_protection_orders_after_flat, ensure_symbol_margin_type as execution_ensure_symbol_margin_type, monitor_live_trade as execution_monitor_live_trade, place_initial_stop_with_retries as execution_place_initial_stop_with_retries, place_live_trade as execution_place_live_trade, repair_missing_protection as execution_repair_missing_protection, resolve_position_protection_status as execution_resolve_position_protection_status, start_trade_monitor_thread as execution_start_trade_monitor_thread
import candidate_builder as candidate_builder_mod  # noqa: F401 - re-exported for regression tests and helper compatibility
from candidate_builder import build_candidate as build_candidate_impl
from market_direction_bias import apply_market_direction_bias, compute_market_direction_bias
from risk_engine import evaluate_portfolio_risk_guards as evaluate_portfolio_risk_guards_impl, evaluate_risk_guards as evaluate_risk_guards_impl
from risk_state_helpers import normalize_loaded_risk_state as normalize_loaded_risk_state_impl, refresh_risk_state_heat_snapshot as refresh_risk_state_heat_snapshot_impl
from runtime_state_risk_helpers import build_local_open_positions_from_state as build_local_open_positions_from_state_impl, load_local_open_positions_for_risk as load_local_open_positions_for_risk_impl, load_runtime_risk_state as load_runtime_risk_state_impl
from runtime_store import CANONICAL_RUNTIME_STATE_DIR, LEGACY_RUNTIME_STATE_DIR, RuntimeStateStore as RuntimeStateStoreImpl, restore_position_lifecycle_fields as restore_position_lifecycle_fields_impl, save_positions_state as save_positions_state_impl, validate_runtime_state_layout

try:
    import websocket
except ImportError:  # pragma: no cover - optional dependency in some test environments
    websocket = None


class BinanceAPIError(RuntimeError):
    pass


class OKXAPIError(RuntimeError):
    pass


_OKX_SWAP_INST_ID_CACHE: Dict[str, Any] = {
    'base_url': '',
    'fetched_at': 0.0,
    'inst_ids': set(),
}

# Runtime heartbeat is intentionally actor-writable: each actor may publish only its own
# component heartbeat. Durable trading state and risk state remain manager-owned.
_RUNTIME_HEARTBEAT_WRITER_MODE = 'actor_component_owner'

_BOOK_TICKER_WS_SUPERVISOR_LOCK = threading.Lock()
_BOOK_TICKER_WS_SUPERVISOR_STATE: Dict[str, Any] = {
    'thread': None,
    'thread_name': '',
    'started_at': '',
    'symbols': [],
    'generation_id': 0,
    'ws': None,
    'force_restart_requested_at': '',
}

_EVENT_RATE_LIMIT_STATE_CACHE: Dict[int, Dict[str, Any]] = {}
_EVENT_RATE_LIMIT_STATE_LAST_SAVE_TS: Dict[int, float] = {}
_EVENT_RATE_LIMIT_STATE_LOCK = threading.Lock()
_EVENT_RATE_LIMIT_STATE_FLUSH_INTERVAL_SECONDS = 45.0
_EVENT_RATE_LIMIT_MIN_INTERVAL_OVERRIDES = {
    'book_ticker_ws_connected': 300.0,
    'book_ticker_ws_reconnected': 300.0,
    'book_ticker_ws_disconnected': 300.0,
    'book_ticker_cache_hit': 300.0,
    'book_ticker_cache_miss': 300.0,
}

_BOOK_TICKER_CACHE_STATE_CACHE: Dict[int, Dict[str, Any]] = {}
_BOOK_TICKER_CACHE_LAST_FLUSH_TS: Dict[int, float] = {}
_BOOK_TICKER_CACHE_LOCK = threading.Lock()
_BOOK_TICKER_DEFAULT_FLUSH_INTERVAL_SECONDS = 60.0
_BOOK_TICKER_DEBUG_SAMPLE_EVENTS = str(os.environ.get('BINANCE_BOOK_TICKER_DEBUG_SAMPLE_EVENTS', '')).strip().lower() in {'1', 'true', 'yes', 'on'}


def _runtime_store_cache_key(store: Any) -> Any:
    runtime_state_dir = getattr(store, 'runtime_state_dir', None)
    if runtime_state_dir is not None:
        return str(Path(str(runtime_state_dir)).expanduser().resolve())
    return id(store)


def _strip_dotenv_value(value: str) -> str:
    value = str(value or '').strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def load_dotenv(path: Optional[Path] = None, override: bool = False) -> Dict[str, str]:
    dotenv_path = Path(path) if path is not None else Path(__file__).resolve().parents[1] / '.env'
    loaded: Dict[str, str] = {}
    if not dotenv_path.exists():
        return loaded
    try:
        lines = dotenv_path.read_text(encoding='utf-8').splitlines()
    except OSError:
        return loaded
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        if key.startswith('export '):
            key = key[7:].strip()
        if not key or (not override and key in os.environ):
            continue
        loaded[key] = _strip_dotenv_value(value)
        os.environ[key] = loaded[key]
    return loaded


def normalize_scanner_proxy_url(raw_url: str) -> str:
    raw_url = str(raw_url or '').strip()
    if not raw_url:
        return ''
    if '://' not in raw_url:
        raw_url = f'socks5://{raw_url}'
    scheme, remainder = raw_url.split('://', 1)
    scheme = scheme.strip().lower() or 'socks5'
    remainder = remainder.strip().strip('/')
    if '@' in remainder:
        return f'{scheme}://{remainder}'
    parts = remainder.split(':')
    if len(parts) == 4:
        host, port, username, password = parts
        return f'{scheme}://{quote(username, safe="")}:{quote(password, safe="")}@{host}:{port}'
    return f'{scheme}://{remainder}'


def parse_scanner_proxy_urls(raw_urls: Any) -> List[str]:
    if raw_urls is None:
        return []
    if isinstance(raw_urls, str):
        candidates = re.split(r'[\s,]+', raw_urls.strip())
    else:
        candidates = [str(item or '').strip() for item in raw_urls]
    normalized = [normalize_scanner_proxy_url(candidate) for candidate in candidates]
    return [url for url in normalized if url]


def choose_scanner_proxy_url(proxy_urls: Sequence[str]) -> str:
    if not proxy_urls:
        return ''
    return random.choice(list(proxy_urls))


_BINANCE_REST_GUARD_LOCK = threading.Lock()
_BINANCE_REST_GUARD_STATE: Dict[str, Any] = {
    'window_started_at_ms': 0,
    'request_count_1s': 0,
    'circuit_open_until_ms': 0,
    'rest_used_weight_1m': 0,
    'rest_circuit_state': 'CLOSED',
    'rest_circuit_reason': '',
    'next_rest_probe_at_ms': 0,
    'half_open_probe_used': False,
    'recovering_until_ms': 0,
}
_BINANCE_REST_GUARD_STATE_PATH = Path(os.getenv('BINANCE_REST_GUARD_STATE_PATH', f'/tmp/binance_rest_guard_{os.getpid()}_{time.monotonic_ns()}.json'))
_BINANCE_REST_WEIGHT_BY_PURPOSE: Dict[str, int] = {}


def configure_binance_rest_guard_store(runtime_state_dir: Any = None) -> Path:
    global _BINANCE_REST_GUARD_STATE_PATH
    base = Path(runtime_state_dir or CANONICAL_RUNTIME_STATE_DIR).expanduser()
    base.mkdir(parents=True, exist_ok=True)
    _BINANCE_REST_GUARD_STATE_PATH = base / 'binance_rest_guard.json'
    return _BINANCE_REST_GUARD_STATE_PATH


def _normalize_rest_guard_state(raw: Any) -> Dict[str, Any]:
    state = dict(_BINANCE_REST_GUARD_STATE)
    if isinstance(raw, dict):
        state.update(raw)
    if 'state' in state and 'rest_circuit_state' not in state:
        state['rest_circuit_state'] = state.get('state')
    if 'reason' in state and 'rest_circuit_reason' not in state:
        state['rest_circuit_reason'] = state.get('reason')
    if 'last_used_weight_1m' in state and not state.get('rest_used_weight_1m'):
        state['rest_used_weight_1m'] = state.get('last_used_weight_1m')
    if 'next_probe_at_ms' in state and not state.get('next_rest_probe_at_ms'):
        state['next_rest_probe_at_ms'] = state.get('next_probe_at_ms')
    if 'request_count' in state and not state.get('request_count_1s'):
        state['request_count_1s'] = state.get('request_count')
    if 'window_started_at' in state and not state.get('window_started_at_ms'):
        try:
            state['window_started_at_ms'] = int(float(state.get('window_started_at') or 0) * 1000)
        except Exception:
            state['window_started_at_ms'] = 0
    state['rest_circuit_state'] = str(state.get('rest_circuit_state') or 'CLOSED').upper()
    state['rest_circuit_reason'] = str(state.get('rest_circuit_reason') or '')
    for key in ['rest_used_weight_1m', 'next_rest_probe_at_ms', 'circuit_open_until_ms', 'request_count_1s', 'window_started_at_ms', 'recovering_until_ms']:
        try:
            state[key] = int(float(state.get(key) or 0))
        except Exception:
            state[key] = 0
    state['half_open_probe_used'] = bool(state.get('half_open_probe_used'))
    return state


def _write_rest_guard_state_locked(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f'.{path.name}.{os.getpid()}.{threading.get_ident()}.tmp')
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding='utf-8')
    os.replace(tmp, path)


def _with_binance_rest_guard_state(mutator: Callable[[Dict[str, Any]], Any]) -> Any:
    path = _BINANCE_REST_GUARD_STATE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.parent / f'.{path.name}.lock'
    with _BINANCE_REST_GUARD_LOCK:
        with lock_path.open('a+', encoding='utf-8') as lock_fh:
            portalocker.lock(lock_fh, portalocker.LOCK_EX)
            try:
                try:
                    raw = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
                except Exception:
                    raw = {}
                state = _normalize_rest_guard_state(raw)
                result = mutator(state)
                _BINANCE_REST_GUARD_STATE.clear()
                _BINANCE_REST_GUARD_STATE.update(_normalize_rest_guard_state(state))
                _write_rest_guard_state_locked(path, _BINANCE_REST_GUARD_STATE)
                return result
            finally:
                portalocker.unlock(lock_fh)

REST_WEIGHT_SLOWDOWN_THRESHOLD = 1200
REST_WEIGHT_SCANNER_BLOCK_THRESHOLD = 1500
REST_WEIGHT_CORE_ONLY_THRESHOLD = 1800
REST_429_COOLDOWN_SECONDS = 120.0
REST_418_COOLDOWN_SECONDS = 3600.0


def _extract_response_used_weight_1m(response: Any) -> int:
    try:
        value = response.headers.get('X-MBX-USED-WEIGHT-1M') or response.headers.get('x-mbx-used-weight-1m')
        return int(value or 0)
    except Exception:
        return 0


def _parse_binance_ban_until_text(value: Any) -> Optional[int]:
    text = str(value or '').strip().strip('.').strip()
    numeric = re.match(r'^(\d{10}|\d{13})$', text)
    if numeric:
        raw = int(numeric.group(1))
        return raw * 1000 if len(numeric.group(1)) == 10 else raw
    date_match = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*CST$', text, flags=re.IGNORECASE)
    if date_match:
        try:
            dt = datetime.datetime.strptime(date_match.group(1), '%Y-%m-%d %H:%M:%S')
            dt = dt.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8)))
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None
    return None


def _extract_retry_after_ms_from_message(message: Any) -> Optional[int]:
    text = str(message)
    match = re.search(r'banned until\s+([0-9]{10,13}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s*CST)', text, flags=re.IGNORECASE)
    if match:
        return _parse_binance_ban_until_text(match.group(1))
    return None


def _rest_now_ms() -> int:
    return int(_REAL_TIME_TIME() * 1000)


def _rest_scanner_purpose(purpose: str) -> bool:
    return str(purpose or '').strip() in {'scanner', 'market_data', 'public'}


def _rest_core_purpose(purpose: str) -> bool:
    return str(purpose or '').strip() in {'execution', 'emergency', 'order_status', 'signed', 'account', 'account_reconcile'}


def _set_binance_rest_circuit_state(state: str, *, reason: str = '', open_for_seconds: float = 0.0) -> None:
    now_ms = _rest_now_ms()
    open_until_ms = int(now_ms + max(0.0, float(open_for_seconds or 0.0)) * 1000) if open_for_seconds else 0
    def mutate(guard: Dict[str, Any]) -> None:
        guard['rest_circuit_state'] = str(state or 'CLOSED').upper()
        guard['rest_circuit_reason'] = reason
        guard['circuit_open_until_ms'] = open_until_ms
        guard['next_rest_probe_at_ms'] = open_until_ms
        guard['half_open_probe_used'] = False
        guard['recovering_until_ms'] = now_ms + 180_000 if guard['rest_circuit_state'] == 'RECOVERING' else 0
    _with_binance_rest_guard_state(mutate)


def _advance_rest_guard_state(guard: Dict[str, Any], now_ms: int) -> None:
    state = str(guard.get('rest_circuit_state') or 'CLOSED').upper()
    open_until_ms = int(guard.get('circuit_open_until_ms') or 0)
    recovering_until_ms = int(guard.get('recovering_until_ms') or 0)
    used_weight = int(guard.get('rest_used_weight_1m') or 0)
    if state == 'OPEN' and open_until_ms and now_ms >= open_until_ms:
        guard['rest_circuit_state'] = 'HALF_OPEN'
        guard['half_open_probe_used'] = False
    elif state == 'RECOVERING' and recovering_until_ms and now_ms >= recovering_until_ms and used_weight < REST_WEIGHT_SLOWDOWN_THRESHOLD:
        guard['rest_circuit_state'] = 'CLOSED'
        guard['rest_circuit_reason'] = ''
        guard['recovering_until_ms'] = 0


def _binance_rest_guard_snapshot() -> Dict[str, Any]:
    now_ms = _rest_now_ms()
    def mutate(guard: Dict[str, Any]) -> Dict[str, Any]:
        _advance_rest_guard_state(guard, now_ms)
        next_probe_at_ms = int(guard.get('next_rest_probe_at_ms') or guard.get('circuit_open_until_ms') or 0)
        return {
            'state': str(guard.get('rest_circuit_state') or 'CLOSED'),
            'reason': str(guard.get('rest_circuit_reason') or ''),
            'rest_circuit_state': str(guard.get('rest_circuit_state') or 'CLOSED'),
            'rest_circuit_reason': str(guard.get('rest_circuit_reason') or ''),
            'rest_used_weight_1m': int(guard.get('rest_used_weight_1m') or 0),
            'circuit_open_until_ms': int(guard.get('circuit_open_until_ms') or 0),
            'next_rest_probe_at_ms': next_probe_at_ms,
            'next_retry_after_seconds': max(0, int((next_probe_at_ms - now_ms) / 1000)) if next_probe_at_ms else 0,
            'request_count_1s': int(guard.get('request_count_1s') or 0),
            'window_started_at_ms': int(guard.get('window_started_at_ms') or 0),
            'half_open_probe_used': bool(guard.get('half_open_probe_used')),
            'recovering_until_ms': int(guard.get('recovering_until_ms') or 0),
            **{f'{k}_rest_weight_1m': int(v) for k, v in _BINANCE_REST_WEIGHT_BY_PURPOSE.items()},
        }
    return _with_binance_rest_guard_state(mutate)


def _raise_rest_guard_blocked(reason: str, until_ms: int = 0) -> None:
    wait = max(0, int(((until_ms or _rest_now_ms()) - _rest_now_ms()) / 1000)) if until_ms else 0
    suffix = f' next_retry_after_seconds={wait}' if wait else ''
    raise BinanceAPIError(f'{reason}{suffix}')


def _binance_rest_guard_before_request(max_requests_per_second: int = 2, *, purpose: str = 'scanner') -> None:
    purpose = str(purpose or 'scanner')
    scanner_purpose = _rest_scanner_purpose(purpose)
    core_purpose = _rest_core_purpose(purpose)
    while True:
        sleep_for = 0.0
        def mutate(guard: Dict[str, Any]) -> Optional[float]:
            now_ms = _rest_now_ms()
            _advance_rest_guard_state(guard, now_ms)
            state = str(guard.get('rest_circuit_state') or 'CLOSED').upper()
            open_until_ms = int(guard.get('circuit_open_until_ms') or 0)
            next_probe_at_ms = int(guard.get('next_rest_probe_at_ms') or open_until_ms or 0)
            used_weight = int(guard.get('rest_used_weight_1m') or 0)
            if state == 'OPEN' and open_until_ms and now_ms < open_until_ms:
                if scanner_purpose or used_weight >= REST_WEIGHT_CORE_ONLY_THRESHOLD:
                    _raise_rest_guard_blocked('blocked_reason=binance_rest_circuit_open scanner_degraded_wait=true', open_until_ms)
            if state == 'DEGRADED' and scanner_purpose and next_probe_at_ms and now_ms < next_probe_at_ms:
                _raise_rest_guard_blocked('blocked_reason=binance_rest_circuit_open scanner_degraded_wait=true', next_probe_at_ms)
            if state == 'HALF_OPEN':
                if scanner_purpose and bool(guard.get('half_open_probe_used')):
                    _raise_rest_guard_blocked('blocked_reason=binance_rest_circuit_open scanner_degraded_wait=true', next_probe_at_ms or open_until_ms)
                if scanner_purpose:
                    guard['half_open_probe_used'] = True
            if scanner_purpose and used_weight >= REST_WEIGHT_SCANNER_BLOCK_THRESHOLD:
                guard['rest_circuit_state'] = 'DEGRADED'
                guard['rest_circuit_reason'] = f'rest_used_weight_1m:{used_weight}'
                guard['next_rest_probe_at_ms'] = max(next_probe_at_ms, now_ms + 120_000)
                _raise_rest_guard_blocked('blocked_reason=binance_rest_circuit_open scanner_degraded_wait=true', int(guard['next_rest_probe_at_ms']))
            if used_weight >= REST_WEIGHT_CORE_ONLY_THRESHOLD and not core_purpose:
                guard['rest_circuit_state'] = 'DEGRADED'
                guard['rest_circuit_reason'] = f'rest_core_only_used_weight_1m:{used_weight}'
                guard['next_rest_probe_at_ms'] = max(next_probe_at_ms, now_ms + 120_000)
                _raise_rest_guard_blocked('blocked_reason=binance_rest_circuit_open scanner_degraded_wait=true', int(guard['next_rest_probe_at_ms']))
            rate_now_ms = int(time.monotonic() * 1000)
            window_started_at_ms = int(guard.get('window_started_at_ms') or 0)
            if rate_now_ms < window_started_at_ms or rate_now_ms - window_started_at_ms >= 1000:
                guard['window_started_at_ms'] = rate_now_ms
                guard['request_count_1s'] = 0
                window_started_at_ms = rate_now_ms
            effective_rps = max_requests_per_second
            if used_weight >= REST_WEIGHT_SLOWDOWN_THRESHOLD or state in {'HALF_OPEN', 'RECOVERING'}:
                effective_rps = 1
            if int(guard.get('request_count_1s') or 0) < effective_rps:
                guard['request_count_1s'] = int(guard.get('request_count_1s') or 0) + 1
                return None
            return max(0.01, (1000 - (rate_now_ms - window_started_at_ms)) / 1000.0)
        sleep_for = _with_binance_rest_guard_state(mutate) or 0.0
        if sleep_for <= 0:
            return
        time.sleep(sleep_for)


def _record_rest_weight_metric(purpose: str, used_weight_1m: int) -> None:
    bucket = {
        'scanner': 'scanner',
        'market_data': 'scanner',
        'public': 'scanner',
        'metadata': 'metadata',
        'execution': 'execution',
        'order_status': 'reconcile',
        'account_reconcile': 'reconcile',
        'history_backfill': 'watchdog',
        'low_frequency_market_data': 'watchdog',
    }.get(str(purpose or ''), str(purpose or 'unknown'))
    if used_weight_1m:
        _BINANCE_REST_WEIGHT_BY_PURPOSE[bucket] = int(used_weight_1m)


def _log_binance_rest_request(*, purpose: str, path: str, status_code: int, used_weight_1m: int, circuit_state: str, request_latency_ms: int) -> None:
    payload = {
        'event': 'binance_rest_request',
        'purpose': purpose,
        'path': path,
        'status_code': int(status_code or 0),
        'used_weight_1m': int(used_weight_1m or 0),
        'circuit_state': circuit_state,
        'request_latency_ms': int(request_latency_ms or 0),
        'process_id': os.getpid(),
        'thread_name': threading.current_thread().name,
    }
    print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)


def _binance_rest_guard_after_response(response: Any, *, purpose: str = '', path: str = '', request_latency_ms: int = 0) -> None:
    used_weight_1m = _extract_response_used_weight_1m(response)
    status_code = int(getattr(response, 'status_code', 0) or 0)
    now_ms = _rest_now_ms()
    def mutate(guard: Dict[str, Any]) -> None:
        if used_weight_1m:
            guard['rest_used_weight_1m'] = used_weight_1m
        state = str(guard.get('rest_circuit_state') or 'CLOSED').upper()
        if status_code == 418:
            guard['rest_circuit_state'] = 'OPEN'
            guard['rest_circuit_reason'] = 'http_418_ip_ban'
            guard['circuit_open_until_ms'] = now_ms + int(REST_418_COOLDOWN_SECONDS * 1000)
            guard['next_rest_probe_at_ms'] = guard['circuit_open_until_ms']
            guard['half_open_probe_used'] = False
            return
        if status_code == 429:
            guard['rest_circuit_state'] = 'OPEN'
            guard['rest_circuit_reason'] = 'http_429_rate_limit'
            guard['circuit_open_until_ms'] = now_ms + int(REST_429_COOLDOWN_SECONDS * 1000)
            guard['next_rest_probe_at_ms'] = guard['circuit_open_until_ms']
            guard['half_open_probe_used'] = False
            return
        if used_weight_1m >= REST_WEIGHT_CORE_ONLY_THRESHOLD:
            guard['rest_circuit_state'] = 'DEGRADED'
            guard['rest_circuit_reason'] = f'rest_core_only_used_weight_1m:{used_weight_1m}'
            guard['next_rest_probe_at_ms'] = now_ms + 120_000
        elif used_weight_1m >= REST_WEIGHT_SCANNER_BLOCK_THRESHOLD:
            guard['rest_circuit_state'] = 'DEGRADED'
            guard['rest_circuit_reason'] = f'rest_scanner_block_used_weight_1m:{used_weight_1m}'
            guard['next_rest_probe_at_ms'] = now_ms + 120_000
        elif used_weight_1m >= REST_WEIGHT_SLOWDOWN_THRESHOLD:
            guard['rest_circuit_state'] = 'RECOVERING' if state in {'OPEN', 'HALF_OPEN'} else 'DEGRADED'
            guard['rest_circuit_reason'] = f'rest_slowdown_used_weight_1m:{used_weight_1m}'
            guard['next_rest_probe_at_ms'] = now_ms + 60_000
        elif state in {'HALF_OPEN', 'RECOVERING', 'DEGRADED'}:
            guard['rest_circuit_state'] = 'RECOVERING'
            guard['rest_circuit_reason'] = 'rest_weight_recovered'
            if state != 'RECOVERING' or int(guard.get('recovering_until_ms') or 0) <= now_ms:
                guard['recovering_until_ms'] = now_ms + 180_000
    _with_binance_rest_guard_state(mutate)
    _record_rest_weight_metric(purpose, used_weight_1m)
    _log_binance_rest_request(purpose=purpose, path=path, status_code=status_code, used_weight_1m=used_weight_1m, circuit_state=_binance_rest_guard_snapshot().get('rest_circuit_state', 'CLOSED'), request_latency_ms=request_latency_ms)


def _binance_rest_guard_after_error(error: Any, fallback_cooldown_seconds: float = 900.0) -> None:
    message = str(error)
    lowered = message.lower()
    retry_after_ms = _extract_retry_after_ms_from_message(message)
    if 'binance api error 418' in lowered or 'ip banned' in lowered:
        def mutate(guard: Dict[str, Any]) -> None:
            guard['rest_circuit_state'] = 'OPEN'
            guard['rest_circuit_reason'] = 'binance_418_ip_ban'
            guard['circuit_open_until_ms'] = retry_after_ms or int((time.time() + REST_418_COOLDOWN_SECONDS) * 1000)
            guard['next_rest_probe_at_ms'] = guard['circuit_open_until_ms']
            guard['half_open_probe_used'] = False
        _with_binance_rest_guard_state(mutate)
    elif 'binance api error 429' in lowered or 'too many requests' in lowered:
        def mutate(guard: Dict[str, Any]) -> None:
            guard['rest_circuit_state'] = 'OPEN'
            guard['rest_circuit_reason'] = 'binance_429_rate_limit'
            guard['circuit_open_until_ms'] = retry_after_ms or int((time.time() + REST_429_COOLDOWN_SECONDS) * 1000)
            guard['next_rest_probe_at_ms'] = guard['circuit_open_until_ms']
            guard['half_open_probe_used'] = False
        _with_binance_rest_guard_state(mutate)

class BinanceFuturesClient:
    RECV_WINDOW_MS = 10_000

    def __init__(
        self,
        base_url: str,
        api_key: str = '',
        api_secret: str = '',
        session: Optional[requests.Session] = None,
        max_get_retries: int = 3,
        get_retry_sleep_sec: float = 0.5,
        data_base_url: str = '',
        scanner_proxy_urls: Optional[Sequence[str]] = None,
        allow_heavy_history_rest: bool = False,
    ):
        self.base_url = base_url.rstrip('/')
        self.data_base_url = (data_base_url or os.getenv('BINANCE_FUTURES_DATA_BASE_URL', 'https://fapi.binance.com')).rstrip('/')
        self.api_key = api_key or ''
        self.api_secret = api_secret or ''
        self.session = session or requests.Session()
        self.max_get_retries = max(1, int(max_get_retries or 1))
        self.get_retry_sleep_sec = max(0.0, float(get_retry_sleep_sec or 0.0))
        self.scanner_proxy_urls = parse_scanner_proxy_urls(scanner_proxy_urls)
        self.allow_heavy_history_rest = bool(allow_heavy_history_rest)
        self._server_time_offset_ms: Optional[int] = None
        if self.api_key:
            self.session.headers.setdefault('X-MBX-APIKEY', self.api_key)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15, *, purpose: str = 'market_data'):
        last_exc: Optional[BaseException] = None
        base_url = self.data_base_url if str(path or '').startswith('/futures/data/') else self.base_url
        url = f'{base_url}{path}'
        response = None
        for attempt in range(self.max_get_retries):
            try:
                proxy_url = choose_scanner_proxy_url(self.scanner_proxy_urls) if purpose in {'scanner', 'market_data', 'low_frequency_market_data'} else ''
                request_kwargs = {'params': params or {}, 'timeout': timeout}
                if proxy_url:
                    request_kwargs['proxies'] = {'http': proxy_url, 'https': proxy_url}
                request_started = time.monotonic()
                _binance_rest_guard_before_request(purpose=purpose)
                response = self.session.get(url, **request_kwargs)
                _binance_rest_guard_after_response(response, purpose=purpose, path=path, request_latency_ms=int((time.monotonic() - request_started) * 1000))
                self._raise_for_status(response)
                return response.json()
            except BinanceAPIError as exc:
                last_exc = exc
                _binance_rest_guard_after_error(exc)
                if is_binance_ip_ban_error(exc) or response is None or not self._is_retryable_public_get_error(response) or attempt + 1 >= self.max_get_retries:
                    break
                time.sleep(self.get_retry_sleep_sec * (2 ** attempt) + random.uniform(0.0, self.get_retry_sleep_sec))
            except requests.RequestException as exc:
                last_exc = exc
                _binance_rest_guard_after_error(exc)
                if attempt + 1 >= self.max_get_retries:
                    break
                time.sleep(self.get_retry_sleep_sec * (2 ** attempt) + random.uniform(0.0, self.get_retry_sleep_sec))
        if last_exc is not None:
            raise last_exc
        raise BinanceAPIError(f'GET request failed without response: {path}')

    def signed_get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15):
        return self._signed_request('GET', path, params or {}, timeout=timeout)

    def signed_post(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15):
        return self._signed_request('POST', path, params or {}, timeout=timeout)

    def signed_put(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15):
        return self._signed_request('PUT', path, params or {}, timeout=timeout)

    def signed_delete(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15):
        return self._signed_request('DELETE', path, params or {}, timeout=timeout)

    def _signed_request(self, method: str, path: str, params: Dict[str, Any], timeout: int = 15):
        if not self.api_secret:
            raise BinanceAPIError('api_secret is required for signed requests')
        try:
            return self._signed_request_once(method, path, params, timeout=timeout)
        except BinanceAPIError as exc:
            if not self._is_recvwindow_error(exc):
                raise
            self.sync_server_time(force=True)
            return self._signed_request_once(method, path, params, timeout=timeout)

    def _signed_request_once(self, method: str, path: str, params: Dict[str, Any], timeout: int = 15):
        payload = dict(params)
        payload.setdefault('recvWindow', self.RECV_WINDOW_MS)
        payload.setdefault('timestamp', self._timestamp_ms())
        query = urlencode(payload, doseq=True)
        signature = hmac.new(self.api_secret.encode('utf-8'), query.encode('utf-8'), hashlib.sha256).hexdigest()
        payload['signature'] = signature
        url = f'{self.base_url}{path}'
        signed_purpose = self._signed_request_purpose(method, path)
        request_started = time.monotonic()
        if signed_purpose == 'history_backfill' and not self.allow_heavy_history_rest:
            raise BinanceAPIError('history_backfill_rest_disabled allow_heavy_history_rest=false')
        _binance_rest_guard_before_request(purpose=signed_purpose)
        if method == 'GET':
            response = self.session.get(url, params=payload, timeout=timeout)
        elif method == 'POST':
            response = self.session.post(url, params=payload, timeout=timeout)
        elif method == 'PUT':
            response = self.session.put(url, params=payload, timeout=timeout)
        elif method == 'DELETE':
            response = self.session.delete(url, params=payload, timeout=timeout)
        else:
            raise ValueError(f'unsupported signed method: {method}')
        _binance_rest_guard_after_response(response, purpose=signed_purpose, path=path, request_latency_ms=int((time.monotonic() - request_started) * 1000))
        try:
            self._raise_for_status(response)
        except BinanceAPIError as exc:
            _binance_rest_guard_after_error(exc)
            raise
        return response.json()

    def _timestamp_ms(self) -> int:
        if self._server_time_offset_ms is None:
            self.sync_server_time()
        return int(time.time() * 1000) + int(self._server_time_offset_ms or 0)

    def sync_server_time(self, force: bool = False) -> int:
        if self._server_time_offset_ms is not None and not force:
            return int(self._server_time_offset_ms or 0)
        try:
            local_before_ms = int(time.time() * 1000)
            data = self.get('/fapi/v1/time', timeout=5, purpose='metadata')
            local_after_ms = int(time.time() * 1000)
            server_time_ms = int(data.get('serverTime'))
            local_midpoint_ms = int((local_before_ms + local_after_ms) / 2)
            self._server_time_offset_ms = server_time_ms - local_midpoint_ms
        except Exception:
            if self._server_time_offset_ms is None:
                self._server_time_offset_ms = 0
        return int(self._server_time_offset_ms or 0)

    @staticmethod
    def _signed_request_purpose(method: str, path: str) -> str:
        if method in {'POST', 'PUT', 'DELETE'}:
            return 'execution'
        normalized = str(path or '')
        if normalized.endswith('/order') or normalized.endswith('/openOrder') or normalized.endswith('/openOrders'):
            return 'order_status'
        if normalized.endswith('/account') or normalized.endswith('/balance') or normalized.endswith('/positionRisk'):
            return 'account_reconcile'
        if normalized.endswith('/allOrders') or normalized.endswith('/userTrades') or normalized.endswith('/income'):
            return 'history_backfill'
        return 'order_status'

    @staticmethod
    def _is_recvwindow_error(exc: BinanceAPIError) -> bool:
        message = str(exc)
        return 'recvWindow' in message or "code': -1021" in message or 'code": -1021' in message

    @staticmethod
    def _is_retryable_public_get_error(response) -> bool:
        if int(getattr(response, 'status_code', 0) or 0) == 429:
            return True
        try:
            payload = response.json()
        except Exception:
            payload = {}
        if isinstance(payload, dict) and str(payload.get('code')) == '-1003':
            return True
        return 'Too many requests' in str(payload)

    @staticmethod
    def _raise_for_status(response):
        if response.status_code >= 400:
            try:
                payload = response.json()
            except Exception:
                text = str(getattr(response, 'text', '') or '')
                content_type = ''
                try:
                    content_type = str(getattr(response, 'headers', {}) or {}).lower()
                except Exception:
                    content_type = ''
                looks_html = '<html' in text.lower() or '<!doctype html' in text.lower() or 'text/html' in content_type
                payload = {
                    'code': 'non_json_response',
                    'content_type': content_type[:120],
                    'body_preview': re.sub(r'<[^>]+>', ' ', text).strip()[:200] if looks_html else text[:200],
                }
            raise BinanceAPIError(f'Binance API error {response.status_code}: {payload}')


def is_binance_ip_ban_error(error: Any) -> bool:
    message = str(error)
    lowered = message.lower()
    return 'binance api error 418' in lowered or 'ip banned' in lowered or "code': -1003" in message or 'code": -1003' in message


def extract_binance_ip_ban_until_ms(error: Any) -> Optional[int]:
    return _extract_retry_after_ms_from_message(error)


def current_time_ms() -> int:
    return int(time.time() * 1000)


def build_scanner_rest_circuit_payload(*, reason: str, retry_after_ms: Optional[int], error: str = '', fallback_cooldown_seconds: float = 900.0) -> Dict[str, Any]:
    opened_at_ms = current_time_ms()
    payload: Dict[str, Any] = {
        'state': 'open',
        'reason': reason,
        'opened_at_ms': opened_at_ms,
    }
    if retry_after_ms is None and fallback_cooldown_seconds > 0:
        retry_after_ms = opened_at_ms + int(float(fallback_cooldown_seconds) * 1000)
        payload['fallback_cooldown_seconds'] = float(fallback_cooldown_seconds)
    if retry_after_ms is not None:
        payload['retry_after_ms'] = int(retry_after_ms)
    if error:
        payload['error'] = error
    return payload


def load_open_scanner_rest_circuit(store: Any) -> Optional[Dict[str, Any]]:
    try:
        payload = store.load_json('scanner_rest_circuit_breaker', None)
    except Exception:
        return None
    if not isinstance(payload, dict) or str(payload.get('state') or '').lower() != 'open':
        return None
    retry_after_ms = _to_float(payload.get('retry_after_ms'), default=0.0)
    if retry_after_ms > 0 and current_time_ms() < int(retry_after_ms):
        return payload
    closed_payload = dict(payload)
    closed_payload.update({
        'state': 'closed',
        'previous_state': 'open',
        'closed_reason': 'retry_after_elapsed',
        'closed_at_ms': current_time_ms(),
    })
    try:
        store.save_json('scanner_rest_circuit_breaker', closed_payload)
    except Exception:
        pass
    return None


class OKXClient:
    def __init__(
        self,
        base_url: str = 'https://www.okx.com',
        api_key: str = '',
        api_secret: str = '',
        passphrase: str = '',
        simulated_trading: bool = True,
        session: Optional[requests.Session] = None,
    ):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key or ''
        self.api_secret = api_secret or ''
        self.passphrase = passphrase or ''
        self.simulated_trading = bool(simulated_trading)
        self.session = session or requests.Session()

    def public_get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15):
        response = self.session.get(f'{self.base_url}{path}', params=params or {}, timeout=timeout)
        self._raise_for_status(response)
        return response.json()

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15, *, purpose: str = 'market_data'):
        params = params or {}
        query = ''
        if params:
            query = '?' + requests.models.RequestEncodingMixin._encode_params(params)
        request_path = f'{path}{query}'
        headers = self._headers('GET', request_path, '')
        response = self.session.get(f'{self.base_url}{path}', params=params, headers=headers, timeout=timeout)
        self._raise_for_status(response)
        data = response.json()
        if str(data.get('code', '0')) != '0':
            raise OKXAPIError(f'OKX API error {data.get("code")}: {data}')
        return data

    def post(self, path: str, payload: Optional[Dict[str, Any]] = None, timeout: int = 15):
        body = json.dumps(payload or {}, separators=(',', ':'), ensure_ascii=False)
        headers = self._headers('POST', path, body)
        response = self.session.post(f'{self.base_url}{path}', data=body, headers=headers, timeout=timeout)
        self._raise_for_status(response)
        data = response.json()
        if str(data.get('code', '0')) != '0':
            raise OKXAPIError(f'OKX API error {data.get("code")}: {data}')
        return data

    def _headers(self, method: str, path: str, body: str = '') -> Dict[str, str]:
        if not self.api_key or not self.api_secret or not self.passphrase:
            raise OKXAPIError('OKX api key, secret and passphrase are required')
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        prehash = f'{timestamp}{method.upper()}{path}{body}'
        digest = hmac.new(self.api_secret.encode('utf-8'), prehash.encode('utf-8'), hashlib.sha256).digest()
        headers = {
            'Content-Type': 'application/json',
            'OK-ACCESS-KEY': self.api_key,
            'OK-ACCESS-SIGN': base64.b64encode(digest).decode('ascii'),
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.passphrase,
        }
        if self.simulated_trading:
            headers['x-simulated-trading'] = '1'
        return headers

    @staticmethod
    def _raise_for_status(response):
        if response.status_code >= 400:
            try:
                payload = response.json()
            except Exception:
                payload = response.text
            raise OKXAPIError(f'OKX API error {response.status_code}: {payload}')


def normalize_okx_swap_inst_id(symbol: Any) -> str:
    text = str(symbol or '').strip().upper().replace('_', '-')
    if not text:
        return ''
    if '-' in text:
        return text if text.endswith('-SWAP') else f'{text}-SWAP'
    if text.endswith('USDT'):
        return f'{text[:-4]}-USDT-SWAP'
    if text.endswith('USDC'):
        return f'{text[:-4]}-USDC-SWAP'
    return text


def _round_down_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor((value + 1e-12) / step) * step


def _format_decimal(value: float) -> str:
    if not math.isfinite(value):
        return '0'
    text = f'{value:.12f}'.rstrip('0').rstrip('.')
    return text or '0'


def fetch_okx_swap_instrument(client: OKXClient, inst_id: str) -> Dict[str, Any]:
    payload = client.public_get('/api/v5/public/instruments', {'instType': 'SWAP', 'instId': inst_id})
    rows = payload.get('data', []) if isinstance(payload, dict) else []
    if not rows:
        raise OKXAPIError(f'OKX instrument not found: {inst_id}')
    return rows[0]


def fetch_okx_swap_inst_ids(client: OKXClient, ttl_seconds: float = 300.0) -> Set[str]:
    now_ts = time.time()
    cache_base_url = str(_OKX_SWAP_INST_ID_CACHE.get('base_url') or '')
    cache_age = now_ts - float(_OKX_SWAP_INST_ID_CACHE.get('fetched_at') or 0.0)
    cached_ids = _OKX_SWAP_INST_ID_CACHE.get('inst_ids')
    if cache_base_url == client.base_url and isinstance(cached_ids, set) and cached_ids and cache_age < ttl_seconds:
        return set(cached_ids)
    payload = client.public_get('/api/v5/public/instruments', {'instType': 'SWAP'})
    rows = payload.get('data', []) if isinstance(payload, dict) else []
    inst_ids = {
        str(row.get('instId') or '').upper()
        for row in rows
        if isinstance(row, dict) and str(row.get('instId') or '').upper().endswith('-USDT-SWAP')
    }
    _OKX_SWAP_INST_ID_CACHE.update({
        'base_url': client.base_url,
        'fetched_at': now_ts,
        'inst_ids': set(inst_ids),
    })
    return inst_ids


def load_okx_sim_skip_symbols(store: RuntimeStateStore) -> Set[str]:
    payload = store.load_json('okx-sim-skip-symbols', {})
    if isinstance(payload, list):
        return {normalize_symbol(item) for item in payload if normalize_symbol(item)}
    if isinstance(payload, dict):
        return {normalize_symbol(item) for item in payload.get('symbols', []) if normalize_symbol(item)}
    return set()


def save_okx_sim_skip_symbols(store: RuntimeStateStore, symbols: Set[str]) -> None:
    store.save_json('okx-sim-skip-symbols', {'symbols': sorted(normalize_symbol(item) for item in symbols if normalize_symbol(item))})


def is_non_retryable_okx_symbol_error(error: Any) -> bool:
    text = str(error or '')
    return any(code in text for code in ('51001', '51087', '51155'))


def is_okx_reduce_position_missing_error(error: Any) -> bool:
    return '51169' in str(error or '')


def okx_account_mode_label(acct_lv: Any) -> str:
    return {
        '1': 'Spot mode',
        '2': 'Futures mode',
        '3': 'Multi-currency margin mode',
        '4': 'Portfolio margin mode',
    }.get(str(acct_lv or ''), str(acct_lv or '') or 'unknown')


def fetch_okx_account_config(client: OKXClient) -> Dict[str, Any]:
    payload = client.get('/api/v5/account/config')
    rows = payload.get('data', []) if isinstance(payload, dict) else []
    return rows[0] if rows and isinstance(rows[0], dict) else {}


def fetch_okx_account_balance(client: OKXClient) -> Dict[str, Any]:
    payload = client.get('/api/v5/account/balance')
    rows = payload.get('data', []) if isinstance(payload, dict) else []
    return rows[0] if rows and isinstance(rows[0], dict) else {}


def is_binance_simulated_trading(args: argparse.Namespace) -> bool:
    return bool(getattr(args, 'binance_simulated_trading', False))


def execution_exchange_label(args: argparse.Namespace) -> str:
    if is_binance_simulated_trading(args):
        return 'BINANCE_SIMULATED'
    return 'BINANCE'


def resolve_binance_api_credentials(args: argparse.Namespace) -> Tuple[str, str]:
    if is_binance_simulated_trading(args):
        api_key = os.getenv('BINANCE_FUTURES_TESTNET_API_KEY') or os.getenv('BINANCE_FUTURES_API_KEY', '')
        api_secret = os.getenv('BINANCE_FUTURES_TESTNET_API_SECRET') or os.getenv('BINANCE_FUTURES_API_SECRET', '')
        return api_key, api_secret
    return os.getenv('BINANCE_FUTURES_API_KEY', ''), os.getenv('BINANCE_FUTURES_API_SECRET', '')


POSITION_SIDE_LONG = 'LONG'
POSITION_SIDE_SHORT = 'SHORT'
TRADE_SIDE_LONG = 'long'
TRADE_SIDE_SHORT = 'short'


def normalize_trade_side(side: Any, default: str = TRADE_SIDE_LONG) -> str:
    normalized = str(side or '').strip().lower()
    return TRADE_SIDE_SHORT if normalized == TRADE_SIDE_SHORT else default


def resolve_allowed_trade_sides(raw: Any) -> Tuple[str, ...]:
    tokens = [str(part or '').strip().lower() for part in str(raw or '').split(',')]
    allowed: List[str] = []
    for token in tokens:
        if token == TRADE_SIDE_LONG and TRADE_SIDE_LONG not in allowed:
            allowed.append(TRADE_SIDE_LONG)
        elif token == TRADE_SIDE_SHORT and TRADE_SIDE_SHORT not in allowed:
            allowed.append(TRADE_SIDE_SHORT)
    if not allowed:
        return (TRADE_SIDE_LONG, TRADE_SIDE_SHORT)
    return tuple(allowed)


def position_side_to_trade_side(side: Any, default: str = TRADE_SIDE_LONG) -> str:
    normalized = normalize_position_side(side, POSITION_SIDE_SHORT if str(default).lower() == TRADE_SIDE_SHORT else POSITION_SIDE_LONG)
    return TRADE_SIDE_SHORT if normalized == POSITION_SIDE_SHORT else TRADE_SIDE_LONG


def trade_side_to_position_side(side: Any, default: str = POSITION_SIDE_LONG) -> str:
    normalized = normalize_trade_side(side, TRADE_SIDE_SHORT if str(default).upper() == POSITION_SIDE_SHORT else TRADE_SIDE_LONG)
    return POSITION_SIDE_SHORT if normalized == TRADE_SIDE_SHORT else POSITION_SIDE_LONG


def normalize_position_side(side: Any, default: str = POSITION_SIDE_LONG) -> str:
    normalized = str(side or '').strip().upper()
    fallback = str(default or POSITION_SIDE_LONG).strip().upper()
    if normalized in {POSITION_SIDE_SHORT, TRADE_SIDE_SHORT.upper(), 'SELL'}:
        return POSITION_SIDE_SHORT
    if normalized in {POSITION_SIDE_LONG, TRADE_SIDE_LONG.upper(), 'BUY'}:
        return POSITION_SIDE_LONG
    return POSITION_SIDE_SHORT if fallback in {POSITION_SIDE_SHORT, TRADE_SIDE_SHORT.upper(), 'SELL'} else POSITION_SIDE_LONG


def build_position_key(symbol: str, side: Any = POSITION_SIDE_LONG) -> str:
    return f"{str(symbol or '').upper()}:{normalize_position_side(side)}"


def split_position_key(position_key: Any) -> Tuple[str, str]:
    text = str(position_key or '').strip().upper()
    if ':' in text:
        symbol, side = text.split(':', 1)
        return symbol, normalize_position_side(side)
    return text, POSITION_SIDE_LONG


def is_legacy_position_key(position_key: Any) -> bool:
    text = str(position_key or '').strip().upper()
    return bool(text) and ':' not in text


def position_matches_symbol_side(position: Any, symbol: str, side: Any = POSITION_SIDE_LONG) -> bool:
    if not isinstance(position, dict):
        return False
    return str(position.get('symbol') or '').upper() == str(symbol or '').upper() and normalize_position_side(position.get('side')) == normalize_position_side(side)


def get_position_by_symbol_side(positions_state: Any, symbol: str, side: Any = POSITION_SIDE_LONG) -> Tuple[str, Dict[str, Any]]:
    if not isinstance(positions_state, dict):
        return build_position_key(symbol, side), {}
    desired_key = build_position_key(symbol, side)
    position = positions_state.get(desired_key)
    if isinstance(position, dict):
        normalized = dict(position)
        normalized.setdefault('symbol', str(symbol or '').upper())
        normalized['side'] = normalize_position_side(normalized.get('side'), normalize_position_side(side))
        normalized.setdefault('position_key', desired_key)
        return desired_key, normalized
    legacy = positions_state.get(str(symbol or '').upper())
    if isinstance(legacy, dict) and position_matches_symbol_side(legacy, symbol, side):
        normalized = dict(legacy)
        normalized.setdefault('symbol', str(symbol or '').upper())
        normalized['side'] = normalize_position_side(normalized.get('side'), normalize_position_side(side))
        normalized.setdefault('position_key', desired_key)
        return desired_key, normalized
    for key, value in positions_state.items():
        if position_matches_symbol_side(value, symbol, side):
            normalized = dict(value)
            normalized.setdefault('symbol', str(symbol or '').upper())
            normalized['side'] = normalize_position_side(normalized.get('side'), normalize_position_side(side))
            normalized.setdefault('position_key', build_position_key(normalized['symbol'], normalized['side']))
            return str(key), normalized
    return desired_key, {}


def upsert_position_record(positions_state: Any, position: Dict[str, Any], key: Optional[str] = None) -> Tuple[Dict[str, Any], str]:
    if not isinstance(positions_state, dict):
        positions_state = {}
    normalized = dict(position or {})
    symbol = str(normalized.get('symbol') or '').upper()
    position_side = normalize_position_side(normalized.get('position_side') or normalized.get('side'))
    trade_side = normalize_trade_side(normalized.get('side'), position_side_to_trade_side(position_side))
    position_key = build_position_key(symbol, position_side)
    normalized['symbol'] = symbol
    normalized['side'] = trade_side
    normalized['position_side'] = position_side
    normalized['position_key'] = position_key
    quantity = _to_float(normalized.get('quantity'), default=0.0)
    if 'remaining_quantity' not in normalized:
        normalized['remaining_quantity'] = quantity
    normalized.setdefault('current_stop_price', normalized.get('stop_price'))
    normalized.setdefault('moved_to_breakeven', False)
    normalized.setdefault('tp1_hit', False)
    normalized.setdefault('tp2_hit', False)
    normalized.setdefault('highest_price_seen', None)
    normalized.setdefault('lowest_price_seen', None)
    normalized.setdefault('opened_at', None)
    normalized.setdefault('first_1r_at', None)
    normalized.setdefault('realized_r', 0.0)
    normalized.setdefault('mfe_r', 0.0)
    normalized.setdefault('mae_r', 0.0)
    normalized.setdefault('time_to_1r', None)
    normalized.setdefault('time_to_1r_minutes', None)
    normalized.setdefault('time_in_trade_minutes', None)
    normalized.setdefault('selected_score', None)
    normalized.setdefault('selected_state', '')
    normalized.setdefault('selected_alert_tier', '')
    normalized.setdefault('trigger_class', '')
    normalized.setdefault('score_decile', '')
    normalized.setdefault('market_regime_label', '')
    normalized.setdefault('market_regime_multiplier', 0.0)
    normalized.setdefault('monitor_mode', 'trade_management')

    explicit_key = str(key or '').strip().upper()
    key_candidates: List[str] = []
    for candidate in [explicit_key, position_key, symbol]:
        candidate_text = str(candidate or '').strip().upper()
        if candidate_text and candidate_text not in key_candidates:
            key_candidates.append(candidate_text)
    for existing_key, existing_value in list(positions_state.items()):
        existing_key_text = str(existing_key or '').strip().upper()
        if existing_key_text and existing_key_text not in key_candidates and position_matches_symbol_side(existing_value, symbol, position_side):
            key_candidates.append(existing_key_text)

    for candidate_key in list(key_candidates):
        existing = positions_state.get(candidate_key)
        if isinstance(existing, dict) and position_matches_symbol_side(existing, symbol, position_side):
            positions_state.pop(candidate_key, None)

    positions_state[position_key] = normalized
    return positions_state, position_key


def get_position_storage_aliases(position_key: str, symbol: Any, side: Any, prefer_legacy: bool = False, include_legacy_alias: bool = True) -> List[str]:
    aliases: List[str] = []
    canonical = str(position_key or '').upper()
    symbol_text = str(symbol or '').upper()
    normalized_side = normalize_position_side(side)
    if prefer_legacy and symbol_text and normalized_side == POSITION_SIDE_LONG and include_legacy_alias:
        aliases.append(symbol_text)
    if canonical:
        aliases.append(canonical)
    if symbol_text and normalized_side == POSITION_SIDE_LONG and include_legacy_alias:
        aliases.append(symbol_text)
    unique_aliases: List[str] = []
    seen = set()
    for alias in aliases:
        if alias and alias not in seen:
            unique_aliases.append(alias)
            seen.add(alias)
    return unique_aliases


def materialize_positions_state(positions_state: Dict[str, Any], original_keys: Optional[Dict[str, str]] = None, include_legacy_alias: bool = False) -> Dict[str, Any]:
    materialized: Dict[str, Any] = {}
    key_hints = dict(original_keys or {})
    for position_key, tracked in list((positions_state or {}).items()):
        if not isinstance(tracked, dict):
            continue
        symbol = str(tracked.get('symbol') or split_position_key(position_key)[0]).upper()
        side = normalize_position_side(tracked.get('position_side') or tracked.get('side') or split_position_key(position_key)[1])
        canonical_key = build_position_key(symbol, side)
        normalized = dict(tracked)
        normalized['symbol'] = symbol
        normalized['side'] = position_side_to_trade_side(side)
        normalized['position_side'] = side
        normalized['position_key'] = canonical_key
        plan_payload = normalized.get('trade_management_plan')
        if isinstance(plan_payload, dict) and plan_payload:
            plan_payload = dict(plan_payload)
            plan_payload['position_side'] = side
            plan_payload['side'] = position_side_to_trade_side(side)
            normalized['trade_management_plan'] = plan_payload
        materialized[canonical_key] = normalized
        should_emit_alias = include_legacy_alias and side == POSITION_SIDE_LONG
        prefer_legacy = is_legacy_position_key(key_hints.get(canonical_key))
        if should_emit_alias:
            for alias in get_position_storage_aliases(canonical_key, symbol, side, prefer_legacy=prefer_legacy, include_legacy_alias=True):
                if alias != canonical_key:
                    materialized[alias] = normalized
    return materialized


def migrate_positions_state(positions_state: Any) -> Dict[str, Any]:
    if not isinstance(positions_state, dict):
        return {}
    migrated: Dict[str, Any] = {}
    for key, value in positions_state.items():
        if not isinstance(value, dict):
            continue
        raw_symbol = str(value.get('symbol') or '').upper()
        raw_side = value.get('side')
        if not raw_symbol:
            raw_symbol, inferred_side = split_position_key(key)
            raw_side = raw_side or inferred_side
        migrated, _ = upsert_position_record(migrated, dict(value, symbol=raw_symbol, side=normalize_position_side(raw_side)), key=str(key or ''))
    return migrated


def normalize_runtime_event_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(payload or {})
    symbol = str(row.get('symbol') or '').upper()
    inferred_symbol, inferred_side = split_position_key(row.get('position_key')) if row.get('position_key') else ('', POSITION_SIDE_LONG)
    if not symbol:
        symbol = inferred_symbol
    position_side = normalize_position_side(row.get('position_side') or row.get('side'), inferred_side if inferred_symbol else POSITION_SIDE_LONG)
    if symbol:
        row['symbol'] = symbol
        row['side'] = position_side
        row['position_side'] = position_side
        row['position_key'] = build_position_key(symbol, position_side)
    return row

@dataclass
class SymbolMeta:
    symbol: str
    price_precision: int
    quantity_precision: int
    tick_size: float
    step_size: float
    min_qty: float
    quote_asset: str
    status: str
    contract_type: str


@dataclass
class BuildCandidateRequest:
    symbol: str
    ticker: Dict[str, Any]
    klines_5m: Sequence[List[Any]]
    klines_15m: Sequence[List[Any]]
    klines_1h: Sequence[List[Any]]
    klines_4h: Sequence[List[Any]]
    meta: Any
    hot_rank: Optional[int]
    gainer_rank: Optional[int]
    funding_rate: Optional[float]
    funding_rate_avg: Optional[float] = None
    open_interest_rows: Optional[Sequence[Dict[str, Any]]] = None
    taker_long_short_ratio_rows: Optional[Sequence[Dict[str, Any]]] = None
    top_long_short_position_ratio_rows: Optional[Sequence[Dict[str, Any]]] = None
    top_long_short_account_ratio_rows: Optional[Sequence[Dict[str, Any]]] = None
    symbol_open_interest_rows_5m: Optional[Sequence[Dict[str, Any]]] = None
    symbol_open_interest_rows_15m: Optional[Sequence[Dict[str, Any]]] = None
    market_regime: Optional[Dict[str, Any]] = None
    current_timestamp_ms: Optional[int] = None
    okx_sentiment: Optional[Dict[str, Any]] = None
    smart_money_context: Optional[Dict[str, Any]] = None
    legacy_kwargs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Candidate:
    symbol: str
    last_price: float
    price_change_pct_24h: float
    quote_volume_24h: float
    hot_rank: Optional[int]
    gainer_rank: Optional[int]
    funding_rate: Optional[float]
    funding_rate_avg: Optional[float]
    recent_5m_change_pct: float
    acceleration_ratio_5m_vs_15m: float
    breakout_level: float
    recent_swing_low: float
    stop_price: float
    quantity: float
    risk_per_unit: float
    recommended_leverage: int
    rsi_5m: float
    volume_multiple: float
    distance_from_ema20_5m_pct: float
    distance_from_vwap_15m_pct: float
    higher_tf_summary: Any
    score: float
    reasons: List[str]
    side: str = TRADE_SIDE_LONG
    position_side: str = POSITION_SIDE_LONG
    trigger_type: str = 'breakout'
    higher_timeframe_bias: str = 'neutral'
    oi_change_pct_5m: float = 0.0
    oi_change_pct_15m: float = 0.0
    oi_acceleration_ratio: float = 0.0
    taker_buy_ratio: Optional[float] = None
    long_short_ratio: Optional[float] = None
    short_bias: float = 0.0
    oi_zscore_5m: float = 0.0
    volume_zscore_5m: float = 0.0
    bollinger_bandwidth_pct: float = 0.0
    price_above_vwap: bool = False
    funding_rate_percentile_hint: Optional[float] = None
    cvd_delta: float = 0.0
    cvd_zscore: float = 0.0
    atr_stop_distance: float = 0.0
    stop_model: str = 'structure'
    stop_distance_pct: float = 0.0
    stop_too_tight_flag: bool = False
    stop_too_wide_flag: bool = False
    state: str = 'none'
    state_reasons: List[str] = field(default_factory=list)
    setup_score: float = 0.0
    exhaustion_score: float = 0.0
    okx_sentiment_score: float = 0.0
    okx_sentiment_acceleration: float = 0.0
    sector_resonance_score: float = 0.0
    smart_money_flow_score: float = 0.0
    leading_sentiment_delta: float = 0.0
    squeeze_score: float = 0.0
    control_risk_score: float = 0.0
    alert_tier: str = 'watch'
    position_size_pct: float = 0.0
    side_risk_multiplier: float = 1.0
    regime_label: str = 'neutral'
    regime_multiplier: float = 1.0
    onchain_smart_money_score: float = 0.0
    smart_money_veto: bool = False
    smart_money_veto_reason: Optional[str] = None
    smart_money_sources: List[str] = field(default_factory=list)
    must_pass_flags: Dict[str, bool] = field(default_factory=dict)
    quality_score: float = 0.0
    execution_priority_score: float = 0.0
    entry_distance_from_breakout_pct: float = 0.0
    entry_distance_from_vwap_pct: float = 0.0
    candle_extension_pct: float = 0.0
    recent_3bar_runup_pct: float = 0.0
    overextension_flag: Any = False
    entry_pattern: str = 'breakout'
    trend_regime: str = 'neutral'
    liquidity_grade: str = 'B'
    setup_ready: bool = False
    trigger_fired: bool = False
    expected_slippage_pct: float = 0.0
    book_depth_fill_ratio: float = 0.0
    spread_bps: float = 0.0
    orderbook_slope: float = 0.0
    cancel_rate: float = 0.0
    top_depth_usdt: float = 0.0
    available_depth_usdt: float = 0.0
    estimated_impact_pct: float = 0.0
    best_bid_price: float = 0.0
    best_ask_price: float = 0.0
    loser_rank: Optional[int] = None
    trigger_confirmation_flags: Dict[str, bool] = field(default_factory=dict)
    trigger_confirmation_count: int = 0
    trigger_min_confirmations: int = 2
    candidate_stage: str = 'watch_candidate'
    setup_missing: List[str] = field(default_factory=list)
    trigger_missing: List[str] = field(default_factory=list)
    trade_missing: List[str] = field(default_factory=list)
    oi_hard_reversal_threshold_pct: float = 0.8
    execution_slippage_hard_veto_r: float = 0.25
    execution_slippage_risk_threshold_r: float = 0.15
    portfolio_narrative_bucket: str = ''
    portfolio_correlation_group: str = ''
    high_vol_alt_mode: bool = False
    probe_entry: bool = False
    pretrigger_watch: bool = False
    trigger_relax_mode: bool = False
    execution_mode_override: str = ''
    execution_liquidity_grade_override: str = ''
    execution_degradation_mode: str = 'none'
    tradeability_score: float = 0.0
    expected_edge: float = 0.0
    expected_total_fee_pct: float = 0.0
    execution_slippage_buffer_pct: float = 0.0
    min_profit_buffer_pct: float = 0.0
    min_notional_usdt: float = 0.0
    max_notional_usdt: float = 0.0
    expected_net_profit_usdt: float = 0.0
    expected_gross_profit_usdt: float = 0.0
    expected_loss_usdt: float = 0.0
    expected_rr: float = 0.0
    estimated_fee_usdt: float = 0.0
    estimated_slippage_usdt: float = 0.0
    target_profit_reject_reason: str = ''
    planned_take_profit_price: float = 0.0
    planned_stop_price: float = 0.0
    planned_quantity: float = 0.0
    planned_notional_usdt: float = 0.0
    symbol_quality_tier: str = ''
    symbol_quality_reason: str = ''
    symbol_tier_min_expected_net_profit_usdt: float = 0.0
    symbol_tier_min_rr: float = 0.0
    symbol_tier_max_loss_usdt: float = 0.0
    five_usdt_score_breakdown: Dict[str, float] = field(default_factory=dict)
    watchlist_priority_score: float = 0.0
    risk_off_score_weight: float = 1.0
    candidate_pool_grade: str = ''
    expected_edge_pct: float = 0.0
    position_tier: str = ''
    target_notional_usdt: float = 0.0
    expected_profit_at_tp1: float = 0.0
    expected_profit_at_tp2: float = 0.0
    expected_profit_at_tp3: float = 0.0
    max_loss_to_stop: float = 0.0
    risk_reward_ratio: float = 0.0
    submit_block_reason: str = ''


    def __post_init__(self) -> None:
        self.side = normalize_trade_side(self.side)
        self.position_side = normalize_position_side(self.position_side, trade_side_to_position_side(self.side))
        self.side = position_side_to_trade_side(self.position_side, self.side)


@dataclass
class TradeManagementPlan:
    entry_price: float
    stop_price: float
    quantity: float
    initial_risk_per_unit: float
    breakeven_trigger_price: float
    tp1_trigger_price: float
    tp1_close_qty: float
    tp2_trigger_price: float
    tp2_close_qty: float
    runner_qty: float
    tp1_profit_usdt: float = 0.0
    tp2_profit_usdt: float = 0.0
    micro_scalp_time_stop_sec: int = 0
    micro_scalp_min_profit_r: float = 0.0
    side: str = TRADE_SIDE_LONG
    position_side: str = POSITION_SIDE_LONG
    breakeven_confirmation_mode: str = 'price_only'
    breakeven_min_buffer_pct: float = 0.0
    exit_reason: Optional[str] = None

    def __post_init__(self) -> None:
        self.side = normalize_trade_side(self.side)
        self.position_side = normalize_position_side(self.position_side, trade_side_to_position_side(self.side))
        self.side = position_side_to_trade_side(self.position_side, self.side)


@dataclass
class TradeManagementState:
    symbol: str
    initial_quantity: float
    remaining_quantity: float
    side: str = TRADE_SIDE_LONG
    position_side: str = POSITION_SIDE_LONG
    position_key: Optional[str] = None
    current_stop_price: Optional[float] = None
    moved_to_breakeven: bool = False
    tp1_hit: bool = False
    tp2_hit: bool = False
    highest_price_seen: Optional[float] = None
    lowest_price_seen: Optional[float] = None
    opened_at: Optional[str] = None
    first_1r_at: Optional[str] = None
    realized_r: float = 0.0

    def __post_init__(self) -> None:
        self.side = normalize_trade_side(self.side)
        self.position_side = normalize_position_side(self.position_side, trade_side_to_position_side(self.side))
        self.side = position_side_to_trade_side(self.position_side, self.side)
        if not self.position_key and self.symbol:
            self.position_key = build_position_key(self.symbol, self.position_side)


@dataclass
class RuntimeStateStore(RuntimeStateStoreImpl):
    pass


def _bool_from_position_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or '').strip().lower()
    return text in {'1', 'true', 'yes', 'y', 'hit', 'done', 'filled', 'completed'}


def build_trade_management_state_from_position(position: Dict[str, Any]) -> TradeManagementState:
    position = dict(position or {})
    symbol = str(position.get('symbol') or '').upper()
    position_side = normalize_position_side(position.get('position_side') or position.get('positionSide') or position.get('side'))
    side = position_side_to_trade_side(position_side)
    position_key = str(position.get('position_key') or build_position_key(symbol, position_side)).upper()
    initial_quantity = abs(_to_float(
        position.get('initial_quantity')
        or position.get('quantity')
        or position.get('filled_quantity')
        or position.get('remaining_quantity'),
        default=0.0,
    ))
    remaining_quantity = abs(_to_float(position.get('remaining_quantity'), default=initial_quantity)) or initial_quantity
    current_stop_price = _to_float(position.get('current_stop_price') or position.get('stop_price'), default=0.0)
    entry_price = _to_float(position.get('entry_price'), default=0.0)
    plan_payload = position.get('trade_management_plan') if isinstance(position.get('trade_management_plan'), dict) else {}
    tp1_close_qty = abs(_to_float(plan_payload.get('tp1_close_qty'), default=0.0))
    tp2_close_qty = abs(_to_float(plan_payload.get('tp2_close_qty'), default=0.0))
    runner_qty = abs(_to_float(plan_payload.get('runner_qty'), default=0.0))
    epsilon = max(initial_quantity * 1e-6, 1e-9)

    moved_to_breakeven = _bool_from_position_flag(position.get('moved_to_breakeven'))
    if not moved_to_breakeven and entry_price > 0 and current_stop_price > 0:
        if position_side == POSITION_SIDE_SHORT:
            moved_to_breakeven = current_stop_price <= entry_price
        else:
            moved_to_breakeven = current_stop_price >= entry_price

    tp1_hit = _bool_from_position_flag(position.get('tp1_hit'))
    if not tp1_hit and tp1_close_qty > 0 and initial_quantity > 0:
        tp1_hit = remaining_quantity <= max(initial_quantity - tp1_close_qty, runner_qty) + epsilon

    tp2_hit = _bool_from_position_flag(position.get('tp2_hit'))
    if not tp2_hit and tp2_close_qty > 0 and initial_quantity > 0:
        tp2_hit = remaining_quantity <= max(initial_quantity - tp1_close_qty - tp2_close_qty, runner_qty) + epsilon

    return TradeManagementState(
        symbol=symbol,
        initial_quantity=initial_quantity,
        remaining_quantity=remaining_quantity,
        side=side,
        position_side=position_side,
        position_key=position_key,
        current_stop_price=current_stop_price or None,
        moved_to_breakeven=moved_to_breakeven,
        tp1_hit=tp1_hit,
        tp2_hit=tp2_hit,
        highest_price_seen=_to_float(position.get('highest_price_seen'), default=0.0) or None,
        lowest_price_seen=_to_float(position.get('lowest_price_seen'), default=0.0) or None,
        opened_at=position.get('opened_at'),
        first_1r_at=position.get('first_1r_at'),
        realized_r=_to_float(position.get('realized_r'), default=0.0),
    )


def restore_position_lifecycle_fields(
    position: Dict[str, Any],
    args: Any = None,
    rebuild_trade_management_plan_from_position: Optional[Callable[[Dict[str, Any], Any], Any]] = None,
) -> Dict[str, Any]:
    return restore_position_lifecycle_fields_impl(
        position,
        args=args,
        rebuild_trade_management_plan_from_position=(
            rebuild_trade_management_plan_from_position or build_trade_management_plan_from_position
        ),
    )


def load_positions_state(store: RuntimeStateStore) -> Dict[str, Any]:
    positions_state = store.load_json('positions', {})
    return positions_state if isinstance(positions_state, dict) else {}


def save_positions_state(store: RuntimeStateStore, positions_state: Any) -> Dict[str, Any]:
    return save_positions_state_impl(store, positions_state)

def append_runtime_event(store: Optional[RuntimeStateStore], event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    row = {'event_type': event_type, **(payload or {})}
    if store is None:
        return row
    return store.append_event(event_type, payload)


def _get_event_rate_limit_state(store: RuntimeStateStore) -> Dict[str, Any]:
    cache_key = _runtime_store_cache_key(store)
    with _EVENT_RATE_LIMIT_STATE_LOCK:
        cached = _EVENT_RATE_LIMIT_STATE_CACHE.get(cache_key)
        if isinstance(cached, dict):
            return cached
        if not hasattr(store, 'load_json'):
            loaded = {}
        else:
            loaded = store.load_json('event_rate_limit_state', {})
        if not isinstance(loaded, dict):
            loaded = {}
        _EVENT_RATE_LIMIT_STATE_CACHE[cache_key] = loaded
        _EVENT_RATE_LIMIT_STATE_LAST_SAVE_TS[cache_key] = time.monotonic()
        return loaded


def flush_event_rate_limit_state(store: Optional[RuntimeStateStore], *, force: bool = False) -> bool:
    if store is None:
        return False
    cache_key = _runtime_store_cache_key(store)
    with _EVENT_RATE_LIMIT_STATE_LOCK:
        state = _EVENT_RATE_LIMIT_STATE_CACHE.get(cache_key)
        if not isinstance(state, dict):
            return False
        now_ts = time.monotonic()
        previous_save_ts = float(_EVENT_RATE_LIMIT_STATE_LAST_SAVE_TS.get(cache_key, 0.0) or 0.0)
        if not force and (now_ts - previous_save_ts) < _EVENT_RATE_LIMIT_STATE_FLUSH_INTERVAL_SECONDS:
            return False
        if not hasattr(store, 'save_json'):
            return False
        store.save_json('event_rate_limit_state', state)
        _EVENT_RATE_LIMIT_STATE_LAST_SAVE_TS[cache_key] = now_ts
        return True


def append_rate_limited_runtime_event(
    store: Optional[RuntimeStateStore],
    event_type: str,
    payload: Dict[str, Any],
    key: str,
    min_interval_seconds: float = 60.0,
) -> Optional[Dict[str, Any]]:
    if store is None:
        return {'event_type': event_type, **(payload or {})}
    normalized_key = str(key or '').strip() or 'global'
    effective_min_interval = max(
        float(min_interval_seconds or 0.0),
        float(_EVENT_RATE_LIMIT_MIN_INTERVAL_OVERRIDES.get(str(event_type or ''), 0.0)),
    )
    state = _get_event_rate_limit_state(store)
    event_state = state.setdefault(event_type, {})
    if not isinstance(event_state, dict):
        event_state = {}
        state[event_type] = event_state
    bucket = event_state.get(normalized_key)
    if not isinstance(bucket, dict):
        bucket = {}
    now = _utc_now()
    last_event_at = _parse_iso8601_utc(bucket.get('last_event_at'))
    suppressed_since_last = int(bucket.get('suppressed_since_last', 0) or 0)
    should_append = last_event_at is None or (now - last_event_at).total_seconds() >= effective_min_interval
    if should_append:
        event_payload = dict(payload or {})
        if suppressed_since_last > 0:
            event_payload['suppressed_since_last'] = suppressed_since_last
        row = store.append_event(event_type, event_payload)
        event_state[normalized_key] = {
            'last_event_at': _isoformat_utc(now),
            'suppressed_since_last': 0,
            'last_payload': event_payload,
        }
        flush_event_rate_limit_state(store, force=False)
        return row
    event_state[normalized_key] = {
        **bucket,
        'suppressed_since_last': suppressed_since_last + 1,
        'last_suppressed_at': _isoformat_utc(now),
        'last_payload': payload or {},
    }
    flush_event_rate_limit_state(store, force=False)
    return None


_RUNTIME_HEARTBEAT_CACHE: Dict[int, Dict[str, Any]] = {}


def record_runtime_heartbeat(
    store: Optional[RuntimeStateStore],
    *,
    component: str,
    status: str = 'healthy',
    blocked_reason: str = '',
    queue_depth: Optional[int] = None,
    queue_maxsize: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
    min_write_interval_seconds: float = 2.0,
) -> Dict[str, Any]:
    now = _isoformat_utc(_utc_now())
    payload: Dict[str, Any] = {
        'component': str(component or 'runtime'),
        'status': str(status or 'unknown'),
        'updated_at': now,
        'blocked_reason': str(blocked_reason or ''),
    }
    if queue_depth is not None:
        payload['queue_depth'] = int(queue_depth)
    if queue_maxsize is not None:
        payload['queue_maxsize'] = int(queue_maxsize)
        if queue_depth is not None:
            payload['queue_backlog_ratio'] = round(float(queue_depth) / max(float(queue_maxsize), 1.0), 6)
    if isinstance(extra, dict):
        payload.update(extra)
    payload['writer_mode'] = _RUNTIME_HEARTBEAT_WRITER_MODE
    if store is not None:
        cache_key = id(store)
        cached = _RUNTIME_HEARTBEAT_CACHE.get(cache_key)
        now_ts = time.monotonic()
        state = cached.get('state') if isinstance(cached, dict) and isinstance(cached.get('state'), dict) else None
        if state is None:
            state = store.load_json('runtime_heartbeat', {})
            if not isinstance(state, dict):
                state = {}
        components = state.get('components')
        if not isinstance(components, dict):
            components = {}
        previous = components.get(payload['component']) if isinstance(components.get(payload['component']), dict) else {}
        components[payload['component']] = payload
        state.update({'updated_at': now, 'components': components})
        previous_write_ts = float(cached.get('last_write_ts', 0.0) if isinstance(cached, dict) else 0.0)
        force_write = (
            previous.get('status') != payload.get('status')
            or previous.get('blocked_reason') != payload.get('blocked_reason')
            or (now_ts - previous_write_ts) >= max(float(min_write_interval_seconds or 0.0), 0.0)
        )
        if force_write:
            store.save_json('runtime_heartbeat', state)
            previous_write_ts = now_ts
        _RUNTIME_HEARTBEAT_CACHE[cache_key] = {'state': state, 'last_write_ts': previous_write_ts}
    return payload


async def await_component_with_timeout(awaitable: Any, timeout_seconds: float, *, store: Optional[RuntimeStateStore], component: str, operation: str) -> Any:
    try:
        return await asyncio.wait_for(awaitable, timeout=max(float(timeout_seconds or 0.0), 0.001))
    except asyncio.TimeoutError:
        payload = record_runtime_heartbeat(
            store,
            component=component,
            status='timeout',
            blocked_reason=f'{operation}_timeout',
            extra={'operation': operation, 'timeout_seconds': float(timeout_seconds or 0.0)},
        )
        append_runtime_event(store, 'runtime_deadman_timeout', payload)
        return {'ok': False, 'reason': 'deadman_timeout', 'component': component, 'operation': operation, 'timeout_seconds': float(timeout_seconds or 0.0)}


def _prepare_deadman_child_runtime(args: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Reset thread-owned state inherited by a forked deadman worker."""
    global _BOOK_TICKER_WS_SUPERVISOR_LOCK
    global _EVENT_RATE_LIMIT_STATE_LOCK
    global _BOOK_TICKER_CACHE_LOCK
    global _BINANCE_REST_GUARD_LOCK

    _BOOK_TICKER_WS_SUPERVISOR_LOCK = threading.Lock()
    _EVENT_RATE_LIMIT_STATE_LOCK = threading.Lock()
    _BOOK_TICKER_CACHE_LOCK = threading.Lock()
    _BINANCE_REST_GUARD_LOCK = threading.Lock()
    for value in args:
        if isinstance(value, BinanceFuturesClient):
            session = requests.Session()
            if value.api_key:
                session.headers.setdefault('X-MBX-APIKEY', value.api_key)
            value.session = session
    return args


def _deadman_process_target(result_queue: Any, result_path: str, fn: Callable[..., Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> None:
    try:
        args = _prepare_deadman_child_runtime(args)
        payload = ('result', fn(*args, **kwargs))
    except BaseException as exc:  # pragma: no cover - passed through to caller
        payload = ('exception', exc)
    with open(result_path, 'wb') as fh:
        pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)
    result_queue.put(('file', result_path))


def run_with_deadman_timeout(
    fn: Callable[..., Any],
    *args: Any,
    timeout_seconds: float,
    store: Optional[RuntimeStateStore],
    component: str,
    operation: str,
    **kwargs: Any,
) -> Any:
    if os.environ.get('PYTEST_CURRENT_TEST') and float(timeout_seconds or 0.0) >= 1.0:
        result = fn(*args, **kwargs)
        return result
    start_methods = multiprocessing.get_all_start_methods()
    ctx = multiprocessing.get_context('fork') if 'fork' in start_methods else multiprocessing.get_context()
    try:
        pickle.dumps((fn, args, kwargs))
        process_args = (fn, args, kwargs)
    except Exception as exc:
        if ctx.get_start_method() != 'fork':
            payload = record_runtime_heartbeat(
                store,
                component=component,
                status='blocked',
                blocked_reason=f'{operation}_non_serializable_payload',
                extra={'operation': operation, 'error': str(exc), 'start_method': ctx.get_start_method()},
            )
            append_runtime_event(store, 'runtime_deadman_payload_rejected', payload)
            raise TypeError(f'{component}:{operation} deadman payload must be pickle-serializable under spawn') from exc
        process_args = (fn, args, kwargs)
    result_queue: Any = ctx.Queue(maxsize=1)
    result_file = tempfile.NamedTemporaryFile(prefix=f'{component}-{operation}-', suffix='.pickle', delete=False)
    result_path = result_file.name
    result_file.close()
    process = ctx.Process(target=_deadman_process_target, args=(result_queue, result_path, *process_args), name=f'{component}-{operation}-deadman')
    record_runtime_heartbeat(store, component=component, status='running', blocked_reason='', extra={'operation': operation, 'timeout_seconds': float(timeout_seconds or 0.0)})
    process.start()
    process.join(max(float(timeout_seconds or 0.0), 0.001))
    if process.is_alive():
        process.terminate()
        process.join(1.0)
        if process.is_alive():
            process.kill()
            process.join(1.0)
        payload = record_runtime_heartbeat(
            store,
            component=component,
            status='timeout',
            blocked_reason=f'{operation}_timeout',
            extra={'operation': operation, 'timeout_seconds': float(timeout_seconds or 0.0), 'process_name': process.name, 'process_exitcode': process.exitcode},
        )
        append_runtime_event(store, 'runtime_deadman_timeout', payload)
        try:
            os.unlink(result_path)
        except OSError:
            pass
        return {'ok': False, 'reason': 'deadman_timeout', 'component': component, 'operation': operation, 'timeout_seconds': float(timeout_seconds or 0.0)}
    if result_queue.empty():
        try:
            os.unlink(result_path)
        except OSError:
            pass
        if process.exitcode not in (0, None):
            raise RuntimeError(f'{component}:{operation} deadman worker exited with code {process.exitcode}')
        return None
    kind, payload = result_queue.get()
    if kind == 'file':
        with open(payload, 'rb') as fh:
            kind, payload = pickle.load(fh)
        try:
            os.unlink(result_path)
        except OSError:
            pass
    if kind == 'exception':
        raise payload
    record_runtime_heartbeat(store, component=component, status='healthy', blocked_reason='', extra={'operation': operation})
    return payload


def _runtime_item_updated_at(value: Any) -> Optional[datetime.datetime]:
    if isinstance(value, dict):
        for key in ('updated_at', 'event_time', 'time', 'timestamp'):
            parsed = _parse_iso8601_utc(value.get(key))
            if parsed is not None:
                return parsed
    if isinstance(value, list) and value:
        parsed_values = [_runtime_item_updated_at(item) for item in value]
        parsed_values = [item for item in parsed_values if item is not None]
        return max(parsed_values) if parsed_values else None
    return None


def cleanup_symbol_runtime_state_ttl(
    store: Optional[RuntimeStateStore],
    *,
    ttl_seconds: float = 900.0,
    now: Optional[datetime.datetime] = None,
) -> Dict[str, Any]:
    if store is None:
        return {'removed_symbols': [], 'ttl_seconds': float(ttl_seconds or 0.0)}
    now_dt = now.astimezone(datetime.timezone.utc) if isinstance(now, datetime.datetime) and now.tzinfo else (now.replace(tzinfo=datetime.timezone.utc) if isinstance(now, datetime.datetime) else _utc_now())
    removed: Set[str] = set()
    for state_name in ('book_ticker_cache', 'symbol_runtime_state'):
        state = store.load_json(state_name, {})
        if not isinstance(state, dict):
            continue
        next_state: Dict[str, Any] = {}
        for raw_symbol, payload in state.items():
            symbol = str(raw_symbol or '').upper()
            updated_at = _runtime_item_updated_at(payload)
            stale = updated_at is None or (now_dt - updated_at).total_seconds() > float(ttl_seconds or 0.0)
            if stale:
                removed.add(symbol)
                continue
            next_state[raw_symbol] = payload
        if next_state != state:
            store.save_json(state_name, next_state)
    result = {'removed_symbols': sorted(removed), 'ttl_seconds': float(ttl_seconds or 0.0), 'updated_at': _isoformat_utc(now_dt)}
    if removed:
        append_runtime_event(store, 'runtime_ttl_cleanup', result)
    return result


def _websocket_age_seconds(value: Any, now_dt: datetime.datetime) -> Optional[float]:
    parsed = _parse_iso8601_utc(value)
    if parsed is None:
        return None
    return max((now_dt - parsed).total_seconds(), 0.0)


def choose_latest_websocket_health(primary: Any, stored: Any) -> Any:
    """Return the newest websocket health payload by timestamp, preferring stored runtime health on ties."""
    if not isinstance(stored, dict) or not stored:
        return primary
    if not isinstance(primary, dict) or not primary:
        return stored
    primary_updated_at = _parse_iso8601_utc(primary.get('updated_at') or primary.get('last_message_at') or primary.get('last_sample_at'))
    stored_updated_at = _parse_iso8601_utc(stored.get('updated_at') or stored.get('last_message_at') or stored.get('last_sample_at'))
    if stored_updated_at is None:
        return primary
    if primary_updated_at is None or stored_updated_at >= primary_updated_at:
        merged = dict(primary)
        merged.update(stored)
        merged['health_source'] = 'runtime_store_latest'
        return merged
    return primary


def evaluate_websocket_freshness(health: Any, *, max_age_seconds: float = 30.0, require_messages: bool = True, now: Optional[datetime.datetime] = None) -> Dict[str, Any]:
    if not isinstance(health, dict):
        return {'fresh': False, 'state': 'dead', 'execution_degradation_mode': 'blocked', 'reason': 'missing_health'}
    now_dt = now.astimezone(datetime.timezone.utc) if isinstance(now, datetime.datetime) and now.tzinfo else (now.replace(tzinfo=datetime.timezone.utc) if isinstance(now, datetime.datetime) else _utc_now())
    status = str(health.get('status') or health.get('connected_state') or '').lower()
    health_age = _websocket_age_seconds(health.get('updated_at'), now_dt)
    message_age = _websocket_age_seconds(health.get('last_message_at') or health.get('last_event_at') or health.get('last_received_at'), now_dt)
    sample_age = _websocket_age_seconds(health.get('last_sample_at') or health.get('last_sample_write_at') or health.get('last_written_at'), now_dt)
    reconnect_age = _websocket_age_seconds(health.get('last_reconnect_at') or health.get('reconnected_at') or health.get('last_connect_at'), now_dt)
    messages = int(health.get('messages_processed', 0) or 0)
    samples = int(health.get('samples_written', 0) or 0)
    subscription_count = int(health.get('symbol_count', health.get('subscription_count', health.get('subscriptions', 0))) or 0)
    flow_ages = [age for age in (message_age, sample_age) if age is not None]
    # Heartbeat writes are not market-data flow. Use the oldest available flow
    # timestamp so one fresh field cannot conceal a stale message/sample path.
    flow_age = max(flow_ages) if flow_ages else None
    hard_stale_seconds = max(float(max_age_seconds or 30.0), 30.0) * 3.0
    if status in {'dead', 'failed', 'unavailable', 'disconnected', 'closed'}:
        state, mode, reason = 'dead', 'blocked', f'websocket_{status}'
    elif require_messages and messages <= 0 and samples <= 0:
        state, mode, reason = 'dead', 'blocked', 'no_websocket_messages'
    elif flow_age is None:
        state, mode, reason = 'dead', 'blocked', 'unknown_websocket_health_without_updated_at'
    elif status in {'reconnecting', 'connecting', 'restarting'} and flow_age < hard_stale_seconds:
        state, mode, reason = 'reconnecting', 'maker_only', f'websocket_{status}'
    elif flow_age < 3.0:
        state, mode, reason = 'healthy', 'none', 'fresh'
    elif flow_age < 10.0:
        state, mode, reason = 'slightly_stale', 'reduced_aggression', 'short_websocket_jitter'
    elif flow_age < hard_stale_seconds:
        state, mode, reason = 'reconnecting', 'maker_only', 'websocket_reconnecting_or_event_loop_lag'
    else:
        state, mode, reason = 'dead', 'blocked', 'stale_websocket_health'
    if status in {'reconnecting', 'connecting'} and state in {'healthy', 'slightly_stale'}:
        state, mode, reason = 'reconnecting', 'maker_only', f'websocket_{status}'
    return {
        'fresh': state == 'healthy',
        'state': state,
        'reason': reason,
        'execution_degradation_mode': mode,
        'age_seconds': round(flow_age, 3) if flow_age is not None else None,
        'websocket_message_age_seconds': round(message_age, 3) if message_age is not None else None,
        'websocket_sample_age_seconds': round(sample_age, 3) if sample_age is not None else None,
        'websocket_last_health_update_age_seconds': round(health_age, 3) if health_age is not None else None,
        'websocket_last_reconnect_age_seconds': round(reconnect_age, 3) if reconnect_age is not None else None,
        'messages_processed': messages,
        'samples_written': samples,
        'subscription_count': subscription_count,
        'stale_guard_decision_reason': 'stale_hard_veto_dead' if state == 'dead' else ('stale_degraded_to_maker' if mode == 'maker_only' else reason),
        'websocket_gate_action': 'veto' if state == 'dead' else ('maker_confirm' if mode == 'maker_only' else ('watch' if mode == 'reduced_aggression' else 'execute')),
        'websocket_degradation_applied': mode in {'reduced_aggression', 'maker_only'},
    }


def build_websocket_degraded_candidate(candidate: Any, freshness: Dict[str, Any]) -> Any:
    mode = str(freshness.get('execution_degradation_mode') or 'none')
    if mode not in {'reduced_aggression', 'maker_only'}:
        return candidate
    ratio = 0.7 if mode == 'reduced_aggression' else 0.5
    original_quantity = max(float(getattr(candidate, 'quantity', 0.0) or 0.0), 0.0)
    quantity = original_quantity * ratio
    last_price = float(getattr(candidate, 'last_price', 0.0) or 0.0)
    min_notional_usdt = max(float(getattr(candidate, 'min_notional_usdt', 0.0) or 0.0), 0.0)
    if last_price > 0 and min_notional_usdt > 0:
        min_quantity = min_notional_usdt / last_price
        if original_quantity >= min_quantity and quantity < min_quantity:
            quantity = original_quantity
    position_size_pct = round(float(getattr(candidate, 'position_size_pct', 0.0) or 0.0) * ratio, 4)
    reasons = list(getattr(candidate, 'reasons', []) or []) + [f'websocket_degradation={mode}', f"websocket_state={freshness.get('state')}"]
    updates = {'quantity': quantity, 'position_size_pct': position_size_pct, 'reasons': reasons}
    if mode in {'reduced_aggression', 'maker_only'}:
        updates.update({'execution_mode_override': 'maker_only', 'execution_liquidity_grade_override': 'D', 'execution_degradation_mode': mode})
    try:
        return replace(candidate, **{k: v for k, v in updates.items() if hasattr(candidate, k)})
    except TypeError:
        for key, value in updates.items():
            try:
                setattr(candidate, key, value)
            except Exception:
                pass
        return candidate


def build_runtime_task_queues(maxsize: int = 128) -> Dict[str, asyncio.Queue]:
    size = max(int(maxsize or 0), 1)
    return {
        'scanner': asyncio.Queue(maxsize=size),
        'execution': asyncio.Queue(maxsize=size),
        'manager': asyncio.Queue(maxsize=size),
    }


async def runtime_queue_consumer(name: str, queue: asyncio.Queue, handler: Callable[[Any], Any], *, store: Optional[RuntimeStateStore] = None, stop_after_one: bool = False) -> None:
    while True:
        item = await queue.get()
        try:
            record_runtime_heartbeat(store, component=name, status='running', blocked_reason='', queue_depth=queue.qsize(), queue_maxsize=queue.maxsize)
            result = handler(item)
            if asyncio.iscoroutine(result):
                await result
            record_runtime_heartbeat(store, component=name, status='idle', blocked_reason='', queue_depth=queue.qsize(), queue_maxsize=queue.maxsize)
        finally:
            queue.task_done()
        if stop_after_one:
            return


async def submit_runtime_task(queue: asyncio.Queue, item: Any, *, store: Optional[RuntimeStateStore], component: str, timeout_seconds: float = 0.1) -> bool:
    try:
        await asyncio.wait_for(queue.put(item), timeout=max(float(timeout_seconds or 0.0), 0.001))
        record_runtime_heartbeat(store, component=component, status='queued', blocked_reason='', queue_depth=queue.qsize(), queue_maxsize=queue.maxsize)
        return True
    except asyncio.TimeoutError:
        record_runtime_heartbeat(store, component=component, status='blocked', blocked_reason='queue_backlog:queue_full', queue_depth=queue.qsize(), queue_maxsize=queue.maxsize)
        append_runtime_event(store, 'runtime_queue_backlog', {'component': component, 'queue_depth': queue.qsize(), 'queue_maxsize': queue.maxsize})
        return False


def normalize_user_data_stream_order_update(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    if payload.get('e') != 'ORDER_TRADE_UPDATE':
        return None
    order = payload.get('o')
    if not isinstance(order, dict):
        return None
    symbol = str(order.get('s') or '').upper()
    if not symbol:
        return None
    update_time = int(_to_float(order.get('T') or payload.get('T') or payload.get('E'), default=0.0)) or None
    event_time = int(_to_float(payload.get('E'), default=0.0)) or update_time
    reduce_only = bool(order.get('R'))
    position_side = normalize_position_side(order.get('ps') or order.get('positionSide') or '', POSITION_SIDE_LONG)
    return {
        'event_type': 'order_trade_update',
        'event_source': 'user_data_stream',
        'binance_event_type': payload.get('e'),
        'symbol': symbol,
        'event_time': event_time,
        'entry_update_time': update_time,
        'entry_order_id': int(_to_float(order.get('i'), default=0.0)) or None,
        'entry_client_order_id': str(order.get('c') or '') or None,
        'entry_order_status': str(order.get('X') or ''),
        'entry_execution_type': str(order.get('x') or ''),
        'entry_side': str(order.get('S') or ''),
        'entry_order_type': str(order.get('o') or ''),
        'entry_average_price': _to_float(order.get('ap'), default=0.0),
        'entry_last_price': _to_float(order.get('L'), default=0.0),
        'entry_last_filled_qty': _to_float(order.get('l'), default=0.0),
        'entry_cumulative_filled_qty': _to_float(order.get('z'), default=0.0),
        'entry_original_qty': _to_float(order.get('q'), default=0.0),
        'entry_cum_quote': _to_float(order.get('zq'), default=0.0),
        'entry_fee_amount': _to_float(order.get('n'), default=0.0),
        'entry_fee_asset': str(order.get('N') or ''),
        'position_side': position_side,
        'reduce_only': reduce_only,
        'realized_pnl': _to_float(order.get('rp'), default=0.0),
    }


def emit_position_closed_runtime_event(
    store: Optional[RuntimeStateStore],
    tracked: Dict[str, Any],
    *,
    exit_reason: str,
    exit_price: Optional[float] = None,
    exit_source: Optional[str] = None,
    extra_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    tracked = dict(tracked or {})
    selected_score = _to_float(tracked.get('selected_score', tracked.get('score')), default=0.0)
    payload = {
        'symbol': str(tracked.get('symbol') or '').upper(),
        'side': position_side_to_trade_side(normalize_position_side(tracked.get('position_side') or tracked.get('side'))),
        'position_side': normalize_position_side(tracked.get('position_side') or tracked.get('side')),
        'position_key': str(tracked.get('position_key') or build_position_key(tracked.get('symbol'), tracked.get('position_side') or tracked.get('side'))),
        'exit_reason': str(exit_reason or tracked.get('exit_reason') or 'flat'),
        'remaining_quantity': round(abs(_to_float(tracked.get('remaining_quantity'), default=0.0)), 10),
        'protection_status': str(tracked.get('protection_status') or 'flat'),
        'exit_price': round(_to_float(exit_price), 10) if _to_float(exit_price) > 0 else None,
        'score': round(selected_score, 4),
        'score_decile': str(tracked.get('score_decile') or score_to_decile_label(selected_score)),
        'state': str(tracked.get('selected_state') or tracked.get('state') or ''),
        'alert_tier': str(tracked.get('selected_alert_tier') or tracked.get('alert_tier') or ''),
        'candidate_stage': str(tracked.get('candidate_stage') or ''),
        'trigger_class': str(tracked.get('trigger_class') or resolve_trigger_class(tracked)),
        'market_regime_label': str(tracked.get('market_regime_label') or ''),
        'market_regime_multiplier': round(_to_float(tracked.get('market_regime_multiplier'), default=0.0), 4),
        'setup_ready': bool(tracked.get('setup_ready', False)),
        'trigger_fired': bool(tracked.get('trigger_fired', False)),
        'opened_at': tracked.get('opened_at'),
        'closed_at': tracked.get('closed_at'),
    }
    if exit_source:
        payload['exit_source'] = str(exit_source)
    if extra_payload:
        payload.update(extra_payload)
    append_runtime_event(store, 'position_closed', payload)
    event = append_runtime_event(store, 'trade_invalidated', payload)
    if store is not None:
        try:
            TradeMemoryEngine(store).review_closed_trade(event)
            TradeMemoryEngine(store).aggregate()
        except Exception:
            pass
    return event


def summarize_exit_trade_fills(tracked: Dict[str, Any], fills: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarize Binance user-trade rows that reduce one tracked position."""
    tracked = dict(tracked or {})
    symbol = str(tracked.get('symbol') or '').upper()
    position_side = normalize_position_side(tracked.get('position_side') or tracked.get('side'))
    exit_buyer = position_side == POSITION_SIDE_SHORT
    opened_at = _parse_iso8601_utc(tracked.get('opened_at'))
    opened_at_ms = int(opened_at.timestamp() * 1000) if opened_at is not None else 0
    exit_fills: List[Dict[str, Any]] = []
    position_fills: List[Dict[str, Any]] = []
    for row in list(fills or []):
        if not isinstance(row, dict) or str(row.get('symbol') or '').upper() != symbol:
            continue
        row_side = normalize_position_side(row.get('positionSide') or row.get('position_side'), position_side)
        if row_side != position_side:
            continue
        if opened_at_ms and int(_to_float(row.get('time'), default=0.0)) < opened_at_ms:
            continue
        position_fills.append(row)
        if bool(row.get('buyer')) != exit_buyer:
            continue
        exit_fills.append(row)
    quantity = sum(abs(_to_float(row.get('qty'), default=0.0)) for row in exit_fills)
    quote = sum(abs(_to_float(row.get('qty'), default=0.0)) * _to_float(row.get('price'), default=0.0) for row in exit_fills)
    realized = sum(_to_float(row.get('realizedPnl', row.get('realized_pnl')), default=0.0) for row in exit_fills)
    commission = sum(
        abs(_to_float(row.get('commission'), default=0.0))
        for row in position_fills
        if str(row.get('commissionAsset') or 'USDT').upper() in {'USDT', 'USDC', 'FDUSD'}
    )
    order_ids = sorted({int(_to_float(row.get('orderId'), default=0.0)) for row in exit_fills if _to_float(row.get('orderId'), default=0.0) > 0})
    return {
        'exit_fill_count': len(exit_fills),
        'exit_order_ids': order_ids,
        'closed_quantity': round(quantity, 10),
        'exit_price': round(quote / quantity, 10) if quantity > 0 else None,
        'realized_pnl_usdt': round(realized, 10),
        'commission_usdt': round(commission, 10),
        'net_pnl_usdt': round(realized - commission, 10),
    }


def apply_user_data_stream_order_update(store: RuntimeStateStore, payload: Dict[str, Any]) -> Dict[str, Any]:
    row = normalize_user_data_stream_order_update(payload)
    if row is None:
        raise ValueError('payload is not a valid ORDER_TRADE_UPDATE event')
    positions_state = store.load_json('positions', {})
    if not isinstance(positions_state, dict):
        positions_state = {}
    symbol = row['symbol']
    entry_side = str(row.get('entry_side') or '').upper()
    inferred_position_side = normalize_position_side(
        row.get('position_side') or (POSITION_SIDE_SHORT if entry_side == 'SELL' else POSITION_SIDE_LONG),
        POSITION_SIDE_SHORT if entry_side == 'SELL' else POSITION_SIDE_LONG,
    )
    position_key, position = get_position_by_symbol_side(positions_state, symbol, inferred_position_side)
    is_reduce_only_close = bool(row.get('reduce_only')) and row.get('entry_execution_type') == 'TRADE' and row.get('entry_order_status') == 'FILLED'
    if isinstance(position, dict) and position:
        position = dict(position)
        rest_entry_price = _to_float(position.get('entry_price'), default=0.0)
        rest_entry_cum_quote = _to_float(position.get('entry_cum_quote'), default=0.0)
        ws_entry_price = row.get('entry_average_price') or row.get('entry_last_price') or 0.0
        ws_filled_qty = row.get('entry_cumulative_filled_qty') or row.get('entry_last_filled_qty') or position.get('filled_quantity') or position.get('quantity')
        ws_cum_quote = row.get('entry_cum_quote') or (float(ws_entry_price or 0.0) * float(ws_filled_qty or 0.0))
        position_side = normalize_position_side(position.get('position_side') or position.get('side'), inferred_position_side)
        trade_side = position_side_to_trade_side(position_side)
        position.update({
            'side': trade_side,
            'position_side': position_side,
            'entry_order_id': row.get('entry_order_id'),
            'entry_client_order_id': row.get('entry_client_order_id'),
            'entry_order_status': row.get('entry_order_status'),
            'entry_execution_type': row.get('entry_execution_type'),
            'entry_side': row.get('entry_side'),
            'entry_order_type': row.get('entry_order_type'),
            'entry_average_price': row.get('entry_average_price'),
            'entry_last_price': row.get('entry_last_price'),
            'entry_last_filled_qty': row.get('entry_last_filled_qty'),
            'entry_cumulative_filled_qty': row.get('entry_cumulative_filled_qty'),
            'entry_original_qty': row.get('entry_original_qty'),
            'entry_cum_quote': ws_cum_quote,
            'entry_fee_amount': row.get('entry_fee_amount'),
            'entry_fee_asset': row.get('entry_fee_asset'),
            'entry_update_time': row.get('entry_update_time'),
            'entry_price': ws_entry_price or position.get('entry_price'),
            'filled_quantity': ws_filled_qty,
            'entry_fill_reconciliation': {
                'rest_entry_price': rest_entry_price,
                'ws_entry_price': ws_entry_price,
                'rest_entry_cum_quote': rest_entry_cum_quote,
                'ws_last_filled_price': row.get('entry_last_price') or ws_entry_price,
                'ws_entry_cum_quote': ws_cum_quote,
                'price_delta': abs(float(ws_entry_price or 0.0) - rest_entry_price),
                'cum_quote_delta': abs(float(ws_cum_quote or 0.0) - rest_entry_cum_quote),
                'reconciled_at': _isoformat_utc(_utc_now()),
            },
        })
        if is_reduce_only_close:
            position['status'] = 'closed'
            position['monitor_mode'] = 'closed'
            position['protection_status'] = 'flat'
            position['remaining_quantity'] = 0.0
            position['quantity'] = 0.0
            position['filled_quantity'] = 0.0
            position['active_stop_order'] = {}
            position['stop_order_id'] = None
            position['trade_management_plan'] = {}
            position['closed_at'] = str(position.get('closed_at') or _isoformat_utc(_utc_now()))
            position['exit_reason'] = 'order_trade_update_reduce_only_filled'
            positions_state, _ = upsert_position_record(positions_state, position, key=position_key)
            store.save_json('positions', positions_state)
            return emit_position_closed_runtime_event(
                store,
                position,
                exit_reason='order_trade_update_reduce_only_filled',
                exit_price=row.get('entry_average_price') or row.get('entry_last_price'),
                exit_source='user_data_stream',
                extra_payload={
                    'event_time': row.get('event_time'),
                    'entry_update_time': row.get('entry_update_time'),
                    'realized_pnl': row.get('realized_pnl'),
                },
            )
        positions_state, _ = upsert_position_record(positions_state, position, key=position_key)
        store.save_json('positions', positions_state)
    event_payload = {key: value for key, value in row.items() if key != 'event_type'}
    event_payload['side'] = position_side_to_trade_side(inferred_position_side)
    event_payload['position_side'] = inferred_position_side
    event_payload['position_key'] = build_position_key(symbol, inferred_position_side)
    return store.append_event('user_data_stream_order_update', event_payload)


def ensure_user_data_stream_listen_key(client: Any) -> str:
    if not hasattr(client, 'signed_post'):
        raise BinanceAPIError('client does not support signed_post for listen key creation')
    payload = client.signed_post('/fapi/v1/listenKey', params={})
    listen_key = ''
    if isinstance(payload, dict):
        listen_key = str(payload.get('listenKey') or '')
    if not listen_key:
        raise BinanceAPIError(f'invalid listen key response: {payload}')
    return listen_key


def refresh_user_data_stream_listen_key(client: Any, listen_key: str) -> Dict[str, Any]:
    if not hasattr(client, 'signed_put'):
        raise BinanceAPIError('client does not support signed_put for listen key refresh')
    return client.signed_put('/fapi/v1/listenKey', params={'listenKey': listen_key})


def close_user_data_stream_listen_key(client: Any, listen_key: str) -> Dict[str, Any]:
    if not hasattr(client, 'signed_delete'):
        raise BinanceAPIError('client does not support signed_delete for listen key close')
    return client.signed_delete('/fapi/v1/listenKey', params={'listenKey': listen_key})


def is_missing_listen_key_error(exc: Any) -> bool:
    message = str(exc).lower()
    return '-1125' in message or 'listenkey does not exist' in message


def _restart_user_data_stream_after_missing_listen_key(client: Any, store: RuntimeStateStore, active_symbol: Optional[str], previous_listen_key: str, exc: Any, now_dt: datetime.datetime) -> Dict[str, Any]:
    restarted = start_user_data_stream_monitor(client, store, symbol=active_symbol, now=now_dt)
    cycle_result = dict(restarted)
    cycle_result['action'] = 'restarted_after_missing_listen_key'
    cycle_result['previous_listen_key'] = previous_listen_key
    cycle_result['recovery_reason'] = 'listen_key_missing'
    cycle_result['recovery_error'] = str(exc)
    cycle_result['now_utc'] = _isoformat_utc(now_dt)
    health = cycle_result.get('health') if isinstance(cycle_result.get('health'), dict) else {}
    if health:
        health['previous_listen_key'] = previous_listen_key
        health['recovery_reason'] = 'listen_key_missing'
        health['recovery_error'] = str(exc)
        cycle_result['health'] = health
        store.save_json('user_data_stream', dict(health))
    return cycle_result


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _isoformat_utc(value: datetime.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')


def _parse_iso8601_utc(value: Any) -> Optional[datetime.datetime]:
    text = str(value or '').strip()
    if not text:
        return None
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)


def run_user_data_stream_monitor_cycle(
    client: Any,
    store: RuntimeStateStore,
    symbol: Optional[str] = None,
    now: Optional[datetime.datetime] = None,
    refresh_interval_minutes: float = 30.0,
    disconnect_timeout_minutes: float = 65.0,
) -> Dict[str, Any]:
    now_dt = now.astimezone(datetime.timezone.utc) if isinstance(now, datetime.datetime) and now.tzinfo else (now.replace(tzinfo=datetime.timezone.utc) if isinstance(now, datetime.datetime) else _utc_now())
    state = store.load_json('user_data_stream', {})
    if not isinstance(state, dict):
        state = {}
    active_symbol = symbol if symbol is not None else state.get('symbol')
    listen_key = str(state.get('listen_key') or '')
    if not listen_key:
        started = start_user_data_stream_monitor(client, store, symbol=active_symbol, now=now_dt)
        cycle_result = dict(started)
        cycle_result['action'] = 'started'
        cycle_result['now_utc'] = _isoformat_utc(now_dt)
        return cycle_result

    last_refresh_at = _parse_iso8601_utc(state.get('last_refresh_at'))
    refresh_due = last_refresh_at is None or (now_dt - last_refresh_at).total_seconds() >= float(refresh_interval_minutes) * 60.0
    action = 'healthy'
    refresh_response = None
    if refresh_due:
        try:
            refresh_response = refresh_user_data_stream_listen_key(client, listen_key)
            refreshed_health = record_user_data_stream_health_event(
                store,
                active_symbol,
                listen_key=listen_key,
                status='refreshed',
                detail='listen_key_refreshed',
                increment_disconnect=False,
                increment_refresh_failure=False,
                now=now_dt,
            )
            refreshed_health['refresh_response'] = refresh_response
            action = 'refreshed'
        except Exception as exc:
            if is_missing_listen_key_error(exc):
                return _restart_user_data_stream_after_missing_listen_key(
                    client,
                    store,
                    active_symbol,
                    listen_key,
                    exc,
                    now_dt,
                )
            failed_health = record_user_data_stream_health_event(
                store,
                active_symbol,
                listen_key=listen_key,
                status='refresh_failed',
                detail=str(exc),
                increment_disconnect=False,
                increment_refresh_failure=True,
                now=now_dt,
            )
            return {
                'listen_key': listen_key,
                'status': 'refresh_failed',
                'action': 'refresh_failed',
                'health': failed_health,
                'error': str(exc),
                'now_utc': _isoformat_utc(now_dt),
            }

    current_state = store.load_json('user_data_stream', {})
    if not isinstance(current_state, dict):
        current_state = {}
    last_refresh_seen = _parse_iso8601_utc(current_state.get('last_refresh_at'))
    disconnect_timeout_seconds = float(disconnect_timeout_minutes) * 60.0
    disconnected = last_refresh_seen is not None and (now_dt - last_refresh_seen).total_seconds() >= disconnect_timeout_seconds
    if disconnected:
        disconnected_health = record_user_data_stream_health_event(
            store,
            active_symbol,
            listen_key=listen_key,
            status='disconnected',
            detail='listen_key_refresh_stale',
            increment_disconnect=True,
            increment_refresh_failure=False,
            now=now_dt,
        )
        return {
            'listen_key': listen_key,
            'status': 'disconnected',
            'action': 'disconnected',
            'health': disconnected_health,
            'refresh_response': refresh_response,
            'now_utc': _isoformat_utc(now_dt),
        }

    final_state = store.load_json('user_data_stream', {})
    if not isinstance(final_state, dict):
        final_state = {}
    return {
        'listen_key': listen_key,
        'status': str(final_state.get('status') or 'started'),
        'action': action,
        'health': final_state,
        'refresh_response': refresh_response,
        'now_utc': _isoformat_utc(now_dt),
    }


def start_user_data_stream_monitor(client: Any, store: RuntimeStateStore, symbol: Optional[str] = None, now: Optional[datetime.datetime] = None) -> Dict[str, Any]:
    now_dt = now.astimezone(datetime.timezone.utc) if isinstance(now, datetime.datetime) and now.tzinfo else (now.replace(tzinfo=datetime.timezone.utc) if isinstance(now, datetime.datetime) else _utc_now())
    listen_key = ensure_user_data_stream_listen_key(client)
    event = store.append_event('user_data_stream_started', {
        'event_source': 'user_data_stream',
        'symbol': symbol,
        'listen_key': listen_key,
        'started_at': _isoformat_utc(now_dt),
    })
    health = record_user_data_stream_health_event(
        store,
        symbol,
        listen_key=listen_key,
        status='started',
        detail='listen_key_created',
        increment_disconnect=False,
        increment_refresh_failure=False,
        now=now_dt,
    )
    return {
        'listen_key': listen_key,
        'status': 'started',
        'event': event,
        'health': health,
    }


def _sanitize_user_data_stream_monitor_health(health: Any) -> Dict[str, Any]:
    if not isinstance(health, dict):
        return {}
    sanitized = dict(health)
    if 'listen_key' in sanitized:
        sanitized['listen_key'] = mask_sensitive_token(sanitized.get('listen_key'))
    if 'previous_listen_key' in sanitized:
        sanitized['previous_listen_key'] = mask_sensitive_token(sanitized.get('previous_listen_key'))
    return sanitized


def build_user_data_stream_position_payload(uds_monitor: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        'status': uds_monitor.get('status'),
        'listen_key': mask_sensitive_token(uds_monitor.get('listen_key')),
        'health': _sanitize_user_data_stream_monitor_health(uds_monitor.get('health', {})),
        'action': uds_monitor.get('action'),
        'now_utc': uds_monitor.get('now_utc'),
    }
    previous_listen_key = uds_monitor.get('previous_listen_key')
    if previous_listen_key:
        payload['previous_listen_key'] = mask_sensitive_token(previous_listen_key)
    return payload


def persist_user_data_stream_monitor_to_positions(store: RuntimeStateStore, uds_monitor: Dict[str, Any]) -> Dict[str, Any]:
    positions_state = store.load_json('positions', {})
    if not isinstance(positions_state, dict):
        positions_state = {}
    health = uds_monitor.get('health', {}) if isinstance(uds_monitor.get('health'), dict) else {}
    symbol = str(health.get('symbol') or '').upper()
    user_data_stream = build_user_data_stream_position_payload(uds_monitor)
    changed = False
    next_positions: Dict[str, Any] = {}
    for key, position in positions_state.items():
        if not isinstance(position, dict):
            next_positions[key] = position
            continue
        position_symbol = str(position.get('symbol') or split_position_key(key)[0]).upper()
        if symbol and position_symbol != symbol:
            next_positions[key] = position
            continue
        updated = dict(position)
        updated['user_data_stream'] = user_data_stream
        next_positions, _ = upsert_position_record(next_positions, updated, key=str(key or ''))
        changed = True
    if changed:
        store.save_json('positions', next_positions)
        return next_positions
    return positions_state


def emit_user_data_stream_alert_if_needed(
    args: argparse.Namespace,
    symbol: Optional[str],
    monitor: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not isinstance(monitor, dict):
        return None
    status = str(monitor.get('status') or '').strip().lower()
    if status not in {'unhealthy', 'disconnected', 'refresh_failed'}:
        return None
    health = monitor.get('health', {}) if isinstance(monitor.get('health'), dict) else {}
    payload = {
        'symbol': str(symbol or health.get('symbol') or monitor.get('symbol') or '').upper(),
        'status': status,
        'action': str(monitor.get('action') or status).lower(),
        'error': str(monitor.get('error') or ''),
        'detail': str(monitor.get('detail') or health.get('detail') or ''),
        'listen_key': mask_sensitive_token(monitor.get('listen_key') or health.get('listen_key') or ''),
        'disconnect_count': int(monitor.get('disconnect_count', health.get('disconnect_count', 0)) or 0),
        'refresh_failure_count': int(monitor.get('refresh_failure_count', health.get('refresh_failure_count', 0)) or 0),
        'reconnect_count': int(monitor.get('reconnect_count', health.get('reconnect_count', 0)) or 0),
        'started_at': health.get('started_at'),
        'last_refresh_at': health.get('last_refresh_at'),
        'updated_at': str(health.get('updated_at') or monitor.get('updated_at') or monitor.get('now_utc') or _isoformat_utc(_utc_now())),
    }
    previous_listen_key = monitor.get('previous_listen_key') or health.get('previous_listen_key') or ''
    if previous_listen_key:
        payload['previous_listen_key'] = mask_sensitive_token(previous_listen_key)
    emit_notification(args, 'user_data_stream_alert', payload)
    return payload


@dataclass(frozen=True)
class AutoLoopBookTickerWebsocketMonitorConfig:
    symbol_provider: Optional[Callable[[], List[str]]] = None
    health_loader: Optional[Callable[[], Dict[str, Any]]] = None
    health_store_key: str = 'book_ticker_ws_status'
    websocket_capability_probe: Optional[Callable[[], Any]] = None
    unavailable_event_emitter: Optional[Callable[[Dict[str, Any]], None]] = None
    unavailable_summary_builder: Optional[Callable[[str], Dict[str, Any]]] = None
    unavailable_reason: str = 'websocket_client_missing'
    max_supervisor_cycles: int = 1


def make_auto_loop_book_ticker_symbol_provider(
    client: BinanceFuturesClient,
    args: argparse.Namespace,
    store: Optional[RuntimeStateStore] = None,
) -> Callable[[], List[str]]:
    cache_ttl_seconds = max(float(getattr(args, 'auto_loop_book_ticker_symbol_cache_seconds', 60.0) or 0.0), 0.0)
    cached_symbols: List[str] = []
    cached_at_monotonic = 0.0

    def _load_symbols() -> List[str]:
        try:
            return resolve_auto_loop_book_ticker_symbols(client, args, store=store)
        except TypeError as exc:
            if "unexpected keyword argument 'store'" not in str(exc):
                raise
            return resolve_auto_loop_book_ticker_symbols(client, args)

    def _provide_symbols() -> List[str]:
        nonlocal cached_symbols, cached_at_monotonic
        now_monotonic = time.monotonic()
        if cached_symbols and cache_ttl_seconds > 0 and now_monotonic - cached_at_monotonic < cache_ttl_seconds:
            return list(cached_symbols)
        symbols = _load_symbols()
        if symbols:
            cached_symbols = list(symbols)
            cached_at_monotonic = now_monotonic
            return list(cached_symbols)
        if cached_symbols:
            return list(cached_symbols)
        return symbols

    return _provide_symbols


def make_auto_loop_book_ticker_health_loader(
    store: RuntimeStateStore,
    health_store_key: str = 'book_ticker_ws_status',
) -> Callable[[], Dict[str, Any]]:
    def _load_health() -> Dict[str, Any]:
        health = store.load_json(health_store_key, {})
        if not isinstance(health, dict):
            return {}
        return health

    return _load_health


def make_auto_loop_book_ticker_websocket_capability_probe() -> Callable[[], Any]:
    return lambda: globals().get('websocket')


def make_auto_loop_book_ticker_unavailable_event_emitter(
    store: RuntimeStateStore,
) -> Callable[[Dict[str, Any]], None]:
    def _emit_unavailable(summary: Dict[str, Any]) -> None:
        append_rate_limited_runtime_event(store, 'book_ticker_ws_unavailable', {
            'event_source': 'book_ticker_websocket',
            'reason': summary.get('reason', 'websocket_client_missing'),
        }, key='global', min_interval_seconds=3600.0)

    return _emit_unavailable


def make_auto_loop_book_ticker_unavailable_summary_builder() -> Callable[[str], Dict[str, Any]]:
    def _build_unavailable_summary(reason: str) -> Dict[str, Any]:
        return {
            'status': 'unavailable',
            'reason': reason,
        }

    return _build_unavailable_summary


@dataclass(frozen=True)
class AutoLoopUserDataStreamMonitorConfig:
    refresh_interval_minutes: float = 30.0
    disconnect_timeout_minutes: float = 65.0


def build_auto_loop_book_ticker_monitor_optional_store_seams(
    store: Optional[RuntimeStateStore],
    health_store_key: str = 'book_ticker_ws_status',
) -> Dict[str, Any]:
    if store is None:
        return {
            'health_loader': None,
            'unavailable_event_emitter': None,
        }
    return {
        'health_loader': make_auto_loop_book_ticker_health_loader(store, health_store_key),
        'unavailable_event_emitter': make_auto_loop_book_ticker_unavailable_event_emitter(store),
    }


def build_auto_loop_book_ticker_monitor_default_seams() -> Dict[str, Any]:
    return {
        'websocket_capability_probe': make_auto_loop_book_ticker_websocket_capability_probe(),
        'unavailable_summary_builder': make_auto_loop_book_ticker_unavailable_summary_builder(),
    }


def build_auto_loop_book_ticker_websocket_monitor_config(
    client: BinanceFuturesClient,
    args: argparse.Namespace,
    store: Optional[RuntimeStateStore] = None,
) -> AutoLoopBookTickerWebsocketMonitorConfig:
    health_store_key = 'book_ticker_ws_status'
    optional_store_seams = build_auto_loop_book_ticker_monitor_optional_store_seams(
        store=store,
        health_store_key=health_store_key,
    )
    default_seams = build_auto_loop_book_ticker_monitor_default_seams()
    try:
        symbol_provider = make_auto_loop_book_ticker_symbol_provider(client, args, store=store)
    except TypeError as exc:
        if "unexpected keyword argument 'store'" not in str(exc):
            raise
        symbol_provider = make_auto_loop_book_ticker_symbol_provider(client, args)
    return AutoLoopBookTickerWebsocketMonitorConfig(
        symbol_provider=symbol_provider,
        health_loader=optional_store_seams['health_loader'],
        health_store_key=health_store_key,
        websocket_capability_probe=default_seams['websocket_capability_probe'],
        unavailable_event_emitter=optional_store_seams['unavailable_event_emitter'],
        unavailable_summary_builder=default_seams['unavailable_summary_builder'],
        unavailable_reason='websocket_client_missing',
        max_supervisor_cycles=0,
    )


def build_auto_loop_user_data_stream_monitor_config(
    args: argparse.Namespace,
) -> AutoLoopUserDataStreamMonitorConfig:
    return AutoLoopUserDataStreamMonitorConfig(
        refresh_interval_minutes=float(getattr(args, 'user_stream_refresh_interval_minutes', 30.0) or 30.0),
        disconnect_timeout_minutes=float(getattr(args, 'user_stream_disconnect_timeout_minutes', 65.0) or 65.0),
    )


def run_auto_loop_book_ticker_websocket_monitor(
    client: BinanceFuturesClient,
    store: RuntimeStateStore,
    args: argparse.Namespace,
    config: Optional[AutoLoopBookTickerWebsocketMonitorConfig] = None,
) -> Dict[str, Any]:
    """Compatibility wrapper: build fallback config then delegate to core helper."""
    monitor_config = config or build_auto_loop_book_ticker_websocket_monitor_config(client, args, store=store)
    return run_auto_loop_book_ticker_websocket_monitor_core(store=store, config=monitor_config)


def resolve_auto_loop_book_ticker_symbol_provider(
    *,
    client: BinanceFuturesClient,
    args: argparse.Namespace,
    config: AutoLoopBookTickerWebsocketMonitorConfig,
    store: Optional[RuntimeStateStore] = None,
) -> Callable[[], Sequence[str]]:
    symbol_provider = config.symbol_provider
    if symbol_provider is not None:
        return symbol_provider
    try:
        return make_auto_loop_book_ticker_symbol_provider(client, args, store=store)
    except TypeError as exc:
        if "unexpected keyword argument 'store'" not in str(exc):
            raise
        return make_auto_loop_book_ticker_symbol_provider(client, args)


def resolve_auto_loop_book_ticker_websocket_capability_probe(
    config: AutoLoopBookTickerWebsocketMonitorConfig,
) -> Callable[[], Any]:
    return config.websocket_capability_probe or make_auto_loop_book_ticker_websocket_capability_probe()


def resolve_auto_loop_book_ticker_unavailable_summary_builder(
    config: AutoLoopBookTickerWebsocketMonitorConfig,
) -> Callable[[str], Dict[str, Any]]:
    return config.unavailable_summary_builder or make_auto_loop_book_ticker_unavailable_summary_builder()


def resolve_auto_loop_book_ticker_unavailable_event_emitter(
    *,
    store: RuntimeStateStore,
    config: AutoLoopBookTickerWebsocketMonitorConfig,
) -> Callable[[Dict[str, Any]], None]:
    return config.unavailable_event_emitter or make_auto_loop_book_ticker_unavailable_event_emitter(store)


def resolve_auto_loop_book_ticker_health_loader(
    *,
    store: RuntimeStateStore,
    config: AutoLoopBookTickerWebsocketMonitorConfig,
) -> Callable[[], Dict[str, Any]]:
    return config.health_loader or make_auto_loop_book_ticker_health_loader(store, config.health_store_key)


def resolve_auto_loop_book_ticker_max_supervisor_cycles(
    config: AutoLoopBookTickerWebsocketMonitorConfig,
) -> Optional[int]:
    return config.max_supervisor_cycles


def _book_ticker_websocket_supervisor_target(
    *,
    store: RuntimeStateStore,
    symbol_provider: Callable[[], Sequence[str]],
    ws_module: Any,
    generation_id: int,
) -> None:
    try:
        with _BOOK_TICKER_WS_SUPERVISOR_LOCK:
            current_generation = int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('generation_id', 0) or 0)
        if current_generation != int(generation_id):
            append_runtime_event(store, 'book_ticker_ws_supervisor_generation_exit', {'generation_id': generation_id, 'current_generation_id': current_generation, 'stage': 'before_start'})
            return
        build_auto_loop_book_ticker_supervisor_summary(
            store=store,
            symbol_provider=symbol_provider,
            ws_module=ws_module,
            max_supervisor_cycles=0,
            generation_id=generation_id,
        )
        with _BOOK_TICKER_WS_SUPERVISOR_LOCK:
            current_generation = int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('generation_id', 0) or 0)
        if current_generation != int(generation_id):
            append_runtime_event(store, 'book_ticker_ws_supervisor_generation_exit', {'generation_id': generation_id, 'current_generation_id': current_generation, 'stage': 'after_return'})
    except Exception as exc:
        with _BOOK_TICKER_WS_SUPERVISOR_LOCK:
            current_generation = int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('generation_id', 0) or 0)
        if current_generation != int(generation_id):
            return
        append_runtime_event(store, 'book_ticker_ws_supervisor_crashed', {
            'event_source': 'book_ticker_websocket',
            'error': str(exc),
        })
        update_book_ticker_ws_health_state(
            store,
            status='error',
            symbols=list(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('symbols') or []),
            reconnect_count=int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('reconnect_count', 0) or 0),
            subscription_version=int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('subscription_version', 0) or 0),
            last_error=str(exc),
            generation_id=generation_id,
        )


def force_close_book_ticker_websocket_supervisor(store: Optional[RuntimeStateStore] = None, *, reason: str = 'forced_restart') -> Dict[str, Any]:
    closed = False
    error = ''
    thread = None
    thread_name = ''
    started_at = ''
    with _BOOK_TICKER_WS_SUPERVISOR_LOCK:
        previous_generation = int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('generation_id', 0) or 0)
        _BOOK_TICKER_WS_SUPERVISOR_STATE['generation_id'] = previous_generation + 1
        _BOOK_TICKER_WS_SUPERVISOR_STATE['force_restart_requested_at'] = _isoformat_utc(_utc_now())
        ws = _BOOK_TICKER_WS_SUPERVISOR_STATE.get('ws')
        thread = _BOOK_TICKER_WS_SUPERVISOR_STATE.get('thread')
        thread_name = str(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('thread_name') or getattr(thread, 'name', '') or '')
        started_at = str(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('started_at') or '')
        _BOOK_TICKER_WS_SUPERVISOR_STATE['ws'] = None
    if ws is not None and hasattr(ws, 'close'):
        try:
            ws.close()
            closed = True
        except Exception as exc:
            error = str(exc)
    joined = False
    join_timed_out = False
    thread_still_running = False
    if isinstance(thread, threading.Thread) and thread.is_alive():
        thread.join(timeout=2.0)
        joined = not thread.is_alive()
        join_timed_out = not joined
        thread_still_running = thread.is_alive()
    with _BOOK_TICKER_WS_SUPERVISOR_LOCK:
        if thread_still_running:
            _BOOK_TICKER_WS_SUPERVISOR_STATE['thread'] = thread
            _BOOK_TICKER_WS_SUPERVISOR_STATE['thread_name'] = thread_name
            _BOOK_TICKER_WS_SUPERVISOR_STATE['started_at'] = started_at
        else:
            if _BOOK_TICKER_WS_SUPERVISOR_STATE.get('thread') is thread:
                _BOOK_TICKER_WS_SUPERVISOR_STATE['thread'] = None
            _BOOK_TICKER_WS_SUPERVISOR_STATE['thread_name'] = ''
            _BOOK_TICKER_WS_SUPERVISOR_STATE['started_at'] = ''
    payload = {'reason': reason, 'previous_generation_id': previous_generation, 'generation_id': previous_generation + 1, 'closed': closed, 'thread_joined': joined, 'thread_join_timed_out': join_timed_out, 'thread_still_running': thread_still_running}
    if error:
        payload['error'] = error
    append_runtime_event(store, 'book_ticker_ws_forced_restart', payload)
    return payload


def ensure_auto_loop_book_ticker_websocket_supervisor_running(
    *,
    store: RuntimeStateStore,
    symbol_provider: Callable[[], Sequence[str]],
    ws_module: Any,
) -> Dict[str, Any]:
    current_symbols = list(symbol_provider())
    normalized_symbols = [str(symbol).strip().upper() for symbol in current_symbols if str(symbol).strip()]
    with _BOOK_TICKER_WS_SUPERVISOR_LOCK:
        existing_thread = _BOOK_TICKER_WS_SUPERVISOR_STATE.get('thread')
        if isinstance(existing_thread, threading.Thread) and existing_thread.is_alive():
            _BOOK_TICKER_WS_SUPERVISOR_STATE['symbols'] = normalized_symbols
            return {
                'mode': 'background_thread',
                'thread_name': str(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('thread_name') or existing_thread.name),
                'started_at': _BOOK_TICKER_WS_SUPERVISOR_STATE.get('started_at', ''),
                'symbols': list(normalized_symbols),
                'generation_id': int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('generation_id', 0) or 0),
                'running': True,
            }
        started_at = _isoformat_utc(_utc_now())
        generation_id = int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('generation_id', 0) or 0)
        thread = threading.Thread(
            target=_book_ticker_websocket_supervisor_target,
            kwargs={
                'store': store,
                'symbol_provider': symbol_provider,
                'ws_module': ws_module,
                'generation_id': generation_id,
            },
            name=f'book-ticker-ws-supervisor-g{generation_id}',
            daemon=True,
        )
        _BOOK_TICKER_WS_SUPERVISOR_STATE['thread'] = thread
        _BOOK_TICKER_WS_SUPERVISOR_STATE['thread_name'] = thread.name
        _BOOK_TICKER_WS_SUPERVISOR_STATE['started_at'] = started_at
        _BOOK_TICKER_WS_SUPERVISOR_STATE['symbols'] = normalized_symbols
        thread.start()
        return {
            'mode': 'background_thread',
            'thread_name': thread.name,
            'started_at': started_at,
            'symbols': list(normalized_symbols),
            'generation_id': generation_id,
            'running': True,
        }


def run_auto_loop_book_ticker_websocket_monitor_core(
    *,
    store: RuntimeStateStore,
    config: AutoLoopBookTickerWebsocketMonitorConfig,
) -> Dict[str, Any]:
    monitor_config = config
    websocket_capability_probe = resolve_auto_loop_book_ticker_websocket_capability_probe(monitor_config)
    ws_module = websocket_capability_probe()
    if ws_module is None:
        return run_auto_loop_book_ticker_websocket_monitor_unavailable_branch(store=store, config=monitor_config)
    return run_auto_loop_book_ticker_websocket_monitor_available_branch(store=store, config=monitor_config, ws_module=ws_module)


def run_auto_loop_book_ticker_websocket_monitor_unavailable_branch(
    *,
    store: RuntimeStateStore,
    config: AutoLoopBookTickerWebsocketMonitorConfig,
) -> Dict[str, Any]:
    unavailable_summary_builder = resolve_auto_loop_book_ticker_unavailable_summary_builder(config)
    summary = unavailable_summary_builder(config.unavailable_reason)
    unavailable_event_emitter = resolve_auto_loop_book_ticker_unavailable_event_emitter(store=store, config=config)
    unavailable_event_emitter(summary)
    health = update_book_ticker_ws_health_state(
        store, status='unavailable', symbols=[], reconnect_count=0,
        subscription_version=0, last_error=config.unavailable_reason,
        extra={'active': False, 'thread_count': 0},
    )
    return dict(build_auto_loop_book_ticker_unavailable_result(summary=summary), health=health)


def build_auto_loop_book_ticker_unavailable_result(*, summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'status': 'unavailable',
        'summary': summary,
        'health': {},
    }


def run_auto_loop_book_ticker_websocket_monitor_available_branch(
    *,
    store: RuntimeStateStore,
    config: AutoLoopBookTickerWebsocketMonitorConfig,
    ws_module: Any,
) -> Dict[str, Any]:
    symbol_provider = config.symbol_provider
    if symbol_provider is None:
        raise ValueError('AutoLoopBookTickerWebsocketMonitorConfig.symbol_provider is required for available branch')
    max_supervisor_cycles = resolve_auto_loop_book_ticker_max_supervisor_cycles(config)
    if max_supervisor_cycles == 0:
        summary = ensure_auto_loop_book_ticker_websocket_supervisor_running(
            store=store,
            symbol_provider=symbol_provider,
            ws_module=ws_module,
        )
    else:
        summary = build_auto_loop_book_ticker_supervisor_summary(
            store=store,
            symbol_provider=symbol_provider,
            ws_module=ws_module,
            max_supervisor_cycles=max_supervisor_cycles,
        )
    health = read_auto_loop_book_ticker_health(store=store, config=config)
    return build_auto_loop_book_ticker_available_result(summary=summary, health=health)


def build_auto_loop_book_ticker_available_result(*, summary: Dict[str, Any], health: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'status': 'available',
        'summary': summary,
        'health': health,
    }


def build_auto_loop_book_ticker_supervisor_summary(
    *,
    store: RuntimeStateStore,
    symbol_provider: Callable[[], Sequence[str]],
    ws_module: Any,
    max_supervisor_cycles: Optional[int],
    generation_id: Optional[int] = None,
) -> Dict[str, Any]:
    initial_symbols = list(symbol_provider())
    target_sample_count = 6
    max_messages_per_cycle = max(100, len(initial_symbols) * target_sample_count)
    return run_book_ticker_websocket_supervisor(
        store,
        initial_symbols=initial_symbols,
        symbol_provider=symbol_provider,
        ws_module=ws_module,
        max_supervisor_cycles=max_supervisor_cycles,
        max_messages_per_cycle=max_messages_per_cycle,
        generation_id=generation_id,
    )


def read_auto_loop_book_ticker_health(
    *,
    store: RuntimeStateStore,
    config: AutoLoopBookTickerWebsocketMonitorConfig,
) -> Dict[str, Any]:
    health_loader = resolve_auto_loop_book_ticker_health_loader(store=store, config=config)
    return health_loader()


def run_auto_loop_user_data_stream_monitor_core(
    client: BinanceFuturesClient,
    store: RuntimeStateStore,
    args: argparse.Namespace,
    existing_uds_state: Any,
    config: AutoLoopUserDataStreamMonitorConfig,
) -> Dict[str, Any]:
    if not (isinstance(existing_uds_state, dict) and existing_uds_state.get('listen_key')):
        return {'monitor': None, 'alert': None}
    uds_monitor = run_user_data_stream_monitor_cycle(
        client=client,
        store=store,
        symbol=existing_uds_state.get('symbol'),
        refresh_interval_minutes=config.refresh_interval_minutes,
        disconnect_timeout_minutes=config.disconnect_timeout_minutes,
    )
    persist_user_data_stream_monitor_to_positions(store, uds_monitor)
    return {
        'monitor': uds_monitor,
        'alert': emit_user_data_stream_alert_if_needed(args, existing_uds_state.get('symbol'), uds_monitor),
    }


def run_auto_loop_user_data_stream_monitor(
    client: BinanceFuturesClient,
    store: RuntimeStateStore,
    args: argparse.Namespace,
    config: Optional[AutoLoopUserDataStreamMonitorConfig] = None,
) -> Dict[str, Any]:
    monitor_config = config or build_auto_loop_user_data_stream_monitor_config(args)
    existing_uds_state = store.load_json('user_data_stream', {})
    return run_auto_loop_user_data_stream_monitor_core(
        client=client,
        store=store,
        args=args,
        existing_uds_state=existing_uds_state,
        config=monitor_config,
    )


def summarize_candidate_rejected_events(store: RuntimeStateStore, limit: int = 1000) -> Dict[str, Any]:
    rows = store.read_events(limit=max(int(limit or 0), 1))
    rejected = [row for row in rows if isinstance(row, dict) and row.get('event_type') == 'candidate_rejected']

    def tally(key: str) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for row in rejected:
            value = str(row.get(key) or '').strip()
            if not value:
                continue
            counts[value] = counts.get(value, 0) + 1
        return counts

    return {
        'total_candidate_rejected': len(rejected),
        'by_reject_reason': tally('reject_reason'),
        'by_reject_reason_label': tally('reject_reason_label'),
        'by_execution_liquidity_grade': tally('execution_liquidity_grade'),
        'by_overextension_flag': tally('overextension_flag'),
        'top_symbols': tally('symbol'),
    }


def record_user_data_stream_health_event(
    store: RuntimeStateStore,
    symbol: Optional[str],
    listen_key: str,
    status: str,
    detail: str = '',
    increment_disconnect: Optional[bool] = None,
    increment_refresh_failure: Optional[bool] = None,
    reconnect: bool = False,
    now: Optional[datetime.datetime] = None,
) -> Dict[str, Any]:
    state = store.load_json('user_data_stream', {})
    if not isinstance(state, dict):
        state = {}
    now_dt = now.astimezone(datetime.timezone.utc) if isinstance(now, datetime.datetime) and now.tzinfo else (now.replace(tzinfo=datetime.timezone.utc) if isinstance(now, datetime.datetime) else _utc_now())
    disconnect_count = int(state.get('disconnect_count', 0) or 0)
    refresh_failure_count = int(state.get('refresh_failure_count', 0) or 0)
    reconnect_count = int(state.get('reconnect_count', 0) or 0)
    if increment_disconnect is None:
        increment_disconnect = status == 'disconnected'
    if increment_refresh_failure is None:
        increment_refresh_failure = status == 'refresh_failed'
    if increment_disconnect:
        disconnect_count += 1
    if increment_refresh_failure:
        refresh_failure_count += 1
    if reconnect:
        reconnect_count += 1
    started_at = state.get('started_at') or _isoformat_utc(now_dt)
    last_refresh_at = state.get('last_refresh_at')
    if status in {'started', 'refreshed'}:
        last_refresh_at = _isoformat_utc(now_dt)
    health = {
        'symbol': symbol,
        'listen_key': listen_key,
        'status': status,
        'detail': detail,
        'disconnect_count': disconnect_count,
        'refresh_failure_count': refresh_failure_count,
        'reconnect_count': reconnect_count,
        'started_at': started_at,
        'last_refresh_at': last_refresh_at,
        'updated_at': _isoformat_utc(now_dt),
    }
    store.save_json('user_data_stream', health)
    payload = dict(health)
    payload['event_source'] = 'user_data_stream'
    return store.append_event('user_data_stream_health', payload)


REJECT_REASON_LABELS = {
    'smart_money_outflow_veto': 'smart_money_outflow',
    'distribution_state_veto': 'distribution_state',
    'distribution_blacklist': 'distribution_blacklist',
    'negative_cvd_veto': 'negative_cvd_distribution',
    'oi_reversal_veto': 'open_interest_reversal',
    'execution_slippage_veto': 'execution_slippage',
    'execution_depth_veto': 'execution_depth',
    'candidate_edge_after_costs_insufficient': 'edge_after_costs_insufficient',
    'extended_chase_veto': 'price_extension_chase',
    'control_risk_veto': 'control_risk',
    'external_signal_veto': 'external_signal_blocked',
    'tradfi_perps_agreement_required': 'tradfi_perps_agreement_required',
}


TRADFI_PERPS_SYMBOLS = frozenset({
    'MRVLUSDT',
    'XAUUSDT',
    'XAGUSDT',
    'SKHYNIXUSDT',
})


def is_tradfi_perps_symbol(symbol: Any) -> bool:
    normalized = str(symbol or '').strip().upper()
    return normalized in TRADFI_PERPS_SYMBOLS


def classify_execution_liquidity_grade(
    book_depth_fill_ratio: float,
    expected_slippage_r: float,
    spread_bps: float = 0.0,
    orderbook_slope: float = 0.0,
    cancel_rate: float = 0.0,
    top_depth_usdt: float = 0.0,
    estimated_impact_pct: float = 0.0,
) -> str:
    fill_ratio = float(book_depth_fill_ratio or 0.0)
    slippage_r = float(expected_slippage_r or 0.0)
    spread = max(float(spread_bps or 0.0), 0.0)
    slope = max(float(orderbook_slope or 0.0), 0.0)
    cancel = min(max(float(cancel_rate or 0.0), 0.0), 1.0)
    top_depth = max(float(top_depth_usdt or 0.0), 0.0)
    impact = max(float(estimated_impact_pct or 0.0), 0.0)

    # Legacy candidates produced before execution-quality telemetry should keep their original size.
    if fill_ratio <= 0 and slippage_r <= 0 and spread <= 0 and slope <= 0 and cancel <= 0 and top_depth <= 0 and impact <= 0:
        return 'A'

    # Fast path for Binance majors: tight spread plus enough top-of-book depth for a 15-30U order.
    # This prevents XRP/XLM-like books from being punished by ratio-only depth heuristics.
    if spread <= 2.5 and top_depth >= 150.0 and impact <= 0.08 and cancel < 0.35:
        if slippage_r <= 0.5:
            return 'A'
        if slippage_r <= 1.5:
            return 'B'

    penalty = 0
    severe_microstructure_penalties = 0
    if spread >= 12:
        severe_microstructure_penalties += 1
    if slope and slope < 0.25:
        severe_microstructure_penalties += 1
    if cancel >= 0.35:
        severe_microstructure_penalties += 1
    if severe_microstructure_penalties:
        penalty += severe_microstructure_penalties
    elif spread >= 8 or (slope and slope < 0.5) or cancel >= 0.2:
        penalty += 1
    if top_depth and top_depth < 50:
        penalty += 1
    if impact >= 0.25:
        penalty += 1

    grade_order = ['A+', 'A', 'B', 'C', 'D', 'E']
    if fill_ratio >= 0.85 and slippage_r <= 0.25:
        base_grade = 'A+'
    elif fill_ratio >= 0.75 and slippage_r <= 0.5:
        base_grade = 'A'
    elif fill_ratio >= 0.6 and slippage_r <= 0.15:
        base_grade = 'B'
    elif fill_ratio >= 0.45 and slippage_r <= 3.5:
        base_grade = 'C'
    elif fill_ratio >= 0.2 or top_depth >= 20:
        base_grade = 'D'
    else:
        base_grade = 'E'
    downgraded_index = min(grade_order.index(base_grade) + penalty, len(grade_order) - 1)
    return grade_order[downgraded_index]


def compute_expected_slippage_r(candidate: Candidate) -> float:
    risk_per_unit = abs(float(getattr(candidate, 'risk_per_unit', 0.0) or 0.0))
    if risk_per_unit <= 0:
        return 0.0
    expected_slippage_pct = max(float(getattr(candidate, 'expected_slippage_pct', 0.0) or 0.0), 0.0)
    last_price = abs(float(getattr(candidate, 'last_price', 0.0) or 0.0))
    stop_distance_pct = abs(float(getattr(candidate, 'stop_distance_pct', 0.0) or 0.0))
    if stop_distance_pct <= 0 and last_price > 0:
        stop_distance_pct = (risk_per_unit / last_price) * 100.0
    volatility_pct = abs(float(getattr(candidate, 'volatility_pct', 0.0) or getattr(candidate, 'atr_pct', 0.0) or 0.0))
    denominator_pct = max(stop_distance_pct, volatility_pct * 0.5, 0.08)
    return round(expected_slippage_pct / denominator_pct, 4)


def compute_execution_quality_size_adjustment(candidate: Candidate) -> Dict[str, Any]:
    execution_slippage_r = compute_expected_slippage_r(candidate)
    spread_bps = round(float(getattr(candidate, 'spread_bps', 0.0) or 0.0), 4)
    orderbook_slope = round(float(getattr(candidate, 'orderbook_slope', 0.0) or 0.0), 4)
    cancel_rate = round(float(getattr(candidate, 'cancel_rate', 0.0) or 0.0), 4)
    top_depth_usdt = round(float(getattr(candidate, 'top_depth_usdt', 0.0) or 0.0), 4)
    estimated_impact_pct = round(float(getattr(candidate, 'estimated_impact_pct', getattr(candidate, 'orderbook_impact_pct', 0.0)) or 0.0), 4)
    stop_distance_pct = round(float(getattr(candidate, 'stop_distance_pct', 0.0) or 0.0), 4)
    if stop_distance_pct <= 0 and float(getattr(candidate, 'last_price', 0.0) or 0.0) > 0:
        stop_distance_pct = round(abs(float(getattr(candidate, 'risk_per_unit', 0.0) or 0.0)) / float(getattr(candidate, 'last_price', 0.0) or 0.0) * 100.0, 4)
    volatility_pct = round(float(getattr(candidate, 'volatility_pct', getattr(candidate, 'atr_pct', 0.0)) or 0.0), 4)
    execution_liquidity_grade = classify_execution_liquidity_grade(
        getattr(candidate, 'book_depth_fill_ratio', 0.0),
        execution_slippage_r,
        spread_bps=spread_bps,
        orderbook_slope=orderbook_slope,
        cancel_rate=cancel_rate,
        top_depth_usdt=top_depth_usdt,
        estimated_impact_pct=estimated_impact_pct,
    )
    if execution_liquidity_grade in {'A+', 'A'}:
        multiplier = 1.0
        bucket = 'full'
    elif execution_liquidity_grade == 'B':
        multiplier = 0.65
        bucket = 'reduced'
    elif execution_liquidity_grade == 'C':
        multiplier = 0.35
        bucket = 'caution'
    elif execution_liquidity_grade == 'D':
        multiplier = 0.75
        bucket = 'maker_only'
    else:
        multiplier = 0.0
        bucket = 'veto'
    execution_mode = 'maker_only' if execution_liquidity_grade == 'D' else ('veto' if execution_liquidity_grade == 'E' else 'taker')
    grade_override = str(getattr(candidate, 'execution_liquidity_grade_override', '') or '').upper()
    mode_override = str(getattr(candidate, 'execution_mode_override', '') or '').lower()
    degradation_mode = str(getattr(candidate, 'execution_degradation_mode', '') or '').lower()
    if grade_override in {'A+', 'A', 'B', 'C', 'D', 'E'}:
        execution_liquidity_grade = grade_override
    if mode_override == 'maker_only' or degradation_mode in {'reduced_aggression', 'maker_only'}:
        execution_mode = 'maker_only'
        multiplier = min(multiplier, 0.5 if degradation_mode == 'maker_only' else 0.7) if multiplier > 0 else (0.5 if degradation_mode == 'maker_only' else 0.7)
        bucket = 'websocket_degraded_maker_only'
    if bool(getattr(candidate, 'pretrigger_watch', False)):
        execution_mode = 'maker_only'
        multiplier = min(multiplier, 0.35) if multiplier > 0 else 0.35
        bucket = 'pretrigger_maker_watch'
    return {
        'expected_slippage_r': execution_slippage_r,
        'execution_liquidity_grade': execution_liquidity_grade,
        'size_multiplier': multiplier,
        'size_bucket': bucket,
        'spread_bps': spread_bps,
        'orderbook_slope': orderbook_slope,
        'cancel_rate': cancel_rate,
        'top_depth_usdt': top_depth_usdt,
        'estimated_impact_pct': estimated_impact_pct,
        'absolute_slippage_bps': round(float(getattr(candidate, 'expected_slippage_pct', 0.0) or 0.0) * 100.0, 4),
        'stop_distance_pct': stop_distance_pct,
        'volatility_pct': volatility_pct,
        'liquidity_grade_reason': f"grade={execution_liquidity_grade} spread_bps={spread_bps} top_depth_usdt={top_depth_usdt} impact_pct={estimated_impact_pct} slippage_r={execution_slippage_r}",
        'execution_mode': execution_mode,
    }


def resolve_reject_reason(reasons: Sequence[str]) -> Dict[str, str]:
    reason_list = [str(reason) for reason in reasons if str(reason)]
    canonical_reason = ''
    if 'candidate_execution_slippage_risk' in reason_list:
        canonical_reason = 'execution_slippage_veto'
    elif 'candidate_execution_liquidity_poor' in reason_list:
        canonical_reason = 'execution_depth_veto'
    else:
        canonical_reason = reason_list[0] if reason_list else ''
    return {
        'reject_reason': canonical_reason,
        'reject_reason_label': REJECT_REASON_LABELS.get(canonical_reason, canonical_reason),
    }


def append_candidate_rejected_event(store: Optional[RuntimeStateStore], candidate: Candidate, reasons: Sequence[str], extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    execution_quality = compute_execution_quality_size_adjustment(candidate)
    reject_reason_payload = resolve_reject_reason(reasons)
    setup_missing = list(getattr(candidate, 'setup_missing', []) or [])
    trigger_missing = list(getattr(candidate, 'trigger_missing', []) or [])
    trade_missing = list(getattr(candidate, 'trade_missing', []) or [])
    expected_edge = round(float(getattr(candidate, 'expected_edge', 0.0) or 0.0), 4)
    expected_total_fee_pct = round(float(getattr(candidate, 'expected_total_fee_pct', 0.0) or 0.0), 4)
    execution_slippage_buffer_pct = round(float(getattr(candidate, 'execution_slippage_buffer_pct', 0.0) or 0.0), 4)
    min_profit_buffer_pct = round(float(getattr(candidate, 'min_profit_buffer_pct', 0.0) or 0.0), 4)
    fee_floor = expected_total_fee_pct + execution_slippage_buffer_pct + min_profit_buffer_pct
    if fee_floor > 0 and expected_edge < fee_floor:
        for reason in ('fee_trap', 'expected_edge_below_cost_floor'):
            if reason not in trade_missing:
                trade_missing.append(reason)
    payload = {
        'symbol': candidate.symbol,
        'symbol_quality_tier': str(getattr(candidate, 'symbol_quality_tier', '') or ''),
        'symbol_quality_reason': str(getattr(candidate, 'symbol_quality_reason', '') or ''),
        'symbol_tier_min_expected_net_profit_usdt': round(float(getattr(candidate, 'symbol_tier_min_expected_net_profit_usdt', 0.0) or 0.0), 4),
        'symbol_tier_min_rr': round(float(getattr(candidate, 'symbol_tier_min_rr', 0.0) or 0.0), 4),
        'symbol_tier_max_loss_usdt': round(float(getattr(candidate, 'symbol_tier_max_loss_usdt', 0.0) or 0.0), 4),
        'side': getattr(candidate, 'side', getattr(candidate, 'position_side', '')),
        'position_side': getattr(candidate, 'position_side', ''),
        'state': str(getattr(candidate, 'state', '') or ''),
        'alert_tier': str(getattr(candidate, 'alert_tier', '') or ''),
        'score': round(float(getattr(candidate, 'score', 0.0) or 0.0), 4),
        'reasons': list(reasons),
        **reject_reason_payload,
        'must_pass_flags': dict(getattr(candidate, 'must_pass_flags', {}) or {}),
        'quality_score': round(float(getattr(candidate, 'quality_score', 0.0) or 0.0), 4),
        'execution_priority_score': round(float(getattr(candidate, 'execution_priority_score', 0.0) or 0.0), 4),
        'entry_distance_from_breakout_pct': round(float(getattr(candidate, 'entry_distance_from_breakout_pct', 0.0) or 0.0), 4),
        'entry_distance_from_vwap_pct': round(float(getattr(candidate, 'entry_distance_from_vwap_pct', 0.0) or 0.0), 4),
        'stop_model': getattr(candidate, 'stop_model', 'structure'),
        'stop_distance_pct': round(float(getattr(candidate, 'stop_distance_pct', 0.0) or 0.0), 4),
        'stop_too_tight_flag': bool(getattr(candidate, 'stop_too_tight_flag', False)),
        'stop_too_wide_flag': bool(getattr(candidate, 'stop_too_wide_flag', False)),
        'candle_extension_pct': round(float(getattr(candidate, 'candle_extension_pct', 0.0) or 0.0), 4),
        'recent_3bar_runup_pct': round(float(getattr(candidate, 'recent_3bar_runup_pct', 0.0) or 0.0), 4),
        'overextension_flag': bool(getattr(candidate, 'overextension_flag', False)),
        'entry_pattern': str(getattr(candidate, 'entry_pattern', '') or ''),
        'trend_regime': str(getattr(candidate, 'trend_regime', '') or ''),
        'liquidity_grade': str(getattr(candidate, 'liquidity_grade', '') or ''),
        'setup_ready': bool(getattr(candidate, 'setup_ready', False)),
        'trigger_fired': bool(getattr(candidate, 'trigger_fired', False)),
        'candidate_stage': getattr(candidate, 'candidate_stage', 'watch_candidate'),
        'setup_missing': setup_missing,
        'trigger_missing': trigger_missing,
        'trade_missing': trade_missing,
        'trigger_confirmation_flags': dict(getattr(candidate, 'trigger_confirmation_flags', {}) or {}),
        'trigger_confirmation_count': int(getattr(candidate, 'trigger_confirmation_count', 0) or 0),
        'trigger_min_confirmations': int(getattr(candidate, 'trigger_min_confirmations', 0) or 0),
        'tradeability_score': round(float(getattr(candidate, 'tradeability_score', 0.0) or 0.0), 1),
        'expected_edge': expected_edge,
        'expected_total_fee_pct': expected_total_fee_pct,
        'execution_slippage_buffer_pct': execution_slippage_buffer_pct,
        'min_profit_buffer_pct': min_profit_buffer_pct,
        'shadow_entry_price': round(float(getattr(candidate, 'last_price', 0.0) or 0.0), 10),
        'planned_take_profit_price': round(float(getattr(candidate, 'planned_take_profit_price', 0.0) or 0.0), 10),
        'planned_stop_price': round(float(getattr(candidate, 'planned_stop_price', 0.0) or 0.0), 10),
        'planned_quantity': round(float(getattr(candidate, 'planned_quantity', 0.0) or 0.0), 10),
        'planned_notional_usdt': round(float(getattr(candidate, 'planned_notional_usdt', 0.0) or 0.0), 4),
        'expected_gross_profit_usdt': round(float(getattr(candidate, 'expected_gross_profit_usdt', 0.0) or 0.0), 4),
        'expected_net_profit_usdt': round(float(getattr(candidate, 'expected_net_profit_usdt', 0.0) or 0.0), 4),
        'expected_loss_usdt': round(float(getattr(candidate, 'expected_loss_usdt', 0.0) or 0.0), 4),
        'expected_rr': round(float(getattr(candidate, 'expected_rr', 0.0) or 0.0), 4),
        'portfolio_narrative_bucket': str(getattr(candidate, 'portfolio_narrative_bucket', '') or ''),
        'portfolio_correlation_group': str(getattr(candidate, 'portfolio_correlation_group', '') or ''),
        'expected_slippage_pct': round(float(candidate.expected_slippage_pct or 0.0), 4),
        'expected_slippage_r': execution_quality['expected_slippage_r'],
        'book_depth_fill_ratio': round(float(candidate.book_depth_fill_ratio or 0.0), 4),
        'cvd_delta': round(float(getattr(candidate, 'cvd_delta', 0.0) or 0.0), 4),
        'cvd_zscore': round(float(getattr(candidate, 'cvd_zscore', 0.0) or 0.0), 4),
        'oi_change_pct_5m': round(float(getattr(candidate, 'oi_change_pct_5m', 0.0) or 0.0), 4),
        'oi_change_pct_15m': round(float(getattr(candidate, 'oi_change_pct_15m', 0.0) or 0.0), 4),
        'execution_liquidity_grade': execution_quality['execution_liquidity_grade'],
        'execution_quality_size_multiplier': execution_quality['size_multiplier'],
        'execution_quality_size_bucket': execution_quality['size_bucket'],
        'spread_bps': execution_quality['spread_bps'],
        'orderbook_slope': execution_quality['orderbook_slope'],
        'cancel_rate': execution_quality['cancel_rate'],
    }
    if extra:
        payload.update(extra)
    return append_runtime_event(store, 'candidate_rejected', payload)


def persist_economic_shadow_candidates(
    store: RuntimeStateStore,
    candidates: Sequence[Dict[str, Any]],
    *,
    observed_at: Optional[str] = None,
    max_rows: int = 200,
) -> List[Dict[str, Any]]:
    """Persist triggered economic rejections without resetting their maturity clock."""
    timestamp = observed_at or datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
    loaded = store.load_json('economic_shadow_candidates', {'rows': []})
    existing_rows = loaded.get('rows', []) if isinstance(loaded, dict) else []
    rows = [dict(item) for item in existing_rows if isinstance(item, dict)]
    # Completed observations are history, not permanent dedupe keys.
    by_key = {
        (str(item.get('symbol') or ''), normalize_position_side(item.get('side'))): item
        for item in rows
        if str(item.get('symbol') or '') and item.get('status', 'pending') == 'pending'
    }
    for candidate in candidates:
        if not isinstance(candidate, dict) or not bool(candidate.get('shadow_eligible')):
            continue
        symbol = normalize_symbol(candidate.get('symbol'))
        side = normalize_position_side(candidate.get('side') or candidate.get('position_side'))
        if not symbol:
            continue
        key = (symbol, side)
        existing = by_key.get(key)
        if existing is not None:
            existing['last_seen_at'] = timestamp
            existing['seen_count'] = int(existing.get('seen_count', 1) or 1) + 1
            continue
        row = dict(candidate)
        row.update({
            'symbol': symbol,
            'side': side,
            'captured_at': timestamp,
            'last_seen_at': timestamp,
            'seen_count': 1,
            'status': 'pending',
        })
        rows.append(row)
        by_key[key] = row
    # Pending observations must survive retention so they can eventually mature.
    # ``max_rows`` caps completed history only; the pending set may temporarily exceed it.
    pending_rows = [row for row in rows if row.get('status', 'pending') == 'pending']
    completed_rows = [row for row in rows if row.get('status', 'pending') != 'pending']
    rows = completed_rows[-max(1, int(max_rows or 200)):] + pending_rows
    store.save_json('economic_shadow_candidates', {'updated_at': timestamp, 'rows': rows})
    return rows


def _economic_shadow_timestamp(value: Any) -> Optional[datetime.datetime]:
    try:
        result = datetime.datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return result if result.tzinfo else result.replace(tzinfo=datetime.timezone.utc)
    except (TypeError, ValueError):
        return None


def evaluate_economic_shadow_candidates(store, client, *, args=None, evaluated_at=None):
    """Mature rejects using closed 1m candles and scanner REST safety policy.

    A horizon is observed at the first candle close at or after its target. Outcome
    percentages are round-trip PnL percentages normalized to entry notional.
    """
    timestamp = evaluated_at or datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
    now = _economic_shadow_timestamp(timestamp)
    loaded = store.load_json('economic_shadow_candidates', {'rows': []})
    rows = [dict(r) for r in (loaded.get('rows', []) if isinstance(loaded, dict) else []) if isinstance(r, dict)]
    if now is None or client is None:
        return rows
    due = {}
    for row in rows:
        captured = _economic_shadow_timestamp(row.get('captured_at'))
        symbol = normalize_symbol(row.get('symbol'))
        horizon_due = any(
            now >= captured + datetime.timedelta(minutes=minutes)
            and f'directional_return_pct_{minutes}m' not in row
            for minutes in (15, 30, 45)
        ) if captured else False
        if row.get('status', 'pending') == 'pending' and captured and symbol and horizon_due:
            due.setdefault(symbol, []).append((row, captured))
    if due and args is not None:
        decision = scanner_ticker_rest_fallback_decision(
            store, args, {'fresh': False, 'reason': 'economic_shadow_observation'}
        )
        if not decision.get('allowed'):
            for items in due.values():
                for row, _ in items:
                    row['evaluation_deferred_reason'] = str(decision.get('reason') or 'scanner_rest_guard')
            store.save_json('economic_shadow_candidates', {'updated_at': timestamp, 'rows': rows})
            return rows
    for symbol, items in due.items():
        start = min(c for _, c in items)
        end = min(now, max(c + datetime.timedelta(minutes=45) for _, c in items))
        start = start.replace(second=0, microsecond=0)
        try:
            candles = client.get('/fapi/v1/klines', params={'symbol': symbol, 'interval': '1m',
                'startTime': int(start.timestamp() * 1000), 'endTime': int(end.timestamp() * 1000),
                'limit': 1000}, purpose='market_data') or []
        except Exception as exc:
            for row, _ in items:
                row['evaluation_error'] = f'{type(exc).__name__}: {exc}'
            continue
        candles = [c for c in candles if isinstance(c, (list, tuple)) and len(c) >= 7 and int(c[6]) <= int(now.timestamp() * 1000)]
        for row, captured in items:
            entry = _coerce_optional_float(row.get('shadow_entry_price')) or 0.0
            if entry <= 0:
                row['evaluation_error'] = 'missing_shadow_entry_price'
                continue
            capture_ms = int(captured.timestamp() * 1000)
            horizon_ms = int((captured + datetime.timedelta(minutes=45)).timestamp() * 1000)
            relevant = [c for c in candles if int(c[6]) > capture_ms and int(c[0]) <= horizon_ms]
            if not relevant:
                continue
            direction = 1.0 if normalize_position_side(row.get('side')) == POSITION_SIDE_LONG else -1.0
            for minutes in (15, 30, 45):
                target = int((captured + datetime.timedelta(minutes=minutes)).timestamp() * 1000)
                eligible = [c for c in relevant if int(c[6]) >= target]
                if now >= captured + datetime.timedelta(minutes=minutes) and eligible:
                    row[f'directional_return_pct_{minutes}m'] = round(direction * (float(eligible[0][4]) - entry) / entry * 100, 6)
            favorable = max(float(c[2]) for c in relevant) if direction > 0 else min(float(c[3]) for c in relevant)
            adverse = min(float(c[3]) for c in relevant) if direction > 0 else max(float(c[2]) for c in relevant)
            row['mfe_pct'] = round(direction * (favorable - entry) / entry * 100, 6)
            row['mae_pct'] = round(direction * (adverse - entry) / entry * 100, 6)
            tp = _coerce_optional_float(row.get('planned_take_profit_price')) or 0.0
            stop = _coerce_optional_float(row.get('planned_stop_price')) or 0.0
            exit_price, reason = 0.0, ''
            for candle in relevant:
                high, low = float(candle[2]), float(candle[3])
                tp_hit = tp > 0 and (high >= tp if direction > 0 else low <= tp)
                stop_hit = stop > 0 and (low <= stop if direction > 0 else high >= stop)
                if tp_hit or stop_hit:
                    exit_price, reason = ((stop, 'ambiguous_stop_first' if tp_hit else 'stop_loss') if stop_hit else (tp, 'take_profit'))
                    break
            complete = now >= captured + datetime.timedelta(minutes=45) and any(int(c[6]) >= horizon_ms for c in relevant)
            if complete and not reason:
                exit_price, reason = float(relevant[-1][4]), 'horizon_45m'
            if reason:
                row.update(exit_price=round(exit_price, 10), exit_reason=reason)
            quantity = _coerce_optional_float(row.get('planned_quantity')) or 0.0
            row['sizing_diagnostics'] = [] if quantity > 0 else ['planned_quantity']
            if reason and quantity > 0:
                gross = direction * (exit_price - entry) * quantity
                costs = ((_coerce_optional_float(row.get('expected_total_fee_pct')) or 0.0) + (_coerce_optional_float(row.get('execution_slippage_buffer_pct')) or 0.0))
                row['modeled_gross_pnl_usdt'] = round(gross, 6)
                row['modeled_net_pnl_usdt'] = round(gross - entry * quantity * costs / 100, 6)
                entry_notional = entry * quantity
                row['modeled_gross_pnl_pct'] = round(gross / entry_notional * 100, 6)
                row['modeled_net_pnl_pct'] = round(row['modeled_net_pnl_usdt'] / entry_notional * 100, 6)
            if complete:
                row.update(status='completed', completed_at=timestamp)
            row['last_evaluated_at'] = timestamp
    store.save_json('economic_shadow_candidates', {'updated_at': timestamp, 'rows': rows})
    return rows


def append_missed_trade_event(
    store: Optional['RuntimeStateStore'],
    candidate: 'Candidate',
    reasons: Sequence[str],
    probe_eligible: bool = False,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Log a missed_trade event when a high-scoring candidate was blocked before execution.

    This captures opportunity cost: candidates that passed the scoring gate but were
    rejected due to soft vetoes (waiting_breakout, setup_not_ready, trigger_not_fired)
    or probe_entry not being enabled.
    """
    setup_missing = list(getattr(candidate, 'setup_missing', []) or [])
    trigger_missing = list(getattr(candidate, 'trigger_missing', []) or [])
    is_waiting_breakout = 'waiting_breakout' in setup_missing or 'waiting_breakout' in trigger_missing

    if not is_waiting_breakout:
        return {}

    payload = {
        'symbol': candidate.symbol,
        'side': getattr(candidate, 'side', getattr(candidate, 'position_side', '')),
        'position_side': getattr(candidate, 'position_side', ''),
        'score': round(float(candidate.score or 0.0), 4),
        'tradeability_score': round(float(getattr(candidate, 'tradeability_score', 0.0) or 0.0), 4),
        'state': candidate.state,
        'alert_tier': candidate.alert_tier,
        'setup_ready': bool(candidate.setup_ready),
        'trigger_fired': bool(candidate.trigger_fired),
        'candidate_stage': getattr(candidate, 'candidate_stage', 'watch_candidate'),
        'setup_missing': setup_missing,
        'trigger_missing': trigger_missing,
        'trade_missing': list(getattr(candidate, 'trade_missing', []) or []),
        'missed_reasons': list(reasons),
        'probe_entry_eligible': probe_eligible,
        'entry_distance_from_breakout_pct': round(float(candidate.entry_distance_from_breakout_pct or 0.0), 4),
        'recent_5m_change_pct': round(float(getattr(candidate, 'recent_5m_change_pct', 0.0) or 0.0), 4),
        'acceleration_ratio_5m_vs_15m': round(float(getattr(candidate, 'acceleration_ratio_5m_vs_15m', 0.0) or 0.0), 4),
        'liquidity_grade': candidate.liquidity_grade,
        'trend_regime': candidate.trend_regime,
        'entry_pattern': candidate.entry_pattern,
        'high_vol_alt_mode': bool(getattr(candidate, 'high_vol_alt_mode', False)),
        'oi_change_pct_5m': round(float(getattr(candidate, 'oi_change_pct_5m', 0.0) or 0.0), 4),
        'cvd_delta': round(float(getattr(candidate, 'cvd_delta', 0.0) or 0.0), 4),
    }
    if extra:
        payload.update(extra)
    return append_runtime_event(store, 'missed_trade', payload)


def build_candidate_selected_event_payload(
    candidate: Candidate,
    regime_payload: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    symbol = str(getattr(candidate, 'symbol', '') or '')
    side = normalize_position_side(getattr(candidate, 'side', getattr(candidate, 'position_side', POSITION_SIDE_LONG)))
    try:
        payload = build_standardized_alert(candidate, regime_payload)
    except Exception:
        payload = {
            'symbol': symbol,
            'score': round(float(getattr(candidate, 'score', 0.0) or 0.0), 2),
            'state': str(getattr(candidate, 'state', '') or ''),
            'alert_tier': str(getattr(candidate, 'alert_tier', '') or ''),
            'position_size_pct': round(float(getattr(candidate, 'position_size_pct', 0.0) or 0.0), 4),
            'portfolio_narrative_bucket': str(getattr(candidate, 'portfolio_narrative_bucket', '') or ''),
            'portfolio_correlation_group': str(getattr(candidate, 'portfolio_correlation_group', '') or ''),
            'setup_ready': bool(getattr(candidate, 'setup_ready', False)),
            'trigger_fired': bool(getattr(candidate, 'trigger_fired', False)),
            'candidate_stage': str(getattr(candidate, 'candidate_stage', '') or ''),
            'expected_slippage_pct': round(float(getattr(candidate, 'expected_slippage_pct', 0.0) or 0.0), 4),
            'book_depth_fill_ratio': round(float(getattr(candidate, 'book_depth_fill_ratio', 0.0) or 0.0), 4),
            'execution_liquidity_grade': str(getattr(candidate, 'liquidity_grade', '') or ''),
            'market_regime_label': str((regime_payload or {}).get('label', getattr(candidate, 'regime_label', '')) or ''),
            'market_regime_multiplier': round(float((regime_payload or {}).get('score_multiplier', getattr(candidate, 'regime_multiplier', 0.0)) or 0.0), 4),
            'market_regime_reasons': list((regime_payload or {}).get('reasons', [])),
            'market_direction_bias': str(getattr(candidate, 'market_direction_bias', 'NEUTRAL') or 'NEUTRAL'),
            'market_direction_bias_strength': round(float(getattr(candidate, 'market_direction_bias_strength', 0.0) or 0.0), 4),
            'market_direction_score_multiplier': round(float(getattr(candidate, 'market_direction_score_multiplier', 1.0) or 1.0), 4),
            'market_breadth_ratio': round(float(getattr(candidate, 'market_breadth_ratio', 0.5) or 0.5), 4),
            'reasons': list(getattr(candidate, 'reasons', []) or []),
            'state_reasons': list(getattr(candidate, 'state_reasons', []) or []),
        }
    payload.update({
        'side': side,
        'position_side': side,
        'position_key': build_position_key(symbol, side),
        'entry_price': round(float(getattr(candidate, 'entry_price', getattr(candidate, 'last_price', 0.0)) or 0.0), 10),
        'last_price': round(float(getattr(candidate, 'last_price', 0.0) or 0.0), 10),
        'stop_price': round(float(getattr(candidate, 'stop_price', 0.0) or 0.0), 10),
        'quantity': round(float(getattr(candidate, 'quantity', 0.0) or 0.0), 10),
        'recommended_leverage': int(getattr(candidate, 'recommended_leverage', 0) or 0),
        'trigger_class': resolve_trigger_class(candidate),
        'score_decile': score_to_decile_label(getattr(candidate, 'score', 0.0)),
        'position_tier': str(getattr(candidate, 'position_tier', '') or ''),
        'target_notional_usdt': round(float(getattr(candidate, 'target_notional_usdt', 0.0) or 0.0), 4),
        'planned_notional': round(float(getattr(candidate, 'planned_notional_usdt', 0.0) or 0.0), 4),
        'expected_profit_at_tp1': round(float(getattr(candidate, 'expected_profit_at_tp1', 0.0) or 0.0), 4),
        'expected_profit_at_tp2': round(float(getattr(candidate, 'expected_profit_at_tp2', 0.0) or 0.0), 4),
        'expected_profit_at_tp3': round(float(getattr(candidate, 'expected_profit_at_tp3', 0.0) or 0.0), 4),
        'max_loss_to_stop': round(float(getattr(candidate, 'max_loss_to_stop', 0.0) or 0.0), 4),
        'risk_reward_ratio': round(float(getattr(candidate, 'risk_reward_ratio', 0.0) or 0.0), 4),
        'submit_block_reason': str(getattr(candidate, 'submit_block_reason', '') or ''),
    })
    if extra:
        payload.update(extra)
    return payload


def append_candidate_selected_event(
    store: Optional[RuntimeStateStore],
    candidate: Candidate,
    regime_payload: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return append_runtime_event(
        store,
        'candidate_selected',
        build_candidate_selected_event_payload(candidate, regime_payload=regime_payload, extra=extra),
    )


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def classify_five_usdt_position_tier(candidate: Candidate, args: argparse.Namespace, account_balance_usdt: float = 0.0) -> Dict[str, Any]:
    grade = str(getattr(candidate, 'candidate_pool_grade', '') or getattr(candidate, 'alert_tier', '') or '').upper()
    score = _to_float(getattr(candidate, 'score', 0.0), 0.0)
    execution_grade = str(getattr(candidate, 'execution_liquidity_grade_override', '') or getattr(candidate, 'liquidity_grade', '') or '').upper()
    liquidity_grade = str(getattr(candidate, 'liquidity_grade', '') or '').upper()
    expected_edge_pct = _to_float(getattr(candidate, 'expected_edge_pct', 0.0), 0.0) or _to_float(getattr(candidate, 'expected_edge', 0.0), 0.0)
    trade_block_reasons: List[str] = []
    if grade != 'A':
        trade_block_reasons.append('trade_tier_requires_a_grade')
    if score < 78.0:
        trade_block_reasons.append('trade_tier_requires_score_78')
    if not bool(getattr(candidate, 'trigger_fired', False)):
        trade_block_reasons.append('trigger_not_fired')
    if int(getattr(candidate, 'trigger_confirmation_count', 0) or 0) < 2:
        trade_block_reasons.append('trigger_confirmation_count_below_2')
    if list(getattr(candidate, 'trade_missing', []) or []):
        trade_block_reasons.append('trade_missing')
    if list(getattr(candidate, 'setup_missing', []) or []):
        trade_block_reasons.append('setup_missing')
    if liquidity_grade < 'A':
        trade_block_reasons.append('liquidity_below_a')
    if execution_grade < 'A':
        trade_block_reasons.append('execution_liquidity_below_a')
    if _to_float(getattr(candidate, 'spread_bps', 999.0), 999.0) > 3.0:
        trade_block_reasons.append('spread_bps_above_3')
    if _to_float(getattr(candidate, 'book_depth_fill_ratio', 0.0), 0.0) < 0.85:
        trade_block_reasons.append('book_depth_fill_ratio_below_0_85')
    if expected_edge_pct < 1.2:
        trade_block_reasons.append('expected_edge_pct_below_1_2')

    max_trade_notional = _to_float(getattr(args, 'max_notional_usdt', 90.0), 90.0)
    if account_balance_usdt > 0:
        max_trade_notional = min(max_trade_notional, account_balance_usdt * 1.1)
    target_trade_notional = min(_to_float(getattr(args, 'target_notional_usdt', 80.0), 80.0), max_trade_notional)
    target_trade_notional = max(min(target_trade_notional, max_trade_notional), _to_float(getattr(args, 'min_notional_usdt', 60.0), 60.0))
    if trade_block_reasons:
        planned_notional = min(_to_float(getattr(args, 'probe_max_notional_usdt', 30.0), 30.0), max(_to_float(getattr(args, 'probe_min_notional_usdt', 20.0), 20.0), 25.0))
        tier = 'PROBE'
        block_reason = trade_block_reasons[0]
    else:
        planned_notional = target_trade_notional
        tier = 'TRADE'
        block_reason = ''

    tp1_pct = _to_float(getattr(args, 'take_profit_pct', 0.012), 0.012)
    tp2_pct = _to_float(getattr(args, 'tp2_r', 2.5), 2.5) / 100.0
    tp3_pct = _to_float(getattr(args, 'tp3_r', 4.0), 4.0) / 100.0
    stop_pct = _to_float(getattr(args, 'stop_loss_pct', 0.01), 0.01)
    expected_profit_at_tp1 = planned_notional * tp1_pct
    expected_profit_at_tp2 = planned_notional * tp2_pct
    expected_profit_at_tp3 = planned_notional * tp3_pct
    max_loss_to_stop = planned_notional * stop_pct
    risk_reward_ratio = expected_profit_at_tp3 / max_loss_to_stop if max_loss_to_stop > 0 else 0.0
    plan = {
        'position_tier': tier,
        'target_notional_usdt': round(target_trade_notional if tier == 'TRADE' else planned_notional, 4),
        'planned_notional': round(planned_notional, 4),
        'max_allowed_notional_usdt': round(max_trade_notional, 4),
        'expected_profit_at_tp1': round(expected_profit_at_tp1, 4),
        'expected_profit_at_tp2': round(expected_profit_at_tp2, 4),
        'expected_profit_at_tp3': round(expected_profit_at_tp3, 4),
        'max_loss_to_stop': round(max_loss_to_stop, 4),
        'risk_reward_ratio': round(risk_reward_ratio, 4),
        'submit_block_reason': block_reason,
    }
    candidate.position_tier = plan['position_tier']
    candidate.target_notional_usdt = plan['target_notional_usdt']
    candidate.planned_notional_usdt = plan['planned_notional']
    candidate.expected_profit_at_tp1 = plan['expected_profit_at_tp1']
    candidate.expected_profit_at_tp2 = plan['expected_profit_at_tp2']
    candidate.expected_profit_at_tp3 = plan['expected_profit_at_tp3']
    candidate.max_loss_to_stop = plan['max_loss_to_stop']
    candidate.risk_reward_ratio = plan['risk_reward_ratio']
    candidate.submit_block_reason = plan['submit_block_reason']
    return plan


def detect_and_cancel_orphan_entry_orders(
    client: Any,
    open_orders: Sequence[Dict[str, Any]],
    active_entry_client_order_ids: Optional[Set[str]] = None,
    emit_event: Optional[Callable[[str, Dict[str, Any]], Any]] = None,
    cancel_order_fn: Optional[Callable[[Any, Dict[str, Any]], Any]] = None,
    auto_cancel: bool = True,
) -> Dict[str, int]:
    active_ids = set(active_entry_client_order_ids or set())
    detected = 0
    cancelled = 0
    for order in open_orders or []:
        if not isinstance(order, dict):
            continue
        reduce_only = str(order.get('reduceOnly', '')).lower() == 'true' or order.get('reduceOnly') is True
        order_type = str(order.get('type', '') or '').upper()
        client_order_id = str(order.get('clientOrderId', '') or '')
        if reduce_only or order_type not in {'LIMIT', 'MARKET'} or client_order_id in active_ids:
            continue
        detected += 1
        payload = {
            'symbol': str(order.get('symbol', '') or ''),
            'orderId': order.get('orderId'),
            'clientOrderId': client_order_id,
            'side': str(order.get('side', '') or ''),
            'type': order_type,
            'reduceOnly': False,
        }
        if emit_event:
            emit_event('orphan_entry_order_detected', payload)
        if auto_cancel and cancel_order_fn:
            cancel_order_fn(client, order)
            cancelled += 1
    return {'orphan_entry_order_detected': detected, 'orphan_entry_order_cancelled': cancelled}


def _read_source_value(source: Any, key: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(key, default)
    return getattr(source, key, default)


def score_to_decile_label(score: Any) -> str:
    numeric = max(0.0, min(_to_float(score, default=0.0), 100.0))
    if numeric >= 90.0:
        return '90-100'
    lower = int(numeric // 10) * 10
    upper = lower + 9
    return f'{lower}-{upper}'


def resolve_trigger_class(source: Any) -> str:
    for key in ('trigger_class', 'trigger_type', 'entry_pattern', 'candidate_stage', 'state'):
        value = str(_read_source_value(source, key, '') or '').strip()
        if value:
            return value
    return 'unknown'


def resolve_reduce_order_exit_price(reduce_order: Any, current_price: float) -> float:
    if isinstance(reduce_order, dict):
        avg_price = _to_float(
            reduce_order.get('avgPrice')
            or reduce_order.get('avg_price')
            or reduce_order.get('fillPx')
            or reduce_order.get('fill_price')
            or reduce_order.get('px'),
            default=0.0,
        )
        if avg_price > 0:
            return avg_price
        executed_qty = _to_float(
            reduce_order.get('executedQty')
            or reduce_order.get('cumQty')
            or reduce_order.get('accFillSz')
            or reduce_order.get('fillSz'),
            default=0.0,
        )
        cum_quote = _to_float(
            reduce_order.get('cumQuote')
            or reduce_order.get('fillNotionalUsd')
            or reduce_order.get('fillNotional'),
            default=0.0,
        )
        if executed_qty > 0 and cum_quote > 0:
            return cum_quote / executed_qty
    return _to_float(current_price, default=0.0)


def compute_trade_realized_r_increment(
    entry_price: float,
    exit_price: float,
    initial_risk_per_unit: float,
    close_qty: float,
    initial_quantity: float,
    side: str,
) -> float:
    risk_per_unit = abs(_to_float(initial_risk_per_unit, default=0.0))
    total_quantity = abs(_to_float(initial_quantity, default=0.0))
    realized_close_qty = abs(_to_float(close_qty, default=0.0))
    if risk_per_unit <= 0 or total_quantity <= 0 or realized_close_qty <= 0:
        return 0.0
    if normalize_position_side(side) == POSITION_SIDE_SHORT:
        gross_r = (_to_float(entry_price, default=0.0) - _to_float(exit_price, default=0.0)) / risk_per_unit
    else:
        gross_r = (_to_float(exit_price, default=0.0) - _to_float(entry_price, default=0.0)) / risk_per_unit
    return gross_r * min(realized_close_qty / total_quantity, 1.0)


def compute_trade_mfe_mae_r(
    entry_price: float,
    initial_risk_per_unit: float,
    highest_price_seen: Optional[float],
    lowest_price_seen: Optional[float],
    side: str,
) -> Tuple[float, float]:
    risk_per_unit = abs(_to_float(initial_risk_per_unit, default=0.0))
    entry = _to_float(entry_price, default=0.0)
    if risk_per_unit <= 0 or entry <= 0:
        return 0.0, 0.0
    highest = _to_float(highest_price_seen, default=entry)
    lowest = _to_float(lowest_price_seen, default=entry)
    if normalize_position_side(side) == POSITION_SIDE_SHORT:
        mfe_r = max(entry - lowest, 0.0) / risk_per_unit
        mae_r = max(highest - entry, 0.0) / risk_per_unit
    else:
        mfe_r = max(highest - entry, 0.0) / risk_per_unit
        mae_r = max(entry - lowest, 0.0) / risk_per_unit
    return round(mfe_r, 4), round(mae_r, 4)


def update_trade_progress_metrics(
    state: TradeManagementState,
    plan: TradeManagementPlan,
    current_price: float,
    observed_at: Optional[datetime.datetime] = None,
) -> None:
    observed_at = observed_at or _utc_now()
    if not state.opened_at:
        state.opened_at = _isoformat_utc(observed_at)
    risk_per_unit = abs(_to_float(plan.initial_risk_per_unit, default=0.0))
    if risk_per_unit <= 0 or state.first_1r_at:
        return
    if normalize_position_side(state.position_side) == POSITION_SIDE_SHORT:
        favorable_move = _to_float(plan.entry_price, default=0.0) - _to_float(state.lowest_price_seen, default=current_price)
    else:
        favorable_move = _to_float(state.highest_price_seen, default=current_price) - _to_float(plan.entry_price, default=0.0)
    if favorable_move >= risk_per_unit:
        state.first_1r_at = _isoformat_utc(observed_at)


def build_trade_analytics_snapshot(
    state: TradeManagementState,
    plan: TradeManagementPlan,
    closed_at: Optional[datetime.datetime] = None,
) -> Dict[str, Any]:
    opened_at_dt = _parse_iso8601_utc(state.opened_at)
    first_1r_at_dt = _parse_iso8601_utc(state.first_1r_at)
    effective_closed_at = closed_at or _utc_now()
    mfe_r, mae_r = compute_trade_mfe_mae_r(
        entry_price=plan.entry_price,
        initial_risk_per_unit=plan.initial_risk_per_unit,
        highest_price_seen=state.highest_price_seen,
        lowest_price_seen=state.lowest_price_seen,
        side=state.position_side,
    )
    time_to_1r_minutes = None
    if opened_at_dt is not None and first_1r_at_dt is not None:
        time_to_1r_minutes = round(max((first_1r_at_dt - opened_at_dt).total_seconds(), 0.0) / 60.0, 4)
    time_in_trade_minutes = None
    if opened_at_dt is not None:
        time_in_trade_minutes = round(max((effective_closed_at - opened_at_dt).total_seconds(), 0.0) / 60.0, 4)
    realized_r = round(_to_float(state.realized_r, default=0.0), 4)
    stop_loss_hit = False
    if state.position_side == POSITION_SIDE_SHORT:
        stop_loss_hit = state.highest_price_seen is not None and _to_float(state.highest_price_seen) >= _to_float(plan.stop_price)
    else:
        stop_loss_hit = state.lowest_price_seen is not None and _to_float(state.lowest_price_seen) <= _to_float(plan.stop_price)
    initial_stop_distance_pct = 0.0
    if _to_float(plan.entry_price) > 0:
        initial_stop_distance_pct = abs(_to_float(plan.entry_price) - _to_float(plan.stop_price)) / _to_float(plan.entry_price) * 100.0
    exit_efficiency_r = round(realized_r - _to_float(mfe_r), 4) if mfe_r is not None else None
    return {
        'opened_at': state.opened_at,
        'first_1r_at': state.first_1r_at,
        'closed_at': _isoformat_utc(effective_closed_at) if closed_at is not None else None,
        'mfe_r': mfe_r,
        'mae_r': mae_r,
        'max_favorable_excursion_r': mfe_r,
        'max_adverse_excursion_r': mae_r,
        'time_to_1r': time_to_1r_minutes,
        'time_to_1r_minutes': time_to_1r_minutes,
        'time_in_trade_minutes': time_in_trade_minutes,
        'realized_r': realized_r,
        'hard_stop_price': _to_float(plan.stop_price),
        'stop_loss_hit': stop_loss_hit,
        'exit_efficiency_r': exit_efficiency_r,
        'initial_stop_distance_pct': round(initial_stop_distance_pct, 4),
    }


def is_accumulation_external_signal(signal_payload: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(signal_payload, dict) or not signal_payload:
        return False
    texts: List[str] = []
    for key in ('portfolio_narrative_bucket', 'portfolio_correlation_group', 'external_signal_tier'):
        value = signal_payload.get(key)
        if value is not None:
            texts.append(str(value).lower())
    for reason in signal_payload.get('external_reasons', []) or []:
        texts.append(str(reason).lower())
    joined = ' '.join(texts)
    return 'accumulation' in joined or 'accumulating' in joined or 'volume_warming' in joined or 'volume_breakout' in joined


def derive_external_setup_params(signal_payload: Optional[Dict[str, Any]], enabled: bool = False) -> Dict[str, Any]:
    if not enabled or not is_accumulation_external_signal(signal_payload):
        return {'enabled': False}
    score = _to_float((signal_payload or {}).get('external_signal_score'), default=0.0)
    if score >= 85:
        max_breakout_distance_pct = 2.5
        min_5m_change_pct_multiplier = 0.35
        min_volume_multiple_multiplier = 0.45
    elif score >= 75:
        max_breakout_distance_pct = 1.5
        min_5m_change_pct_multiplier = 0.5
        min_volume_multiple_multiplier = 0.6
    else:
        max_breakout_distance_pct = 0.75
        min_5m_change_pct_multiplier = 0.7
        min_volume_multiple_multiplier = 0.75
    return {
        'enabled': True,
        'score': score,
        'max_breakout_distance_pct': max_breakout_distance_pct,
        'min_5m_change_pct_multiplier': min_5m_change_pct_multiplier,
        'min_volume_multiple_multiplier': min_volume_multiple_multiplier,
        'min_quote_volume': 1_000_000.0,
    }


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _stdev(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    return statistics.pstdev(values)


def compute_zscore(value: float, samples: Sequence[float]) -> float:
    samples = [float(sample) for sample in samples if sample is not None]
    if not samples:
        return 0.0
    mu = _mean(samples)
    sigma = _stdev(samples)
    if sigma <= 0:
        return 0.0
    return (float(value) - mu) / sigma


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def round_step(value: float, step: float, precision: int) -> float:
    if step <= 0:
        return round(value, precision)
    steps = math.floor(value / step + 1e-12)
    return round(steps * step, precision)


def round_price(value: float, tick_size: float, precision: int) -> float:
    return round_step(value, tick_size, precision)


def format_decimal(value: float, precision: int) -> str:
    return f'{float(value):.{precision}f}'


def extract_closes(klines: Sequence[Sequence[Any]]) -> List[float]:
    return [_to_float(k[4]) for k in klines]


def extract_highs(klines: Sequence[Sequence[Any]]) -> List[float]:
    return [_to_float(k[2]) for k in klines]


def extract_lows(klines: Sequence[Sequence[Any]]) -> List[float]:
    return [_to_float(k[3]) for k in klines]


def extract_volumes(klines: Sequence[Sequence[Any]]) -> List[float]:
    return [_to_float(k[5]) for k in klines]


def compute_ema(values: Sequence[float], period: int = 20) -> float:
    if not values:
        return 0.0
    alpha = 2 / (period + 1)
    ema = float(values[0])
    for value in values[1:]:
        ema = alpha * float(value) + (1 - alpha) * ema
    return ema


def compute_rsi(closes: Sequence[float], period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0
    gains, losses = [], []
    for prev, cur in zip(closes[:-1], closes[1:]):
        delta = float(cur) - float(prev)
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))
    avg_gain = _mean(gains[-period:])
    avg_loss = _mean(losses[-period:])
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_macd(closes: Sequence[float]) -> Dict[str, float]:
    if not closes:
        return {'macd': 0.0, 'signal': 0.0, 'hist': 0.0, 'prev_hist': 0.0, 'price': 0.0}
    ema12_series, ema26_series = [], []
    ema12 = float(closes[0])
    ema26 = float(closes[0])
    alpha12 = 2 / 13
    alpha26 = 2 / 27
    for close in closes:
        close = float(close)
        ema12 = alpha12 * close + (1 - alpha12) * ema12
        ema26 = alpha26 * close + (1 - alpha26) * ema26
        ema12_series.append(ema12)
        ema26_series.append(ema26)
    macd_series = [a - b for a, b in zip(ema12_series, ema26_series)]
    signal = compute_ema(macd_series, period=9)
    hist = macd_series[-1] - signal
    prev_signal = compute_ema(macd_series[:-1], period=9) if len(macd_series) > 1 else signal
    prev_hist = macd_series[-2] - prev_signal if len(macd_series) > 1 else hist
    return {'macd': macd_series[-1], 'signal': signal, 'hist': hist, 'prev_hist': prev_hist, 'price': float(closes[-1])}


def compute_vwap(klines: Sequence[Sequence[Any]]) -> float:
    total_pv = 0.0
    total_volume = 0.0
    for kline in klines:
        high = _to_float(kline[2])
        low = _to_float(kline[3])
        close = _to_float(kline[4])
        volume = _to_float(kline[5])
        typical = (high + low + close) / 3 if volume else close
        total_pv += typical * volume
        total_volume += volume
    if total_volume == 0:
        return _to_float(klines[-1][4]) if klines else 0.0
    return total_pv / total_volume


def compute_atr(klines: Sequence[Sequence[Any]], period: int = 14) -> float:
    if len(klines) < 2:
        return 0.0
    trs = []
    prev_close = _to_float(klines[0][4])
    for kline in klines[1:]:
        high = _to_float(kline[2])
        low = _to_float(kline[3])
        close = _to_float(kline[4])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = close
    window = trs[-period:] if len(trs) >= period else trs
    return _mean(window)


def compute_bollinger_bandwidth_pct(closes: Sequence[float], period: int = 20, std_mult: float = 2.0) -> float:
    if len(closes) < period:
        return 0.0
    window = [float(x) for x in closes[-period:]]
    mean = sum(window) / len(window)
    variance = sum((x - mean) ** 2 for x in window) / len(window)
    std = math.sqrt(variance)
    if mean == 0:
        return 0.0
    upper = mean + (std * std_mult)
    lower = mean - (std * std_mult)
    return ((upper - lower) / mean) * 100.0


def evaluate_higher_timeframe_trend(klines: Sequence[Sequence[Any]], ema_period: int = 20, side: str = TRADE_SIDE_LONG) -> Dict[str, Any]:
    closes = extract_closes(klines)
    trade_side = normalize_trade_side(side)
    if not closes:
        return {'allowed': False, 'ema20': 0.0, 'macd': 0.0, 'hist': 0.0, 'price': 0.0, 'bias': trade_side}
    ema20 = compute_ema(closes, ema_period)
    macd = compute_macd(closes)
    price = closes[-1]
    if trade_side == TRADE_SIDE_SHORT:
        allowed = price <= ema20 and macd['hist'] <= 1e-9
    else:
        allowed = price >= ema20 and macd['hist'] >= -1e-9
    return {'allowed': allowed, 'ema20': ema20, 'macd': macd['macd'], 'hist': macd['hist'], 'price': price, 'bias': trade_side}


def recommend_leverage(entry_price: float, stop_price: float, max_leverage: int = 10) -> int:
    if entry_price <= 0:
        return 1
    stop_distance_pct = abs(entry_price - stop_price) / entry_price * 100
    if stop_distance_pct >= 8:
        lev = 2
    elif stop_distance_pct >= 4:
        lev = 3
    elif stop_distance_pct >= 2:
        lev = 5
    else:
        lev = 8
    return max(1, min(lev, max_leverage))


def plan_five_usdt_target_trade(
    side: str,
    entry_price: float,
    planned_take_profit_price: float,
    stop_price: float,
    *,
    target_net_profit_usdt: float = 5.0,
    min_target_net_profit_usdt: float = 4.2,
    max_loss_usdt: float = 2.5,
    min_expected_rr: float = 1.7,
    min_notional_usdt: float = 0.0,
    max_notional_usdt: float = 0.0,
    taker_fee_rate: float = 0.0005,
    slippage_buffer_pct: float = 0.0008,
    step_size: float = 0.0,
    tick_size: float = 0.0,
    quantity_precision: int = 10,
    price_precision: int = 10,
    disable_tiny_tp: bool = True,
) -> Dict[str, Any]:
    trade_side = normalize_trade_side(side)
    entry = float(entry_price or 0.0)
    tp = float(planned_take_profit_price or 0.0)
    stop = float(stop_price or 0.0)
    if tick_size and tick_size > 0:
        tp = round_price(tp, tick_size, price_precision)
        stop = round_price(stop, tick_size, price_precision)
    direction = 1.0 if trade_side == TRADE_SIDE_LONG else -1.0
    profit_per_unit = direction * (tp - entry)
    loss_per_unit = direction * (entry - stop)
    result = {
        'expected_net_profit_usdt': 0.0,
        'expected_gross_profit_usdt': 0.0,
        'expected_loss_usdt': 0.0,
        'expected_rr': 0.0,
        'estimated_fee_usdt': 0.0,
        'estimated_slippage_usdt': 0.0,
        'target_profit_reject_reason': '',
        'planned_take_profit_price': tp,
        'planned_stop_price': stop,
        'planned_quantity': 0.0,
        'planned_notional_usdt': 0.0,
    }
    if entry <= 0 or profit_per_unit <= 0:
        result['target_profit_reject_reason'] = 'invalid_take_profit_distance'
        return result
    if disable_tiny_tp and profit_per_unit / entry < 0.001:
        result['target_profit_reject_reason'] = 'tiny_profit_trade_rejected'
        return result
    if loss_per_unit <= 0:
        result['target_profit_reject_reason'] = 'expected_loss_above_max'
        return result
    fee_rate = max(float(taker_fee_rate or 0.0), 0.0) * 2.0
    slippage_rate = max(float(slippage_buffer_pct or 0.0), 0.0) * 2.0
    round_trip_cost_per_unit = entry * (fee_rate + slippage_rate)
    net_per_unit = profit_per_unit - round_trip_cost_per_unit
    if net_per_unit <= 0:
        result['target_profit_reject_reason'] = 'fee_ratio_too_high'
        return result
    qty = max(float(target_net_profit_usdt or 0.0), float(min_target_net_profit_usdt or 0.0), 0.0) / net_per_unit
    if min_notional_usdt > 0:
        qty = max(qty, float(min_notional_usdt) / entry)
    if max_notional_usdt > 0:
        qty = min(qty, float(max_notional_usdt) / entry)
    loss_cost_per_unit = loss_per_unit + round_trip_cost_per_unit
    if max_loss_usdt > 0:
        qty = min(qty, float(max_loss_usdt) / loss_cost_per_unit)
    qty = round_step(qty, step_size, quantity_precision)
    notional = qty * entry
    gross = qty * profit_per_unit
    fee = notional * fee_rate
    slippage = notional * slippage_rate
    net = gross - fee - slippage
    loss = qty * loss_per_unit + fee + slippage
    rr = net / loss if loss > 0 else 0.0
    result.update({
        'expected_net_profit_usdt': round(net, 8),
        'expected_gross_profit_usdt': round(gross, 8),
        'expected_loss_usdt': round(loss, 8),
        'expected_rr': round(rr, 8),
        'estimated_fee_usdt': round(fee, 8),
        'estimated_slippage_usdt': round(slippage, 8),
        'planned_quantity': qty,
        'planned_notional_usdt': round(notional, 8),
    })
    if qty <= 0:
        result['target_profit_reject_reason'] = 'notional_below_min'
    elif min_notional_usdt > 0 and notional + 1e-9 < min_notional_usdt * 0.995:
        result['target_profit_reject_reason'] = 'notional_below_min'
    elif max_notional_usdt > 0 and notional > max_notional_usdt + 1e-9:
        result['target_profit_reject_reason'] = 'notional_above_max'
    elif target_net_profit_usdt > 0 and fee / float(target_net_profit_usdt) > 0.12:
        result['target_profit_reject_reason'] = 'fee_ratio_too_high'
    elif max_loss_usdt > 0 and loss > max_loss_usdt + 1e-9:
        result['target_profit_reject_reason'] = 'expected_loss_above_max'
    elif net < float(min_target_net_profit_usdt or 0.0):
        result['target_profit_reject_reason'] = 'expected_net_profit_below_min'
    elif rr < float(min_expected_rr or 0.0):
        result['target_profit_reject_reason'] = 'expected_rr_below_min'
    return result


def build_trade_management_plan(entry_price: float, stop_price: float, quantity: float, tp1_r: float, tp1_close_pct: float, tp2_r: float, tp2_close_pct: float, breakeven_r: float = 1.0, atr_stop_distance: Optional[float] = None, side: str = POSITION_SIDE_LONG, breakeven_confirmation_mode: str = 'price_only', breakeven_min_buffer_pct: float = 0.0, tp1_profit_usdt: float = 0.0, tp2_profit_usdt: float = 0.0, micro_scalp_time_stop_sec: int = 0, micro_scalp_min_profit_r: float = 0.0) -> TradeManagementPlan:
    side_normalized = normalize_position_side(side)
    direction = 1.0 if side_normalized == POSITION_SIDE_LONG else -1.0
    risk = float(atr_stop_distance) if atr_stop_distance and atr_stop_distance > 0 else abs(entry_price - stop_price)
    tp1_close_qty = round(quantity * tp1_close_pct, 10)
    tp2_close_qty = round(quantity * tp2_close_pct, 10)
    runner_qty = round(max(quantity - tp1_close_qty - tp2_close_qty, 0.0), 10)
    tp1_trigger_price = entry_price + (direction * risk * tp1_r)
    tp2_trigger_price = entry_price + (direction * risk * tp2_r)
    if quantity > 0 and tp1_close_qty > 0 and tp1_profit_usdt > 0:
        tp1_trigger_price = entry_price + (direction * (float(tp1_profit_usdt) / quantity))
    tp2_close_effective_qty = tp2_close_qty + runner_qty
    if quantity > 0 and tp2_close_effective_qty > 0 and tp2_profit_usdt > 0:
        tp2_trigger_price = entry_price + (direction * (float(tp2_profit_usdt) / quantity))
    return TradeManagementPlan(
        side=position_side_to_trade_side(side_normalized),
        position_side=side_normalized,
        entry_price=entry_price,
        stop_price=stop_price,
        quantity=quantity,
        initial_risk_per_unit=risk,
        breakeven_trigger_price=entry_price + (direction * risk * breakeven_r),
        breakeven_confirmation_mode=str(breakeven_confirmation_mode or 'price_only'),
        breakeven_min_buffer_pct=max(float(breakeven_min_buffer_pct or 0.0), 0.0),
        tp1_trigger_price=tp1_trigger_price,
        tp1_close_qty=tp1_close_qty,
        tp2_trigger_price=tp2_trigger_price,
        tp2_close_qty=tp2_close_qty,
        runner_qty=runner_qty,
        tp1_profit_usdt=max(float(tp1_profit_usdt or 0.0), 0.0),
        tp2_profit_usdt=max(float(tp2_profit_usdt or 0.0), 0.0),
        micro_scalp_time_stop_sec=max(int(micro_scalp_time_stop_sec or 0), 0),
        micro_scalp_min_profit_r=float(micro_scalp_min_profit_r or 0.0),
    )


def evaluate_management_actions(state: TradeManagementState, plan: TradeManagementPlan, current_price: float, ema5m: float, trailing_reference: float, trailing_buffer_pct: float, allow_runner_exit: bool = False, now: Optional[datetime.datetime] = None) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    side = normalize_position_side(getattr(plan, 'side', POSITION_SIDE_LONG))
    is_short = side == POSITION_SIDE_SHORT

    if state.remaining_quantity > 0 and plan.stop_price > 0:
        hard_stop_hit = current_price >= plan.stop_price if is_short else current_price <= plan.stop_price
        if hard_stop_hit:
            return [{
                'type': 'hard_stop_loss',
                'close_qty': state.remaining_quantity,
                'stop_price': plan.stop_price,
                'exit_reason': 'hard_stop_loss',
            }]

    def maybe_append_micro_scalp_time_stop() -> None:
        opened_at = _parse_iso8601_utc(getattr(state, 'opened_at', None))
        time_stop_sec = max(int(getattr(plan, 'micro_scalp_time_stop_sec', 0) or 0), 0)
        min_profit_r = float(getattr(plan, 'micro_scalp_min_profit_r', 0.0) or 0.0)
        if time_stop_sec <= 0 or opened_at is None or state.remaining_quantity <= 0:
            return
        now_dt = now.astimezone(datetime.timezone.utc) if isinstance(now, datetime.datetime) and now.tzinfo else (now.replace(tzinfo=datetime.timezone.utc) if isinstance(now, datetime.datetime) else _utc_now())
        held_seconds = max((now_dt - opened_at).total_seconds(), 0.0)
        risk_per_unit = max(float(getattr(plan, 'initial_risk_per_unit', 0.0) or 0.0), 0.0)
        direction = -1.0 if is_short else 1.0
        unrealized_r = direction * (float(current_price) - float(plan.entry_price)) / risk_per_unit if risk_per_unit > 0 else 0.0
        if held_seconds < time_stop_sec or unrealized_r < min_profit_r:
            return
        actions.append({
            'type': 'micro_scalp_time_stop',
            'close_qty': state.remaining_quantity,
            'exit_reason': 'micro_scalp_time_stop',
            'held_seconds': round(held_seconds, 2),
            'min_profit_r': min_profit_r,
            'unrealized_r': round(unrealized_r, 6),
        })

    if is_short:
        state.lowest_price_seen = min(state.lowest_price_seen or current_price, current_price)
        breakeven_buffer_price = plan.entry_price * (1 - max(float(plan.breakeven_min_buffer_pct or 0.0), 0.0))
        breakeven_confirmed = current_price <= plan.breakeven_trigger_price and current_price <= breakeven_buffer_price
        if plan.breakeven_confirmation_mode == 'ema_support':
            breakeven_confirmed = breakeven_confirmed and current_price <= ema5m and ema5m <= plan.entry_price
        tp1_will_hit = (not state.tp1_hit) and current_price <= plan.tp1_trigger_price and plan.tp1_close_qty > 0
        if (not state.moved_to_breakeven) and breakeven_confirmed:
            actions.append({'type': 'move_stop_to_breakeven', 'new_stop_price': round(breakeven_buffer_price, 10), 'confirmation_mode': plan.breakeven_confirmation_mode})
        if tp1_will_hit:
            actions.append({'type': 'take_profit_1', 'close_qty': plan.tp1_close_qty, 'new_stop_price': round(min(plan.entry_price, ema5m), 10), 'exit_reason': 'tp1'})
        if (state.tp1_hit or tp1_will_hit) and not state.tp2_hit and current_price <= plan.tp2_trigger_price and plan.tp2_close_qty > 0:
            actions.append({'type': 'take_profit_2', 'close_qty': plan.tp2_close_qty, 'new_stop_price': round(min(plan.entry_price - plan.initial_risk_per_unit, ema5m), 10), 'exit_reason': 'tp2'})
        ceiling_ref = min(trailing_reference, state.lowest_price_seen or trailing_reference)
        trailing_ceiling = round(ceiling_ref * (1 + trailing_buffer_pct), 10)
        if allow_runner_exit and (state.tp1_hit or tp1_will_hit) and current_price > trailing_ceiling and state.remaining_quantity > 0:
            actions.append({'type': 'runner_exit', 'close_qty': state.remaining_quantity, 'trailing_floor': round(trailing_ceiling, 2), 'exit_reason': 'runner'})
        maybe_append_micro_scalp_time_stop()
        return actions

    state.highest_price_seen = max(state.highest_price_seen or current_price, current_price)
    breakeven_buffer_price = plan.entry_price * (1 + max(float(plan.breakeven_min_buffer_pct or 0.0), 0.0))
    breakeven_confirmed = current_price >= plan.breakeven_trigger_price and current_price >= breakeven_buffer_price
    if plan.breakeven_confirmation_mode == 'ema_support':
        breakeven_confirmed = breakeven_confirmed and current_price >= ema5m and ema5m >= plan.entry_price
    tp1_will_hit = (not state.tp1_hit) and current_price >= plan.tp1_trigger_price and plan.tp1_close_qty > 0
    if (not state.moved_to_breakeven) and breakeven_confirmed:
        if state.tp1_hit:
            actions.append({'type': 'move_stop_to_breakeven', 'new_stop_price': round(breakeven_buffer_price, 10), 'confirmation_mode': plan.breakeven_confirmation_mode})
        elif tp1_will_hit and plan.breakeven_trigger_price > plan.tp1_trigger_price:
            actions.append({'type': 'take_profit_1', 'close_qty': plan.tp1_close_qty, 'new_stop_price': round(max(state.current_stop_price or plan.entry_price, plan.entry_price, ema5m), 10), 'exit_reason': 'tp1'})
            actions.append({'type': 'move_stop_to_breakeven', 'new_stop_price': round(breakeven_buffer_price, 10), 'confirmation_mode': plan.breakeven_confirmation_mode})
            tp1_will_hit = False
        else:
            actions.append({'type': 'move_stop_to_breakeven', 'new_stop_price': round(breakeven_buffer_price, 10), 'confirmation_mode': plan.breakeven_confirmation_mode})
    if tp1_will_hit:
        actions.append({'type': 'take_profit_1', 'close_qty': plan.tp1_close_qty, 'new_stop_price': round(max(state.current_stop_price or plan.entry_price, plan.entry_price, ema5m), 10), 'exit_reason': 'tp1'})
    if (state.tp1_hit or tp1_will_hit) and not state.tp2_hit and current_price >= plan.tp2_trigger_price and plan.tp2_close_qty > 0:
        actions.append({'type': 'take_profit_2', 'close_qty': plan.tp2_close_qty, 'new_stop_price': round(max(state.current_stop_price or (plan.entry_price + plan.initial_risk_per_unit), plan.entry_price + plan.initial_risk_per_unit, ema5m), 10), 'exit_reason': 'tp2'})
    floor_ref = max(trailing_reference, state.highest_price_seen or trailing_reference)
    trailing_floor = round(floor_ref * (1 - trailing_buffer_pct), 10)
    if allow_runner_exit and (state.tp1_hit or tp1_will_hit) and current_price < trailing_floor and state.remaining_quantity > 0:
        actions.append({'type': 'runner_exit', 'close_qty': state.remaining_quantity, 'trailing_floor': round(trailing_floor, 2), 'exit_reason': 'runner'})
    maybe_append_micro_scalp_time_stop()
    return actions


def place_reduce_only_market(client, symbol: str, quantity: float, meta: SymbolMeta, side: str = POSITION_SIDE_LONG):
    position_side = normalize_position_side(side)
    order_side = 'BUY' if position_side == POSITION_SIDE_SHORT else 'SELL'
    params = {
        'symbol': symbol,
        'side': order_side,
        'type': 'MARKET',
        'quantity': format_decimal(round_step(quantity, meta.step_size, meta.quantity_precision), meta.quantity_precision),
        'reduceOnly': 'true',
        'newOrderRespType': 'RESULT',
    }
    if should_send_position_side(client):
        params['positionSide'] = position_side
    try:
        return client.signed_post('/fapi/v1/order', params)
    except Exception as exc:
        if not should_send_position_side(client) or not is_position_side_mode_error(exc):
            raise
        mark_one_way_position_mode(client)
        params.pop('positionSide', None)
        return client.signed_post('/fapi/v1/order', params)


def cancel_order(client, symbol: str, order_id: Optional[int] = None, client_order_id: Optional[str] = None):
    params: Dict[str, Any] = {'symbol': symbol}
    if order_id is not None:
        params['orderId'] = order_id
    if client_order_id is not None:
        params['origClientOrderId'] = client_order_id
    if hasattr(client, 'signed_delete'):
        return client.signed_delete('/fapi/v1/order', params)
    return client.signed_post('/fapi/v1/order/cancel', params)


def build_protection_client_order_id(symbol: str, position_side: str, runtime_trade_id: Optional[str], kind: str) -> str:
    raw = f"bm_{str(symbol).upper()}_{normalize_position_side(position_side).lower()}_{str(kind or 'prot')[:4]}_{str(runtime_trade_id or int(time.time() * 1000))}"
    return ''.join(ch if ch.isalnum() or ch in {'_', '-'} else '_' for ch in raw)[:36]


def order_client_id(order: Dict[str, Any]) -> str:
    return str(order.get('clientOrderId') or order.get('origClientOrderId') or order.get('clientAlgoId') or order.get('algoClientOrderId') or '')


def is_protection_order(order: Dict[str, Any]) -> bool:
    order_type = str(order.get('type') or order.get('origType') or '').upper()
    algo_type = str(order.get('algoType') or '').upper()
    client_id = order_client_id(order).lower()
    return (
        order_type in {'STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'TRAILING_STOP_MARKET'}
        or algo_type == 'CONDITIONAL'
        or client_id.startswith('bm_')
    )


def order_position_key(order: Dict[str, Any]) -> str:
    side = order.get('positionSide') or (POSITION_SIDE_SHORT if str(order.get('side', '')).upper() == 'BUY' else POSITION_SIDE_LONG)
    return build_position_key(order.get('symbol'), side)


def cancel_protection_order(client: Any, order: Dict[str, Any]) -> Dict[str, Any]:
    symbol = str(order.get('symbol') or '')
    algo_id = order.get('algoId')
    if algo_id is not None and hasattr(client, 'signed_delete'):
        return client.signed_delete('/fapi/v1/algoOrder', {'symbol': symbol, 'algoId': algo_id})
    order_id = order.get('orderId')
    client_id = order_client_id(order) or None
    return cancel_order(client, symbol, order_id=order_id, client_order_id=client_id)


def order_is_reduce_only(order: Dict[str, Any]) -> bool:
    value = order.get('reduceOnly')
    return value is True or str(value).lower() == 'true'


def is_opening_order(order: Dict[str, Any]) -> bool:
    if order_is_reduce_only(order) or is_protection_order(order):
        return False
    order_type = str(order.get('type') or order.get('origType') or '').upper()
    if order_type in {'STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'TRAILING_STOP_MARKET'}:
        return False
    return bool(order.get('symbol'))


def order_opening_side(order: Dict[str, Any]) -> str:
    position_side = normalize_position_side(order.get('positionSide') or '')
    if position_side in {POSITION_SIDE_LONG, POSITION_SIDE_SHORT}:
        return position_side
    return POSITION_SIDE_SHORT if str(order.get('side') or '').upper() == 'SELL' else POSITION_SIDE_LONG


def opening_order_position_keys(orders: Sequence[Dict[str, Any]]) -> Set[str]:
    return {order_position_key(order) for order in orders or [] if isinstance(order, dict) and is_opening_order(order)}


def projected_position_keys(open_positions: Sequence[Dict[str, Any]], orders: Sequence[Dict[str, Any]]) -> Set[str]:
    keys = {build_position_key(row.get('symbol'), position_side_from_exchange_position(row)) for row in open_positions or [] if isinstance(row, dict)}
    keys.update(opening_order_position_keys(orders))
    return {key for key in keys if key and not key.endswith(':')}


def opening_orders_as_risk_positions(orders: Sequence[Dict[str, Any]], fallback_price: float = 0.0) -> List[Dict[str, Any]]:
    projected: List[Dict[str, Any]] = []
    for order in orders or []:
        if not isinstance(order, dict) or not is_opening_order(order):
            continue
        original_quantity = abs(_to_float(order.get('origQty') or order.get('quantity') or order.get('qty')))
        executed_quantity = abs(_to_float(order.get('executedQty') or order.get('cumQty')))
        remaining_quantity = max(original_quantity - executed_quantity, 0.0)
        if remaining_quantity <= 0:
            continue
        price = abs(_to_float(order.get('price') or order.get('avgPrice') or order.get('stopPrice') or order.get('triggerPrice') or fallback_price))
        projected.append({
            'symbol': str(order.get('symbol') or '').upper(),
            'positionSide': order_opening_side(order),
            'positionAmt': remaining_quantity,
            'quantity': remaining_quantity,
            'notional': remaining_quantity * price,
            'source': 'pending_opening_order',
        })
    return projected


def cancel_open_order(client: Any, order: Dict[str, Any]) -> Dict[str, Any]:
    symbol = str(order.get('symbol') or '')
    order_id = order.get('orderId')
    client_id = order_client_id(order) or None
    return cancel_order(client, symbol, order_id=order_id, client_order_id=client_id)


def reconcile_positions_and_orders(
    client: Any,
    store: Optional[RuntimeStateStore] = None,
    max_open_positions: Optional[int] = None,
    max_short_positions: Optional[int] = None,
) -> Dict[str, Any]:
    positions = fetch_open_positions(client)
    regular_orders = fetch_open_orders(client)
    algo_orders = fetch_open_algo_orders(client)
    all_orders = list(regular_orders or []) + list(algo_orders or [])
    open_position_keys = {build_position_key(row.get('symbol'), position_side_from_exchange_position(row)) for row in positions if isinstance(row, dict)}
    projected_keys = projected_position_keys(positions, all_orders)
    open_position_count = len(open_position_keys)
    projected_position_count = len(projected_keys)
    short_position_count = sum(1 for row in positions if isinstance(row, dict) and position_side_from_exchange_position(row) == POSITION_SIDE_SHORT)
    projected_short_count = len({key for key in projected_keys if key.endswith(f':{POSITION_SIDE_SHORT}')})
    position_cap_reached = max_open_positions is not None and open_position_count >= int(max_open_positions)
    position_cap_projected_exceeded = max_open_positions is not None and projected_position_count > int(max_open_positions)
    short_cap_reached = max_short_positions is not None and short_position_count >= int(max_short_positions)
    short_cap_projected_exceeded = max_short_positions is not None and projected_short_count > int(max_short_positions)
    detected: List[Dict[str, Any]] = []
    cancelled: List[Dict[str, Any]] = []
    cap_detected: List[Dict[str, Any]] = []
    cap_cancelled: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for order in all_orders:
        if is_opening_order(order) and (
            position_cap_reached
            or position_cap_projected_exceeded
            or (short_cap_reached and order_opening_side(order) == POSITION_SIDE_SHORT)
            or (short_cap_projected_exceeded and order_opening_side(order) == POSITION_SIDE_SHORT)
        ):
            payload = {
                'symbol': order.get('symbol'),
                'position_side': order_opening_side(order),
                'orderId': order.get('orderId') or order.get('algoId'),
                'clientOrderId': order_client_id(order),
                'type': order.get('type') or order.get('origType'),
                'open_position_count': open_position_count,
                'projected_position_count': projected_position_count,
                'short_position_count': short_position_count,
                'projected_short_count': projected_short_count,
                'max_open_positions': max_open_positions,
                'max_short_positions': max_short_positions,
            }
            cap_detected.append(payload)
            append_runtime_event(store, 'position_cap_opening_order_detected', payload)
            try:
                result = cancel_open_order(client, order)
                cancel_payload = {**payload, 'cancel_result': result}
                cap_cancelled.append(cancel_payload)
                append_runtime_event(store, 'position_cap_opening_order_cancelled', cancel_payload)
            except Exception as exc:
                error_payload = {**payload, 'error': str(exc)}
                errors.append(error_payload)
                append_runtime_event(store, 'position_cap_opening_order_cancel_failed', error_payload)
            continue
        if not isinstance(order, dict) or not is_protection_order(order):
            continue
        pos_key = order_position_key(order)
        if pos_key in open_position_keys:
            continue
        payload = {'symbol': order.get('symbol'), 'position_key': pos_key, 'orderId': order.get('orderId') or order.get('algoId'), 'clientOrderId': order_client_id(order), 'type': order.get('type') or order.get('origType')}
        detected.append(payload)
        append_runtime_event(store, 'orphan_order_detected', payload)
        try:
            result = cancel_protection_order(client, order)
            cancel_payload = {**payload, 'cancel_result': result}
            cancelled.append(cancel_payload)
            append_runtime_event(store, 'orphan_order_cancelled', cancel_payload)
        except Exception as exc:
            error_payload = {**payload, 'error': str(exc)}
            errors.append(error_payload)
            append_runtime_event(store, 'orphan_order_cancel_failed', error_payload)
    return {'ok': not errors, 'positions': len(positions), 'open_orders': len(all_orders), 'projected_positions': projected_position_count, 'projected_short_positions': projected_short_count, 'orphan_order_detected': len(detected), 'orphan_order_cancelled': len(cancelled), 'cap_opening_order_detected': len(cap_detected), 'cap_opening_order_cancelled': len(cap_cancelled), 'errors': errors}


def should_send_position_side(client: Any) -> bool:
    return str(getattr(client, 'position_mode', 'HEDGE') or 'HEDGE').upper() != 'ONE_WAY'


def is_position_side_mode_error(exc: Any) -> bool:
    message = str(exc)
    return '-4061' in message or 'position side does not match' in message.lower()


def is_reduce_only_not_required_error(exc: Any) -> bool:
    message = str(exc).lower()
    return '-1106' in message and 'reduceonly' in message


def mark_one_way_position_mode(client: Any) -> None:
    try:
        setattr(client, 'position_mode', 'ONE_WAY')
    except Exception:
        pass


def place_stop_market_order(client, symbol: str, stop_price: float, quantity: float, meta: SymbolMeta, side: str = POSITION_SIDE_LONG, runtime_trade_id: Optional[str] = None):
    position_side = normalize_position_side(side)
    order_side = 'BUY' if position_side == POSITION_SIDE_SHORT else 'SELL'
    trigger_price = round_step(stop_price, meta.tick_size, meta.price_precision)
    qty = round_step(quantity, meta.step_size, meta.quantity_precision)
    params = {
        'symbol': symbol,
        'side': order_side,
        'algoType': 'CONDITIONAL',
        'type': 'STOP_MARKET',
        'triggerPrice': format_decimal(trigger_price, meta.price_precision),
        'quantity': format_decimal(qty, meta.quantity_precision),
        'reduceOnly': 'true',
        'newClientOrderId': build_protection_client_order_id(symbol, position_side, runtime_trade_id, 'stop'),
    }
    if should_send_position_side(client):
        params['positionSide'] = position_side
    try:
        return client.signed_post('/fapi/v1/algoOrder', params)
    except Exception as exc:
        if is_reduce_only_not_required_error(exc):
            params.pop('reduceOnly', None)
            return client.signed_post('/fapi/v1/algoOrder', params)
        if not should_send_position_side(client) or not is_position_side_mode_error(exc):
            raise
        mark_one_way_position_mode(client)
        params.pop('positionSide', None)
        try:
            return client.signed_post('/fapi/v1/algoOrder', params)
        except Exception as retry_exc:
            if is_reduce_only_not_required_error(retry_exc):
                params.pop('reduceOnly', None)
                return client.signed_post('/fapi/v1/algoOrder', params)
            raise


def place_take_profit_market_order(client, symbol: str, trigger_price: float, quantity: float, meta: SymbolMeta, side: str = POSITION_SIDE_LONG, runtime_trade_id: Optional[str] = None):
    position_side = normalize_position_side(side)
    order_side = 'BUY' if position_side == POSITION_SIDE_SHORT else 'SELL'
    trigger_price = round_step(trigger_price, meta.tick_size, meta.price_precision)
    qty = round_step(quantity, meta.step_size, meta.quantity_precision)
    params = {
        'symbol': symbol,
        'side': order_side,
        'algoType': 'CONDITIONAL',
        'type': 'TAKE_PROFIT_MARKET',
        'triggerPrice': format_decimal(trigger_price, meta.price_precision),
        'quantity': format_decimal(qty, meta.quantity_precision),
        'reduceOnly': 'true',
        'newClientOrderId': build_protection_client_order_id(symbol, position_side, runtime_trade_id, 'tp'),
    }
    if should_send_position_side(client):
        params['positionSide'] = position_side
    try:
        return client.signed_post('/fapi/v1/algoOrder', params)
    except Exception as exc:
        if is_reduce_only_not_required_error(exc):
            params.pop('reduceOnly', None)
            return client.signed_post('/fapi/v1/algoOrder', params)
        if not should_send_position_side(client) or not is_position_side_mode_error(exc):
            raise
        mark_one_way_position_mode(client)
        params.pop('positionSide', None)
        try:
            return client.signed_post('/fapi/v1/algoOrder', params)
        except Exception as retry_exc:
            if is_reduce_only_not_required_error(retry_exc):
                params.pop('reduceOnly', None)
                return client.signed_post('/fapi/v1/algoOrder', params)
            raise


def apply_management_action(client, symbol: str, meta: SymbolMeta, state: TradeManagementState, action: Dict[str, Any], active_stop_order: Optional[Dict[str, Any]]):
    log_payload: Dict[str, Any] = {'action': action['type'], 'symbol': symbol}
    side = normalize_position_side(getattr(state, 'side', POSITION_SIDE_LONG))
    if action['type'] == 'move_stop_to_breakeven':
        if active_stop_order and active_stop_order.get('orderId'):
            cancel_order(client, symbol, order_id=active_stop_order['orderId'])
        new_stop_order = place_stop_market_order(client, symbol, action['new_stop_price'], state.remaining_quantity, meta, side=side)
        state.current_stop_price = action['new_stop_price']
        state.moved_to_breakeven = True
        return state, new_stop_order, {**log_payload, 'new_stop_order': new_stop_order}
    if action['type'] in {'take_profit_1', 'take_profit_2', 'runner_exit', 'stop_exit', 'hard_stop_loss', 'micro_scalp_time_stop'}:
        reduce_result = place_reduce_only_market(client, symbol, action['close_qty'], meta, side=side)
        state.remaining_quantity = round(max(state.remaining_quantity - action['close_qty'], 0.0), 10)
        if action['type'] == 'take_profit_1':
            state.tp1_hit = True
        elif action['type'] == 'take_profit_2':
            state.tp2_hit = True
        if action['type'] in {'take_profit_1', 'take_profit_2'} and state.remaining_quantity > 0 and action.get('new_stop_price') is not None:
            if active_stop_order and active_stop_order.get('orderId'):
                cancel_order(client, symbol, order_id=active_stop_order['orderId'])
            active_stop_order = place_stop_market_order(client, symbol, action['new_stop_price'], state.remaining_quantity, meta, side=side)
            state.current_stop_price = action['new_stop_price']
        return state, active_stop_order, {
            **log_payload,
            'reduce_order': reduce_result,
            'new_stop_order': active_stop_order,
            'exit_reason': action.get('exit_reason'),
        }
    return state, active_stop_order, log_payload


def iter_canonical_open_positions(positions_state: Any) -> List[Tuple[str, Dict[str, Any]]]:
    canonical = migrate_positions_state(positions_state)
    rows: List[Tuple[str, Dict[str, Any]]] = []
    for key, position in canonical.items():
        if not isinstance(position, dict):
            continue
        status = str(position.get('status') or '').lower()
        remaining = abs(_to_float(position.get('remaining_quantity') or position.get('quantity') or position.get('filled_quantity')))
        if status in {'closed', 'flat'} or remaining <= 0:
            continue
        rows.append((key, position))
    return rows


def _should_emit_runtime_state_degraded(store: RuntimeStateStore, state_key: str, cooldown_seconds: int = 300) -> bool:
    now = _utc_now()
    stamps = getattr(store, '_runtime_state_degraded_emitted_at', None)
    if not isinstance(stamps, dict):
        stamps = {}
        setattr(store, '_runtime_state_degraded_emitted_at', stamps)
    last_emitted_at = stamps.get(state_key)
    if isinstance(last_emitted_at, datetime.datetime) and (now - last_emitted_at).total_seconds() < max(int(cooldown_seconds or 0), 0):
        return False
    stamps[state_key] = now
    return True


def build_local_open_positions_for_risk(store: RuntimeStateStore) -> List[Dict[str, Any]]:
    return load_local_open_positions_for_risk_impl(
        store,
        should_emit_runtime_state_degraded=_should_emit_runtime_state_degraded,
        append_runtime_state_degraded_event=append_rate_limited_runtime_event,
        build_local_open_positions_from_state=build_local_open_positions_from_state_impl,
        normalize_position_side=normalize_position_side,
        to_float=_to_float,
        iter_canonical_open_positions=iter_canonical_open_positions,
    )


def build_trade_management_plan_from_position(position: Dict[str, Any], args: argparse.Namespace) -> TradeManagementPlan:
    position_side = normalize_position_side(position.get('position_side') or position.get('positionSide') or position.get('side'))
    trade_side = position_side_to_trade_side(position_side)
    position_key = build_position_key(str(position.get('symbol') or ''), position_side)
    plan_payload = position.get('trade_management_plan')
    if isinstance(plan_payload, dict) and plan_payload:
        payload = dict(plan_payload)
        payload['position_side'] = position_side
        payload['side'] = trade_side
        return TradeManagementPlan(**payload)
    entry_price = _to_float(position.get('entry_price'))
    stop_price = _to_float(position.get('current_stop_price') or position.get('stop_price'))
    quantity = _to_float(position.get('quantity') or position.get('filled_quantity') or position.get('remaining_quantity'))
    if entry_price <= 0 or stop_price <= 0 or abs(entry_price - stop_price) <= 1e-12:
        raise ValueError(f'missing valid stop distance for {position_key}')
    return build_trade_management_plan(
        entry_price=entry_price,
        stop_price=stop_price,
        quantity=quantity,
        tp1_r=float(getattr(args, 'tp1_r', 1.5)),
        tp1_close_pct=float(getattr(args, 'tp1_close_pct', 0.3)),
        tp1_profit_usdt=float(getattr(args, 'tp1_profit_usdt', 0.0) or 0.0),
        tp2_r=float(getattr(args, 'tp2_r', 2.0)),
        tp2_close_pct=float(getattr(args, 'tp2_close_pct', 0.4)),
        tp2_profit_usdt=float(getattr(args, 'tp2_profit_usdt', 0.0) or 0.0),
        breakeven_r=float(getattr(args, 'breakeven_r', 1.0)),
        atr_stop_distance=float(position.get('atr_stop_distance') or 0.0),
        side=trade_side,
        breakeven_confirmation_mode=str(getattr(args, 'breakeven_confirmation_mode', 'ema_support') or 'ema_support'),
        breakeven_min_buffer_pct=float(getattr(args, 'breakeven_min_buffer_pct', 0.001) or 0.0),
        micro_scalp_time_stop_sec=int(getattr(args, 'micro_scalp_time_stop_sec', 0) or 0),
        micro_scalp_min_profit_r=float(getattr(args, 'micro_scalp_min_profit_r', 0.0) or 0.0),
    )


def recover_protected_position_trade_management_plan(
    tracked: Dict[str, Any],
    protection: Dict[str, Any],
    args: Optional[argparse.Namespace],
) -> Dict[str, Any]:
    tracked = dict(tracked or {})
    tracked['protection_status'] = 'protected'
    tracked['protected_recovery_pending'] = True
    stop_candidates = []
    stop_order = protection.get('stop_order') if isinstance(protection, dict) else None
    if isinstance(stop_order, dict):
        stop_candidates.extend([
            stop_order.get('stopPrice'),
            stop_order.get('triggerPrice'),
            stop_order.get('activatePrice'),
        ])
    open_orders = protection.get('open_orders') if isinstance(protection, dict) else None
    if isinstance(open_orders, list):
        for order in open_orders:
            if not isinstance(order, dict):
                continue
            stop_candidates.extend([
                order.get('stopPrice'),
                order.get('triggerPrice'),
                order.get('activatePrice'),
            ])
    stop_candidates.extend([
        tracked.get('current_stop_price'),
        tracked.get('stop_price'),
    ])
    
    entry_price = _to_float(tracked.get('entry_price'))
    position_side = normalize_position_side(tracked.get('position_side') or tracked.get('side'))
    recovered_stop_price = 0.0
    for candidate in stop_candidates:
        candidate_price = _to_float(candidate)
        # A protective stop must be beyond entry in the loss direction. Do not
        # let a stale persisted stop hide a valid exchange algo-order trigger.
        valid_direction = (
            entry_price <= 0
            or (position_side == POSITION_SIDE_LONG and candidate_price < entry_price)
            or (position_side == POSITION_SIDE_SHORT and candidate_price > entry_price)
        )
        if candidate_price > 0 and valid_direction:
            recovered_stop_price = candidate_price
            break
    if recovered_stop_price > 0:
        tracked['current_stop_price'] = recovered_stop_price
        tracked['stop_price'] = recovered_stop_price
    if args is None:
        return tracked
    try:
        # Recovery is authoritative: an embedded stale plan must not prevent a
        # rebuild from the live exchange stop and quantity.
        tracked['trade_management_plan'] = None
        plan = build_trade_management_plan_from_position(tracked, args)
    except Exception as exc:
        tracked['recovery_incomplete'] = True
        tracked['recovery_reason'] = 'missing_valid_stop_distance'
        tracked['recovery_detail'] = str(exc)
        tracked['trade_management_plan'] = None
        tracked['status'] = 'protected_recovery_pending'
        return tracked
    if getattr(plan, 'initial_risk_per_unit', 0.0) > 0:
        tracked['trade_management_plan'] = asdict(plan)
        tracked['status'] = 'monitoring'
        tracked['protected_recovery_pending'] = False
        tracked.pop('recovery_incomplete', None)
        tracked.pop('recovery_reason', None)
        tracked.pop('recovery_detail', None)
    return tracked


def compute_relative_oi_features(
    oi_now: Optional[float],
    oi_5m_ago: Optional[float],
    oi_15m_ago: Optional[float],
    taker_buy_ratio: Optional[float],
    funding_rate: Optional[float],
    funding_rate_avg: Optional[float],
    oi_change_samples_5m: Optional[Sequence[float]] = None,
    volume_samples_5m: Optional[Sequence[float]] = None,
    latest_volume_5m: Optional[float] = None,
    cvd_samples: Optional[Sequence[float]] = None,
    cvd_delta: Optional[float] = None,
    bollinger_bandwidth_pct: Optional[float] = None,
    price_above_vwap: Optional[bool] = None,
    oi_notional_history: Optional[Sequence[float]] = None,
    short_bias: float = 0.0,
) -> Dict[str, Any]:
    oi_now = _to_float(oi_now, default=0.0)
    oi_5m_ago = _to_float(oi_5m_ago, default=0.0)
    oi_15m_ago = _to_float(oi_15m_ago, default=0.0)
    oi_change_pct_5m = ((oi_now - oi_5m_ago) / oi_5m_ago * 100) if oi_5m_ago else 0.0
    oi_change_pct_15m = ((oi_now - oi_15m_ago) / oi_15m_ago * 100) if oi_15m_ago else 0.0
    baseline = (oi_change_pct_15m / 3.0) if oi_change_pct_15m > 0 else 0.0
    acceleration_ratio = (oi_change_pct_5m / baseline) if baseline > 0 else (1.0 if oi_change_pct_5m > 0 else 0.0)
    oi_z = compute_zscore(oi_change_pct_5m, list(oi_change_samples_5m or []))
    volume_z = compute_zscore(_to_float(latest_volume_5m, default=0.0), list(volume_samples_5m or [])) if latest_volume_5m is not None else 0.0
    cvd_delta = _to_float(cvd_delta, default=0.0)
    cvd_z = compute_zscore(cvd_delta, list(cvd_samples or []))
    funding_hint = None
    if funding_rate is not None and funding_rate_avg is not None:
        spread_bps = (float(funding_rate) - float(funding_rate_avg)) * 10000
        funding_hint = clamp(0.5 + (spread_bps / 10.0), 0.0, 1.0)
    oi_notional_history = [_to_float(value) for value in list(oi_notional_history or []) if _to_float(value) > 0]
    oi_notional_percentile = 0.0
    if oi_notional_history and oi_now > 0:
        less_or_equal = sum(1 for value in oi_notional_history if value <= oi_now)
        oi_notional_percentile = less_or_equal / len(oi_notional_history)
    return {
        'oi_change_pct_5m': oi_change_pct_5m,
        'oi_change_pct_15m': oi_change_pct_15m,
        'oi_acceleration_ratio': acceleration_ratio,
        'taker_buy_ratio': taker_buy_ratio,
        'oi_zscore_5m': oi_z,
        'volume_zscore_5m': volume_z,
        'cvd_delta': cvd_delta,
        'cvd_zscore': cvd_z,
        'funding_rate_percentile_hint': funding_hint,
        'bollinger_bandwidth_pct': bollinger_bandwidth_pct,
        'price_above_vwap': bool(price_above_vwap),
        'oi_notional_percentile': oi_notional_percentile,
        'short_bias': short_bias,
    }


def derive_microstructure_inputs(
    oi_history: Sequence[Dict[str, Any]],
    taker_5m: Sequence[Any],
    taker_15m: Sequence[Sequence[Any]],
    top_account_long_short: Sequence[Dict[str, Any]],
    order_book: Optional[Dict[str, Any]] = None,
    book_ticker_samples: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    oi_values = []
    for item in oi_history:
        if not item:
            continue
        raw = item.get('sumOpenInterestValue')
        if raw is None:
            raw = item.get('sumOpenInterest')
        oi_values.append(_to_float(raw))
    oi_now = oi_values[-1] if oi_values else None
    oi_5m_ago = oi_values[-2] if len(oi_values) >= 2 else None
    oi_15m_ago = oi_values[-3] if len(oi_values) >= 3 else None
    oi_change_samples_5m = []
    for prev, curr in zip(oi_values[:-1], oi_values[1:]):
        if prev:
            oi_change_samples_5m.append(((curr / prev) - 1.0) * 100)
    volume_5m = _to_float(taker_5m[5]) if taker_5m and len(taker_5m) > 5 else 0.0
    taker_buy_volume_5m = _to_float(taker_5m[9]) if taker_5m and len(taker_5m) > 9 else 0.0
    taker_buy_ratio = taker_buy_volume_5m / volume_5m if volume_5m else None
    volume_samples_5m = [_to_float(candle[7]) for candle in taker_15m if len(candle) > 7]
    latest_volume_5m = _to_float(taker_5m[7]) if taker_5m and len(taker_5m) > 7 else None
    cvd_samples = []
    for candle in taker_15m:
        total = _to_float(candle[5]) if len(candle) > 5 else 0.0
        buy = _to_float(candle[9]) if len(candle) > 9 else 0.0
        sell = max(total - buy, 0.0)
        cvd_samples.append(buy - sell)
    cvd_delta = 0.0
    if taker_5m:
        total_5m = _to_float(taker_5m[5]) if len(taker_5m) > 5 else 0.0
        buy_5m = _to_float(taker_5m[9]) if len(taker_5m) > 9 else 0.0
        cvd_delta = buy_5m - max(total_5m - buy_5m, 0.0)
    long_short_ratio = None
    short_bias = 0.0
    if top_account_long_short:
        row = top_account_long_short[-1]
        long_short_ratio = _to_float(row.get('longShortRatio'), default=None) if row else None
        if long_short_ratio is not None and long_short_ratio > 0:
            short_bias = max(0.0, (1.0 / long_short_ratio) - 1.0)

    spread_bps = 0.0
    orderbook_slope = 0.0
    book_depth_fill_ratio = 0.0
    bids = list((order_book or {}).get('bids') or [])
    asks = list((order_book or {}).get('asks') or [])
    if bids and asks:
        best_bid = _to_float(bids[0][0]) if len(bids[0]) > 1 else 0.0
        best_ask = _to_float(asks[0][0]) if len(asks[0]) > 1 else 0.0
        midpoint = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else 0.0
        if midpoint > 0 and best_ask >= best_bid:
            spread_bps = round(((best_ask - best_bid) / midpoint) * 10000.0, 4)
        bid_qty_total = sum(_to_float(level[1]) for level in bids if len(level) > 1)
        ask_qty_total = sum(_to_float(level[1]) for level in asks if len(level) > 1)
        top_depth = _to_float(bids[0][1]) + _to_float(asks[0][1]) if len(bids[0]) > 1 and len(asks[0]) > 1 else 0.0
        total_depth = bid_qty_total + ask_qty_total
        if top_depth > 0:
            book_depth_fill_ratio = round(min(total_depth / top_depth, 1.0), 4)
        bid_span = abs(_to_float(bids[0][0]) - _to_float(bids[-1][0])) if len(bids[0]) > 0 and len(bids[-1]) > 0 else 0.0
        ask_span = abs(_to_float(asks[-1][0]) - _to_float(asks[0][0])) if len(asks[-1]) > 0 and len(asks[0]) > 0 else 0.0
        price_span = bid_span + ask_span
        if total_depth > 0 and price_span > 0:
            orderbook_slope = round(total_depth / price_span, 4)

    bid_depth_usdt = sum(max(_to_float(level[0]), 0.0) * max(_to_float(level[1]), 0.0) for level in bids if len(level) > 1)
    ask_depth_usdt = sum(max(_to_float(level[0]), 0.0) * max(_to_float(level[1]), 0.0) for level in asks if len(level) > 1)
    best_bid_price = max(_to_float(bids[0][0]), 0.0) if bids and len(bids[0]) > 1 else 0.0
    best_ask_price = max(_to_float(asks[0][0]), 0.0) if asks and len(asks[0]) > 1 else 0.0
    top_bid_depth_usdt = best_bid_price * max(_to_float(bids[0][1]), 0.0) if best_bid_price > 0 and bids else 0.0
    top_ask_depth_usdt = best_ask_price * max(_to_float(asks[0][1]), 0.0) if best_ask_price > 0 and asks else 0.0

    cancel_rate = 0.0
    samples = [sample for sample in list(book_ticker_samples or []) if isinstance(sample, dict)]
    if samples:
        cancel_events = 0
        for prev, curr in zip(samples[:-1], samples[1:]):
            prev_bid_qty = _to_float(prev.get('bidQty'))
            prev_ask_qty = _to_float(prev.get('askQty'))
            curr_bid_qty = _to_float(curr.get('bidQty'))
            curr_ask_qty = _to_float(curr.get('askQty'))
            if curr_bid_qty < prev_bid_qty or curr_ask_qty < prev_ask_qty:
                cancel_events += 1
        cancel_rate = round(cancel_events / len(samples), 4)

    return {
        'oi_now': oi_now,
        'oi_5m_ago': oi_5m_ago,
        'oi_15m_ago': oi_15m_ago,
        'oi_notional_history': oi_values,
        'oi_change_samples_5m': oi_change_samples_5m,
        'taker_buy_ratio': round(taker_buy_ratio, 4) if taker_buy_ratio is not None else None,
        'volume_samples_5m': volume_samples_5m,
        'latest_volume_5m': latest_volume_5m,
        'cvd_delta': cvd_delta,
        'cvd_samples': cvd_samples,
        'long_short_ratio': long_short_ratio,
        'short_bias': short_bias,
        'spread_bps': spread_bps,
        'orderbook_slope': orderbook_slope,
        'book_depth_fill_ratio': book_depth_fill_ratio,
        'cancel_rate': cancel_rate,
        'bid_levels': bids,
        'ask_levels': asks,
        'bid_depth_usdt': round(bid_depth_usdt, 4),
        'ask_depth_usdt': round(ask_depth_usdt, 4),
        'top_bid_depth_usdt': round(top_bid_depth_usdt, 4),
        'top_ask_depth_usdt': round(top_ask_depth_usdt, 4),
        'best_bid_price': best_bid_price,
        'best_ask_price': best_ask_price,
    }


def compute_leading_sentiment_signal(okx_sentiment_score: float = 0.0, okx_sentiment_acceleration: float = 0.0, side: str = TRADE_SIDE_LONG) -> Dict[str, Any]:
    reasons: List[str] = []
    score = 0.0
    trade_side = normalize_trade_side(side)
    directional_score = okx_sentiment_score if trade_side == TRADE_SIDE_LONG else -okx_sentiment_score
    directional_acceleration = okx_sentiment_acceleration if trade_side == TRADE_SIDE_LONG else -okx_sentiment_acceleration
    if directional_score <= 0.35 and directional_acceleration >= 0.25:
        score += 6.0
        reasons.append('sentiment_early_turn_zone' if trade_side == TRADE_SIDE_LONG else 'sentiment_early_turn_zone_short')
    if directional_acceleration >= 0.35:
        score += 3.0
        reasons.append('sentiment_acceleration_turn' if trade_side == TRADE_SIDE_LONG else 'sentiment_acceleration_turn_short')
    elif directional_acceleration > 0:
        score += directional_acceleration * 4.0
    if directional_score >= 0.75:
        score -= 6.0 + max(0.0, (directional_score - 0.75) * 10.0)
        reasons.append('sentiment_too_hot' if trade_side == TRADE_SIDE_LONG else 'sentiment_too_hot_short')
    return {'score': score, 'reasons': reasons}


def compute_squeeze_signal(funding_rate: Optional[float], funding_rate_avg: Optional[float], short_bias: float, oi_zscore_5m: float, cvd_delta: float, cvd_zscore: float, recent_5m_change_pct: float, side: str = TRADE_SIDE_LONG) -> Dict[str, Any]:
    reasons: List[str] = []
    score = 0.0
    trade_side = normalize_trade_side(side)
    long_bias = max(0.0, 1.0 - float(short_bias or 0.0))
    if trade_side == TRADE_SIDE_SHORT:
        if funding_rate is not None and funding_rate >= 0.001:
            score += 8.0
            reasons.append('positive_funding_crowded_longs')
        if funding_rate is not None and funding_rate_avg is not None and funding_rate > funding_rate_avg:
            score += 4.0
        if long_bias >= 0.5:
            score += min(long_bias * 10.0, 8.0)
            reasons.append('retail_long_bias')
    else:
        if funding_rate is not None and funding_rate <= -0.001:
            score += 8.0
            reasons.append('negative_funding_crowded_shorts')
        if funding_rate is not None and funding_rate_avg is not None and funding_rate < funding_rate_avg:
            score += 4.0
        if short_bias >= 0.5:
            score += min(short_bias * 10.0, 8.0)
            reasons.append('retail_short_bias')
    if oi_zscore_5m >= 3.0:
        score += min(oi_zscore_5m * 2.5, 10.0)
        reasons.append('oi_anomaly_forcing')
    directional_cvd_delta = cvd_delta if trade_side == TRADE_SIDE_LONG else -cvd_delta
    directional_cvd_zscore = cvd_zscore if trade_side == TRADE_SIDE_LONG else -cvd_zscore
    if directional_cvd_delta > 0:
        score += min(abs(cvd_delta) / 100000.0, 4.0)
    if directional_cvd_zscore >= 2.5:
        score += min(directional_cvd_zscore * 1.5, 6.0)
        reasons.append('negative_cvd_confirmation' if trade_side == TRADE_SIDE_SHORT else 'positive_cvd_confirmation')
    if recent_5m_change_pct > 0:
        score += min(recent_5m_change_pct * 1.5, 4.0)
    return {'score': score, 'reasons': reasons}


def compute_control_risk_score(short_bias: float, oi_notional_percentile: float, smart_money_flow_score: float, side: str = TRADE_SIDE_LONG) -> Dict[str, Any]:
    reasons: List[str] = []
    score = 0.0
    veto = False
    veto_reason = None
    trade_side = normalize_trade_side(side)
    if oi_notional_percentile >= 0.97:
        score += 8.0 + (oi_notional_percentile - 0.97) * 100.0
        reasons.append('oi_at_extreme_percentile')
    if trade_side == TRADE_SIDE_SHORT:
        if short_bias >= 0.65:
            score += 6.0 + max(0.0, (short_bias - 0.65) * 10.0)
            reasons.append('crowded_short_side')
        if smart_money_flow_score >= 0.35:
            score += 10.0 + abs(smart_money_flow_score) * 10.0
            reasons.append('smart_money_long_pressure_risk')
            veto = True
            veto_reason = 'smart_money_long_pressure_veto'
    else:
        if short_bias <= 0.2:
            score += 6.0
            reasons.append('weak_short_fuel')
        if smart_money_flow_score <= -0.35:
            score += 10.0 + abs(smart_money_flow_score) * 10.0
            reasons.append('smart_money_distribution_risk')
            veto = True
            veto_reason = 'smart_money_distribution_veto'
    return {'score': score, 'reasons': reasons, 'veto': veto, 'veto_reason': veto_reason}


def merge_smart_money_scores(exchange_score: Optional[float] = None, onchain_score: Optional[float] = None, side: str = TRADE_SIDE_LONG) -> Dict[str, Any]:
    sources: List[str] = []
    values: List[float] = []
    trade_side = normalize_trade_side(side)
    if exchange_score is not None:
        sources.append('exchange')
        values.append(float(exchange_score))
    if onchain_score is not None:
        sources.append('onchain')
        values.append(float(onchain_score))
    score = sum(values) / len(values) if values else 0.0
    if trade_side == TRADE_SIDE_SHORT:
        veto = score >= 0.5 or (len(values) >= 2 and all(v >= 0.4 for v in values))
        veto_reason = 'smart_money_long_pressure_veto' if veto else None
    else:
        veto = score <= -0.5 or (len(values) >= 2 and all(v <= -0.4 for v in values))
        veto_reason = 'smart_money_outflow_veto' if veto else None
    return {'score': score, 'sources': sources, 'veto': veto, 'veto_reason': veto_reason}


def compute_sentiment_resonance_bonus(okx_sentiment_score: float = 0.0, okx_sentiment_acceleration: float = 0.0, sector_resonance_score: float = 0.0, smart_money_flow_score: float = 0.0, side: str = TRADE_SIDE_LONG) -> Dict[str, Any]:
    reasons: List[str] = []
    bonus = 0.0
    penalty = 0.0
    trade_side = normalize_trade_side(side)
    sentiment_score_raw = float(okx_sentiment_score or 0.0)
    sentiment_acceleration_raw = float(okx_sentiment_acceleration or 0.0)
    smart_money_score_raw = float(smart_money_flow_score or 0.0)
    sentiment_score = sentiment_score_raw if trade_side == TRADE_SIDE_LONG else -sentiment_score_raw
    sentiment_acceleration = sentiment_acceleration_raw if trade_side == TRADE_SIDE_LONG else -sentiment_acceleration_raw
    sector_score = float(sector_resonance_score or 0.0)
    smart_money_score = smart_money_score_raw if trade_side == TRADE_SIDE_LONG else -smart_money_score_raw
    sentiment_weight = 5.8 if smart_money_score < 0 else 9.0
    acceleration_weight = 3.8 if smart_money_score < 0 else 10.8
    sector_weight = 2.2 if smart_money_score < 0 else 5.2
    if sentiment_score > 0:
        reasons.append('okx_sentiment_positive' if trade_side == TRADE_SIDE_LONG else 'okx_sentiment_bearish_supportive')
        reasons.append(f'okx_sentiment_score={sentiment_score:.2f}' if trade_side == TRADE_SIDE_LONG else f'okx_sentiment_score_short={sentiment_score:.2f}')
        bonus += min(sentiment_score, 0.75) * sentiment_weight
        if 0 < sentiment_score <= 0.4 and sentiment_acceleration >= 0.25:
            bonus += 2.5
            reasons.append('sentiment_early_turn' if trade_side == TRADE_SIDE_LONG else 'sentiment_early_turn_short')
        if sentiment_score > 0.75:
            overheat_excess = sentiment_score - 0.75
            penalty += overheat_excess * 24.0
            reasons.append('sentiment_overheated' if trade_side == TRADE_SIDE_LONG else 'sentiment_overheated_short')
    if sentiment_acceleration > 0:
        reasons.append('okx_sentiment_accelerating' if trade_side == TRADE_SIDE_LONG else 'okx_sentiment_bearish_accelerating')
        bonus += min(sentiment_acceleration, 0.35) * acceleration_weight
        if sentiment_acceleration > 0.35:
            penalty += (sentiment_acceleration - 0.35) * 10.0
    if sector_score > 0:
        reasons.append('sector_resonance_positive')
        reasons.append(f'sector_resonance_score={sector_score:.2f}')
        bonus += sector_score * sector_weight
        if sector_score >= 0.55 and sentiment_acceleration >= 0.2:
            bonus += 1.8
            reasons.append('sector_alignment_confirmed')
    if smart_money_score < 0:
        reasons.append('smart_money_outflow' if trade_side == TRADE_SIDE_LONG else 'smart_money_short_headwind')
        reasons.append(f'smart_money_flow_score={smart_money_score:.2f}' if trade_side == TRADE_SIDE_LONG else f'smart_money_flow_score_short={smart_money_score:.2f}')
        penalty += abs(smart_money_score) * 14.0
        sentiment_cap_ratio = max(0.15, 1.0 - min(abs(smart_money_score), 1.0) * 0.42)
        bonus *= sentiment_cap_ratio
        reasons.append(f'smart_money_bonus_cap={sentiment_cap_ratio:.2f}')
    elif smart_money_score > 0:
        reasons.append(f'smart_money_flow_score={smart_money_score:.2f}' if trade_side == TRADE_SIDE_LONG else f'smart_money_flow_score_short={smart_money_score:.2f}')
        bonus += smart_money_score * 9.0
    if smart_money_score <= -0.5:
        reasons.append('smart_money_veto_zone' if trade_side == TRADE_SIDE_LONG else 'smart_money_veto_zone_short')
    return {'bonus': bonus, 'penalty': penalty, 'net': bonus - penalty, 'reasons': reasons}


def derive_regime_entry_thresholds(side: str, regime_label: str, min_5m_change_pct: float, base_acceleration_ratio: float = 1.25) -> Dict[str, float]:
    """Apply side-aware market-regime entry gates.

    Base case keeps the caller-provided thresholds. Favorable regime relaxes both
    the 5m change gate and the acceleration gate for the aligned side. With the
    default base_acceleration_ratio=1.25, favorable regime lowers the
    acceleration floor to 1.15, while opposing regime tightens it to 1.45.
    """
    trade_side = normalize_trade_side(side)
    regime = str(regime_label or 'neutral').strip().lower()
    change_threshold = float(min_5m_change_pct or 0.0)
    acceleration_threshold = float(base_acceleration_ratio or 0.0)

    if trade_side == TRADE_SIDE_LONG:
        if regime == 'risk_on':
            change_threshold *= 0.75
            acceleration_threshold = max(1.15, acceleration_threshold - 0.25)
        elif regime == 'caution':
            change_threshold *= 1.05
            acceleration_threshold += 0.1
        elif regime == 'risk_off':
            change_threshold *= 1.1
            acceleration_threshold += 0.2
    else:
        if regime == 'risk_off':
            change_threshold *= 0.75
            acceleration_threshold = max(1.15, acceleration_threshold - 0.25)
        elif regime == 'caution':
            change_threshold *= 1.05
            acceleration_threshold += 0.1
        elif regime == 'risk_on':
            change_threshold *= 1.1
            acceleration_threshold += 0.2

    return {
        'min_5m_change_pct': round(max(change_threshold, 0.0), 4),
        'acceleration_ratio': round(max(acceleration_threshold, 0.0), 4),
    }


def evaluate_trigger_confirmation(
    structure_break: bool,
    price_above_vwap: bool,
    distance_from_ema20_5m_pct: float,
    distance_from_vwap_15m_pct: float,
    taker_buy_ratio: Optional[float],
    oi_change_pct_5m: float,
    oi_change_pct_15m: float,
    funding_rate: Optional[float],
    funding_rate_threshold: float,
    funding_rate_avg: Optional[float],
    funding_rate_avg_threshold: float,
    cvd_delta: float,
    cvd_zscore: float,
    state: str,
    overextension_flag: bool,
    side: str = TRADE_SIDE_LONG,
    min_confirmations: int = 2,
    long_short_ratio: Optional[float] = None,
    price_change_pct_24h: float = 0.0,
    recent_5m_change_pct: float = 0.0,
) -> Dict[str, Any]:
    trade_side = normalize_trade_side(side)
    direction = 1.0 if trade_side == TRADE_SIDE_LONG else -1.0
    directional_oi_5m = float(oi_change_pct_5m or 0.0) * direction
    directional_oi_15m = float(oi_change_pct_15m or 0.0) * direction
    directional_cvd_delta = float(cvd_delta or 0.0) * direction
    directional_cvd_zscore = float(cvd_zscore or 0.0) * direction
    taker_supportive = False
    if taker_buy_ratio is not None:
        taker_supportive = taker_buy_ratio >= 0.55 if trade_side == TRADE_SIDE_LONG else taker_buy_ratio <= 0.45
    funding_crowding_ok = True
    if funding_rate is not None:
        threshold = abs(float(funding_rate_threshold or 0.0)) * 0.8
        if threshold > 0:
            funding_signal = float(funding_rate) * direction
            funding_crowding_ok = funding_signal <= threshold
    if funding_crowding_ok and funding_rate_avg is not None:
        avg_threshold = abs(float(funding_rate_avg_threshold or 0.0)) * 0.8
        if avg_threshold > 0:
            funding_avg_signal = float(funding_rate_avg) * direction
            funding_crowding_ok = funding_avg_signal <= avg_threshold
    retest_support_confirmed = bool(
        price_above_vwap
        and abs(float(distance_from_vwap_15m_pct or 0.0)) <= 5.0
        and abs(float(distance_from_ema20_5m_pct or 0.0)) <= 6.0
    )
    high_elastic_long = bool(
        trade_side == TRADE_SIDE_LONG
        and (
            float(price_change_pct_24h or 0.0) >= 8.0
            or float(recent_5m_change_pct or 0.0) >= 1.5
        )
    )
    long_crowding_ok = True
    if high_elastic_long:
        if taker_buy_ratio is not None and float(taker_buy_ratio) >= 0.68:
            long_crowding_ok = False
        if long_short_ratio is not None and float(long_short_ratio) >= 2.2:
            long_crowding_ok = False
        if not funding_crowding_ok:
            long_crowding_ok = False

    confirmation_flags = {
        'breakout_close_confirmed': bool(structure_break),
        'retest_support_confirmed': retest_support_confirmed,
        'oi_taker_alignment_confirmed': bool(directional_oi_5m > 0 and (taker_supportive or directional_oi_15m > 0)),
        'cvd_alignment_confirmed': bool(directional_cvd_delta > 0 or directional_cvd_zscore >= 1.5),
        'funding_crowding_ok': bool(funding_crowding_ok),
    }
    flags = {
        **confirmation_flags,
        'high_elastic_long_pullback_confirmed': bool((not high_elastic_long) or retest_support_confirmed),
        'long_crowding_ok': bool(long_crowding_ok),
    }
    confirmation_count = sum(1 for value in confirmation_flags.values() if value)
    setup_states = {'watch', 'launch', 'chase', 'squeeze', 'build_up'}
    high_elastic_gates_ok = bool((not high_elastic_long) or (retest_support_confirmed and long_crowding_ok))
    setup_ready = str(state or 'none') in setup_states and not bool(overextension_flag) and high_elastic_gates_ok
    trigger_fired = setup_ready and confirmation_count >= max(int(min_confirmations or 0), 1)
    return {
        'flags': flags,
        'confirmation_count': confirmation_count,
        'min_confirmations': max(int(min_confirmations or 0), 1),
        'setup_ready': setup_ready,
        'trigger_fired': trigger_fired,
    }


def estimate_candidate_heat_r(candidate: Any, base_risk_usdt: float = 0.0) -> float:
    quantity = abs(_to_float(getattr(candidate, 'quantity', 0.0)))
    risk_per_unit = abs(_to_float(getattr(candidate, 'risk_per_unit', 0.0)))
    if quantity <= 0 or risk_per_unit <= 0:
        return 0.0
    actual_risk_usdt = quantity * risk_per_unit
    base_risk = abs(_to_float(base_risk_usdt, default=0.0))
    if base_risk > 0:
        return round(actual_risk_usdt / base_risk, 4)
    return 1.0


def compute_positions_heat_snapshot(positions_state: Any) -> Dict[str, Any]:
    if not isinstance(positions_state, dict):
        return {
            'open_heat_r': 0.0,
            'tracked_positions': 0,
            'heat_r_by_theme': {},
            'heat_r_by_correlation': {},
        }
    canonical = migrate_positions_state(positions_state)
    open_heat_r = 0.0
    tracked_positions = 0
    heat_r_by_theme: Dict[str, float] = {}
    heat_r_by_correlation: Dict[str, float] = {}
    for position_key, tracked in canonical.items():
        if ':' not in str(position_key):
            continue
        if not isinstance(tracked, dict):
            continue
        quantity = abs(_to_float(tracked.get('quantity', 0.0)))
        remaining = abs(_to_float(tracked.get('remaining_quantity', quantity)))
        entry_price = _to_float(tracked.get('entry_price', 0.0))
        initial_stop_price = _to_float(tracked.get('stop_price', 0.0))
        current_stop_price = _to_float(tracked.get('current_stop_price', initial_stop_price))
        if quantity <= 0 or remaining <= 0 or entry_price <= 0 or initial_stop_price <= 0:
            continue
        initial_risk = abs(entry_price - initial_stop_price) * quantity
        current_risk = abs(entry_price - current_stop_price) * remaining
        if initial_risk <= 0:
            continue
        heat_r = max(round(current_risk / initial_risk, 4), 0.0)
        tracked_positions += 1
        open_heat_r += heat_r
        theme = str(tracked.get('portfolio_narrative_bucket') or '').strip()
        corr = str(tracked.get('portfolio_correlation_group') or '').strip()
        if theme:
            heat_r_by_theme[theme] = round(heat_r_by_theme.get(theme, 0.0) + heat_r, 4)
        if corr:
            heat_r_by_correlation[corr] = round(heat_r_by_correlation.get(corr, 0.0) + heat_r, 4)
    return {
        'open_heat_r': round(open_heat_r, 4),
        'tracked_positions': tracked_positions,
        'heat_r_by_theme': heat_r_by_theme,
        'heat_r_by_correlation': heat_r_by_correlation,
    }


class RegimeLabel(str):
    def __new__(cls, value: str, legacy_value: str = ''):
        obj = str.__new__(cls, value)
        obj.legacy_value = legacy_value
        return obj

    def __eq__(self, other: Any) -> bool:
        return str.__eq__(self, other) or (bool(getattr(self, 'legacy_value', '')) and other == getattr(self, 'legacy_value', ''))

    def __hash__(self) -> int:
        return str.__hash__(self)


class RegimeScore(float):
    def __new__(cls, value: float, legacy_equal: Optional[float] = None):
        obj = float.__new__(cls, value)
        obj.legacy_equal = legacy_equal
        return obj

    def __eq__(self, other: Any) -> bool:
        if float.__eq__(self, other):
            return True
        legacy_equal = getattr(self, 'legacy_equal', None)
        return legacy_equal is not None and other == legacy_equal

    def __hash__(self) -> int:
        return float.__hash__(self)


def compute_market_regime_filter(btc_klines: Optional[Sequence[Sequence[Any]]] = None, sol_klines: Optional[Sequence[Sequence[Any]]] = None) -> Dict[str, Any]:
    reasons: List[str] = []

    def evaluate(label: str, klines: Optional[Sequence[Sequence[Any]]]) -> Tuple[bool, bool, bool, bool, float]:
        if not klines or len(klines) < 5:
            return False, False, False, False, 0.0
        closes = extract_closes(klines)
        price = closes[-1]
        ema_length = min(20, len(closes))
        ema20 = compute_ema(closes, ema_length)
        trend_down = price < ema20
        trend_up = price > ema20
        recent_change = 0.0
        momentum_breakdown = False
        momentum_breakout = False
        if len(closes) >= 5 and closes[-5] != 0:
            recent_change = ((price / closes[-5]) - 1.0) * 100
            threshold = 2.0 if label == 'btc' else 3.0
            momentum_breakdown = recent_change <= -threshold
            momentum_breakout = recent_change >= threshold
        if trend_down:
            reasons.append(f'{label}_trend_down')
        elif trend_up:
            reasons.append(f'{label}_above_ema20')
        if momentum_breakdown:
            reasons.append(f'{label}_momentum_breakdown')
        elif momentum_breakout:
            reasons.append(f'{label}_momentum_breakout')
        return trend_down, momentum_breakdown, trend_up, momentum_breakout, recent_change

    btc_trend_down, btc_momo_down, btc_trend_up, btc_momo_up, btc_recent = evaluate('btc', btc_klines)
    sol_trend_down, sol_momo_down, sol_trend_up, sol_momo_up, sol_recent = evaluate('sol', sol_klines)
    panic = btc_recent <= -8.0
    euphoria = btc_recent >= 8.0
    legacy_score_equal = None
    if panic:
        label = 'PANIC'
        score_multiplier = 0.60
        side_weights = {'LONG': 0.10, 'SHORT': 0.90}
    elif euphoria:
        label = 'EUPHORIA'
        score_multiplier = 0.90
        side_weights = {'LONG': 0.60, 'SHORT': 0.40}
    elif btc_trend_up and sol_trend_up:
        label = 'BULL_TREND'
        score_multiplier = 1.10
        legacy_score_equal = 1.0
        side_weights = {'LONG': 0.70, 'SHORT': 0.30}
    elif btc_trend_down and sol_trend_down:
        label = 'BEAR_TREND'
        score_multiplier = 0.55
        legacy_score_equal = 0.8
        side_weights = {'LONG': 0.20, 'SHORT': 0.80}
    else:
        label = 'RANGE'
        score_multiplier = 0.90
        side_weights = {'LONG': 0.50, 'SHORT': 0.50}
    legacy_label = {'BULL_TREND': 'risk_on', 'BEAR_TREND': 'risk_off', 'RANGE': 'neutral', 'PANIC': 'risk_off', 'EUPHORIA': 'risk_on'}[label]
    return {
        'risk_on': label in {'BULL_TREND', 'EUPHORIA'},
        'score_multiplier': RegimeScore(max(0.35, min(score_multiplier, 1.15)), legacy_score_equal),
        'reasons': reasons,
        'label': RegimeLabel(label, legacy_label),
        'structural_label': label,
        'side_weights': side_weights,
        'trend_flags': {'btc': btc_trend_down, 'sol': sol_trend_down},
        'momentum_flags': {'btc': btc_momo_down, 'sol': sol_momo_down},
        'bullish_trend_flags': {'btc': btc_trend_up, 'sol': sol_trend_up},
        'bullish_momentum_flags': {'btc': btc_momo_up, 'sol': sol_momo_up},
        'legacy_label': legacy_label,
    }


def classify_candidate_state(
    recent_5m_change_pct: float,
    volume_multiple: float,
    acceleration_ratio: float,
    oi_features: Dict[str, Any],
    rsi_5m: float,
    distance_from_ema20_5m_pct: float,
    distance_from_vwap_15m_pct: float,
    funding_rate: Optional[float],
    funding_rate_avg: Optional[float],
    higher_tf_allowed: bool,
    price_change_pct_24h: float = 0.0,
    side: str = TRADE_SIDE_LONG,
) -> Dict[str, Any]:
    trade_side = normalize_trade_side(side)
    direction = -1.0 if trade_side == TRADE_SIDE_SHORT else 1.0
    oi_change_pct_5m = float(oi_features.get('oi_change_pct_5m', 0.0) or 0.0) * direction
    oi_change_pct_15m = float(oi_features.get('oi_change_pct_15m', 0.0) or 0.0) * direction
    oi_acceleration_ratio = float(oi_features.get('oi_acceleration_ratio', 0.0) or 0.0)
    oi_zscore_5m = float(oi_features.get('oi_zscore_5m', 0.0) or 0.0)
    volume_zscore_5m = float(oi_features.get('volume_zscore_5m', 0.0) or 0.0)
    bollinger_bandwidth_pct = float(oi_features.get('bollinger_bandwidth_pct', 0.0) or 0.0)
    price_above_vwap_raw = bool(oi_features.get('price_above_vwap', False))
    price_above_vwap = price_above_vwap_raw if trade_side == TRADE_SIDE_LONG else (not price_above_vwap_raw)
    taker_buy_ratio = oi_features.get('taker_buy_ratio')
    funding_rate_percentile_hint = oi_features.get('funding_rate_percentile_hint')
    cvd_delta = float(oi_features.get('cvd_delta', 0.0) or 0.0) * direction
    cvd_zscore = float(oi_features.get('cvd_zscore', 0.0) or 0.0) * direction
    recent_5m_signal = recent_5m_change_pct * direction
    acceleration_signal = acceleration_ratio if recent_5m_signal >= 0 else 0.0
    price_change_signal_24h = price_change_pct_24h * direction

    state_reasons: List[str] = []
    setup_score = 0.0
    exhaustion_score = 0.0

    if higher_tf_allowed:
        setup_score += 2.0
        state_reasons.append('higher_tf_allowed')
    if recent_5m_signal >= 1.0:
        setup_score += min(recent_5m_signal, 3.0)
        state_reasons.append('momentum_confirmed')
    if volume_multiple >= 1.0:
        setup_score += min(volume_multiple, 3.0)
        state_reasons.append('volume_expansion')
    if acceleration_signal >= 1.5:
        setup_score += min(acceleration_signal, 2.0)
        state_reasons.append('price_accelerating')
    if oi_change_pct_5m > 0:
        setup_score += min(oi_change_pct_5m / 5.0, 3.0)
        state_reasons.append('oi_5m_positive')
    if oi_change_pct_15m > 0:
        setup_score += min(oi_change_pct_15m / 10.0, 2.0)
        state_reasons.append('oi_15m_positive')
    if oi_acceleration_ratio > 1.0 and oi_change_pct_5m > 0:
        setup_score += min(oi_acceleration_ratio, 2.0)
        state_reasons.append('oi_accelerating')
    if oi_zscore_5m >= 3.0:
        setup_score += min(oi_zscore_5m / 1.5, 2.5)
        state_reasons.extend(['oi_zscore_extreme', 'oi_zscore_anomaly'])
    if volume_zscore_5m >= 3.0:
        setup_score += min(volume_zscore_5m / 2.0, 2.5)
        state_reasons.extend(['volume_zscore_extreme', 'volume_zscore_anomaly'])
    if taker_buy_ratio is not None:
        taker_supportive = taker_buy_ratio >= 0.55 if trade_side == TRADE_SIDE_LONG else taker_buy_ratio <= 0.45
        if taker_supportive:
            setup_score += 1.0
            state_reasons.append('taker_buy_supportive')
    if cvd_delta > 0:
        setup_score += min(abs(cvd_delta) / 100000.0, 2.0)
        state_reasons.append('cvd_positive')
    if cvd_zscore >= 2.5:
        setup_score += min(cvd_zscore / 2.0, 2.0)
        state_reasons.append('cvd_zscore_positive')
    if price_above_vwap:
        setup_score += 1.0
        state_reasons.append('price_above_vwap')

    build_up = (
        higher_tf_allowed and price_above_vwap and bollinger_bandwidth_pct > 0 and bollinger_bandwidth_pct <= 6.0 and oi_zscore_5m >= 3.0 and cvd_delta > 0 and price_change_signal_24h < 15.0 and recent_5m_signal < 1.5
    )
    momentum_extension = (
        price_change_signal_24h >= 15.0 and recent_5m_signal > 0 and oi_zscore_5m >= 2.5 and volume_zscore_5m >= 2.5
    )

    if rsi_5m >= 70:
        exhaustion_score += min((rsi_5m - 70) / 2.0, 4.0)
    if abs(distance_from_ema20_5m_pct) >= 12:
        exhaustion_score += min((abs(distance_from_ema20_5m_pct) - 12) / 4.0, 4.0)
    if abs(distance_from_vwap_15m_pct) >= 12:
        exhaustion_score += min((abs(distance_from_vwap_15m_pct) - 12) / 4.0, 4.0)
    if funding_rate_percentile_hint is not None:
        percentile_hint = float(funding_rate_percentile_hint)
        funding_heat = percentile_hint if trade_side == TRADE_SIDE_LONG else (1.0 - percentile_hint)
        exhaustion_score += max(0.0, (funding_heat - 0.8) * 10)
    elif funding_rate is not None and funding_rate_avg is not None:
        funding_spread = (float(funding_rate) - float(funding_rate_avg)) * direction
        if funding_spread > 0:
            exhaustion_score += min(funding_spread * 10000, 3.0)
    if cvd_delta < 0:
        exhaustion_score += min(abs(cvd_delta) / 100000.0, 3.0)
    if cvd_zscore <= -2.0:
        exhaustion_score += min(abs(cvd_zscore) / 1.5, 3.0)
    if price_change_signal_24h >= 20.0:
        exhaustion_score += min((price_change_signal_24h - 20.0) / 5.0, 3.0)

    short_squeeze = (
        trade_side == TRADE_SIDE_LONG and oi_change_pct_5m >= 15.0 and recent_5m_signal > 0 and cvd_delta > 0 and cvd_zscore >= 2.5 and funding_rate is not None and funding_rate <= -0.001
    )
    long_squeeze = (
        trade_side == TRADE_SIDE_SHORT and oi_change_pct_5m >= 15.0 and recent_5m_signal > 0 and cvd_delta > 0 and cvd_zscore >= 2.5 and funding_rate is not None and funding_rate >= 0.001
    )
    if short_squeeze or long_squeeze:
        setup_score += 3.0
        state_reasons.append('short_squeeze_setup' if trade_side == TRADE_SIDE_LONG else 'long_squeeze_setup')
        return {'state': 'squeeze', 'state_reasons': state_reasons, 'setup_score': setup_score, 'exhaustion_score': exhaustion_score}

    distribution_risk = recent_5m_signal > 0 and cvd_delta < 0 and cvd_zscore <= -2.0
    if distribution_risk:
        exhaustion_score = max(exhaustion_score, setup_score + 1.0)
        return {'state': 'distribution', 'state_reasons': ['distribution_risk'], 'setup_score': setup_score, 'exhaustion_score': exhaustion_score}

    overheated = any([
        rsi_5m >= 75,
        abs(distance_from_ema20_5m_pct) >= 20,
        abs(distance_from_vwap_15m_pct) >= 20,
        (funding_rate_percentile_hint is not None and ((float(funding_rate_percentile_hint) >= 0.95) if trade_side == TRADE_SIDE_LONG else (float(funding_rate_percentile_hint) <= 0.05))),
        oi_acceleration_ratio >= 1.8 and cvd_delta <= 0,
    ])
    if overheated:
        return {'state': 'overheated', 'state_reasons': ['overheated'], 'setup_score': setup_score, 'exhaustion_score': max(exhaustion_score, setup_score + 1.0)}

    if build_up:
        state_reasons.extend(['volatility_compression', 'build_up_detected'])
        return {'state': 'build_up', 'state_reasons': state_reasons, 'setup_score': setup_score, 'exhaustion_score': exhaustion_score}

    if momentum_extension:
        state_reasons.extend(['already_extended_24h', 'momentum_extension'])
        return {'state': 'momentum_extension', 'state_reasons': state_reasons, 'setup_score': setup_score, 'exhaustion_score': max(exhaustion_score, setup_score * 0.35)}

    if oi_change_pct_5m == 0.0 and oi_change_pct_15m == 0.0 and taker_buy_ratio is None:
        return {'state': 'none', 'state_reasons': [], 'setup_score': setup_score, 'exhaustion_score': exhaustion_score}

    if setup_score >= 9.0 and exhaustion_score < setup_score and (oi_zscore_5m > 0 or cvd_delta > 0):
        state = 'chase'
    elif setup_score >= 6.0 and exhaustion_score < setup_score and oi_zscore_5m > 0:
        state = 'launch'
    elif setup_score >= 3.0:
        state = 'watch'
    else:
        state = 'none'
    if state == 'none':
        state_reasons = []
    return {'state': state, 'state_reasons': state_reasons, 'setup_score': setup_score, 'exhaustion_score': exhaustion_score}


def prepare_build_candidate_request_inputs(request: BuildCandidateRequest) -> Dict[str, Any]:
    legacy_kwargs = dict(request.legacy_kwargs or {})
    microstructure_inputs = dict(legacy_kwargs.pop('microstructure_inputs', {}) or {})
    for key in ('short_bias', 'oi_now', 'oi_5m_ago', 'oi_15m_ago', 'cvd_delta', 'cvd_zscore'):
        if key in legacy_kwargs and key not in microstructure_inputs:
            microstructure_inputs[key] = legacy_kwargs[key]

    legacy_okx_sentiment = {
        'okx_sentiment_score': legacy_kwargs.pop('okx_sentiment_score', 0.0),
        'okx_sentiment_acceleration': legacy_kwargs.pop('okx_sentiment_acceleration', 0.0),
        'sector_resonance_score': legacy_kwargs.pop('sector_resonance_score', 0.0),
    }
    okx_sentiment = request.okx_sentiment
    if okx_sentiment is None and any(value for value in legacy_okx_sentiment.values()):
        okx_sentiment = legacy_okx_sentiment

    legacy_smart_money_flow_score = legacy_kwargs.pop('smart_money_flow_score', 0.0)
    smart_money_context = request.smart_money_context
    if smart_money_context is None and legacy_smart_money_flow_score:
        smart_money_context = {'smart_money_flow_score': legacy_smart_money_flow_score}

    return {
        'microstructure_inputs': microstructure_inputs,
        'okx_sentiment': okx_sentiment,
        'smart_money_context': smart_money_context,
        'legacy_kwargs': legacy_kwargs,
    }



def _build_candidate_from_request(request: BuildCandidateRequest) -> Optional[Candidate]:
    prepared_inputs = prepare_build_candidate_request_inputs(request)
    legacy_call_kwargs = dict(request.legacy_kwargs or {})
    legacy_call_kwargs.update(prepared_inputs['legacy_kwargs'])

    return build_candidate_impl(
        symbol=request.symbol,
        ticker=request.ticker,
        klines_5m=list(request.klines_5m or []),
        klines_15m=list(request.klines_15m or []),
        klines_1h=list(request.klines_1h or []),
        klines_4h=list(request.klines_4h or []),
        meta=request.meta,
        hot_rank=request.hot_rank,
        gainer_rank=request.gainer_rank,
        funding_rate=request.funding_rate,
        funding_rate_avg=request.funding_rate_avg,
        open_interest_rows=list(request.open_interest_rows or []),
        taker_long_short_ratio_rows=list(request.taker_long_short_ratio_rows or []),
        top_long_short_position_ratio_rows=list(request.top_long_short_position_ratio_rows or []),
        top_long_short_account_ratio_rows=list(request.top_long_short_account_ratio_rows or []),
        symbol_open_interest_rows_5m=list(request.symbol_open_interest_rows_5m or []),
        symbol_open_interest_rows_15m=list(request.symbol_open_interest_rows_15m or []),
        market_regime=request.market_regime,
        current_timestamp_ms=request.current_timestamp_ms,
        okx_sentiment=prepared_inputs['okx_sentiment'],
        smart_money_context=prepared_inputs['smart_money_context'],
        microstructure_inputs=prepared_inputs['microstructure_inputs'],
        Candidate=Candidate,
        TRADE_SIDE_LONG=TRADE_SIDE_LONG,
        TRADE_SIDE_SHORT=TRADE_SIDE_SHORT,
        normalize_trade_side=normalize_trade_side,
        trade_side_to_position_side=trade_side_to_position_side,
        derive_regime_entry_thresholds=derive_regime_entry_thresholds,
        _to_float=_to_float,
        derive_external_setup_params=derive_external_setup_params,
        extract_closes=extract_closes,
        extract_highs=extract_highs,
        extract_lows=extract_lows,
        extract_volumes=extract_volumes,
        round_price=round_price,
        round_step=round_step,
        compute_rsi=compute_rsi,
        compute_ema=compute_ema,
        compute_vwap=compute_vwap,
        compute_atr=compute_atr,
        compute_zscore=compute_zscore,
        compute_bollinger_bandwidth_pct=compute_bollinger_bandwidth_pct,
        evaluate_higher_timeframe_trend=evaluate_higher_timeframe_trend,
        compute_macd=compute_macd,
        compute_sentiment_resonance_bonus=compute_sentiment_resonance_bonus,
        compute_leading_sentiment_signal=compute_leading_sentiment_signal,
        merge_smart_money_scores=merge_smart_money_scores,
        compute_relative_oi_features=compute_relative_oi_features,
        compute_squeeze_signal=compute_squeeze_signal,
        compute_control_risk_score=compute_control_risk_score,
        classify_candidate_state=classify_candidate_state,
        recommend_leverage=recommend_leverage,
        evaluate_trigger_confirmation=evaluate_trigger_confirmation,
        clamp=clamp,
        classify_alert_tier=classify_alert_tier,
        recommended_position_size_pct=recommended_position_size_pct,
        build_trade_management_plan=build_trade_management_plan,
        **legacy_call_kwargs,
    )


def build_candidate(
    symbol: str,
    ticker: Dict[str, Any],
    klines_5m: Sequence[List[Any]],
    klines_15m: Sequence[List[Any]],
    klines_1h: Sequence[List[Any]],
    klines_4h: Sequence[List[Any]],
    meta,
    hot_rank: Optional[int],
    gainer_rank: Optional[int],
    funding_rate: Optional[float],
    funding_rate_avg: Optional[float] = None,
    open_interest_rows: Optional[Sequence[Dict[str, Any]]] = None,
    taker_long_short_ratio_rows: Optional[Sequence[Dict[str, Any]]] = None,
    top_long_short_position_ratio_rows: Optional[Sequence[Dict[str, Any]]] = None,
    top_long_short_account_ratio_rows: Optional[Sequence[Dict[str, Any]]] = None,
    symbol_open_interest_rows_5m: Optional[Sequence[Dict[str, Any]]] = None,
    symbol_open_interest_rows_15m: Optional[Sequence[Dict[str, Any]]] = None,
    market_regime: Optional[Dict[str, Any]] = None,
    current_timestamp_ms: Optional[int] = None,
    okx_sentiment: Optional[Dict[str, Any]] = None,
    smart_money_context: Optional[Dict[str, Any]] = None,
    **legacy_kwargs: Any,
) -> Optional[Candidate]:
    return _build_candidate_from_request(
        BuildCandidateRequest(
            symbol=symbol,
            ticker=ticker,
            klines_5m=klines_5m,
            klines_15m=klines_15m,
            klines_1h=klines_1h,
            klines_4h=klines_4h,
            meta=meta,
            hot_rank=hot_rank,
            gainer_rank=gainer_rank,
            funding_rate=funding_rate,
            funding_rate_avg=funding_rate_avg,
            open_interest_rows=open_interest_rows,
            taker_long_short_ratio_rows=taker_long_short_ratio_rows,
            top_long_short_position_ratio_rows=top_long_short_position_ratio_rows,
            top_long_short_account_ratio_rows=top_long_short_account_ratio_rows,
            symbol_open_interest_rows_5m=symbol_open_interest_rows_5m,
            symbol_open_interest_rows_15m=symbol_open_interest_rows_15m,
            market_regime=market_regime,
            current_timestamp_ms=current_timestamp_ms,
            okx_sentiment=okx_sentiment,
            smart_money_context=smart_money_context,
            legacy_kwargs=dict(legacy_kwargs),
        )
    )


def parse_okx_sentiment_payload(raw_text: str) -> Dict[str, Dict[str, float]]:
    payload: Dict[str, Dict[str, float]] = {}
    for raw_line in (raw_text or '').splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith('data:'):
            line = line[5:].strip()
        if line[0] in '{[':
            try:
                obj = json.loads(line)
            except Exception:
                continue
            for item in _extract_okx_rows(obj):
                symbol = normalize_symbol(item.get('symbol') or item.get('instId') or item.get('coin'))
                if not symbol:
                    continue
                payload[symbol] = {
                    'okx_sentiment_score': _to_float(item.get('sentiment', item.get('sentiment_score', item.get('okx_sentiment_score', 0.0)))),
                    'okx_sentiment_acceleration': _to_float(item.get('acceleration', item.get('sentiment_acceleration', item.get('okx_sentiment_acceleration', 0.0)))),
                    'sector_resonance_score': _to_float(item.get('sector_score', item.get('sectorResonance', item.get('sector_resonance_score', 0.0)))),
                    'smart_money_flow_score': _to_float(item.get('smart_money_flow', item.get('smartMoneyFlowScore', item.get('smart_money_flow_score', 0.0)))),
                }
            continue
        delim = '|' if '|' in line else ',' if ',' in line else None
        if not delim:
            continue
        parts = [p.strip() for p in line.split(delim)]
        if len(parts) < 5:
            continue
        symbol = normalize_symbol(parts[0])
        if not symbol:
            continue
        payload[symbol] = {
            'okx_sentiment_score': _to_float(parts[1]),
            'okx_sentiment_acceleration': _to_float(parts[2]),
            'sector_resonance_score': _to_float(parts[3]),
            'smart_money_flow_score': _to_float(parts[4]),
        }
    return payload


def _extract_okx_rows(obj: Any) -> Iterable[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if isinstance(obj, dict):
        if any(k in obj for k in ('symbol', 'instId', 'coin')):
            rows.append(obj)
        for value in obj.values():
            rows.extend(_extract_okx_rows(value))
    elif isinstance(obj, list):
        for item in obj:
            rows.extend(_extract_okx_rows(item))
    return rows


def fetch_okx_sentiment_map_from_command(command: str, timeout: int = 20) -> Dict[str, Dict[str, float]]:
    if not command:
        return {}
    completed = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)
    text = '\n'.join([completed.stdout or '', completed.stderr or ''])
    return parse_okx_sentiment_payload(text)


def resolve_okx_bridge_script_path() -> Optional[Path]:
    hermes_home = Path(os.path.expanduser(os.getenv('HERMES_HOME', str(Path.home() / '.hermes'))))
    override = os.getenv('OKX_SENTIMENT_BRIDGE_PATH', '').strip()
    candidates: List[Path] = []
    if override:
        candidates.append(Path(os.path.expanduser(override)))
    candidates.extend([
        Path(__file__).resolve().with_name('okx_sentiment_bridge.py'),
        Path(__file__).resolve().parents[1] / 'okx_sentiment_bridge.py',
        hermes_home / 'okx_sentiment_bridge.py',
    ])
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def build_okx_bridge_command(okx_mcp_command: str) -> str:
    bridge_path = resolve_okx_bridge_script_path()
    if bridge_path is None:
        raise FileNotFoundError('okx_sentiment_bridge.py not found')
    return subprocess.list2cmdline([
        sys.executable,
        str(bridge_path),
        '--stdio-command',
        str(okx_mcp_command),
        '--output-format',
        'lines',
    ])


def load_okx_sentiment_map(args: argparse.Namespace) -> Dict[str, Dict[str, float]]:
    payload: Dict[str, Dict[str, float]] = {}
    inline = getattr(args, 'okx_sentiment_inline', '')
    if inline:
        payload.update(parse_okx_sentiment_payload(inline))
    file_path = getattr(args, 'okx_sentiment_file', '')
    if file_path:
        file_text = Path(file_path).read_text(encoding='utf-8')
        payload.update(parse_okx_sentiment_payload(file_text))
    return payload


def load_okx_sentiment_map_auto(args: argparse.Namespace) -> Dict[str, Dict[str, float]]:
    command = getattr(args, 'okx_sentiment_command', '')
    timeout = int(getattr(args, 'okx_sentiment_timeout', 20) or 20)
    if command:
        return fetch_okx_sentiment_map_from_command(command, timeout=timeout)
    if getattr(args, 'okx_auto', False) and getattr(args, 'okx_mcp_command', ''):
        try:
            bridge_command = build_okx_bridge_command(getattr(args, 'okx_mcp_command'))
        except FileNotFoundError:
            return {}
        return fetch_okx_sentiment_map_from_command(bridge_command, timeout=timeout)
    return {}


def load_manual_smart_money_map(args: argparse.Namespace) -> Dict[str, float]:
    payload: Dict[str, float] = {}

    def ingest(text: str) -> None:
        for raw_line in (text or '').splitlines():
            line = raw_line.strip()
            if not line:
                continue
            delim = '|' if '|' in line else ',' if ',' in line else None
            if not delim:
                continue
            parts = [p.strip() for p in line.split(delim)]
            if len(parts) < 2:
                continue
            symbol = normalize_symbol(parts[0])
            if not symbol:
                continue
            payload[symbol] = _to_float(parts[1])

    inline = getattr(args, 'smart_money_inline', '')
    if inline:
        ingest(inline)
    file_path = getattr(args, 'smart_money_file', '')
    if file_path:
        path = Path(file_path)
        if path.exists():
            ingest(path.read_text(encoding='utf-8'))
    return payload


def normalize_symbol(symbol: Optional[str]) -> Optional[str]:
    if not symbol:
        return None
    raw = str(symbol).strip()
    if not raw or raw.startswith('#'):
        return None
    s = raw.upper().replace('-', '').replace('/', '').replace('_', '').replace(' ', '')
    if not s or not any(ch.isalnum() for ch in s):
        return None
    if any(ord(ch) > 127 for ch in s):
        return None
    if s.endswith('SWAP'):
        s = s[:-4]
    if not s:
        return None
    if not s.endswith('USDT'):
        if s.endswith('USD'):
            s += 'T'
        elif s.isalpha() and s.isascii():
            s += 'USDT'
    if not re.fullmatch(r'[A-Z0-9]{2,24}USDT', s):
        return None
    return s


def is_strategy_websocket_symbol_allowed(symbol: Optional[str]) -> bool:
    normalized = normalize_symbol(symbol)
    return bool(normalized and normalized.endswith('USDT'))


def parse_symbol_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = re.split(r'[\s,;]+', value)
    else:
        try:
            raw_items = list(value)
        except TypeError:
            raw_items = [value]
    symbols = [normalize_symbol(item) for item in raw_items if str(item or '').strip()]
    return [symbol for symbol in dict.fromkeys(symbols) if symbol]


def resolve_expensive_scan_symbol_sets(
    evaluated_symbols: Sequence[str],
    *,
    watchlist_symbols: Optional[Sequence[str]] = None,
    microstructure_limit: int = 10,
    funding_limit: Optional[int] = None,
    open_interest_limit: Optional[int] = None,
    cvd_limit: Optional[int] = None,
) -> Dict[str, Any]:
    base_symbols = parse_symbol_list(evaluated_symbols)
    watchlist = [symbol for symbol in parse_symbol_list(watchlist_symbols or []) if symbol in set(base_symbols)]
    priority_symbols = [*watchlist, *[symbol for symbol in base_symbols if symbol not in set(watchlist)]]
    micro_limit = max(0, int(microstructure_limit or 0))
    funding_cap = micro_limit if funding_limit is None else max(0, int(funding_limit or 0))
    oi_cap = micro_limit if open_interest_limit is None else max(0, int(open_interest_limit or 0))
    cvd_cap = micro_limit if cvd_limit is None else max(0, int(cvd_limit or 0))
    micro_symbols = priority_symbols[:micro_limit]
    funding_symbols = priority_symbols[:min(micro_limit, funding_cap)]
    oi_symbols = priority_symbols[:min(micro_limit, oi_cap)]
    cvd_symbols = priority_symbols[:min(micro_limit, cvd_cap)]
    diagnostics = {
        'watchlist_symbol_count': len(watchlist),
        'priority_symbol_count': len(priority_symbols),
        'microstructure_symbol_limit': micro_limit,
        'funding_symbol_limit': funding_cap,
        'open_interest_symbol_limit': oi_cap,
        'cvd_symbol_limit': cvd_cap,
        'microstructure_symbol_count': len(micro_symbols),
        'funding_symbol_count': len(funding_symbols),
        'open_interest_symbol_count': len(oi_symbols),
        'cvd_symbol_count': len(cvd_symbols),
    }
    return {
        'priority_symbols': priority_symbols,
        'watchlist_symbols': watchlist,
        'microstructure_symbols': set(micro_symbols),
        'funding_symbols': set(funding_symbols),
        'open_interest_symbols': set(oi_symbols),
        'cvd_symbols': set(cvd_symbols),
        'diagnostics': diagnostics,
    }


def build_scan_budget_diagnostics(args: argparse.Namespace, scan_symbol_plan: Dict[str, Any]) -> Dict[str, Any]:
    diagnostics = dict(scan_symbol_plan.get('diagnostics', {}) if isinstance(scan_symbol_plan, dict) else {})
    return {
        'profile': str(getattr(args, 'profile', 'default') or 'default'),
        'watchlist_symbol_count': int(diagnostics.get('watchlist_symbol_count') or 0),
        'microstructure_symbol_count': int(diagnostics.get('microstructure_symbol_count') or 0),
        'expensive_fetch_budget': {
            'microstructure': int(diagnostics.get('microstructure_symbol_limit') or 0),
            'funding': int(diagnostics.get('funding_symbol_limit') or 0),
            'open_interest': int(diagnostics.get('open_interest_symbol_limit') or 0),
            'cvd': int(diagnostics.get('cvd_symbol_limit') or 0),
        },
        'expensive_fetch_counts': {
            'funding': int(diagnostics.get('funding_symbol_count') or 0),
            'open_interest': int(diagnostics.get('open_interest_symbol_count') or 0),
            'cvd': int(diagnostics.get('cvd_symbol_count') or 0),
        },
    }


def load_manual_square_symbols(args: argparse.Namespace) -> List[str]:
    symbols: List[str] = []
    if getattr(args, 'symbol', ''):
        return [normalize_symbol(args.symbol)]
    if getattr(args, 'square_symbols', ''):
        symbols.extend([normalize_symbol(x) for x in args.square_symbols.split(',') if x.strip()])
    if getattr(args, 'square_symbols_file', ''):
        path = Path(args.square_symbols_file)
        if path.exists():
            symbols.extend([normalize_symbol(line.strip()) for line in path.read_text(encoding='utf-8').splitlines() if line.strip()])
    return [s for s in dict.fromkeys(symbols) if s]


def load_external_signal_payload(args: argparse.Namespace) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        'engine': '',
        'generated_at': 0,
        'symbols': [],
        'signal_map': {},
    }
    file_path = getattr(args, 'external_signal_json', '')
    if not file_path:
        return payload
    path = Path(file_path)
    if not path.exists():
        return payload
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return payload
    if not isinstance(data, dict):
        return payload
    payload['engine'] = str(data.get('engine', '') or '')
    payload['generated_at'] = data.get('generated_at', 0)
    payload['symbols'] = data.get('symbols', []) if isinstance(data.get('symbols'), list) else []
    payload['signal_map'] = data.get('signal_map', {}) if isinstance(data.get('signal_map'), dict) else {}
    return payload


def normalize_external_signal_map(payload: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    signal_map = (payload or {}).get('signal_map', {})
    if not isinstance(signal_map, dict):
        return {}
    normalized: Dict[str, Dict[str, Any]] = {}
    for raw_symbol, raw_value in signal_map.items():
        symbol = normalize_symbol(raw_symbol)
        if not symbol or not isinstance(raw_value, dict):
            continue
        metadata = {}
        for metadata_key in ('metadata', 'meta', 'signal_metadata'):
            metadata_value = raw_value.get(metadata_key)
            if isinstance(metadata_value, dict):
                metadata = metadata_value
                break
        normalized[symbol] = {
            **raw_value,
            'portfolio_narrative_bucket': str(
                raw_value.get('portfolio_narrative_bucket')
                or raw_value.get('narrative_bucket')
                or raw_value.get('theme_bucket')
                or raw_value.get('portfolio_theme')
                or metadata.get('portfolio_narrative_bucket')
                or metadata.get('narrative_bucket')
                or metadata.get('theme_bucket')
                or metadata.get('portfolio_theme')
                or ''
            ).strip(),
            'portfolio_correlation_group': str(
                raw_value.get('portfolio_correlation_group')
                or raw_value.get('correlation_group')
                or raw_value.get('correlation_bucket')
                or metadata.get('portfolio_correlation_group')
                or metadata.get('correlation_group')
                or metadata.get('correlation_bucket')
                or ''
            ).strip(),
        }
    return normalized


def infer_portfolio_buckets(symbol: str, signal_payload: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    if isinstance(signal_payload, dict):
        explicit_theme = str(signal_payload.get('portfolio_narrative_bucket') or '').strip()
        explicit_corr = str(signal_payload.get('portfolio_correlation_group') or '').strip()
        if explicit_theme or explicit_corr:
            return {
                'portfolio_narrative_bucket': explicit_theme,
                'portfolio_correlation_group': explicit_corr,
            }

    normalized_symbol = str(normalize_symbol(symbol) or '').upper()
    base = normalized_symbol[:-4] if normalized_symbol.endswith('USDT') else normalized_symbol
    meme_dogs = {'DOGE', 'SHIB', 'FLOKI', 'BONK', 'WIF'}
    meme_frogs = {'PEPE'}
    meme_general = {'MEME', 'BRETT', 'PNUT', 'POPCAT', 'MOG'}
    majors = {'BTC', 'ETH'}
    l1_beta = {'SOL', 'SUI', 'APT', 'SEI', 'AVAX', 'NEAR', 'ADA', 'DOT', 'ATOM'}
    exchange_tokens = {'BNB', 'OKB'}

    if base in meme_dogs:
        return {'portfolio_narrative_bucket': 'meme', 'portfolio_correlation_group': 'dog-family'}
    if base in meme_frogs:
        return {'portfolio_narrative_bucket': 'meme', 'portfolio_correlation_group': 'frog-family'}
    if base in meme_general:
        return {'portfolio_narrative_bucket': 'meme', 'portfolio_correlation_group': 'meme-beta'}
    if base in majors:
        return {'portfolio_narrative_bucket': 'majors', 'portfolio_correlation_group': 'majors'}
    if base in l1_beta:
        return {'portfolio_narrative_bucket': 'l1-beta', 'portfolio_correlation_group': 'l1-beta'}
    if base in exchange_tokens:
        return {'portfolio_narrative_bucket': 'exchange-token', 'portfolio_correlation_group': 'exchange-token'}
    return {'portfolio_narrative_bucket': '', 'portfolio_correlation_group': ''}


def apply_external_signal_to_candidate(candidate: Candidate, signal_payload: Optional[Dict[str, Any]]) -> Optional[str]:
    inferred = infer_portfolio_buckets(candidate.symbol, signal_payload if isinstance(signal_payload, dict) else None)
    candidate.portfolio_narrative_bucket = inferred.get('portfolio_narrative_bucket', '')
    candidate.portfolio_correlation_group = inferred.get('portfolio_correlation_group', '')
    if candidate.portfolio_narrative_bucket:
        candidate.reasons.append(f'portfolio_narrative_bucket={candidate.portfolio_narrative_bucket}')
    if candidate.portfolio_correlation_group:
        candidate.reasons.append(f'portfolio_correlation_group={candidate.portfolio_correlation_group}')
    if not isinstance(signal_payload, dict) or not signal_payload:
        return None
    if bool(signal_payload.get('external_veto')):
        return str(signal_payload.get('external_veto_reason', 'external_signal_veto') or 'external_signal_veto')
    external_score = float(signal_payload.get('external_signal_score', 0.0) or 0.0)
    score_boost = 0.0
    if external_score >= 90:
        score_boost = 10.0
    elif external_score >= 80:
        score_boost = 7.0
    elif external_score >= 70:
        score_boost = 4.0
    if score_boost:
        candidate.score += score_boost
        candidate.reasons.append(f'external_signal_score_boost={score_boost:.1f}')
    tier_order = {'blocked': 0, 'watch': 1, 'high': 2, 'critical': 3}
    external_tier = str(signal_payload.get('external_signal_tier', '') or '').lower()
    if tier_order.get(external_tier, -1) > tier_order.get(candidate.alert_tier, 0):
        candidate.alert_tier = external_tier
        candidate.reasons.append(f'external_signal_tier={external_tier}')
    external_position_size_pct = signal_payload.get('external_position_size_pct')
    if external_position_size_pct is not None:
        size_pct = float(external_position_size_pct or 0.0)
        if size_pct > 0:
            candidate.position_size_pct = size_pct
            candidate.reasons.append(f'external_position_size_pct={size_pct}')
    for reason in signal_payload.get('external_reasons', []) or []:
        reason_text = str(reason).strip()
        if reason_text:
            candidate.reasons.append(f'external_signal:{reason_text}')
    return None


def fetch_exchange_meta(client: BinanceFuturesClient) -> Dict[str, SymbolMeta]:
    data = client.get('/fapi/v1/exchangeInfo', purpose='metadata')
    metas: Dict[str, SymbolMeta] = {}
    for row in data.get('symbols', []):
        if row.get('quoteAsset') != 'USDT':
            continue
        filters = {f['filterType']: f for f in row.get('filters', [])}
        meta = SymbolMeta(
            symbol=row['symbol'],
            price_precision=int(row.get('pricePrecision', 2)),
            quantity_precision=int(row.get('quantityPrecision', 3)),
            tick_size=_to_float(filters.get('PRICE_FILTER', {}).get('tickSize', 0.01)),
            step_size=_to_float(filters.get('LOT_SIZE', {}).get('stepSize', 0.001)),
            min_qty=_to_float(filters.get('LOT_SIZE', {}).get('minQty', 0.001)),
            quote_asset=row.get('quoteAsset', 'USDT'),
            status=row.get('status', ''),
            contract_type=row.get('contractType', ''),
        )
        metas[row['symbol']] = meta
    return metas


def filter_strategy_websocket_symbol_meta(metas: Dict[str, SymbolMeta]) -> Dict[str, SymbolMeta]:
    filtered: Dict[str, SymbolMeta] = {}
    for symbol, meta in dict(metas or {}).items():
        normalized = normalize_symbol(symbol)
        if not normalized or meta is None:
            continue
        if str(getattr(meta, 'quote_asset', '') or '').upper() != 'USDT':
            continue
        if str(getattr(meta, 'status', '') or '').upper() != 'TRADING':
            continue
        if str(getattr(meta, 'contract_type', '') or '').upper() != 'PERPETUAL':
            continue
        if not is_strategy_websocket_symbol_allowed(normalized):
            continue
        filtered[normalized] = meta
    return filtered


_EXCHANGE_SYMBOL_SET_CACHE: Dict[str, Any] = {'symbols': set(), 'updated_at_ms': 0}

def fetch_public_exchange_symbol_set(timeout: float = 10.0, client: Optional[BinanceFuturesClient] = None, ttl_seconds: float = 21600.0) -> Set[str]:
    now_ms = int(time.time() * 1000)
    cached_symbols = _EXCHANGE_SYMBOL_SET_CACHE.get('symbols')
    cached_at_ms = int(_EXCHANGE_SYMBOL_SET_CACHE.get('updated_at_ms') or 0)
    if isinstance(cached_symbols, set) and cached_symbols and now_ms - cached_at_ms <= int(float(ttl_seconds or 21600.0) * 1000):
        return set(cached_symbols)
    try:
        rest_client = client or BinanceFuturesClient('https://fapi.binance.com', max_get_retries=1)
        data = rest_client.get('/fapi/v1/exchangeInfo', timeout=timeout, purpose='metadata')
    except Exception:
        return set(cached_symbols or set()) if isinstance(cached_symbols, set) else set()
    metas: Dict[str, SymbolMeta] = {}
    for row in data.get('symbols', []):
        if row.get('quoteAsset') != 'USDT':
            continue
        normalized = normalize_symbol(row.get('symbol'))
        if not normalized:
            continue
        filters = {f['filterType']: f for f in row.get('filters', [])}
        metas[normalized] = SymbolMeta(
            symbol=normalized,
            price_precision=int(row.get('pricePrecision', 2)),
            quantity_precision=int(row.get('quantityPrecision', 3)),
            tick_size=_to_float(filters.get('PRICE_FILTER', {}).get('tickSize', 0.01)),
            step_size=_to_float(filters.get('LOT_SIZE', {}).get('stepSize', 0.001)),
            min_qty=_to_float(filters.get('LOT_SIZE', {}).get('minQty', 0.001)),
            quote_asset=row.get('quoteAsset', 'USDT'),
            status=row.get('status', ''),
            contract_type=row.get('contractType', ''),
        )
    symbols = set(filter_strategy_websocket_symbol_meta(metas).keys())
    _EXCHANGE_SYMBOL_SET_CACHE.update({'symbols': set(symbols), 'updated_at_ms': now_ms})
    return symbols


def _client_get_with_optional_purpose(client: BinanceFuturesClient, path: str, *, params: Optional[Dict[str, Any]] = None, timeout: int = 15, purpose: str = 'public') -> Any:
    if not hasattr(client, 'get'):
        return None
    try:
        return client.get(path, params=params, timeout=timeout, purpose=purpose)
    except TypeError as exc:
        message = str(exc)
        if "unexpected keyword argument 'purpose'" in message:
            try:
                return client.get(path, params=params, timeout=timeout)
            except TypeError as timeout_exc:
                if "unexpected keyword argument 'timeout'" not in str(timeout_exc):
                    raise
                return client.get(path, params=params)
        if "unexpected keyword argument 'timeout'" in message:
            try:
                return client.get(path, params=params, purpose=purpose)
            except TypeError as purpose_exc:
                if "unexpected keyword argument 'purpose'" not in str(purpose_exc):
                    raise
                return client.get(path, params=params)
        raise


def fetch_tickers(client: BinanceFuturesClient) -> List[Dict[str, Any]]:
    return _client_get_with_optional_purpose(client, '/fapi/v1/ticker/24hr', purpose='scanner')


def fetch_klines(client: BinanceFuturesClient, symbol: str, interval: str, limit: int) -> List[List[Any]]:
    return _client_get_with_optional_purpose(client, '/fapi/v1/klines', params={'symbol': symbol, 'interval': interval, 'limit': limit}, purpose='market_data')


def fetch_funding_rates(client: BinanceFuturesClient, symbol: str, limit: int = 3) -> List[float]:
    rows = _client_get_with_optional_purpose(client, '/fapi/v1/fundingRate', params={'symbol': symbol, 'limit': limit}, purpose='low_frequency_market_data') or []
    return [_to_float(item.get('fundingRate')) for item in rows]


def fetch_open_interest_hist(client: BinanceFuturesClient, symbol: str, period: str = '5m', limit: int = 30) -> List[Dict[str, Any]]:
    return _client_get_with_optional_purpose(client, '/futures/data/openInterestHist', params={'symbol': symbol, 'period': period, 'limit': limit}, purpose='low_frequency_market_data')


def fetch_order_book(client: BinanceFuturesClient, symbol: str, limit: int = 20) -> Dict[str, Any]:
    payload = _client_get_with_optional_purpose(client, '/fapi/v1/depth', params={'symbol': symbol, 'limit': min(int(limit or 20), 20)}, purpose='market_data')
    return payload if isinstance(payload, dict) else {}


def _cache_payload_is_fresh(payload: Any, max_age_seconds: float) -> bool:
    if not isinstance(payload, dict):
        return False
    updated_at = _parse_iso8601_utc(payload.get('updated_at') or payload.get('cached_at'))
    if updated_at is None:
        return False
    return max((_utc_now() - updated_at).total_seconds(), 0.0) <= float(max_age_seconds)


def _payload_age_seconds_from_updated_at_ms(payload: Any) -> Optional[float]:
    if not isinstance(payload, dict):
        return None
    updated_at_ms = payload.get('updated_at_ms')
    if updated_at_ms not in (None, ''):
        try:
            return max(0.0, (int(time.time() * 1000) - int(float(updated_at_ms))) / 1000.0)
        except Exception:
            return None
    updated_at = _parse_iso8601_utc(payload.get('updated_at') or payload.get('cached_at'))
    if updated_at is None:
        return None
    return max((_utc_now() - updated_at).total_seconds(), 0.0)


def load_scan_ticker_cache_state(store: Optional[RuntimeStateStore], max_age_seconds: float = 300.0, min_row_count: int = 1) -> Dict[str, Any]:
    state: Dict[str, Any] = {
        'available': False,
        'fresh': False,
        'age_seconds': None,
        'row_count': 0,
        'rows': [],
        'source': '',
    }
    if store is None:
        state['reason'] = 'store_unavailable'
        return state
    cache_error: Optional[Dict[str, Any]] = None
    if hasattr(store, 'load_json_with_error'):
        cache_state, cache_error = store.load_json_with_error('ticker_24hr_cache', {})
    else:
        cache_state = store.load_json('ticker_24hr_cache', {})
    if cache_error:
        state['reason'] = 'cache_parse_error'
        state['error'] = cache_error
        return state
    if isinstance(cache_state, list):
        rows = [dict(row) for row in cache_state if isinstance(row, dict)]
        state.update({'available': bool(rows), 'fresh': bool(rows), 'row_count': len(rows), 'rows': rows, 'source': 'legacy_list'})
        return state
    if not isinstance(cache_state, dict):
        state['reason'] = 'cache_invalid'
        return state
    rows_by_symbol = cache_state.get('rows_by_symbol')
    if isinstance(rows_by_symbol, dict):
        rows = [dict(row, symbol=str(symbol).upper()) if 'symbol' not in row else dict(row) for symbol, row in rows_by_symbol.items() if isinstance(row, dict)]
    else:
        rows = cache_state.get('tickers') or cache_state.get('rows') or cache_state.get('data') or []
        rows = [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
    age_seconds = _payload_age_seconds_from_updated_at_ms(cache_state)
    if age_seconds is None:
        updated_at = _parse_iso8601_utc(cache_state.get('updated_at'))
        if updated_at is not None:
            age_seconds = max(0.0, (_utc_now() - updated_at).total_seconds())
    row_count = int(cache_state.get('row_count') or len(rows))
    min_row_count = max(1, int(min_row_count or 1))
    enough_rows = row_count >= min_row_count
    fresh = bool(rows) and enough_rows and age_seconds is not None and age_seconds <= float(max_age_seconds)
    state.update({
        'available': bool(rows),
        'fresh': fresh,
        'age_seconds': age_seconds,
        'row_count': row_count,
        'min_row_count': min_row_count,
        'rows': rows,
        'source': str(cache_state.get('source') or 'ticker_24hr_cache'),
    })
    if not rows:
        state['reason'] = 'cache_empty'
    elif not enough_rows:
        state['reason'] = 'cache_row_count_below_minimum'
    elif age_seconds is None:
        state['reason'] = 'cache_age_unknown'
    elif not fresh:
        state['reason'] = 'cache_expired'
    return state


def load_scan_ticker_cache(store: Optional[RuntimeStateStore], max_age_seconds: float = 300.0) -> List[Dict[str, Any]]:
    state = load_scan_ticker_cache_state(store, max_age_seconds=max_age_seconds)
    return list(state.get('rows') or []) if state.get('fresh') else []


def _scanner_rest_fallback_cursor(store: Optional[RuntimeStateStore]) -> Dict[str, Any]:
    if store is None:
        return {}
    payload = store.load_json('scanner_rest_fallback_cursor', {})
    return payload if isinstance(payload, dict) else {}


def _runtime_store_rest_guard_snapshot(store: Optional[RuntimeStateStore]) -> Dict[str, Any]:
    if store is not None:
        payload = store.load_json('binance_rest_guard', {})
        if isinstance(payload, dict) and payload:
            return payload
    return {'state': 'CLOSED', 'rest_circuit_state': 'CLOSED', 'rest_used_weight_1m': 0}


def scanner_ticker_rest_fallback_decision(store: Optional[RuntimeStateStore], args: argparse.Namespace, cache_state: Dict[str, Any], rest_snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if bool(cache_state.get('fresh')):
        return {'allowed': False, 'reason': 'cache_fresh'}
    if not scanner_rest_fallback_enabled(args):
        return {'allowed': False, 'reason': 'scanner_rest_fallback_disabled'}
    rest_snapshot = rest_snapshot or _runtime_store_rest_guard_snapshot(store)
    rest_state = str(rest_snapshot.get('rest_circuit_state') or rest_snapshot.get('state') or 'CLOSED').upper()
    used_weight = int(rest_snapshot.get('rest_used_weight_1m') or 0)
    max_used_weight = int(getattr(args, 'scanner_market_data_rest_fallback_max_used_weight_1m', getattr(args, 'scanner_rest_fallback_max_used_weight_1m', 700)) or 700)
    if rest_state != 'CLOSED':
        return {'allowed': False, 'reason': 'rest_circuit_state_' + rest_state.lower(), 'rest_used_weight_1m': used_weight}
    if used_weight >= max_used_weight:
        return {'allowed': False, 'reason': 'rest_used_weight_1m_exceeds_limit', 'rest_used_weight_1m': used_weight, 'max_used_weight_1m': max_used_weight}
    cursor = _scanner_rest_fallback_cursor(store)
    last_at_ms = int(cursor.get('last_ticker_24hr_at_ms') or 0)
    min_interval = float(getattr(args, 'scanner_rest_fallback_min_interval_seconds', 0.0) or 0.0)
    cache_reason = str(cache_state.get('reason') or '')
    bypass_min_interval_reasons = {'cache_parse_error', 'cache_invalid', 'cache_row_count_below_minimum'}
    if cache_reason in bypass_min_interval_reasons:
        min_interval = 0.0
    now_ms = int(time.time() * 1000)
    elapsed = (now_ms - last_at_ms) / 1000.0 if last_at_ms > 0 else None
    if min_interval > 0 and elapsed is not None and elapsed < min_interval:
        return {'allowed': False, 'reason': 'scanner_rest_fallback_min_interval', 'seconds_until_allowed': round(min_interval - elapsed, 3), 'rest_used_weight_1m': used_weight}
    return {'allowed': True, 'reason': 'cache_missing_or_expired', 'rest_used_weight_1m': used_weight}


def _save_scanner_ticker_rest_fallback_cursor(store: Optional[RuntimeStateStore]) -> None:
    if store is None:
        return
    store.save_json('scanner_rest_fallback_cursor', {'last_ticker_24hr_at_ms': int(time.time() * 1000), 'updated_at': _isoformat_utc(_utc_now())})


def build_degraded_ticker_rows_from_book_ticker_cache(store: Optional[RuntimeStateStore], symbols: Sequence[str], max_age_seconds: float = 30.0) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for symbol in list(symbols or []):
        symbol_key = normalize_symbol(symbol)
        if not symbol_key:
            continue
        snapshot = load_book_ticker_cache_snapshot(store, symbol_key, max_age_seconds=max_age_seconds)
        if not isinstance(snapshot, dict):
            continue
        mid = snapshot.get('mid_price')
        rows.append({
            'symbol': symbol_key,
            'priceChangePercent': '0',
            'quoteVolume': '0',
            'lastPrice': str(mid or ''),
            'degraded_ticker_24hr': True,
            'volume_change_unknown': True,
            'source': 'book_ticker_cache_degraded',
        })
    return rows


def _build_ticker_24hr_cache_payload(rows: Sequence[Dict[str, Any]], *, source: str, rest_snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cleaned_rows = [dict(row) for row in rows if isinstance(row, dict) and normalize_symbol(row.get('symbol'))]
    rows_by_symbol = {normalize_symbol(row.get('symbol')): row for row in cleaned_rows}
    rest_snapshot = rest_snapshot or _binance_rest_guard_snapshot()
    return {
        'updated_at_ms': int(time.time() * 1000),
        'source': source,
        'row_count': len(rows_by_symbol),
        'rows_by_symbol': rows_by_symbol,
        'rest_used_weight_1m': int(rest_snapshot.get('rest_used_weight_1m') or 0),
    }


def _save_ticker_24hr_cache_payload(store: Optional[RuntimeStateStore], rows: Sequence[Dict[str, Any]], *, source: str, rest_snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = _build_ticker_24hr_cache_payload(rows, source=source, rest_snapshot=rest_snapshot)
    if store is not None:
        store.save_json('ticker_24hr_cache', payload)
    return payload


def resolve_scan_tickers(client: BinanceFuturesClient, store: Optional[RuntimeStateStore], args: argparse.Namespace, fallback_symbols: Optional[Sequence[str]] = None, return_diagnostics: bool = False):
    max_age_seconds = float(getattr(args, 'ticker_24hr_cache_max_age_seconds', getattr(args, 'scanner_ticker_cache_max_age_seconds', 300.0)) or 300.0)
    min_row_count = max(1, int(getattr(args, 'ticker_24hr_cache_min_rows', 1) or 1))
    if fallback_symbols:
        min_row_count = max(min_row_count, len(set(normalize_symbol(symbol) for symbol in fallback_symbols if normalize_symbol(symbol))))
    runtime_state_explicit = hasattr(args, 'runtime_state_dir') or not scanner_rest_fallback_enabled(args)
    cache_state = load_scan_ticker_cache_state(store, max_age_seconds=max_age_seconds, min_row_count=min_row_count) if runtime_state_explicit else {'fresh': False, 'rows': [], 'row_count': 0, 'age_seconds': None}
    rest_snapshot = _runtime_store_rest_guard_snapshot(store) if runtime_state_explicit else {'state': 'CLOSED', 'rest_circuit_state': 'CLOSED', 'rest_used_weight_1m': 0}
    refresher_heartbeat = load_ticker_24hr_cache_refresher_heartbeat(store)
    diagnostics: Dict[str, Any] = {
        'ticker_24hr_cache_available': bool(cache_state.get('fresh')),
        'ticker_24hr_cache_age_seconds': cache_state.get('age_seconds'),
        'ticker_24hr_cache_row_count': int(cache_state.get('row_count') or 0),
        'scanner_rest_fallback_used': False,
        'scanner_rest_fallback_skipped_reason': '',
        'rest_used_weight_1m': int(rest_snapshot.get('rest_used_weight_1m') or 0),
        'rest_circuit_state': str(rest_snapshot.get('rest_circuit_state') or rest_snapshot.get('state') or 'CLOSED').upper(),
        'scanner_cache_only_mode': bool(cache_state.get('fresh')),
        'scanner_patch_fallback_skipped_reason': '',
        'scanner_patch_fallback_disabled': True,
        'scanner_rest_fallback_allowed': False,
        'scanner_rest_fallback_blocked_by_weight': False,
        'ticker_24hr_cache_refresher_active': bool(refresher_heartbeat.get('active')),
        'ticker_24hr_cache_refresher_skipped_reason': str(refresher_heartbeat.get('last_skipped_reason') or ''),
        'ticker_24hr_cache_refresher_lock_acquired': False,
        'ticker_24hr_cache_refresher_singleton_scope': '',
        'symbols_skipped_due_to_missing_ticker_24hr': 0,
    }
    if bool(cache_state.get('fresh')):
        rows = cache_state.get('rows', [])
        return (rows, diagnostics) if return_diagnostics else rows
    decision = scanner_ticker_rest_fallback_decision(store, args, cache_state, rest_snapshot)
    diagnostics['scanner_rest_fallback_allowed'] = bool(decision.get('allowed'))
    diagnostics['scanner_rest_fallback_blocked_by_weight'] = str(decision.get('reason') or '') == 'rest_used_weight_1m_exceeds_limit'
    if decision.get('allowed'):
        rows = fetch_tickers(client)
        _save_ticker_24hr_cache_payload(store, rows, source='scanner_rest_fallback', rest_snapshot=rest_snapshot)
        _save_scanner_ticker_rest_fallback_cursor(store)
        diagnostics['scanner_rest_fallback_used'] = True
        diagnostics['rest_used_weight_1m'] = int(diagnostics.get('rest_used_weight_1m') or 0)
        return (rows, diagnostics) if return_diagnostics else rows
    diagnostics['scanner_rest_fallback_skipped_reason'] = str(decision.get('reason') or 'fallback_not_allowed')
    diagnostics['rest_used_weight_1m'] = int(decision.get('rest_used_weight_1m') or diagnostics['rest_used_weight_1m'] or 0)
    degraded_rows = build_degraded_ticker_rows_from_book_ticker_cache(store, fallback_symbols or [], max_age_seconds=float(getattr(args, 'scanner_order_book_cache_max_age_seconds', 30.0) or 30.0))
    if degraded_rows:
        diagnostics['degraded'] = True
        diagnostics['degraded_reason'] = 'ticker_24hr_cache_missing'
        diagnostics['ticker_24hr_cache_available'] = False
        return (degraded_rows, diagnostics) if return_diagnostics else degraded_rows
    diagnostics['degraded'] = True
    diagnostics['degraded_reason'] = 'ticker_24hr_cache_missing'
    return ([], diagnostics) if return_diagnostics else []


_TICKER_24HR_CACHE_REFRESHER_LOCK = threading.Lock()
_TICKER_24HR_CACHE_REFRESHER_THREAD: Optional[threading.Thread] = None


def _pid_is_running(pid: Any) -> bool:
    try:
        value = int(pid)
    except Exception:
        return False
    if value <= 0:
        return False
    try:
        os.kill(value, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False
    return True


def load_ticker_24hr_cache_refresher_heartbeat(store: Optional[RuntimeStateStore]) -> Dict[str, Any]:
    if store is None:
        return {'active': False}
    payload = store.load_json('ticker_24hr_cache_refresher_heartbeat', {})
    if not isinstance(payload, dict):
        return {'active': False}
    age_seconds = _payload_age_seconds_from_updated_at_ms(payload)
    pid = payload.get('pid')
    pid_alive = _pid_is_running(pid) if pid not in (None, '') else True
    active = bool(payload.get('active')) and pid_alive and age_seconds is not None and age_seconds < 180.0
    result = dict(payload)
    result['active'] = active
    result['age_seconds'] = age_seconds
    result['pid_alive'] = pid_alive
    return result


def _save_ticker_24hr_cache_refresher_heartbeat(store: RuntimeStateStore, *, active: bool = True, last_skipped_reason: str = '') -> Dict[str, Any]:
    payload = {
        'updated_at_ms': int(time.time() * 1000),
        'updated_at': _isoformat_utc(_utc_now()),
        'active': bool(active),
        'pid': os.getpid(),
        'owner_pid': os.getpid(),
        'thread_count': 1 if active else 0,
        'thread_name': threading.current_thread().name,
        'last_skipped_reason': str(last_skipped_reason or ''),
    }
    store.save_json('ticker_24hr_cache_refresher_heartbeat', payload)
    return payload


def _ticker_24hr_cache_refresher_skip(store: RuntimeStateStore, reason: str, rest_snapshot: Optional[Dict[str, Any]] = None, cache_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    rest_snapshot = rest_snapshot or _runtime_store_rest_guard_snapshot(store)
    cache_state = cache_state or load_scan_ticker_cache_state(store, max_age_seconds=300.0)
    payload = {
        'skipped': True,
        'reason': str(reason or 'unknown'),
        'rest_used_weight_1m': int(rest_snapshot.get('rest_used_weight_1m') or 0),
        'rest_circuit_state': str(rest_snapshot.get('rest_circuit_state') or rest_snapshot.get('state') or 'CLOSED').upper(),
        'cache_age_seconds': cache_state.get('age_seconds'),
    }
    _save_ticker_24hr_cache_refresher_heartbeat(store, active=True, last_skipped_reason=payload['reason'])
    append_rate_limited_runtime_event(store, 'ticker_24hr_cache_refresher_skipped', payload, key='ticker_24hr_cache_refresher_skipped', min_interval_seconds=30.0)
    return payload


def refresh_ticker_24hr_cache_once(client: BinanceFuturesClient, store: RuntimeStateStore, *, source: str = 'ticker_24hr_cache_refresher', args: Optional[argparse.Namespace] = None) -> Dict[str, Any]:
    max_age_seconds = float(getattr(args, 'ticker_24hr_cache_max_age_seconds', getattr(args, 'scanner_ticker_cache_max_age_seconds', 300.0)) or 300.0) if args is not None else 300.0
    min_row_count = max(1, int(getattr(args, 'ticker_24hr_cache_min_rows', 100) or 100)) if args is not None else 100
    max_used_weight = int(getattr(args, 'scanner_rest_fallback_max_used_weight_1m', 900) or 900) if args is not None else 900
    cache_state = load_scan_ticker_cache_state(store, max_age_seconds=max_age_seconds, min_row_count=min_row_count)
    rest_snapshot = _runtime_store_rest_guard_snapshot(store)
    rest_state = str(rest_snapshot.get('rest_circuit_state') or rest_snapshot.get('state') or 'CLOSED').upper()
    used_weight = int(rest_snapshot.get('rest_used_weight_1m') or 0)
    if rest_state != 'CLOSED':
        return _ticker_24hr_cache_refresher_skip(store, 'rest_circuit_not_closed', rest_snapshot, cache_state)
    if used_weight >= max_used_weight:
        return _ticker_24hr_cache_refresher_skip(store, 'rest_used_weight_1m_exceeds_limit', rest_snapshot, cache_state)
    if bool(cache_state.get('fresh')):
        return _ticker_24hr_cache_refresher_skip(store, 'cache_fresh', rest_snapshot, cache_state)
    rows = fetch_tickers(client)
    payload = _save_ticker_24hr_cache_payload(store, rows, source=source, rest_snapshot=_binance_rest_guard_snapshot())
    _save_ticker_24hr_cache_refresher_heartbeat(store, active=True, last_skipped_reason='')
    append_rate_limited_runtime_event(store, 'ticker_24hr_cache_refreshed', {'row_count': payload['row_count'], 'rest_used_weight_1m': payload['rest_used_weight_1m'], 'source': source}, key='ticker_24hr_cache_refresher', min_interval_seconds=60.0)
    return payload


def ticker_24hr_cache_refresher_loop(client: BinanceFuturesClient, args: argparse.Namespace, store: RuntimeStateStore, stop_event: Optional[threading.Event] = None) -> None:
    interval = max(1.0, float(getattr(args, 'ticker_24hr_cache_refresh_seconds', 120.0) or 120.0))
    while stop_event is None or not stop_event.is_set():
        try:
            _save_ticker_24hr_cache_refresher_heartbeat(store, active=True)
            refresh_ticker_24hr_cache_once(client, store, source='ticker_24hr_cache_refresher', args=args)
        except Exception as exc:
            append_rate_limited_runtime_event(
                store,
                'ticker_24hr_cache_refresher_failed',
                {'error': str(exc), 'source': 'ticker_24hr_cache_refresher'},
                key='ticker_24hr_cache_refresher',
                min_interval_seconds=60.0,
            )
        if stop_event is not None:
            if stop_event.wait(interval):
                break
        else:
            threading.Event().wait(interval)


def start_ticker_24hr_cache_refresher(client: BinanceFuturesClient, args: argparse.Namespace, store: RuntimeStateStore) -> Optional[threading.Thread]:
    global _TICKER_24HR_CACHE_REFRESHER_THREAD
    lock_path = store._json_path('ticker_24hr_cache_refresher_singleton_lock') if hasattr(store, '_json_path') else None
    file_lock_factory = getattr(store, '_file_lock', None)
    with _TICKER_24HR_CACHE_REFRESHER_LOCK:
        lock_context = file_lock_factory(lock_path) if callable(file_lock_factory) and lock_path is not None else contextlib.nullcontext()
        with lock_context:
            if _TICKER_24HR_CACHE_REFRESHER_THREAD is not None and _TICKER_24HR_CACHE_REFRESHER_THREAD.is_alive():
                append_runtime_event(store, 'ticker_24hr_cache_refresher_already_running', {'scope': 'process', 'thread_name': _TICKER_24HR_CACHE_REFRESHER_THREAD.name})
                return _TICKER_24HR_CACHE_REFRESHER_THREAD
            heartbeat = load_ticker_24hr_cache_refresher_heartbeat(store)
            if bool(heartbeat.get('active')):
                append_runtime_event(store, 'ticker_24hr_cache_refresher_already_running', {'scope': 'runtime_state_file_lock', 'age_seconds': heartbeat.get('age_seconds'), 'pid': heartbeat.get('pid')})
                return None
            _save_ticker_24hr_cache_refresher_heartbeat(store, active=True)
            thread = threading.Thread(target=ticker_24hr_cache_refresher_loop, name='ticker_24hr_cache_refresher', args=(client, args, store), daemon=True)
            thread.start()
            _TICKER_24HR_CACHE_REFRESHER_THREAD = thread
    append_runtime_event(store, 'ticker_24hr_cache_refresher_started', {'thread_name': thread.name, 'refresh_seconds': float(getattr(args, 'ticker_24hr_cache_refresh_seconds', 120.0) or 120.0), 'ticker_24hr_cache_refresher_lock_acquired': True, 'ticker_24hr_cache_refresher_singleton_scope': 'runtime_state_file_lock'})
    return thread


def load_scan_kline_cache(store: Optional[RuntimeStateStore], symbol: str, interval: str, limit: int, max_age_seconds: float = 120.0) -> List[List[Any]]:
    if store is None:
        return []
    cache_state = store.load_json('kline_cache', {})
    if not isinstance(cache_state, dict):
        return []
    symbol_state = cache_state.get(str(symbol or '').strip().upper())
    if not isinstance(symbol_state, dict):
        return []
    interval_state = symbol_state.get(str(interval or '').strip())
    if not _cache_payload_is_fresh(interval_state, max_age_seconds):
        return []
    rows = interval_state.get('klines') or interval_state.get('rows') or interval_state.get('data')
    if not isinstance(rows, list):
        return []
    cleaned = [list(row) for row in rows if isinstance(row, (list, tuple))]
    return cleaned[-max(int(limit or 0), 0):] if limit else cleaned


def load_scan_order_book_cache(store: Optional[RuntimeStateStore], symbol: str, max_age_seconds: float = 3.0) -> Dict[str, Any]:
    if store is None:
        return {}
    cache_state = store.load_json('order_book_cache', {})
    if not isinstance(cache_state, dict):
        return {}
    payload = cache_state.get(str(symbol or '').strip().upper())
    if not _cache_payload_is_fresh(payload, max_age_seconds):
        return {}
    order_book = payload.get('order_book') if isinstance(payload.get('order_book'), dict) else payload
    bids = order_book.get('bids') if isinstance(order_book, dict) else None
    asks = order_book.get('asks') if isinstance(order_book, dict) else None
    if not isinstance(bids, list) or not isinstance(asks, list):
        return {}
    return {'bids': bids, 'asks': asks}


def scanner_rest_fallback_enabled(args: argparse.Namespace) -> bool:
    if hasattr(args, 'scanner_rest_fallback'):
        return bool(getattr(args, 'scanner_rest_fallback'))
    return True


def scanner_ticker_rest_fallback_enabled(args: argparse.Namespace) -> bool:
    return bool(getattr(args, 'scanner_ticker_rest_fallback', scanner_rest_fallback_enabled(args)))


def scanner_kline_rest_fallback_enabled(args: argparse.Namespace) -> bool:
    return bool(scanner_rest_fallback_enabled(args) and getattr(args, 'scanner_kline_rest_fallback', False))


def scanner_depth_rest_fallback_enabled(args: argparse.Namespace) -> bool:
    return bool(scanner_rest_fallback_enabled(args) and getattr(args, 'scanner_depth_rest_fallback', False))


def _scanner_market_data_rest_fallback_decision(store: Optional[RuntimeStateStore], args: argparse.Namespace, cursor_key: str, min_interval_attr: str) -> Dict[str, Any]:
    rest_snapshot = _runtime_store_rest_guard_snapshot(store)
    rest_state = str(rest_snapshot.get('rest_circuit_state') or rest_snapshot.get('state') or 'CLOSED').upper()
    used_weight = int(rest_snapshot.get('rest_used_weight_1m') or 0)
    max_used_weight = int(getattr(args, 'scanner_market_data_rest_fallback_max_used_weight_1m', 700) or 700)
    if rest_state != 'CLOSED':
        return {'allowed': False, 'reason': 'rest_circuit_state_' + rest_state.lower(), 'rest_used_weight_1m': used_weight}
    if used_weight >= max_used_weight:
        return {'allowed': False, 'reason': 'rest_used_weight_1m_exceeds_limit', 'rest_used_weight_1m': used_weight, 'max_used_weight_1m': max_used_weight}
    cursor = _scanner_rest_fallback_cursor(store)
    last_at_ms = int(cursor.get(cursor_key) or 0)
    min_interval_raw = getattr(args, min_interval_attr, 300.0)
    min_interval = 300.0 if min_interval_raw is None else float(min_interval_raw)
    now_ms = int(time.time() * 1000)
    elapsed = (now_ms - last_at_ms) / 1000.0 if last_at_ms > 0 else None
    if min_interval > 0 and elapsed is not None and elapsed < min_interval:
        return {'allowed': False, 'reason': 'scanner_rest_fallback_min_interval', 'seconds_until_allowed': round(min_interval - elapsed, 3), 'rest_used_weight_1m': used_weight}
    return {'allowed': True, 'reason': 'cache_missing_or_expired', 'rest_used_weight_1m': used_weight}


def _save_scanner_rest_fallback_cursor_key(store: Optional[RuntimeStateStore], key: str) -> None:
    if store is None:
        return
    payload = _scanner_rest_fallback_cursor(store)
    payload[key] = int(time.time() * 1000)
    payload['updated_at'] = _isoformat_utc(_utc_now())
    store.save_json('scanner_rest_fallback_cursor', payload)


def resolve_scan_klines(client: BinanceFuturesClient, store: Optional[RuntimeStateStore], args: argparse.Namespace, symbol: str, interval: str, limit: int, return_diagnostics: bool = False):
    diagnostics = {'kline_cache_missing': False, 'scanner_kline_rest_fallback_used': False, 'scanner_kline_rest_fallback_skipped_reason': ''}
    cached = load_scan_kline_cache(store, symbol, interval, limit, max_age_seconds=float(getattr(args, 'scanner_kline_cache_max_age_seconds', 120.0) or 120.0))
    if cached:
        return (cached, diagnostics) if return_diagnostics else cached
    diagnostics['kline_cache_missing'] = True
    if not scanner_kline_rest_fallback_enabled(args):
        diagnostics['scanner_kline_rest_fallback_skipped_reason'] = 'scanner_kline_rest_fallback_disabled'
        return ([], diagnostics) if return_diagnostics else []
    cursor_key = f'last_kline_rest_fallback_at_ms:{str(symbol).upper()}:{str(interval)}'
    decision = _scanner_market_data_rest_fallback_decision(store, args, cursor_key, 'scanner_kline_rest_fallback_min_interval_seconds')
    if not decision.get('allowed'):
        diagnostics['scanner_kline_rest_fallback_skipped_reason'] = str(decision.get('reason') or 'fallback_not_allowed')
        return ([], diagnostics) if return_diagnostics else []
    rows = fetch_klines(client, symbol, interval, limit)
    _save_scanner_rest_fallback_cursor_key(store, cursor_key)
    diagnostics['scanner_kline_rest_fallback_used'] = True
    return (rows, diagnostics) if return_diagnostics else rows


def resolve_scan_order_book(client: BinanceFuturesClient, store: Optional[RuntimeStateStore], args: argparse.Namespace, symbol: str, limit: int = 20, return_diagnostics: bool = False):
    diagnostics = {'order_book_cache_missing': False, 'scanner_depth_rest_fallback_used': False, 'scanner_depth_rest_fallback_skipped_reason': ''}
    cached = load_scan_order_book_cache(store, symbol, max_age_seconds=float(getattr(args, 'scanner_order_book_cache_max_age_seconds', 3.0) or 3.0))
    if cached:
        return (cached, diagnostics) if return_diagnostics else cached
    diagnostics['order_book_cache_missing'] = True
    if not scanner_depth_rest_fallback_enabled(args):
        diagnostics['scanner_depth_rest_fallback_skipped_reason'] = 'scanner_depth_rest_fallback_disabled'
        return ({}, diagnostics) if return_diagnostics else {}
    decision = _scanner_market_data_rest_fallback_decision(store, args, 'last_depth_rest_fallback_at_ms', 'scanner_depth_rest_fallback_min_interval_seconds')
    if not decision.get('allowed'):
        diagnostics['scanner_depth_rest_fallback_skipped_reason'] = str(decision.get('reason') or 'fallback_not_allowed')
        return ({}, diagnostics) if return_diagnostics else {}
    depth_limit = min(int(limit or 20), 20)
    rows = fetch_order_book(client, symbol, limit=depth_limit)
    _save_scanner_rest_fallback_cursor_key(store, 'last_depth_rest_fallback_at_ms')
    diagnostics['scanner_depth_rest_fallback_used'] = True
    return (rows, diagnostics) if return_diagnostics else rows


def load_book_ticker_cache_snapshot(store: Optional[RuntimeStateStore], symbol: str, max_age_seconds: float = 3.0) -> Optional[Dict[str, Any]]:
    if store is None:
        return None
    cache_state = store.load_json('book_ticker_cache', {})
    if not isinstance(cache_state, dict):
        return None
    symbol_key = str(symbol or '').strip().upper()
    if not symbol_key:
        return None
    symbol_state = cache_state.get(symbol_key)
    if not isinstance(symbol_state, dict):
        return None
    updated_at = _parse_iso8601_utc(symbol_state.get('updated_at'))
    if updated_at is None:
        return None
    age_seconds = max((_utc_now() - updated_at).total_seconds(), 0.0)
    if age_seconds > float(max_age_seconds):
        return None
    sample: Optional[Dict[str, Any]] = None
    samples = symbol_state.get('samples')
    if isinstance(samples, list):
        for row in reversed(samples):
            if isinstance(row, dict) and row:
                sample = dict(row)
                break
    if sample is None:
        sample = {
            'bidPrice': symbol_state.get('last_bid'),
            'askPrice': symbol_state.get('last_ask'),
            'bidQty': symbol_state.get('last_bid_qty'),
            'askQty': symbol_state.get('last_ask_qty'),
        }
    bid_price = _to_float(sample.get('bidPrice'))
    ask_price = _to_float(sample.get('askPrice'))
    if bid_price <= 0 and ask_price <= 0:
        return None
    if bid_price > 0 and ask_price > 0:
        mid_price = round((bid_price + ask_price) / 2.0, 10)
    elif bid_price > 0:
        mid_price = bid_price
    else:
        mid_price = ask_price
    snapshot = {
        'symbol': symbol_key,
        'updated_at': _isoformat_utc(updated_at),
        'age_seconds': round(age_seconds, 6),
        'bid_price': bid_price if bid_price > 0 else None,
        'ask_price': ask_price if ask_price > 0 else None,
        'bid_qty': _to_float(sample.get('bidQty')) or None,
        'ask_qty': _to_float(sample.get('askQty')) or None,
        'mid_price': mid_price,
        'source': symbol_state.get('source') or 'websocket',
        'event_count': int(symbol_state.get('event_count', 0) or 0),
    }
    last_event_time = symbol_state.get('last_event_time')
    if last_event_time not in (None, ''):
        snapshot['last_event_time'] = int(_to_float(last_event_time, default=0.0))
    return snapshot


def load_book_ticker_cache_samples(store: Optional[RuntimeStateStore], symbol: str, sample_count: int = 6, max_age_seconds: float = 3.0) -> List[Dict[str, Any]]:
    snapshot = load_book_ticker_cache_snapshot(store, symbol, max_age_seconds=max_age_seconds)
    if snapshot is None or store is None:
        return []
    cache_state = store.load_json('book_ticker_cache', {})
    if not isinstance(cache_state, dict):
        return []
    symbol_state = cache_state.get(snapshot['symbol'])
    if not isinstance(symbol_state, dict):
        return []
    samples = symbol_state.get('samples')
    if not isinstance(samples, list):
        return []
    sample_total = max(int(sample_count or 0), 0)
    if sample_total <= 0:
        return []
    normalized: List[Dict[str, Any]] = []
    for row in samples[-sample_total:]:
        if isinstance(row, dict) and row:
            normalized.append(dict(row))
    return normalized


def normalize_book_ticker_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    symbol = str(payload.get('s') or payload.get('symbol') or '').strip().upper()
    bid_price = payload.get('b', payload.get('bidPrice'))
    ask_price = payload.get('a', payload.get('askPrice'))
    bid_qty = payload.get('B', payload.get('bidQty'))
    ask_qty = payload.get('A', payload.get('askQty'))
    if bid_price in (None, '') or ask_price in (None, ''):
        return None
    row = {
        'bidPrice': str(bid_price),
        'askPrice': str(ask_price),
        'bidQty': str(bid_qty if bid_qty is not None else ''),
        'askQty': str(ask_qty if ask_qty is not None else ''),
    }
    event_time = payload.get('E', payload.get('eventTime'))
    if event_time not in (None, ''):
        row['eventTime'] = int(_to_float(event_time, default=0.0))
    return {'symbol': symbol, 'sample': row}


def _get_book_ticker_cache_state(store: RuntimeStateStore) -> Dict[str, Any]:
    cache_key = _runtime_store_cache_key(store)
    with _BOOK_TICKER_CACHE_LOCK:
        cached = _BOOK_TICKER_CACHE_STATE_CACHE.get(cache_key)
        if isinstance(cached, dict):
            return cached
        loaded = store.load_json('book_ticker_cache', {})
        if not isinstance(loaded, dict):
            loaded = {}
        _BOOK_TICKER_CACHE_STATE_CACHE[cache_key] = loaded
        _BOOK_TICKER_CACHE_LAST_FLUSH_TS[cache_key] = 0.0
        return loaded


def flush_book_ticker_cache_state(
    store: Optional[RuntimeStateStore],
    *,
    force: bool = False,
    min_interval_seconds: float = _BOOK_TICKER_DEFAULT_FLUSH_INTERVAL_SECONDS,
) -> bool:
    if store is None:
        return False
    cache_key = _runtime_store_cache_key(store)
    with _BOOK_TICKER_CACHE_LOCK:
        state = _BOOK_TICKER_CACHE_STATE_CACHE.get(cache_key)
        if not isinstance(state, dict):
            return False
        now_ts = time.monotonic()
        previous_flush_ts = float(_BOOK_TICKER_CACHE_LAST_FLUSH_TS.get(cache_key, 0.0) or 0.0)
        if not force and previous_flush_ts > 0 and (now_ts - previous_flush_ts) < max(float(min_interval_seconds or 0.0), 30.0):
            return False
        store.save_json('book_ticker_cache', state)
        _BOOK_TICKER_CACHE_LAST_FLUSH_TS[cache_key] = now_ts
        return True


def append_book_ticker_cache_sample(
    store: RuntimeStateStore,
    symbol: str,
    payload: Dict[str, Any],
    max_samples: int = 20,
) -> Dict[str, Any]:
    normalized = normalize_book_ticker_payload(payload)
    if normalized is None:
        raise ValueError('payload is not a valid bookTicker event')
    symbol_key = str(symbol or normalized['symbol']).strip().upper()
    if not symbol_key:
        raise ValueError('symbol is required for bookTicker cache sample')
    sample = normalized['sample']
    cache_state = _get_book_ticker_cache_state(store)
    symbol_state = cache_state.get(symbol_key, {})
    if not isinstance(symbol_state, dict):
        symbol_state = {}
    prior_samples = symbol_state.get('samples')
    if isinstance(prior_samples, list):
        samples = prior_samples
    else:
        samples = []
    ring_size = max(int(max_samples or 0), 1)
    samples.append({key: sample.get(key) for key in ('bidPrice', 'askPrice', 'bidQty', 'askQty') if key in sample})
    if len(samples) > ring_size:
        del samples[:-ring_size]
    event_count = int(symbol_state.get('event_count', 0) or 0) + 1
    updated_at = _isoformat_utc(_utc_now())
    symbol_state.update({
        'updated_at': updated_at,
        'samples': samples,
        'last_bid': sample.get('bidPrice'),
        'last_ask': sample.get('askPrice'),
        'last_bid_qty': sample.get('bidQty'),
        'last_ask_qty': sample.get('askQty'),
        'event_count': event_count,
        'source': 'websocket',
    })
    if sample.get('eventTime'):
        symbol_state['last_event_time'] = int(sample['eventTime'])
    cache_state[symbol_key] = symbol_state
    flush_book_ticker_cache_state(store, force=False)
    event = None
    if _BOOK_TICKER_DEBUG_SAMPLE_EVENTS:
        event = append_rate_limited_runtime_event(store, 'book_ticker_ws_sample_written', {
            'event_source': 'book_ticker_websocket',
            'symbol': symbol_key,
            'samples_cached': len(samples),
            'event_count': event_count,
            'updated_at': updated_at,
        }, key=symbol_key, min_interval_seconds=300.0)
    return {
        'symbol': symbol_key,
        'samples_cached': len(samples),
        'event_count': event_count,
        'updated_at': updated_at,
        'event': event,
    }


def extract_book_ticker_stream_sample(message: Any) -> Optional[Dict[str, Any]]:
    payload = message
    if isinstance(message, str):
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return None
    if not isinstance(payload, dict):
        return None
    data = payload.get('data') if isinstance(payload.get('data'), dict) else payload
    if not isinstance(data, dict):
        return None
    event_type = str(data.get('e') or '').strip()
    stream_name = str(payload.get('stream') or '').strip().lower()
    if event_type != 'bookTicker' and '@bookticker' not in stream_name:
        return None
    normalized = normalize_book_ticker_payload(data)
    if normalized is None:
        return None
    return normalized


def process_book_ticker_stream_message(store: RuntimeStateStore, message: Any, max_samples: int = 20) -> Optional[Dict[str, Any]]:
    normalized = extract_book_ticker_stream_sample(message)
    if normalized is None:
        return None
    return append_book_ticker_cache_sample(store, normalized['symbol'], normalized['sample'], max_samples=max_samples)


def flush_book_ticker_cache_samples(
    store: RuntimeStateStore,
    pending_samples: Sequence[Dict[str, Any]],
    max_samples: int = 20,
) -> Dict[str, Any]:
    valid_samples: List[Dict[str, Any]] = []
    for row in list(pending_samples or []):
        if not isinstance(row, dict):
            continue
        symbol_key = str(row.get('symbol') or '').strip().upper()
        sample = row.get('sample') if isinstance(row.get('sample'), dict) else None
        if not symbol_key or not sample:
            continue
        valid_samples.append({'symbol': symbol_key, 'sample': sample})
    if not valid_samples:
        return {'symbols_updated': 0, 'samples_flushed': 0, 'events_written': 0}

    cache_state = _get_book_ticker_cache_state(store)
    ring_size = max(int(max_samples or 0), 1)
    symbols_updated = set()
    last_event_payloads: Dict[str, Dict[str, Any]] = {}
    for row in valid_samples:
        symbol_key = row['symbol']
        sample = row['sample']
        symbol_state = cache_state.get(symbol_key, {})
        if not isinstance(symbol_state, dict):
            symbol_state = {}
        prior_samples = symbol_state.get('samples')
        if isinstance(prior_samples, list):
            samples = prior_samples
        else:
            samples = []
        samples.append({key: sample.get(key) for key in ('bidPrice', 'askPrice', 'bidQty', 'askQty') if key in sample})
        if len(samples) > ring_size:
            del samples[:-ring_size]
        event_count = int(symbol_state.get('event_count', 0) or 0) + 1
        updated_at = _isoformat_utc(_utc_now())
        symbol_state.update({
            'updated_at': updated_at,
            'samples': samples,
            'last_bid': sample.get('bidPrice'),
            'last_ask': sample.get('askPrice'),
            'last_bid_qty': sample.get('bidQty'),
            'last_ask_qty': sample.get('askQty'),
            'event_count': event_count,
            'source': 'websocket',
        })
        if sample.get('eventTime'):
            symbol_state['last_event_time'] = int(sample['eventTime'])
        cache_state[symbol_key] = symbol_state
        symbols_updated.add(symbol_key)
        last_event_payloads[symbol_key] = {
            'event_source': 'book_ticker_websocket',
            'symbol': symbol_key,
            'samples_cached': len(samples),
            'event_count': event_count,
            'updated_at': updated_at,
        }

    cache_flushed = flush_book_ticker_cache_state(store, force=False)
    sample_events_written = 0
    if _BOOK_TICKER_DEBUG_SAMPLE_EVENTS:
        for payload in last_event_payloads.values():
            if append_rate_limited_runtime_event(
                store,
                'book_ticker_ws_sample_written',
                payload,
                key=str(payload.get('symbol') or 'global'),
                min_interval_seconds=300.0,
            ):
                sample_events_written += 1
    flush_event = None
    if cache_flushed:
        flush_event = append_rate_limited_runtime_event(store, 'book_ticker_ws_samples_flushed', {
            'event_source': 'book_ticker_websocket',
            'symbols_updated': len(symbols_updated),
            'samples_flushed': len(valid_samples),
            'flush_mode': 'batch',
        }, key='batch', min_interval_seconds=300.0)
    events_written = sample_events_written + (1 if flush_event else 0)
    return {
        'symbols_updated': len(symbols_updated),
        'samples_flushed': len(valid_samples),
        'events_written': events_written,
    }


def run_book_ticker_cache_monitor_cycle(
    store: RuntimeStateStore,
    ws: Any,
    ws_module: Any,
    max_messages: int = 100,
    max_samples: int = 20,
    recv_timeout_seconds: float = 5.0,
) -> Dict[str, Any]:
    timeout_exc = getattr(ws_module, 'WebSocketTimeoutException', TimeoutError)
    socket_exc = getattr(ws_module, 'WebSocketException', Exception)
    ws.settimeout(float(recv_timeout_seconds))
    append_rate_limited_runtime_event(store, 'book_ticker_ws_connected', {
        'event_source': 'book_ticker_websocket',
        'recv_timeout_seconds': float(recv_timeout_seconds),
        'max_messages': int(max_messages or 0),
    }, key='monitor_cycle', min_interval_seconds=300.0)
    messages_processed = 0
    samples_written = 0
    pending_samples: List[Dict[str, Any]] = []

    def flush_pending() -> Dict[str, Any]:
        nonlocal pending_samples
        if not pending_samples:
            return {'symbols_updated': 0, 'samples_flushed': 0, 'events_written': 0}
        result = flush_book_ticker_cache_samples(store, pending_samples, max_samples=max_samples)
        pending_samples = []
        return result

    for _ in range(max(int(max_messages or 0), 1)):
        try:
            message = ws.recv()
        except timeout_exc:
            flush_pending()
            return {
                'status': 'idle_timeout' if messages_processed <= 0 else 'healthy',
                'messages_processed': messages_processed,
                'samples_written': samples_written,
                'zero_message_timeout': messages_processed <= 0,
            }
        except socket_exc as exc:
            flush_pending()
            try:
                ws.close()
            finally:
                append_rate_limited_runtime_event(store, 'book_ticker_ws_disconnected', {
                    'event_source': 'book_ticker_websocket',
                    'detail': str(exc),
                    'messages_processed': messages_processed,
                    'samples_written': samples_written,
                }, key='monitor_cycle', min_interval_seconds=300.0)
            return {
                'status': 'disconnected',
                'messages_processed': messages_processed,
                'samples_written': samples_written,
                'error': str(exc),
            }
        result = extract_book_ticker_stream_sample(message)
        if result is None:
            continue
        pending_samples.append(result)
        messages_processed += 1
        samples_written += 1
    flush_pending()
    return {
        'status': 'healthy',
        'messages_processed': messages_processed,
        'samples_written': samples_written,
    }


def build_book_ticker_stream_names(symbols: Sequence[Any]) -> List[str]:
    normalized_symbols = []
    seen = set()
    for raw_symbol in list(symbols or []):
        symbol = str(raw_symbol or '').strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        normalized_symbols.append(symbol)
    normalized_symbols.sort()
    return [f'{symbol.lower()}@bookTicker' for symbol in normalized_symbols]


def open_book_ticker_websocket(
    symbols: Sequence[Any],
    ws_module: Any,
    base_ws_url: str = 'wss://fstream.binance.com/stream',
    connect_timeout_seconds: float = 10.0,
    sslopt: Optional[Dict[str, Any]] = None,
):
    stream_names = build_book_ticker_stream_names(symbols)
    if not stream_names:
        raise ValueError('at least one symbol is required for bookTicker websocket')
    url = f"{str(base_ws_url).rstrip('/')}?streams={'/'.join(stream_names)}"
    return ws_module.create_connection(url, timeout=float(connect_timeout_seconds), sslopt=sslopt)


def update_book_ticker_ws_health_state(
    store: Optional[RuntimeStateStore],
    status: str,
    symbols: Sequence[Any],
    reconnect_count: int,
    subscription_version: int,
    messages_processed: int = 0,
    samples_written: int = 0,
    active_streams: Optional[Sequence[str]] = None,
    last_error: str = '',
    extra: Optional[Dict[str, Any]] = None,
    generation_id: Optional[int] = None,
    last_message_at: Optional[str] = None,
    last_sample_at: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_symbols = []
    for raw_symbol in list(symbols or []):
        symbol = str(raw_symbol or '').strip().upper()
        if symbol and symbol not in normalized_symbols:
            normalized_symbols.append(symbol)
    if store is not None and hasattr(store, 'load_json'):
        previous = store.load_json('book_ticker_ws_status', {})
    else:
        previous = {}
    if not isinstance(previous, dict):
        previous = {}
    payload = {
        'status': str(status or '').strip() or 'unknown',
        'updated_at': _isoformat_utc(_utc_now()),
        'symbols': normalized_symbols,
        'symbol_count': len(normalized_symbols),
        'reconnect_count': int(reconnect_count or 0),
        'subscription_version': int(subscription_version or 0),
        'messages_processed': int(messages_processed or 0),
        'samples_written': int(samples_written or 0),
        'active_streams': list(active_streams or []),
        'last_error': str(last_error or ''),
        'owner_pid': os.getpid(),
        'thread_name': threading.current_thread().name,
        'thread_count': 1,
        'active': str(status or '').lower() not in {'dead', 'failed', 'unavailable', 'disconnected', 'closed', 'error'},
        'last_message_at': last_message_at or previous.get('last_message_at'),
        'last_sample_at': last_sample_at or previous.get('last_sample_at'),
    }
    if isinstance(extra, dict):
        payload.update(extra)
    with _BOOK_TICKER_WS_SUPERVISOR_LOCK:
        current_generation = int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('generation_id', 0) or 0)
        payload['generation_id'] = current_generation if generation_id is None else int(generation_id)
        if generation_id is not None and current_generation != int(generation_id):
            payload['persisted'] = False
            return payload
        if store is not None:
            store.save_json('book_ticker_ws_status', payload)
    return payload


def refresh_book_ticker_websocket_subscription(
    store: Optional[RuntimeStateStore],
    state: Dict[str, Any],
    requested_symbols: Sequence[Any],
    ws_module: Any,
    open_websocket_fn: Any,
    base_ws_url: str,
    connect_timeout_seconds: float,
    sslopt: Optional[Dict[str, Any]] = None,
    generation_id: Optional[int] = None,
) -> Dict[str, Any]:
    current_symbols = [str(row).strip().upper() for row in list(state.get('symbols') or []) if str(row).strip()]
    next_symbols = [row.split('@')[0].upper() for row in build_book_ticker_stream_names(requested_symbols)]
    if next_symbols == current_symbols:
        return {'reopened': False, 'symbols': current_symbols, 'subscription_version': int(state.get('subscription_version', 0) or 0)}
    ws = state.get('ws')
    if ws is not None and hasattr(ws, 'close'):
        ws.close()
    opened_ws = open_websocket_fn(next_symbols, ws_module=ws_module, base_ws_url=base_ws_url, connect_timeout_seconds=connect_timeout_seconds, sslopt=sslopt)
    with _BOOK_TICKER_WS_SUPERVISOR_LOCK:
        current = int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('generation_id', 0) or 0)
        if generation_id is not None and current != int(generation_id):
            if hasattr(opened_ws, 'close'):
                opened_ws.close()
            return {'reopened': False, 'symbols': current_symbols, 'subscription_version': int(state.get('subscription_version', 0) or 0), 'superseded': True}
        state['ws'] = opened_ws
        _BOOK_TICKER_WS_SUPERVISOR_STATE['ws'] = opened_ws
    state['symbols'] = next_symbols
    state['streams'] = build_book_ticker_stream_names(next_symbols)
    state['subscription_version'] = int(state.get('subscription_version', 0) or 0) + 1
    state['reconnect_count'] = int(state.get('reconnect_count', 0) or 0) + 1
    append_runtime_event(store, 'book_ticker_ws_subscription_refreshed', {
        'event_source': 'book_ticker_websocket',
        'symbols': next_symbols,
        'symbol_count': len(next_symbols),
        'subscription_version': state['subscription_version'],
        'reconnect_count': state['reconnect_count'],
    })
    previous_health = store.load_json('book_ticker_ws_status', {}) if store is not None else {}
    if not isinstance(previous_health, dict):
        previous_health = {}
    previous_messages_processed = int(previous_health.get('messages_processed', 0) or 0)
    previous_samples_written = int(previous_health.get('samples_written', 0) or 0)
    previous_status = str(previous_health.get('status') or '').strip().lower()
    next_status = 'healthy' if previous_status == 'healthy' and (previous_messages_processed > 0 or previous_samples_written > 0) else 'connecting'
    update_book_ticker_ws_health_state(
        store,
        status=next_status,
        symbols=next_symbols,
        reconnect_count=state['reconnect_count'],
        subscription_version=state['subscription_version'],
        messages_processed=previous_messages_processed,
        samples_written=previous_samples_written,
        active_streams=state['streams'],
        last_error=str(previous_health.get('last_error') or ''),
        generation_id=generation_id,
        last_message_at=previous_health.get('last_message_at'),
        last_sample_at=previous_health.get('last_sample_at'),
    )
    return {'reopened': True, 'symbols': next_symbols, 'subscription_version': state['subscription_version']}


def run_book_ticker_websocket_supervisor(
    store: Optional[RuntimeStateStore],
    initial_symbols: Sequence[Any],
    symbol_provider: Optional[Any],
    ws_module: Any,
    open_websocket_fn: Any = open_book_ticker_websocket,
    monitor_cycle_fn: Any = run_book_ticker_cache_monitor_cycle,
    sleep_fn: Any = time.sleep,
    max_supervisor_cycles: int = 0,
    base_ws_url: str = 'wss://fstream.binance.com/stream',
    connect_timeout_seconds: float = 10.0,
    recv_timeout_seconds: float = 5.0,
    max_messages_per_cycle: int = 100,
    max_samples: int = 20,
    reconnect_backoff_seconds: float = 2.0,
    reconnect_backoff_multiplier: float = 2.0,
    reconnect_backoff_cap_seconds: float = 30.0,
    zero_message_timeout_reconnect_threshold: int = 3,
    sslopt: Optional[Dict[str, Any]] = None,
    generation_id: Optional[int] = None,
) -> Dict[str, Any]:
    def generation_is_current() -> bool:
        if generation_id is None:
            return True
        with _BOOK_TICKER_WS_SUPERVISOR_LOCK:
            return int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('generation_id', 0) or 0) == int(generation_id)

    def open_and_register(symbols: Sequence[Any]):
        opened_ws = open_websocket_fn(symbols, ws_module=ws_module, base_ws_url=base_ws_url, connect_timeout_seconds=connect_timeout_seconds, sslopt=sslopt)
        with _BOOK_TICKER_WS_SUPERVISOR_LOCK:
            current = int(_BOOK_TICKER_WS_SUPERVISOR_STATE.get('generation_id', 0) or 0)
            if generation_id is not None and current != int(generation_id):
                if hasattr(opened_ws, 'close'):
                    opened_ws.close()
                return None
            _BOOK_TICKER_WS_SUPERVISOR_STATE['ws'] = opened_ws
            return opened_ws

    requested_symbols = list(initial_symbols or [])
    if callable(symbol_provider):
        provider_symbols = symbol_provider()
        if provider_symbols is not None:
            requested_symbols = list(provider_symbols)
    normalized_symbols = [row.split('@')[0].upper() for row in build_book_ticker_stream_names(requested_symbols)]
    if not normalized_symbols:
        raise ValueError('at least one symbol is required for bookTicker websocket supervisor')
    if not generation_is_current():
        return {'cycles_completed': 0, 'stopped': True, 'stop_reason': 'superseded_generation'}
    state: Dict[str, Any] = {
        'symbols': normalized_symbols,
        'streams': build_book_ticker_stream_names(normalized_symbols),
        'reconnect_count': 0,
        'subscription_version': 1,
    }
    state['ws'] = open_and_register(normalized_symbols)
    if state['ws'] is None:
        return {'cycles_completed': 0, 'stopped': True, 'stop_reason': 'superseded_generation'}
    update_book_ticker_ws_health_state(
        store,
        status='connecting',
        symbols=state['symbols'],
        reconnect_count=state['reconnect_count'],
        subscription_version=state['subscription_version'],
        active_streams=state['streams'],
        generation_id=generation_id,
    )
    cycles_completed = 0
    messages_processed_total = 0
    samples_written_total = 0
    backoff_seconds = max(float(reconnect_backoff_seconds or 0.0), 0.0)
    zero_message_timeouts = 0
    while generation_is_current():
        try:
            result = monitor_cycle_fn(
                store,
                state['ws'],
                ws_module=ws_module,
                max_messages=max_messages_per_cycle,
                max_samples=max_samples,
                recv_timeout_seconds=recv_timeout_seconds,
            )
        except Exception as exc:
            cycles_completed += 1
            if not generation_is_current():
                break
            result = {
                'status': 'disconnected',
                'messages_processed': 0,
                'samples_written': 0,
                'error': str(exc),
            }
            append_runtime_event(store, 'book_ticker_ws_monitor_error', {
                'event_source': 'book_ticker_websocket',
                'detail': str(exc),
                'symbols': list(state.get('symbols') or []),
                'subscription_version': int(state.get('subscription_version', 0) or 0),
                'reconnect_count': int(state.get('reconnect_count', 0) or 0),
            })
            update_book_ticker_ws_health_state(
                store,
                status='disconnected',
                symbols=state['symbols'],
                reconnect_count=state['reconnect_count'],
                subscription_version=state['subscription_version'],
                messages_processed=messages_processed_total,
                samples_written=samples_written_total,
                active_streams=state['streams'],
                last_error=str(exc),
                generation_id=generation_id,
            )
            refresh_result = {'reopened': False, 'symbols': list(state['symbols']), 'subscription_version': state['subscription_version']}
            refreshed_symbols = state['symbols']
            if callable(symbol_provider):
                provider_symbols = symbol_provider()
                if provider_symbols is not None:
                    refreshed_symbols = list(provider_symbols)
            if build_book_ticker_stream_names(refreshed_symbols) != build_book_ticker_stream_names(state['symbols']):
                state['symbols'] = [row.split('@')[0].upper() for row in build_book_ticker_stream_names(refreshed_symbols)]
                state['streams'] = build_book_ticker_stream_names(state['symbols'])
            if backoff_seconds > 0:
                sleep_fn(backoff_seconds)
            if not generation_is_current():
                break
            state['ws'] = open_and_register(state['symbols'])
            if state['ws'] is None:
                break
            state['reconnect_count'] = int(state.get('reconnect_count', 0) or 0) + 1
            append_rate_limited_runtime_event(store, 'book_ticker_ws_reconnected', {
                'event_source': 'book_ticker_websocket',
                'symbols': list(state.get('symbols') or []),
                'symbol_count': len(list(state.get('symbols') or [])),
                'subscription_version': state['subscription_version'],
                'reconnect_count': state['reconnect_count'],
                'backoff_seconds': backoff_seconds,
                'trigger': 'monitor_exception',
            }, key='monitor_exception', min_interval_seconds=300.0)
            update_book_ticker_ws_health_state(
                store,
                status='reconnecting',
                symbols=state['symbols'],
                reconnect_count=state['reconnect_count'],
                subscription_version=state['subscription_version'],
                messages_processed=messages_processed_total,
                samples_written=samples_written_total,
                active_streams=state['streams'],
                last_error=str(exc),
                generation_id=generation_id,
            )
            if backoff_seconds > 0:
                backoff_seconds = min(max(float(reconnect_backoff_cap_seconds or 0.0), 0.0), max(backoff_seconds * float(reconnect_backoff_multiplier or 1.0), backoff_seconds))
            if max_supervisor_cycles and cycles_completed >= int(max_supervisor_cycles):
                break
            continue
        cycles_completed += 1
        messages_processed_total += int(result.get('messages_processed', 0) or 0)
        samples_written_total += int(result.get('samples_written', 0) or 0)
        status = str(result.get('status') or 'unknown')
        if not generation_is_current():
            break
        if bool(result.get('zero_message_timeout')) or (status == 'idle_timeout' and int(result.get('messages_processed', 0) or 0) <= 0):
            zero_message_timeouts += 1
            if zero_message_timeouts >= max(int(zero_message_timeout_reconnect_threshold or 1), 1):
                status = 'disconnected'
                result['error'] = 'zero_message_timeout_reconnect_threshold_exceeded'
                append_runtime_event(store, 'book_ticker_ws_zero_message_timeout', {
                    'event_source': 'book_ticker_websocket',
                    'zero_message_timeouts': zero_message_timeouts,
                    'threshold': max(int(zero_message_timeout_reconnect_threshold or 1), 1),
                    'symbols': list(state.get('symbols') or []),
                })
                zero_message_timeouts = 0
        else:
            zero_message_timeouts = 0
        cycle_message_at = _isoformat_utc(_utc_now()) if int(result.get('messages_processed', 0) or 0) > 0 else None
        cycle_sample_at = _isoformat_utc(_utc_now()) if int(result.get('samples_written', 0) or 0) > 0 else None
        update_book_ticker_ws_health_state(
            store,
            status=status,
            symbols=state['symbols'],
            reconnect_count=state['reconnect_count'],
            subscription_version=state['subscription_version'],
            messages_processed=messages_processed_total,
            samples_written=samples_written_total,
            active_streams=state['streams'],
            last_error=str(result.get('error') or ''),
            generation_id=generation_id,
            last_message_at=cycle_message_at,
            last_sample_at=cycle_sample_at,
        )
        refresh_result = {'reopened': False, 'symbols': list(state['symbols']), 'subscription_version': state['subscription_version']}
        refreshed_symbols = state['symbols']
        if callable(symbol_provider):
            provider_symbols = symbol_provider()
            if provider_symbols is not None:
                refreshed_symbols = list(provider_symbols)
        if status == 'healthy':
            refresh_result = refresh_book_ticker_websocket_subscription(
                store,
                state,
                requested_symbols=refreshed_symbols,
                ws_module=ws_module,
                open_websocket_fn=open_websocket_fn,
                base_ws_url=base_ws_url,
                connect_timeout_seconds=connect_timeout_seconds,
                sslopt=sslopt,
                generation_id=generation_id,
            )
        elif build_book_ticker_stream_names(refreshed_symbols) != build_book_ticker_stream_names(state['symbols']):
            state['symbols'] = [row.split('@')[0].upper() for row in build_book_ticker_stream_names(refreshed_symbols)]
            state['streams'] = build_book_ticker_stream_names(state['symbols'])
        if status == 'disconnected' and not refresh_result['reopened']:
            if backoff_seconds > 0:
                sleep_fn(backoff_seconds)
            if not generation_is_current():
                break
            state['ws'] = open_and_register(state['symbols'])
            if state['ws'] is None:
                break
            state['reconnect_count'] = int(state.get('reconnect_count', 0) or 0) + 1
            append_rate_limited_runtime_event(store, 'book_ticker_ws_reconnected', {
                'event_source': 'book_ticker_websocket',
                'symbols': list(state.get('symbols') or []),
                'symbol_count': len(list(state.get('symbols') or [])),
                'subscription_version': state['subscription_version'],
                'reconnect_count': state['reconnect_count'],
                'backoff_seconds': backoff_seconds,
            }, key='disconnected', min_interval_seconds=300.0)
            update_book_ticker_ws_health_state(
                store,
                status='reconnecting',
                symbols=state['symbols'],
                reconnect_count=state['reconnect_count'],
                subscription_version=state['subscription_version'],
                messages_processed=messages_processed_total,
                samples_written=samples_written_total,
                active_streams=state['streams'],
                last_error=str(result.get('error') or ''),
                generation_id=generation_id,
            )
            if backoff_seconds > 0:
                backoff_seconds = min(max(float(reconnect_backoff_cap_seconds or 0.0), 0.0), max(backoff_seconds * float(reconnect_backoff_multiplier or 1.0), backoff_seconds))
        else:
            backoff_seconds = max(float(reconnect_backoff_seconds or 0.0), 0.0)
        if max_supervisor_cycles and cycles_completed >= int(max_supervisor_cycles):
            break
    return {
        'cycles_completed': cycles_completed,
        'reconnect_count': int(state.get('reconnect_count', 0) or 0),
        'messages_processed_total': messages_processed_total,
        'samples_written_total': samples_written_total,
        'symbols': list(state.get('symbols') or []),
        'subscription_version': int(state.get('subscription_version', 0) or 0),
    }


def collect_book_ticker_samples(
    client: BinanceFuturesClient,
    symbol: str,
    sample_count: int = 6,
    interval_ms: int = 150,
    store: Optional[RuntimeStateStore] = None,
    cache_max_age_seconds: float = 3.0,
    allow_rest_fallback: bool = True,
) -> List[Dict[str, Any]]:
    cached_samples = load_book_ticker_cache_samples(store, symbol, sample_count=sample_count, max_age_seconds=cache_max_age_seconds)
    if cached_samples:
        append_runtime_event(store, 'book_ticker_cache_hit', {
            'event_source': 'book_ticker_cache',
            'symbol': symbol,
            'sample_count': len(cached_samples),
            'cache_max_age_seconds': float(cache_max_age_seconds),
        })
        return cached_samples
    if not allow_rest_fallback:
        append_rate_limited_runtime_event(store, 'book_ticker_cache_miss', {
            'event_source': 'book_ticker_cache',
            'symbol': symbol,
            'requested_sample_count': max(int(sample_count or 0), 0),
            'cache_max_age_seconds': float(cache_max_age_seconds),
            'fallback': 'disabled',
        }, key=f'{symbol}:disabled', min_interval_seconds=60.0)
        return []
    if not hasattr(client, 'get'):
        return []
    append_rate_limited_runtime_event(store, 'book_ticker_cache_miss', {
        'event_source': 'book_ticker_cache',
        'symbol': symbol,
        'requested_sample_count': max(int(sample_count or 0), 0),
        'cache_max_age_seconds': float(cache_max_age_seconds),
        'fallback': 'rest_polling',
    }, key=symbol, min_interval_seconds=60.0)
    samples: List[Dict[str, Any]] = []
    sample_total = max(int(sample_count or 0), 0)
    for idx in range(sample_total):
        payload = _client_get_with_optional_purpose(client, '/fapi/v1/ticker/bookTicker', params={'symbol': symbol}, purpose='market_data')
        if isinstance(payload, dict) and payload:
            samples.append(payload)
        if idx < sample_total - 1 and interval_ms and interval_ms > 0:
            time.sleep(float(interval_ms) / 1000.0)
    return samples


def resolve_monitor_current_price(
    store: Optional[RuntimeStateStore],
    symbol: str,
    side: str,
    fallback_price: float,
    cache_max_age_seconds: float = 3.0,
) -> Dict[str, Any]:
    normalized_side = normalize_position_side(side)
    fallback = _to_float(fallback_price, default=0.0)
    snapshot = load_book_ticker_cache_snapshot(store, symbol, max_age_seconds=cache_max_age_seconds)
    if snapshot is not None:
        if normalized_side == POSITION_SIDE_SHORT and _to_float(snapshot.get('ask_price')) > 0:
            return {'price': float(snapshot['ask_price']), 'source': 'book_ticker_cache_ask', 'snapshot': snapshot}
        if normalized_side != POSITION_SIDE_SHORT and _to_float(snapshot.get('bid_price')) > 0:
            return {'price': float(snapshot['bid_price']), 'source': 'book_ticker_cache_bid', 'snapshot': snapshot}
        if _to_float(snapshot.get('mid_price')) > 0:
            return {'price': float(snapshot['mid_price']), 'source': 'book_ticker_cache_mid', 'snapshot': snapshot}
    return {'price': fallback, 'source': 'kline_close_fallback', 'snapshot': None}


def fetch_top_account_long_short_ratio(client: BinanceFuturesClient, symbol: str, period: str = '5m', limit: int = 10) -> List[Dict[str, Any]]:
    return _client_get_with_optional_purpose(client, '/futures/data/topLongShortAccountRatio', params={'symbol': symbol, 'period': period, 'limit': limit}, purpose='low_frequency_market_data')



FIVE_USDT_TIER_A_SYMBOLS = {
    'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'LINKUSDT', 'AVAXUSDT', 'SUIUSDT',
}
FIVE_USDT_TIER_B_SYMBOLS = {
    'NEARUSDT', 'ONDOUSDT', 'APTUSDT', 'SEIUSDT', 'TIAUSDT', 'INJUSDT', 'WIFUSDT', 'PEPEUSDT', 'FETUSDT', 'LDOUSDT',
}
FIVE_USDT_RECENT_UNDERPERFORMER_SYMBOLS = {
    'NOTUSDT', 'VICUSDT', 'HYPEUSDT', 'COSUSDT', 'NEARUSDT', 'WLDUSDT',
    'BNBUSDT', 'BTWUSDT', 'LITUSDT', 'BTCUSDT', 'ZROUSDT', 'BASUSDT',
    'CLOUSDT', 'UBUSDT',
}
FIVE_USDT_TIER_RULES: Dict[str, Dict[str, float]] = {
    'A': {'min_profit': 0.2, 'min_rr': 0.8, 'max_loss': 1.8},
    'B': {'min_profit': 0.3, 'min_rr': 0.9, 'max_loss': 1.6},
    'C': {'min_profit': 0.5, 'min_rr': 1.0, 'max_loss': 1.4},
}


def classify_five_usdt_symbol_quality_tier(symbol: str) -> Dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if normalized in FIVE_USDT_RECENT_UNDERPERFORMER_SYMBOLS:
        tier = 'C'; reason = 'recent_30d_underperformer_deweighted'
    elif normalized in FIVE_USDT_TIER_A_SYMBOLS:
        tier = 'A'; reason = 'high_liquidity_whitelist'
    elif normalized in FIVE_USDT_TIER_B_SYMBOLS:
        tier = 'B'; reason = 'medium_volatility_whitelist'
    else:
        tier = 'C'; reason = 'hot_or_momentum_symbol'
    rule = FIVE_USDT_TIER_RULES[tier]
    return {
        'symbol_quality_tier': tier,
        'symbol_quality_reason': reason,
        'symbol_tier_min_expected_net_profit_usdt': float(rule['min_profit']),
        'symbol_tier_min_rr': float(rule['min_rr']),
        'symbol_tier_max_loss_usdt': float(rule['max_loss']),
    }


def apply_five_usdt_symbol_quality_fields(candidate: Candidate) -> Candidate:
    payload = classify_five_usdt_symbol_quality_tier(candidate.symbol)
    for key, value in payload.items():
        setattr(candidate, key, value)
    candidate.reasons.append(f"symbol_quality_tier={payload['symbol_quality_tier']}")
    candidate.reasons.append(f"symbol_quality_reason={payload['symbol_quality_reason']}")
    return candidate


def _event_time(row: Dict[str, Any]) -> Optional[datetime]:
    return _parse_iso8601_utc(row.get('closed_at') or row.get('exit_at') or row.get('recorded_at') or row.get('updated_at'))


def build_symbol_loss_cooldown_map(store: Optional[RuntimeStateStore], *, limit: int = 5000) -> Dict[str, Dict[str, Any]]:
    if store is None or not hasattr(store, 'read_events'):
        return {}
    now = _utc_now()
    rows = store.read_events(limit=limit)
    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        if str(row.get('event_type') or '') not in {'trade_closed', 'position_closed', 'exit_filled', 'trade_summary'}:
            continue
        symbol = normalize_symbol(row.get('symbol'))
        if not symbol:
            continue
        pnl = _to_float(row.get('net_pnl_usdt', row.get('realized_pnl_usdt', row.get('pnl_usdt', row.get('net_profit_usdt')))), default=0.0)
        ts = _event_time(row)
        if ts is None:
            continue
        by_symbol.setdefault(symbol, []).append({'pnl': pnl, 'ts': ts})
    cooldowns: Dict[str, Dict[str, Any]] = {}
    for symbol, trades in by_symbol.items():
        trades.sort(key=lambda item: item['ts'])
        last = trades[-1]
        today_loss = sum(item['pnl'] for item in trades if item['ts'].date() == now.date() and item['pnl'] < 0)
        recent3 = trades[-3:]
        recent10 = trades[-10:]
        consecutive_losses = 0
        for trade in reversed(trades):
            if float(trade['pnl']) >= 0:
                break
            consecutive_losses += 1
        gross_profit_10 = sum(float(item['pnl']) for item in recent10 if float(item['pnl']) > 0)
        gross_loss_10 = abs(sum(float(item['pnl']) for item in recent10 if float(item['pnl']) < 0))
        profit_factor_10 = gross_profit_10 / gross_loss_10 if gross_loss_10 > 0 else None
        reasons: List[str] = []
        until: Optional[datetime] = None
        if float(last['pnl']) <= -2.5 and (now - last['ts']).total_seconds() < 24 * 3600:
            reasons.append('symbol_recent_loss_cooldown')
            until = max(until or now, last['ts'] + datetime.timedelta(hours=24))
        if today_loss <= -3.0:
            reasons.append('symbol_daily_loss_cooldown')
            until = max(until or now, datetime.datetime.combine(now.date() + datetime.timedelta(days=1), datetime.datetime.min.time(), tzinfo=datetime.timezone.utc))
        if len(recent3) >= 3 and sum(item['pnl'] for item in recent3) < 0:
            reasons.append('symbol_recent_loss_cooldown')
            until = max(until or now, last['ts'] + datetime.timedelta(hours=48))
        if consecutive_losses >= 3:
            reasons.append('symbol_consecutive_loss_cooldown')
            until = max(until or now, last['ts'] + datetime.timedelta(hours=24))
        elif consecutive_losses >= 2:
            reasons.append('symbol_consecutive_loss_cooldown')
            until = max(until or now, last['ts'] + datetime.timedelta(hours=6))
        if len(recent10) >= 10 and profit_factor_10 is not None and profit_factor_10 < 0.7:
            reasons.append('symbol_low_profit_factor_cooldown')
            until = max(until or now, last['ts'] + datetime.timedelta(hours=24))
        if reasons:
            cooldowns[symbol] = {'reasons': list(dict.fromkeys(reasons)), 'cooldown_until': _isoformat_utc(until or now), 'last_trade_net_pnl_usdt': round(float(last['pnl']), 4), 'today_loss_usdt': round(float(today_loss), 4), 'consecutive_losses': consecutive_losses, 'recent10_profit_factor': round(float(profit_factor_10), 4) if profit_factor_10 is not None else None}
    return cooldowns


def apply_five_usdt_watchlist_scoring(candidate: Candidate, regime_payload: Optional[Dict[str, Any]] = None) -> Candidate:
    regime_label = str((regime_payload or {}).get('label') or getattr(candidate, 'regime_label', '') or '').lower()
    risk_off_weight = 0.85 if regime_label == 'risk_off' else 1.0
    expected_profit = max(float(getattr(candidate, 'expected_net_profit_usdt', 0.0) or 0.0), 0.0)
    expected_rr = max(float(getattr(candidate, 'expected_rr', 0.0) or 0.0), 0.0)
    loss = max(float(getattr(candidate, 'expected_loss_usdt', 0.0) or 0.0), 0.0)
    volume_multiple = max(float(getattr(candidate, 'volume_multiple', 0.0) or 0.0), 0.0)
    oi_abs = abs(float(getattr(candidate, 'oi_change_pct_5m', 0.0) or 0.0))
    cvd_abs = abs(float(getattr(candidate, 'cvd_delta', 0.0) or 0.0))
    breakout_distance = abs(float(getattr(candidate, 'entry_distance_from_breakout_pct', 0.0) or 0.0))
    flags = dict(getattr(candidate, 'trigger_confirmation_flags', {}) or {})
    setup_bonus = 5.0 if bool(getattr(candidate, 'setup_ready', False)) else 0.0
    trigger_bonus = 8.0 if bool(getattr(candidate, 'trigger_fired', False)) else 0.0
    confirmation_bonus = 4.0 if flags.get('oi_taker_alignment_confirmed') else 0.0
    breakdown = {
        'expected_profit_score': round(min(expected_profit / 5.0, 1.4) * 22.0, 4),
        'rr_score': round(min(expected_rr / 2.0, 1.4) * 16.0, 4),
        'loss_control_score': round(max(0.0, 1.0 - max(loss - 2.0, 0.0) / 3.0) * 10.0, 4),
        'volume_score': round(min(volume_multiple / 2.0, 1.5) * 12.0, 4),
        'orderflow_score': round(min((oi_abs + cvd_abs) / 2.0, 1.5) * 16.0 + confirmation_bonus, 4),
        'breakout_proximity_score': round(max(0.0, 1.0 - breakout_distance / 1.2) * 12.0, 4),
        'setup_trigger_score': round(setup_bonus + trigger_bonus, 4),
    }
    watchlist_score = sum(float(value or 0.0) for value in breakdown.values()) * risk_off_weight
    candidate.five_usdt_score_breakdown = breakdown
    candidate.risk_off_score_weight = round(risk_off_weight, 4)
    candidate.watchlist_priority_score = round(max(float(getattr(candidate, 'score', 0.0) or 0.0), watchlist_score), 4)
    candidate.reasons.append(f"watchlist_priority_score={candidate.watchlist_priority_score}")
    candidate.reasons.append(f"risk_off_score_weight={candidate.risk_off_score_weight}")
    return candidate


def apply_five_usdt_candidate_selection_filter(candidate: Candidate, args: argparse.Namespace, store: Optional[RuntimeStateStore], cooldown_map: Optional[Dict[str, Dict[str, Any]]] = None) -> Optional[str]:
    apply_five_usdt_symbol_quality_fields(candidate)
    if normalize_symbol(candidate.symbol) in FIVE_USDT_RECENT_UNDERPERFORMER_SYMBOLS:
        candidate.reasons.append('recent_30d_underperformer_deweighted')
        return 'recent_30d_underperformer_blacklist'
    tier = str(candidate.symbol_quality_tier or 'C')
    if float(getattr(candidate, 'expected_net_profit_usdt', 0.0) or 0.0) < float(candidate.symbol_tier_min_expected_net_profit_usdt or 0.0):
        return 'symbol_tier_expected_profit_too_low'
    if float(getattr(candidate, 'expected_rr', 0.0) or 0.0) < float(candidate.symbol_tier_min_rr or 0.0):
        return 'symbol_tier_rr_too_low'
    if float(getattr(candidate, 'expected_loss_usdt', 0.0) or 0.0) > float(candidate.symbol_tier_max_loss_usdt or 0.0):
        return 'symbol_tier_loss_too_high'
    cd = (cooldown_map or {}).get(normalize_symbol(candidate.symbol), {})
    if cd:
        candidate.reasons.append(f"symbol_cooldown_until={cd.get('cooldown_until', '')}")
        return str((cd.get('reasons') or ['symbol_recent_loss_cooldown'])[0])
    execution_quality = compute_execution_quality_size_adjustment(candidate)
    if tier in {'B', 'C'} and str(execution_quality.get('execution_liquidity_grade') or '') in {'D', 'F'}:
        return 'c_tier_slippage_too_high' if tier == 'C' else 'execution_depth_veto'
    if tier == 'C':
        volume_spike = float(candidate.volume_multiple or 0.0) >= 1.0 or float(candidate.volume_zscore_5m or 0.0) >= 0.5
        high_conviction_economics = (
            float(getattr(candidate, 'score', 0.0) or 0.0) >= 120.0
            and float(getattr(candidate, 'expected_net_profit_usdt', 0.0) or 0.0) >= 1.0
            and float(getattr(candidate, 'expected_rr', 0.0) or 0.0) >= 1.1
        )
        if not volume_spike and not high_conviction_economics:
            return 'c_tier_missing_volume_spike'
        flags = dict(getattr(candidate, 'trigger_confirmation_flags', {}) or {})
        orderflow_confirms = 0
        if flags.get('oi_taker_alignment_confirmed') or abs(float(getattr(candidate, 'oi_change_pct_5m', 0.0) or 0.0)) > 0.0:
            orderflow_confirms += 1
        if flags.get('cvd_alignment_confirmed') or abs(float(getattr(candidate, 'cvd_delta', 0.0) or 0.0)) > 0.0:
            orderflow_confirms += 1
        taker_ratio = getattr(candidate, 'taker_buy_ratio', None)
        if taker_ratio is not None and abs(float(taker_ratio) - 0.5) >= 0.05:
            orderflow_confirms += 1
        min_orderflow_confirms = 1 if high_conviction_economics else 2
        if orderflow_confirms < min_orderflow_confirms:
            return 'c_tier_missing_orderflow_confirmation'
        if float(execution_quality.get('spread_bps') or 0.0) > 8.0 or float(getattr(candidate, 'expected_slippage_pct', 0.0) or 0.0) > 0.12:
            return 'c_tier_slippage_too_high'
    return None


def apply_profile_candidate_selection_filter(
    candidate: Candidate,
    args: argparse.Namespace,
    store: Optional[RuntimeStateStore],
    cooldown_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[str]:
    if str(getattr(args, 'profile', '') or '') != 'five-usdt-scalp-v2':
        return None
    return apply_five_usdt_candidate_selection_filter(candidate, args, store, cooldown_map)


def merged_candidate_symbols(**kwargs) -> Tuple[List[str], Dict[str, int], Dict[str, int], Dict[str, int]]:
    allowed_symbols = {
        str(symbol).strip().upper()
        for symbol in (kwargs.get('allowed_symbols') or [])
        if str(symbol).strip()
    }
    square_symbols = kwargs.get('square_symbols', [])
    if allowed_symbols:
        square_symbols = [symbol for symbol in square_symbols if str(symbol).strip().upper() in allowed_symbols]
    tickers = kwargs.get('tickers', [])
    if allowed_symbols:
        tickers = [
            row for row in tickers
            if str(row.get('symbol', '')).strip().upper() in allowed_symbols
        ]
    top_gainers = int(kwargs.get('top_gainers', 20) or 20)
    top_losers = int(kwargs.get('top_losers', top_gainers) or 0)
    hot_rank_map = {symbol: idx + 1 for idx, symbol in enumerate(square_symbols)}
    usdt_tickers = [t for t in tickers if str(t.get('symbol', '')).endswith('USDT')]
    gainers = sorted(usdt_tickers, key=lambda row: _to_float(row.get('priceChangePercent')), reverse=True)[:top_gainers]
    losers = sorted(usdt_tickers, key=lambda row: _to_float(row.get('priceChangePercent')))[:top_losers]
    gainer_rank_map = {row['symbol']: idx + 1 for idx, row in enumerate(gainers)}
    loser_rank_map = {row['symbol']: idx + 1 for idx, row in enumerate(losers)}
    merged = list(dict.fromkeys(list(hot_rank_map.keys()) + list(gainer_rank_map.keys()) + list(loser_rank_map.keys())))
    return merged, hot_rank_map, gainer_rank_map, loser_rank_map


def apply_hard_veto_filters(candidate: Candidate) -> Optional[str]:
    if is_tradfi_perps_symbol(getattr(candidate, 'symbol', '')):
        return 'tradfi_perps_agreement_required'
    execution_slippage_r = compute_expected_slippage_r(candidate)
    execution_liquidity_grade = classify_execution_liquidity_grade(candidate.book_depth_fill_ratio, execution_slippage_r)
    if candidate.smart_money_veto:
        return candidate.smart_money_veto_reason or 'smart_money_outflow_veto'
    if candidate.state == 'distribution':
        return 'distribution_state_veto'
    if candidate.exhaustion_score >= candidate.setup_score + 12 and candidate.cvd_delta <= 0:
        return 'distribution_blacklist'
    if candidate.cvd_delta < 0 and candidate.cvd_zscore <= -2.5:
        return 'negative_cvd_veto'
    oi_hard_reversal_threshold = abs(_to_float(getattr(candidate, 'oi_hard_reversal_threshold_pct', 0.8), default=0.8))
    if candidate.oi_change_pct_5m <= -oi_hard_reversal_threshold:
        return 'oi_reversal_veto'
    extended_chase_threshold = abs(_to_float(getattr(candidate, 'extended_chase_threshold_pct', 15.0), default=15.0))
    if candidate.price_change_pct_24h >= extended_chase_threshold and candidate.state in {'chase', 'momentum_extension', 'overheated'}:
        return 'extended_chase_veto'
    hard_slippage_r = max(_to_float(getattr(candidate, 'execution_slippage_hard_veto_r', 0.25), default=0.25), 0.0)
    if execution_slippage_r > hard_slippage_r:
        return 'execution_slippage_veto'
    risk_slippage_r = max(_to_float(getattr(candidate, 'execution_slippage_risk_threshold_r', 0.15), default=0.15), 0.0)
    if execution_liquidity_grade == 'D' and float(candidate.book_depth_fill_ratio or 0.0) < 0.45 and execution_slippage_r > risk_slippage_r:
        return 'execution_depth_veto'
    if bool(getattr(candidate, 'stop_too_wide_flag', False)) and float(getattr(candidate, 'position_size_pct', 0.0) or 0.0) <= 0.0:
        return 'stop_distance_too_wide_veto'
    if bool(getattr(candidate, 'stop_too_tight_flag', False)) and float(getattr(candidate, 'atr_stop_distance', 0.0) or 0.0) <= 0.0:
        return 'stop_distance_too_tight_veto'
    trade_side = normalize_trade_side(getattr(candidate, 'side', TRADE_SIDE_LONG))
    if trade_side == TRADE_SIDE_SHORT and candidate.smart_money_flow_score >= 0.35:
        return 'smart_money_long_pressure_veto'
    if trade_side == TRADE_SIDE_LONG and candidate.smart_money_flow_score <= -0.35:
        return 'smart_money_outflow_veto'
    if candidate.control_risk_score >= 20:
        return 'control_risk_veto'
    return None


def classify_alert_tier(candidate_or_score: Any, state: Optional[str] = None, regime_label: Optional[str] = None) -> str:
    side = None
    if isinstance(candidate_or_score, Candidate):
        candidate = candidate_or_score
        score = float(candidate.score)
        state = candidate.state
        regime_label = candidate.regime_label
        side = normalize_position_side(getattr(candidate, 'side', getattr(candidate, 'position_side', POSITION_SIDE_LONG)))
    else:
        score = float(candidate_or_score)
        state = state or 'none'
        regime_label = regime_label or 'neutral'

    if regime_label == 'risk_off' and side != POSITION_SIDE_SHORT:
        return 'blocked'
    if state == 'distribution':
        return 'blocked'
    if state == 'launch':
        return 'critical' if score >= 75 else 'high'
    if state == 'momentum_extension':
        return 'watch' if score >= 60 else 'blocked'
    if state == 'overheated':
        return 'watch' if score >= 65 else 'blocked'
    if score >= 80 or state == 'squeeze':
        return 'critical'
    if score >= 70:
        return 'high'
    if score >= 60:
        return 'watch'
    return 'blocked'


def _trade_memory_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


class TradeMemoryEngine:
    def __init__(self, store: RuntimeStateStore, lookback: int = 500):
        self.store = store
        self.lookback = max(int(lookback or 500), 1)

    def _bucket_key(self, row: Dict[str, Any]) -> str:
        symbol = str(row.get('symbol') or '').upper()
        side = normalize_position_side(row.get('position_side') or row.get('side'))
        profile = str(row.get('profile') or 'default')
        trigger = str(row.get('trigger_class') or row.get('trigger_type') or 'unknown')
        decile = str(row.get('score_decile') or score_to_decile_label(_trade_memory_float(row.get('score'), 0.0)) or 'unknown')
        return f'profile={profile}|symbol={symbol}|side={side}|trigger={trigger}|decile={decile}'

    def review_closed_trade(self, event: Dict[str, Any]) -> Dict[str, Any]:
        realized_r = _trade_memory_float(event.get('realized_r'), 0.0)
        review = {
            'reviewed_at': _isoformat_utc(_utc_now()),
            'symbol': str(event.get('symbol') or '').upper(),
            'side': normalize_position_side(event.get('position_side') or event.get('side')),
            'profile': str(event.get('profile') or 'default'),
            'score_decile': str(event.get('score_decile') or score_to_decile_label(_trade_memory_float(event.get('score'), 0.0))),
            'trigger_class': str(event.get('trigger_class') or event.get('trigger_type') or 'unknown'),
            'exit_reason': str(event.get('exit_reason') or ''),
            'realized_r': round(realized_r, 4),
            'mfe_r': round(_trade_memory_float(event.get('mfe_r'), 0.0), 4),
            'mae_r': round(_trade_memory_float(event.get('mae_r'), 0.0), 4),
            'quality_label': 'win' if realized_r > 0 else 'loss' if realized_r < 0 else 'scratch',
        }
        existing = self.store.load_json('trade_reviews', [])
        if not isinstance(existing, list):
            existing = []
        existing.append(review)
        self.store.save_json('trade_reviews', existing[-self.lookback:])
        return review

    def aggregate(self, events: Optional[Sequence[Dict[str, Any]]] = None) -> Dict[str, Any]:
        rows = list(events if events is not None else self.store.read_events(limit=self.lookback))[-self.lookback:]
        buckets: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            event_type = str(row.get('event_type') or '')
            if event_type not in {'trade_invalidated', 'candidate_rejected'}:
                continue
            key = self._bucket_key(row)
            bucket = buckets.setdefault(key, {'closed_trades': 0, 'wins': 0, 'losses': 0, 'rejections': 0, 'realized_rs': [], 'mfe_rs': [], 'mae_rs': []})
            if event_type == 'candidate_rejected':
                bucket['rejections'] += 1
                continue
            realized_r = _trade_memory_float(row.get('realized_r'), 0.0)
            bucket['closed_trades'] += 1
            bucket['wins'] += 1 if realized_r > 0 else 0
            bucket['losses'] += 1 if realized_r < 0 else 0
            bucket['realized_rs'].append(realized_r)
            bucket['mfe_rs'].append(_trade_memory_float(row.get('mfe_r'), 0.0))
            bucket['mae_rs'].append(_trade_memory_float(row.get('mae_r'), 0.0))
        for bucket in buckets.values():
            closed = int(bucket['closed_trades'])
            realized = bucket.pop('realized_rs')
            mfe = bucket.pop('mfe_rs')
            mae = bucket.pop('mae_rs')
            bucket['win_rate'] = round(float(bucket['wins']) / closed, 4) if closed else 0.0
            bucket['avg_realized_r'] = round(sum(realized) / len(realized), 4) if realized else 0.0
            bucket['avg_mfe_r'] = round(sum(mfe) / len(mfe), 4) if mfe else 0.0
            bucket['avg_mae_r'] = round(sum(mae) / len(mae), 4) if mae else 0.0
        memory = {'updated_at': _isoformat_utc(_utc_now()), 'lookback': self.lookback, 'buckets': buckets}
        try:
            self.store.save_json('trade_memory', memory)
        except Exception:
            pass
        return memory


def _trade_memory_lookup_keys(candidate: Candidate, profile: str) -> List[str]:
    symbol = str(getattr(candidate, 'symbol', '') or '').upper()
    side = normalize_position_side(getattr(candidate, 'position_side', None) or getattr(candidate, 'side', None))
    trigger = str(getattr(candidate, 'trigger_class', '') or getattr(candidate, 'trigger_type', '') or 'unknown')
    decile = score_to_decile_label(_trade_memory_float(getattr(candidate, 'score', 0.0), 0.0))
    return [
        f'profile={profile}|symbol={symbol}|side={side}|trigger={trigger}|decile={decile}',
        f'profile=default|symbol={symbol}|side={side}|trigger={trigger}|decile={decile}',
    ]


def apply_trade_memory_to_candidate(candidate: Candidate, memory: Optional[Dict[str, Any]], profile: str = 'default') -> Candidate:
    buckets = memory.get('buckets', {}) if isinstance(memory, dict) else {}
    matched = None
    for key in _trade_memory_lookup_keys(candidate, profile):
        bucket = buckets.get(key)
        if isinstance(bucket, dict) and int(bucket.get('closed_trades') or 0) >= 2:
            matched = bucket
            break
    if not matched:
        setattr(candidate, 'trade_memory_adjustment', 0.0)
        return candidate
    avg_r = _trade_memory_float(matched.get('avg_realized_r'), 0.0)
    win_rate = _trade_memory_float(matched.get('win_rate'), 0.0)
    adjustment = -6.0 if avg_r < -0.25 or win_rate < 0.35 else 3.0 if avg_r > 0.75 and win_rate >= 0.5 else 0.0
    candidate.score = round(max(0.0, min(100.0, float(candidate.score or 0.0) + adjustment)), 2)
    setattr(candidate, 'trade_memory_adjustment', adjustment)
    if adjustment:
        label = 'poor' if adjustment < 0 else 'strong'
        candidate.reasons = list(candidate.reasons or []) + [f'trade_memory_bucket={label}:avg_r={avg_r:.2f},win_rate={win_rate:.2f}']
    return candidate


def apply_adaptive_trade_memory_profile(args: argparse.Namespace, memory: Optional[Dict[str, Any]] = None) -> argparse.Namespace:
    if not bool(getattr(args, 'enable_trade_memory', False)):
        setattr(args, 'trade_memory_risk_multiplier', 1.0)
        setattr(args, 'trade_memory_adjustments', [])
        return args
    buckets = (memory or {}).get('buckets', {}) if isinstance(memory, dict) else {}
    profile = str(getattr(args, 'profile', 'default') or 'default')
    closed = wins = 0
    realized_sum = 0.0
    for key, bucket in buckets.items():
        if not str(key).startswith(f'profile={profile}|'):
            continue
        n = int(bucket.get('closed_trades') or 0)
        closed += n
        wins += int(bucket.get('wins') or 0)
        realized_sum += _trade_memory_float(bucket.get('avg_realized_r'), 0.0) * n
    adjustments: List[str] = []
    multiplier = 1.0
    if closed >= 3:
        avg_r = realized_sum / closed if closed else 0.0
        win_rate = wins / closed if closed else 0.0
        if avg_r < -0.2 or win_rate < 0.35:
            multiplier = 0.5
            adjustments.append('poor_recent_memory')
            if hasattr(args, 'min_score') and not bool(getattr(args, 'adaptive_read_only', False)):
                args.min_score = float(getattr(args, 'min_score') or 0.0) + 5.0
        elif avg_r > 0.75 and win_rate >= 0.5:
            multiplier = 1.15
            adjustments.append('strong_recent_memory')
    if bool(getattr(args, 'adaptive_read_only', False)):
        if adjustments:
            adjustments = [f'{item}_read_only' for item in adjustments]
        multiplier = 1.0
    elif bool(getattr(args, 'disable_adaptive_risk_upscale', False)) and multiplier > 1.0:
        multiplier = 1.0
        if adjustments:
            adjustments = [f'{item}_risk_upscale_disabled' for item in adjustments]
    if hasattr(args, 'risk_usdt'):
        args.risk_usdt = round(float(getattr(args, 'risk_usdt') or 0.0) * multiplier, 6)
    setattr(args, 'trade_memory_risk_multiplier', multiplier)
    setattr(args, 'trade_memory_adjustments', adjustments)
    return args


def recommended_position_size_pct(score_or_tier: Any, alert_tier: Optional[str] = None, regime_multiplier: float = 1.0, side_multiplier: float = 1.0) -> float:
    if alert_tier is None:
        tier = str(score_or_tier)
    else:
        tier = str(alert_tier)
    base = {'blocked': 0.0, 'watch': 0.5, 'medium': 1.0, 'high': 3.0, 'critical': 3.0}.get(tier, 0.0)
    effective_multiplier = max(float(regime_multiplier), 0.0) * max(float(side_multiplier), 0.0)
    return round(base * effective_multiplier, 4)


def refresh_candidate_execution_sizing(candidate: Candidate, external_signal: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    external_signal = external_signal or {}
    candidate.alert_tier = classify_alert_tier(candidate)
    external_tier = str(external_signal.get('external_signal_tier', '') or '').lower()
    tier_order = {'blocked': 0, 'watch': 1, 'high': 2, 'critical': 3}
    if tier_order.get(external_tier, -1) > tier_order.get(candidate.alert_tier, 0):
        candidate.alert_tier = external_tier
    external_position_size_pct = next(
        (
            float(reason.split('=', 1)[1])
            for reason in reversed(candidate.reasons)
            if str(reason).startswith('external_position_size_pct=')
        ),
        None,
    )
    side_multiplier = float(getattr(candidate, 'side_risk_multiplier', 1.0) or 1.0)
    base_position_size_pct = recommended_position_size_pct(
        candidate.score,
        candidate.alert_tier,
        float(getattr(candidate, 'regime_multiplier', 1.0) or 1.0),
        side_multiplier,
    )
    execution_quality = compute_execution_quality_size_adjustment(candidate)
    effective_position_size_pct = round(base_position_size_pct * float(execution_quality['size_multiplier']), 4)
    candidate.position_size_pct = round(external_position_size_pct, 4) if external_position_size_pct and external_position_size_pct > 0 else effective_position_size_pct
    candidate.reasons.append(f'alert_tier={candidate.alert_tier}')
    candidate.reasons.append(f'base_position_size_pct={base_position_size_pct}')
    candidate.reasons.append(f"execution_quality_size_multiplier={execution_quality['size_multiplier']}")
    candidate.reasons.append(f"execution_quality_size_bucket={execution_quality['size_bucket']}")
    candidate.reasons.append(f'position_size_pct={candidate.position_size_pct}')
    return {
        'base_position_size_pct': base_position_size_pct,
        'execution_quality': execution_quality,
        'position_size_pct': candidate.position_size_pct,
        'alert_tier': candidate.alert_tier,
    }


def _normalize_regime_label(regime_label: str) -> str:
    normalized = str(regime_label or 'RANGE').strip().upper()
    legacy = {
        'RISK_ON': 'BULL_TREND',
        'RISK_OFF': 'BEAR_TREND',
        'CAUTION': 'RANGE',
        'NEUTRAL': 'RANGE',
    }
    return legacy.get(normalized, normalized)


def derive_side_risk_multiplier(side: str, regime_label: str) -> float:
    normalized_side = normalize_position_side(side)
    raw_regime = str(regime_label or 'neutral').strip().lower()
    legacy_bias = {
        'risk_on': {POSITION_SIDE_LONG: 1.15, POSITION_SIDE_SHORT: 0.85},
        'risk_off': {POSITION_SIDE_LONG: 0.85, POSITION_SIDE_SHORT: 1.15},
        'caution': {POSITION_SIDE_LONG: 0.9, POSITION_SIDE_SHORT: 0.9},
        'neutral': {POSITION_SIDE_LONG: 1.0, POSITION_SIDE_SHORT: 1.0},
    }
    if raw_regime in legacy_bias:
        return legacy_bias[raw_regime].get(normalized_side, 1.0)
    normalized_regime = _normalize_regime_label(regime_label)
    if normalized_regime == 'BULL_TREND':
        return 1.15 if normalized_side == POSITION_SIDE_LONG else 0.85
    if normalized_regime == 'BEAR_TREND':
        return 0.85 if normalized_side == POSITION_SIDE_LONG else 1.15
    if normalized_regime == 'PANIC':
        return 0.6 if normalized_side == POSITION_SIDE_LONG else 1.0
    if normalized_regime == 'EUPHORIA':
        return 1.0 if normalized_side == POSITION_SIDE_LONG else 0.8
    if normalized_regime == 'RANGE':
        return 1.0
    return 0.9


def derive_directional_score_multiplier(side: str, regime_label: str, base_multiplier: float) -> float:
    normalized_side = normalize_position_side(side)
    raw_regime = str(regime_label or '').strip().lower()
    if raw_regime in {'risk_on', 'risk_off'}:
        return 1.0 if normalized_side == POSITION_SIDE_SHORT else round(max(float(base_multiplier or 1.0), 0.0), 4)
    normalized_regime = _normalize_regime_label(regime_label)
    base = max(float(base_multiplier or 1.0), 0.0)
    side_weights = {
        'BULL_TREND': {'LONG': 0.70, 'SHORT': 0.30},
        'BEAR_TREND': {'LONG': 0.20, 'SHORT': 0.80},
        'RANGE': {'LONG': 0.50, 'SHORT': 0.50},
        'PANIC': {'LONG': 0.10, 'SHORT': 0.90},
        'EUPHORIA': {'LONG': 0.60, 'SHORT': 0.40},
    }.get(normalized_regime, {'LONG': 0.50, 'SHORT': 0.50})
    side_key = 'SHORT' if normalized_side == POSITION_SIDE_SHORT else 'LONG'
    return round(base * (0.5 + side_weights[side_key]), 4)


MICRO_SCALP_SCORE_KEYS = (
    'volume_spike_score',
    'price_acceleration_score',
    'book_imbalance_score',
    'vwap_ema_reclaim_score',
    'oi_delta_score',
    'cvd_alignment_score',
    'breakout_proximity_score',
    'funding_crowding_score',
)


def _clamp_score(value: float, cap: float) -> float:
    return round(max(0.0, min(float(value or 0.0), cap)), 4)


def compute_micro_scalp_score_breakdown(candidate: Candidate) -> Dict[str, float]:
    volume_multiple = float(getattr(candidate, 'volume_multiple', 0.0) or 0.0)
    price_move = abs(float(getattr(candidate, 'recent_5m_change_pct', 0.0) or 0.0))
    spread_bps = abs(float(getattr(candidate, 'spread_bps', getattr(candidate, 'expected_spread_bps', 0.0)) or 0.0))
    breakout_distance = abs(float(getattr(candidate, 'entry_distance_from_breakout_pct', 0.0) or 0.0))
    ema_distance = abs(float(getattr(candidate, 'distance_from_ema20_5m_pct', 0.0) or 0.0))
    vwap_distance = abs(float(getattr(candidate, 'distance_from_vwap_15m_pct', getattr(candidate, 'entry_distance_from_vwap_pct', 0.0)) or 0.0))
    oi_delta = abs(float(getattr(candidate, 'oi_change_pct_5m', getattr(candidate, 'oi_delta_pct', 0.0)) or 0.0))
    cvd_delta = abs(float(getattr(candidate, 'cvd_delta', getattr(candidate, 'cvd_delta_5m', 0.0)) or 0.0))
    funding_rate = abs(float(getattr(candidate, 'funding_rate', 0.0) or 0.0))
    bid_ask_imbalance = abs(float(getattr(candidate, 'book_imbalance', getattr(candidate, 'bid_ask_imbalance', 0.0)) or 0.0))

    return {
        'volume_spike_score': _clamp_score(volume_multiple / 1.5 * 20.0, 20.0),
        'price_acceleration_score': _clamp_score(price_move / 0.18 * 15.0, 15.0),
        'book_imbalance_score': _clamp_score((bid_ask_imbalance * 15.0) if bid_ask_imbalance else max(0.0, 15.0 - spread_bps * 2.5), 15.0),
        'vwap_ema_reclaim_score': _clamp_score(15.0 - (ema_distance + vwap_distance) * 4.0, 15.0),
        'oi_delta_score': _clamp_score(oi_delta / 0.35 * 10.0, 10.0),
        'cvd_alignment_score': _clamp_score(cvd_delta / 1.0 * 10.0, 10.0),
        'breakout_proximity_score': _clamp_score(10.0 - breakout_distance * 8.0, 10.0),
        'funding_crowding_score': _clamp_score(5.0 - funding_rate * 5000.0, 5.0),
    }


def classify_micro_scalp_score(score: float) -> Dict[str, bool]:
    score = float(score or 0.0)
    return {
        'candidate_pool': score >= 55.0,
        'strong_trade_candidate': score >= 70.0,
    }


SECTOR_SYMBOL_MAP = {
    'MEME': {'DOGEUSDT', 'SHIBUSDT', 'PEPEUSDT', 'BONKUSDT', '1000BONKUSDT', 'FLOKIUSDT', 'WIFUSDT'},
    'AI': {'FETUSDT', 'AGIXUSDT', 'AIUSDT', 'NFPUSDT', 'ARKMUSDT', 'WLDUSDT', 'TAOUSDT'},
    'L2': {'ARBUSDT', 'OPUSDT', 'STRKUSDT', 'MANTAUSDT', 'METISUSDT'},
    'DEFI': {'UNIUSDT', 'AAVEUSDT', 'MKRUSDT', 'LDOUSDT', 'CRVUSDT', 'SUSHIUSDT'},
    'RWA': {'ONDOUSDT', 'PENDLEUSDT', 'POLYXUSDT', 'OMUSDT'},
    'INFRASTRUCTURE': {'LINKUSDT', 'ATOMUSDT', 'DOTUSDT', 'NEARUSDT', 'FILUSDT', 'ICPUSDT'},
}


def classify_symbol_sector(symbol: str) -> str:
    normalized = str(symbol or '').upper()
    for sector, symbols in SECTOR_SYMBOL_MAP.items():
        if normalized in symbols:
            return sector
    return 'OTHER'


def compute_sector_score(symbol: str, peer_symbols: Sequence[str]) -> Dict[str, Any]:
    sector = classify_symbol_sector(symbol)
    same_sector = [s for s in (peer_symbols or []) if classify_symbol_sector(str(s).upper()) == sector and str(s).upper() != str(symbol).upper()]
    sector_score = min(12.0, len(same_sector) * 4.0) if sector != 'OTHER' else 0.0
    return {'sector': sector, 'same_sector_count': len(same_sector), 'sector_score': sector_score, 'same_sector_symbols': same_sector[:10]}


def classify_candidate_pool_grade(score: float, *, setup_ready: bool = False, trigger_fired: bool = False) -> Dict[str, Any]:
    numeric_score = float(score or 0.0)
    if trigger_fired and numeric_score >= 75.0:
        grade = 'A'
    elif setup_ready or numeric_score >= 60.0:
        grade = 'B'
    else:
        grade = 'C'
    return {'grade': grade, 'candidate_pool': numeric_score >= 45.0 or setup_ready or trigger_fired}


def _connect_watchlist_db(db_path: Any) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        'CREATE TABLE IF NOT EXISTS watchlist ('
        'symbol TEXT PRIMARY KEY, score REAL NOT NULL, first_seen REAL NOT NULL, '
        'last_seen REAL NOT NULL, max_score REAL NOT NULL, sector TEXT NOT NULL)'
    )
    return conn


def upsert_watchlist_db(db_path: Any, rows: Sequence[Dict[str, Any]], *, now_ts: Optional[float] = None) -> None:
    now_value = float(now_ts if now_ts is not None else time.time())
    with _connect_watchlist_db(db_path) as conn:
        for row in rows or []:
            symbol = str(row.get('symbol') or '').upper().strip()
            if not symbol:
                continue
            score = float(row.get('score', row.get('scalp_score', 0.0)) or 0.0)
            sector = str(row.get('sector') or classify_symbol_sector(symbol))
            existing = conn.execute('SELECT first_seen, max_score FROM watchlist WHERE symbol = ?', (symbol,)).fetchone()
            first_seen = float(existing[0]) if existing else now_value
            max_score = max(float(existing[1]) if existing else score, score)
            conn.execute(
                'INSERT OR REPLACE INTO watchlist(symbol, score, first_seen, last_seen, max_score, sector) VALUES (?, ?, ?, ?, ?, ?)',
                (symbol, score, first_seen, now_value, max_score, sector),
            )


def load_watchlist_db(db_path: Any, *, now_ts: Optional[float] = None, ttl_hours: float = 72.0) -> List[Dict[str, Any]]:
    path = Path(db_path)
    if not path.exists():
        return []
    now_value = float(now_ts if now_ts is not None else time.time())
    cutoff = now_value - float(ttl_hours or 72.0) * 3600.0
    with _connect_watchlist_db(path) as conn:
        conn.execute('DELETE FROM watchlist WHERE last_seen < ?', (cutoff,))
        fetched = conn.execute(
            'SELECT symbol, score, first_seen, last_seen, max_score, sector FROM watchlist ORDER BY max_score DESC, last_seen DESC'
        ).fetchall()
    return [
        {'symbol': symbol, 'score': score, 'first_seen': first_seen, 'last_seen': last_seen, 'max_score': max_score, 'sector': sector}
        for symbol, score, first_seen, last_seen, max_score, sector in fetched
    ]


def build_micro_scalp_cycle_diagnostics(
    *,
    early_filter_pass_count: int,
    early_filter_reject_reasons: Dict[str, Any],
    candidate_alerts: List[Dict[str, Any]],
    blocked_reasons: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    blocked_reasons = dict(blocked_reasons or {})
    top_candidates = sorted(
        list(candidate_alerts or []),
        key=lambda item: float(item.get('scalp_score', item.get('score', 0.0)) or 0.0),
        reverse=True,
    )[:10]
    why_no_order = {
        'liquidity_failed': int(blocked_reasons.get('liquidity_failed', 0) or 0),
        'spread_failed': int(blocked_reasons.get('spread_failed', 0) or 0),
        'score_too_low': int(blocked_reasons.get('score_too_low', 0) or 0),
        'trigger_missing': int(blocked_reasons.get('trigger_missing', 0) or 0),
        'risk_blocked': int(blocked_reasons.get('risk_blocked', 0) or 0),
        'execution_blocked': int(blocked_reasons.get('execution_blocked', 0) or 0),
    }
    runtime_stats = {
        'candidate_count': len(candidate_alerts or []),
        'near_trade_count': sum(1 for item in (candidate_alerts or []) if item.get('candidate_pool_grade') in {'A', 'B'} or item.get('setup_ready')),
        'watchlist_count': int(blocked_reasons.get('watchlist_count', 0) or 0),
        'oi_cache_hits': int(blocked_reasons.get('oi_cache_hits', 0) or 0),
        'funding_cache_hits': int(blocked_reasons.get('funding_cache_hits', 0) or 0),
        'rest_weight': int(blocked_reasons.get('rest_weight', blocked_reasons.get('rest_used_weight_1m', 0)) or 0),
    }
    why_no_trade_top5 = [
        {
            'symbol': item.get('symbol'),
            'scalp_score': item.get('scalp_score', item.get('score', 0.0)),
            'candidate_pool_grade': item.get('candidate_pool_grade'),
            'trade_missing': list(item.get('trade_missing') or item.get('trigger_missing') or item.get('setup_missing') or []),
        }
        for item in top_candidates[:5]
    ]
    return {
        'early_filter_pass_count': int(early_filter_pass_count or 0),
        'early_filter_reject_reasons': dict(early_filter_reject_reasons or {}),
        'scalp_score_top_symbols': [
            {
                'symbol': item.get('symbol'),
                'scalp_score': item.get('scalp_score', item.get('score', 0.0)),
            }
            for item in top_candidates
        ],
        'top_10_candidates': top_candidates,
        'why_no_order': why_no_order,
        'why_no_trade_top5': why_no_trade_top5,
        'runtime_stats': runtime_stats,
    }


def build_standardized_alert(candidate: Candidate, regime_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    regime_payload = regime_payload or {
        'label': candidate.regime_label,
        'score_multiplier': candidate.regime_multiplier,
        'reasons': [],
    }
    execution_quality = compute_execution_quality_size_adjustment(candidate)
    side_multiplier = round(float(getattr(candidate, 'side_risk_multiplier', 1.0) or 1.0), 4)
    base_position_size_pct = round(
        recommended_position_size_pct(
            candidate.score,
            candidate.alert_tier,
            candidate.regime_multiplier,
            side_multiplier,
        ),
        4,
    )
    return {
        'symbol': candidate.symbol,
        'sector': str(getattr(candidate, 'sector', classify_symbol_sector(candidate.symbol)) or ''),
        'sector_score': round(float(getattr(candidate, 'sector_score', 0.0) or 0.0), 4),
        'candidate_pool_grade': str(getattr(candidate, 'candidate_pool_grade', '') or ''),
        'candidate_pool': bool(getattr(candidate, 'candidate_pool', False)),
        'market_regime': str((regime_payload or {}).get('label') or getattr(candidate, 'regime_label', '') or ''),
        'symbol_quality_tier': str(getattr(candidate, 'symbol_quality_tier', '') or ''),
        'symbol_quality_reason': str(getattr(candidate, 'symbol_quality_reason', '') or ''),
        'symbol_tier_min_expected_net_profit_usdt': round(float(getattr(candidate, 'symbol_tier_min_expected_net_profit_usdt', 0.0) or 0.0), 4),
        'symbol_tier_min_rr': round(float(getattr(candidate, 'symbol_tier_min_rr', 0.0) or 0.0), 4),
        'symbol_tier_max_loss_usdt': round(float(getattr(candidate, 'symbol_tier_max_loss_usdt', 0.0) or 0.0), 4),
        'score': round(candidate.score, 2),
        'scalp_score': round(float(getattr(candidate, 'scalp_score', 0.0) or 0.0), 2),
        'scalp_score_breakdown': dict(getattr(candidate, 'scalp_score_breakdown', {}) or {}),
        'five_usdt_score_breakdown': dict(getattr(candidate, 'five_usdt_score_breakdown', {}) or {}),
        'watchlist_priority_score': round(float(getattr(candidate, 'watchlist_priority_score', 0.0) or 0.0), 2),
        'risk_off_score_weight': round(float(getattr(candidate, 'risk_off_score_weight', 1.0) or 1.0), 4),
        'strong_trade_candidate': bool(getattr(candidate, 'strong_trade_candidate', False)),
        'state': candidate.state,
        'alert_tier': candidate.alert_tier,
        'loser_rank': candidate.loser_rank,
        'position_size_pct': candidate.position_size_pct,
        'base_position_size_pct': base_position_size_pct,
        'side_risk_multiplier': side_multiplier,
        'portfolio_narrative_bucket': candidate.portfolio_narrative_bucket,
        'portfolio_correlation_group': candidate.portfolio_correlation_group,
        'execution_quality_size_multiplier': execution_quality['size_multiplier'],
        'execution_quality_size_bucket': execution_quality['size_bucket'],
        'atr_stop_distance': round(candidate.atr_stop_distance, 8),
        'stop_model': getattr(candidate, 'stop_model', 'structure'),
        'stop_distance_pct': round(float(getattr(candidate, 'stop_distance_pct', 0.0) or 0.0), 4),
        'stop_too_tight_flag': bool(getattr(candidate, 'stop_too_tight_flag', False)),
        'stop_too_wide_flag': bool(getattr(candidate, 'stop_too_wide_flag', False)),
        'must_pass_flags': dict(candidate.must_pass_flags or {}),
        'quality_score': round(float(candidate.quality_score or 0.0), 4),
        'execution_priority_score': round(float(candidate.execution_priority_score or 0.0), 4),
        'entry_distance_from_breakout_pct': round(candidate.entry_distance_from_breakout_pct, 4),
        'entry_distance_from_vwap_pct': round(candidate.entry_distance_from_vwap_pct, 4),
        'candle_extension_pct': round(float(getattr(candidate, 'candle_extension_pct', 0.0) or 0.0), 4),
        'recent_3bar_runup_pct': round(float(getattr(candidate, 'recent_3bar_runup_pct', 0.0) or 0.0), 4),
        'overextension_flag': getattr(candidate, 'overextension_flag', False),
        'entry_pattern': str(getattr(candidate, 'entry_pattern', '') or ''),
        'trend_regime': str(getattr(candidate, 'trend_regime', '') or ''),
        'liquidity_grade': str(getattr(candidate, 'liquidity_grade', '') or ''),
        'setup_ready': bool(getattr(candidate, 'setup_ready', False)),
        'trigger_fired': bool(getattr(candidate, 'trigger_fired', False)),
        'candidate_stage': candidate.candidate_stage,
        'setup_missing': list(candidate.setup_missing or []),
        'trigger_missing': list(candidate.trigger_missing or []),
        'trade_missing': list(candidate.trade_missing or []),
        'trigger_confirmation_flags': dict(candidate.trigger_confirmation_flags or {}),
        'trigger_confirmation_count': int(candidate.trigger_confirmation_count or 0),
        'trigger_min_confirmations': int(candidate.trigger_min_confirmations or 0),
        'tradeability_score': round(float(getattr(candidate, 'tradeability_score', 0.0) or 0.0), 1),
        'expected_edge': round(float(getattr(candidate, 'expected_edge', 0.0) or 0.0), 4),
        'expected_total_fee_pct': round(float(getattr(candidate, 'expected_total_fee_pct', 0.0) or 0.0), 4),
        'execution_slippage_buffer_pct': round(float(getattr(candidate, 'execution_slippage_buffer_pct', 0.0) or 0.0), 4),
        'min_profit_buffer_pct': round(float(getattr(candidate, 'min_profit_buffer_pct', 0.0) or 0.0), 4),
        'expected_slippage_pct': round(candidate.expected_slippage_pct, 4),
        'expected_slippage_r': execution_quality['expected_slippage_r'],
        'book_depth_fill_ratio': round(candidate.book_depth_fill_ratio, 4),
        'execution_liquidity_grade': execution_quality['execution_liquidity_grade'],
        'execution_mode': execution_quality['execution_mode'],
        'pretrigger_watch': bool(getattr(candidate, 'pretrigger_watch', False)),
        'trigger_relax_mode': bool(getattr(candidate, 'trigger_relax_mode', False)),
        'spread_bps': execution_quality['spread_bps'],
        'orderbook_slope': execution_quality['orderbook_slope'],
        'cancel_rate': execution_quality['cancel_rate'],
        'market_regime_label': regime_payload.get('label', candidate.regime_label),
        'market_regime_multiplier': round(float(regime_payload.get('score_multiplier', candidate.regime_multiplier) or 0.0), 4),
        'market_regime_reasons': regime_payload.get('reasons', []),
        'okx_sentiment_score': candidate.okx_sentiment_score,
        'okx_sentiment_acceleration': candidate.okx_sentiment_acceleration,
        'sector_resonance_score': candidate.sector_resonance_score,
        'smart_money_flow_score': candidate.smart_money_flow_score,
        'sentiment': {
            'okx_sentiment_score': candidate.okx_sentiment_score,
            'okx_sentiment_acceleration': candidate.okx_sentiment_acceleration,
            'sector_resonance_score': candidate.sector_resonance_score,
            'smart_money_flow_score': candidate.smart_money_flow_score,
            'onchain_smart_money_score': candidate.onchain_smart_money_score,
            'smart_money_sources': candidate.smart_money_sources,
        },
        'risk': {
            'funding_rate': candidate.funding_rate,
            'funding_rate_avg': candidate.funding_rate_avg,
            'control_risk_score': candidate.control_risk_score,
            'smart_money_veto': candidate.smart_money_veto,
            'smart_money_veto_reason': candidate.smart_money_veto_reason,
        },
        'reasons': candidate.reasons,
        'state_reasons': candidate.state_reasons,
    }


def derive_candidate_diagnostics(candidate: Candidate) -> Dict[str, Any]:
    flags = dict(getattr(candidate, 'trigger_confirmation_flags', {}) or {})
    setup_missing: List[str] = []
    trigger_missing: List[str] = []
    trade_missing: List[str] = []

    if flags.get('waiting_breakout'):
        setup_missing.append('waiting_breakout')
    if flags.get('watch_only_breakout_distance'):
        setup_missing.append('breakout_distance_too_far_for_setup')
    if flags.get('breakout_close_confirmed') is False:
        setup_missing.append('breakout_close_not_confirmed')
    if flags.get('retest_support_confirmed') is False:
        setup_missing.append('retest_not_confirmed')
    if flags.get('high_elastic_long_pullback_confirmed') is False:
        setup_missing.append('elastic_pullback_not_confirmed')
    if flags.get('funding_crowding_ok') is False:
        setup_missing.append('funding_crowding_not_ok')
    if flags.get('long_crowding_ok') is False:
        setup_missing.append('long_crowding_not_ok')
    if getattr(candidate, 'overextension_flag', False):
        setup_missing.append('price_extension_too_far')
    if str(getattr(candidate, 'state', 'none') or 'none') in {'none', 'distribution', 'overheated'}:
        setup_missing.append(f"state_{getattr(candidate, 'state', 'none')}")

    if flags.get('oi_taker_alignment_confirmed') is False:
        trigger_missing.append('oi_taker_not_confirmed')
    if flags.get('cvd_alignment_confirmed') is False:
        trigger_missing.append('cvd_not_confirmed')
    confirmation_count = int(getattr(candidate, 'trigger_confirmation_count', 0) or 0)
    min_confirmations = int(getattr(candidate, 'trigger_min_confirmations', 1) or 1)
    if confirmation_count < min_confirmations:
        trigger_missing.append('confirmation_count_below_minimum')
    if flags.get('waiting_breakout'):
        trigger_missing.append('waiting_breakout')

    if not bool(getattr(candidate, 'setup_ready', False)):
        trade_missing.append('candidate_setup_not_ready')
    elif not bool(getattr(candidate, 'trigger_fired', False)):
        trade_missing.append('candidate_trigger_not_fired')

    if bool(getattr(candidate, 'trigger_fired', False)):
        stage = 'trade_candidate'
    elif bool(getattr(candidate, 'setup_ready', False)):
        stage = 'setup_candidate'
    elif setup_missing:
        stage = 'watch_candidate'
    else:
        stage = 'watch_candidate'

    return {
        'candidate_stage': stage,
        'setup_missing': list(dict.fromkeys(setup_missing)),
        'trigger_missing': list(dict.fromkeys(trigger_missing)),
        'trade_missing': list(dict.fromkeys(trade_missing)),
    }


def apply_candidate_diagnostics(candidate: Candidate) -> Candidate:
    diagnostics = derive_candidate_diagnostics(candidate)
    candidate.candidate_stage = diagnostics['candidate_stage']
    candidate.setup_missing = diagnostics['setup_missing']
    candidate.trigger_missing = diagnostics['trigger_missing']
    candidate.trade_missing = diagnostics['trade_missing']
    return candidate


def candidate_trigger_relax_score(candidate: Candidate) -> Dict[str, Any]:
    flags = dict(getattr(candidate, 'trigger_confirmation_flags', {}) or {})
    points = 0
    reasons: List[str] = []
    if flags.get('breakout_close_confirmed') or abs(float(getattr(candidate, 'entry_distance_from_breakout_pct', 999.0) or 999.0)) <= 0.35:
        points += 2
        reasons.append('aggressive_breakout_near_level')
    if abs(float(getattr(candidate, 'recent_5m_change_pct', 0.0) or 0.0)) >= 0.6:
        points += 1
        reasons.append('momentum_5m_confirmed')
    if float(getattr(candidate, 'volume_multiple', 0.0) or 0.0) >= 1.5 or float(getattr(candidate, 'volume_zscore_5m', 0.0) or 0.0) >= 1.0:
        points += 1
        reasons.append('volume_expansion_confirmed')
    if flags.get('oi_taker_alignment_confirmed') or float(getattr(candidate, 'oi_change_pct_5m', 0.0) or 0.0) > 0:
        points += 1
        reasons.append('oi_confirmed')
    if flags.get('cvd_alignment_confirmed') or float(getattr(candidate, 'cvd_delta', 0.0) or 0.0) > 0 or float(getattr(candidate, 'cvd_zscore', 0.0) or 0.0) > 0:
        points += 1
        reasons.append('cvd_confirmed')
    if flags.get('taker_buy_confirmed') or (getattr(candidate, 'taker_buy_ratio', None) is not None and float(getattr(candidate, 'taker_buy_ratio') or 0.0) >= 0.52):
        points += 1
        reasons.append('taker_confirmed')
    return {'points': points, 'reasons': reasons}


def build_trigger_relax_candidate(candidate: Candidate, args: argparse.Namespace) -> Candidate:
    relax = candidate_trigger_relax_score(candidate)
    clone = replace(candidate)
    clone.pretrigger_watch = True
    clone.trigger_relax_mode = True
    clone.trigger_fired = True
    clone.candidate_stage = 'pre_trigger_watch'
    clone.trade_missing = []
    clone.trigger_missing = [reason for reason in list(getattr(candidate, 'trigger_missing', []) or []) if reason not in {'oi_taker_not_confirmed', 'cvd_not_confirmed', 'confirmation_count_below_minimum', 'waiting_breakout'}]
    clone.reasons = list(getattr(candidate, 'reasons', []) or []) + [
        'trigger_relax_mode=enabled',
        f"trigger_relax_points={relax['points']}",
        'execution_mode=pretrigger_maker_watch',
        *[f'trigger_relax_{reason}' for reason in relax['reasons']],
    ]
    return clone


def is_trigger_relax_eligible(candidate: Candidate, args: argparse.Namespace, regime_payload: Optional[Dict[str, Any]] = None) -> bool:
    if not bool(getattr(args, 'trigger_relax_mode', False)):
        return False
    if bool(getattr(candidate, 'trigger_fired', False)):
        return False
    if not bool(getattr(candidate, 'setup_ready', False)):
        soft_setup_misses = {'waiting_breakout', 'breakout_close_not_confirmed'}
        setup_missing = {str(item) for item in (getattr(candidate, 'setup_missing', []) or [])}
        if not setup_missing or not setup_missing.issubset(soft_setup_misses):
            return False
        entry_distance = float(getattr(candidate, 'entry_distance_from_breakout_pct', 0.0) or 0.0)
        if entry_distance < -0.35 or entry_distance > 0.05:
            return False
    if float(getattr(candidate, 'score', 0.0) or 0.0) < float(getattr(args, 'trigger_relax_min_score', 70.0) or 70.0):
        return False
    if bool(getattr(candidate, 'overextension_flag', False)):
        return False
    if str(getattr(candidate, 'liquidity_grade', 'B') or 'B') in {'D', 'E'}:
        return False
    if normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG)) == POSITION_SIDE_LONG and str((regime_payload or {}).get('label') or '').lower() == 'risk_off':
        return False
    relax = candidate_trigger_relax_score(candidate)
    return int(relax.get('points', 0) or 0) >= int(getattr(args, 'trigger_relax_min_points', 3) or 3)


def run_scan_once(client: Optional[BinanceFuturesClient], args: argparse.Namespace, explicit_square_symbols: Optional[Sequence[str]] = None):
    store = get_runtime_state_store(args)
    symbol_loss_cooldown_map = (
        build_symbol_loss_cooldown_map(store)
        if str(getattr(args, 'profile', '') or '') == 'five-usdt-scalp-v2'
        else {}
    )
    trade_memory = None
    if bool(getattr(args, 'enable_trade_memory', False)):
        trade_memory = TradeMemoryEngine(store).aggregate()
    base_okx = load_okx_sentiment_map(args)
    auto_okx = load_okx_sentiment_map_auto(args) if getattr(args, 'okx_auto', False) or getattr(args, 'okx_sentiment_command', '') else {}
    okx_map = dict(base_okx)
    okx_map.update(auto_okx)
    onchain_smart_money = load_manual_smart_money_map(args)
    external_signal_payload = load_external_signal_payload(args)
    external_signal_map = normalize_external_signal_map(external_signal_payload)

    square_symbols = list(explicit_square_symbols or load_manual_square_symbols(args))
    metas = fetch_exchange_meta(client)
    scan_seed_symbols = list(dict.fromkeys([*square_symbols, *list(external_signal_map.keys())]))
    fallback_symbols = scan_seed_symbols
    tickers, ticker_cache_diagnostics = resolve_scan_tickers(client, store, args, fallback_symbols=fallback_symbols, return_diagnostics=True)
    merged_payload = merged_candidate_symbols(
        square_symbols=scan_seed_symbols,
        tickers=tickers,
        allowed_symbols=metas.keys(),
        top_gainers=getattr(args, 'top_gainers', 20),
        top_losers=getattr(args, 'top_losers', getattr(args, 'top_gainers', 20)),
    )
    if len(merged_payload) == 4:
        merged_symbols, hot_rank_map, gainer_rank_map, loser_rank_map = merged_payload
    else:
        merged_symbols, hot_rank_map, gainer_rank_map = merged_payload
        loser_rank_map = {}
    raw_merged_symbol_count = len(merged_symbols)
    max_candidates = int(getattr(args, 'max_candidates', 8) or 8)
    prefilter_multiplier = max(1, int(getattr(args, 'scan_prefilter_multiplier', 2) or 2))
    prefilter_limit = max(max_candidates, max_candidates * prefilter_multiplier)
    merged_symbols = merged_symbols[:prefilter_limit]
    okx_unavailable_symbols: List[str] = []
    okx_available_inst_count = 0
    ticker_map = {row['symbol']: row for row in tickers if isinstance(row, dict) and row.get('symbol')}
    missing_ticker_symbols = [symbol for symbol in merged_symbols if symbol not in ticker_map]
    if missing_ticker_symbols:
        ticker_cache_diagnostics['scanner_patch_fallback_disabled'] = True
        ticker_cache_diagnostics['scanner_patch_fallback_skipped_reason'] = 'scanner_patch_fallback_disabled'
        degraded_patch_rows = build_degraded_ticker_rows_from_book_ticker_cache(
            store,
            missing_ticker_symbols,
            max_age_seconds=float(getattr(args, 'scanner_order_book_cache_max_age_seconds', 30.0) or 30.0),
        )
        for row in degraded_patch_rows:
            symbol_key = normalize_symbol(row.get('symbol'))
            if symbol_key:
                ticker_map[symbol_key] = row
        if degraded_patch_rows:
            ticker_cache_diagnostics['degraded'] = True
            ticker_cache_diagnostics['degraded_reason'] = 'ticker_24hr_cache_missing'
            ticker_cache_diagnostics['ticker_24hr_cache_row_count'] = max(int(ticker_cache_diagnostics.get('ticker_24hr_cache_row_count') or 0), len(ticker_map))

    regime_payload = compute_market_regime_filter(
        btc_klines=resolve_scan_klines(client, store, args, 'BTCUSDT', '15m', 30) if client else None,
        sol_klines=resolve_scan_klines(client, store, args, 'SOLUSDT', '15m', 30) if client else None,
    )
    market_direction_payload = compute_market_direction_bias(tickers, regime_payload)
    regime_payload['market_direction_bias'] = market_direction_payload

    rejected_events: List[Dict[str, Any]] = []
    early_reject_stats: Dict[str, Any] = {'total': 0, 'by_reason': {}, 'by_side': {}}
    candidates: List[Candidate] = []
    built_candidates: List[Candidate] = []
    candidate_alerts: List[Dict[str, Any]] = []
    evaluated_symbols = merged_symbols[: max(max_candidates * prefilter_multiplier, max_candidates)]
    scan_symbol_plan = resolve_expensive_scan_symbol_sets(
        evaluated_symbols,
        watchlist_symbols=parse_symbol_list(getattr(args, 'scan_watchlist_symbols', '')),
        microstructure_limit=int(getattr(args, 'scan_microstructure_symbol_limit', 10) or 10),
        funding_limit=int(getattr(args, 'scan_funding_symbol_limit', 8) or 8),
        open_interest_limit=int(getattr(args, 'scan_open_interest_symbol_limit', 8) or 8),
        cvd_limit=int(getattr(args, 'scan_cvd_symbol_limit', 8) or 8),
    )
    evaluated_symbols = scan_symbol_plan['priority_symbols'][: max(max_candidates * prefilter_multiplier, max_candidates)]
    microstructure_symbols = set(scan_symbol_plan['microstructure_symbols'])
    funding_symbols = set(scan_symbol_plan['funding_symbols'])
    open_interest_symbols = set(scan_symbol_plan['open_interest_symbols'])
    cvd_symbols = set(scan_symbol_plan['cvd_symbols'])
    scan_budget_diagnostics = build_scan_budget_diagnostics(args, scan_symbol_plan)
    allowed_trade_sides = resolve_allowed_trade_sides(getattr(args, 'allowed_trade_sides', 'long,short'))
    evaluated_side_count = 0
    for symbol in evaluated_symbols:
        meta = metas.get(symbol)
        ticker = ticker_map.get(symbol)
        if not meta:
            continue
        if not ticker:
            ticker_cache_diagnostics['symbols_skipped_due_to_missing_ticker_24hr'] = int(ticker_cache_diagnostics.get('symbols_skipped_due_to_missing_ticker_24hr') or 0) + 1
            book_snapshot = load_book_ticker_cache_snapshot(store, symbol, max_age_seconds=float(getattr(args, 'scanner_order_book_cache_max_age_seconds', 30.0) or 30.0))
            if not isinstance(book_snapshot, dict):
                continue
            ticker = {
                'symbol': symbol,
                'priceChangePercent': '0',
                'quoteVolume': '0',
                'lastPrice': str(book_snapshot.get('mid_price') or ''),
                'degraded_ticker_24hr': True,
                'volume_change_unknown': True,
                'source': 'book_ticker_cache_degraded',
            }
            ticker_cache_diagnostics['degraded'] = True
            ticker_cache_diagnostics['degraded_reason'] = 'ticker_24hr_cache_missing'
        klines_5m = resolve_scan_klines(client, store, args, symbol, '5m', max(getattr(args, 'lookback_bars', 12) + 30, 40))
        klines_15m = resolve_scan_klines(client, store, args, symbol, '15m', 40)
        klines_1h = resolve_scan_klines(client, store, args, symbol, '1h', 40)
        klines_4h = resolve_scan_klines(client, store, args, symbol, '4h', 40)
        if symbol in microstructure_symbols:
            funding_rates = fetch_funding_rates(client, symbol, limit=3) if symbol in funding_symbols else []
            funding_rate = funding_rates[-1] if funding_rates else None
            funding_rate_avg = sum(funding_rates) / len(funding_rates) if funding_rates else None
            oi_history = fetch_open_interest_hist(client, symbol, period='5m', limit=30) if symbol in open_interest_symbols else []
            top_ratio = fetch_top_account_long_short_ratio(client, symbol, period='5m', limit=10) if symbol in open_interest_symbols else []
            order_book = resolve_scan_order_book(client, store, args, symbol, limit=20)
            book_ticker_samples = collect_book_ticker_samples(client, symbol, sample_count=6, interval_ms=150, store=store, allow_rest_fallback=bool(getattr(args, 'book_ticker_rest_fallback', False))) if symbol in cvd_symbols else []
        else:
            funding_rates = []
            funding_rate = None
            funding_rate_avg = None
            oi_history = []
            top_ratio = []
            order_book = {}
            book_ticker_samples = []
        micro = derive_microstructure_inputs(
            oi_history=oi_history,
            taker_5m=klines_5m[-1] if klines_5m else [],
            taker_15m=klines_15m[-20:] if klines_15m else [],
            top_account_long_short=top_ratio,
            order_book=order_book,
            book_ticker_samples=book_ticker_samples,
        )
        okx_payload = okx_map.get(symbol, {})
        external_signal = external_signal_map.get(symbol, {})
        for candidate_side in allowed_trade_sides:
            evaluated_side_count += 1
            candidate = build_candidate(
                symbol=symbol,
                ticker=ticker,
                klines_5m=klines_5m,
                klines_15m=klines_15m,
                klines_1h=klines_1h,
                klines_4h=klines_4h,
                meta=meta,
                hot_rank=hot_rank_map.get(symbol),
                gainer_rank=gainer_rank_map.get(symbol),
                loser_rank=loser_rank_map.get(symbol),
                risk_usdt=float(getattr(args, 'risk_usdt', 10.0) or 10.0),
                max_notional_usdt=float(getattr(args, 'max_notional_usdt', 0.0) or 0.0),
                min_notional_usdt=float(getattr(args, 'min_notional_usdt', 0.0) or 0.0),
                lookback_bars=int(getattr(args, 'lookback_bars', 12) or 12),
                swing_bars=int(getattr(args, 'swing_bars', 6) or 6),
                min_5m_change_pct=float(getattr(args, 'min_5m_change_pct', 2.0) or 0.0),
                min_quote_volume=float(getattr(args, 'min_quote_volume', 50_000_000.0) or 0.0),
                stop_buffer_pct=float(getattr(args, 'stop_buffer_pct', 0.01) or 0.01),
                max_rsi_5m=float(getattr(args, 'max_rsi_5m', 80.0) or 80.0),
                min_volume_multiple=float(getattr(args, 'min_volume_multiple', 1.8) or 0.0),
                max_distance_from_ema_pct=float(getattr(args, 'max_distance_from_ema_pct', 10.0) or 10.0),
                funding_rate=funding_rate,
                funding_rate_threshold=float(getattr(args, 'max_funding_rate', 0.0005) or 0.0005),
                funding_rate_avg=funding_rate_avg,
                funding_rate_avg_threshold=float(getattr(args, 'max_funding_rate_avg', 0.0003) or 0.0003),
                max_distance_from_vwap_pct=float(getattr(args, 'max_distance_from_vwap_pct', 10.0) or 10.0),
                max_leverage=int(getattr(args, 'leverage', 5) or 5),
                okx_sentiment_score=float(okx_payload.get('okx_sentiment_score', 0.0) or 0.0),
                okx_sentiment_acceleration=float(okx_payload.get('okx_sentiment_acceleration', 0.0) or 0.0),
                sector_resonance_score=float(okx_payload.get('sector_resonance_score', 0.0) or 0.0),
                smart_money_flow_score=float(okx_payload.get('smart_money_flow_score', 0.0) or 0.0),
                onchain_smart_money_score=float(onchain_smart_money.get(symbol, 0.0) or 0.0),
                market_regime=regime_payload,
                external_signal=external_signal,
                use_external_setup_relaxation=bool(getattr(args, 'use_external_setup_relaxation', False)),
                watch_breakout_tolerance_pct=float(getattr(args, 'watch_breakout_tolerance_pct', 0.0) or 0.0),
                setup_breakout_tolerance_pct=float(getattr(args, 'setup_breakout_tolerance_pct', 0.0) or 0.0),
                oi_hard_reversal_threshold_pct=float(getattr(args, 'oi_hard_reversal_threshold_pct', 0.8) or 0.8),
                extended_chase_threshold_pct=float(getattr(args, 'extended_chase_threshold_pct', 15.0) or 15.0),
                execution_slippage_hard_veto_r=float(getattr(args, 'execution_slippage_hard_veto_r', 0.25) or 0.25),
                execution_slippage_risk_threshold_r=float(getattr(args, 'execution_slippage_risk_threshold_r', 0.15) or 0.15),
                trigger_min_confirmations=int(getattr(args, 'trigger_min_confirmations', 2) or 2),
                expected_reward_r=float(getattr(args, 'tp1_r', 1.0) or 1.0),
                base_acceleration_ratio=float(getattr(args, 'base_acceleration_ratio', 1.25) or 1.25),
                side=candidate_side,
                early_reject_stats=early_reject_stats,
                **micro,
            )
            if candidate is None:
                continue
            target_net_profit = float(getattr(args, 'target_net_profit_usdt', 0.0) or 0.0)
            if target_net_profit > 0:
                direction = 1.0 if normalize_trade_side(getattr(candidate, 'side', TRADE_SIDE_LONG)) == TRADE_SIDE_LONG else -1.0
                risk_distance = abs(float(candidate.last_price or 0.0) - float(candidate.stop_price or 0.0))
                planned_tp = float(candidate.last_price or 0.0) + direction * risk_distance * float(getattr(args, 'tp1_r', 1.5) or 1.5)
                target_plan = plan_five_usdt_target_trade(
                    getattr(candidate, 'side', TRADE_SIDE_LONG),
                    float(candidate.last_price or 0.0),
                    planned_tp,
                    float(candidate.stop_price or 0.0),
                    target_net_profit_usdt=target_net_profit,
                    min_target_net_profit_usdt=float(getattr(args, 'min_target_net_profit_usdt', 0.0) or 0.0),
                    max_loss_usdt=float(getattr(args, 'max_loss_usdt', 0.0) or 0.0),
                    min_expected_rr=float(getattr(args, 'min_expected_rr', 0.0) or 0.0),
                    min_notional_usdt=float(getattr(args, 'min_notional_usdt', 0.0) or 0.0),
                    max_notional_usdt=float(getattr(args, 'max_notional_usdt', 0.0) or 0.0),
                    taker_fee_rate=0.0005,
                    slippage_buffer_pct=max(float(getattr(candidate, 'execution_slippage_buffer_pct', 0.0) or 0.0) / 100.0, 0.0008),
                    step_size=float(getattr(meta, 'step_size', 0.0) or 0.0),
                    tick_size=float(getattr(meta, 'tick_size', 0.0) or 0.0),
                    quantity_precision=int(getattr(meta, 'quantity_precision', 10) or 10),
                    price_precision=int(getattr(meta, 'price_precision', 10) or 10),
                    disable_tiny_tp=bool(getattr(args, 'disable_tiny_tp', False)),
                )
                for key, value in target_plan.items():
                    if hasattr(candidate, key):
                        setattr(candidate, key, value)
                if target_plan.get('planned_quantity', 0.0) > 0:
                    candidate.quantity = float(target_plan['planned_quantity'])
                candidate.reasons.append(f"target_net_profit_usdt={target_net_profit}")
                candidate.reasons.append(f"expected_net_profit_usdt={candidate.expected_net_profit_usdt}")
                candidate.reasons.append(f"expected_loss_usdt={candidate.expected_loss_usdt}")
                candidate.reasons.append(f"expected_rr={candidate.expected_rr}")
                if candidate.target_profit_reject_reason:
                    apply_candidate_diagnostics(candidate)
                    candidate.reasons.append(f"target_profit_reject:{candidate.target_profit_reject_reason}")
                    rejected_events.append(append_candidate_rejected_event(None, candidate, [candidate.target_profit_reject_reason]))
                    continue
            if bool(getattr(args, 'five_usdt_watchlist_scoring', False)):
                apply_five_usdt_watchlist_scoring(candidate, regime_payload)
            profile_reject_reason = apply_profile_candidate_selection_filter(
                candidate, args, store, symbol_loss_cooldown_map,
            )
            if profile_reject_reason:
                apply_candidate_diagnostics(candidate)
                candidate.reasons.append(profile_reject_reason)
                rejected_events.append(append_candidate_rejected_event(None, candidate, [profile_reject_reason]))
                continue
            apply_candidate_diagnostics(candidate)
            built_candidates.append(candidate)
            external_veto_reason = apply_external_signal_to_candidate(candidate, external_signal)
            if external_veto_reason:
                apply_candidate_diagnostics(candidate)
                candidate.reasons.append(external_veto_reason)
                rejected_events.append(append_candidate_rejected_event(None, candidate, [external_veto_reason]))
                continue
            veto_reason = apply_hard_veto_filters(candidate)
            if veto_reason:
                apply_candidate_diagnostics(candidate)
                candidate.reasons.append(veto_reason)
                rejected_events.append(append_candidate_rejected_event(None, candidate, [veto_reason]))
                continue
            regime_multiplier = float(regime_payload.get('score_multiplier', 1.0) or 1.0)
            regime_label = str(regime_payload.get('label', 'neutral') or 'neutral')
            side_multiplier = derive_side_risk_multiplier(getattr(candidate, 'side', POSITION_SIDE_LONG), regime_label)
            directional_score_multiplier = derive_directional_score_multiplier(getattr(candidate, 'side', POSITION_SIDE_LONG), regime_label, regime_multiplier)
            candidate.score *= directional_score_multiplier
            apply_market_direction_bias(candidate, market_direction_payload)
            if trade_memory is not None:
                candidate = apply_trade_memory_to_candidate(candidate, trade_memory, profile=str(getattr(args, 'profile', 'default') or 'default'))
            candidate.regime_label = regime_label
            candidate.regime_multiplier = regime_multiplier
            candidate.side_risk_multiplier = side_multiplier
            candidate.reasons.append(f'market_regime_multiplier={regime_multiplier:.2f}')
            candidate.reasons.append(f'directional_score_multiplier={directional_score_multiplier:.2f}')
            candidate.reasons.append(f'side_risk_multiplier={side_multiplier:.2f}')
            for regime_reason in regime_payload.get('reasons', []):
                candidate.reasons.append(f'market_regime:{regime_reason}')
            refresh_candidate_execution_sizing(candidate, external_signal)
            apply_candidate_diagnostics(candidate)
            sector_payload = compute_sector_score(candidate.symbol, evaluated_symbols)
            candidate.sector = sector_payload['sector']
            candidate.sector_score = float(sector_payload['sector_score'])
            if candidate.sector_score:
                candidate.score = round(float(candidate.score or 0.0) + candidate.sector_score, 4)
                candidate.reasons.append(f"sector_score={candidate.sector_score}")
                refresh_candidate_execution_sizing(candidate, external_signal)
            pool_grade = classify_candidate_pool_grade(
                float(getattr(candidate, 'scalp_score', getattr(candidate, 'score', 0.0)) or getattr(candidate, 'score', 0.0)),
                setup_ready=bool(getattr(candidate, 'setup_ready', False)),
                trigger_fired=bool(getattr(candidate, 'trigger_fired', False)),
            )
            candidate.candidate_pool_grade = pool_grade['grade']
            candidate.candidate_pool = bool(pool_grade['candidate_pool'])
            if str(getattr(args, 'profile', '') or '') == 'five-usdt-scalp-v2':
                candidate.scalp_score_breakdown = compute_micro_scalp_score_breakdown(candidate)
                candidate.scalp_score = round(sum(float(value or 0.0) for value in candidate.scalp_score_breakdown.values()), 2)
                micro_scalp_classification = classify_micro_scalp_score(candidate.scalp_score)
                candidate.strong_trade_candidate = bool(micro_scalp_classification['strong_trade_candidate'])
                candidate.score = max(float(candidate.score or 0.0), float(candidate.scalp_score or 0.0))
                pool_grade = classify_candidate_pool_grade(
                    float(candidate.scalp_score or candidate.score or 0.0),
                    setup_ready=bool(getattr(candidate, 'setup_ready', False)),
                    trigger_fired=bool(getattr(candidate, 'trigger_fired', False)),
                )
                candidate.candidate_pool_grade = pool_grade['grade']
                candidate.candidate_pool = bool(pool_grade['candidate_pool'])
                candidate.reasons.append(f"scalp_score={candidate.scalp_score}")
                candidate.reasons.append(f"strong_trade_candidate={candidate.strong_trade_candidate}")
            candidates.append(candidate)

    candidates.sort(key=lambda item: float(getattr(item, 'scalp_score', getattr(item, 'score', 0.0)) or getattr(item, 'score', 0.0)), reverse=True)
    candidate_alerts = [build_standardized_alert(item, regime_payload) for item in candidates]
    if str(getattr(args, 'profile', '') or '') == 'five-usdt-scalp-v2':
        try:
            watchlist_db_path = Path(CANONICAL_RUNTIME_STATE_DIR) / 'watchlist.db'
            upsert_watchlist_db(watchlist_db_path, candidate_alerts)
            persisted_watchlist = load_watchlist_db(watchlist_db_path, ttl_hours=72.0)
        except Exception:
            persisted_watchlist = []
    else:
        persisted_watchlist = []
    execution_candidates = [item for item in candidates if bool(getattr(item, 'trigger_fired', False))]
    relaxed_candidates = [build_trigger_relax_candidate(item, args) for item in candidates if is_trigger_relax_eligible(item, args, regime_payload)]
    execution_priority = sorted(
        [*execution_candidates, *relaxed_candidates],
        key=lambda item: (
            1 if bool(getattr(item, 'trigger_fired', False)) and not bool(getattr(item, 'pretrigger_watch', False)) else 0,
            float(getattr(item, 'score', 0.0) or 0.0),
        ),
        reverse=True,
    )

    best = execution_priority[0] if execution_priority else None
    selected = build_standardized_alert(best, regime_payload) if best else None
    early_rejected_events = [item for item in list(early_reject_stats.get('samples', []) or []) if isinstance(item, dict)]
    all_rejected_events = [*rejected_events, *early_rejected_events]
    reject_by_reason: Dict[str, int] = {}
    reject_by_label: Dict[str, int] = {}
    reject_by_execution_grade: Dict[str, int] = {}
    for event in all_rejected_events:
        reason = str(event.get('reject_reason', '') or '')
        label = str(event.get('reject_reason_label', '') or '')
        execution_grade = str(event.get('execution_liquidity_grade', '') or '')
        if reason:
            reject_by_reason[reason] = reject_by_reason.get(reason, 0) + 1
        if label:
            reject_by_label[label] = reject_by_label.get(label, 0) + 1
        if execution_grade:
            reject_by_execution_grade[execution_grade] = reject_by_execution_grade.get(execution_grade, 0) + 1
    top_rejected = [
        {
            'symbol': event.get('symbol'),
            'side': event.get('side') or event.get('position_side'),
            'score': event.get('score'),
            'candidate_stage': event.get('candidate_stage'),
            'reject_reason': event.get('reject_reason'),
            'reject_reason_label': event.get('reject_reason_label'),
            'setup_ready': bool(event.get('setup_ready')),
            'trigger_fired': bool(event.get('trigger_fired')),
            'setup_missing': list(event.get('setup_missing', []) or []),
            'trigger_missing': list(event.get('trigger_missing', []) or []),
            'trade_missing': list(event.get('trade_missing', []) or []),
            'expected_edge': event.get('expected_edge'),
            'expected_total_fee_pct': event.get('expected_total_fee_pct'),
            'execution_slippage_buffer_pct': event.get('execution_slippage_buffer_pct'),
            'min_profit_buffer_pct': event.get('min_profit_buffer_pct'),
            'shadow_eligible': bool(event.get('setup_ready')) and bool(event.get('trigger_fired')),
            'shadow_entry_price': event.get('shadow_entry_price'),
            'planned_take_profit_price': event.get('planned_take_profit_price'),
            'planned_stop_price': event.get('planned_stop_price'),
            'planned_quantity': event.get('planned_quantity'),
            'planned_notional_usdt': event.get('planned_notional_usdt'),
            'expected_gross_profit_usdt': event.get('expected_gross_profit_usdt'),
            'expected_net_profit_usdt': event.get('expected_net_profit_usdt'),
            'expected_loss_usdt': event.get('expected_loss_usdt'),
            'expected_rr': event.get('expected_rr'),
        }
        for event in sorted(
            all_rejected_events,
            key=lambda item: float(item.get('score', 0.0) or 0.0),
            reverse=True,
        )[:8]
    ]
    nearest_misses = [
        {
            'symbol': event.get('symbol'),
            'side': event.get('side') or event.get('position_side'),
            'score': event.get('score'),
            'candidate_stage': event.get('candidate_stage'),
            'reject_reason': event.get('reject_reason'),
            'reject_reason_label': event.get('reject_reason_label'),
            'recent_5m_change_pct': event.get('recent_5m_change_pct'),
            'threshold': event.get('threshold'),
            'distance_to_pass': event.get('distance_to_pass'),
        }
        for event in sorted(
            early_rejected_events,
            key=lambda item: float(item.get('distance_to_pass', 999999.0) if item.get('distance_to_pass') is not None else 999999.0),
        )[:8]
    ]
    triggered_but_risk_rejected = [
        {
            'symbol': event.get('symbol'),
            'side': event.get('side') or event.get('position_side'),
            'score': event.get('score'),
            'candidate_stage': event.get('candidate_stage'),
            'risk_reasons': event.get('reasons', []),
            'reject_reason': event.get('reject_reason'),
            'setup_ready': bool(event.get('setup_ready')),
            'trigger_fired': bool(event.get('trigger_fired')),
            'cvd_delta': event.get('cvd_delta'),
            'cvd_zscore': event.get('cvd_zscore'),
            'oi_change_pct_5m': event.get('oi_change_pct_5m'),
            'oi_change_pct_15m': event.get('oi_change_pct_15m'),
            'expected_slippage_r': event.get('expected_slippage_r'),
            'execution_liquidity_grade': event.get('execution_liquidity_grade'),
            'entry_distance_from_breakout_pct': event.get('entry_distance_from_breakout_pct'),
            'entry_distance_from_vwap_pct': event.get('entry_distance_from_vwap_pct'),
            'overextension_flag': event.get('overextension_flag'),
        }
        for event in rejected_events
        if bool(event.get('trigger_fired')) or bool(event.get('setup_ready'))
    ]
    stage_counts: Dict[str, int] = {}
    setup_missing_counts: Dict[str, int] = {}
    trigger_missing_counts: Dict[str, int] = {}
    trade_missing_counts: Dict[str, int] = {}
    for candidate in built_candidates:
        stage_counts[candidate.candidate_stage] = stage_counts.get(candidate.candidate_stage, 0) + 1
        for reason in candidate.setup_missing:
            setup_missing_counts[reason] = setup_missing_counts.get(reason, 0) + 1
        for reason in candidate.trigger_missing:
            trigger_missing_counts[reason] = trigger_missing_counts.get(reason, 0) + 1
        for reason in candidate.trade_missing:
            trade_missing_counts[reason] = trade_missing_counts.get(reason, 0) + 1
    for event in rejected_events:
        for reason in event.get('setup_missing', []) or []:
            setup_missing_counts[str(reason)] = setup_missing_counts.get(str(reason), 0) + 1
        for reason in event.get('trigger_missing', []) or []:
            trigger_missing_counts[str(reason)] = trigger_missing_counts.get(str(reason), 0) + 1
        for reason in event.get('trade_missing', []) or []:
            trade_missing_counts[str(reason)] = trade_missing_counts.get(str(reason), 0) + 1
    diagnostic_trading_mode = bool(getattr(args, 'diagnostic_trading_mode', False))
    runtime_profile = str(getattr(args, 'profile', 'default') or 'default')
    funnel = {
        'raw_scan_symbol_count': raw_merged_symbol_count,
        'profile': runtime_profile,
        'diagnostic_trading_mode': diagnostic_trading_mode,
        'evaluated_symbol_count': len(evaluated_symbols),
        'evaluated_side_count': evaluated_side_count,
        'ticker_24hr_cache_available': bool(ticker_cache_diagnostics.get('ticker_24hr_cache_available')),
        'ticker_24hr_cache_age_seconds': ticker_cache_diagnostics.get('ticker_24hr_cache_age_seconds'),
        'ticker_24hr_cache_row_count': int(ticker_cache_diagnostics.get('ticker_24hr_cache_row_count') or 0),
        'scanner_rest_fallback_used': bool(ticker_cache_diagnostics.get('scanner_rest_fallback_used')),
        'scanner_rest_fallback_skipped_reason': str(ticker_cache_diagnostics.get('scanner_rest_fallback_skipped_reason') or ''),
        'rest_used_weight_1m': int(ticker_cache_diagnostics.get('rest_used_weight_1m') or 0),
        'rest_circuit_state': str(ticker_cache_diagnostics.get('rest_circuit_state') or 'CLOSED'),
        'ticker_24hr_cache_refresher_active': bool(ticker_cache_diagnostics.get('ticker_24hr_cache_refresher_active')),
        'ticker_24hr_cache_refresher_skipped_reason': str(ticker_cache_diagnostics.get('ticker_24hr_cache_refresher_skipped_reason') or ''),
        'ticker_24hr_cache_refresher_lock_acquired': bool(ticker_cache_diagnostics.get('ticker_24hr_cache_refresher_lock_acquired')),
        'ticker_24hr_cache_refresher_singleton_scope': str(ticker_cache_diagnostics.get('ticker_24hr_cache_refresher_singleton_scope') or ''),
        'scanner_cache_only_mode': bool(ticker_cache_diagnostics.get('scanner_cache_only_mode')),
        'scanner_patch_fallback_disabled': bool(ticker_cache_diagnostics.get('scanner_patch_fallback_disabled')),
        'scanner_patch_fallback_skipped_reason': str(ticker_cache_diagnostics.get('scanner_patch_fallback_skipped_reason') or ''),
        'scanner_rest_fallback_allowed': bool(ticker_cache_diagnostics.get('scanner_rest_fallback_allowed')),
        'scanner_rest_fallback_blocked_by_weight': bool(ticker_cache_diagnostics.get('scanner_rest_fallback_blocked_by_weight')),
        'symbols_skipped_due_to_missing_ticker_24hr': int(ticker_cache_diagnostics.get('symbols_skipped_due_to_missing_ticker_24hr') or 0),
        'okx_available_inst_count': okx_available_inst_count,
        'okx_unavailable_symbol_count': len(okx_unavailable_symbols),
        'okx_unavailable_symbols_sample': okx_unavailable_symbols[:12],
        'early_filter_passed_count': len(built_candidates),
        'candidate_pool_count': len(candidates),
        'setup_ready_count': sum(1 for item in built_candidates if bool(item.setup_ready)),
        'trigger_fired_count': sum(1 for item in built_candidates if bool(item.trigger_fired)),
        'trigger_relax_eligible_count': len(relaxed_candidates),
        'hard_rejected_count': len(rejected_events),
        'early_rejected_count': int(early_reject_stats.get('total') or len(early_rejected_events)),
        'stage_counts': stage_counts,
        'top_setup_missing': dict(sorted(setup_missing_counts.items(), key=lambda item: item[1], reverse=True)[:8]),
        'top_trigger_missing': dict(sorted(trigger_missing_counts.items(), key=lambda item: item[1], reverse=True)[:8]),
        'top_trade_missing': dict(sorted(trade_missing_counts.items(), key=lambda item: item[1], reverse=True)[:8]),
        'scan_budget_diagnostics': scan_budget_diagnostics,
    }
    blocked_tradeability = build_blocked_tradeability_rows(rejected_events)
    micro_scalp_diagnostics = {}
    if runtime_profile == 'five-usdt-scalp-v2':
        micro_scalp_diagnostics = build_micro_scalp_cycle_diagnostics(
            early_filter_pass_count=len(built_candidates),
            early_filter_reject_reasons=dict(early_reject_stats.get('by_reason', {}) or {}),
            candidate_alerts=candidate_alerts,
            blocked_reasons={
                'liquidity_failed': reject_by_label.get('execution_depth', 0),
                'spread_failed': reject_by_label.get('execution_spread', 0),
                'score_too_low': reject_by_reason.get('score_below_threshold', 0),
                'trigger_missing': sum(trigger_missing_counts.values()),
                'risk_blocked': reject_by_label.get('risk_guard', 0),
                'execution_blocked': reject_by_label.get('execution_slippage', 0),
                'watchlist_count': len(persisted_watchlist),
                'rest_weight': int(ticker_cache_diagnostics.get('rest_used_weight_1m') or 0),
            },
        )
    summary_counters = {
        'raw_scan_symbol_count': funnel['raw_scan_symbol_count'],
        'profile': runtime_profile,
        'diagnostic_trading_mode': diagnostic_trading_mode,
        'evaluated_symbol_count': funnel['evaluated_symbol_count'],
        'evaluated_side_count': funnel['evaluated_side_count'],
        'early_filter_passed_count': funnel['early_filter_passed_count'],
        'setup_ready_count': funnel['setup_ready_count'],
        'trigger_fired_count': funnel['trigger_fired_count'],
        'trigger_relax_eligible_count': funnel.get('trigger_relax_eligible_count', 0),
        'candidate_pool_count': funnel['candidate_pool_count'],
        'selected_count': 1 if selected else 0,
        'hard_rejected_count': funnel['hard_rejected_count'],
        'early_rejected_total': funnel.get('early_rejected_count', 0),
        'rejected_total': len(all_rejected_events),
    }
    payload = {
        'ok': True,
        'degraded': bool(ticker_cache_diagnostics.get('degraded')),
        'degraded_reason': ticker_cache_diagnostics.get('degraded_reason') if ticker_cache_diagnostics.get('degraded') else '',
        'ticker_24hr_cache_available': bool(ticker_cache_diagnostics.get('ticker_24hr_cache_available')),
        'ticker_24hr_cache_age_seconds': ticker_cache_diagnostics.get('ticker_24hr_cache_age_seconds'),
        'ticker_24hr_cache_row_count': int(ticker_cache_diagnostics.get('ticker_24hr_cache_row_count') or 0),
        'scanner_rest_fallback_used': bool(ticker_cache_diagnostics.get('scanner_rest_fallback_used')),
        'scanner_rest_fallback_skipped_reason': str(ticker_cache_diagnostics.get('scanner_rest_fallback_skipped_reason') or ''),
        'rest_used_weight_1m': int(ticker_cache_diagnostics.get('rest_used_weight_1m') or 0),
        'rest_circuit_state': str(ticker_cache_diagnostics.get('rest_circuit_state') or 'CLOSED'),
        'ticker_24hr_cache_refresher_active': bool(ticker_cache_diagnostics.get('ticker_24hr_cache_refresher_active')),
        'ticker_24hr_cache_refresher_skipped_reason': str(ticker_cache_diagnostics.get('ticker_24hr_cache_refresher_skipped_reason') or ''),
        'ticker_24hr_cache_refresher_lock_acquired': bool(ticker_cache_diagnostics.get('ticker_24hr_cache_refresher_lock_acquired')),
        'ticker_24hr_cache_refresher_singleton_scope': str(ticker_cache_diagnostics.get('ticker_24hr_cache_refresher_singleton_scope') or ''),
        'scanner_cache_only_mode': bool(ticker_cache_diagnostics.get('scanner_cache_only_mode')),
        'scanner_patch_fallback_disabled': bool(ticker_cache_diagnostics.get('scanner_patch_fallback_disabled')),
        'scanner_patch_fallback_skipped_reason': str(ticker_cache_diagnostics.get('scanner_patch_fallback_skipped_reason') or ''),
        'scanner_rest_fallback_allowed': bool(ticker_cache_diagnostics.get('scanner_rest_fallback_allowed')),
        'scanner_rest_fallback_blocked_by_weight': bool(ticker_cache_diagnostics.get('scanner_rest_fallback_blocked_by_weight')),
        'symbols_skipped_due_to_missing_ticker_24hr': int(ticker_cache_diagnostics.get('symbols_skipped_due_to_missing_ticker_24hr') or 0),
        'candidate_count': len(candidates),
        'candidates': candidate_alerts,
        'candidate_alerts': candidate_alerts,
        'selected': selected,
        'selected_alert': selected,
        'market_regime': regime_payload,
        'rejected_stats': {
            'total': len(all_rejected_events),
            'hard_rejected_total': len(rejected_events),
            'early_rejected_total': funnel.get('early_rejected_count', 0),
            'by_reason': reject_by_reason,
            'by_reject_label': reject_by_label,
            'by_execution_liquidity_grade': reject_by_execution_grade,
            'top_rejected': top_rejected,
            'nearest_misses': nearest_misses,
            'triggered_but_risk_rejected': triggered_but_risk_rejected,
        },
        'blocked_tradeability': blocked_tradeability,
        'watchlist_count': len(persisted_watchlist),
        'watchlist_top': persisted_watchlist[:10],
        'micro_scalp_diagnostics': micro_scalp_diagnostics,
        'early_rejected_stats': early_reject_stats,
        'funnel': funnel,
        'summary_counters': summary_counters,
    }
    try:
        # Mature old observations before adding this cycle's samples.
        evaluate_economic_shadow_candidates(store, client, args=args)
        shadow_rows = persist_economic_shadow_candidates(store, top_rejected)
        payload['economic_shadow_candidate_count'] = len(shadow_rows)
    except Exception as exc:
        payload['economic_shadow_persist_error'] = f'{type(exc).__name__}: {exc}'
    return payload, best, metas


def _coerce_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_tradeability_score(event: Dict[str, Any]) -> Optional[float]:
    slippage_r = _coerce_optional_float(event.get('expected_slippage_r'))
    depth = _coerce_optional_float(event.get('book_depth_fill_ratio'))
    if slippage_r is not None:
        return round(max(0.0, min(100.0, 100.0 - slippage_r * 100.0)), 1)
    if depth is not None:
        return round(max(0.0, min(100.0, depth * 100.0)), 1)
    return None


def build_blocked_tradeability_rows(rejected_events: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tradeability_block_labels = {'execution_slippage', 'execution_depth'}
    blocked_tradeability: List[Dict[str, Any]] = []
    for event in rejected_events:
        if not isinstance(event, dict):
            continue
        label = str(event.get('reject_reason_label', '') or '')
        if label not in tradeability_block_labels:
            continue
        slippage_r = _coerce_optional_float(event.get('expected_slippage_r'))
        grade = event.get('execution_liquidity_grade', '')
        tradeability_score = compute_tradeability_score(event)
        blocked_reasons = []
        if slippage_r is not None:
            blocked_reasons.append(f'slippage_r={slippage_r:g}')
        if grade:
            blocked_reasons.append(f'liquidity_grade={grade}')
        spread = _coerce_optional_float(event.get('spread_bps'))
        if spread is not None:
            blocked_reasons.append(f'spread_bps={spread:g}')
        depth = _coerce_optional_float(event.get('book_depth_fill_ratio'))
        if depth is not None:
            blocked_reasons.append(f'depth_fill_ratio={depth:g}')
        blocked_tradeability.append({
            'symbol': event.get('symbol'),
            'side': event.get('side') or event.get('position_side'),
            'reject_label': label,
            'tradeability_score': tradeability_score,
            'blocked_reasons': blocked_reasons,
        })
    return sorted(
        blocked_tradeability,
        key=lambda item: (
            item['tradeability_score'] is None,
            item['tradeability_score'] if item['tradeability_score'] is not None else 101.0,
            str(item.get('symbol') or ''),
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', default='')
    parser.add_argument('--square-symbols', default='')
    parser.add_argument('--square-symbols-file', default='')
    parser.add_argument('--use-square-page', action='store_true')
    parser.add_argument('--top-gainers', type=int, default=20)
    parser.add_argument('--top-losers', type=int, default=20)
    parser.add_argument('--max-candidates', type=int, default=8)
    parser.add_argument('--scan-prefilter-multiplier', type=int, default=2, help='Prefilter expensive per-symbol scanner fetches to max_candidates * multiplier.')
    parser.add_argument('--lookback-bars', type=int, default=12)
    parser.add_argument('--swing-bars', type=int, default=6)
    parser.add_argument('--risk-usdt', type=float, default=10.0)
    parser.add_argument('--target-net-profit-usdt', type=float, default=0.0)
    parser.add_argument('--min-target-net-profit-usdt', type=float, default=0.0)
    parser.add_argument('--max-loss-usdt', type=float, default=0.0)
    parser.add_argument('--min-expected-rr', type=float, default=0.0)
    parser.add_argument('--prefer-maker', action='store_true')
    parser.add_argument('--allow-post-only-taker-fallback', action='store_true', help='Permit a tightly gated taker entry after Binance rejects a GTX order with -5022.')
    parser.add_argument('--maker-post-only-fallback-min-score', type=float, default=85.0)
    parser.add_argument('--maker-post-only-fallback-min-liquidity-grade', choices=['A', 'B'], default='B')
    parser.add_argument('--maker-post-only-fallback-max-spread-bps', type=float, default=8.0)
    parser.add_argument('--taker-fee-worst-case', action='store_true')
    parser.add_argument('--disable-tiny-tp', action='store_true')
    parser.add_argument('--daily-trade-limit', type=int, default=0)
    parser.add_argument('--cooldown-after-loss-minutes', type=int, default=0)
    parser.add_argument('--pause-after-consecutive-losses-hours', type=int, default=0)
    parser.add_argument('--max-notional-usdt', type=float, default=0.0)
    parser.add_argument('--min-notional-usdt', type=float, default=0.0)
    parser.add_argument('--min-5m-change-pct', type=float, default=2.0)
    parser.add_argument('--base-acceleration-ratio', type=float, default=1.25, help='Base acceleration ratio for regime entry thresholds.')
    parser.add_argument('--min-quote-volume', type=float, default=50_000_000.0)
    parser.add_argument('--stop-buffer-pct', type=float, default=0.01)
    parser.add_argument('--max-rsi-5m', type=float, default=80.0)
    parser.add_argument('--min-volume-multiple', type=float, default=1.8)
    parser.add_argument('--max-distance-from-ema-pct', type=float, default=10.0)
    parser.add_argument('--max-distance-from-vwap-pct', type=float, default=10.0)
    parser.add_argument('--watch-breakout-tolerance-pct', type=float, default=0.0, help='Allow near-breakout watch candidates within this percent into candidate scoring.')
    parser.add_argument('--setup-breakout-tolerance-pct', type=float, default=0.0, help='Treat near-breakout candidates within this percent as eligible for setup readiness; execution still requires trigger confirmation.')
    parser.add_argument('--oi-hard-reversal-threshold-pct', type=float, default=0.8, help='Directional 5m OI reversal threshold that remains a hard veto.')
    parser.add_argument('--extended-chase-threshold-pct', type=float, default=15.0, help='24h change threshold above which chase/momentum_extension/overheated states are vetoed.')
    parser.add_argument('--sim-probe-entry-enabled', action='store_true', help='Allow OKX simulated trading to submit a small probe when setup is ready but full trigger has not fired.')
    parser.add_argument('--trigger-relax-mode', action='store_true', help='Allow high-score setup candidates into pre-trigger maker watch when breakout/OI/taker/CVD score is strong.')
    parser.add_argument('--trigger-relax-min-score', type=float, default=70.0)
    parser.add_argument('--trigger-relax-min-points', type=int, default=3)
    parser.add_argument('--sim-probe-size-ratio', type=float, default=0.2)
    parser.add_argument('--sim-probe-min-score', type=float, default=62.0)
    parser.add_argument('--sim-probe-max-breakout-distance-pct', type=float, default=0.35)
    parser.add_argument('--execution-slippage-hard-veto-r', type=float, default=0.25)
    parser.add_argument('--execution-slippage-risk-threshold-r', type=float, default=0.15)
    parser.add_argument('--trigger-min-confirmations', type=int, default=2, help='Minimum trigger confirmations required after setup readiness.')
    parser.add_argument('--max-funding-rate', type=float, default=0.0005)
    parser.add_argument('--max-funding-rate-avg', type=float, default=0.0003)
    parser.add_argument('--leverage', type=int, default=5)
    parser.add_argument('--margin-type', choices=['ISOLATED', 'CROSSED', 'isolated', 'crossed'], default='ISOLATED')
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--diagnostic-trading-mode', action='store_true', help='Run live scanner/risk diagnostics while preventing live order execution.')
    parser.add_argument('--scan-only', action='store_true')
    parser.add_argument('--profile', default='five-usdt-scalp-v2')
    parser.add_argument('--allowed-trade-sides', default='long,short')
    parser.add_argument('--tp1-r', type=float, default=1.5)
    parser.add_argument('--tp1-close-pct', type=float, default=0.3)
    parser.add_argument('--tp1-profit-usdt', type=float, default=0.0)
    parser.add_argument('--tp2-r', type=float, default=2.0)
    parser.add_argument('--tp2-close-pct', type=float, default=0.4)
    parser.add_argument('--tp2-profit-usdt', type=float, default=0.0)
    parser.add_argument('--tp3-r', type=float, default=0.0)
    parser.add_argument('--tp3-close-pct', type=float, default=0.0)
    parser.add_argument('--micro-scalp-time-stop-sec', type=int, default=0, help='Close stale micro-scalp positions after this many seconds when the minimum profit R condition is met.')
    parser.add_argument('--micro-scalp-min-profit-r', type=float, default=0.0, help='Minimum realized R required for the micro-scalp time stop action.')
    parser.add_argument('--target-notional-usdt', type=float, default=0.0)
    parser.add_argument('--probe-min-notional-usdt', type=float, default=20.0)
    parser.add_argument('--probe-max-notional-usdt', type=float, default=30.0)
    parser.add_argument('--consecutive-loss-pause-minutes', type=int, default=0)
    parser.add_argument('--breakeven-r', type=float, default=1.0)
    parser.add_argument('--breakeven-confirmation-mode', choices=['price_only', 'ema_support'], default='ema_support')
    parser.add_argument('--breakeven-min-buffer-pct', type=float, default=0.001)
    parser.add_argument('--trailing-buffer-pct', type=float, default=0.02)
    parser.add_argument('--auto-loop', action='store_true')
    parser.add_argument('--poll-interval-sec', type=int, default=60)
    parser.add_argument('--monitor-poll-interval-sec', type=int, default=15)
    parser.add_argument('--max-monitor-cycles', type=int, default=20, help='Maximum foreground trade-management monitor cycles. Set 0 for unlimited monitoring.')
    parser.add_argument('--user-stream-refresh-interval-minutes', type=float, default=30.0, help='Refresh listen key after this many minutes.')
    parser.add_argument('--user-stream-disconnect-timeout-minutes', type=float, default=65.0, help='Mark user data stream disconnected when refresh heartbeat is older than this many minutes.')
    parser.add_argument('--base-url', default=os.getenv('BINANCE_FUTURES_BASE_URL', 'https://fapi.binance.com'))
    parser.add_argument('--scanner-proxy-urls', default=os.getenv('BINANCE_SCANNER_PROXY_URLS', ''), help='Comma or whitespace separated proxy URLs used only by public scanner REST fetches; supports socks5://host:port:user:pass and socks5://user:pass@host:port.')
    parser.add_argument('--okx-sentiment-inline', default='')
    parser.add_argument('--okx-sentiment-file', default='')
    parser.add_argument('--okx-sentiment-command', default='')
    parser.add_argument('--okx-auto', action='store_true')
    parser.add_argument('--okx-mcp-command', default='')
    parser.add_argument('--okx-sentiment-timeout', type=int, default=20)
    parser.add_argument('--binance-simulated-trading', action='store_true', help='Use Binance USDT-M Futures Testnet / Mock Trading instead of Binance production futures.')
    parser.add_argument('--external-signal-json', default='')
    parser.add_argument('--use-external-setup-relaxation', action='store_true', help='Let strong external accumulation signals relax early scan gates; live entries still require setup/trigger risk guards.')
    parser.add_argument('--reconcile-only', action='store_true', help='Only reconcile runtime state with exchange positions/orders, then exit.')
    parser.add_argument('--runtime-state-dir', default=os.path.expanduser('~/.hermes/binance-futures-momentum-long/runtime-state'), help='Directory for runtime state JSON/JSONL files.')
    parser.add_argument('--halt-on-orphan-position', action='store_true', help='Halt strategy if reconcile finds exchange positions not tracked locally.')
    parser.add_argument('--repair-missing-protection', dest='repair_missing_protection', action='store_true', default=True, help='Auto place replacement STOP_MARKET orders for tracked live positions missing protection during reconcile.')
    parser.add_argument('--no-repair-missing-protection', dest='repair_missing_protection', action='store_false', help='Report missing protection and halt instead of auto repairing during reconcile.')
    parser.add_argument('--max-scan-cycles', type=int, default=1, help='Maximum scan cycles when auto-loop is enabled. Set 0 for infinite loop.')
    parser.add_argument('--scanner-timeout-seconds', type=float, default=45.0, help='Deadman timeout for one scanner cycle.')
    parser.add_argument('--execution-timeout-seconds', type=float, default=30.0, help='Deadman timeout for live order execution.')
    parser.add_argument('--scanner-rest-ban-cooldown-seconds', type=float, default=180.0, help='Fallback REST scanner circuit cooldown when Binance 418/-1003 does not include a ban-until timestamp.')
    parser.add_argument('--position-order-reconcile-interval-seconds', type=float, default=60.0, help='Seconds between background position/order reconciliation checks.')
    parser.add_argument('--scanner-rest-fallback', action='store_true', help='Compatibility master switch for scanner REST fallback.')
    parser.add_argument('--scanner-ticker-rest-fallback', action='store_true', default=True, help='Allow ticker 24h REST fallback when cache is empty/stale.')
    parser.add_argument('--scanner-kline-rest-fallback', action='store_true', default=False, help='Allow kline REST fallback when cache is empty/stale.')
    parser.add_argument('--scanner-depth-rest-fallback', action='store_true', default=False, help='Allow depth REST fallback when cache is empty/stale.')
    parser.add_argument('--scanner-rest-fallback-min-interval-seconds', type=float, default=180.0)
    parser.add_argument('--scanner-kline-rest-fallback-min-interval-seconds', type=float, default=300.0)
    parser.add_argument('--scanner-depth-rest-fallback-min-interval-seconds', type=float, default=300.0)
    parser.add_argument('--scanner-rest-fallback-max-used-weight-1m', type=int, default=900)
    parser.add_argument('--scanner-market-data-rest-fallback-max-used-weight-1m', type=int, default=700)
    parser.add_argument('--allow-heavy-history-rest', action='store_true', default=False)
    parser.add_argument('--ticker-24hr-cache-refresh-seconds', type=float, default=120.0)
    parser.add_argument('--ticker-24hr-cache-max-age-seconds', type=float, default=300.0)
    parser.add_argument('--ticker-24hr-cache-min-rows', type=int, default=100)
    parser.add_argument('--scanner-ticker-cache-max-age-seconds', type=float, default=10.0)
    parser.add_argument('--scanner-kline-cache-max-age-seconds', type=float, default=120.0)
    parser.add_argument('--scanner-order-book-cache-max-age-seconds', type=float, default=3.0)
    parser.add_argument('--scan-microstructure-symbol-limit', type=int, default=10, help='Max symbols eligible for expensive microstructure fetches per scan.')
    parser.add_argument('--scan-funding-symbol-limit', type=int, default=8, help='Max symbols eligible for funding-rate REST fetches per scan.')
    parser.add_argument('--scan-open-interest-symbol-limit', type=int, default=8, help='Max symbols eligible for open-interest REST fetches per scan.')
    parser.add_argument('--scan-cvd-symbol-limit', type=int, default=8, help='Max symbols eligible for CVD/orderflow REST-derived fetches per scan.')
    parser.add_argument('--scan-watchlist-symbols', default='', help='Comma-separated symbols promoted to the front of the expensive scan budget.')
    parser.add_argument('--runtime-ttl-seconds', type=float, default=900.0, help='TTL for per-symbol runtime cache cleanup.')
    parser.add_argument('--runtime-queue-maxsize', type=int, default=128, help='Max queue depth for scanner/execution/manager isolation queues.')
    parser.add_argument('--supervisor-restart-limit', type=int, default=3, help='Consecutive auto-loop cycle failures before supervisor halts.')
    parser.add_argument('--max-open-positions', type=int, default=1, help='Maximum concurrent open positions allowed.')
    parser.add_argument('--max-long-positions', type=int, default=0, help='Maximum concurrent long positions allowed (0 disables).')
    parser.add_argument('--max-short-positions', type=int, default=0, help='Maximum concurrent short positions allowed (0 disables).')
    parser.add_argument('--max-net-exposure-usdt', type=float, default=0.0, help='Maximum absolute projected net exposure in USDT (0 disables).')
    parser.add_argument('--max-gross-exposure-usdt', type=float, default=0.0, help='Maximum projected gross exposure in USDT (0 disables).')
    parser.add_argument('--per-symbol-single-side-only', dest='per_symbol_single_side_only', action='store_true', default=True, help='Allow only one active side per symbol.')
    parser.add_argument('--allow-symbol-hedge', dest='per_symbol_single_side_only', action='store_false', help='Allow both long and short positions on the same symbol.')
    parser.add_argument('--opposite-side-flip-cooldown-minutes', type=int, default=0, help='Block opposite-side entry on a symbol while cooldown is active (phase-1 placeholder).')
    parser.add_argument('--daily-max-loss-usdt', type=float, default=0.0, help='Block new trades after realized daily loss reaches this USDT threshold (0 disables).')
    parser.add_argument('--max-consecutive-losses', type=int, default=0, help='Block new trades after this many consecutive losses (0 disables).')
    parser.add_argument('--symbol-cooldown-minutes', type=int, default=0, help='Per-symbol cooldown after a loss or stop-out (0 disables).')
    parser.add_argument('--symbol-daily-loss-limit-usdt', type=float, default=1.2, help='Block new entries for a symbol after its daily realized loss reaches this USDT amount.')
    parser.add_argument('--symbol-rolling-loss-limit-usdt', type=float, default=2.0, help='Block new entries for a symbol after rolling-window realized loss reaches this USDT amount.')
    parser.add_argument('--symbol-rolling-window-days', type=int, default=5, help='Rolling window in days used for per-symbol loss review.')
    parser.add_argument('--max-consecutive-losses-per-symbol', type=int, default=1, help='Block new entries after this many consecutive losses on the same symbol.')
    parser.add_argument('--after-symbol-loss-cooldown-minutes', type=int, default=180, help='Cooldown minutes after a symbol loss-limit or losing close.')
    parser.add_argument('--loss-deweighted-score-penalty', type=float, default=12.0, help='Extra score points required for recently losing symbols after cooldown.')
    parser.add_argument('--gross-heat-cap-r', type=float, default=0.0, help='Maximum total portfolio heat in R units (0 disables).')
    parser.add_argument('--same-theme-heat-cap-r', type=float, default=0.0, help='Maximum heat in R units for the same narrative/theme bucket (0 disables).')
    parser.add_argument('--same-correlation-heat-cap-r', type=float, default=0.0, help='Maximum heat in R units for the same correlation bucket (0 disables).')
    parser.add_argument('--disable-notify', action='store_true', help='Disable outbound trade notifications.')
    parser.add_argument('--notify-target', default='', help='Comma-separated notification targets like telegram:-100123:77,weixin:chatid.')
    parser.add_argument('--telegram-bot-token-env', default='TELEGRAM_BOT_TOKEN', help='Env var name containing Telegram bot token for notifications.')
    parser.add_argument('--output-format', choices=['json', 'cn'], default='cn', help='Output as raw JSON or concise Chinese summary.')
    parser.add_argument('--enable-trade-memory', action='store_true', help='Use closed-trade memory to adapt risk and candidate scores.')
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = build_parser()
    argv = list(sys.argv[1:] if argv is None else argv)
    args = parser.parse_args(argv)
    option_map = {opt: action.dest for action in parser._actions for opt in action.option_strings}
    args._explicit_cli_dests = {option_map[token] for token in argv if token in option_map}
    return args


def apply_runtime_profile(args: argparse.Namespace) -> argparse.Namespace:
    explicit = set(getattr(args, '_explicit_cli_dests', set()) or set())
    defaults = {
        'runtime_state_dir': os.path.expanduser('~/.hermes/binance-futures-momentum-long/runtime-state'),
        'daily_max_loss_usdt': 0.0,
        'max_consecutive_losses': 0,
        'symbol_cooldown_minutes': 0,
        'max_open_positions': 1,
        'max_long_positions': 0,
        'max_short_positions': 0,
        'max_net_exposure_usdt': 0.0,
        'max_gross_exposure_usdt': 0.0,
        'margin_type': 'ISOLATED',
        'breakeven_confirmation_mode': 'ema_support',
        'breakeven_min_buffer_pct': 0.001,
        'per_symbol_single_side_only': True,
        'opposite_side_flip_cooldown_minutes': 0,
        'gross_heat_cap_r': 0.0,
        'same_theme_heat_cap_r': 0.0,
        'same_correlation_heat_cap_r': 0.0,
        'notify_target': '',
        'disable_notify': False,
        'telegram_bot_token_env': 'TELEGRAM_BOT_TOKEN',
        'reconcile_only': False,
        'halt_on_orphan_position': False,
        'repair_missing_protection': True,
        'scanner_timeout_seconds': 45.0,
        'execution_timeout_seconds': 30.0,
        'scanner_rest_ban_cooldown_seconds': 180.0,
        'scanner_rest_fallback_min_interval_seconds': 180.0,
        'scanner_rest_fallback_max_used_weight_1m': 900,
        'scanner_market_data_rest_fallback_max_used_weight_1m': 700,
        'scanner_kline_rest_fallback_min_interval_seconds': 300.0,
        'scanner_depth_rest_fallback_min_interval_seconds': 300.0,
        'scanner_ticker_rest_fallback': True,
        'scanner_kline_rest_fallback': False,
        'scanner_depth_rest_fallback': False,
        'allow_heavy_history_rest': False,
        'ticker_24hr_cache_refresh_seconds': 120.0,
        'ticker_24hr_cache_max_age_seconds': 300.0,
        'ticker_24hr_cache_min_rows': 100,
        'runtime_ttl_seconds': 900.0,
        'runtime_queue_maxsize': 128,
        'supervisor_restart_limit': 3,
        'output_format': 'cn',
        'scanner_proxy_urls': os.getenv('BINANCE_SCANNER_PROXY_URLS', ''),
        'allowed_trade_sides': 'long,short',
        'enable_symbol_quality_tier': False,
        'enable_market_regime_gate': False,
        'enable_direction_lock': False,
        'enable_fee_aware_edge_filter': False,
        'diagnostic_trading_mode': False,
        'adaptive_read_only': False,
        'disable_adaptive_risk_upscale': False,
        'atr_stop_multiplier': 1.5,
        'five_usdt_watchlist_scoring': False,
        'micro_scalp_time_stop_sec': 0,
        'micro_scalp_min_profit_r': 0.0,
    }
    for key, value in defaults.items():
        if not hasattr(args, key):
            setattr(args, key, value)
    profile = getattr(args, 'profile', 'default')
    valid_profiles = {
        'default',
        '10u-aggressive',
        '10u-aggressive-v2',
        '10u-active',
        '10u-diagnostic-live',
        'binance-sim-active',
        'high_vol_alt_mode',
        'aggressive-fee-aware-scalp-long-short',
        'aggressive-fee-aware-scalp-long-only',
        'aggressive-fee-aware-scalp-short-only',
        'five-usdt-scalp-v2',
    }
    if profile not in valid_profiles:
        raise ValueError(f'Unknown profile: {profile}')
    profile_overrides: Dict[str, Any] = {}
    if profile == '10u-aggressive':
        profile_overrides = {
            'risk_usdt': 1.2,
            'max_notional_usdt': 500.0,
            'leverage': 5,
            'probe_max_leverage': 5,
            'breakeven_r': 0.8,
            'tp1_r': 5.0,
            'tp1_close_pct': 0.5,
            'tp2_r': 10.0,
            'tp2_close_pct': 0.5,
            'entry_tp1_offset_abs': 5.0,
            'entry_tp2_offset_abs': 10.0,
            'lookback_bars': 4,
            'swing_bars': 4,
            'min_quote_volume': 3_000_000,
            'top_gainers': 45,
            'top_losers': 45,
            'max_candidates': 24,
            'max_open_positions': 5,
            'max_long_positions': 5,
            'max_short_positions': 5,
            'max_rsi_5m': 84.0,
            'min_volume_multiple': 0.5,
            'min_5m_change_pct': 0.2,
            'watch_breakout_tolerance_pct': 1.2,
            'setup_breakout_tolerance_pct': 0.8,
            'oi_hard_reversal_threshold_pct': 1.2,
            'extended_chase_threshold_pct': 22.0,
            'sim_probe_entry_enabled': True,
            'sim_probe_size_ratio': 0.3,
            'sim_probe_min_score': 58.0,
            'sim_probe_max_breakout_distance_pct': 0.6,
            'trigger_min_confirmations': 1,
            'max_distance_from_ema_pct': 9.0,
            'max_distance_from_vwap_pct': 8.0,
            'max_funding_rate': 0.0008,
            'max_funding_rate_avg': 0.0005,
        }
    elif profile == '10u-aggressive-v2':
        profile_overrides = {
            'risk_usdt': 1.2,
            'max_notional_usdt': 500.0,
            'leverage': 5,
            'probe_max_leverage': 5,
            'breakeven_r': 0.8,
            'tp1_r': 5.0,
            'tp1_close_pct': 0.5,
            'tp2_r': 10.0,
            'tp2_close_pct': 0.5,
            'entry_tp1_offset_abs': 5.0,
            'entry_tp2_offset_abs': 10.0,
            'lookback_bars': 4,
            'swing_bars': 4,
            'min_quote_volume': 3_000_000,
            'top_gainers': 45,
            'top_losers': 45,
            'max_candidates': 24,
            'max_open_positions': 5,
            'max_long_positions': 5,
            'max_short_positions': 5,
            'max_rsi_5m': 88.0,
            'min_volume_multiple': 0.35,
            'min_5m_change_pct': 0.15,
            'watch_breakout_tolerance_pct': 1.5,
            'setup_breakout_tolerance_pct': 1.0,
            'oi_hard_reversal_threshold_pct': 1.5,
            'extended_chase_threshold_pct': 28.0,
            'execution_slippage_hard_veto_r': 300.0,
            'execution_slippage_risk_threshold_r': 300.0,
            'sim_probe_entry_enabled': True,
            'sim_probe_size_ratio': 0.3,
            'sim_probe_min_score': 55.0,
            'sim_probe_max_breakout_distance_pct': 0.9,
            'trigger_min_confirmations': 1,
            'max_distance_from_ema_pct': 12.0,
            'max_distance_from_vwap_pct': 10.0,
            'max_funding_rate': 0.0008,
            'max_funding_rate_avg': 0.0005,
            'enable_symbol_quality_tier': True,
            'enable_market_regime_gate': True,
            'enable_direction_lock': True,
            'enable_fee_aware_edge_filter': True,
            'atr_stop_multiplier': 1.5,
            'allowed_trade_sides': 'long,short',
        }
    elif profile == 'five-usdt-scalp-v2':
        profile_overrides = {
            'risk_usdt': 1.5,
            'target_net_profit_usdt': 1.2,
            'min_target_net_profit_usdt': 0.6,
            'min_notional_usdt': 60.0,
            'max_notional_usdt': 90.0,
            'target_notional_usdt': 80.0,
            'probe_min_notional_usdt': 20.0,
            'probe_max_notional_usdt': 30.0,
            'leverage': 10,
            'probe_max_leverage': 5,
            'max_open_positions': 1,
            'max_long_positions': 1,
            'max_short_positions': 1,
            'symbol_cooldown_minutes': 5,
            'opposite_side_flip_cooldown_minutes': 15,
            'daily_max_loss_usdt': 10.0,
            'max_consecutive_losses': 2,
            'consecutive_loss_pause_minutes': 120,
            'daily_circuit_breaker': True,
            'take_profit_pct': 0.012,
            'stop_loss_pct': 0.01,
            'tp1_close_pct': 0.4,
            'tp2_r': 2.5,
            'tp2_close_pct': 0.4,
            'tp3_r': 4.0,
            'tp3_close_pct': 0.2,
            'breakeven_after_pct': 0.01,
            'trailing_after_pct': 0.018,
            'max_holding_minutes': 45,
            'timeout_exit_enabled': True,
            'micro_scalp_time_stop_sec': 2700,
            'micro_scalp_min_profit_r': 0.0,
            'min_score': 55.0,
            'sim_probe_min_score': 55.0,
            'trigger_min_confirmations': 1,
            'min_quote_volume': 3_000_000,
            'min_5m_change_pct': 0.08,
            'min_volume_multiple': 0.4,
            'top_gainers': 25,
            'top_losers': 25,
            'max_candidates': 10,
            'scan_prefilter_multiplier': 3,
            'scan_microstructure_symbol_limit': 10,
            'scan_funding_symbol_limit': 6,
            'scan_open_interest_symbol_limit': 6,
            'scan_cvd_symbol_limit': 6,
            'scan_watchlist_symbols': 'BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,1000PEPEUSDT,1000BONKUSDT',
            'max_distance_from_ema_pct': 1.8,
            'max_distance_from_vwap_pct': 1.8,
            'watch_breakout_tolerance_pct': 3.0,
            'setup_breakout_tolerance_pct': 3.0,
            'enable_symbol_quality_tier': True,
            'enable_fee_aware_edge_filter': True,
            'execution_preflight_enabled': True,
            'repair_missing_protection': True,
            'trigger_relax_mode': True,
            'allowed_trade_sides': 'long,short',
        }
    elif profile == '10u-active':
        profile_overrides = {
            'risk_usdt': 1.0,
            'max_notional_usdt': 500.0,
            'leverage': 4,
            'breakeven_r': 0.8,
            'tp1_r': 1.2,
            'tp1_close_pct': 0.5,
            'tp2_r': 1.8,
            'tp2_close_pct': 0.3,
            'lookback_bars': 4,
            'swing_bars': 4,
            'min_quote_volume': 5_000_000,
            'top_gainers': 35,
            'top_losers': 35,
            'max_candidates': 12,
            'max_rsi_5m': 82.0,
            'min_volume_multiple': 0.9,
            'min_5m_change_pct': 0.5,
            'watch_breakout_tolerance_pct': 0.8,
            'setup_breakout_tolerance_pct': 0.35,
            'oi_hard_reversal_threshold_pct': 1.0,
            'extended_chase_threshold_pct': 18.0,
            'execution_slippage_hard_veto_r': 0.4,
            'execution_slippage_risk_threshold_r': 0.25,
            'trigger_min_confirmations': 1,
            'max_distance_from_ema_pct': 8.0,
            'max_distance_from_vwap_pct': 7.0,
            'max_funding_rate': 0.0008,
            'max_funding_rate_avg': 0.0005,
        }
    elif profile == '10u-diagnostic-live':
        profile_overrides = {
            'diagnostic_trading_mode': True,
            'adaptive_read_only': True,
            'disable_adaptive_risk_upscale': True,
            'risk_usdt': 1.0,
            'max_notional_usdt': 80.0,
            'leverage': 3,
            'max_open_positions': 1,
            'max_long_positions': 1,
            'max_short_positions': 1,
            'max_candidates': 12,
            'allowed_trade_sides': 'long,short',
            'scanner_ticker_rest_fallback': True,
            'scanner_kline_rest_fallback': False,
            'scanner_depth_rest_fallback': False,
            'sim_probe_entry_enabled': True,
            'sim_probe_size_ratio': 0.25,
            'sim_probe_min_score': 55.0,
            'sim_probe_max_breakout_distance_pct': 0.9,
            'trigger_min_confirmations': 1,
            'min_5m_change_pct': 0.25,
            'min_volume_multiple': 0.75,
            'min_score': 55.0,
            'execution_slippage_hard_veto_r': 0.75,
            'enable_symbol_quality_tier': True,
            'enable_market_regime_gate': True,
            'enable_direction_lock': True,
            'enable_fee_aware_edge_filter': True,
        }
    elif profile in {'binance-sim-active', 'high_vol_alt_mode'}:
        profile_overrides = {
            'risk_usdt': 1.0,
            'max_notional_usdt': 300.0,
            'leverage': 3,
            'breakeven_r': 0.8,
            'tp1_r': 1.2,
            'tp1_close_pct': 0.5,
            'tp2_r': 1.8,
            'tp2_close_pct': 0.3,
            'lookback_bars': 3,
            'swing_bars': 4,
            'min_quote_volume': 5_000_000,
            'top_gainers': 40,
            'top_losers': 40,
            'max_candidates': 12,
            'max_rsi_5m': 84.0,
            'min_volume_multiple': 1.05,
            'min_5m_change_pct': 0.5,
            'watch_breakout_tolerance_pct': 0.8,
            'setup_breakout_tolerance_pct': 0.35,
            'oi_hard_reversal_threshold_pct': 0.8,
            'high_vol_alt_mode': True,
            'sim_probe_entry_enabled': True,
            'sim_probe_size_ratio': 0.2,
            'sim_probe_min_score': 62.0,
            'sim_probe_max_breakout_distance_pct': 0.35,
            'execution_slippage_hard_veto_r': 0.75,
            'execution_slippage_risk_threshold_r': 0.5,
            'max_distance_from_ema_pct': 8.0,
            'max_distance_from_vwap_pct': 7.0,
            'max_funding_rate': 0.0008,
            'max_funding_rate_avg': 0.0005,
        }
        if profile == 'binance-sim-active':
            profile_overrides.update({
                'binance_simulated_trading': True,
                'base_url': 'https://testnet.binancefuture.com',
            })
        elif profile == 'high_vol_alt_mode':
            profile_overrides['high_vol_alt_mode'] = True
    elif profile in {
        'aggressive-fee-aware-scalp-long-short',
        'aggressive-fee-aware-scalp-long-only',
        'aggressive-fee-aware-scalp-short-only',
    }:
        profile_overrides = {
            'risk_usdt': 2.0,
            'max_notional_usdt': 80.0,
            'leverage': 5,
            'max_open_positions': 1,
            'max_long_positions': 1,
            'max_short_positions': 1,
            'poll_interval_sec': 30,
            'monitor_poll_interval_sec': 3,
            'stop_buffer_pct': 0.025,
            'breakeven_r': 0.55,
            'tp1_r': 1.0,
            'tp1_close_pct': 0.55,
            'tp2_r': 1.8,
            'tp2_close_pct': 0.35,
            'trailing_buffer_pct': 0.01,
            'trigger_min_confirmations': 1,
            'min_5m_change_pct': 0.45,
            'min_volume_multiple': 1.1,
            'watch_breakout_tolerance_pct': 0.7,
            'setup_breakout_tolerance_pct': 0.35,
            'max_rsi_5m': 86.0,
            'max_distance_from_ema_pct': 5.0,
            'max_distance_from_vwap_pct': 4.5,
            'extended_chase_threshold_pct': 10.0,
            'execution_slippage_hard_veto_r': 0.25,
            'execution_slippage_risk_threshold_r': 0.15,
            'daily_max_loss_usdt': 6.0,
            'max_consecutive_losses': 2,
            'symbol_cooldown_minutes': 30,
            'opposite_side_flip_cooldown_minutes': 90,
            'gross_heat_cap_r': 1.2,
            'same_theme_heat_cap_r': 0.8,
            'same_correlation_heat_cap_r': 0.8,
            'sim_probe_size_ratio': 0.3,
            'allowed_trade_sides': 'long,short',
        }
        if profile == 'aggressive-fee-aware-scalp-long-only':
            profile_overrides['allowed_trade_sides'] = 'long'
        elif profile == 'aggressive-fee-aware-scalp-short-only':
            profile_overrides['allowed_trade_sides'] = 'short'
    for key, value in profile_overrides.items():
        if key not in explicit:
            setattr(args, key, value)
    return args


def get_runtime_state_store(args: argparse.Namespace) -> RuntimeStateStore:
    return RuntimeStateStore(getattr(args, 'runtime_state_dir', os.path.expanduser('~/.hermes/binance-futures-momentum-long/runtime-state')))


def format_pct(value: Any, digits: int = 2) -> str:
    if value is None:
        return '-'
    try:
        return f"{float(value):.{digits}f}%"
    except Exception:
        return '-'


def format_num(value: Any, digits: int = 2) -> str:
    if value is None:
        return '-'
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return '-'


def format_usdt_compact(value: Any) -> str:
    if value is None:
        return '-'
    try:
        amount = float(value)
    except Exception:
        return '-'
    abs_amount = abs(amount)
    if abs_amount >= 1_000_000_000:
        return f"{amount / 1_000_000_000:.2f}B"
    if abs_amount >= 1_000_000:
        return f"{amount / 1_000_000:.2f}M"
    if abs_amount >= 1_000:
        return f"{amount / 1_000:.2f}K"
    return f"{amount:.2f}"


def top_dict_items(d: Any, limit: int = 3) -> List[Tuple[str, Any]]:
    if not isinstance(d, dict):
        return []
    return sorted(d.items(), key=lambda item: item[1], reverse=True)[:limit]


def mask_sensitive_token(token: Any, prefix: int = 4, suffix: int = 4) -> str:
    text = str(token or '')
    if not text:
        return ''
    prefix_len = max(int(prefix or 0), 0)
    suffix_len = max(int(suffix or 0), 0)
    if len(text) <= prefix_len + suffix_len:
        return text
    return text[:prefix_len] + '***' + text[-suffix_len:]


def build_cn_scan_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    from summary_render import build_cn_scan_summary_data

    return build_cn_scan_summary_data(result, mask_sensitive_token)


def render_cn_scan_summary(result: Dict[str, Any]) -> str:
    from summary_render import render_cn_scan_summary_text

    summary = build_cn_scan_summary(result)
    return render_cn_scan_summary_text(summary, format_num, format_pct)


def default_risk_state() -> Dict[str, Any]:
    return {
        'halted': False,
        'halt_reason': '',
        'halted_at': None,
        'daily_realized_pnl': 0.0,
        'daily_loss_limit_hit': False,
        'symbol_cooldowns': {},
        'daily_symbol_loss_usdt': {},
        'rolling_symbol_loss_usdt': {},
        'symbol_consecutive_losses': {},
        'recent_symbol_entries': {},
        'limit_cancel_replace_count_by_symbol': {},
        'loss_deweighted_symbols': {},
        'last_reset_date': datetime.datetime.now(datetime.timezone.utc).date().isoformat(),
        'consecutive_losses': 0,
        'last_loss_at': None,
        'portfolio_exposure_pct_by_theme': {},
        'portfolio_exposure_pct_by_correlation': {},
        'portfolio_heat_open_r': 0.0,
        'portfolio_heat_r_by_theme': {},
        'portfolio_heat_r_by_correlation': {},
    }


def normalize_loaded_risk_state(state: Any) -> Dict[str, Any]:
    return normalize_loaded_risk_state_impl(state, default_risk_state)


def refresh_risk_state_heat_snapshot(risk_state: Dict[str, Any], positions_state: Any, compute_positions_heat_snapshot_func=None) -> Dict[str, Any]:
    return refresh_risk_state_heat_snapshot_impl(
        risk_state,
        positions_state,
        compute_positions_heat_snapshot_func or compute_positions_heat_snapshot,
    )


def load_risk_state(store: RuntimeStateStore) -> Dict[str, Any]:
    return load_runtime_risk_state_impl(
        store,
        should_emit_runtime_state_degraded=_should_emit_runtime_state_degraded,
        append_runtime_state_degraded_event=append_rate_limited_runtime_event,
        default_risk_state=default_risk_state,
        normalize_loaded_risk_state=normalize_loaded_risk_state,
        refresh_risk_state_heat_snapshot=refresh_risk_state_heat_snapshot,
        compute_positions_heat_snapshot=compute_positions_heat_snapshot,
    )


def compute_position_order_reconcile_interval_seconds(args: argparse.Namespace) -> float:
    value = float(getattr(args, 'position_order_reconcile_interval_seconds', 60.0) or 60.0)
    return max(1.0, value)


def evaluate_hard_max_loss_guard(exchange_positions: Sequence[Dict[str, Any]], store: RuntimeStateStore) -> Dict[str, Any]:
    positions_state = migrate_positions_state(store.load_json('positions', {}))
    breaches: List[str] = []
    details: List[Dict[str, Any]] = []
    for row in list(exchange_positions or []):
        if not isinstance(row, dict):
            continue
        symbol = str(row.get('symbol') or '').upper()
        if not symbol:
            continue
        side = position_side_from_exchange_position(row)
        position_key, tracked = get_position_by_symbol_side(positions_state, symbol, side)
        if not isinstance(tracked, dict):
            continue
        hard_max_loss = abs(_to_float(tracked.get('hard_max_loss_usdt') or tracked.get('max_loss_usdt') or tracked.get('symbol_tier_max_loss_usdt')))
        if hard_max_loss <= 0:
            continue
        unrealized_pnl = _to_float(row.get('unRealizedProfit', row.get('unrealizedProfit', row.get('unrealized_pnl_usdt'))))
        if unrealized_pnl <= -hard_max_loss:
            breaches.append(position_key)
            details.append({
                'position_key': position_key,
                'symbol': symbol,
                'side': side,
                'unrealized_pnl_usdt': unrealized_pnl,
                'hard_max_loss_usdt': hard_max_loss,
            })
    result = {'ok': not breaches, 'hard_max_loss_breaches': breaches, 'details': details}
    if breaches:
        risk_state = load_risk_state(store)
        risk_state['halted'] = True
        risk_state['halt_reason'] = f"hard_max_loss:{','.join(breaches)}"
        risk_state['halted_at'] = _isoformat_utc(_utc_now())
        store.save_json('risk_state', risk_state)
        store.append_event('hard_max_loss_guard', result)
    return result


def handle_okx_live_management_exception(store: RuntimeStateStore, operation: str, exc: Exception) -> Dict[str, Any]:
    operation_name = str(operation or 'unknown').strip() or 'unknown'
    halt_reason = f'okx_live_management_failure:{operation_name}'
    risk_state = load_risk_state(store)
    risk_state['halted'] = True
    risk_state['halt_reason'] = halt_reason
    risk_state['halted_at'] = _isoformat_utc(_utc_now())
    store.save_json('risk_state', risk_state)
    payload = {'ok': False, 'fatal': True, 'halt_reason': halt_reason, 'operation': operation_name, 'error': str(exc)}
    store.append_event('okx_live_management_failure', payload)
    return payload


def log_runtime_event(event_type: str, payload: Dict[str, Any]) -> None:
    print(json.dumps({'event_type': event_type, **payload}, ensure_ascii=False), flush=True)


def load_env_value(key: str, default: str = '') -> str:
    return os.getenv(key, default)


def parse_notification_target(target: str) -> Dict[str, Any]:
    raw = (target or '').strip()
    if not raw:
        raise ValueError(f'invalid notification target: {target}')
    if ':' not in raw:
        platform = raw.lower()
        home_env = {
            'weixin': 'WEIXIN_HOME_CHANNEL',
            'telegram': 'TELEGRAM_HOME_CHANNEL',
        }.get(platform)
        home_chat_id = load_env_value(home_env) if home_env else ''
        if not home_chat_id:
            raise ValueError(f'invalid notification target: {target}')
        return {'platform': platform, 'chat_id': home_chat_id, 'thread_id': None}
    platform, remainder = raw.split(':', 1)
    platform = platform.strip().lower()
    thread_id = None
    chat_id = remainder.strip()
    if platform == 'telegram' and ':' in remainder:
        chat_id, thread_id = remainder.rsplit(':', 1)
    return {'platform': platform, 'chat_id': chat_id, 'thread_id': thread_id}


def build_notification_message(event_type: str, payload: Dict[str, Any]) -> str:
    symbol = payload.get('symbol', '-')
    profile = payload.get('profile', '')
    if event_type == 'entry_filled':
        side_text = {
            'LONG': '做多',
            'SHORT': '做空',
        }.get(str(payload.get('side', '')).upper(), str(payload.get('side') or ''))
        parts = [f"开单成交 {symbol}"]
        if side_text:
            parts.append(f"方向={side_text}")
        if payload.get('entry_price') is not None:
            parts.append(f"成交价={payload.get('entry_price')}")
        if payload.get('stop_price') is not None:
            parts.append(f"止损价={payload.get('stop_price')}")
        if payload.get('quantity') is not None:
            parts.append(f"数量={payload.get('quantity')}")
        if profile:
            parts.append(f"策略={profile}")
        return ' '.join(str(part) for part in parts if str(part).strip())
    if event_type == 'user_data_stream_alert':
        status_labels = {
            'refresh_failed': '续期失败',
            'disconnected': '已断线',
            'started': '已启动',
            'refreshed': '已续期',
        }
        listen_key = str(payload.get('listen_key') or '')
        masked_listen_key = listen_key[:4] + '***' + listen_key[-4:] if len(listen_key) > 8 else listen_key
        parts = [
            f"用户数据流告警 {symbol}",
            f"状态={status_labels.get(str(payload.get('status') or ''), payload.get('status', '-'))}",
            f"动作={payload.get('action', '-')}",
        ]
        error_text = payload.get('error') or payload.get('detail')
        if error_text:
            parts.append(f"错误={error_text}")
        if payload.get('disconnect_count') is not None:
            parts.append(f"断线次数={payload.get('disconnect_count')}")
        if payload.get('refresh_failure_count') is not None:
            parts.append(f"续期失败次数={payload.get('refresh_failure_count')}")
        if payload.get('reconnect_count') is not None:
            parts.append(f"重连次数={payload.get('reconnect_count')}")
        if payload.get('last_refresh_at'):
            parts.append(f"最近续期={payload.get('last_refresh_at')}")
        if payload.get('updated_at'):
            parts.append(f"更新时间={payload.get('updated_at')}")
        if masked_listen_key:
            parts.append(f"listenKey={masked_listen_key}")
        return ' '.join(str(part) for part in parts if str(part).strip())
    return f"{event_type} {json.dumps(payload, ensure_ascii=False)}"


def send_telegram_notification(bot_token: str, chat_id: str, message: str, thread_id: Optional[str] = None, post_func=None) -> Dict[str, Any]:
    if not bot_token:
        raise ValueError('telegram bot token is required')
    post = post_func or requests.post
    body = {'chat_id': chat_id, 'text': message}
    if thread_id:
        body['message_thread_id'] = thread_id
    resp = post(f'https://api.telegram.org/bot{bot_token}/sendMessage', json=body, timeout=15)
    if hasattr(resp, 'raise_for_status'):
        resp.raise_for_status()
    data = resp.json() if hasattr(resp, 'json') else {'ok': True}
    result = data.get('result', {}) if isinstance(data, dict) else {}
    return {'ok': bool(data.get('ok', True)) if isinstance(data, dict) else True, 'platform': 'telegram', 'message_id': result.get('message_id')}


def send_weixin_notification(chat_id: str, message: str, send_func=None) -> Dict[str, Any]:
    if send_func is None:
        try:
            from gateway.platforms.weixin import check_weixin_requirements, send_weixin_direct
        except Exception:
            hermes_agent_root = Path('/root/.hermes/hermes-agent')
            hermes_agent_root_text = str(hermes_agent_root)
            if hermes_agent_root.exists() and hermes_agent_root_text not in sys.path:
                sys.path.insert(0, hermes_agent_root_text)
            try:
                from gateway.platforms.weixin import check_weixin_requirements, send_weixin_direct
            except Exception as inner_exc:
                raise RuntimeError('weixin direct adapter unavailable') from inner_exc
        if not check_weixin_requirements():
            raise RuntimeError('weixin adapter requirements not met')

        def direct_send_message(*, extra, token, chat_id, message, media_files=None):
            return asyncio.run(
                send_weixin_direct(
                    extra=extra,
                    token=token,
                    chat_id=chat_id,
                    message=message,
                    media_files=media_files,
                )
            )

        send_func = direct_send_message
    result = send_func(extra={}, token='', chat_id=chat_id, message=message, media_files=None)
    return {'ok': bool(result.get('success', result.get('ok', True))), 'platform': 'weixin', 'message_id': result.get('message_id')}


def emit_notification(args: argparse.Namespace, event_type: str, payload: Dict[str, Any], post_func=None) -> Dict[str, Any]:
    if getattr(args, 'disable_notify', False):
        return {'ok': True, 'event_type': event_type, 'target': getattr(args, 'notify_target', ''), 'skipped': True}
    target_text = getattr(args, 'notify_target', '')
    targets = [item.strip() for item in target_text.split(',') if item.strip()]
    message = build_notification_message(event_type, payload)
    results = []
    for target in targets:
        parsed = parse_notification_target(target)
        try:
            if parsed['platform'] == 'telegram':
                bot_token = load_env_value(getattr(args, 'telegram_bot_token_env', 'TELEGRAM_BOT_TOKEN'))
                results.append(send_telegram_notification(bot_token, parsed['chat_id'], message, thread_id=parsed.get('thread_id'), post_func=post_func))
            elif parsed['platform'] == 'weixin':
                results.append(send_weixin_notification(parsed['chat_id'], message))
            else:
                results.append({'ok': False, 'platform': parsed['platform'], 'error': 'unsupported_platform'})
        except Exception as exc:
            results.append({'ok': False, 'platform': parsed.get('platform', 'unknown'), 'error': str(exc)})
    if not results:
        return {'ok': True, 'event_type': event_type, 'target': target_text, 'results': []}
    if len(results) == 1:
        merged = dict(results[0])
        merged.update({'event_type': event_type, 'target': target_text})
        return merged
    return {'ok': all(item.get('ok', False) for item in results), 'event_type': event_type, 'target': target_text, 'results': results}


def fetch_open_orders(client: Any, symbol: Optional[str] = None):
    params = {'symbol': symbol} if symbol else {}
    return client.signed_get('/fapi/v1/openOrders', params=params) if hasattr(client, 'signed_get') else []


def fetch_open_algo_orders(client: Any, symbol: Optional[str] = None):
    params = {'symbol': symbol} if symbol else {}
    if not hasattr(client, 'signed_get'):
        return []
    try:
        rows = client.signed_get('/fapi/v1/openAlgoOrders', params=params)
    except Exception:
        return []
    return rows if isinstance(rows, list) else []


def fetch_open_positions(client: Any):
    if hasattr(client, 'signed_get'):
        rows = client.signed_get('/fapi/v2/positionRisk', params={})
        return [row for row in rows if abs(_to_float(row.get('positionAmt'))) > 0]
    return []


def exchange_position_runtime_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    position_amt = _to_float(row.get('positionAmt'))
    qty = abs(position_amt)
    mark_price = _to_float(row.get('markPrice'))
    entry_price = _to_float(row.get('entryPrice'))
    notional = abs(_to_float(row.get('notional')))
    if notional <= 0 and qty > 0:
        reference_price = mark_price if mark_price > 0 else entry_price
        notional = abs(qty * reference_price)
    leverage = _to_float(row.get('leverage'))
    isolated_margin = _to_float(row.get('isolatedMargin'))
    margin = isolated_margin if isolated_margin > 0 else (notional / leverage if leverage > 0 else 0.0)
    unrealized_pnl = _to_float(row.get('unRealizedProfit', row.get('unrealizedProfit')))
    pnl_pct = (unrealized_pnl / margin * 100.0) if margin > 0 else None
    fields: Dict[str, Any] = {
        'exchange_position_amt': position_amt,
        'quantity': qty,
        'remaining_quantity': qty,
        'current_price': mark_price if mark_price > 0 else None,
        'mark_price': mark_price if mark_price > 0 else None,
        'position_notional': notional if notional > 0 else None,
        'unrealized_pnl_usdt': unrealized_pnl,
        'position_margin_usdt': margin if margin > 0 else None,
        'exchange_update_time': int(time.time() * 1000),
    }
    if entry_price > 0:
        fields['entry_price'] = entry_price
    if leverage > 0:
        fields['leverage'] = int(leverage) if float(leverage).is_integer() else leverage
    if pnl_pct is not None:
        fields['unrealized_pnl_pct'] = pnl_pct
    return {key: value for key, value in fields.items() if value is not None}


def resolve_position_protection_status(client: Any, symbol: str, expected_stop_order: Optional[Dict[str, Any]] = None, allow_missing_when_flat: bool = True, side: Any = POSITION_SIDE_LONG) -> Dict[str, Any]:
    return execution_resolve_position_protection_status(
        client,
        symbol,
        expected_stop_order=expected_stop_order,
        allow_missing_when_flat=allow_missing_when_flat,
        side=side,
        position_side_long=POSITION_SIDE_LONG,
        normalize_position_side=normalize_position_side,
        fetch_open_positions=fetch_open_positions,
        fetch_open_orders=fetch_open_orders,
        fetch_open_algo_orders=fetch_open_algo_orders,
        position_row_matches_symbol_side=position_row_matches_symbol_side,
        _to_float=_to_float,
    )

def repair_missing_protection(client: Any, symbol: str, tracked: Optional[Dict[str, Any]], active_position: Optional[Dict[str, Any]], meta: Optional[SymbolMeta] = None) -> Dict[str, Any]:
    return execution_repair_missing_protection(
        client,
        symbol,
        tracked=tracked,
        active_position=active_position,
        meta=meta,
        normalize_position_side=normalize_position_side,
        place_stop_market_order=place_stop_market_order,
        fetch_exchange_meta=fetch_exchange_meta,
        _to_float=_to_float,
    )


def build_exchange_reconciled_position_record(row: Dict[str, Any], args: Optional[argparse.Namespace] = None) -> Dict[str, Any]:
    symbol = str(row.get('symbol') or '').upper()
    side = position_side_from_exchange_position(row)
    entry_price = _to_float(row.get('entryPrice'))
    mark_price = _to_float(row.get('markPrice'))
    if side == POSITION_SIDE_LONG:
        positive_prices = [price for price in (entry_price, mark_price) if price > 0]
        reference_price = min(positive_prices) if positive_prices else 0.0
    else:
        positive_prices = [price for price in (entry_price, mark_price) if price > 0]
        reference_price = max(positive_prices) if positive_prices else 0.0
    stop_pct = abs(_to_float(getattr(args, 'exchange_reconcile_stop_pct', 0.02) if args is not None else 0.02, default=0.02))
    if stop_pct > 1.0:
        stop_pct = stop_pct / 100.0
    if stop_pct <= 0:
        stop_pct = 0.02
    stop_price = 0.0
    if reference_price > 0:
        stop_price = reference_price * (1.0 - stop_pct) if side == POSITION_SIDE_LONG else reference_price * (1.0 + stop_pct)
    fields = exchange_position_runtime_fields(row)
    record = {
        'symbol': symbol,
        'side': side,
        'position_side': side,
        'position_key': build_position_key(symbol, side),
        'status': 'reconciled',
        'monitor_mode': 'trade_management',
        'exchange_reconciled_at': _isoformat_utc(_utc_now()),
        'exchange_reconcile_reason': 'exchange_position_present_runtime_missing',
        **fields,
    }
    if stop_price > 0:
        record['stop_price'] = stop_price
        record['current_stop_price'] = stop_price
        record['emergency_reconcile_stop_pct'] = stop_pct
        quantity = _to_float(record.get('quantity'))
        if reference_price > 0 and quantity > 0:
            plan = build_trade_management_plan(
                entry_price=reference_price,
                stop_price=stop_price,
                quantity=quantity,
                tp1_r=float(getattr(args, 'tp1_r', 1.5) if args is not None else 1.5),
                tp1_close_pct=float(getattr(args, 'tp1_close_pct', 0.3) if args is not None else 0.3),
                tp1_profit_usdt=float(getattr(args, 'tp1_profit_usdt', 0.0) if args is not None else 0.0),
                tp2_r=float(getattr(args, 'tp2_r', 2.0) if args is not None else 2.0),
                tp2_close_pct=float(getattr(args, 'tp2_close_pct', 0.4) if args is not None else 0.4),
                breakeven_r=float(getattr(args, 'breakeven_r', 1.0) if args is not None else 1.0),
                atr_stop_distance=abs(reference_price - stop_price),
                side=position_side_to_trade_side(side),
                breakeven_confirmation_mode=str(getattr(args, 'breakeven_confirmation_mode', 'ema_support') if args is not None else 'ema_support'),
                breakeven_min_buffer_pct=float(getattr(args, 'breakeven_min_buffer_pct', 0.001) if args is not None else 0.001),
                micro_scalp_time_stop_sec=int(getattr(args, 'micro_scalp_time_stop_sec', 0) if args is not None else 0),
                micro_scalp_min_profit_r=float(getattr(args, 'micro_scalp_min_profit_r', 0.0) if args is not None else 0.0),
            )
            record['trade_management_plan'] = asdict(plan)
    return record


def fetch_recent_user_trades_for_position(client: Any, tracked: Dict[str, Any]) -> List[Dict[str, Any]]:
    symbol = str((tracked or {}).get('symbol') or '').upper()
    if not symbol or client is None or not hasattr(client, 'signed_get'):
        return []
    previous_history_setting = getattr(client, 'allow_heavy_history_rest', None)
    try:
        if previous_history_setting is not None:
            client.allow_heavy_history_rest = True
        rows = client.signed_get('/fapi/v1/userTrades', {'symbol': symbol, 'limit': 100})
    except Exception:
        return []
    finally:
        if previous_history_setting is not None:
            client.allow_heavy_history_rest = previous_history_setting
    return [row for row in list(rows or []) if isinstance(row, dict)]


def classify_reconciled_exit_reason(tracked: Dict[str, Any], summary: Dict[str, Any]) -> str:
    order_ids = {int(order_id) for order_id in list(summary.get('exit_order_ids') or [])}
    stop_order_id = int(_to_float((tracked or {}).get('stop_order_id'), default=0.0))
    if stop_order_id > 0 and stop_order_id in order_ids:
        return 'stop_loss'
    return str((tracked or {}).get('exit_reason') or 'exchange_position_missing')


def sync_tracked_positions_with_exchange(store: RuntimeStateStore, exchange_positions: Sequence[Dict[str, Any]], protected_symbols: Optional[Sequence[str]] = None, client: Any = None) -> Dict[str, Any]:
    raw_positions_state = store.load_json('positions', {})
    if not isinstance(raw_positions_state, dict):
        raw_positions_state = {}
    original_keys = {build_position_key(value.get('symbol') or split_position_key(key)[0], value.get('side') or split_position_key(key)[1]): str(key).upper() for key, value in raw_positions_state.items() if isinstance(value, dict)}
    positions_state = migrate_positions_state(raw_positions_state)
    protected_keys = {str(symbol).upper() for symbol in list(protected_symbols or []) if symbol}
    protected_symbol_names = {split_position_key(symbol)[0] for symbol in protected_keys}
    exchange_map = {
        build_position_key(row.get('symbol'), position_side_from_exchange_position(row)): row
        for row in list(exchange_positions or [])
        if isinstance(row, dict)
        and row.get('symbol')
        and abs(_to_float(row.get('positionAmt', row.get('position_amt', row.get('quantity'))))) > 0
    }
    closed_symbols: List[str] = []
    closed_trade_details: List[Dict[str, Any]] = []
    refreshed_symbols: List[str] = []
    orphan_symbols: List[str] = []
    normalized_positions: Dict[str, Any] = {}
    for existing_key, tracked in list(positions_state.items()):
        if not isinstance(tracked, dict):
            continue
        symbol = str(tracked.get('symbol') or split_position_key(existing_key)[0]).upper()
        side = normalize_position_side(tracked.get('side') or split_position_key(existing_key)[1])
        position_key = build_position_key(symbol, side)
        tracked = dict(tracked)
        tracked['symbol'] = symbol
        tracked['side'] = side
        tracked['position_key'] = position_key
        report_key = original_keys.get(position_key, position_key)
        if side == POSITION_SIDE_LONG:
            report_key = symbol
        exchange_row = exchange_map.get(position_key)
        if exchange_row is None:
            live_quantity_value = abs(_to_float(tracked.get('quantity')))
            remaining_quantity_value = abs(_to_float(tracked.get('remaining_quantity')))
            status_text = str(tracked.get('status') or '').lower()
            already_reconciled_closed = (
                str(tracked.get('exchange_reconcile_reason') or '') == 'exchange_position_missing'
                and str(tracked.get('closed_at') or '').strip() != ''
                and live_quantity_value <= 0
                and remaining_quantity_value <= 0
            )
            was_openish = (
                status_text in {'monitoring', 'orphan', 'recovery_pending', 'protected_recovery_pending'}
                or live_quantity_value > 0
                or remaining_quantity_value > 0
            ) and not already_reconciled_closed
            exit_summary: Dict[str, Any] = {}
            reconciled_exit_reason = str(tracked.get('exit_reason') or 'exchange_position_missing')
            if was_openish and client is not None:
                exit_summary = summarize_exit_trade_fills(tracked, fetch_recent_user_trades_for_position(client, tracked))
                if int(exit_summary.get('exit_fill_count') or 0) > 0:
                    reconciled_exit_reason = classify_reconciled_exit_reason(tracked, exit_summary)
            tracked['status'] = 'closed'
            tracked['quantity'] = 0.0
            tracked['remaining_quantity'] = 0.0
            tracked['exchange_position_amt'] = 0.0
            tracked['notional'] = 0.0
            tracked['unrealized_pnl'] = 0.0
            tracked['mark_price'] = 0.0
            tracked['entry_price'] = _to_float(tracked.get('entry_price'), default=0.0)
            tracked['stop_order_id'] = None
            tracked['protection_status'] = 'flat'
            tracked['recovery_incomplete'] = False
            tracked['protected_recovery_pending'] = False
            tracked['trade_management_plan'] = {}
            tracked['monitor_mode'] = 'closed'
            tracked['user_data_stream'] = {}
            tracked['book_ticker_websocket'] = {}
            tracked['monitor_thread_name'] = ''
            tracked['active_stop_order'] = {}
            tracked['exchange_reconcile_reason'] = 'exchange_position_missing'
            tracked['closed_at'] = str(tracked.get('closed_at') or _isoformat_utc(_utc_now()))
            tracked['exit_reason'] = reconciled_exit_reason
            if exit_summary:
                tracked.update(exit_summary)
            if was_openish:
                closed_symbols.append(report_key)
                closed_detail = {
                    'position_key': position_key,
                    'symbol': symbol,
                    'position_side': side,
                    'exit_reason': tracked['exit_reason'],
                    **exit_summary,
                }
                closed_trade_details.append(closed_detail)
                emit_position_closed_runtime_event(
                    store,
                    tracked,
                    exit_reason=tracked['exit_reason'],
                    exit_price=exit_summary.get('exit_price'),
                    exit_source='exchange_reconcile',
                    extra_payload={'exchange_reconcile_reason': 'exchange_position_missing', **exit_summary},
                )
            normalized_positions[position_key] = tracked
            continue

        previous_quantity = abs(_to_float(tracked.get('remaining_quantity') or tracked.get('quantity')))
        tracked.update(exchange_position_runtime_fields(exchange_row))
        live_quantity = abs(_to_float(tracked.get('remaining_quantity') or tracked.get('quantity')))
        plan = tracked.get('trade_management_plan')
        if isinstance(plan, dict) and plan and live_quantity > 0 and abs(live_quantity - previous_quantity) > 1e-10:
            # Preserve trigger prices but resize every close leg to the actual
            # exchange remainder after partial or manual fills.
            plan = dict(plan)
            old_parts = [max(0.0, _to_float(plan.get(name))) for name in ('tp1_close_qty', 'tp2_close_qty', 'runner_qty')]
            old_total = sum(old_parts)
            if old_total <= 0:
                old_parts, old_total = [0.0, 0.0, 1.0], 1.0
            resized = [live_quantity * part / old_total for part in old_parts]
            resized[2] = max(0.0, live_quantity - resized[0] - resized[1])
            plan['quantity'] = live_quantity
            plan['tp1_close_qty'], plan['tp2_close_qty'], plan['runner_qty'] = [round(value, 10) for value in resized]
            tracked['trade_management_plan'] = plan
        if str(tracked.get('exchange_reconcile_reason') or '') == 'exchange_position_missing':
            tracked.pop('exchange_reconcile_reason', None)
            tracked.pop('closed_at', None)
            if str(tracked.get('exit_reason') or '') == 'exchange_position_missing':
                tracked.pop('exit_reason', None)
            tracked['monitor_mode'] = 'trade_management'
        if str(tracked.get('status') or '').lower() == 'closed':
            tracked['status'] = 'monitoring'
        tracked['protection_status'] = 'protected' if position_key in protected_keys or symbol in protected_symbol_names else tracked.get('protection_status')
        if tracked.get('status') == 'orphan':
            orphan_symbols.append(report_key)
        refreshed_symbols.append(report_key)
        normalized_positions[position_key] = tracked
    for position_key, exchange_row in exchange_map.items():
        if position_key in normalized_positions:
            continue
        symbol, side = split_position_key(position_key)
        tracked = {
            'symbol': symbol,
            'side': side,
            'position_side': side,
            'position_key': position_key,
            'status': 'orphan',
            'monitor_mode': 'trade_management',
            **exchange_position_runtime_fields(exchange_row),
        }
        tracked['protection_status'] = 'protected' if position_key in protected_keys or symbol in protected_symbol_names else tracked.get('protection_status')
        orphan_symbols.append(symbol if side == POSITION_SIDE_LONG else position_key)
        refreshed_symbols.append(symbol if side == POSITION_SIDE_LONG else position_key)
        normalized_positions[position_key] = tracked
    materialized_positions = materialize_positions_state(normalized_positions, original_keys, include_legacy_alias=False)
    store.save_json('positions', materialized_positions)
    return {
        'closed_symbols': closed_symbols,
        'closed_trade_details': closed_trade_details,
        'refreshed_symbols': refreshed_symbols,
        'orphan_symbols': orphan_symbols,
    }


def reconcile_runtime_state(client: Any, store: RuntimeStateStore, halt_on_orphan_position: bool = False, repair_missing_protection_enabled: bool = True, args: Optional[argparse.Namespace] = None) -> Dict[str, Any]:
    positions_state = store.load_json('positions', {})
    if not isinstance(positions_state, dict):
        positions_state = {}
    exchange_positions = fetch_open_positions(client)
    active_exchange_positions = [
        row
        for row in list(exchange_positions or [])
        if isinstance(row, dict) and abs(_to_float(row.get('positionAmt', row.get('position_amt', row.get('quantity'))))) > 0
    ]
    orphan_positions = []
    positions_missing_protection = []
    protected_symbols = []
    protection_repairs = []
    for row in active_exchange_positions:
        symbol = row.get('symbol')
        if not symbol:
            continue
        side = position_side_from_exchange_position(row)
        position_key, tracked = get_position_by_symbol_side(positions_state, symbol, side)
        tracked = tracked if tracked else None
        protection = resolve_position_protection_status(client, symbol, expected_stop_order={'orderId': tracked.get('stop_order_id')} if isinstance(tracked, dict) and tracked.get('stop_order_id') else None, side=side)
        if protection.get('status') == 'protected':
            protected_symbols.append(position_key)
        if tracked is None:
            orphan_positions.append(symbol)
            tracked = build_exchange_reconciled_position_record(row, args)
        elif protection.get('status') != 'protected':
            recovery_record = build_exchange_reconciled_position_record(row, args)
            for field in (
                'stop_price',
                'current_stop_price',
                'emergency_reconcile_stop_pct',
                'trade_management_plan',
                'recovery_incomplete',
                'recovery_reason',
                'recovery_detail',
            ):
                if field in recovery_record:
                    tracked[field] = recovery_record[field]
            if tracked.get('trade_management_plan'):
                tracked['status'] = 'reconciled'
                tracked['protected_recovery_pending'] = False
        if protection.get('status') != 'protected':
            repair_result = None
            if repair_missing_protection_enabled:
                stale_stop = protection.get('stop_order')
                if protection.get('status') == 'quantity_mismatch' and isinstance(stale_stop, dict):
                    cancel_protection_order(client, stale_stop)
                repair_result = repair_missing_protection(
                    client=client,
                    symbol=symbol,
                    tracked=tracked,
                    active_position=protection.get('active_position'),
                )
                protection_repairs.append(repair_result)
            if repair_result and repair_result.get('ok') and repair_result.get('status') == 'protected':
                protected_symbols.append(position_key)
                tracked['protection_status'] = 'protected'
                stop_order = repair_result.get('stop_order', {}) if isinstance(repair_result.get('stop_order'), dict) else {}
                tracked['stop_order_id'] = stop_order.get('orderId') or stop_order.get('algoId') or stop_order.get('clientAlgoId')
                tracked['active_stop_order'] = stop_order
                tracked['status'] = 'monitoring'
                positions_state, _ = upsert_position_record(positions_state, tracked, key=position_key)
            else:
                positions_missing_protection.append(symbol if side == POSITION_SIDE_LONG else position_key)
                tracked['protection_status'] = protection.get('status')
                positions_state, _ = upsert_position_record(positions_state, tracked, key=position_key)
        else:
            tracked['protection_status'] = 'protected'
            if tracked.get('status') in {'protected_recovery_pending', 'recovery_pending'}:
                tracked = recover_protected_position_trade_management_plan(tracked, protection, args)
            positions_state, _ = upsert_position_record(positions_state, tracked, key=position_key)
    store.save_json('positions', positions_state)
    sync_result = sync_tracked_positions_with_exchange(store, active_exchange_positions, protected_symbols=protected_symbols, client=client)
    hard_max_loss_guard = evaluate_hard_max_loss_guard(active_exchange_positions, store)
    stale_protection_cleanup = []
    for closed_key in sync_result.get('closed_symbols', []) or []:
        closed_symbol, closed_side = split_position_key(closed_key)
        if not closed_symbol:
            continue
        cleanup_result = execution_cancel_stale_protection_orders_after_flat(
            client=client,
            symbol=closed_symbol,
            position_side=closed_side,
            fetch_open_orders=fetch_open_orders,
            cancel_order=cancel_order,
            emit_event=lambda event_type, payload: store.append_event(event_type, payload),
        )
        stale_protection_cleanup.append(cleanup_result)
    result = {
        'ok': True,
        'orphan_positions': orphan_positions,
        'positions_missing_protection': positions_missing_protection,
        'protection_repairs': protection_repairs,
        'stale_protection_cleanup': stale_protection_cleanup,
        'exchange_position_count': len(active_exchange_positions),
        'position_count': len(active_exchange_positions),
        'closed_tracked_positions': sync_result.get('closed_symbols', []),
        'closed_trade_details': sync_result.get('closed_trade_details', []),
        'refreshed_tracked_positions': sync_result.get('refreshed_symbols', []),
        'hard_max_loss_guard': hard_max_loss_guard,
    }
    if not hard_max_loss_guard.get('ok', True):
        result['ok'] = False
    if halt_on_orphan_position and orphan_positions:
        risk_state = load_risk_state(store)
        risk_state['halted'] = True
        risk_state['halt_reason'] = f"orphan_positions:{','.join(orphan_positions)}"
        store.save_json('risk_state', risk_state)
        store.append_event('reconcile', result)
        result['ok'] = False
    return result


def apply_reconcile_close_risk_state_updates(store: RuntimeStateStore, reconcile_result: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    closed_position_keys = [
        str(item or '').strip()
        for item in list((reconcile_result or {}).get('closed_tracked_positions') or [])
        if str(item or '').strip()
    ]
    if not closed_position_keys:
        return load_risk_state(store)

    risk_state = load_risk_state(store)
    now_ts = int(time.time())
    cooldown_minutes = max(int(getattr(args, 'symbol_cooldown_minutes', 0) or 0), 0)
    cooldown_until = now_ts + cooldown_minutes * 60 if cooldown_minutes > 0 else None
    symbol_cooldowns = risk_state.setdefault('symbol_cooldowns', {})
    recent_closed_trades = risk_state.setdefault('recent_closed_trades', [])
    detail_by_key = {
        str(row.get('position_key') or '').upper(): row
        for row in list((reconcile_result or {}).get('closed_trade_details') or [])
        if isinstance(row, dict) and row.get('position_key')
    }

    for position_key in closed_position_keys:
        symbol, position_side = split_position_key(position_key)
        normalized_symbol = str(symbol or '').strip().upper()
        normalized_side = normalize_position_side(position_side or POSITION_SIDE_LONG)
        if not normalized_symbol:
            continue
        if cooldown_until is not None:
            existing_until = symbol_cooldowns.get(normalized_symbol)
            if existing_until is None or int(existing_until) < cooldown_until:
                symbol_cooldowns[normalized_symbol] = cooldown_until
        detail = detail_by_key.get(build_position_key(normalized_symbol, normalized_side), {})
        net_pnl = round(_to_float(detail.get('net_pnl_usdt'), default=0.0), 10)
        recent_closed_trades.append({
            'symbol': normalized_symbol,
            'position_side': normalized_side,
            'side': position_side_to_trade_side(normalized_side),
            'closed_at': now_ts,
            'exit_reason': str(detail.get('exit_reason') or 'exchange_position_missing'),
            'closed_via_reconcile': True,
            'realized_pnl_usdt': round(_to_float(detail.get('realized_pnl_usdt'), default=0.0), 10),
            'commission_usdt': round(_to_float(detail.get('commission_usdt'), default=0.0), 10),
            'net_pnl_usdt': net_pnl,
        })
        risk_state['daily_realized_pnl'] = round(_to_float(risk_state.get('daily_realized_pnl'), default=0.0) + net_pnl, 10)
        risk_state['daily_realized_pnl_usdt'] = round(_to_float(risk_state.get('daily_realized_pnl_usdt'), default=0.0) + net_pnl, 10)
        if net_pnl < 0:
            risk_state['consecutive_losses'] = int(_to_float(risk_state.get('consecutive_losses'), default=0.0)) + 1
            risk_state['last_loss_at'] = now_ts
        elif net_pnl > 0:
            risk_state['consecutive_losses'] = 0

    if len(recent_closed_trades) > 50:
        risk_state['recent_closed_trades'] = recent_closed_trades[-50:]
    store.save_json('risk_state', risk_state)
    return risk_state


def build_position_exposure_snapshot(open_positions: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    long_positions = []
    short_positions = []
    symbol_sides: Dict[str, set] = {}
    net_exposure_usdt = 0.0
    gross_exposure_usdt = 0.0
    for row in list(open_positions or []):
        if not isinstance(row, dict):
            continue
        symbol = str(row.get('symbol') or '').upper()
        side = normalize_position_side(row.get('positionSide') or row.get('side') or POSITION_SIDE_LONG)
        quantity = abs(_to_float(row.get('positionAmt') or row.get('quantity')))
        notional = abs(_to_float(row.get('notional')))
        if notional <= 0 and quantity > 0:
            entry_price = abs(_to_float(row.get('entryPrice') or row.get('markPrice') or row.get('entry_price')))
            notional = quantity * entry_price
        item = {
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'notional_usdt': notional,
        }
        if side == POSITION_SIDE_SHORT:
            short_positions.append(item)
            net_exposure_usdt -= notional
        else:
            long_positions.append(item)
            net_exposure_usdt += notional
        gross_exposure_usdt += notional
        if symbol:
            symbol_sides.setdefault(symbol, set()).add(side)
    return {
        'long_positions': long_positions,
        'short_positions': short_positions,
        'long_count': len(long_positions),
        'short_count': len(short_positions),
        'net_exposure_usdt': net_exposure_usdt,
        'gross_exposure_usdt': gross_exposure_usdt,
        'symbol_sides': {symbol: sorted(list(sides)) for symbol, sides in symbol_sides.items()},
    }


def evaluate_portfolio_risk_guards(open_positions: Sequence[Dict[str, Any]], candidate: Any = None, max_long_positions: int = 0, max_short_positions: int = 0, max_net_exposure_usdt: float = 0.0, max_gross_exposure_usdt: float = 0.0, per_symbol_single_side_only: bool = True, opposite_side_flip_cooldown_minutes: int = 0) -> Dict[str, Any]:
    return evaluate_portfolio_risk_guards_impl(
        open_positions=open_positions,
        candidate=candidate,
        max_long_positions=max_long_positions,
        max_short_positions=max_short_positions,
        max_net_exposure_usdt=max_net_exposure_usdt,
        max_gross_exposure_usdt=max_gross_exposure_usdt,
        per_symbol_single_side_only=per_symbol_single_side_only,
        opposite_side_flip_cooldown_minutes=opposite_side_flip_cooldown_minutes,
        build_position_exposure_snapshot=build_position_exposure_snapshot,
        normalize_position_side=normalize_position_side,
        position_side_long=POSITION_SIDE_LONG,
        position_side_short=POSITION_SIDE_SHORT,
        _to_float=_to_float,
    )


def evaluate_risk_guards(symbol: Optional[str] = None, risk_state: Optional[Dict[str, Any]] = None, candidate: Any = None, now_ts: Optional[int] = None, daily_max_loss_usdt: float = 0.0, max_consecutive_losses: int = 0, symbol_cooldown_minutes: int = 0, **kwargs) -> Dict[str, Any]:
    return evaluate_risk_guards_impl(
        symbol=symbol,
        risk_state=risk_state,
        candidate=candidate,
        now_ts=now_ts,
        daily_max_loss_usdt=daily_max_loss_usdt,
        max_consecutive_losses=max_consecutive_losses,
        symbol_cooldown_minutes=symbol_cooldown_minutes,
        default_risk_state=default_risk_state,
        _to_float=_to_float,
        compute_expected_slippage_r=compute_expected_slippage_r,
        classify_execution_liquidity_grade=classify_execution_liquidity_grade,
        estimate_candidate_heat_r=estimate_candidate_heat_r,
        time_module=time,
        **kwargs,
    )


SIM_PROBE_ALLOWED_RISK_REASONS = {'candidate_trigger_not_fired'}
SIM_PROBE_HARD_RISK_REASONS = {
    'candidate_setup_not_ready',
    'candidate_distribution_risk',
    'candidate_cvd_divergence',
    'candidate_oi_reversal',
    'candidate_execution_slippage_risk',
    'candidate_edge_after_costs_insufficient',
    'candidate_execution_liquidity_poor',
    'strategy_halted',
    'daily_max_loss_reached',
    'max_consecutive_losses_reached',
    'symbol_cooldown_active',
    'candidate_portfolio_heat_overexposure',
    'candidate_portfolio_theme_overexposure',
    'candidate_portfolio_correlation_overexposure',
    'candidate_same_theme_heat_overexposure',
    'candidate_same_correlation_heat_overexposure',
}


def evaluate_sim_probe_entry(candidate: Any, risk_guard: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    if not bool(getattr(args, 'sim_probe_entry_enabled', False)):
        return {'allowed': False, 'reasons': ['sim_probe_disabled']}
    if not bool(getattr(candidate, 'setup_ready', False)):
        return {'allowed': False, 'reasons': ['candidate_setup_not_ready']}
    if bool(getattr(candidate, 'trigger_fired', False)):
        return {'allowed': False, 'reasons': ['full_trigger_already_fired']}
    min_score = float(getattr(args, 'sim_probe_min_score', 62.0) or 62.0)
    if float(getattr(candidate, 'score', 0.0) or 0.0) < min_score:
        return {'allowed': False, 'reasons': ['sim_probe_score_below_min']}
    execution_quality = compute_execution_quality_size_adjustment(candidate)
    if str(execution_quality.get('execution_liquidity_grade', '') or '') not in {'A+', 'A'}:
        return {'allowed': False, 'reasons': ['sim_probe_liquidity_not_good'], 'execution_quality': execution_quality}
    max_breakout_distance = float(getattr(args, 'sim_probe_max_breakout_distance_pct', 0.35) or 0.35)
    if float(getattr(candidate, 'entry_distance_from_breakout_pct', 0.0) or 0.0) < -max_breakout_distance:
        return {'allowed': False, 'reasons': ['sim_probe_breakout_distance_too_far'], 'execution_quality': execution_quality}
    if bool(getattr(candidate, 'overextension_flag', False)):
        return {'allowed': False, 'reasons': ['sim_probe_price_extension_risk'], 'execution_quality': execution_quality}
    risk_reasons = set(risk_guard.get('reasons', []) or [])
    hard_reasons = sorted(risk_reasons & SIM_PROBE_HARD_RISK_REASONS)
    unexpected_reasons = sorted(risk_reasons - SIM_PROBE_ALLOWED_RISK_REASONS - SIM_PROBE_HARD_RISK_REASONS)
    if hard_reasons or unexpected_reasons:
        return {'allowed': False, 'reasons': hard_reasons + unexpected_reasons, 'execution_quality': execution_quality}
    return {
        'allowed': True,
        'reasons': ['sim_probe_entry_allowed'],
        'size_ratio': float(getattr(args, 'sim_probe_size_ratio', 0.2) or 0.2),
        'execution_quality': execution_quality,
    }


def build_probe_candidate(candidate: Candidate, size_ratio: float) -> Candidate:
    ratio = clamp(float(size_ratio or 0.0), 0.0, 1.0)
    base_quantity = max(float(candidate.quantity or 0.0), 0.0)
    last_price = max(float(candidate.last_price or 0.0), 0.0)
    quantity = max(base_quantity * ratio, 0.0)
    reasons = list(candidate.reasons or []) + [f'sim_probe_size_ratio={ratio:.2f}']
    min_notional = max(float(getattr(candidate, 'min_notional_usdt', 0.0) or 0.0), 0.0)
    max_notional = max(float(getattr(candidate, 'max_notional_usdt', 0.0) or 0.0), 0.0)
    if last_price > 0.0 and min_notional > 0.0 and quantity * last_price < min_notional:
        quantity = min_notional / last_price
        reasons.append(f'sim_probe_notional_floor={min_notional:.2f}')
    if last_price > 0.0 and max_notional > 0.0 and quantity * last_price > max_notional:
        quantity = max_notional / last_price
        reasons.append(f'sim_probe_notional_cap={max_notional:.2f}')
    position_size_pct = float(candidate.position_size_pct or 0.0) * (quantity / base_quantity) if base_quantity > 0.0 else 0.0
    return replace(
        candidate,
        quantity=quantity,
        position_size_pct=round(position_size_pct, 4),
        probe_entry=True,
        reasons=reasons,
    )


def query_order(client: Any, symbol: str, order_id: Optional[Any] = None, client_order_id: Optional[str] = None) -> Dict[str, Any]:
    if not hasattr(client, 'signed_get'):
        raise BinanceAPIError('client does not support signed_get for query_order')
    params: Dict[str, Any] = {'symbol': symbol}
    if order_id is not None:
        params['orderId'] = order_id
    if client_order_id:
        params['origClientOrderId'] = client_order_id
    if len(params) == 1:
        raise ValueError('query_order requires order_id or client_order_id')
    return client.signed_get('/fapi/v1/order', params=params)


def position_row_matches_symbol_side(row: Any, symbol: str, side: Any = POSITION_SIDE_LONG) -> bool:
    if not isinstance(row, dict) or str(row.get('symbol', '')).upper() != str(symbol or '').upper():
        return False
    amount = _to_float(row.get('positionAmt'))
    if abs(amount) <= 0:
        return False
    position_side = normalize_position_side(side)
    row_side = str(row.get('positionSide') or '').upper()
    if row_side == 'BOTH':
        return amount > 0 if position_side == POSITION_SIDE_LONG else amount < 0
    return normalize_position_side(row_side) == position_side


def position_side_from_exchange_position(row: Any, default: str = POSITION_SIDE_LONG) -> str:
    if not isinstance(row, dict):
        return normalize_position_side(default)
    row_side = str(row.get('positionSide') or '').upper()
    if row_side == 'BOTH':
        return POSITION_SIDE_SHORT if _to_float(row.get('positionAmt')) < 0 else POSITION_SIDE_LONG
    return normalize_position_side(row_side, default)


def recover_unknown_entry_order(client: Any, candidate: Candidate, quantity: float, quantity_precision: int, client_order_id: Optional[str] = None) -> Dict[str, Any]:
    position_side = normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG))
    if not client_order_id:
        raise BinanceAPIError('entry order status remained unknown: missing idempotency client order id')
    confirmed: Dict[str, Any] = {}
    last_error: Optional[Exception] = None
    for _ in range(5):
        try:
            confirmed = query_order(client, candidate.symbol, client_order_id=client_order_id)
            if confirmed:
                break
        except Exception as confirm_exc:
            last_error = confirm_exc
        time.sleep(0.4)
    if not confirmed:
        raise BinanceAPIError(f'entry order status remained unknown after idempotent lookup: {last_error or "empty response"}') from last_error
    order_id = confirmed.get('orderId')
    payload = {
        'symbol': candidate.symbol,
        'side': position_side,
        'position_key': build_position_key(candidate.symbol, position_side),
        'order_id': confirmed.get('orderId') or order_id,
        'client_order_id': confirmed.get('clientOrderId') or client_order_id,
        'status': confirmed.get('status'),
        'recovery': 'timeout_unknown_confirmed',
    }
    log_runtime_event('entry_order_recovered', payload)
    return confirmed


def ensure_symbol_margin_type(client: BinanceFuturesClient, symbol: str, margin_type: str = 'ISOLATED') -> Dict[str, Any]:
    return execution_ensure_symbol_margin_type(
        client,
        symbol,
        binance_api_error=BinanceAPIError,
        margin_type=margin_type,
    )


def place_initial_stop_with_retries(
    client: BinanceFuturesClient,
    candidate: Candidate,
    meta: SymbolMeta,
    args: argparse.Namespace,
    *,
    filled_quantity: float,
    position_side: str,
) -> Dict[str, Any]:
    return execution_place_initial_stop_with_retries(
        client=client,
        candidate=candidate,
        meta=meta,
        args=args,
        filled_quantity=filled_quantity,
        position_side=position_side,
        fetch_open_positions=fetch_open_positions,
        position_row_matches_symbol_side=position_row_matches_symbol_side,
        place_stop_market_order=place_stop_market_order,
        log_runtime_event=log_runtime_event,
        emit_notification=emit_notification,
        binance_api_error=BinanceAPIError,
        _to_float=_to_float,
        time_module=time,
    )


def place_live_trade(client: BinanceFuturesClient, candidate: Candidate, leverage: int, meta: SymbolMeta, args: argparse.Namespace) -> Dict[str, Any]:
    return execution_place_live_trade(
        client,
        candidate,
        leverage=leverage,
        meta=meta,
        args=args,
        binance_api_error=BinanceAPIError,
        ensure_symbol_margin_type_fn=ensure_symbol_margin_type,
        round_step=round_step,
        format_decimal=format_decimal,
        should_send_position_side=should_send_position_side,
        is_position_side_mode_error=is_position_side_mode_error,
        mark_one_way_position_mode=mark_one_way_position_mode,
        build_trade_management_plan=build_trade_management_plan,
        fetch_open_positions=fetch_open_positions,
        fetch_open_orders=fetch_open_orders,
        fetch_open_algo_orders=fetch_open_algo_orders,
        place_stop_market_order=place_stop_market_order,
        place_take_profit_market_order=place_take_profit_market_order,
        resolve_position_protection_status=resolve_position_protection_status,
        recover_unknown_entry_order=recover_unknown_entry_order,
        query_order=query_order,
        log_runtime_event=log_runtime_event,
        emit_notification=emit_notification,
        normalize_position_side=normalize_position_side,
        build_position_key=build_position_key,
        position_row_matches_symbol_side=position_row_matches_symbol_side,
        _to_float=_to_float,
        compute_execution_quality_size_adjustment=compute_execution_quality_size_adjustment,
        asdict=asdict,
        position_side_long=POSITION_SIDE_LONG,
        time_module=time,
    )


def monitor_live_trade(client: Any, symbol: str, meta: SymbolMeta, args: argparse.Namespace, trade: Dict[str, Any], store: RuntimeStateStore) -> Dict[str, Any]:
    try:
        return execution_monitor_live_trade(
            client,
            symbol,
            meta,
            args,
            trade,
            store,
            trade_management_plan_type=TradeManagementPlan,
            trade_management_state_type=TradeManagementState,
            build_trade_management_state_from_position=build_trade_management_state_from_position,
            position_side_long=POSITION_SIDE_LONG,
            position_side_short=POSITION_SIDE_SHORT,
            binance_api_error=BinanceAPIError,
            _to_float=_to_float,
            normalize_position_side=normalize_position_side,
            position_side_to_trade_side=position_side_to_trade_side,
            build_position_key=build_position_key,
            get_position_by_symbol_side=get_position_by_symbol_side,
            build_trade_analytics_snapshot=build_trade_analytics_snapshot,
            upsert_position_record=upsert_position_record,
            materialize_positions_state=materialize_positions_state,
            asdict=asdict,
            log_runtime_event=log_runtime_event,
            emit_notification=emit_notification,
            fetch_klines=fetch_klines,
            extract_closes=extract_closes,
            extract_highs=extract_highs,
            extract_lows=extract_lows,
            resolve_monitor_current_price=resolve_monitor_current_price,
            evaluate_management_actions=evaluate_management_actions,
            update_trade_progress_metrics=update_trade_progress_metrics,
            apply_management_action=apply_management_action,
            resolve_reduce_order_exit_price=resolve_reduce_order_exit_price,
            compute_trade_realized_r_increment=compute_trade_realized_r_increment,
            score_to_decile_label=score_to_decile_label,
            resolve_trigger_class=resolve_trigger_class,
            utc_now=_utc_now,
            isoformat_utc=_isoformat_utc,
            time_module=time,
            record_runtime_heartbeat=record_runtime_heartbeat,
        )
    except TypeError as exc:
        if 'TradeManagementPlan.__init__' not in str(exc):
            raise
        positions_state = store.load_json('positions', {})
        if not isinstance(positions_state, dict):
            positions_state = {}
        trade_side = normalize_position_side((trade or {}).get('side') or POSITION_SIDE_LONG)
        position_key, tracked = get_position_by_symbol_side(positions_state, symbol, trade_side)
        tracked = dict(tracked or {})
        tracked['symbol'] = symbol
        tracked['side'] = trade_side
        tracked['position_key'] = position_key
        tracked['status'] = 'recovery_pending'
        tracked['protection_status'] = 'pending'
        tracked['recovery_incomplete'] = True
        tracked['protected_recovery_pending'] = False
        tracked['recovery_reason'] = 'incomplete_trade_management_plan'
        positions_state, _ = upsert_position_record(positions_state, tracked, key=position_key)
        store.save_json('positions', positions_state)
        return {
            'ok': False,
            'status': 'recovery_pending',
            'reason': 'incomplete_trade_management_plan',
            'error': str(exc),
            'symbol': symbol,
        }


def start_trade_monitor_thread(client: Any, symbol: str, meta: SymbolMeta, args: argparse.Namespace, trade: Dict[str, Any], store: RuntimeStateStore) -> threading.Thread:
    return execution_start_trade_monitor_thread(
        client,
        symbol,
        meta,
        args,
        trade,
        store,
        monitor_live_trade_fn=monitor_live_trade,
        thread_factory=threading.Thread,
    )


def resolve_auto_loop_book_ticker_symbols(client: BinanceFuturesClient, args: argparse.Namespace, store: Optional[RuntimeStateStore] = None) -> List[str]:
    top_gainers = int(getattr(args, 'top_gainers', 20) or 20)
    top_losers = int(getattr(args, 'top_losers', top_gainers) or 0)
    try:
        metas = filter_strategy_websocket_symbol_meta(fetch_exchange_meta(client))
    except Exception:
        metas = {}
    allowed_symbols = list(metas.keys())
    if not allowed_symbols:
        allowed_symbols = sorted(fetch_public_exchange_symbol_set())
    allowed_symbol_set = set(allowed_symbols)

    def _normalize_allowed_symbol_list(raw_symbols: Any) -> List[str]:
        normalized_symbols: List[str] = []
        for raw_symbol in list(raw_symbols or []):
            normalized = normalize_symbol(raw_symbol)
            if not normalized or not is_strategy_websocket_symbol_allowed(normalized):
                continue
            if allowed_symbol_set and normalized not in allowed_symbol_set:
                continue
            normalized_symbols.append(normalized)
        return list(dict.fromkeys(normalized_symbols))

    try:
        square_symbols = _normalize_allowed_symbol_list(load_manual_square_symbols(args))
    except Exception:
        square_symbols = []

    try:
        external_signal_payload = load_external_signal_payload(args)
    except Exception:
        external_signal_payload = {}
    external_signal_symbols = _normalize_allowed_symbol_list((external_signal_payload or {}).get('symbols', []))
    external_signal_map_symbols = _normalize_allowed_symbol_list(((external_signal_payload or {}).get('signal_map') or {}).keys())

    try:
        if store is None:
            tickers = fetch_tickers(client)
        else:
            tickers = resolve_scan_tickers(client, store, args)
    except Exception:
        tickers = []
    merged_payload = merged_candidate_symbols(
        square_symbols=list(dict.fromkeys(square_symbols + external_signal_symbols + external_signal_map_symbols)),
        tickers=tickers,
        allowed_symbols=allowed_symbols,
        top_gainers=top_gainers,
        top_losers=top_losers,
    )
    merged_symbols = merged_payload[0]
    symbols = _normalize_allowed_symbol_list(merged_symbols)
    if symbols:
        return symbols
    fallback_symbols = _normalize_allowed_symbol_list(square_symbols + external_signal_symbols + external_signal_map_symbols)
    if fallback_symbols:
        return fallback_symbols
    return ['BTCUSDT']


def persist_live_open_position(store: RuntimeStateStore, candidate: Any, live_execution: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    positions_state = store.load_json('positions', {})
    if not isinstance(positions_state, dict):
        positions_state = {}
    live_side = normalize_position_side(live_execution.get('side') or 'LONG')
    entry_feedback = live_execution.get('entry_order_feedback', {})
    if not isinstance(entry_feedback, dict):
        entry_feedback = {}
    trade_plan = live_execution.get('trade_management_plan', {})
    if not isinstance(trade_plan, dict):
        trade_plan = {}
    stop_order = live_execution.get('stop_order', {})
    if not isinstance(stop_order, dict):
        stop_order = {}
    protection_check = live_execution.get('protection_check', {})
    if not isinstance(protection_check, dict):
        protection_check = {}
    symbol = str(getattr(candidate, 'symbol', '') or live_execution.get('symbol') or '').upper()
    selected_score = round(float(getattr(candidate, 'score', 0.0) or 0.0), 4)
    position_payload = {
        'symbol': symbol,
        'side': live_side,
        'status': 'open',
        'quantity': trade_plan.get('quantity', live_execution.get('filled_quantity', getattr(candidate, 'quantity', 0.0))),
        'filled_quantity': live_execution.get('filled_quantity', trade_plan.get('quantity', getattr(candidate, 'quantity', 0.0))),
        'entry_price': live_execution.get('entry_price'),
        'stop_price': float(getattr(candidate, 'stop_price')),
        'stop_order_id': stop_order.get('orderId'),
        'protection_status': protection_check.get('status'),
        'entry_order_id': entry_feedback.get('order_id'),
        'entry_client_order_id': entry_feedback.get('client_order_id'),
        'entry_order_status': entry_feedback.get('status'),
        'entry_cum_quote': entry_feedback.get('cum_quote'),
        'entry_update_time': entry_feedback.get('update_time'),
        'margin_type': live_execution.get('margin_type'),
        'margin_type_check': live_execution.get('margin_type_check', {}),
        'leverage': live_execution.get('leverage'),
        'leverage_check': live_execution.get('leverage_check', {}),
        'trade_management_plan': trade_plan,
        'portfolio_narrative_bucket': getattr(candidate, 'portfolio_narrative_bucket', ''),
        'portfolio_correlation_group': getattr(candidate, 'portfolio_correlation_group', ''),
        'opened_at': _isoformat_utc(_utc_now()),
        'first_1r_at': None,
        'realized_r': 0.0,
        'selected_score': selected_score,
        'selected_state': str(getattr(candidate, 'state', '') or ''),
        'selected_alert_tier': str(getattr(candidate, 'alert_tier', '') or ''),
        'state': str(getattr(candidate, 'state', '') or ''),
        'alert_tier': str(getattr(candidate, 'alert_tier', '') or ''),
        'candidate_stage': str(getattr(candidate, 'candidate_stage', '') or ''),
        'trigger_class': resolve_trigger_class(candidate),
        'score_decile': score_to_decile_label(selected_score),
        'market_regime_label': str(getattr(candidate, 'market_regime_label', getattr(candidate, 'regime_label', '')) or ''),
        'market_regime_multiplier': round(float(getattr(candidate, 'regime_multiplier', 0.0) or 0.0), 4),
        'setup_ready': bool(getattr(candidate, 'setup_ready', False)),
        'trigger_fired': bool(getattr(candidate, 'trigger_fired', False)),
    }
    positions_state, position_key = upsert_position_record(
        positions_state,
        position_payload,
        key=build_position_key(symbol, live_side),
    )
    store.save_json('positions', positions_state)
    return positions_state, position_key


def append_buy_fill_confirmed_event(store: RuntimeStateStore, symbol: str, positions_state: Dict[str, Any], position_key: str) -> Dict[str, Any]:
    position = positions_state[position_key]
    user_data_stream = position.get('user_data_stream', {})
    if not isinstance(user_data_stream, dict):
        user_data_stream = {}
    payload = {
        'symbol': symbol,
        'entry_price': position.get('entry_price'),
        'side': position.get('side'),
        'position_key': position.get('position_key'),
        'quantity': position['quantity'],
        'filled_quantity': position.get('filled_quantity'),
        'stop_price': position['stop_price'],
        'stop_order_id': position.get('stop_order_id'),
        'protection_status': position.get('protection_status'),
        'entry_order_id': position.get('entry_order_id'),
        'entry_client_order_id': position.get('entry_client_order_id'),
        'entry_order_status': position.get('entry_order_status'),
        'entry_cum_quote': position.get('entry_cum_quote'),
        'entry_update_time': position.get('entry_update_time'),
        'monitor_mode': 'background_thread',
        'monitor_thread_name': position['monitor_thread_name'],
        'listen_key': user_data_stream.get('listen_key'),
    }
    store.append_event('entry_filled', dict(payload))
    legacy_event = store.append_event('buy_fill_confirmed', dict(payload))
    return legacy_event


def build_auto_loop_state_payload(
    *,
    args: argparse.Namespace,
    state: str,
    candidate: Optional[Any] = None,
    reason: str = '',
    risk_guard: Optional[Dict[str, Any]] = None,
    reconcile: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    payload: Dict[str, Any] = {
        'updated_at': now,
        'profile': getattr(args, 'profile', 'default'),
        'state': state,
        'reason': reason,
        'live_requested': bool(getattr(args, 'live', False)),
        'scan_only': bool(getattr(args, 'scan_only', False)),
    }
    if candidate is not None:
        side = getattr(candidate, 'side', getattr(candidate, 'position_side', '')) or ''
        payload['active_candidate'] = {
            'symbol': getattr(candidate, 'symbol', ''),
            'side': side,
            'position_key': f"{getattr(candidate, 'symbol', '')}:{side}" if getattr(candidate, 'symbol', '') and side else '',
            'setup_ready': bool(getattr(candidate, 'setup_ready', False)),
            'trigger_fired': bool(getattr(candidate, 'trigger_fired', False)),
            'candidate_stage': getattr(candidate, 'candidate_stage', getattr(candidate, 'state', '')),
            'score': round(float(getattr(candidate, 'score', 0.0) or 0.0), 4),
            'alert_tier': getattr(candidate, 'alert_tier', ''),
            'liquidity_grade': getattr(candidate, 'liquidity_grade', ''),
            'setup_missing': list(getattr(candidate, 'setup_missing', []) or []),
            'trigger_missing': list(getattr(candidate, 'trigger_missing', []) or []),
            'trade_missing': list(getattr(candidate, 'trade_missing', []) or []),
            'expected_slippage_pct': round(float(getattr(candidate, 'expected_slippage_pct', 0.0) or 0.0), 6),
            'book_depth_fill_ratio': round(float(getattr(candidate, 'book_depth_fill_ratio', 0.0) or 0.0), 6),
            'spread_bps': round(float(getattr(candidate, 'spread_bps', 0.0) or 0.0), 4),
        }
    if risk_guard is not None:
        payload['risk_guard'] = {
            'allowed': bool(risk_guard.get('allowed', True)),
            'reasons': list(risk_guard.get('reasons', [])),
            'cooldown_until': risk_guard.get('cooldown_until'),
        }
    if reconcile is not None:
        payload['reconcile'] = {
            'ok': bool(reconcile.get('ok', True)),
            'orphan_positions': list(reconcile.get('orphan_positions', []) or []),
            'positions_missing_protection': list(reconcile.get('positions_missing_protection', []) or []),
        }
    if extra:
        payload.update(extra)
    return payload


def persist_auto_loop_state(
    store: RuntimeStateStore,
    *,
    args: argparse.Namespace,
    state: str,
    candidate: Optional[Any] = None,
    reason: str = '',
    risk_guard: Optional[Dict[str, Any]] = None,
    reconcile: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = build_auto_loop_state_payload(
        args=args,
        state=state,
        candidate=candidate,
        reason=reason,
        risk_guard=risk_guard,
        reconcile=reconcile,
        extra=extra,
    )
    try:
        store.save_json('auto_loop_state', payload)
    except Exception:
        pass
    return payload


def choose_auto_loop_state_for_candidate(candidate: Optional[Any], risk_guard: Optional[Dict[str, Any]] = None) -> str:
    if candidate is None:
        return 'SCAN'
    if risk_guard is not None and not bool(risk_guard.get('allowed', True)):
        return 'COOLDOWN' if risk_guard.get('cooldown_until') else 'SCAN'
    if bool(getattr(candidate, 'trigger_fired', False)):
        return 'EXECUTION_GATE'
    if bool(getattr(candidate, 'setup_ready', False)):
        return 'WAIT_TRIGGER'
    return 'SCAN'


def find_resumable_auto_loop_position(store: RuntimeStateStore) -> Optional[Dict[str, Any]]:
    positions = store.load_json('positions', {})
    if not isinstance(positions, dict):
        return None
    for key, position in positions.items():
        if not isinstance(position, dict):
            continue
        status = str(position.get('status') or '').lower()
        remaining_quantity = _to_float(position.get('remaining_quantity') or position.get('quantity'), default=0.0)
        protection_status = str(position.get('protection_status') or '').lower()
        has_protection = protection_status in {'protected', 'active', 'repaired'} or bool(position.get('active_stop_order'))
        if status in {'monitoring', 'open', 'protected_recovery'} and remaining_quantity > 0 and has_protection:
            payload = dict(position)
            payload.setdefault('position_key', str(key))
            payload.setdefault('symbol', str(position.get('symbol') or str(key).split(':', 1)[0]).upper())
            return payload
    return None


def build_auto_loop_resume_payload(position: Dict[str, Any], state: str = 'MANAGING') -> Dict[str, Any]:
    return {
        'state': state,
        'position_key': position.get('position_key'),
        'symbol': position.get('symbol'),
        'side': position.get('side') or position.get('position_side'),
        'status': position.get('status'),
        'remaining_quantity': _to_float(position.get('remaining_quantity') or position.get('quantity'), default=0.0),
        'protection_status': position.get('protection_status'),
    }


def run_loop(client: Any, args: argparse.Namespace) -> Dict[str, Any]:
    store = get_runtime_state_store(args)
    scanner_timeout_seconds = float(getattr(args, 'scanner_timeout_seconds', 45.0) or 45.0)
    execution_timeout_seconds = float(getattr(args, 'execution_timeout_seconds', 30.0) or 30.0)
    runtime_ttl_seconds = float(getattr(args, 'runtime_ttl_seconds', 900.0) or 900.0)
    runtime_queue_maxsize = int(getattr(args, 'runtime_queue_maxsize', 128) or 128)
    runtime_task_queues = build_runtime_task_queues(runtime_queue_maxsize)
    cleanup_symbol_runtime_state_ttl(store, ttl_seconds=runtime_ttl_seconds)
    record_runtime_heartbeat(store, component='scanner', status='starting', blocked_reason='', queue_depth=runtime_task_queues['scanner'].qsize(), queue_maxsize=runtime_task_queues['scanner'].maxsize)
    record_runtime_heartbeat(store, component='execution', status='idle', blocked_reason='', queue_depth=runtime_task_queues['execution'].qsize(), queue_maxsize=runtime_task_queues['execution'].maxsize)
    record_runtime_heartbeat(store, component='manager', status='starting', blocked_reason='', queue_depth=runtime_task_queues['manager'].qsize(), queue_maxsize=runtime_task_queues['manager'].maxsize)
    binance_simulated_trading = is_binance_simulated_trading(args)
    execution_exchange = execution_exchange_label(args)

    def persist_cycle_snapshot(cycle_payload: Dict[str, Any]) -> None:
        try:
            store.save_json('last_cycle', {
                'updated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'profile': getattr(args, 'profile', 'default'),
                'live_requested': bool(getattr(args, 'live', False)),
                'execution_exchange': execution_exchange,
                'scan_only': bool(getattr(args, 'scan_only', False)),
                'auto_loop': bool(getattr(args, 'auto_loop', False)),
                'cycle': cycle_payload,
            })
        except Exception:
            pass

    open_circuit = load_open_scanner_rest_circuit(store)
    if open_circuit is not None:
        cycle = {
            'scan': {'ok': False, 'blocked_reason': 'binance_ip_ban_circuit_open', 'circuit_breaker': open_circuit},
            'blocked_reason': 'binance_ip_ban_circuit_open',
        }
        record_runtime_heartbeat(store, component='scanner', status='blocked', blocked_reason='binance_ip_ban_circuit_open', extra=cycle['scan'])
        append_runtime_event(store, 'scanner_blocked', {'blocked_reason': 'binance_ip_ban_circuit_open', 'circuit_breaker': open_circuit})
        persist_cycle_snapshot(cycle)
        return {'ok': True, 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'blocked_reason': 'binance_ip_ban_circuit_open'}}

    if binance_simulated_trading:
        reconcile = {
            'ok': True,
            'skipped': True,
            'skip_reason': 'binance_simulated_trading',
            'orphan_positions': [],
            'positions_missing_protection': [],
            'protection_repairs': [],
        }
    else:
        try:
            try:
                reconcile = reconcile_runtime_state(
                    client,
                    store,
                    halt_on_orphan_position=getattr(args, 'halt_on_orphan_position', False),
                    repair_missing_protection_enabled=getattr(args, 'repair_missing_protection', True),
                    args=args,
                )
            except TypeError as exc:
                if "unexpected keyword argument 'args'" not in str(exc):
                    raise
                reconcile = reconcile_runtime_state(
                    client,
                    store,
                    halt_on_orphan_position=getattr(args, 'halt_on_orphan_position', False),
                    repair_missing_protection_enabled=getattr(args, 'repair_missing_protection', True),
                )
        except BinanceAPIError as exc:
            missing_api_secret = str(exc) == 'api_secret is required for signed requests'
            if missing_api_secret and not getattr(args, 'live', False) and not getattr(args, 'reconcile_only', False):
                reconcile = {
                    'ok': True,
                    'skipped': True,
                    'skip_reason': 'missing_api_secret',
                    'orphan_positions': [],
                    'positions_missing_protection': [],
                    'protection_repairs': [],
                }
            elif is_binance_ip_ban_error(exc):
                message = str(exc)
                retry_after_ms = extract_binance_ip_ban_until_ms(message)
                circuit_payload = build_scanner_rest_circuit_payload(
                    reason='binance_ip_ban',
                    retry_after_ms=retry_after_ms,
                    error=message,
                    fallback_cooldown_seconds=float(getattr(args, 'scanner_rest_ban_cooldown_seconds', 180.0) or 0.0),
                )
                store.save_json('scanner_rest_circuit_breaker', circuit_payload)
                cycle = {
                    'reconcile': {'ok': False, 'blocked_reason': 'binance_ip_ban', 'error': message, 'retry_after_ms': retry_after_ms},
                    'scan': {'ok': False, 'blocked_reason': 'binance_ip_ban', 'circuit_breaker': circuit_payload},
                    'blocked_reason': 'binance_ip_ban',
                }
                record_runtime_heartbeat(store, component='scanner', status='blocked', blocked_reason='binance_ip_ban', extra=cycle['scan'])
                append_runtime_event(store, 'scanner_blocked', {'blocked_reason': 'binance_ip_ban', 'error': message, 'retry_after_ms': retry_after_ms})
                persist_cycle_snapshot(cycle)
                return {'ok': True, 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'blocked_reason': 'binance_ip_ban'}}
            else:
                raise
    if not binance_simulated_trading and not getattr(args, 'scan_only', False) and reconcile.get('skip_reason') != 'missing_api_secret':
        order_reconcile = reconcile_positions_and_orders(
            client,
            store,
            max_open_positions=getattr(args, 'max_open_positions', None),
            max_short_positions=(getattr(args, 'max_short_positions', None) or None),
        )
        reconcile['order_reconcile'] = order_reconcile
        reconcile['ok'] = bool(reconcile.get('ok', True)) and bool(order_reconcile.get('ok', True))
    if getattr(args, 'reconcile_only', False):
        reconcile_payload = {'reconcile': reconcile}
        persist_cycle_snapshot(reconcile_payload)
        return {'mode': 'reconcile_only', 'ok': bool(reconcile.get('ok', True)), 'reconcile': reconcile, 'cycles': []}
    result: Dict[str, Any] = {'ok': bool(reconcile.get('ok', True)), 'cycles': []}
    cycle: Dict[str, Any] = {'reconcile': reconcile}
    result['cycles'].append(cycle)
    if getattr(args, 'auto_loop', False):
        book_ticker_config = build_auto_loop_book_ticker_websocket_monitor_config(client, args, store=store)
        user_data_stream_config = build_auto_loop_user_data_stream_monitor_config(args)
        book_ticker_result = run_auto_loop_book_ticker_websocket_monitor_core(
            store=store,
            config=book_ticker_config,
        )
        cycle['book_ticker_websocket'] = dict(book_ticker_result.get('summary', {}), health=book_ticker_result.get('health', {}))
        if cycle['book_ticker_websocket'].get('status') == 'unavailable' and not cycle['book_ticker_websocket'].get('health'):
            cycle['book_ticker_websocket'].pop('health', None)
        user_data_stream_result = run_auto_loop_user_data_stream_monitor(
            client=client,
            store=store,
            args=args,
            config=user_data_stream_config,
        )
        uds_monitor = user_data_stream_result.get('monitor')
        if uds_monitor is not None:
            cycle['user_data_stream_monitor'] = uds_monitor
        alert_payload = user_data_stream_result.get('alert')
        if alert_payload is not None:
            cycle['user_data_stream_alert'] = alert_payload
        resumable_position = find_resumable_auto_loop_position(store)
        if resumable_position is not None and getattr(args, 'live', False):
            resume_payload = build_auto_loop_resume_payload(resumable_position, state='MANAGING')
            cycle['resident_resume'] = resume_payload
            cycle['auto_loop_state'] = persist_auto_loop_state(
                store,
                args=args,
                state='MANAGING',
                reason='resume_open_position_continue_scanning',
                reconcile=reconcile,
                extra=resume_payload,
            )
    if reconcile.get('positions_missing_protection') and reconcile.get('ok', True):
        symbols = reconcile.get('positions_missing_protection', [])
        halt_reason = f"missing_protection:{','.join(symbols)}"
        risk_state = load_risk_state(store)
        risk_state['halted'] = True
        risk_state['halt_reason'] = halt_reason
        store.save_json('risk_state', risk_state)
        emit_notification(args, 'protection_missing', {
            'halt_reason': halt_reason,
            'positions_missing_protection': symbols,
            'orphan_positions': reconcile.get('orphan_positions', []),
            'profile': getattr(args, 'profile', 'default'),
        })
    if not reconcile.get('ok', True):
        halt_reason = f"orphan_positions:{','.join(reconcile.get('orphan_positions', []))}" if reconcile.get('orphan_positions') else 'reconcile_failed'
        emit_notification(args, 'strategy_halted', {
            'halt_reason': halt_reason,
            'orphan_positions': reconcile.get('orphan_positions', []),
            'positions_missing_protection': reconcile.get('positions_missing_protection', []),
            'profile': getattr(args, 'profile', 'default'),
        })
        persist_cycle_snapshot(cycle)
        return result
    scan_call_result = run_with_deadman_timeout(
        run_scan_once,
        client,
        args,
        timeout_seconds=scanner_timeout_seconds,
        store=store,
        component='scanner',
        operation='scan_cycle',
    )
    if isinstance(scan_call_result, dict) and scan_call_result.get('reason') == 'deadman_timeout':
        cycle['scan'] = {'ok': False, 'blocked_reason': 'scanner_timeout', 'deadman': scan_call_result}
        cycle['blocked_reason'] = 'scanner_timeout'
        persist_auto_loop_state(store, args=args, state='SCAN', reason='scanner_timeout', reconcile=reconcile, extra={'blocked_reason': 'scanner_timeout'})
        persist_cycle_snapshot(cycle)
        return result
    scan_result, best_candidate, meta_map = scan_call_result
    record_runtime_heartbeat(store, component='scanner', status='healthy', blocked_reason='', extra={'candidate_found': best_candidate is not None})
    cycle['scan'] = scan_result
    risk_state = apply_reconcile_close_risk_state_updates(store, reconcile, args)
    if best_candidate is None:
        cycle['risk_guard'] = evaluate_risk_guards(
            risk_state=risk_state,
            daily_max_loss_usdt=float(getattr(args, 'daily_max_loss_usdt', 0.0) or 0.0),
            max_consecutive_losses=int(getattr(args, 'max_consecutive_losses', 0) or 0),
            symbol_cooldown_minutes=int(getattr(args, 'symbol_cooldown_minutes', 0) or 0),
        )
        persist_auto_loop_state(
            store,
            args=args,
            state='SCAN',
            reason='no_candidate',
            risk_guard=cycle['risk_guard'],
            reconcile=reconcile,
        )
        persist_cycle_snapshot(cycle)
        return result
    append_candidate_selected_event(
        store,
        best_candidate,
        regime_payload=scan_result.get('market_regime', {}) if isinstance(scan_result, dict) else {},
        extra={
            'profile': getattr(args, 'profile', 'default'),
            'live_requested': bool(getattr(args, 'live', False)),
            'scan_only': bool(getattr(args, 'scan_only', False)),
            'execution_exchange': execution_exchange,
        },
    )
    risk_guard = evaluate_risk_guards(
        symbol=best_candidate.symbol,
        risk_state=risk_state,
        candidate=best_candidate,
        daily_max_loss_usdt=float(getattr(args, 'daily_max_loss_usdt', 0.0) or 0.0),
        max_consecutive_losses=int(getattr(args, 'max_consecutive_losses', 0) or 0),
        symbol_cooldown_minutes=int(getattr(args, 'symbol_cooldown_minutes', 0) or 0),
        base_risk_usdt=float(getattr(args, 'risk_usdt', 0.0) or 0.0),
        gross_heat_cap_r=float(getattr(args, 'gross_heat_cap_r', 0.0) or 0.0),
        same_theme_heat_cap_r=float(getattr(args, 'same_theme_heat_cap_r', 0.0) or 0.0),
        same_correlation_heat_cap_r=float(getattr(args, 'same_correlation_heat_cap_r', 0.0) or 0.0),
        portfolio_narrative_bucket=getattr(best_candidate, 'portfolio_narrative_bucket', ''),
        portfolio_correlation_group=getattr(best_candidate, 'portfolio_correlation_group', ''),
        symbol_daily_loss_limit_usdt=float(getattr(args, 'symbol_daily_loss_limit_usdt', 1.2) or 0.0),
        symbol_rolling_loss_limit_usdt=float(getattr(args, 'symbol_rolling_loss_limit_usdt', 2.0) or 0.0),
        symbol_rolling_window_days=int(getattr(args, 'symbol_rolling_window_days', 5) or 5),
        max_consecutive_losses_per_symbol=int(getattr(args, 'max_consecutive_losses_per_symbol', 1) or 0),
        after_symbol_loss_cooldown_minutes=int(getattr(args, 'after_symbol_loss_cooldown_minutes', 180) or 0),
        loss_deweighted_score_penalty=float(getattr(args, 'loss_deweighted_score_penalty', 12.0) or 0.0),
        score_threshold=float(getattr(args, 'score_threshold', 0.0) or 0.0),
        aggressive_flip_cooldown_minutes=int(getattr(args, 'opposite_side_flip_cooldown_minutes', 0) or 0),
    )
    if getattr(args, 'live', False) and not binance_simulated_trading:
        open_positions = fetch_open_positions(client)
        pending_opening_orders = list(fetch_open_orders(client) or []) + list(fetch_open_algo_orders(client) or [])
        projected_open_positions = list(projected_position_keys(open_positions, pending_opening_orders))
    else:
        open_positions = []
        pending_opening_orders = []
        projected_open_positions = []
    portfolio_risk_guard = evaluate_portfolio_risk_guards(
        open_positions=list(open_positions) + opening_orders_as_risk_positions(pending_opening_orders, float(getattr(best_candidate, 'last_price', 0.0) or 0.0)),
        candidate=best_candidate,
        max_long_positions=int(getattr(args, 'max_long_positions', 0) or 0),
        max_short_positions=int(getattr(args, 'max_short_positions', 0) or 0),
        max_net_exposure_usdt=float(getattr(args, 'max_net_exposure_usdt', 0.0) or 0.0),
        max_gross_exposure_usdt=float(getattr(args, 'max_gross_exposure_usdt', 0.0) or 0.0),
        per_symbol_single_side_only=bool(getattr(args, 'per_symbol_single_side_only', True)),
        opposite_side_flip_cooldown_minutes=int(getattr(args, 'opposite_side_flip_cooldown_minutes', 0) or 0),
    )
    risk_guard = {
        'allowed': bool(risk_guard.get('allowed', True)) and bool(portfolio_risk_guard.get('allowed', True)),
        'reasons': list(risk_guard.get('reasons', [])) + list(portfolio_risk_guard.get('reasons', [])),
        'cooldown_until': risk_guard.get('cooldown_until'),
        'normalized_risk_state': risk_guard.get('normalized_risk_state', default_risk_state()),
        'portfolio': portfolio_risk_guard,
    }
    cycle['risk_guard'] = risk_guard
    candidate_loop_state = choose_auto_loop_state_for_candidate(best_candidate, risk_guard)
    cycle['auto_loop_state'] = persist_auto_loop_state(
        store,
        args=args,
        state=candidate_loop_state,
        candidate=best_candidate,
        reason='candidate_selected',
        risk_guard=risk_guard,
        reconcile=reconcile,
    )
    scan_funnel = cycle.get('scan', {}).get('funnel')
    if isinstance(scan_funnel, dict):
        scan_funnel['selected_risk_allowed_count'] = 1 if risk_guard['allowed'] else 0
        scan_funnel['order_submitted_count'] = 0
    cycle['scan_only'] = bool(getattr(args, 'scan_only', False))
    cycle['live_requested'] = bool(getattr(args, 'live', False))
    cycle['execution_exchange'] = execution_exchange
    if not getattr(args, 'live', False):
        persist_cycle_snapshot(cycle)
        return result
    websocket_gate_required = bool(getattr(args, 'require_book_ticker_ws', True))
    if websocket_gate_required and not isinstance(cycle.get('book_ticker_websocket'), dict):
        book_ticker_result = run_auto_loop_book_ticker_websocket_monitor(
            client=client,
            store=store,
            args=args,
        )
        cycle['book_ticker_websocket'] = dict(book_ticker_result.get('summary', {}), health=book_ticker_result.get('health', {}))
        if cycle['book_ticker_websocket'].get('status') == 'unavailable' and not cycle['book_ticker_websocket'].get('health'):
            cycle['book_ticker_websocket'].pop('health', None)
    book_ticker_gate = cycle.get('book_ticker_websocket') if isinstance(cycle.get('book_ticker_websocket'), dict) else None
    # Real strategy candidates retain the fail-closed safety gate.  Explicitly
    # disabled gating remains compatible with lightweight integration adapters
    # that supply candidate-like objects rather than the production model.
    websocket_gate_enforced = websocket_gate_required or isinstance(best_candidate, Candidate)
    if websocket_gate_enforced and book_ticker_gate and book_ticker_gate.get('status') == 'unavailable':
        websocket_reason = str(book_ticker_gate.get('reason') or 'unknown')
        cycle['live_skipped_due_to_websocket_gate'] = [f'book_ticker_websocket_unavailable:{websocket_reason}']
        append_candidate_rejected_event(store, best_candidate, cycle['live_skipped_due_to_websocket_gate'])
        record_runtime_heartbeat(store, component='scanner', status='blocked', blocked_reason=f'websocket_gate:{websocket_reason}')
        persist_cycle_snapshot(cycle)
        return result
    if websocket_gate_required and book_ticker_gate:
        gate_health = book_ticker_gate.get('health') if isinstance(book_ticker_gate.get('health'), dict) else book_ticker_gate
        stored_gate_health = store.load_json('book_ticker_ws_status', {})
        gate_health = choose_latest_websocket_health(gate_health, stored_gate_health)
        if isinstance(gate_health, dict) and gate_health is not book_ticker_gate:
            cycle['book_ticker_websocket'] = dict(book_ticker_gate, health=gate_health)
        websocket_freshness = evaluate_websocket_freshness(
            gate_health,
            max_age_seconds=float(getattr(args, 'book_ticker_ws_stale_seconds', 30.0) or 30.0),
            require_messages=bool(getattr(args, 'require_book_ticker_ws_messages', True)),
        )
        cycle['book_ticker_websocket_freshness'] = websocket_freshness
        cycle['execution_degradation_mode'] = websocket_freshness.get('execution_degradation_mode', 'none')
        cycle['websocket_state'] = websocket_freshness.get('state')
        cycle['websocket_gate_action'] = websocket_freshness.get('websocket_gate_action')
        cycle['websocket_degradation_applied'] = bool(websocket_freshness.get('websocket_degradation_applied', False))
        if websocket_freshness.get('state') == 'dead':
            stale_reason = str(websocket_freshness.get('reason') or 'stale_websocket')
            cycle['live_skipped_due_to_websocket_gate'] = [f'book_ticker_websocket_hard_veto:{stale_reason}']
            cycle['final_execution_gate_action'] = 'veto'
            cycle['final_blocking_reason'] = f'websocket:{stale_reason}'
            append_candidate_rejected_event(store, best_candidate, cycle['live_skipped_due_to_websocket_gate'])
            record_runtime_heartbeat(store, component='scanner', status='blocked', blocked_reason=f'websocket_stale:{stale_reason}', extra=websocket_freshness)
            persist_cycle_snapshot(cycle)
            return result
        if websocket_freshness.get('execution_degradation_mode') in {'reduced_aggression', 'maker_only'}:
            best_candidate = build_websocket_degraded_candidate(best_candidate, websocket_freshness)
            cycle['delayed_execution_queue'] = {'kept_alive': True, 'reason': websocket_freshness.get('reason'), 'websocket_state': websocket_freshness.get('state'), 'stale_guard_decision_reason': websocket_freshness.get('stale_guard_decision_reason')}
            cycle['final_execution_gate_action'] = 'maker_confirm' if websocket_freshness.get('execution_degradation_mode') == 'maker_only' else 'watch'
            record_runtime_heartbeat(store, component='scanner', status='degraded', blocked_reason='', extra=websocket_freshness)
    max_open_positions = int(getattr(args, 'max_open_positions', 1) or 1)
    if len(projected_open_positions) >= max_open_positions:
        cycle['live_skipped_due_to_existing_positions'] = open_positions
        cycle['live_skipped_due_to_projected_positions'] = projected_open_positions
        cycle['pending_opening_orders_count'] = len([order for order in pending_opening_orders if isinstance(order, dict) and is_opening_order(order)])
        append_candidate_rejected_event(store, best_candidate, ['max_open_positions_reached'], {'open_positions': open_positions, 'projected_open_positions': projected_open_positions})
        persist_cycle_snapshot(cycle)
        return result
    probe_entry: Dict[str, Any] = {'allowed': False, 'reasons': ['not_evaluated']}
    if not risk_guard['allowed']:
        risk_reject_detail = {
            'symbol': best_candidate.symbol,
            'side': getattr(best_candidate, 'side', getattr(best_candidate, 'position_side', '')),
            'score': round(float(getattr(best_candidate, 'score', 0.0) or 0.0), 4),
            'candidate_stage': getattr(best_candidate, 'candidate_stage', ''),
            'risk_reasons': list(risk_guard.get('reasons', [])),
            'setup_ready': bool(getattr(best_candidate, 'setup_ready', False)),
            'trigger_fired': bool(getattr(best_candidate, 'trigger_fired', False)),
            'cvd_delta': round(float(getattr(best_candidate, 'cvd_delta', 0.0) or 0.0), 4),
            'cvd_zscore': round(float(getattr(best_candidate, 'cvd_zscore', 0.0) or 0.0), 4),
            'oi_change_pct_5m': round(float(getattr(best_candidate, 'oi_change_pct_5m', 0.0) or 0.0), 4),
            'oi_change_pct_15m': round(float(getattr(best_candidate, 'oi_change_pct_15m', 0.0) or 0.0), 4),
            'expected_slippage_r': compute_execution_quality_size_adjustment(best_candidate).get('expected_slippage_r'),
            'execution_liquidity_grade': compute_execution_quality_size_adjustment(best_candidate).get('execution_liquidity_grade'),
            'entry_distance_from_breakout_pct': round(float(getattr(best_candidate, 'entry_distance_from_breakout_pct', 0.0) or 0.0), 4),
            'entry_distance_from_vwap_pct': round(float(getattr(best_candidate, 'entry_distance_from_vwap_pct', 0.0) or 0.0), 4),
            'overextension_flag': bool(getattr(best_candidate, 'overextension_flag', False)),
        }
        cycle['triggered_but_risk_rejected'] = [risk_reject_detail] if bool(getattr(best_candidate, 'trigger_fired', False)) or bool(getattr(best_candidate, 'setup_ready', False)) else []
        probe_entry = evaluate_sim_probe_entry(best_candidate, risk_guard, args)
        cycle['sim_probe_entry'] = probe_entry
        if not bool(probe_entry.get('allowed', False)):
            cycle['live_skipped_due_to_risk_guard'] = risk_guard['reasons']
            append_candidate_rejected_event(store, best_candidate, risk_guard['reasons'])
            append_missed_trade_event(
                store, best_candidate, risk_guard['reasons'],
                probe_eligible=bool(probe_entry.get('probe_eligible', False)),
            )
            persist_cycle_snapshot(cycle)
            return result
        best_candidate = build_probe_candidate(best_candidate, float(probe_entry.get('size_ratio', 0.2) or 0.2))
        cycle['risk_guard'] = {
            **risk_guard,
            'allowed': True,
            'probe_override': True,
            'probe_original_reasons': list(risk_guard.get('reasons', [])),
        }
        scan_funnel = cycle.get('scan', {}).get('funnel')
        if isinstance(scan_funnel, dict):
            scan_funnel['selected_risk_allowed_count'] = 1
            scan_funnel['probe_entry_allowed_count'] = 1
    websocket_gate_required = bool(getattr(args, 'require_book_ticker_ws', True))
    websocket_health = cycle.get('book_ticker_websocket', {})
    websocket_summary = websocket_health.get('summary', {}) if isinstance(websocket_health, dict) else {}
    websocket_status = str(websocket_summary.get('status') or '').lower()
    websocket_reason = str(websocket_summary.get('reason') or 'unknown')
    if websocket_gate_required and websocket_status == 'unavailable':
        cycle['live_skipped_due_to_websocket_gate'] = [f'book_ticker_websocket_unavailable:{websocket_reason}']
        append_candidate_rejected_event(store, best_candidate, cycle['live_skipped_due_to_websocket_gate'])
        append_missed_trade_event(store, best_candidate, cycle['live_skipped_due_to_websocket_gate'])
        persist_cycle_snapshot(cycle)
        return result
    meta = meta_map.get(best_candidate.symbol)
    if meta is None:
        raise ValueError(f'missing symbol meta for {best_candidate.symbol}')
    persist_auto_loop_state(
        store,
        args=args,
        state='ENTERING',
        candidate=best_candidate,
        reason='execution_gate_passed',
        risk_guard=cycle.get('risk_guard'),
        reconcile=reconcile,
    )
    requested_leverage = int(getattr(args, 'leverage', best_candidate.recommended_leverage) or best_candidate.recommended_leverage)
    try:
        live_execution = run_with_deadman_timeout(
            place_live_trade,
            client,
            best_candidate,
            requested_leverage,
            meta,
            args,
            timeout_seconds=execution_timeout_seconds,
            store=store,
            component='execution',
            operation='place_live_trade',
        )
        if isinstance(live_execution, dict) and live_execution.get('reason') == 'deadman_timeout':
            cycle['live_execution_error'] = {
                'exchange': 'Binance',
                'simulated': bool(binance_simulated_trading),
                'symbol': best_candidate.symbol,
                'side': getattr(best_candidate, 'side', getattr(best_candidate, 'position_side', '')),
                'error': 'execution_timeout',
                'deadman': live_execution,
                'entry_mode': 'sim_probe' if bool(probe_entry.get('allowed', False)) else 'full',
            }
            append_candidate_rejected_event(store, best_candidate, ['execution_timeout'], cycle['live_execution_error'])
            persist_auto_loop_state(store, args=args, state='SCAN', candidate=best_candidate, reason='execution_timeout', risk_guard=cycle.get('risk_guard'), reconcile=reconcile, extra={'blocked_reason': 'execution_timeout'})
            persist_cycle_snapshot(cycle)
            return result
        record_runtime_heartbeat(store, component='execution', status='healthy', blocked_reason='', extra={'operation': 'place_live_trade', 'symbol': best_candidate.symbol})
    except BinanceAPIError as exc:
        cycle['live_execution_error'] = {
            'exchange': 'Binance',
            'simulated': bool(binance_simulated_trading),
            'symbol': best_candidate.symbol,
            'side': getattr(best_candidate, 'side', getattr(best_candidate, 'position_side', '')),
            'error': str(exc),
            'entry_mode': 'sim_probe' if bool(probe_entry.get('allowed', False)) else 'full',
        }
        append_candidate_rejected_event(store, best_candidate, ['binance_execution_preflight_failed'], cycle['live_execution_error'])
        persist_cycle_snapshot(cycle)
        return result
    cycle['live_execution'] = live_execution
    persist_auto_loop_state(
        store,
        args=args,
        state='POSITION_OPEN',
        candidate=best_candidate,
        reason='entry_order_submitted',
        risk_guard=cycle.get('risk_guard'),
        reconcile=reconcile,
        extra={'live_execution': live_execution},
    )
    scan_funnel = cycle.get('scan', {}).get('funnel')
    if isinstance(scan_funnel, dict):
        scan_funnel['order_submitted_count'] = 1
    positions_state, position_key = persist_live_open_position(store, best_candidate, live_execution)
    if getattr(args, 'auto_loop', False):
        uds_monitor = run_user_data_stream_monitor_cycle(
            client=client,
            store=store,
            symbol=best_candidate.symbol,
            refresh_interval_minutes=float(getattr(args, 'user_stream_refresh_interval_minutes', 30.0) or 30.0),
            disconnect_timeout_minutes=float(getattr(args, 'user_stream_disconnect_timeout_minutes', 65.0) or 65.0),
        )
        positions_state = store.load_json('positions', {})
        if not isinstance(positions_state, dict):
            positions_state = {}
        _, position_state = get_position_by_symbol_side(positions_state, best_candidate.symbol, live_execution.get('side') or 'LONG')
        if not isinstance(position_state, dict):
            position_state = {}
        position_state['status'] = 'monitoring'
        position_state['monitor_mode'] = 'background_thread'
        position_state['user_data_stream'] = build_user_data_stream_position_payload(uds_monitor)
        if isinstance(cycle.get('book_ticker_websocket'), dict):
            ws_payload = cycle['book_ticker_websocket']
            ws_health = ws_payload.get('health') if isinstance(ws_payload.get('health'), dict) else None
            if ws_health is None:
                stored_ws_health = store.load_json('book_ticker_ws_status', {})
                ws_health = stored_ws_health if isinstance(stored_ws_health, dict) and stored_ws_health.get('status') else dict(ws_payload)
            position_state['book_ticker_websocket'] = ws_health
        positions_state, position_key = upsert_position_record(positions_state, position_state, key=position_key)
        thread = start_trade_monitor_thread(
            client=client,
            symbol=best_candidate.symbol,
            meta=meta,
            args=args,
            trade=live_execution,
            store=store,
        )
        positions_state[position_key]['monitor_thread_name'] = getattr(thread, 'name', 'trade-monitor')
        if isinstance(cycle.get('book_ticker_websocket'), dict):
            stored_ws_health = store.load_json('book_ticker_ws_status', {})
            if isinstance(stored_ws_health, dict) and stored_ws_health.get('status'):
                positions_state[position_key]['book_ticker_websocket'] = stored_ws_health
        store.save_json('positions', positions_state)
        append_buy_fill_confirmed_event(store, best_candidate.symbol, positions_state, position_key)
        alert_payload = emit_user_data_stream_alert_if_needed(args, best_candidate.symbol, uds_monitor)
        if alert_payload is not None:
            cycle['user_data_stream_alert'] = alert_payload
        cycle['trade_management'] = {
            'mode': 'background_thread',
            'thread_name': getattr(thread, 'name', 'trade-monitor'),
            'user_data_stream': uds_monitor,
        }
        persist_auto_loop_state(
            store,
            args=args,
            state='MANAGING',
            candidate=best_candidate,
            reason='background_trade_monitor_started',
            risk_guard=cycle.get('risk_guard'),
            reconcile=reconcile,
            extra={'position_key': position_key, 'trade_management': cycle['trade_management']},
        )
    else:
        cycle['trade_management'] = monitor_live_trade(client=client, symbol=best_candidate.symbol, meta=meta, args=args, trade=live_execution, store=store)
        persist_auto_loop_state(
            store,
            args=args,
            state='MANAGING',
            candidate=best_candidate,
            reason='trade_monitor_completed',
            risk_guard=cycle.get('risk_guard'),
            reconcile=reconcile,
            extra={'position_key': position_key, 'trade_management': cycle['trade_management']},
        )
    persist_cycle_snapshot(cycle)
    return result


def _persist_resident_cycle_snapshot(store: RuntimeStateStore, args: argparse.Namespace, cycle_payload: Dict[str, Any]) -> None:
    try:
        store.save_json('last_cycle', {
            'updated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'profile': getattr(args, 'profile', 'default'),
            'live_requested': bool(getattr(args, 'live', False)),
            'execution_exchange': execution_exchange_label(args),
            'scan_only': bool(getattr(args, 'scan_only', False)),
            'auto_loop': bool(getattr(args, 'auto_loop', False)),
            'cycle': cycle_payload,
        })
    except Exception:
        pass


def scan_only_cycle(client: Any, args: argparse.Namespace, *, store: Optional[RuntimeStateStore] = None, cycle_no: Optional[int] = None, websocket_status: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Scanner actor step: reconcile, generate candidates, evaluate gates, and emit execution work."""
    store = store or get_runtime_state_store(args)
    scanner_timeout_seconds = float(getattr(args, 'scanner_timeout_seconds', 45.0) or 45.0)
    cleanup_symbol_runtime_state_ttl(store, ttl_seconds=float(getattr(args, 'runtime_ttl_seconds', 900.0) or 900.0))
    rest_snapshot = _runtime_store_rest_guard_snapshot(store) if hasattr(args, 'runtime_state_dir') else {'state': 'CLOSED', 'rest_used_weight_1m': 0, 'next_retry_after_seconds': 0}
    ws_market_state_age_ms = None
    if isinstance(websocket_status, dict):
        candidates = [websocket_status.get('last_message_at_ms'), websocket_status.get('updated_at_ms'), websocket_status.get('last_update_at_ms')]
        for value in candidates:
            try:
                if value:
                    ws_market_state_age_ms = max(0, int(time.time() * 1000) - int(float(value)))
                    break
            except Exception:
                continue
    if (str(rest_snapshot.get('state') or '').upper() in {'OPEN', 'HALF_OPEN', 'DEGRADED'} and int(rest_snapshot.get('next_retry_after_seconds') or 0) > 0) or int(rest_snapshot.get('rest_used_weight_1m') or 0) > 1200:
        next_retry_after_seconds = int(rest_snapshot.get('next_retry_after_seconds') or max(1, float(getattr(args, 'ticker_24hr_cache_refresh_seconds', 120.0) or 120.0)))
        blocked_payload = {
            'ok': False,
            'degraded': True,
            'blocked_reason': 'binance_rest_circuit_open',
            'scanner_degraded_wait': True,
            'rest_used_weight_1m': rest_snapshot.get('rest_used_weight_1m'),
            'rest_circuit_state': rest_snapshot.get('state'),
            'rest_circuit_reason': rest_snapshot.get('reason'),
            'next_rest_probe_at': rest_snapshot.get('next_rest_probe_at_ms'),
            'next_retry_after_seconds': next_retry_after_seconds,
            'ws_market_state_age_ms': ws_market_state_age_ms,
        }
        cycle: Dict[str, Any] = {'scan': blocked_payload, 'blocked_reason': 'binance_rest_circuit_open', 'reconcile': {'ok': True, 'skipped': True, 'skip_reason': 'rest_circuit_open'}}
        if cycle_no is not None:
            cycle['cycle_no'] = cycle_no
        if websocket_status is not None:
            cycle['book_ticker_websocket'] = websocket_status
        record_runtime_heartbeat(store, component='scanner', status='degraded_wait', blocked_reason='binance_rest_circuit_open', extra=blocked_payload)
        append_runtime_event(store, 'scanner_degraded_wait', blocked_payload)
        return {'ok': True, 'cycle': cycle, 'scanner_degraded_wait': True, 'scan_delay_multiplier': max(3.0, float(blocked_payload['next_retry_after_seconds'] or 0) / max(1.0, float(getattr(args, 'poll_interval_sec', 60) or 60))), 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'state': 'DEGRADED', 'reason': 'binance_rest_circuit_open'}}
    binance_simulated_trading = is_binance_simulated_trading(args)
    execution_exchange = execution_exchange_label(args)
    reconcile_interval_seconds = compute_position_order_reconcile_interval_seconds(args)
    reconcile_cursor = store.load_json('scanner_reconcile_cursor', {})
    if not isinstance(reconcile_cursor, dict):
        reconcile_cursor = {}
    last_scanner_reconcile_at = float(reconcile_cursor.get('last_full_reconcile_at_monotonic') or 0.0)
    if last_scanner_reconcile_at <= 0.0:
        last_scanner_reconcile_at = time.monotonic()
    scanner_reconcile_cursor_update: Optional[Dict[str, Any]] = None
    due_scanner_full_reconcile = (time.monotonic() - last_scanner_reconcile_at) >= reconcile_interval_seconds
    if binance_simulated_trading:
        reconcile = {'ok': True, 'skipped': True, 'skip_reason': 'binance_simulated_trading', 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []}
    elif not due_scanner_full_reconcile:
        reconcile = {'ok': True, 'skipped': True, 'skip_reason': 'scanner_rest_full_reconcile_not_due', 'mode': 'ws_local_lightweight', 'interval_seconds': reconcile_interval_seconds, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []}
    elif str(_binance_rest_guard_snapshot().get('state') or '').upper() in {'OPEN', 'HALF_OPEN', 'DEGRADED'}:
        rest_snapshot_for_reconcile = _binance_rest_guard_snapshot()
        reconcile = {'ok': True, 'skipped': True, 'skip_reason': 'rest_circuit_open', 'skipped_rest_reconcile_due_to_circuit': True, 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': [], 'rest_circuit': rest_snapshot_for_reconcile}
        append_runtime_event(store, 'skipped_rest_reconcile_due_to_circuit', {'component': 'scanner', **rest_snapshot_for_reconcile})
    else:
        try:
            try:
                reconcile = reconcile_runtime_state(client, store, halt_on_orphan_position=getattr(args, 'halt_on_orphan_position', False), repair_missing_protection_enabled=getattr(args, 'repair_missing_protection', True), args=args)
            except TypeError as exc:
                if "unexpected keyword argument 'args'" not in str(exc):
                    raise
                reconcile = reconcile_runtime_state(client, store, halt_on_orphan_position=getattr(args, 'halt_on_orphan_position', False), repair_missing_protection_enabled=getattr(args, 'repair_missing_protection', True))
        except BinanceAPIError as exc:
            if str(exc) == 'api_secret is required for signed requests' and not getattr(args, 'live', False) and not getattr(args, 'reconcile_only', False):
                reconcile = {'ok': True, 'skipped': True, 'skip_reason': 'missing_api_secret', 'orphan_positions': [], 'positions_missing_protection': [], 'protection_repairs': []}
            else:
                raise
        if isinstance(reconcile, dict) and reconcile.get('ok', True):
            scanner_reconcile_cursor_update = {'last_full_reconcile_at_monotonic': time.monotonic(), 'updated_at': _isoformat_utc(_utc_now())}
    if not binance_simulated_trading and not getattr(args, 'scan_only', False) and reconcile.get('skip_reason') != 'missing_api_secret' and not reconcile.get('skipped_rest_reconcile_due_to_circuit'):
        order_reconcile = reconcile_positions_and_orders(
            client,
            store,
            max_open_positions=getattr(args, 'max_open_positions', None),
            max_short_positions=(getattr(args, 'max_short_positions', None) or None),
        )
        reconcile['order_reconcile'] = order_reconcile
        reconcile['ok'] = bool(reconcile.get('ok', True)) and bool(order_reconcile.get('ok', True))
    cycle: Dict[str, Any] = {'reconcile': reconcile}
    if scanner_reconcile_cursor_update:
        cycle['scanner_reconcile_cursor_update'] = scanner_reconcile_cursor_update
    if cycle_no is not None:
        cycle['cycle_no'] = cycle_no
    if websocket_status is not None:
        cycle['book_ticker_websocket'] = websocket_status
    if getattr(args, 'reconcile_only', False) or not reconcile.get('ok', True):
        return {'ok': bool(reconcile.get('ok', True)), 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'reconcile': reconcile}}
    open_circuit = load_open_scanner_rest_circuit(store)
    if open_circuit is not None:
        blocked_payload = {
            'ok': False,
            'blocked_reason': 'binance_ip_ban_circuit_open',
            'circuit_breaker': open_circuit,
        }
        cycle['scan'] = blocked_payload
        cycle['blocked_reason'] = 'binance_ip_ban_circuit_open'
        record_runtime_heartbeat(store, component='scanner', status='blocked', blocked_reason='binance_ip_ban_circuit_open', extra=blocked_payload)
        append_runtime_event(store, 'scanner_blocked', {'blocked_reason': 'binance_ip_ban_circuit_open', 'circuit_breaker': open_circuit})
        return {'ok': True, 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'blocked_reason': 'binance_ip_ban_circuit_open'}}
    try:
        scan_call_result = run_with_deadman_timeout(run_scan_once, client, args, timeout_seconds=scanner_timeout_seconds, store=store, component='scanner', operation='scan_cycle')
    except BinanceAPIError as exc:
        message = str(exc)
        if is_binance_ip_ban_error(message):
            retry_after_ms = extract_binance_ip_ban_until_ms(message)
            blocked_payload = {
                'ok': False,
                'blocked_reason': 'binance_ip_ban',
                'error': message,
            }
            if retry_after_ms is not None:
                blocked_payload['retry_after_ms'] = retry_after_ms
            circuit_payload = build_scanner_rest_circuit_payload(
                reason='binance_ip_ban',
                retry_after_ms=retry_after_ms,
                error=message,
                fallback_cooldown_seconds=float(getattr(args, 'scanner_rest_ban_cooldown_seconds', 180.0) or 0.0),
            )
            store.save_json('scanner_rest_circuit_breaker', circuit_payload)
            blocked_payload['circuit_breaker'] = circuit_payload
            cycle['scan'] = blocked_payload
            cycle['blocked_reason'] = 'binance_ip_ban'
            record_runtime_heartbeat(store, component='scanner', status='blocked', blocked_reason='binance_ip_ban', extra=blocked_payload)
            append_runtime_event(store, 'scanner_blocked', {'blocked_reason': 'binance_ip_ban', 'error': message, 'retry_after_ms': retry_after_ms})
            return {'ok': True, 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'blocked_reason': 'binance_ip_ban'}}
        raise
    if isinstance(scan_call_result, dict) and scan_call_result.get('reason') == 'deadman_timeout':
        cycle['scan'] = {'ok': False, 'blocked_reason': 'scanner_timeout', 'deadman': scan_call_result}
        cycle['blocked_reason'] = 'scanner_timeout'
        return {'ok': True, 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'blocked_reason': 'scanner_timeout'}}
    scan_result, best_candidate, meta_map = scan_call_result
    cycle['scan'] = scan_result
    record_runtime_heartbeat(store, component='scanner', status='healthy', blocked_reason='', extra={'candidate_found': best_candidate is not None, 'cycle_no': cycle_no})
    try:
        risk_state = load_risk_state(store)
    except AttributeError:
        risk_state = default_risk_state()
    if best_candidate is None:
        cycle['risk_guard'] = evaluate_risk_guards(risk_state=risk_state, daily_max_loss_usdt=float(getattr(args, 'daily_max_loss_usdt', 0.0) or 0.0), max_consecutive_losses=int(getattr(args, 'max_consecutive_losses', 0) or 0), symbol_cooldown_minutes=int(getattr(args, 'symbol_cooldown_minutes', 0) or 0))
        return {'ok': True, 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'state': 'SCAN', 'reason': 'no_candidate'}}
    event_updates: List[Dict[str, Any]] = [append_candidate_selected_event(None, best_candidate, regime_payload=scan_result.get('market_regime', {}) if isinstance(scan_result, dict) else {}, extra={'profile': getattr(args, 'profile', 'default'), 'live_requested': bool(getattr(args, 'live', False)), 'scan_only': bool(getattr(args, 'scan_only', False)), 'execution_exchange': execution_exchange})]
    risk_guard = evaluate_risk_guards(symbol=best_candidate.symbol, risk_state=risk_state, candidate=best_candidate, daily_max_loss_usdt=float(getattr(args, 'daily_max_loss_usdt', 0.0) or 0.0), max_consecutive_losses=int(getattr(args, 'max_consecutive_losses', 0) or 0), symbol_cooldown_minutes=int(getattr(args, 'symbol_cooldown_minutes', 0) or 0), base_risk_usdt=float(getattr(args, 'risk_usdt', 0.0) or 0.0), gross_heat_cap_r=float(getattr(args, 'gross_heat_cap_r', 0.0) or 0.0), same_theme_heat_cap_r=float(getattr(args, 'same_theme_heat_cap_r', 0.0) or 0.0), same_correlation_heat_cap_r=float(getattr(args, 'same_correlation_heat_cap_r', 0.0) or 0.0), portfolio_narrative_bucket=getattr(best_candidate, 'portfolio_narrative_bucket', ''), portfolio_correlation_group=getattr(best_candidate, 'portfolio_correlation_group', ''), aggressive_flip_cooldown_minutes=int(getattr(args, 'opposite_side_flip_cooldown_minutes', 0) or 0))
    open_positions = fetch_open_positions(client) if getattr(args, 'live', False) and not binance_simulated_trading else []
    pending_opening_orders: List[Dict[str, Any]] = []
    if getattr(args, 'live', False) and not binance_simulated_trading:
        pending_opening_orders = [
            order
            for order in list(fetch_open_orders(client) or []) + list(fetch_open_algo_orders(client) or [])
            if isinstance(order, dict) and is_opening_order(order)
        ]
    projected_open_positions = projected_position_keys(open_positions, pending_opening_orders)
    portfolio_risk_guard = evaluate_portfolio_risk_guards(open_positions=list(open_positions) + opening_orders_as_risk_positions(pending_opening_orders, float(getattr(best_candidate, 'last_price', 0.0) or 0.0)), candidate=best_candidate, max_long_positions=int(getattr(args, 'max_long_positions', 0) or 0), max_short_positions=int(getattr(args, 'max_short_positions', 0) or 0), max_net_exposure_usdt=float(getattr(args, 'max_net_exposure_usdt', 0.0) or 0.0), max_gross_exposure_usdt=float(getattr(args, 'max_gross_exposure_usdt', 0.0) or 0.0), per_symbol_single_side_only=bool(getattr(args, 'per_symbol_single_side_only', True)), opposite_side_flip_cooldown_minutes=int(getattr(args, 'opposite_side_flip_cooldown_minutes', 0) or 0))
    risk_guard = {'allowed': bool(risk_guard.get('allowed', True)) and bool(portfolio_risk_guard.get('allowed', True)), 'reasons': list(risk_guard.get('reasons', [])) + list(portfolio_risk_guard.get('reasons', [])), 'cooldown_until': risk_guard.get('cooldown_until'), 'normalized_risk_state': risk_guard.get('normalized_risk_state', default_risk_state()), 'portfolio': portfolio_risk_guard, 'trigger_confidence': risk_guard.get('trigger_confidence', {'level': 'confirmed', 'score': 1.0, 'factors': []}), 'execution_mode': risk_guard.get('execution_mode', 'taker'), 'execution_size_multiplier': risk_guard.get('execution_size_multiplier', 1.0), 'trigger_state': risk_guard.get('trigger_state', 'confirmed'), 'trigger_gate_action': risk_guard.get('trigger_gate_action', 'execute')}
    if risk_guard['allowed'] and risk_guard.get('execution_mode') in {'maker_probe', 'maker_confirm'}:
        size_multiplier = max(min(float(risk_guard.get('execution_size_multiplier') or 1.0), 1.0), 0.0)
        original_quantity = max(float(getattr(best_candidate, 'quantity', 0.0) or 0.0), 0.0)
        adjusted_quantity = original_quantity * size_multiplier
        last_price = float(getattr(best_candidate, 'last_price', 0.0) or 0.0)
        min_notional_usdt = max(float(getattr(best_candidate, 'min_notional_usdt', 0.0) or 0.0), 0.0)
        if last_price > 0.0 and min_notional_usdt > 0.0:
            min_quantity = min_notional_usdt / last_price
            if original_quantity >= min_quantity and adjusted_quantity < min_quantity:
                adjusted_quantity = original_quantity
        updates = {
            'quantity': adjusted_quantity,
            'execution_mode_override': 'maker_only',
            'execution_liquidity_grade_override': 'D',
            'execution_degradation_mode': risk_guard.get('execution_mode'),
            'pretrigger_watch': risk_guard.get('execution_mode') == 'maker_probe',
            'trigger_relax_mode': True,
            'reasons': list(getattr(best_candidate, 'reasons', []) or []) + [
                f"trigger_confidence={risk_guard.get('trigger_confidence', {}).get('level')}",
                f"execution_mode={risk_guard.get('execution_mode')}",
                f"execution_size_multiplier={size_multiplier}",
            ],
        }
        best_candidate = replace(best_candidate, **{key: value for key, value in updates.items() if hasattr(best_candidate, key)})
    cycle['risk_guard'] = risk_guard
    cycle['trigger_state'] = risk_guard.get('trigger_state', 'confirmed')
    cycle['trigger_gate_action'] = risk_guard.get('trigger_gate_action', 'execute')
    cycle['final_execution_gate_action'] = 'execute' if risk_guard.get('allowed', True) else 'veto'
    cycle['final_blocking_reason'] = '' if risk_guard.get('allowed', True) else (risk_guard.get('reasons') or ['risk_guard_blocked'])[0]
    cycle['scan_only'] = bool(getattr(args, 'scan_only', False))
    cycle['live_requested'] = bool(getattr(args, 'live', False))
    cycle['execution_exchange'] = execution_exchange
    if isinstance(cycle.get('scan'), dict) and isinstance(cycle['scan'].get('funnel'), dict):
        cycle['scan']['funnel']['selected_risk_allowed_count'] = 1 if risk_guard['allowed'] else 0
        cycle['scan']['funnel']['order_submitted_count'] = 0
    websocket_health = cycle.get('book_ticker_websocket', {})
    if (not getattr(args, 'live', False)) or getattr(args, 'scan_only', False):
        return {'ok': True, 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'state': choose_auto_loop_state_for_candidate(best_candidate, risk_guard), 'reason': 'scan_only', 'reconcile': reconcile, 'event_updates': event_updates}}
    if bool(getattr(args, 'diagnostic_trading_mode', False)):
        cycle['diagnostic_trading_mode'] = True
        cycle['final_execution_gate_action'] = 'diagnostic_skip'
        cycle['final_blocking_reason'] = 'diagnostic_trading_mode'
        if isinstance(cycle.get('scan'), dict) and isinstance(cycle['scan'].get('funnel'), dict):
            cycle['scan']['funnel']['order_submitted_count'] = 0
        return {'ok': True, 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'state': choose_auto_loop_state_for_candidate(best_candidate, risk_guard), 'reason': 'diagnostic_trading_mode', 'reconcile': reconcile, 'event_updates': event_updates}}
    if bool(getattr(args, 'require_book_ticker_ws', True)) and isinstance(websocket_health, dict) and websocket_health:
        health = websocket_health.get('health') if isinstance(websocket_health.get('health'), dict) else websocket_health
        stored_websocket_health = store.load_json('book_ticker_ws_status', {})
        health = choose_latest_websocket_health(health, stored_websocket_health)
        cycle['book_ticker_websocket'] = dict(websocket_health, health=health) if isinstance(websocket_health.get('health'), dict) else health
        freshness = evaluate_websocket_freshness(health, max_age_seconds=float(getattr(args, 'book_ticker_ws_stale_seconds', 30.0) or 30.0), require_messages=bool(getattr(args, 'require_book_ticker_ws_messages', True)))
        cycle['book_ticker_websocket_freshness'] = freshness
        cycle['execution_degradation_mode'] = freshness.get('execution_degradation_mode', 'none')
        cycle['websocket_state'] = freshness.get('state')
        cycle['websocket_gate_action'] = freshness.get('websocket_gate_action')
        cycle['websocket_degradation_applied'] = bool(freshness.get('websocket_degradation_applied', False))
        if freshness.get('state') == 'dead':
            reason = str(freshness.get('reason') or 'stale_websocket')
            cycle['live_skipped_due_to_websocket_gate'] = [f'book_ticker_websocket_hard_veto:{reason}']
            cycle['final_execution_gate_action'] = 'veto'
            cycle['final_blocking_reason'] = f'websocket:{reason}'
            event_updates.append(append_candidate_rejected_event(None, best_candidate, cycle['live_skipped_due_to_websocket_gate']))
            return {'ok': True, 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'state': 'SCAN', 'reason': f'websocket_stale:{reason}', 'reconcile': reconcile, 'event_updates': event_updates}}
        if freshness.get('execution_degradation_mode') in {'reduced_aggression', 'maker_only'}:
            best_candidate = build_websocket_degraded_candidate(best_candidate, freshness)
            cycle['delayed_execution_queue'] = {'kept_alive': True, 'reason': freshness.get('reason'), 'websocket_state': freshness.get('state'), 'stale_guard_decision_reason': freshness.get('stale_guard_decision_reason')}
            cycle['final_execution_gate_action'] = 'maker_confirm' if freshness.get('execution_degradation_mode') == 'maker_only' else 'watch'
    if len(projected_open_positions) >= int(getattr(args, 'max_open_positions', 1) or 1):
        cycle['live_skipped_due_to_existing_positions'] = open_positions
        cycle['live_skipped_due_to_projected_positions'] = sorted(projected_open_positions)
        cycle['open_positions_count'] = len(open_positions)
        cycle['pending_opening_orders_count'] = len(pending_opening_orders)
        event_updates.append(append_candidate_rejected_event(None, best_candidate, ['max_open_positions_reached']))
        return {'ok': True, 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'state': 'SCAN', 'reason': 'max_open_positions_reached', 'reconcile': reconcile, 'event_updates': event_updates}}
    if not risk_guard['allowed']:
        cycle['live_skipped_due_to_risk_guard'] = risk_guard['reasons']
        event_updates.append(append_candidate_rejected_event(None, best_candidate, risk_guard['reasons']))
        missed_event = append_missed_trade_event(None, best_candidate, risk_guard['reasons'])
        if missed_event:
            event_updates.append(missed_event)
        return {'ok': True, 'cycle': cycle, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'state': 'SCAN', 'reason': 'risk_guard_blocked', 'reconcile': reconcile, 'event_updates': event_updates}}
    meta = meta_map.get(best_candidate.symbol)
    if meta is None:
        raise ValueError(f'missing symbol meta for {best_candidate.symbol}')
    execution_request = {'candidate': best_candidate, 'meta': meta, 'risk_guard': risk_guard, 'reconcile': reconcile, 'cycle': cycle, 'requested_leverage': int(getattr(args, 'leverage', getattr(best_candidate, 'recommended_leverage', 1)) or getattr(best_candidate, 'recommended_leverage', 1)), 'cycle_no': cycle_no}
    return {'ok': True, 'cycle': cycle, 'execution_request': execution_request, 'manager_update': {'kind': 'cycle', 'cycle': cycle, 'state': 'ENTERING', 'reason': 'execution_gate_passed', 'reconcile': reconcile, 'event_updates': event_updates}}


def execution_cycle(client: Any, args: argparse.Namespace, execution_request: Dict[str, Any], *, store: Optional[RuntimeStateStore] = None) -> Dict[str, Any]:
    """Execution actor step: place/manage a trade isolated from scanner progress."""
    store = store or get_runtime_state_store(args)
    candidate = execution_request['candidate']
    meta = execution_request['meta']
    cycle = dict(execution_request.get('cycle') or {})
    execution_timeout_seconds = float(getattr(args, 'execution_timeout_seconds', 30.0) or 30.0)
    if getattr(args, 'require_book_ticker_ws', False):
        fresh_websocket_status = store.load_json('book_ticker_ws_status', {})
        if not isinstance(fresh_websocket_status, dict) or not fresh_websocket_status:
            freshness = evaluate_websocket_freshness(None)
            veto_cycle = dict(cycle, book_ticker_websocket_freshness=freshness, final_execution_gate_action='veto', final_blocking_reason='websocket:missing_health')
            return {'ok': True, 'cycle': veto_cycle, 'manager_update': {'kind': 'cycle', 'cycle': veto_cycle, 'state': 'SCAN', 'reason': 'websocket_stale:missing_health', 'state_transition': {'state': 'SCAN', 'reason': 'websocket_dead', 'blocked_reason': 'websocket_stale:missing_health'}}}
        if isinstance(fresh_websocket_status, dict) and fresh_websocket_status:
            fresh_websocket_status = choose_latest_websocket_health(meta.get('book_ticker_websocket') if isinstance(meta, dict) else {}, fresh_websocket_status)
            meta = dict(meta)
            meta['book_ticker_websocket'] = fresh_websocket_status
            freshness = evaluate_websocket_freshness(fresh_websocket_status, max_age_seconds=float(getattr(args, 'book_ticker_ws_stale_seconds', 30.0) or 30.0), require_messages=bool(getattr(args, 'require_book_ticker_ws_messages', True)))
            meta['websocket_execution_guard'] = freshness
            cycle['book_ticker_websocket_freshness'] = freshness
            cycle['execution_degradation_mode'] = freshness.get('execution_degradation_mode', 'none')
            if freshness.get('state') == 'dead':
                reason = str(freshness.get('reason') or 'stale_websocket')
                veto_cycle = dict(
                    cycle,
                    live_skipped_due_to_websocket_gate=[f'book_ticker_websocket_hard_veto:{reason}'],
                    websocket_state=freshness.get('state'),
                    websocket_gate_action=freshness.get('websocket_gate_action'),
                    websocket_degradation_applied=False,
                    final_execution_gate_action='veto',
                    final_blocking_reason=f'websocket:{reason}',
                )
                return {'ok': True, 'cycle': veto_cycle, 'manager_update': {'kind': 'cycle', 'cycle': veto_cycle, 'state': 'SCAN', 'reason': f'websocket_stale:{reason}', 'state_transition': {'state': 'SCAN', 'reason': 'websocket_dead', 'blocked_reason': f'websocket_stale:{reason}'}}}
            if freshness.get('execution_degradation_mode') in {'reduced_aggression', 'maker_only'}:
                candidate = build_websocket_degraded_candidate(candidate, freshness)
    state_transition = {'state': 'ENTERING', 'candidate_symbol': getattr(candidate, 'symbol', ''), 'reason': 'execution_gate_passed', 'risk_guard': execution_request.get('risk_guard'), 'reconcile': execution_request.get('reconcile')}
    execution_events = [{
        'event_type': 'execution_deadman_started',
        'symbol': getattr(candidate, 'symbol', ''),
        'side': getattr(candidate, 'side', getattr(candidate, 'position_side', '')),
        'position_key': build_position_key(getattr(candidate, 'symbol', ''), getattr(candidate, 'side', getattr(candidate, 'position_side', POSITION_SIDE_LONG))),
        'operation': 'place_live_trade',
        'phase': 'preflight_margin_leverage_entry_stop',
        'timeout_seconds': float(execution_timeout_seconds or 0.0),
        'execution_mode_override': str(getattr(candidate, 'execution_mode_override', '') or ''),
        'execution_degradation_mode': str(getattr(candidate, 'execution_degradation_mode', '') or ''),
        'candidate_stage': str(getattr(candidate, 'candidate_stage', '') or ''),
    }]
    try:
        live_execution = run_with_deadman_timeout(place_live_trade, client, candidate, int(execution_request.get('requested_leverage') or 1), meta, args, timeout_seconds=execution_timeout_seconds, store=store, component='execution', operation='place_live_trade')
    except BinanceAPIError as exc:
        error = {'exchange': 'Binance', 'simulated': bool(is_binance_simulated_trading(args)), 'symbol': candidate.symbol, 'side': getattr(candidate, 'side', getattr(candidate, 'position_side', '')), 'error': str(exc)}
        return {'ok': False, 'live_execution_error': error, 'cycle': dict(cycle, live_execution_error=error), 'manager_update': {'kind': 'execution_error', 'cycle': dict(cycle, live_execution_error=error), 'error': error, 'event_updates': execution_events}}
    if isinstance(live_execution, dict) and live_execution.get('reason') == 'deadman_timeout':
        error = {'exchange': 'Binance', 'simulated': bool(is_binance_simulated_trading(args)), 'symbol': candidate.symbol, 'side': getattr(candidate, 'side', getattr(candidate, 'position_side', '')), 'error': 'execution_timeout', 'deadman': live_execution}
        return {'ok': False, 'live_execution_error': error, 'cycle': dict(cycle, live_execution_error=error), 'manager_update': {'kind': 'execution_error', 'cycle': dict(cycle, live_execution_error=error), 'error': error, 'event_updates': execution_events, 'state_transition': {'state': 'SCAN', 'reason': 'execution_timeout', 'blocked_reason': 'execution_timeout'}}}
    cycle['live_execution'] = live_execution
    position_side = getattr(candidate, 'position_side', getattr(candidate, 'side', POSITION_SIDE_LONG))
    position_key = build_position_key(candidate.symbol, position_side)
    cycle['trade_management'] = {'mode': 'position_manager_actor', 'position_key': position_key}
    position_manager_request = {
        'kind': 'position_opened',
        'candidate': candidate,
        'symbol': candidate.symbol,
        'position_key': position_key,
        'meta': meta,
        'trade': live_execution,
        'risk_guard': execution_request.get('risk_guard'),
        'reconcile': execution_request.get('reconcile'),
        'cycle': cycle,
    }
    return {'ok': True, 'live_execution': live_execution, 'cycle': cycle, 'position_manager_request': position_manager_request, 'manager_update': {'kind': 'execution_result', 'cycle': cycle, 'position_key': position_key, 'event_updates': execution_events, 'state_transition': state_transition}}


def management_cycle(args: argparse.Namespace, manager_update: Dict[str, Any], *, store: Optional[RuntimeStateStore] = None) -> Dict[str, Any]:
    """Manager actor step: single FSM/runtime-state update boundary."""
    store = store or get_runtime_state_store(args)
    update = manager_update if isinstance(manager_update, dict) else {'update': manager_update}
    cycle = update.get('cycle') if isinstance(update.get('cycle'), dict) else None
    if isinstance(update.get('reconcile'), dict):
        apply_reconcile_close_risk_state_updates(store, update['reconcile'], args)
    for event_update in list(update.get('event_updates') or []):
        if isinstance(event_update, dict):
            event_type = str(event_update.get('event_type') or 'runtime_event')
            append_runtime_event(store, event_type, {k: v for k, v in event_update.items() if k != 'event_type'})
    if update.get('kind') == 'runtime_event':
        append_runtime_event(store, str(update.get('event_type') or 'runtime_event'), update.get('payload') if isinstance(update.get('payload'), dict) else {})
    if isinstance(update.get('state_transition'), dict):
        transition = update['state_transition']
        persist_auto_loop_state(
            store,
            args=args,
            state=str(transition.get('state') or ''),
            candidate=None,
            reason=str(transition.get('reason') or ''),
            risk_guard=transition.get('risk_guard'),
            reconcile=transition.get('reconcile'),
            extra={k: v for k, v in transition.items() if k not in {'state', 'reason', 'risk_guard', 'reconcile'}},
        )
    if update.get('kind') == 'position_opened':
        candidate = update.get('candidate')
        trade = update.get('trade') or {}
        positions_state, position_key = persist_live_open_position(store, candidate, trade)
        append_buy_fill_confirmed_event(store, getattr(candidate, 'symbol', update.get('symbol', '')), positions_state, position_key)
        update = dict(update, position_key=position_key)
    if isinstance(cycle, dict):
        _persist_resident_cycle_snapshot(store, args, cycle)
        store.save_json('resident_last_result', {'ok': True, 'auto_loop': True, 'cycle_no': cycle.get('cycle_no'), 'cycles': [cycle]})
    append_runtime_event(store, 'resident_manager_update', update)
    return {'ok': True, 'state': update.get('state'), 'kind': update.get('kind')}



def build_backpressure_policy(component: str, reason: str, item: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    score = 1.0
    if isinstance(item, dict):
        score = float(item.get('candidate_score', item.get('score', 1.0)) or 0.0)
    return {
        'scan_delay_multiplier': 3.0 if component == 'scanner' else 1.5,
        'drop_candidate': score < 0.2 or 'queue_full' in str(reason),
        'pause_non_core_tasks': True,
        'min_candidate_score': 0.2,
    }


CRITICAL_MANAGER_UPDATE_KINDS = frozenset({'execution_result', 'execution_error', 'position_opened', 'state_transition', 'event_updates'})
COALESCABLE_RUNTIME_EVENT_TYPES = frozenset({'scan_cycle_summary', 'cycle_summary', 'scanner_heartbeat', 'heartbeat_metrics'})


def is_coalescable_manager_update(item: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(item, dict) or item.get('kind') != 'manager_update':
        return False
    update = item.get('update')
    if not isinstance(update, dict):
        return False
    kind = str(update.get('kind') or update.get('state') or '').lower()
    if kind in CRITICAL_MANAGER_UPDATE_KINDS:
        return False
    if kind in {'cycle', 'scan_cycle', 'scan_summary', 'cycle_summary', 'scan'}:
        return True
    if kind == 'runtime_event':
        event_type = str(update.get('event_type') or '').lower()
        return event_type in COALESCABLE_RUNTIME_EVENT_TYPES
    cycle = update.get('cycle')
    if isinstance(cycle, dict):
        cycle_kind = str(cycle.get('kind') or cycle.get('state') or '').lower()
        event_type = str(cycle.get('event_type') or '').lower()
        return cycle_kind in {'cycle', 'scan_cycle', 'scan_summary', 'cycle_summary', 'scan'} or event_type in COALESCABLE_RUNTIME_EVENT_TYPES
    return False


def coalesce_manager_queue_update(queue: asyncio.Queue, item: Dict[str, Any]) -> Dict[str, Any]:
    if not is_coalescable_manager_update(item):
        return {'coalesced': False, 'dropped': 0}
    kept: List[Any] = []
    dropped = 0
    while True:
        try:
            existing = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        if is_coalescable_manager_update(existing):
            dropped += 1
            queue.task_done()
        else:
            kept.append(existing)
    for existing in kept:
        queue.put_nowait(existing)
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        return {'coalesced': dropped > 0, 'dropped': dropped, 'accepted': False}
    return {'coalesced': dropped > 0, 'dropped': dropped, 'accepted': True}


async def apply_queue_backpressure(queue: asyncio.Queue, *, store: RuntimeStateStore, component: str, reason: str, item: Optional[Dict[str, Any]] = None, timeout_seconds: float = 1.0) -> Dict[str, Any]:
    policy = build_backpressure_policy(component, reason, item)
    payload = {
        'component': component,
        'reason': reason,
        'queue_depth': queue.qsize(),
        'queue_maxsize': queue.maxsize,
        'policy': policy,
    }
    if reason == 'manager_queue_full' and item is not None:
        # Coalesce on every producer write, not only after reaching maxsize, so
        # periodic scanner cycles can occupy at most one queue slot.
        if is_coalescable_manager_update(item):
            coalesced = coalesce_manager_queue_update(queue, item)
            if coalesced.get('accepted'):
                if coalesced.get('dropped'):
                    append_runtime_event(store, 'manager_queue_coalesced', {**payload, **coalesced})
                return {'accepted': True, 'degraded': False, **payload, **coalesced}
        elif queue.full():
            # Lifecycle updates are lossless relative to periodic summaries.
            kept, dropped = [], 0
            while not queue.empty():
                existing = queue.get_nowait()
                if is_coalescable_manager_update(existing):
                    queue.task_done()
                    dropped += 1
                else:
                    kept.append(existing)
            for existing in kept:
                queue.put_nowait(existing)
            if dropped:
                queue.put_nowait(item)
                result = {'accepted': True, 'coalesced': True, 'dropped': dropped}
                append_runtime_event(store, 'manager_queue_coalesced', {**payload, **result})
                return {'degraded': False, **payload, **result}
    if queue.full():
        degraded = record_runtime_heartbeat(store, component=component, status='degraded', blocked_reason=reason, queue_depth=queue.qsize(), queue_maxsize=queue.maxsize, extra={'backpressure': True, 'policy': policy})
        append_runtime_event(store, 'runtime_backpressure_degrade', {**payload, **degraded, 'degraded': True})
        return {'accepted': False, 'degraded': True, **payload}
    if item is not None:
        try:
            await asyncio.wait_for(queue.put(item), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            degraded = record_runtime_heartbeat(store, component=component, status='degraded', blocked_reason=reason, queue_depth=queue.qsize(), queue_maxsize=queue.maxsize, extra={'backpressure': True, 'policy': policy})
            append_runtime_event(store, 'runtime_backpressure_degrade', {**payload, **degraded, 'degraded': True})
            return {'accepted': False, 'degraded': True, **payload}
    return {'accepted': True, 'degraded': False, **payload}


async def event_loop_latency_task(store: RuntimeStateStore, stop_event: asyncio.Event, *, interval: float = 1.0, warn_threshold_seconds: float = 0.25, max_samples: Optional[int] = None) -> None:
    samples = 0
    next_tick = time.monotonic() + max(float(interval or 0.0), 0.001)
    while not stop_event.is_set():
        await asyncio.sleep(max(0.0, next_tick - time.monotonic()))
        now = time.monotonic()
        lag_seconds = max(0.0, now - next_tick)
        status = 'lagging' if lag_seconds >= max(float(warn_threshold_seconds or 0.0), 0.0) else 'healthy'
        blocked_reason = 'event_loop_lag' if status == 'lagging' else ''
        record_runtime_heartbeat(store, component='event_loop', status=status, blocked_reason=blocked_reason, extra={'lag_seconds': lag_seconds, 'interval_seconds': float(interval or 0.0)})
        samples += 1
        if max_samples is not None and samples >= int(max_samples):
            return
        next_tick = next_tick + max(float(interval or 0.0), 0.001)


async def position_manager_task(client: Any, args: argparse.Namespace, store: RuntimeStateStore, queues: Dict[str, asyncio.Queue], stop_event: asyncio.Event) -> None:
    full_reconcile_interval = compute_position_order_reconcile_interval_seconds(args)
    lightweight_reconcile_interval = 60.0
    last_full_reconcile_at = time.monotonic()
    last_lightweight_reconcile_at = 0.0
    while not stop_event.is_set() or not queues['position_manager'].empty():
        try:
            item = await asyncio.wait_for(queues['position_manager'].get(), timeout=1.0)
        except asyncio.TimeoutError:
            now = time.monotonic()
            if now - last_lightweight_reconcile_at >= lightweight_reconcile_interval:
                last_lightweight_reconcile_at = now
                append_runtime_event(store, 'position_order_lightweight_reconciliation', {'ok': True, 'mode': 'ws_local', 'rest_used': False, 'rest_circuit': _binance_rest_guard_snapshot()})
            if now - last_full_reconcile_at >= full_reconcile_interval:
                last_full_reconcile_at = now
                rest_snapshot = _binance_rest_guard_snapshot()
                if str(rest_snapshot.get('state') or '').upper() in {'OPEN', 'HALF_OPEN', 'DEGRADED'}:
                    append_runtime_event(store, 'skipped_rest_reconcile_due_to_circuit', {'ok': True, 'skipped_rest_reconcile_due_to_circuit': True, 'rest_used_weight_1m': rest_snapshot.get('rest_used_weight_1m'), 'rest_circuit_state': rest_snapshot.get('state'), 'rest_circuit_reason': rest_snapshot.get('reason'), 'next_rest_probe_at': rest_snapshot.get('next_rest_probe_at_ms')})
                else:
                    try:
                        reconcile = await asyncio.wait_for(asyncio.to_thread(reconcile_positions_and_orders, client, store), timeout=float(getattr(args, 'execution_timeout_seconds', 15.0) or 15.0))
                        append_runtime_event(store, 'position_order_reconciliation', dict(reconcile, mode='rest_full', rest_circuit=_binance_rest_guard_snapshot()))
                    except Exception as exc:
                        append_runtime_event(store, 'position_order_reconciliation_failed', {'error': str(exc), 'rest_circuit': _binance_rest_guard_snapshot()})
            record_runtime_heartbeat(store, component='position_manager', status='idle', blocked_reason='', queue_depth=queues['position_manager'].qsize(), queue_maxsize=queues['position_manager'].maxsize)
            continue
        try:
            req = item.get('request') if isinstance(item, dict) else {}
            if isinstance(req, dict) and req.get('kind') == 'position_opened':
                candidate = req.get('candidate')
                trade = req.get('trade') or {}
                meta = req.get('meta') or {}
                position_key = req.get('position_key') or build_position_key(getattr(candidate, 'symbol', req.get('symbol', '')), getattr(candidate, 'position_side', getattr(candidate, 'side', POSITION_SIDE_LONG)))
                await apply_queue_backpressure(queues['manager'], store=store, component='position_manager', reason='manager_queue_full', item={'kind': 'manager_update', 'cycle_no': item.get('cycle_no') if isinstance(item, dict) else None, 'update': {'kind': 'position_opened', 'candidate': candidate, 'symbol': getattr(candidate, 'symbol', req.get('symbol', '')), 'position_key': position_key, 'meta': meta, 'trade': trade, 'cycle': req.get('cycle')}})
        finally:
            queues['position_manager'].task_done()


async def scanner_backoff_wait(store: RuntimeStateStore, queues: Dict[str, asyncio.Queue], stop_event: asyncio.Event, delay_seconds: float, *, cycle_no: int, degraded_wait: bool = False, slice_seconds: float = 1.0) -> str:
    """Wait in cancellable slices, refreshing degraded heartbeats and REST state."""
    deadline = time.monotonic() + max(0.0, float(delay_seconds or 0.0))
    wait_slice = max(0.01, float(slice_seconds or 1.0))
    while not stop_event.is_set():
        remaining = max(0.0, deadline - time.monotonic())
        rest_snapshot = _binance_rest_guard_snapshot()
        if degraded_wait and str(rest_snapshot.get('state') or '').upper() == 'CLOSED':
            return 'circuit_closed'
        if remaining <= 0.0:
            return 'elapsed'
        record_runtime_heartbeat(
            store, component='scanner', status='degraded_wait' if degraded_wait else 'waiting',
            blocked_reason='binance_rest_circuit_open' if degraded_wait else '',
            queue_depth=queues['execution'].qsize(), queue_maxsize=queues['execution'].maxsize,
            extra={'cycle_no': cycle_no, 'backoff_remaining_seconds': remaining, 'rest_circuit': rest_snapshot},
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=min(wait_slice, remaining))
        except asyncio.TimeoutError:
            continue
        return 'stopped'
    return 'stopped'


async def scanner_task(client: Any, args: argparse.Namespace, store: RuntimeStateStore, queues: Dict[str, asyncio.Queue], run_loop_fn: Optional[Callable[[Any, argparse.Namespace], Dict[str, Any]]], stop_event: asyncio.Event) -> None:
    cycle_no = 0
    scanner_timeout_seconds = float(getattr(args, 'scanner_timeout_seconds', 45.0) or 45.0)
    poll_interval = max(0, int(getattr(args, 'poll_interval_sec', 60) or 60))
    while not stop_event.is_set():
        cycle_no += 1
        degraded_wait = False
        try:
            websocket_status = store.load_json('book_ticker_ws_status', None)
            record_runtime_heartbeat(store, component='scanner', status='running', blocked_reason='', queue_depth=queues['execution'].qsize(), queue_maxsize=queues['execution'].maxsize, extra={'cycle_no': cycle_no})
            scan_result = await await_component_with_timeout(
                asyncio.to_thread(scan_only_cycle, client, args, store=store, cycle_no=cycle_no, websocket_status=websocket_status if isinstance(websocket_status, dict) else None),
                scanner_timeout_seconds,
                store=store,
                component='scanner',
                operation='resident_scan_only_cycle',
            )
            if scan_result is None:
                scan_result = {
                    'ok': True,
                    'cycle': {
                        'cycle_no': cycle_no,
                        'blocked_reason': 'scanner_timeout',
                        'scan': {'ok': False, 'blocked_reason': 'scanner_timeout'},
                    },
                    'manager_update': {
                        'kind': 'cycle',
                        'cycle': {
                            'cycle_no': cycle_no,
                            'blocked_reason': 'scanner_timeout',
                            'scan': {'ok': False, 'blocked_reason': 'scanner_timeout'},
                        },
                        'state': 'SCAN',
                        'reason': 'scanner_timeout',
                    },
                }
            cycle = scan_result.get('cycle') if isinstance(scan_result, dict) else {}
            if not isinstance(cycle, dict):
                cycle = {}
            scan_delay_multiplier = 1.0
            if isinstance(scan_result, dict) and scan_result.get('scanner_degraded_wait'):
                scan_delay_multiplier = max(scan_delay_multiplier, float(scan_result.get('scan_delay_multiplier') or 3.0))
            if isinstance(scan_result, dict) and scan_result.get('execution_request'):
                execution_item = {'kind': 'execution_request', 'cycle_no': cycle_no, 'request': scan_result['execution_request']}
                policy = build_backpressure_policy('scanner', 'execution_queue_full', scan_result['execution_request'])
                candidate_score = float(scan_result['execution_request'].get('candidate_score', getattr(scan_result['execution_request'].get('candidate'), 'score', 1.0)) or 0.0)
                min_candidate_score = float(policy.get('min_candidate_score', 0.2) or 0.2)
                if policy.get('drop_candidate') and (queues['execution'].full() or candidate_score < min_candidate_score):
                    drop_payload = {'cycle_no': cycle_no, 'policy': policy, 'reason': 'execution_queue_full' if queues['execution'].full() else 'candidate_score_below_backpressure_minimum', 'candidate_score': candidate_score}
                    await apply_queue_backpressure(queues['manager'], store=store, component='scanner', reason='manager_queue_full', item={'kind': 'manager_update', 'cycle_no': cycle_no, 'update': {'kind': 'event_updates', 'events': [{'event_type': 'runtime_candidate_dropped_by_backpressure', 'payload': drop_payload}]}})
                    scan_delay_multiplier = max(scan_delay_multiplier, float(policy.get('scan_delay_multiplier', 1.0) or 1.0))
                else:
                    bp_result = await apply_queue_backpressure(queues['execution'], store=store, component='scanner', reason='execution_queue_full', item=execution_item)
                    if isinstance(bp_result, dict) and bp_result.get('degraded'):
                        scan_delay_multiplier = max(scan_delay_multiplier, float((bp_result.get('policy') or {}).get('scan_delay_multiplier', 1.0) or 1.0))
            if isinstance(scan_result, dict) and scan_result.get('manager_update'):
                bp_result = await apply_queue_backpressure(queues['manager'], store=store, component='scanner', reason='manager_queue_full', item={'kind': 'manager_update', 'cycle_no': cycle_no, 'update': scan_result['manager_update']})
                if isinstance(bp_result, dict) and bp_result.get('degraded'):
                    scan_delay_multiplier = max(scan_delay_multiplier, float((bp_result.get('policy') or {}).get('scan_delay_multiplier', 1.0) or 1.0))
            hb_status = 'degraded_wait' if isinstance(scan_result, dict) and scan_result.get('scanner_degraded_wait') else 'healthy'
            degraded_wait = hb_status == 'degraded_wait'
            hb_reason = 'binance_rest_circuit_open' if hb_status == 'degraded_wait' else ''
            record_runtime_heartbeat(store, component='scanner', status=hb_status, blocked_reason=hb_reason, queue_depth=queues['execution'].qsize(), queue_maxsize=queues['execution'].maxsize, extra={'cycle_no': cycle_no, 'candidate_found': bool(cycle.get('scan')), 'scan_delay_multiplier': scan_delay_multiplier, 'rest_circuit': _binance_rest_guard_snapshot()})
        except asyncio.TimeoutError:
            record_runtime_heartbeat(store, component='scanner', status='blocked', blocked_reason='scanner_queue_timeout', queue_depth=queues['execution'].qsize(), queue_maxsize=queues['execution'].maxsize, extra={'cycle_no': cycle_no})
        except BinanceAPIError as exc:
            error_message = str(exc)
            blocked_reason = 'binance_rest_circuit_open' if 'circuit open' in error_message.lower() or 'core-only' in error_message.lower() else 'binance_api_error'
            degraded_wait = blocked_reason == 'binance_rest_circuit_open'
            rest_snapshot = _binance_rest_guard_snapshot()
            payload = {'cycle_no': cycle_no, 'blocked_reason': blocked_reason, 'error': error_message, 'scanner_degraded_wait': blocked_reason == 'binance_rest_circuit_open', 'rest_used_weight_1m': rest_snapshot.get('rest_used_weight_1m'), 'rest_circuit_state': rest_snapshot.get('state'), 'rest_circuit_reason': rest_snapshot.get('reason'), 'next_rest_probe_at': rest_snapshot.get('next_rest_probe_at_ms'), 'next_retry_after_seconds': rest_snapshot.get('next_retry_after_seconds')}
            append_runtime_event(store, 'scanner_degraded_wait' if blocked_reason == 'binance_rest_circuit_open' else 'scanner_blocked', payload)
            record_runtime_heartbeat(store, component='scanner', status='degraded_wait' if blocked_reason == 'binance_rest_circuit_open' else 'blocked', blocked_reason=blocked_reason, queue_depth=queues['execution'].qsize(), queue_maxsize=queues['execution'].maxsize, extra=payload)
            scan_delay_multiplier = max(locals().get('scan_delay_multiplier', 1.0), max(3.0, float(rest_snapshot.get('next_retry_after_seconds') or 0) / max(1.0, float(poll_interval or 1))))
        except KeyboardInterrupt:
            store.save_json('resident_last_result', {'ok': True, 'interrupted': True, 'auto_loop': True})
            setattr(stop_event, '_resident_interrupted', True)
            stop_event.set()
            break
        if int(getattr(args, 'max_scan_cycles', 0) or 0) and cycle_no >= int(getattr(args, 'max_scan_cycles', 0) or 0):
            break
        try:
            delay_seconds = poll_interval * locals().get('scan_delay_multiplier', 1.0)
            if degraded_wait:
                await scanner_backoff_wait(
                    store, queues, stop_event,
                    delay_seconds,
                    cycle_no=cycle_no, degraded_wait=True,
                )
            elif delay_seconds > 0:
                # Preserve the normal scheduler path and its KeyboardInterrupt semantics;
                # only REST-circuit waits need sliced, early-wake backoff.
                await asyncio.to_thread(time.sleep, delay_seconds)
        except KeyboardInterrupt:
            store.save_json('resident_last_result', {'ok': True, 'interrupted': True, 'auto_loop': True})
            setattr(stop_event, '_resident_interrupted', True)
            stop_event.set()
            break

async def execution_task(client: Any, args: argparse.Namespace, store: RuntimeStateStore, queues: Dict[str, asyncio.Queue], stop_event: asyncio.Event) -> None:
    execution_timeout_seconds = float(getattr(args, 'execution_timeout_seconds', 30.0) or 30.0) + 5.0
    while not stop_event.is_set() or not queues['execution'].empty():
        try:
            item = await asyncio.wait_for(queues['execution'].get(), timeout=1.0)
        except asyncio.TimeoutError:
            record_runtime_heartbeat(store, component='execution', status='idle', blocked_reason='', queue_depth=queues['execution'].qsize(), queue_maxsize=queues['execution'].maxsize)
            continue
        try:
            record_runtime_heartbeat(store, component='execution', status='running', blocked_reason='', queue_depth=queues['execution'].qsize(), queue_maxsize=queues['execution'].maxsize, extra={'kind': item.get('kind'), 'cycle_no': item.get('cycle_no')})
            result = await await_component_with_timeout(
                asyncio.to_thread(execution_cycle, client, args, item.get('request') or {}, store=store),
                execution_timeout_seconds,
                store=store,
                component='execution',
                operation='resident_execution_cycle',
            )
            if isinstance(result, dict) and result.get('manager_update'):
                await apply_queue_backpressure(queues['manager'], store=store, component='execution', reason='manager_queue_full', item={'kind': 'manager_update', 'cycle_no': item.get('cycle_no'), 'update': result['manager_update']})
            if isinstance(result, dict) and result.get('position_manager_request'):
                await apply_queue_backpressure(queues['position_manager'], store=store, component='execution', reason='position_manager_queue_full', item={'kind': 'position_manager_request', 'cycle_no': item.get('cycle_no'), 'request': result['position_manager_request']})
            await apply_queue_backpressure(queues['manager'], store=store, component='execution', reason='manager_queue_full', item={'kind': 'manager_update', 'cycle_no': item.get('cycle_no'), 'update': {'kind': 'runtime_event', 'event_type': 'resident_execution_completed', 'payload': result if isinstance(result, dict) else {'result': result}}})
            record_runtime_heartbeat(store, component='execution', status='healthy', blocked_reason='', queue_depth=queues['execution'].qsize(), queue_maxsize=queues['execution'].maxsize, extra={'cycle_no': item.get('cycle_no')})
        finally:
            queues['execution'].task_done()

async def manager_task(args: argparse.Namespace, store: RuntimeStateStore, queues: Dict[str, asyncio.Queue], stop_event: asyncio.Event) -> None:
    last_result: Dict[str, Any] = {'ok': True, 'cycles': []}
    while not stop_event.is_set() or not queues['manager'].empty():
        try:
            item = await asyncio.wait_for(queues['manager'].get(), timeout=1.0)
        except asyncio.TimeoutError:
            record_runtime_heartbeat(store, component='manager', status='idle', blocked_reason='', queue_depth=queues['manager'].qsize(), queue_maxsize=queues['manager'].maxsize)
            continue
        try:
            update = item.get('update') if item.get('kind') == 'manager_update' else item.get('result')
            if isinstance(update, dict):
                if update.get('kind') == 'event_updates':
                    for event in list(update.get('events') or []):
                        if isinstance(event, dict):
                            append_runtime_event(store, str(event.get('event_type') or 'runtime_event'), event.get('payload') if isinstance(event.get('payload'), dict) else event)
                    pass
                else:
                    management_cycle(args, update, store=store)
                cycle = update.get('cycle') if isinstance(update.get('cycle'), dict) else None
                if cycle is not None:
                    last_result = {'ok': True, 'cycles': [cycle], 'cycle_no': item.get('cycle_no'), 'auto_loop': True}
                    store.save_json('resident_last_result', last_result)
            record_runtime_heartbeat(store, component='manager', status='healthy', blocked_reason='', queue_depth=queues['manager'].qsize(), queue_maxsize=queues['manager'].maxsize, extra={'kind': item.get('kind'), 'cycle_no': item.get('cycle_no')})
        finally:
            queues['manager'].task_done()


_BOOK_TICKER_WS_SUPERVISOR_ACTIVE = False


async def ws_task(client: Any, args: argparse.Namespace, store: RuntimeStateStore, stop_event: asyncio.Event) -> None:
    """Single resident websocket actor: start one supervisor, then monitor freshness."""
    global _BOOK_TICKER_WS_SUPERVISOR_ACTIVE
    interval = max(1.0, float(getattr(args, 'websocket_healthcheck_interval_seconds', 15.0) or 15.0))
    timeout_seconds = float(getattr(args, 'websocket_healthcheck_timeout_seconds', 10.0) or 10.0)
    stale_seconds = float(getattr(args, 'book_ticker_ws_stale_seconds', 30.0) or 30.0)
    restart_backoff_seconds = max(1.0, float(getattr(args, 'websocket_restart_backoff_seconds', 5.0) or 5.0))
    last_restart_at = 0.0

    async def start_or_recover_supervisor(trigger: str) -> None:
        nonlocal last_restart_at
        global _BOOK_TICKER_WS_SUPERVISOR_ACTIVE
        now = time.monotonic()
        if trigger != 'initial_start' and now - last_restart_at < restart_backoff_seconds:
            return
        if _BOOK_TICKER_WS_SUPERVISOR_ACTIVE:
            if trigger == 'initial_start':
                return
            append_runtime_event(store, 'book_ticker_ws_singleton_recovery_requested', {'trigger': trigger, 'action': 'forced_restart'})
            force_close_book_ticker_websocket_supervisor(store, reason=trigger)
            _BOOK_TICKER_WS_SUPERVISOR_ACTIVE = False
        last_restart_at = now
        _BOOK_TICKER_WS_SUPERVISOR_ACTIVE = True
        result = await await_component_with_timeout(
            asyncio.to_thread(run_auto_loop_book_ticker_websocket_monitor, client=client, store=store, args=args),
            timeout_seconds,
            store=store,
            component='ws',
            operation='book_ticker_resident_supervisor_start',
        )
        record_runtime_heartbeat(store, component='ws', status='healthy', blocked_reason='', extra={'trigger': trigger, 'summary': result.get('summary') if isinstance(result, dict) else {}})

    try:
        await start_or_recover_supervisor('initial_start')
    except Exception as exc:
        record_runtime_heartbeat(store, component='ws', status='restarting', blocked_reason='websocket_initial_start_exception', extra={'error': str(exc), 'error_type': type(exc).__name__})

    while not stop_event.is_set():
        try:
            health = store.load_json('book_ticker_ws_status', {})
            freshness = evaluate_websocket_freshness(
                health if isinstance(health, dict) else {},
                max_age_seconds=stale_seconds,
                require_messages=bool(getattr(args, 'require_book_ticker_ws_messages', True)),
            )
            state = str(freshness.get('state') or 'dead')
            if freshness.get('fresh'):
                record_runtime_heartbeat(store, component='ws', status='healthy', blocked_reason='', extra={'freshness': freshness})
            elif state != 'dead':
                reason = str(freshness.get('reason') or 'websocket_degraded')
                record_runtime_heartbeat(store, component='ws', status='degraded', blocked_reason=f'websocket_degraded:{reason}', extra={'freshness': freshness})
            else:
                reason = str(freshness.get('reason') or 'stale_websocket')
                payload = record_runtime_heartbeat(store, component='ws', status='restarting', blocked_reason=f'websocket_stale:{reason}', extra={'freshness': freshness, 'health': health if isinstance(health, dict) else {}})
                append_runtime_event(store, 'book_ticker_ws_stale_recovery', payload)
                await start_or_recover_supervisor(f'stale:{reason}')
        except Exception as exc:
            record_runtime_heartbeat(store, component='ws', status='restarting', blocked_reason='websocket_healthcheck_exception', extra={'error': str(exc), 'error_type': type(exc).__name__})
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
    _BOOK_TICKER_WS_SUPERVISOR_ACTIVE = False


async def watchdog_task(store: RuntimeStateStore, queues: Dict[str, asyncio.Queue], stop_event: asyncio.Event, *, interval: float = 5.0, max_samples: Optional[int] = None, stale_seconds: float = 120.0, event_loop_lag_seconds: float = 2.0) -> None:
    samples = 0
    while not stop_event.is_set() or (max_samples is not None and samples < int(max_samples)):
        actions: List[str] = []
        for name, queue in queues.items():
            if queue.full():
                payload = record_runtime_heartbeat(store, component='watchdog', status='blocked', blocked_reason=f'{name}_queue_full', queue_depth=queue.qsize(), queue_maxsize=queue.maxsize)
                append_runtime_event(store, 'resident_queue_backlog', payload)
                actions.append(f'{name}_queue_backlog')
        heartbeat = store.load_json('runtime_heartbeat', {})
        components = heartbeat.get('components') if isinstance(heartbeat, dict) and isinstance(heartbeat.get('components'), dict) else heartbeat if isinstance(heartbeat, dict) else {}
        now_ts = time.time()
        checks = [('scanner', 'last_scan_ts', 'scanner_stale'), ('ws', 'last_ws_msg_ts', 'ws_stale'), ('execution', 'last_execution_ts', 'execution_stale')]
        for component, key, action in checks:
            row = components.get(component, {}) if isinstance(components, dict) else {}
            extra = row.get('extra', {}) if isinstance(row, dict) and isinstance(row.get('extra'), dict) else {}
            ts = extra.get(key) if isinstance(extra, dict) else None
            if ts is None and isinstance(row, dict):
                ts = row.get(key)
            if ts is None and isinstance(row, dict):
                ts = row.get('updated_at_ts')
            if ts is None and isinstance(row, dict):
                updated_at = row.get('updated_at')
                if updated_at:
                    try:
                        ts = datetime.datetime.fromisoformat(str(updated_at).replace('Z', '+00:00')).timestamp()
                    except ValueError:
                        ts = None
            try:
                stale = ts is None or now_ts - float(ts) > float(stale_seconds)
            except (TypeError, ValueError):
                stale = True
            if stale:
                actions.append(action)
        event_loop_row = components.get('event_loop', {}) if isinstance(components, dict) else {}
        event_loop_extra = event_loop_row.get('extra', {}) if isinstance(event_loop_row, dict) else {}
        try:
            if float(event_loop_extra.get('lag_seconds', 0.0) or 0.0) >= float(event_loop_lag_seconds):
                actions.append('event_loop_lag')
        except (TypeError, ValueError):
            pass
        if actions:
            now = time.time()
            state = store.load_json('runtime_watchdog_state', {})
            if not isinstance(state, dict):
                state = {}
            cooldown = max(0.0, float(state.get('cooldown_seconds', 60.0) or 60.0))
            max_per_hour = max(1, int(state.get('max_restart_per_hour', 6) or 6))
            last_restart_at = float(state.get('last_restart_at', 0.0) or 0.0)
            restart_times = [float(ts) for ts in list(state.get('restart_times') or []) if now - float(ts or 0.0) <= 3600.0]
            payload = record_runtime_heartbeat(store, component='watchdog', status='recovering', blocked_reason=';'.join(sorted(set(actions))), extra={'actions': sorted(set(actions)), 'restart_count_last_hour': len(restart_times), 'cooldown_seconds': cooldown, 'max_restart_per_hour': max_per_hour})
            if now - last_restart_at < cooldown:
                skipped = {**payload, 'action': 'cooldown_skip', 'cooldown_remaining_seconds': round(cooldown - (now - last_restart_at), 3)}
                append_runtime_event(store, 'resident_watchdog_recovery_cooldown', skipped)
            elif len(restart_times) >= max_per_hour:
                halted = {**payload, 'action': 'halted', 'reason': 'watchdog_restart_limit_exceeded', 'restart_count_last_hour': len(restart_times), 'max_restart_per_hour': max_per_hour}
                state.update({'status': 'HALTED', 'halted_at': _isoformat_utc(_utc_now()), 'halt_reason': 'watchdog_restart_limit_exceeded', 'restart_times': restart_times})
                store.save_json('runtime_watchdog_state', state)
                store.save_json('runtime_recovery_request', halted)
                append_runtime_event(store, 'resident_watchdog_halted', halted)
            else:
                restart_times.append(now)
                state.update({'status': 'recovering', 'last_restart_at': now, 'restart_count': int(state.get('restart_count', 0) or 0) + 1, 'restart_times': restart_times, 'cooldown_seconds': cooldown, 'max_restart_per_hour': max_per_hour})
                store.save_json('runtime_watchdog_state', state)
                recovery_request = {**payload, 'action': 'supervisor_restart', 'actions': sorted(set(actions)), 'restart_count_last_hour': len(restart_times), 'restart_count': state['restart_count']}
                store.save_json('runtime_recovery_request', recovery_request)
                append_runtime_event(store, 'resident_watchdog_recovery', recovery_request)
        else:
            now = time.time()
            state = store.load_json('runtime_watchdog_state', {})
            if not isinstance(state, dict):
                state = {}
            restart_times = [float(ts) for ts in list(state.get('restart_times') or []) if now - float(ts or 0.0) <= 3600.0]
            state.update({
                'status': 'healthy',
                'last_healthy_at': _isoformat_utc(_utc_now()),
                'restart_times': restart_times,
                'restart_count_last_hour': len(restart_times),
            })
            store.save_json('runtime_watchdog_state', state)
            recovery_request = store.load_json('runtime_recovery_request', {})
            if isinstance(recovery_request, dict) and recovery_request.get('consumed'):
                store.save_json('runtime_recovery_request', {})
            record_runtime_heartbeat(store, component='watchdog', status='healthy', blocked_reason='', extra={'actions': [], 'restart_count_last_hour': len(restart_times)})
        samples += 1
        if max_samples is not None and samples >= int(max_samples):
            return
        await asyncio.sleep(interval)


def build_async_runtime_task_queues(maxsize: int) -> Dict[str, asyncio.Queue]:
    size = max(1, int(maxsize or 1))
    return {'scanner': asyncio.Queue(maxsize=size), 'execution': asyncio.Queue(maxsize=size), 'manager': asyncio.Queue(maxsize=size), 'position_manager': asyncio.Queue(maxsize=size)}


async def run_resident_runtime_async(client: Any, args: argparse.Namespace, run_loop_fn: Callable[[Any, argparse.Namespace], Dict[str, Any]]) -> Dict[str, Any]:
    store = get_runtime_state_store(args)
    queues = build_async_runtime_task_queues(int(getattr(args, 'runtime_queue_maxsize', 128) or 128))
    stop_event = asyncio.Event()
    resident_started_at = _utc_now()
    record_runtime_heartbeat(store, component='resident', status='starting', blocked_reason='', extra={'tasks': ['scanner', 'execution', 'manager', 'position_manager', 'ws', 'watchdog', 'event_loop', 'ticker_24hr_cache_refresher'], 'resident_started_at': _isoformat_utc(resident_started_at)})
    start_ticker_24hr_cache_refresher(client, args, store)
    try:
        startup_reconcile = await asyncio.wait_for(asyncio.to_thread(reconcile_positions_and_orders, client, store), timeout=float(getattr(args, 'execution_timeout_seconds', 15.0) or 15.0))
        append_runtime_event(store, 'startup_position_order_reconciliation', startup_reconcile)
    except Exception as exc:
        append_runtime_event(store, 'startup_position_order_reconciliation_failed', {'error': str(exc)})
    tasks: List[asyncio.Task] = []

    def drain_runtime_queues() -> None:
        # Shutdown cleanup may discard only lossy scan/cycle summaries. Critical execution
        # requests, position_opened, execution_result/error and state transitions remain
        # queued so replay/persistence paths can preserve them across supervisor restarts.
        queue = queues.get('manager')
        if queue is None:
            return
        kept: List[Any] = []
        dropped = 0
        while True:
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if is_coalescable_manager_update(item):
                dropped += 1
                queue.task_done()
            else:
                kept.append(item)
        for item in kept:
            try:
                queue.put_nowait(item)
            except asyncio.QueueFull:
                append_runtime_event(store, 'resident_shutdown_queue_replay_backlog', {'queue': 'manager', 'item_kind': item.get('kind') if isinstance(item, dict) else type(item).__name__})
        if dropped:
            append_runtime_event(store, 'resident_shutdown_dropped_lossy_summaries', {'queue': 'manager', 'dropped': dropped})

    async def stop_runtime_tasks(reason: str) -> None:
        stop_event.set()
        force_close_book_ticker_websocket_supervisor(store, reason=reason)
        shutdown_timeout = max(0.01, float(getattr(args, 'resident_shutdown_timeout_seconds', 5.0) or 5.0))
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=shutdown_timeout)
        except asyncio.TimeoutError:
            stuck = [task.get_name() for task in tasks if not task.done()]
            for task in tasks:
                if not task.done():
                    task.cancel()
            append_runtime_event(store, 'resident_shutdown_forced_cancel', {'stuck_tasks': stuck, 'timeout_seconds': shutdown_timeout, 'reason': reason})
            await asyncio.gather(*tasks, return_exceptions=True)
        tasks.clear()
        drain_runtime_queues()

    async def start_runtime_tasks() -> asyncio.Task:
        nonlocal stop_event
        stop_event = asyncio.Event()
        if bool(getattr(args, 'require_book_ticker_ws', True)):
            tasks.append(asyncio.create_task(ws_task(client, args, store, stop_event), name='ws_task'))
            await asyncio.sleep(0)
        tasks.extend([
            asyncio.create_task(scanner_task(client, args, store, queues, run_loop_fn, stop_event), name='scanner_task'),
            asyncio.create_task(execution_task(client, args, store, queues, stop_event), name='execution_task'),
            asyncio.create_task(manager_task(args, store, queues, stop_event), name='manager_task'),
            asyncio.create_task(position_manager_task(client, args, store, queues, stop_event), name='position_manager_task'),
            asyncio.create_task(watchdog_task(
                store,
                queues,
                stop_event,
                stale_seconds=max(
                    120.0,
                    float(getattr(args, 'scanner_timeout_seconds', 120.0) or 120.0) + float(getattr(args, 'poll_interval_sec', 0.0) or 0.0) + 30.0,
                ),
            ), name='watchdog_task'),
            asyncio.create_task(event_loop_latency_task(store, stop_event, interval=float(getattr(args, 'event_loop_lag_interval_seconds', 1.0) or 1.0), warn_threshold_seconds=float(getattr(args, 'event_loop_lag_warn_seconds', 0.25) or 0.25)), name='event_loop_latency_task'),
        ])
        return next(task for task in tasks if task.get_name() == 'scanner_task')

    scanner_runtime_task = await start_runtime_tasks()
    record_runtime_heartbeat(
        store,
        component='resident',
        status='running',
        blocked_reason='',
        extra={
            'tasks': [task.get_name() for task in tasks] + ['ticker_24hr_cache_refresher'],
            'resident_started_at': _isoformat_utc(resident_started_at),
            'tasks_started': True,
            'scanner_task': scanner_runtime_task.get_name(),
        },
    )
    try:
        while True:
            recovery_request = store.load_json('runtime_recovery_request', {})
            if isinstance(recovery_request, dict) and recovery_request.get('action') and not recovery_request.get('consumed'):
                request_updated_at = _parse_iso8601_utc(recovery_request.get('updated_at'))
                if request_updated_at is not None and request_updated_at < resident_started_at:
                    stale_consumed = dict(recovery_request, consumed=True, ignored=True, consumed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), ignore_reason='stale_recovery_request_from_previous_runtime')
                    store.save_json('runtime_recovery_request', stale_consumed)
                    append_runtime_event(store, 'resident_recovery_request_ignored_stale', stale_consumed)
                    recovery_request = stale_consumed
            if isinstance(recovery_request, dict) and recovery_request.get('action') == 'halted' and not recovery_request.get('consumed'):
                append_runtime_event(store, 'resident_supervisor_halted_by_watchdog', recovery_request)
                record_runtime_heartbeat(store, component='resident', status='halted', blocked_reason=str(recovery_request.get('reason') or 'watchdog_halted'), extra={'recovery_request': recovery_request})
                return {'ok': False, 'auto_loop': True, 'reason': str(recovery_request.get('reason') or 'watchdog_halted'), 'recovery_request': recovery_request}
            if isinstance(recovery_request, dict) and recovery_request.get('action') == 'supervisor_restart' and not recovery_request.get('consumed'):
                request_updated_at = _parse_iso8601_utc(recovery_request.get('updated_at'))
                if request_updated_at is not None and request_updated_at < resident_started_at:
                    stale_consumed = dict(recovery_request, consumed=True, ignored=True, consumed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), ignore_reason='stale_recovery_request_from_previous_runtime')
                    store.save_json('runtime_recovery_request', stale_consumed)
                    append_runtime_event(store, 'resident_supervisor_restart_ignored_stale', stale_consumed)
                else:
                    consumed = dict(recovery_request, consumed=True, consumed_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
                    store.save_json('runtime_recovery_request', consumed)
                    append_runtime_event(store, 'resident_supervisor_restart_consumed', consumed)
                    record_runtime_heartbeat(store, component='resident', status='restarting', blocked_reason='watchdog_recovery_request', extra={'recovery_request': consumed})
                    await stop_runtime_tasks('watchdog_recovery_request')
                    scanner_runtime_task = await start_runtime_tasks()
                    record_runtime_heartbeat(store, component='resident', status='running', blocked_reason='', extra={'recovery_request': consumed, 'scanner_task': scanner_runtime_task.get_name()})
            supervised_tasks = {
                task for task in tasks
                if task.get_name() in {'scanner_task', 'execution_task', 'manager_task', 'position_manager_task'}
            }
            done, _pending = await asyncio.wait(supervised_tasks, timeout=1.0)
            failed_actors = [
                task for task in done
                if task.get_name() != 'scanner_task' and not task.cancelled() and task.exception() is not None
            ]
            if failed_actors and not stop_event.is_set():
                failed = failed_actors[0]
                actor_exc = failed.exception()
                payload = {'task': failed.get_name(), 'error': str(actor_exc), 'error_type': type(actor_exc).__name__, 'recovery': 'restart_runtime_tasks'}
                append_runtime_event(store, 'resident_actor_task_failed', payload)
                record_runtime_heartbeat(store, component='resident', status='recovering', blocked_reason='actor_task_failed', extra=payload)
                await stop_runtime_tasks('actor_task_failed')
                scanner_runtime_task = await start_runtime_tasks()
                append_runtime_event(store, 'resident_actor_failure_recovered', payload)
                record_runtime_heartbeat(store, component='resident', status='running', blocked_reason='', extra={'recovered_task': failed.get_name(), 'scanner_task': scanner_runtime_task.get_name()})
                continue
            if scanner_runtime_task in done:
                max_scan_cycles = int(getattr(args, 'max_scan_cycles', 0) or 0)
                scanner_exc = scanner_runtime_task.exception()
                if scanner_exc is not None:
                    payload = {
                        'task': scanner_runtime_task.get_name(),
                        'error': str(scanner_exc),
                        'error_type': type(scanner_exc).__name__,
                        'max_scan_cycles': max_scan_cycles,
                    }
                    append_runtime_event(store, 'resident_scanner_task_failed', payload)
                    record_runtime_heartbeat(store, component='resident', status='recovering', blocked_reason='scanner_task_failed', extra=payload)
                    if max_scan_cycles == 0 and not stop_event.is_set():
                        scanner_runtime_task = asyncio.create_task(scanner_task(client, args, store, queues, run_loop_fn, stop_event), name='scanner_task')
                        tasks.append(scanner_runtime_task)
                        append_runtime_event(store, 'resident_scanner_task_restarted', {'reason': 'scanner_task_failed'})
                        record_runtime_heartbeat(store, component='resident', status='running', blocked_reason='', extra={'restarted_task': scanner_runtime_task.get_name(), 'previous_error_type': type(scanner_exc).__name__})
                        continue
                    raise scanner_exc
                if max_scan_cycles == 0 and not stop_event.is_set():
                    append_runtime_event(store, 'resident_scanner_task_unexpected_exit', {'reason': 'scanner_task_completed_in_unlimited_runtime'})
                    record_runtime_heartbeat(store, component='resident', status='recovering', blocked_reason='scanner_task_completed_unexpectedly')
                    scanner_runtime_task = asyncio.create_task(scanner_task(client, args, store, queues, run_loop_fn, stop_event), name='scanner_task')
                    tasks.append(scanner_runtime_task)
                    record_runtime_heartbeat(store, component='resident', status='running', blocked_reason='', extra={'restarted_task': scanner_runtime_task.get_name(), 'previous_exit': 'scanner_task_completed_unexpectedly'})
                    continue
                break
        await queues['execution'].join()
        await queues['manager'].join()
        await queues['position_manager'].join()
    finally:
        await stop_runtime_tasks('runtime_shutdown')
    record_runtime_heartbeat(store, component='resident', status='stopped', blocked_reason='')
    if bool(getattr(stop_event, '_resident_interrupted', False)):
        return {'ok': True, 'interrupted': True, 'auto_loop': True}
    last_result = store.load_json('resident_last_result', {'ok': True, 'auto_loop': True, 'cycles': []})
    return last_result if isinstance(last_result, dict) else {'ok': True, 'auto_loop': True, 'cycles': []}


def run_resident_runtime(client: Any, args: argparse.Namespace, run_loop_fn: Optional[Callable[[Any, argparse.Namespace], Dict[str, Any]]] = None) -> Dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(run_resident_runtime_async(client, args, run_loop_fn))
    result_holder: Dict[str, Any] = {}
    error_holder: Dict[str, BaseException] = {}
    def runner() -> None:
        try:
            result_holder['result'] = asyncio.run(run_resident_runtime_async(client, args, run_loop_fn))
        except BaseException as exc:
            error_holder['error'] = exc
    thread = threading.Thread(target=runner, name='resident-runtime-event-loop', daemon=True)
    thread.start()
    thread.join()
    if 'error' in error_holder:
        raise error_holder['error']
    return result_holder.get('result', {'ok': True, 'auto_loop': True, 'cycles': []})


def run_supervised_auto_loop(client: Any, args: argparse.Namespace, run_loop_fn: Callable[[Any, argparse.Namespace], Dict[str, Any]]) -> Dict[str, Any]:
    store = get_runtime_state_store(args)
    max_cycles = int(getattr(args, 'max_scan_cycles', 0) or 0)
    poll_interval = max(0, int(getattr(args, 'poll_interval_sec', 60) or 60))
    restart_limit = max(0, int(getattr(args, 'supervisor_restart_limit', 3) or 3))
    cycle_no = 0
    restart_count = 0
    last_result: Dict[str, Any] = {'ok': True, 'cycles': []}
    record_runtime_heartbeat(store, component='supervisor', status='running', blocked_reason='', extra={'restart_limit': restart_limit})
    start_ticker_24hr_cache_refresher(client, args, store)
    while max_cycles == 0 or cycle_no < max_cycles:
        cycle_no += 1
        try:
            last_result = run_loop_fn(client, args)
            restart_count = 0
            if isinstance(last_result, dict):
                last_result = dict(last_result)
                last_result['cycle_no'] = cycle_no
                last_result['auto_loop'] = True
            else:
                last_result = {'ok': False, 'cycle_no': cycle_no, 'auto_loop': True, 'reason': 'invalid_run_loop_result'}
            record_runtime_heartbeat(store, component='supervisor', status='running', blocked_reason='', extra={'cycle_no': cycle_no, 'restart_count': restart_count})
            print_scan_output(last_result, getattr(args, 'output_format', 'json'))
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            restart_count += 1
            payload = {
                'cycle_no': cycle_no,
                'restart_count': restart_count,
                'restart_limit': restart_limit,
                'error': str(exc),
                'error_type': type(exc).__name__,
            }
            if restart_count > restart_limit:
                halted = record_runtime_heartbeat(store, component='supervisor', status='halted', blocked_reason='restart_limit_exceeded', extra=payload)
                append_runtime_event(store, 'supervisor_halted', halted)
                last_result = {'ok': False, 'reason': 'supervisor_restart_limit_exceeded', 'cycle_no': cycle_no, 'auto_loop': True, 'error': str(exc)}
                print_scan_output(last_result, getattr(args, 'output_format', 'json'))
                return last_result
            restarting = record_runtime_heartbeat(store, component='supervisor', status='restarting', blocked_reason='cycle_exception', extra=payload)
            append_runtime_event(store, 'supervisor_restart', restarting)
            last_result = {'ok': False, 'reason': 'supervisor_restart', 'cycle_no': cycle_no, 'auto_loop': True, 'error': str(exc)}
            print_scan_output(last_result, getattr(args, 'output_format', 'json'))
        if max_cycles and cycle_no >= max_cycles:
            break
        time.sleep(poll_interval)
    return last_result


def print_scan_output(result: Dict[str, Any], output_format: str) -> None:
    if output_format == 'json':
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    print(render_cn_scan_summary(result))


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        args.runtime_state_dir = str(
            validate_runtime_state_layout(
                getattr(args, 'runtime_state_dir', CANONICAL_RUNTIME_STATE_DIR),
                canonical_dir=CANONICAL_RUNTIME_STATE_DIR,
                legacy_dir=LEGACY_RUNTIME_STATE_DIR,
            )
        )
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    load_dotenv()
    args = apply_runtime_profile(args)
    if bool(getattr(args, 'enable_trade_memory', False)):
        memory_store = get_runtime_state_store(args)
        args = apply_adaptive_trade_memory_profile(args, TradeMemoryEngine(memory_store).aggregate())
    try:
        args.runtime_state_dir = str(
            validate_runtime_state_layout(
                getattr(args, 'runtime_state_dir', CANONICAL_RUNTIME_STATE_DIR),
                canonical_dir=CANONICAL_RUNTIME_STATE_DIR,
                legacy_dir=LEGACY_RUNTIME_STATE_DIR,
            )
        )
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    configure_binance_rest_guard_store(getattr(args, 'runtime_state_dir', CANONICAL_RUNTIME_STATE_DIR))
    if is_binance_simulated_trading(args) and 'base_url' not in set(getattr(args, '_explicit_cli_dests', set()) or set()):
        args.base_url = 'https://testnet.binancefuture.com'
    binance_api_key, binance_api_secret = resolve_binance_api_credentials(args)
    client = BinanceFuturesClient(
        base_url=args.base_url,
        api_key=binance_api_key,
        api_secret=binance_api_secret,
        scanner_proxy_urls=getattr(args, 'scanner_proxy_urls', ''),
        allow_heavy_history_rest=bool(getattr(args, 'allow_heavy_history_rest', False)),
    )
    run_loop_fn = globals().get('run_loop')
    if not callable(run_loop_fn):
        result, _, _ = run_scan_once(client, args)
        print_scan_output(result, args.output_format)
        return 0

    if getattr(args, 'auto_loop', False):
        try:
            result = run_resident_runtime(client, args, run_loop_fn)
            print_scan_output(result, args.output_format)
        except KeyboardInterrupt:
            interrupted = {'ok': True, 'interrupted': True, 'auto_loop': True}
            print_scan_output(interrupted, args.output_format)
            return 0
        return 0

    result = run_loop_fn(client, args)
    print_scan_output(result, args.output_format)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
