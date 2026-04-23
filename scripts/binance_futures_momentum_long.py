from __future__ import annotations

import argparse
import datetime
import hashlib
import hmac
import json
import math
import os
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlencode

import requests


class BinanceAPIError(RuntimeError):
    pass


class BinanceFuturesClient:
    def __init__(self, base_url: str, api_key: str = '', api_secret: str = '', session: Optional[requests.Session] = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key or ''
        self.api_secret = api_secret or ''
        self.session = session or requests.Session()
        if self.api_key:
            self.session.headers.setdefault('X-MBX-APIKEY', self.api_key)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15):
        response = self.session.get(f'{self.base_url}{path}', params=params or {}, timeout=timeout)
        self._raise_for_status(response)
        return response.json()

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
        payload = dict(params)
        payload.setdefault('timestamp', int(time.time() * 1000))
        query = urlencode(payload, doseq=True)
        signature = hmac.new(self.api_secret.encode('utf-8'), query.encode('utf-8'), hashlib.sha256).hexdigest()
        payload['signature'] = signature
        url = f'{self.base_url}{path}'
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
        self._raise_for_status(response)
        return response.json()

    @staticmethod
    def _raise_for_status(response):
        if response.status_code >= 400:
            try:
                payload = response.json()
            except Exception:
                payload = response.text
            raise BinanceAPIError(f'Binance API error {response.status_code}: {payload}')




POSITION_SIDE_LONG = 'LONG'
POSITION_SIDE_SHORT = 'SHORT'
TRADE_SIDE_LONG = 'long'
TRADE_SIDE_SHORT = 'short'


def normalize_trade_side(side: Any, default: str = TRADE_SIDE_LONG) -> str:
    normalized = str(side or '').strip().lower()
    return TRADE_SIDE_SHORT if normalized == TRADE_SIDE_SHORT else default


def position_side_to_trade_side(side: Any, default: str = TRADE_SIDE_LONG) -> str:
    normalized = normalize_position_side(side, POSITION_SIDE_SHORT if str(default).lower() == TRADE_SIDE_SHORT else POSITION_SIDE_LONG)
    return TRADE_SIDE_SHORT if normalized == POSITION_SIDE_SHORT else TRADE_SIDE_LONG


def trade_side_to_position_side(side: Any, default: str = POSITION_SIDE_LONG) -> str:
    normalized = normalize_trade_side(side, TRADE_SIDE_SHORT if str(default).upper() == POSITION_SIDE_SHORT else TRADE_SIDE_LONG)
    return POSITION_SIDE_SHORT if normalized == TRADE_SIDE_SHORT else POSITION_SIDE_LONG


def normalize_position_side(side: Any, default: str = POSITION_SIDE_LONG) -> str:
    normalized = str(side or '').strip().upper()
    fallback = str(default or POSITION_SIDE_LONG).strip().upper()
    return POSITION_SIDE_SHORT if normalized == POSITION_SIDE_SHORT else POSITION_SIDE_LONG if normalized == POSITION_SIDE_LONG else POSITION_SIDE_SHORT if fallback == POSITION_SIDE_SHORT else POSITION_SIDE_LONG


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


def materialize_positions_state(positions_state: Dict[str, Any], original_keys: Optional[Dict[str, str]] = None, include_legacy_alias: bool = True) -> Dict[str, Any]:
    materialized: Dict[str, Any] = {}
    key_hints = dict(original_keys or {})
    for position_key, tracked in list((positions_state or {}).items()):
        if not isinstance(tracked, dict):
            continue
        symbol = str(tracked.get('symbol') or split_position_key(position_key)[0]).upper()
        side = normalize_position_side(tracked.get('side') or split_position_key(position_key)[1])
        canonical_key = build_position_key(symbol, side)
        normalized = dict(tracked)
        normalized['symbol'] = symbol
        normalized['side'] = side
        normalized['position_key'] = canonical_key
        materialized[canonical_key] = normalized
        prefer_legacy = is_legacy_position_key(key_hints.get(canonical_key))
        if include_legacy_alias:
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

    def __post_init__(self) -> None:
        self.side = normalize_trade_side(self.side)
        self.position_side = normalize_position_side(self.position_side, trade_side_to_position_side(self.side))
        self.side = position_side_to_trade_side(self.position_side, self.side)
        if not self.position_key and self.symbol:
            self.position_key = build_position_key(self.symbol, self.position_side)


@dataclass
class RuntimeStateStore:
    runtime_state_dir: str

    def _dir(self) -> Path:
        return Path(self.runtime_state_dir).expanduser()

    def _path(self) -> Path:
        return self._dir() / 'runtime_state.json'

    def _json_path(self, name: str) -> Path:
        return self._dir() / f'{name}.json'

    def _events_path(self) -> Path:
        return self._dir() / 'events.jsonl'

    def load(self) -> Dict[str, Any]:
        path = self._path()
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            return {}

    def save(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        path = self._path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return payload

    def load_json(self, name: str, default: Any = None) -> Any:
        path = self._json_path(name)
        if not path.exists():
            return default
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            return default
        if name == 'positions':
            migrated = migrate_positions_state(payload)
            materialized = materialize_positions_state(migrated, include_legacy_alias=True)
            if materialized != payload:
                path.write_text(json.dumps(materialized, ensure_ascii=False, indent=2), encoding='utf-8')
            return materialized
        return payload

    def save_json(self, name: str, payload: Any) -> Any:
        path = self._json_path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        normalized_payload = materialize_positions_state(migrate_positions_state(payload), include_legacy_alias=True) if name == 'positions' else payload
        path.write_text(json.dumps(normalized_payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return normalized_payload

    def append_event(self, event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        path = self._events_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        row = {'event_type': event_type, **normalize_runtime_event_payload(payload or {})}
        with path.open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + '\n')
        return row

    def read_events(self, limit: int = 1000) -> List[Dict[str, Any]]:
        path = self._events_path()
        if not path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        max_rows = max(int(limit or 0), 1)
        try:
            with path.open('r', encoding='utf-8') as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(row, dict):
                        rows.append(row)
        except Exception:
            return []
        if len(rows) <= max_rows:
            return rows
        return rows[-max_rows:]


def append_runtime_event(store: Optional[RuntimeStateStore], event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    row = {'event_type': event_type, **(payload or {})}
    if store is None:
        return row
    return store.append_event(event_type, payload)


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
    inferred_position_side = POSITION_SIDE_SHORT if entry_side == 'SELL' else POSITION_SIDE_LONG
    position_key, position = get_position_by_symbol_side(positions_state, symbol, inferred_position_side)
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
    'extended_chase_veto': 'price_extension_chase',
    'control_risk_veto': 'control_risk',
    'external_signal_veto': 'external_signal_blocked',
}


def classify_execution_liquidity_grade(
    book_depth_fill_ratio: float,
    expected_slippage_r: float,
    spread_bps: float = 0.0,
    orderbook_slope: float = 0.0,
    cancel_rate: float = 0.0,
) -> str:
    fill_ratio = float(book_depth_fill_ratio or 0.0)
    slippage_r = float(expected_slippage_r or 0.0)
    spread = max(float(spread_bps or 0.0), 0.0)
    slope = max(float(orderbook_slope or 0.0), 0.0)
    cancel = min(max(float(cancel_rate or 0.0), 0.0), 1.0)
    penalty = 0
    if spread >= 12:
        penalty += 0.5
    if spread >= 18:
        penalty += 0.5
    if slope and slope < 0.25:
        penalty += 0.5
    elif slope and slope < 0.5:
        penalty += 0.25
    if cancel >= 0.35:
        penalty += 0.5
    elif cancel >= 0.2:
        penalty += 0.25

    grade_order = ['A+', 'A', 'B', 'C', 'D']
    if fill_ratio >= 0.85 and slippage_r <= 0.05:
        base_grade = 'A+'
    elif fill_ratio >= 0.75 and slippage_r <= 0.1:
        base_grade = 'A'
    elif fill_ratio >= 0.6 and slippage_r <= 0.15:
        base_grade = 'B'
    elif fill_ratio >= 0.45 and slippage_r <= 0.25:
        base_grade = 'C'
    else:
        base_grade = 'D'
    downgraded_index = min(grade_order.index(base_grade) + int(math.ceil(penalty)), len(grade_order) - 1)
    return grade_order[downgraded_index]


def compute_expected_slippage_r(candidate: Candidate) -> float:
    risk_per_unit = abs(float(getattr(candidate, 'risk_per_unit', 0.0) or 0.0))
    if risk_per_unit <= 0:
        return 0.0
    return round(float(getattr(candidate, 'expected_slippage_pct', 0.0) or 0.0) / risk_per_unit, 4)


def compute_execution_quality_size_adjustment(candidate: Candidate) -> Dict[str, Any]:
    execution_slippage_r = compute_expected_slippage_r(candidate)
    spread_bps = round(float(getattr(candidate, 'spread_bps', 0.0) or 0.0), 4)
    orderbook_slope = round(float(getattr(candidate, 'orderbook_slope', 0.0) or 0.0), 4)
    cancel_rate = round(float(getattr(candidate, 'cancel_rate', 0.0) or 0.0), 4)
    execution_liquidity_grade = classify_execution_liquidity_grade(
        getattr(candidate, 'book_depth_fill_ratio', 0.0),
        execution_slippage_r,
        spread_bps=spread_bps,
        orderbook_slope=orderbook_slope,
        cancel_rate=cancel_rate,
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
    else:
        multiplier = 0.15
        bucket = 'minimal'
    return {
        'expected_slippage_r': execution_slippage_r,
        'execution_liquidity_grade': execution_liquidity_grade,
        'size_multiplier': multiplier,
        'size_bucket': bucket,
        'spread_bps': spread_bps,
        'orderbook_slope': orderbook_slope,
        'cancel_rate': cancel_rate,
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
    payload = {
        'symbol': candidate.symbol,
        'state': candidate.state,
        'alert_tier': candidate.alert_tier,
        'score': round(float(candidate.score or 0.0), 4),
        'reasons': list(reasons),
        **reject_reason_payload,
        'must_pass_flags': dict(candidate.must_pass_flags or {}),
        'quality_score': round(float(candidate.quality_score or 0.0), 4),
        'execution_priority_score': round(float(candidate.execution_priority_score or 0.0), 4),
        'entry_distance_from_breakout_pct': round(float(candidate.entry_distance_from_breakout_pct or 0.0), 4),
        'entry_distance_from_vwap_pct': round(float(candidate.entry_distance_from_vwap_pct or 0.0), 4),
        'candle_extension_pct': round(float(candidate.candle_extension_pct or 0.0), 4),
        'recent_3bar_runup_pct': round(float(candidate.recent_3bar_runup_pct or 0.0), 4),
        'overextension_flag': candidate.overextension_flag,
        'entry_pattern': candidate.entry_pattern,
        'trend_regime': candidate.trend_regime,
        'liquidity_grade': candidate.liquidity_grade,
        'setup_ready': bool(candidate.setup_ready),
        'trigger_fired': bool(candidate.trigger_fired),
        'expected_slippage_pct': round(float(candidate.expected_slippage_pct or 0.0), 4),
        'expected_slippage_r': execution_quality['expected_slippage_r'],
        'book_depth_fill_ratio': round(float(candidate.book_depth_fill_ratio or 0.0), 4),
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


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


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


def build_trade_management_plan(entry_price: float, stop_price: float, quantity: float, tp1_r: float, tp1_close_pct: float, tp2_r: float, tp2_close_pct: float, breakeven_r: float = 1.0, atr_stop_distance: Optional[float] = None, side: str = POSITION_SIDE_LONG) -> TradeManagementPlan:
    side_normalized = normalize_position_side(side)
    direction = 1.0 if side_normalized == POSITION_SIDE_LONG else -1.0
    risk = float(atr_stop_distance) if atr_stop_distance and atr_stop_distance > 0 else abs(entry_price - stop_price)
    tp1_close_qty = round(quantity * tp1_close_pct, 10)
    tp2_close_qty = round(quantity * tp2_close_pct, 10)
    runner_qty = round(max(quantity - tp1_close_qty - tp2_close_qty, 0.0), 10)
    return TradeManagementPlan(
        side=side_normalized,
        entry_price=entry_price,
        stop_price=stop_price,
        quantity=quantity,
        initial_risk_per_unit=risk,
        breakeven_trigger_price=entry_price + (direction * risk * breakeven_r),
        tp1_trigger_price=entry_price + (direction * risk * tp1_r),
        tp1_close_qty=tp1_close_qty,
        tp2_trigger_price=entry_price + (direction * risk * tp2_r),
        tp2_close_qty=tp2_close_qty,
        runner_qty=runner_qty,
    )


def evaluate_management_actions(state: TradeManagementState, plan: TradeManagementPlan, current_price: float, ema5m: float, trailing_reference: float, trailing_buffer_pct: float, allow_runner_exit: bool = False) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    side = normalize_position_side(getattr(plan, 'side', POSITION_SIDE_LONG))
    is_short = side == POSITION_SIDE_SHORT

    if is_short:
        state.lowest_price_seen = min(state.lowest_price_seen or current_price, current_price)
        breakeven_buffer_price = plan.entry_price * (1 - max(float(plan.breakeven_min_buffer_pct or 0.0), 0.0))
        breakeven_confirmed = current_price <= plan.breakeven_trigger_price and current_price <= breakeven_buffer_price
        if plan.breakeven_confirmation_mode == 'ema_support':
            breakeven_confirmed = breakeven_confirmed and current_price <= ema5m and ema5m <= plan.entry_price
        if not state.moved_to_breakeven and breakeven_confirmed:
            actions.append({'type': 'move_stop_to_breakeven', 'new_stop_price': round(plan.entry_price, 10), 'confirmation_mode': plan.breakeven_confirmation_mode})
        if not state.tp1_hit and current_price <= plan.tp1_trigger_price and plan.tp1_close_qty > 0:
            actions.append({'type': 'take_profit_1', 'close_qty': plan.tp1_close_qty, 'new_stop_price': round(min(plan.entry_price, ema5m), 10), 'exit_reason': 'tp1'})
        if state.tp1_hit and not state.tp2_hit and current_price <= plan.tp2_trigger_price and plan.tp2_close_qty > 0:
            actions.append({'type': 'take_profit_2', 'close_qty': plan.tp2_close_qty, 'new_stop_price': round(min(plan.entry_price - plan.initial_risk_per_unit, ema5m), 10), 'exit_reason': 'tp2'})
        ceiling_ref = min(trailing_reference, state.lowest_price_seen or trailing_reference)
        trailing_ceiling = round(ceiling_ref * (1 + trailing_buffer_pct), 10)
        if allow_runner_exit and state.tp1_hit and current_price > trailing_ceiling and state.remaining_quantity > 0:
            actions.append({'type': 'runner_exit', 'close_qty': state.remaining_quantity, 'trailing_floor': round(trailing_ceiling, 2), 'exit_reason': 'runner'})
        return actions

    state.highest_price_seen = max(state.highest_price_seen or current_price, current_price)
    breakeven_buffer_price = plan.entry_price * (1 + max(float(plan.breakeven_min_buffer_pct or 0.0), 0.0))
    breakeven_confirmed = current_price >= plan.breakeven_trigger_price and current_price >= breakeven_buffer_price
    if plan.breakeven_confirmation_mode == 'ema_support':
        breakeven_confirmed = breakeven_confirmed and current_price >= ema5m and ema5m >= plan.entry_price
    if not state.moved_to_breakeven and breakeven_confirmed:
        actions.append({'type': 'move_stop_to_breakeven', 'new_stop_price': round(plan.entry_price, 10), 'confirmation_mode': plan.breakeven_confirmation_mode})
    if not state.tp1_hit and current_price >= plan.tp1_trigger_price and plan.tp1_close_qty > 0:
        actions.append({'type': 'take_profit_1', 'close_qty': plan.tp1_close_qty, 'new_stop_price': round(max(plan.entry_price, ema5m), 10), 'exit_reason': 'tp1'})
    if state.tp1_hit and not state.tp2_hit and current_price >= plan.tp2_trigger_price and plan.tp2_close_qty > 0:
        actions.append({'type': 'take_profit_2', 'close_qty': plan.tp2_close_qty, 'new_stop_price': round(max(plan.entry_price + plan.initial_risk_per_unit, ema5m), 10), 'exit_reason': 'tp2'})
    floor_ref = max(trailing_reference, state.highest_price_seen or trailing_reference)
    trailing_floor = round(floor_ref * (1 - trailing_buffer_pct), 10)
    if allow_runner_exit and state.tp1_hit and current_price < trailing_floor and state.remaining_quantity > 0:
        actions.append({'type': 'runner_exit', 'close_qty': state.remaining_quantity, 'trailing_floor': round(trailing_floor, 2), 'exit_reason': 'runner'})
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
        'positionSide': position_side,
    }
    return client.signed_post('/fapi/v1/order', params)


def cancel_order(client, symbol: str, order_id: Optional[int] = None, client_order_id: Optional[str] = None):
    params: Dict[str, Any] = {'symbol': symbol}
    if order_id is not None:
        params['orderId'] = order_id
    if client_order_id is not None:
        params['origClientOrderId'] = client_order_id
    return client.signed_post('/fapi/v1/order/cancel', params)


def place_stop_market_order(client, symbol: str, stop_price: float, quantity: float, meta: SymbolMeta, side: str = POSITION_SIDE_LONG):
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
        'positionSide': position_side,
    }
    return client.signed_post('/fapi/v1/algoOrder', params)


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
    if action['type'] in {'take_profit_1', 'take_profit_2', 'runner_exit'}:
        reduce_result = place_reduce_only_market(client, symbol, action['close_qty'], meta, side=side)
        state.remaining_quantity = round(max(state.remaining_quantity - action['close_qty'], 0.0), 10)
        if action['type'] == 'take_profit_1':
            state.tp1_hit = True
        elif action['type'] == 'take_profit_2':
            state.tp2_hit = True
        if action['type'] != 'runner_exit' and state.remaining_quantity > 0 and action.get('new_stop_price') is not None:
            if active_stop_order and active_stop_order.get('orderId'):
                cancel_order(client, symbol, order_id=active_stop_order['orderId'])
            active_stop_order = place_stop_market_order(client, symbol, action['new_stop_price'], state.remaining_quantity, meta, side=side)
            state.current_stop_price = action['new_stop_price']
        return state, active_stop_order, {**log_payload, 'reduce_order': reduce_result, 'new_stop_order': active_stop_order}
    return state, active_stop_order, log_payload


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
    }


def compute_leading_sentiment_signal(okx_sentiment_score: float = 0.0, okx_sentiment_acceleration: float = 0.0) -> Dict[str, Any]:
    reasons: List[str] = []
    score = 0.0
    if okx_sentiment_score <= 0.35 and okx_sentiment_acceleration >= 0.25:
        score += 6.0
        reasons.append('sentiment_early_turn_zone')
    if okx_sentiment_acceleration >= 0.35:
        score += 3.0
        reasons.append('sentiment_acceleration_turn')
    elif okx_sentiment_acceleration > 0:
        score += okx_sentiment_acceleration * 4.0
    if okx_sentiment_score >= 0.75:
        score -= 6.0 + max(0.0, (okx_sentiment_score - 0.75) * 10.0)
        reasons.append('sentiment_too_hot')
    return {'score': score, 'reasons': reasons}


def compute_squeeze_signal(funding_rate: Optional[float], funding_rate_avg: Optional[float], short_bias: float, oi_zscore_5m: float, cvd_delta: float, cvd_zscore: float, recent_5m_change_pct: float) -> Dict[str, Any]:
    reasons: List[str] = []
    score = 0.0
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
    if cvd_delta > 0:
        score += min(abs(cvd_delta) / 100000.0, 4.0)
    if cvd_zscore >= 2.5:
        score += min(cvd_zscore * 1.5, 6.0)
        reasons.append('positive_cvd_confirmation')
    if recent_5m_change_pct > 0:
        score += min(recent_5m_change_pct * 1.5, 4.0)
    return {'score': score, 'reasons': reasons}


def compute_control_risk_score(short_bias: float, oi_notional_percentile: float, smart_money_flow_score: float) -> Dict[str, Any]:
    reasons: List[str] = []
    score = 0.0
    veto = False
    veto_reason = None
    if oi_notional_percentile >= 0.97:
        score += 8.0 + (oi_notional_percentile - 0.97) * 100.0
        reasons.append('oi_at_extreme_percentile')
    if short_bias <= 0.2:
        score += 6.0
        reasons.append('weak_short_fuel')
    if smart_money_flow_score <= -0.35:
        score += 10.0 + abs(smart_money_flow_score) * 10.0
        reasons.append('smart_money_distribution_risk')
        veto = True
        veto_reason = 'smart_money_distribution_veto'
    return {'score': score, 'reasons': reasons, 'veto': veto, 'veto_reason': veto_reason}


def merge_smart_money_scores(exchange_score: Optional[float] = None, onchain_score: Optional[float] = None) -> Dict[str, Any]:
    sources: List[str] = []
    values: List[float] = []
    if exchange_score is not None:
        sources.append('exchange')
        values.append(float(exchange_score))
    if onchain_score is not None:
        sources.append('onchain')
        values.append(float(onchain_score))
    score = sum(values) / len(values) if values else 0.0
    veto = score <= -0.5 or (len(values) >= 2 and all(v <= -0.4 for v in values))
    return {'score': score, 'sources': sources, 'veto': veto, 'veto_reason': 'smart_money_outflow_veto' if veto else None}


def compute_sentiment_resonance_bonus(okx_sentiment_score: float = 0.0, okx_sentiment_acceleration: float = 0.0, sector_resonance_score: float = 0.0, smart_money_flow_score: float = 0.0) -> Dict[str, Any]:
    reasons: List[str] = []
    bonus = 0.0
    penalty = 0.0
    sentiment_score = float(okx_sentiment_score or 0.0)
    sentiment_acceleration = float(okx_sentiment_acceleration or 0.0)
    sector_score = float(sector_resonance_score or 0.0)
    smart_money_score = float(smart_money_flow_score or 0.0)
    sentiment_weight = 5.8 if smart_money_score < 0 else 9.0
    acceleration_weight = 3.8 if smart_money_score < 0 else 10.8
    sector_weight = 2.2 if smart_money_score < 0 else 5.2
    if sentiment_score > 0:
        reasons.append('okx_sentiment_positive')
        reasons.append(f'okx_sentiment_score={sentiment_score:.2f}')
        bonus += min(sentiment_score, 0.75) * sentiment_weight
        if 0 < sentiment_score <= 0.4 and sentiment_acceleration >= 0.25:
            bonus += 2.5
            reasons.append('sentiment_early_turn')
        if sentiment_score > 0.75:
            overheat_excess = sentiment_score - 0.75
            penalty += overheat_excess * 24.0
            reasons.append('sentiment_overheated')
    if sentiment_acceleration > 0:
        reasons.append('okx_sentiment_accelerating')
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
        reasons.append('smart_money_outflow')
        reasons.append(f'smart_money_flow_score={smart_money_score:.2f}')
        penalty += abs(smart_money_score) * 14.0
        sentiment_cap_ratio = max(0.15, 1.0 - min(abs(smart_money_score), 1.0) * 0.42)
        bonus *= sentiment_cap_ratio
        reasons.append(f'smart_money_bonus_cap={sentiment_cap_ratio:.2f}')
    elif smart_money_score > 0:
        reasons.append(f'smart_money_flow_score={smart_money_score:.2f}')
        bonus += smart_money_score * 9.0
    if smart_money_score <= -0.5:
        reasons.append('smart_money_veto_zone')
    return {'bonus': bonus, 'penalty': penalty, 'net': bonus - penalty, 'reasons': reasons}


def compute_market_regime_filter(btc_klines: Optional[Sequence[Sequence[Any]]] = None, sol_klines: Optional[Sequence[Sequence[Any]]] = None) -> Dict[str, Any]:
    reasons: List[str] = []
    score_multiplier = 1.0

    def evaluate(label: str, klines: Optional[Sequence[Sequence[Any]]]) -> Tuple[bool, bool]:
        if not klines or len(klines) < 5:
            return False, False
        closes = extract_closes(klines)
        price = closes[-1]
        ema_length = min(20, len(closes))
        ema20 = compute_ema(closes, ema_length)
        trend_down = price < ema20
        momentum_breakdown = False
        if len(closes) >= 5 and closes[-5] != 0:
            recent_change = ((price / closes[-5]) - 1.0) * 100
            threshold = -2.0 if label == 'btc' else -3.0
            momentum_breakdown = recent_change <= threshold
        if trend_down:
            reasons.append(f'{label}_trend_down')
        if momentum_breakdown:
            reasons.append(f'{label}_momentum_breakdown')
        return trend_down, momentum_breakdown

    btc_trend, btc_momo = evaluate('btc', btc_klines)
    sol_trend, sol_momo = evaluate('sol', sol_klines)
    btc_bad = btc_trend or btc_momo
    sol_bad = sol_trend or sol_momo
    if btc_bad:
        score_multiplier *= 0.7
    if sol_bad:
        score_multiplier *= 0.8
    if (btc_trend and sol_trend) or (btc_momo and sol_momo):
        label = 'risk_off'
        score_multiplier = min(score_multiplier, 0.55)
    elif btc_bad or sol_bad:
        label = 'caution'
        score_multiplier = min(score_multiplier, 0.85)
    else:
        label = 'neutral'
    return {
        'risk_on': label == 'neutral',
        'score_multiplier': max(0.35, min(score_multiplier, 1.15)),
        'reasons': reasons,
        'label': label,
        'trend_flags': {'btc': btc_trend, 'sol': sol_trend},
        'momentum_flags': {'btc': btc_momo, 'sol': sol_momo},
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


def build_candidate(
    symbol: str,
    ticker: Dict[str, Any],
    klines_5m: List[List[Any]],
    klines_15m: List[List[Any]],
    klines_1h: List[List[Any]],
    klines_4h: List[List[Any]],
    meta: SymbolMeta,
    hot_rank: Optional[int],
    gainer_rank: Optional[int],
    risk_usdt: float,
    lookback_bars: int,
    swing_bars: int,
    min_5m_change_pct: float,
    min_quote_volume: float,
    stop_buffer_pct: float,
    max_rsi_5m: float,
    min_volume_multiple: float,
    max_distance_from_ema_pct: float,
    funding_rate: Optional[float],
    funding_rate_threshold: float,
    funding_rate_avg: Optional[float] = None,
    funding_rate_avg_threshold: float = 0.0003,
    max_distance_from_vwap_pct: float = 10.0,
    max_leverage: int = 5,
    okx_sentiment_score: float = 0.0,
    okx_sentiment_acceleration: float = 0.0,
    sector_resonance_score: float = 0.0,
    smart_money_flow_score: float = 0.0,
    microstructure_inputs: Optional[Dict[str, Any]] = None,
    max_notional_usdt: float = 0.0,
    side: str = TRADE_SIDE_LONG,
    **legacy_kwargs: Any,
) -> Optional[Candidate]:
    if len(klines_5m) < max(lookback_bars + 2, swing_bars + 20, 30):
        return None
    if len(klines_15m) < 20 or len(klines_1h) < 25 or len(klines_4h) < 25:
        return None

    trade_side = normalize_trade_side(side)
    position_side = trade_side_to_position_side(trade_side)
    higher_timeframe_bias = trade_side

    closes_5m = extract_closes(klines_5m)
    highs_5m = extract_highs(klines_5m)
    lows_5m = extract_lows(klines_5m)
    volumes_5m = extract_volumes(klines_5m)
    closes_15m = extract_closes(klines_15m)

    last_price = closes_5m[-1]
    prev_close = closes_5m[-2]
    recent_5m_change_pct_raw = ((last_price / prev_close) - 1.0) * 100 if prev_close else 0.0
    recent_5m_change_pct = recent_5m_change_pct_raw if trade_side == TRADE_SIDE_LONG else -recent_5m_change_pct_raw
    if trade_side == TRADE_SIDE_SHORT:
        breakout_level = min(lows_5m[-(lookback_bars + 1):-1])
        recent_swing_low = max(highs_5m[-(swing_bars + 1):-1])
        stop_price_raw = recent_swing_low * (1.0 + stop_buffer_pct)
    else:
        breakout_level = max(prior_highs := highs_5m[-(lookback_bars + 1):-1])
        recent_swing_low = min(lows_5m[-(swing_bars + 1):-1])
        stop_price_raw = recent_swing_low * (1.0 - stop_buffer_pct)
    stop_price = round_price(stop_price_raw, meta.tick_size, meta.price_precision)
    if stop_price <= 0:
        return None
    if trade_side == TRADE_SIDE_SHORT and stop_price <= last_price:
        return None
    if trade_side == TRADE_SIDE_LONG and stop_price >= last_price:
        return None
    risk_per_unit = abs(last_price - stop_price)
    if risk_per_unit <= 0:
        return None
    quantity = round_step(risk_usdt / risk_per_unit, meta.step_size, meta.quantity_precision)
    if max_notional_usdt > 0:
        max_qty_by_notional = round_step(max_notional_usdt / last_price, meta.step_size, meta.quantity_precision)
        quantity = min(quantity, max_qty_by_notional)
    if quantity < meta.min_qty or quantity <= 0:
        return None

    rsi_5m = compute_rsi(closes_5m, period=14)
    avg_volume_20 = sum(volumes_5m[-21:-1]) / 20
    volume_multiple = (volumes_5m[-1] / avg_volume_20) if avg_volume_20 > 0 else 0.0
    ema20_5m = compute_ema(closes_5m, 20)
    distance_from_ema20_5m_pct_raw = ((last_price / ema20_5m) - 1.0) * 100 if ema20_5m else 0.0
    distance_from_ema20_5m_pct = distance_from_ema20_5m_pct_raw if trade_side == TRADE_SIDE_LONG else -distance_from_ema20_5m_pct_raw
    vwap_15m = compute_vwap(klines_15m[-20:])
    distance_from_vwap_15m_pct_raw = ((last_price / vwap_15m) - 1.0) * 100 if vwap_15m else 0.0
    distance_from_vwap_15m_pct = distance_from_vwap_15m_pct_raw if trade_side == TRADE_SIDE_LONG else -distance_from_vwap_15m_pct_raw
    atr_stop_distance = compute_atr(klines_5m, period=14) * 1.5
    oi_change_samples_5m = []
    if len(closes_5m) >= 22:
        for idx in range(1, min(len(closes_5m), 22)):
            prev_close_i = closes_5m[-(idx + 1)]
            curr_close_i = closes_5m[-idx]
            if prev_close_i:
                oi_change_samples_5m.append(((curr_close_i / prev_close_i) - 1.0) * 100)
    volume_samples_5m = volumes_5m[-21:-1] if len(volumes_5m) >= 21 else volumes_5m[:-1]
    oi_zscore_price_proxy = compute_zscore(recent_5m_change_pct, oi_change_samples_5m)
    volume_zscore_price_proxy = compute_zscore(volumes_5m[-1], volume_samples_5m)
    bollinger_bandwidth_pct = compute_bollinger_bandwidth_pct(closes_5m, period=20)
    price_above_vwap = bool(vwap_15m and (last_price >= vwap_15m if trade_side == TRADE_SIDE_LONG else last_price <= vwap_15m))

    if atr_stop_distance > 0:
        atr_stop_price_raw = last_price - atr_stop_distance if trade_side == TRADE_SIDE_LONG else last_price + atr_stop_distance
        atr_stop_price = round_price(atr_stop_price_raw, meta.tick_size, meta.price_precision)
        atr_stop_valid = 0 < atr_stop_price < last_price if trade_side == TRADE_SIDE_LONG else atr_stop_price > last_price
        if atr_stop_valid:
            stop_price = max(stop_price, atr_stop_price) if trade_side == TRADE_SIDE_LONG else min(stop_price, atr_stop_price)
            risk_per_unit = abs(last_price - stop_price)
            if risk_per_unit <= 0:
                return None
            quantity = round_step(risk_usdt / risk_per_unit, meta.step_size, meta.quantity_precision)
            if max_notional_usdt > 0:
                max_qty_by_notional = round_step(max_notional_usdt / last_price, meta.step_size, meta.quantity_precision)
                quantity = min(quantity, max_qty_by_notional)
            if quantity < meta.min_qty or quantity <= 0:
                return None

    trend_1h = evaluate_higher_timeframe_trend(klines_1h, side=trade_side)
    trend_4h = evaluate_higher_timeframe_trend(klines_4h, side=trade_side)
    higher_tf_allowed = trend_1h['allowed'] or trend_4h['allowed']
    macd_5m = compute_macd(closes_5m)
    structure_break = last_price > max(closes_5m[-6:-1]) if trade_side == TRADE_SIDE_LONG else last_price < min(closes_5m[-6:-1])
    avg_15m_change_pct = 0.0
    if len(closes_15m) >= 5:
        pct_changes_15m = []
        for prev, curr in zip(closes_15m[-5:-1], closes_15m[-4:]):
            if prev:
                pct_changes_15m.append(((curr / prev) - 1.0) * 100)
        avg_15m_change_pct = sum(pct_changes_15m) / len(pct_changes_15m) if pct_changes_15m else 0.0
    if trade_side == TRADE_SIDE_SHORT:
        avg_15m_signal = -avg_15m_change_pct
        acceleration_ratio = recent_5m_change_pct / avg_15m_signal if avg_15m_signal > 0 else (99.0 if recent_5m_change_pct > 0 else 0.0)
    else:
        acceleration_ratio = recent_5m_change_pct / avg_15m_change_pct if avg_15m_change_pct > 0 else (99.0 if recent_5m_change_pct > 0 else 0.0)
    quote_volume_24h = _to_float(ticker.get('quoteVolume', 0.0))
    price_change_pct_24h = _to_float(ticker.get('priceChangePercent', 0.0))

    if trade_side == TRADE_SIDE_LONG and last_price <= breakout_level:
        return None
    if trade_side == TRADE_SIDE_SHORT and last_price >= breakout_level:
        return None
    if recent_5m_change_pct < min_5m_change_pct:
        return None
    if quote_volume_24h < min_quote_volume:
        return None
    if not higher_tf_allowed:
        return None
    if volume_multiple < min_volume_multiple:
        return None
    if trade_side == TRADE_SIDE_LONG:
        if funding_rate is not None and funding_rate > funding_rate_threshold:
            return None
        if funding_rate_avg is not None and funding_rate_avg > funding_rate_avg_threshold:
            return None
    else:
        if funding_rate is not None and funding_rate < (-funding_rate_threshold):
            return None
        if funding_rate_avg is not None and funding_rate_avg < (-funding_rate_avg_threshold):
            return None
    if not structure_break:
        return None
    if trade_side == TRADE_SIDE_LONG and macd_5m['hist'] <= macd_5m['prev_hist']:
        return None
    if trade_side == TRADE_SIDE_SHORT and macd_5m['hist'] >= macd_5m['prev_hist']:
        return None
    if acceleration_ratio < 1.5:
        return None

    reasons: List[str] = []
    score = 0.0
    if hot_rank is not None:
        score += max(0.0, 1 - ((hot_rank - 1) / 10)) * 40
        reasons.append(f'square_hot_rank={hot_rank}')
    if gainer_rank is not None:
        score += max(0.0, 1 - ((gainer_rank - 1) / 20)) * 60
        reasons.append(f'gainer_rank={gainer_rank}')
    if hot_rank is not None and gainer_rank is not None:
        score += 20
        reasons.append('hot_gainer_intersection')
    score += min(recent_5m_change_pct * 6, 20)
    reasons.append(f'recent_5m_change_pct={recent_5m_change_pct:.2f}')
    score += min(volume_multiple * 8, 20)
    reasons.append(f'volume_multiple={volume_multiple:.2f}')
    score += min(acceleration_ratio * 5, 15)
    reasons.append(f'acceleration_ratio={acceleration_ratio:.2f}')
    price_change_signal_24h = price_change_pct_24h if trade_side == TRADE_SIDE_LONG else -price_change_pct_24h
    score += min(max(price_change_signal_24h, 0.0), 15)
    reasons.append(f'price_change_24h={price_change_pct_24h:.2f}')
    reasons.extend([f'{trade_side}_breakout_confirmed', f'rsi_5m={rsi_5m:.2f}', f'distance_from_ema20_5m_pct={distance_from_ema20_5m_pct:.2f}', f'distance_from_vwap_15m_pct={distance_from_vwap_15m_pct:.2f}'])
    if funding_rate is not None:
        reasons.append(f'funding_rate={funding_rate:.5f}')
        funding_headroom = (funding_rate_threshold - funding_rate) if trade_side == TRADE_SIDE_LONG else (funding_rate + funding_rate_threshold)
        score += max(0.0, funding_headroom * 10000)
    if funding_rate_avg is not None:
        reasons.append(f'funding_rate_avg={funding_rate_avg:.5f}')
        funding_avg_headroom = (funding_rate_avg_threshold - funding_rate_avg) if trade_side == TRADE_SIDE_LONG else (funding_rate_avg + funding_rate_avg_threshold)
        score += max(0.0, funding_avg_headroom * 10000)

    sentiment_bonus_payload = compute_sentiment_resonance_bonus(okx_sentiment_score, okx_sentiment_acceleration, sector_resonance_score, smart_money_flow_score)
    score += sentiment_bonus_payload['net']
    reasons.extend(sentiment_bonus_payload['reasons'])
    if okx_sentiment_score:
        reasons.append(f'okx_sentiment_score={okx_sentiment_score:.2f}')
    if okx_sentiment_acceleration:
        reasons.append(f'okx_sentiment_acceleration={okx_sentiment_acceleration:.2f}')
    if sector_resonance_score:
        reasons.append(f'sector_resonance_score={sector_resonance_score:.2f}')
    if smart_money_flow_score:
        reasons.append(f'smart_money_flow_score={smart_money_flow_score:.2f}')
    leading_payload = compute_leading_sentiment_signal(okx_sentiment_score, okx_sentiment_acceleration)
    score += leading_payload['score']
    reasons.extend(leading_payload['reasons'])

    microstructure_inputs = dict(microstructure_inputs or {})
    if legacy_kwargs:
        for key, value in legacy_kwargs.items():
            if key == 'market_regime':
                continue
            microstructure_inputs.setdefault(key, value)
    onchain_smart_money_score_raw = legacy_kwargs.get('onchain_smart_money_score')
    onchain_smart_money_score = float(onchain_smart_money_score_raw or 0.0)
    smart_money_merge = merge_smart_money_scores(
        exchange_score=smart_money_flow_score,
        onchain_score=onchain_smart_money_score_raw if onchain_smart_money_score_raw is not None else None,
    )
    smart_money_effective = float(smart_money_merge['score'])

    if microstructure_inputs.get('long_short_ratio') is not None:
        reasons.append(f"long_short_ratio={float(microstructure_inputs['long_short_ratio']):.2f}")
    if microstructure_inputs.get('short_bias', 0.0) > 0:
        reasons.append(f"short_bias={float(microstructure_inputs['short_bias']):.2f}")
    if trend_1h['allowed']:
        score += 12
        reasons.append(f'trend_1h_{trade_side}')
    if trend_4h['allowed']:
        score += 12
        reasons.append(f'trend_4h_{trade_side}')
    if (trade_side == TRADE_SIDE_LONG and macd_5m['hist'] > 0) or (trade_side == TRADE_SIDE_SHORT and macd_5m['hist'] < 0):
        score += 8
        reasons.append(f'macd_hist_{trade_side}')
    if structure_break:
        score += 6
        reasons.append(f'micro_structure_break_{trade_side}')

    oi_features = compute_relative_oi_features(
        oi_now=microstructure_inputs.get('oi_now'),
        oi_5m_ago=microstructure_inputs.get('oi_5m_ago'),
        oi_15m_ago=microstructure_inputs.get('oi_15m_ago'),
        taker_buy_ratio=microstructure_inputs.get('taker_buy_ratio'),
        funding_rate=funding_rate,
        funding_rate_avg=funding_rate_avg,
        oi_change_samples_5m=microstructure_inputs.get('oi_change_samples_5m'),
        volume_samples_5m=microstructure_inputs.get('volume_samples_5m'),
        latest_volume_5m=microstructure_inputs.get('latest_volume_5m'),
        cvd_samples=microstructure_inputs.get('cvd_samples'),
        cvd_delta=float(microstructure_inputs.get('cvd_delta', 0.0) or 0.0),
        bollinger_bandwidth_pct=bollinger_bandwidth_pct,
        price_above_vwap=price_above_vwap,
        oi_notional_history=microstructure_inputs.get('oi_notional_history'),
        short_bias=float(microstructure_inputs.get('short_bias', 0.0) or 0.0),
    )
    oi_features.update({
        'oi_zscore_5m': max(float(oi_features.get('oi_zscore_5m', 0.0) or 0.0), oi_zscore_price_proxy),
        'volume_zscore_5m': max(float(oi_features.get('volume_zscore_5m', 0.0) or 0.0), volume_zscore_price_proxy),
        'bollinger_bandwidth_pct': bollinger_bandwidth_pct,
        'price_above_vwap': price_above_vwap,
    })

    squeeze_payload = compute_squeeze_signal(
        funding_rate=funding_rate,
        funding_rate_avg=funding_rate_avg,
        short_bias=float(oi_features.get('short_bias', 0.0) or 0.0),
        oi_zscore_5m=float(oi_features.get('oi_zscore_5m', 0.0) or 0.0),
        cvd_delta=float(oi_features.get('cvd_delta', 0.0) or 0.0),
        cvd_zscore=float(oi_features.get('cvd_zscore', 0.0) or 0.0),
        recent_5m_change_pct=recent_5m_change_pct,
    )
    score += squeeze_payload['score']
    reasons.extend(squeeze_payload['reasons'])

    control_risk_payload = compute_control_risk_score(
        short_bias=float(oi_features.get('short_bias', 0.0) or 0.0),
        oi_notional_percentile=float(oi_features.get('oi_notional_percentile', 0.0) or 0.0),
        smart_money_flow_score=smart_money_effective,
    )
    score -= control_risk_payload['score']
    reasons.extend(control_risk_payload['reasons'])

    state_payload = classify_candidate_state(
        recent_5m_change_pct=recent_5m_change_pct,
        volume_multiple=volume_multiple,
        acceleration_ratio=acceleration_ratio,
        oi_features=oi_features,
        rsi_5m=rsi_5m,
        distance_from_ema20_5m_pct=distance_from_ema20_5m_pct,
        distance_from_vwap_15m_pct=distance_from_vwap_15m_pct,
        funding_rate=funding_rate,
        funding_rate_avg=funding_rate_avg,
        higher_tf_allowed=higher_tf_allowed,
        price_change_pct_24h=price_change_pct_24h,
        side=trade_side,
    )
    squeeze_reason = 'launch_short_squeeze' if trade_side == TRADE_SIDE_LONG else 'launch_long_squeeze'
    if squeeze_payload['score'] >= 12 and state_payload['state'] in {'squeeze', 'overheated', 'momentum_extension'}:
        state_payload = {
            **state_payload,
            'state': 'launch',
            'state_reasons': list(state_payload.get('state_reasons', [])) + [squeeze_reason],
            'exhaustion_score': min(float(state_payload.get('exhaustion_score', 0.0) or 0.0), max(float(state_payload.get('setup_score', 0.0) or 0.0) - 0.5, 0.0)),
        }
    score += state_payload['setup_score'] - (state_payload['exhaustion_score'] * 0.5)
    reasons.extend(smart_money_merge['sources'])

    regime_payload = legacy_kwargs.get('market_regime') or {}
    regime_label = str(regime_payload.get('label', 'neutral') or 'neutral')
    regime_multiplier = float(regime_payload.get('score_multiplier', 1.0) or 1.0)
    recommended_leverage = recommend_leverage(last_price, stop_price, max_leverage=max_leverage)
    if breakout_level:
        entry_distance_from_breakout_pct = (((last_price / breakout_level) - 1.0) * 100) if trade_side == TRADE_SIDE_LONG else (((breakout_level / last_price) - 1.0) * 100)
    else:
        entry_distance_from_breakout_pct = 0.0
    entry_distance_from_vwap_pct = abs(distance_from_vwap_15m_pct)
    short_squeeze_launch = squeeze_reason in state_payload.get('state_reasons', [])
    overextension_flag = bool(
        state_payload['state'] in {'overheated', 'momentum_extension'}
        or (not short_squeeze_launch and entry_distance_from_breakout_pct >= max(min(max_distance_from_ema_pct * 0.5, 3.0), 0.75))
        or (not short_squeeze_launch and entry_distance_from_vwap_pct >= max(min(max_distance_from_vwap_pct * 0.5, 3.0), 0.75))
    )
    setup_ready = state_payload['state'] in {'watch', 'launch', 'chase', 'squeeze'}
    trigger_fired = setup_ready and not overextension_flag and (last_price > breakout_level if trade_side == TRADE_SIDE_LONG else last_price < breakout_level)
    expected_slippage_pct = round(max(entry_distance_from_breakout_pct, 0.0) * 0.35, 4)
    book_depth_fill_ratio = round(clamp(1.0 - (expected_slippage_pct / 2.0), 0.0, 1.0), 4)
    initial_alert_tier = classify_alert_tier(score, state_payload['state'], regime_label)
    initial_position_size_pct = recommended_position_size_pct(score, initial_alert_tier, regime_multiplier)
    candidate = Candidate(
        symbol=symbol,
        last_price=last_price,
        price_change_pct_24h=price_change_pct_24h,
        quote_volume_24h=quote_volume_24h,
        hot_rank=hot_rank,
        gainer_rank=gainer_rank,
        funding_rate=funding_rate,
        funding_rate_avg=funding_rate_avg,
        recent_5m_change_pct=recent_5m_change_pct,
        acceleration_ratio_5m_vs_15m=acceleration_ratio,
        breakout_level=breakout_level,
        recent_swing_low=recent_swing_low,
        stop_price=stop_price,
        quantity=quantity,
        risk_per_unit=risk_per_unit,
        recommended_leverage=recommended_leverage,
        rsi_5m=rsi_5m,
        volume_multiple=volume_multiple,
        distance_from_ema20_5m_pct=distance_from_ema20_5m_pct,
        distance_from_vwap_15m_pct=distance_from_vwap_15m_pct,
        higher_tf_summary={'1h': trend_1h, '4h': trend_4h},
        score=score,
        reasons=reasons,
        side=trade_side,
        position_side=position_side,
        trigger_type='breakout',
        higher_timeframe_bias=higher_timeframe_bias,
        oi_change_pct_5m=oi_features['oi_change_pct_5m'],
        oi_change_pct_15m=oi_features['oi_change_pct_15m'],
        oi_acceleration_ratio=oi_features['oi_acceleration_ratio'],
        taker_buy_ratio=oi_features.get('taker_buy_ratio'),
        long_short_ratio=microstructure_inputs.get('long_short_ratio'),
        short_bias=float(microstructure_inputs.get('short_bias', 0.0) or 0.0),
        oi_zscore_5m=oi_features.get('oi_zscore_5m', 0.0),
        volume_zscore_5m=oi_features.get('volume_zscore_5m', 0.0),
        bollinger_bandwidth_pct=oi_features.get('bollinger_bandwidth_pct', 0.0),
        price_above_vwap=oi_features.get('price_above_vwap', False),
        funding_rate_percentile_hint=oi_features.get('funding_rate_percentile_hint'),
        cvd_delta=oi_features.get('cvd_delta', 0.0),
        cvd_zscore=oi_features.get('cvd_zscore', 0.0),
        atr_stop_distance=atr_stop_distance,
        state=state_payload['state'],
        state_reasons=state_payload['state_reasons'],
        setup_score=state_payload['setup_score'],
        exhaustion_score=state_payload['exhaustion_score'],
        okx_sentiment_score=okx_sentiment_score,
        okx_sentiment_acceleration=okx_sentiment_acceleration,
        sector_resonance_score=sector_resonance_score,
        smart_money_flow_score=smart_money_effective,
        leading_sentiment_delta=leading_payload['score'],
        squeeze_score=squeeze_payload['score'],
        control_risk_score=control_risk_payload['score'],
        alert_tier=initial_alert_tier,
        position_size_pct=initial_position_size_pct,
        regime_label=regime_label,
        regime_multiplier=regime_multiplier,
        onchain_smart_money_score=onchain_smart_money_score,
        smart_money_veto=bool(smart_money_merge['veto'] or control_risk_payload['veto']),
        smart_money_veto_reason=smart_money_merge['veto_reason'] or control_risk_payload['veto_reason'],
        smart_money_sources=list(smart_money_merge['sources']),
        entry_distance_from_breakout_pct=entry_distance_from_breakout_pct,
        entry_distance_from_vwap_pct=entry_distance_from_vwap_pct,
        overextension_flag=overextension_flag,
        setup_ready=setup_ready,
        trigger_fired=trigger_fired,
        expected_slippage_pct=expected_slippage_pct,
        book_depth_fill_ratio=book_depth_fill_ratio,
    )
    candidate.reasons.append(f'alert_tier={candidate.alert_tier}')
    candidate.reasons.append(f'position_size_pct={candidate.position_size_pct}')
    return candidate


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
        bridge_command = f"python /root/.hermes/okx_sentiment_bridge.py --stdio-command {getattr(args, 'okx_mcp_command')}"
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
    if s.endswith('SWAP'):
        s = s[:-4]
    if not s:
        return None
    if not s.endswith('USDT'):
        if s.endswith('USD'):
            s += 'T'
        elif s.isalpha():
            s += 'USDT'
    return s


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
        normalized[symbol] = raw_value
    return normalized


def apply_external_signal_to_candidate(candidate: Candidate, signal_payload: Optional[Dict[str, Any]]) -> Optional[str]:
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
    data = client.get('/fapi/v1/exchangeInfo')
    metas: Dict[str, SymbolMeta] = {}
    for row in data.get('symbols', []):
        if row.get('quoteAsset') != 'USDT':
            continue
        filters = {f['filterType']: f for f in row.get('filters', [])}
        metas[row['symbol']] = SymbolMeta(
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
    return metas


def fetch_tickers(client: BinanceFuturesClient) -> List[Dict[str, Any]]:
    return client.get('/fapi/v1/ticker/24hr')


def fetch_klines(client: BinanceFuturesClient, symbol: str, interval: str, limit: int) -> List[List[Any]]:
    return client.get('/fapi/v1/klines', params={'symbol': symbol, 'interval': interval, 'limit': limit})


def fetch_funding_rates(client: BinanceFuturesClient, symbol: str, limit: int = 3) -> List[float]:
    rows = client.get('/fapi/v1/fundingRate', params={'symbol': symbol, 'limit': limit})
    return [_to_float(item.get('fundingRate')) for item in rows]


def fetch_open_interest_hist(client: BinanceFuturesClient, symbol: str, period: str = '5m', limit: int = 30) -> List[Dict[str, Any]]:
    return client.get('/futures/data/openInterestHist', params={'symbol': symbol, 'period': period, 'limit': limit})


def fetch_order_book(client: BinanceFuturesClient, symbol: str, limit: int = 20) -> Dict[str, Any]:
    if not hasattr(client, 'get'):
        return {}
    return client.get('/fapi/v1/depth', params={'symbol': symbol, 'limit': int(limit or 20)})


def load_book_ticker_cache_samples(store: Optional[RuntimeStateStore], symbol: str, sample_count: int = 6, max_age_seconds: float = 3.0) -> List[Dict[str, Any]]:
    if store is None:
        return []
    cache_state = store.load_json('book_ticker_cache', {})
    if not isinstance(cache_state, dict):
        return []
    symbol_state = cache_state.get(symbol)
    if not isinstance(symbol_state, dict):
        return []
    samples = symbol_state.get('samples')
    if not isinstance(samples, list):
        return []
    updated_at = _parse_iso8601_utc(symbol_state.get('updated_at'))
    if updated_at is None:
        return []
    age_seconds = (_utc_now() - updated_at).total_seconds()
    if age_seconds > float(max_age_seconds):
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
    cache_state = store.load_json('book_ticker_cache', {})
    if not isinstance(cache_state, dict):
        cache_state = {}
    symbol_state = cache_state.get(symbol_key, {})
    if not isinstance(symbol_state, dict):
        symbol_state = {}
    prior_samples = symbol_state.get('samples', [])
    if not isinstance(prior_samples, list):
        prior_samples = []
    ring_size = max(int(max_samples or 0), 1)
    samples = [dict(row) for row in prior_samples if isinstance(row, dict) and row]
    samples.append({key: value for key, value in sample.items() if key in {'bidPrice', 'askPrice', 'bidQty', 'askQty'}})
    samples = samples[-ring_size:]
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
    store.save_json('book_ticker_cache', cache_state)
    event = append_runtime_event(store, 'book_ticker_ws_sample_written', {
        'event_source': 'book_ticker_websocket',
        'symbol': symbol_key,
        'samples_cached': len(samples),
        'event_count': event_count,
        'updated_at': updated_at,
    })
    return {
        'symbol': symbol_key,
        'samples_cached': len(samples),
        'event_count': event_count,
        'updated_at': updated_at,
        'event': event,
    }


def process_book_ticker_stream_message(store: RuntimeStateStore, message: Any, max_samples: int = 20) -> Optional[Dict[str, Any]]:
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
    return append_book_ticker_cache_sample(store, normalized['symbol'], data, max_samples=max_samples)


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
    append_runtime_event(store, 'book_ticker_ws_connected', {
        'event_source': 'book_ticker_websocket',
        'recv_timeout_seconds': float(recv_timeout_seconds),
        'max_messages': int(max_messages or 0),
    })
    messages_processed = 0
    samples_written = 0
    for _ in range(max(int(max_messages or 0), 1)):
        try:
            message = ws.recv()
        except timeout_exc:
            return {
                'status': 'healthy',
                'messages_processed': messages_processed,
                'samples_written': samples_written,
            }
        except socket_exc as exc:
            try:
                ws.close()
            finally:
                append_runtime_event(store, 'book_ticker_ws_disconnected', {
                    'event_source': 'book_ticker_websocket',
                    'detail': str(exc),
                    'messages_processed': messages_processed,
                    'samples_written': samples_written,
                })
            return {
                'status': 'disconnected',
                'messages_processed': messages_processed,
                'samples_written': samples_written,
                'error': str(exc),
            }
        result = process_book_ticker_stream_message(store, message, max_samples=max_samples)
        if result is None:
            continue
        messages_processed += 1
        samples_written += 1
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
) -> Dict[str, Any]:
    normalized_symbols = []
    for raw_symbol in list(symbols or []):
        symbol = str(raw_symbol or '').strip().upper()
        if symbol and symbol not in normalized_symbols:
            normalized_symbols.append(symbol)
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
    }
    if isinstance(extra, dict):
        payload.update(extra)
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
) -> Dict[str, Any]:
    current_symbols = [str(row).strip().upper() for row in list(state.get('symbols') or []) if str(row).strip()]
    next_symbols = [row.split('@')[0].upper() for row in build_book_ticker_stream_names(requested_symbols)]
    if next_symbols == current_symbols:
        return {'reopened': False, 'symbols': current_symbols, 'subscription_version': int(state.get('subscription_version', 0) or 0)}
    ws = state.get('ws')
    if ws is not None and hasattr(ws, 'close'):
        ws.close()
    state['ws'] = open_websocket_fn(next_symbols, ws_module=ws_module, base_ws_url=base_ws_url, connect_timeout_seconds=connect_timeout_seconds, sslopt=sslopt)
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
    update_book_ticker_ws_health_state(
        store,
        status='connecting',
        symbols=next_symbols,
        reconnect_count=state['reconnect_count'],
        subscription_version=state['subscription_version'],
        active_streams=state['streams'],
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
    sslopt: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    requested_symbols = list(initial_symbols or [])
    if callable(symbol_provider):
        provider_symbols = symbol_provider()
        if provider_symbols is not None:
            requested_symbols = list(provider_symbols)
    normalized_symbols = [row.split('@')[0].upper() for row in build_book_ticker_stream_names(requested_symbols)]
    if not normalized_symbols:
        raise ValueError('at least one symbol is required for bookTicker websocket supervisor')
    state: Dict[str, Any] = {
        'symbols': normalized_symbols,
        'streams': build_book_ticker_stream_names(normalized_symbols),
        'reconnect_count': 0,
        'subscription_version': 1,
    }
    state['ws'] = open_websocket_fn(normalized_symbols, ws_module=ws_module, base_ws_url=base_ws_url, connect_timeout_seconds=connect_timeout_seconds, sslopt=sslopt)
    update_book_ticker_ws_health_state(
        store,
        status='connecting',
        symbols=state['symbols'],
        reconnect_count=state['reconnect_count'],
        subscription_version=state['subscription_version'],
        active_streams=state['streams'],
    )
    cycles_completed = 0
    messages_processed_total = 0
    samples_written_total = 0
    backoff_seconds = max(float(reconnect_backoff_seconds or 0.0), 0.0)
    while True:
        result = monitor_cycle_fn(
            store,
            state['ws'],
            ws_module=ws_module,
            max_messages=max_messages_per_cycle,
            max_samples=max_samples,
            recv_timeout_seconds=recv_timeout_seconds,
        )
        cycles_completed += 1
        messages_processed_total += int(result.get('messages_processed', 0) or 0)
        samples_written_total += int(result.get('samples_written', 0) or 0)
        status = str(result.get('status') or 'unknown')
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
            )
        elif build_book_ticker_stream_names(refreshed_symbols) != build_book_ticker_stream_names(state['symbols']):
            state['symbols'] = [row.split('@')[0].upper() for row in build_book_ticker_stream_names(refreshed_symbols)]
            state['streams'] = build_book_ticker_stream_names(state['symbols'])
        if status == 'disconnected' and not refresh_result['reopened']:
            if backoff_seconds > 0:
                sleep_fn(backoff_seconds)
            state['ws'] = open_websocket_fn(state['symbols'], ws_module=ws_module, base_ws_url=base_ws_url, connect_timeout_seconds=connect_timeout_seconds, sslopt=sslopt)
            state['reconnect_count'] = int(state.get('reconnect_count', 0) or 0) + 1
            append_runtime_event(store, 'book_ticker_ws_reconnected', {
                'event_source': 'book_ticker_websocket',
                'symbols': list(state.get('symbols') or []),
                'symbol_count': len(list(state.get('symbols') or [])),
                'subscription_version': state['subscription_version'],
                'reconnect_count': state['reconnect_count'],
                'backoff_seconds': backoff_seconds,
            })
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
    if not hasattr(client, 'get'):
        return []
    append_runtime_event(store, 'book_ticker_cache_miss', {
        'event_source': 'book_ticker_cache',
        'symbol': symbol,
        'requested_sample_count': max(int(sample_count or 0), 0),
        'cache_max_age_seconds': float(cache_max_age_seconds),
        'fallback': 'rest_polling',
    })
    samples: List[Dict[str, Any]] = []
    sample_total = max(int(sample_count or 0), 0)
    for idx in range(sample_total):
        payload = client.get('/fapi/v1/ticker/bookTicker', params={'symbol': symbol})
        if isinstance(payload, dict) and payload:
            samples.append(payload)
        if idx < sample_total - 1 and interval_ms and interval_ms > 0:
            time.sleep(float(interval_ms) / 1000.0)
    return samples


def fetch_top_account_long_short_ratio(client: BinanceFuturesClient, symbol: str, period: str = '5m', limit: int = 10) -> List[Dict[str, Any]]:
    return client.get('/futures/data/topLongShortAccountRatio', params={'symbol': symbol, 'period': period, 'limit': limit})


def merged_candidate_symbols(**kwargs) -> Tuple[List[str], Dict[str, int], Dict[str, int]]:
    square_symbols = kwargs.get('square_symbols', [])
    tickers = kwargs.get('tickers', [])
    top_gainers = int(kwargs.get('top_gainers', 20) or 20)
    hot_rank_map = {symbol: idx + 1 for idx, symbol in enumerate(square_symbols)}
    usdt_tickers = [t for t in tickers if str(t.get('symbol', '')).endswith('USDT')]
    gainers = sorted(usdt_tickers, key=lambda row: _to_float(row.get('priceChangePercent')), reverse=True)[:top_gainers]
    gainer_rank_map = {row['symbol']: idx + 1 for idx, row in enumerate(gainers)}
    merged = list(dict.fromkeys(list(hot_rank_map.keys()) + list(gainer_rank_map.keys())))
    return merged, hot_rank_map, gainer_rank_map


def apply_hard_veto_filters(candidate: Candidate) -> Optional[str]:
    execution_slippage_r = compute_expected_slippage_r(candidate)
    execution_liquidity_grade = classify_execution_liquidity_grade(candidate.book_depth_fill_ratio, execution_slippage_r)
    if candidate.smart_money_veto:
        return 'smart_money_outflow_veto'
    if candidate.state == 'distribution':
        return 'distribution_state_veto'
    if candidate.exhaustion_score >= candidate.setup_score + 12 and candidate.cvd_delta <= 0:
        return 'distribution_blacklist'
    if candidate.cvd_delta < 0 and candidate.cvd_zscore <= -2.5:
        return 'negative_cvd_veto'
    if candidate.oi_change_pct_5m < 0:
        return 'oi_reversal_veto'
    if candidate.price_change_pct_24h >= 15.0 and candidate.state in {'chase', 'momentum_extension', 'overheated'}:
        return 'extended_chase_veto'
    if execution_slippage_r > 0.25:
        return 'execution_slippage_veto'
    if execution_liquidity_grade == 'D' and float(candidate.book_depth_fill_ratio or 0.0) < 0.45 and execution_slippage_r > 0.15:
        return 'execution_depth_veto'
    if candidate.smart_money_flow_score <= -0.35:
        return 'smart_money_outflow_veto'
    if candidate.control_risk_score >= 20:
        return 'control_risk_veto'
    return None


def classify_alert_tier(candidate_or_score: Any, state: Optional[str] = None, regime_label: Optional[str] = None) -> str:
    if isinstance(candidate_or_score, Candidate):
        candidate = candidate_or_score
        score = float(candidate.score)
        state = candidate.state
        regime_label = candidate.regime_label
    else:
        score = float(candidate_or_score)
        state = state or 'none'
        regime_label = regime_label or 'neutral'

    if regime_label == 'risk_off':
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


def recommended_position_size_pct(score_or_tier: Any, alert_tier: Optional[str] = None, regime_multiplier: float = 1.0, side_multiplier: float = 1.0) -> float:
    if alert_tier is None:
        tier = str(score_or_tier)
    else:
        tier = str(alert_tier)
    base = {'blocked': 0.0, 'watch': 0.5, 'medium': 1.0, 'high': 3.0, 'critical': 3.0}.get(tier, 0.0)
    effective_multiplier = max(float(regime_multiplier), 0.0) * max(float(side_multiplier), 0.0)
    return round(base * effective_multiplier, 4)


def derive_side_risk_multiplier(side: str, regime_label: str) -> float:
    normalized_side = normalize_position_side(side)
    normalized_regime = str(regime_label or 'neutral').strip().lower()
    if normalized_regime == 'risk_on':
        return 1.15 if normalized_side == POSITION_SIDE_LONG else 0.85
    if normalized_regime == 'risk_off':
        return 0.85 if normalized_side == POSITION_SIDE_LONG else 1.15
    if normalized_regime == 'caution':
        return 0.9
    return 1.0


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
        'score': round(candidate.score, 2),
        'state': candidate.state,
        'alert_tier': candidate.alert_tier,
        'position_size_pct': candidate.position_size_pct,
        'base_position_size_pct': base_position_size_pct,
        'side_risk_multiplier': side_multiplier,
        'execution_quality_size_multiplier': execution_quality['size_multiplier'],
        'execution_quality_size_bucket': execution_quality['size_bucket'],
        'atr_stop_distance': round(candidate.atr_stop_distance, 8),
        'must_pass_flags': dict(candidate.must_pass_flags or {}),
        'quality_score': round(float(candidate.quality_score or 0.0), 4),
        'execution_priority_score': round(float(candidate.execution_priority_score or 0.0), 4),
        'entry_distance_from_breakout_pct': round(candidate.entry_distance_from_breakout_pct, 4),
        'entry_distance_from_vwap_pct': round(candidate.entry_distance_from_vwap_pct, 4),
        'candle_extension_pct': round(float(candidate.candle_extension_pct or 0.0), 4),
        'recent_3bar_runup_pct': round(float(candidate.recent_3bar_runup_pct or 0.0), 4),
        'overextension_flag': candidate.overextension_flag,
        'entry_pattern': candidate.entry_pattern,
        'trend_regime': candidate.trend_regime,
        'liquidity_grade': candidate.liquidity_grade,
        'setup_ready': bool(candidate.setup_ready),
        'trigger_fired': bool(candidate.trigger_fired),
        'expected_slippage_pct': round(candidate.expected_slippage_pct, 4),
        'expected_slippage_r': execution_quality['expected_slippage_r'],
        'book_depth_fill_ratio': round(candidate.book_depth_fill_ratio, 4),
        'execution_liquidity_grade': execution_quality['execution_liquidity_grade'],
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


def run_scan_once(client: Optional[BinanceFuturesClient], args: argparse.Namespace, explicit_square_symbols: Optional[Sequence[str]] = None):
    store = get_runtime_state_store(args)
    base_okx = load_okx_sentiment_map(args)
    auto_okx = load_okx_sentiment_map_auto(args) if getattr(args, 'okx_auto', False) or getattr(args, 'okx_sentiment_command', '') else {}
    okx_map = dict(base_okx)
    okx_map.update(auto_okx)
    onchain_smart_money = load_manual_smart_money_map(args)
    external_signal_payload = load_external_signal_payload(args)
    external_signal_map = normalize_external_signal_map(external_signal_payload)

    square_symbols = list(explicit_square_symbols or load_manual_square_symbols(args))
    metas = fetch_exchange_meta(client)
    tickers = fetch_tickers(client)
    merged_symbols, hot_rank_map, gainer_rank_map = merged_candidate_symbols(square_symbols=square_symbols, tickers=tickers, top_gainers=getattr(args, 'top_gainers', 20))
    ticker_map = {row['symbol']: row for row in tickers}

    regime_payload = compute_market_regime_filter(
        btc_klines=fetch_klines(client, 'BTCUSDT', '15m', 30) if client else None,
        sol_klines=fetch_klines(client, 'SOLUSDT', '15m', 30) if client else None,
    )

    rejected_events: List[Dict[str, Any]] = []
    candidates: List[Candidate] = []
    candidate_alerts: List[Dict[str, Any]] = []
    max_candidates = int(getattr(args, 'max_candidates', 8) or 8)
    for symbol in merged_symbols[: max(max_candidates * 2, max_candidates)]:
        meta = metas.get(symbol)
        ticker = ticker_map.get(symbol)
        if not meta or not ticker:
            continue
        klines_5m = fetch_klines(client, symbol, '5m', max(getattr(args, 'lookback_bars', 12) + 30, 40))
        klines_15m = fetch_klines(client, symbol, '15m', 40)
        klines_1h = fetch_klines(client, symbol, '1h', 40)
        klines_4h = fetch_klines(client, symbol, '4h', 40)
        funding_rates = fetch_funding_rates(client, symbol, limit=3)
        funding_rate = funding_rates[-1] if funding_rates else None
        funding_rate_avg = sum(funding_rates) / len(funding_rates) if funding_rates else None
        oi_history = fetch_open_interest_hist(client, symbol, period='5m', limit=30)
        top_ratio = fetch_top_account_long_short_ratio(client, symbol, period='5m', limit=10)
        order_book = fetch_order_book(client, symbol, limit=20)
        book_ticker_samples = collect_book_ticker_samples(client, symbol, sample_count=6, interval_ms=150, store=store)
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
        for candidate_side in (TRADE_SIDE_LONG, TRADE_SIDE_SHORT):
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
                risk_usdt=float(getattr(args, 'risk_usdt', 10.0) or 10.0),
                max_notional_usdt=float(getattr(args, 'max_notional_usdt', 0.0) or 0.0),
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
                side=candidate_side,
                **micro,
            )
            if candidate is None:
                continue
            external_veto_reason = apply_external_signal_to_candidate(candidate, external_signal)
            if external_veto_reason:
                candidate.reasons.append(external_veto_reason)
                rejected_events.append(append_candidate_rejected_event(None, candidate, [external_veto_reason]))
                continue
            veto_reason = apply_hard_veto_filters(candidate)
            if veto_reason:
                candidate.reasons.append(veto_reason)
                rejected_events.append(append_candidate_rejected_event(None, candidate, [veto_reason]))
                continue
            regime_multiplier = float(regime_payload.get('score_multiplier', 1.0) or 1.0)
            regime_label = str(regime_payload.get('label', 'neutral') or 'neutral')
            side_multiplier = derive_side_risk_multiplier(getattr(candidate, 'side', POSITION_SIDE_LONG), regime_label)
            candidate.score *= regime_multiplier
            candidate.regime_label = regime_label
            candidate.regime_multiplier = regime_multiplier
            candidate.side_risk_multiplier = side_multiplier
            candidate.reasons.append(f'market_regime_multiplier={regime_multiplier:.2f}')
            candidate.reasons.append(f'side_risk_multiplier={side_multiplier:.2f}')
            for regime_reason in regime_payload.get('reasons', []):
                candidate.reasons.append(f'market_regime:{regime_reason}')
            candidate.alert_tier = classify_alert_tier(candidate)
            external_tier = str(external_signal.get('external_signal_tier', '') or '').lower()
            tier_order = {'blocked': 0, 'watch': 1, 'high': 2, 'critical': 3}
            if tier_order.get(external_tier, -1) > tier_order.get(candidate.alert_tier, 0):
                candidate.alert_tier = external_tier
            candidate.reasons.append(f'alert_tier={candidate.alert_tier}')
            external_position_size_pct = next(
                (
                    float(reason.split('=', 1)[1])
                    for reason in reversed(candidate.reasons)
                    if str(reason).startswith('external_position_size_pct=')
                ),
                None,
            )
            base_position_size_pct = recommended_position_size_pct(
                candidate.score,
                candidate.alert_tier,
                candidate.regime_multiplier,
                side_multiplier,
            )
            execution_quality = compute_execution_quality_size_adjustment(candidate)
            effective_position_size_pct = round(base_position_size_pct * float(execution_quality['size_multiplier']), 4)
            candidate.position_size_pct = round(external_position_size_pct, 4) if external_position_size_pct and external_position_size_pct > 0 else effective_position_size_pct
            candidate.reasons.append(f'base_position_size_pct={base_position_size_pct}')
            candidate.reasons.append(f"execution_quality_size_multiplier={execution_quality['size_multiplier']}")
            candidate.reasons.append(f"execution_quality_size_bucket={execution_quality['size_bucket']}")
            candidate.reasons.append(f'position_size_pct={candidate.position_size_pct}')
            candidates.append(candidate)

    candidates.sort(key=lambda item: item.score, reverse=True)
    candidate_alerts = [build_standardized_alert(item, regime_payload) for item in candidates]
    best = candidates[0] if candidates else None
    selected = build_standardized_alert(best, regime_payload) if best else None
    reject_by_reason: Dict[str, int] = {}
    reject_by_label: Dict[str, int] = {}
    reject_by_execution_grade: Dict[str, int] = {}
    for event in rejected_events:
        reason = str(event.get('reject_reason', '') or '')
        label = str(event.get('reject_reason_label', '') or '')
        execution_grade = str(event.get('execution_liquidity_grade', '') or '')
        if reason:
            reject_by_reason[reason] = reject_by_reason.get(reason, 0) + 1
        if label:
            reject_by_label[label] = reject_by_label.get(label, 0) + 1
        if execution_grade:
            reject_by_execution_grade[execution_grade] = reject_by_execution_grade.get(execution_grade, 0) + 1
    payload = {
        'ok': True,
        'candidate_count': len(candidates),
        'candidates': candidate_alerts,
        'candidate_alerts': candidate_alerts,
        'selected': selected,
        'selected_alert': selected,
        'market_regime': regime_payload,
        'rejected_stats': {
            'total': len(rejected_events),
            'by_reason': reject_by_reason,
            'by_reject_label': reject_by_label,
            'by_execution_liquidity_grade': reject_by_execution_grade,
        },
    }
    return payload, best, metas


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', default='')
    parser.add_argument('--square-symbols', default='')
    parser.add_argument('--square-symbols-file', default='')
    parser.add_argument('--use-square-page', action='store_true')
    parser.add_argument('--top-gainers', type=int, default=20)
    parser.add_argument('--max-candidates', type=int, default=8)
    parser.add_argument('--lookback-bars', type=int, default=12)
    parser.add_argument('--swing-bars', type=int, default=6)
    parser.add_argument('--risk-usdt', type=float, default=10.0)
    parser.add_argument('--max-notional-usdt', type=float, default=0.0)
    parser.add_argument('--min-5m-change-pct', type=float, default=2.0)
    parser.add_argument('--min-quote-volume', type=float, default=50_000_000.0)
    parser.add_argument('--stop-buffer-pct', type=float, default=0.01)
    parser.add_argument('--max-rsi-5m', type=float, default=80.0)
    parser.add_argument('--min-volume-multiple', type=float, default=1.8)
    parser.add_argument('--max-distance-from-ema-pct', type=float, default=10.0)
    parser.add_argument('--max-distance-from-vwap-pct', type=float, default=10.0)
    parser.add_argument('--max-funding-rate', type=float, default=0.0005)
    parser.add_argument('--max-funding-rate-avg', type=float, default=0.0003)
    parser.add_argument('--leverage', type=int, default=5)
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--scan-only', action='store_true')
    parser.add_argument('--profile', default='default')
    parser.add_argument('--tp1-r', type=float, default=1.5)
    parser.add_argument('--tp1-close-pct', type=float, default=0.3)
    parser.add_argument('--tp2-r', type=float, default=2.0)
    parser.add_argument('--tp2-close-pct', type=float, default=0.4)
    parser.add_argument('--breakeven-r', type=float, default=1.0)
    parser.add_argument('--trailing-buffer-pct', type=float, default=0.02)
    parser.add_argument('--auto-loop', action='store_true')
    parser.add_argument('--poll-interval-sec', type=int, default=60)
    parser.add_argument('--monitor-poll-interval-sec', type=int, default=15)
    parser.add_argument('--user-stream-refresh-interval-minutes', type=float, default=30.0, help='Refresh listen key after this many minutes.')
    parser.add_argument('--user-stream-disconnect-timeout-minutes', type=float, default=65.0, help='Mark user data stream disconnected when refresh heartbeat is older than this many minutes.')
    parser.add_argument('--base-url', default=os.getenv('BINANCE_FUTURES_BASE_URL', 'https://fapi.binance.com'))
    parser.add_argument('--okx-sentiment-inline', default='')
    parser.add_argument('--okx-sentiment-file', default='')
    parser.add_argument('--okx-sentiment-command', default='')
    parser.add_argument('--okx-auto', action='store_true')
    parser.add_argument('--okx-mcp-command', default='')
    parser.add_argument('--okx-sentiment-timeout', type=int, default=20)
    parser.add_argument('--external-signal-json', default='')
    parser.add_argument('--reconcile-only', action='store_true', help='Only reconcile runtime state with exchange positions/orders, then exit.')
    parser.add_argument('--runtime-state-dir', default=os.path.expanduser('~/.hermes/binance-futures-momentum-long/runtime-state'), help='Directory for runtime state JSON/JSONL files.')
    parser.add_argument('--halt-on-orphan-position', action='store_true', help='Halt strategy if reconcile finds exchange positions not tracked locally.')
    parser.add_argument('--repair-missing-protection', dest='repair_missing_protection', action='store_true', default=True, help='Auto place replacement STOP_MARKET orders for tracked live positions missing protection during reconcile.')
    parser.add_argument('--no-repair-missing-protection', dest='repair_missing_protection', action='store_false', help='Report missing protection and halt instead of auto repairing during reconcile.')
    parser.add_argument('--max-scan-cycles', type=int, default=1, help='Maximum scan cycles when auto-loop is enabled. Set 0 for infinite loop.')
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
    parser.add_argument('--disable-notify', action='store_true', help='Disable outbound trade notifications.')
    parser.add_argument('--notify-target', default='', help='Comma-separated notification targets like telegram:-100123:77,weixin:chatid.')
    parser.add_argument('--telegram-bot-token-env', default='TELEGRAM_BOT_TOKEN', help='Env var name containing Telegram bot token for notifications.')
    parser.add_argument('--output-format', choices=['json', 'cn'], default='cn', help='Output as raw JSON or concise Chinese summary.')
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
        'per_symbol_single_side_only': True,
        'opposite_side_flip_cooldown_minutes': 0,
        'notify_target': '',
        'disable_notify': False,
        'telegram_bot_token_env': 'TELEGRAM_BOT_TOKEN',
        'reconcile_only': False,
        'halt_on_orphan_position': False,
        'repair_missing_protection': True,
        'output_format': 'cn',
    }
    for key, value in defaults.items():
        if not hasattr(args, key):
            setattr(args, key, value)
    profile = getattr(args, 'profile', 'default')
    profile_overrides: Dict[str, Any] = {}
    if profile == '10u-aggressive':
        profile_overrides = {
            'risk_usdt': 1.2,
            'leverage': 5,
            'breakeven_r': 0.8,
            'tp1_r': 1.2,
            'tp1_close_pct': 0.5,
            'tp2_r': 1.8,
            'tp2_close_pct': 0.3,
            'min_quote_volume': 20_000_000,
            'top_gainers': 12,
            'max_candidates': 5,
            'max_rsi_5m': 76.0,
            'min_volume_multiple': 2.4,
            'min_5m_change_pct': 2.5,
            'max_distance_from_ema_pct': 6.0,
            'max_distance_from_vwap_pct': 5.0,
            'max_funding_rate': 0.0004,
            'max_funding_rate_avg': 0.00025,
        }
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


def build_cn_scan_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    cycles = result.get('cycles') if isinstance(result, dict) else None
    cycle = cycles[0] if isinstance(cycles, list) and cycles else {}
    scan = cycle.get('scan', {}) if isinstance(cycle, dict) else {}
    selected = scan.get('selected_alert') or scan.get('selected')
    market_regime = scan.get('market_regime', {}) if isinstance(scan, dict) else {}
    rejected_stats = scan.get('rejected_stats', {}) if isinstance(scan, dict) else {}
    candidates = scan.get('candidate_alerts') or scan.get('candidates') or []
    cycle_mode = 'dry-run'
    if isinstance(cycle, dict):
        if cycle.get('scan_only'):
            cycle_mode = 'scan-only'
        elif cycle.get('live_requested') or cycle.get('live_execution') or cycle.get('live_skipped_due_to_risk_guard') or cycle.get('live_skipped_due_to_existing_positions'):
            cycle_mode = 'live'

    summary = {
        'ok': result.get('ok', True) if isinstance(result, dict) else True,
        '模式': cycle_mode,
        '市场状态': {
            '标签': market_regime.get('label', '-'),
            '乘数': market_regime.get('score_multiplier', 1.0),
            '原因': market_regime.get('reasons', []),
        },
        '扫描概览': {
            '候选数': scan.get('candidate_count', 0),
            '拒绝数': rejected_stats.get('total', 0),
            '主要拒绝原因': [
                {'原因': key, '数量': value}
                for key, value in top_dict_items(rejected_stats.get('by_reject_label', {}), limit=4)
            ],
        },
        '首选标的': None,
        '候选列表': [],
    }

    if isinstance(selected, dict):
        summary['首选标的'] = {
            '交易对': selected.get('symbol', '-'),
            '评级': selected.get('alert_tier', '-'),
            '状态': selected.get('state', '-'),
            '得分': selected.get('score'),
            '建议仓位': selected.get('position_size_pct'),
            '入场价': selected.get('entry_price', selected.get('last_price')),
            '止损价': selected.get('stop_price'),
            '预期滑点R': selected.get('expected_slippage_r'),
            '流动性': selected.get('execution_liquidity_grade', selected.get('liquidity_grade')),
            '理由': selected.get('reasons', [])[:6],
        }

    for item in list(candidates)[:5]:
        if not isinstance(item, dict):
            continue
        summary['候选列表'].append({
            '交易对': item.get('symbol', '-'),
            '评级': item.get('alert_tier', '-'),
            '状态': item.get('state', '-'),
            '得分': item.get('score'),
            '24h涨幅': item.get('price_change_pct_24h'),
            '5m涨幅': item.get('recent_5m_change_pct'),
            '建议仓位': item.get('position_size_pct'),
            '流动性': item.get('execution_liquidity_grade', item.get('liquidity_grade')),
        })
    return summary


def render_cn_scan_summary(result: Dict[str, Any]) -> str:
    summary = build_cn_scan_summary(result)
    market = summary.get('市场状态', {})
    overview = summary.get('扫描概览', {})
    selected = summary.get('首选标的')
    lines = [
        f"扫描模式: {summary.get('模式', 'dry-run')}",
        f"市场状态: {market.get('标签', '-')} ×{format_num(market.get('乘数', 1.0), 2)}",
    ]
    reasons = market.get('原因', []) or []
    if reasons:
        lines.append(f"状态原因: {', '.join(str(x) for x in reasons[:4])}")
    lines.append(f"扫描结果: 候选 {overview.get('候选数', 0)} 个 | 拒绝 {overview.get('拒绝数', 0)} 个")
    reject_items = overview.get('主要拒绝原因', []) or []
    if reject_items:
        reject_text = '，'.join(f"{item['原因']} {item['数量']}" for item in reject_items)
        lines.append(f"主要拦截: {reject_text}")
    if selected:
        lines.extend([
            '',
            '首选标的',
            f"- {selected.get('交易对')} | {selected.get('评级')} | {selected.get('状态')} | 得分 {format_num(selected.get('得分'), 1)}",
            f"- 入场 {format_num(selected.get('入场价'), 6)} | 止损 {format_num(selected.get('止损价'), 6)} | 建议仓位 {format_pct(selected.get('建议仓位'), 1)}",
            f"- 执行质量 {selected.get('流动性', '-')} | 预期滑点R {format_num(selected.get('预期滑点R'), 3)}",
        ])
        selected_reasons = selected.get('理由', []) or []
        if selected_reasons:
            lines.append(f"- 关键信号: {'，'.join(str(x) for x in selected_reasons[:5])}")
    candidate_rows = summary.get('候选列表', []) or []
    if candidate_rows:
        lines.extend(['', '候选列表'])
        for idx, item in enumerate(candidate_rows, start=1):
            lines.append(
                f"{idx}. {item.get('交易对')} | {item.get('评级')} | {item.get('状态')} | 得分 {format_num(item.get('得分'), 1)} | 24h {format_pct(item.get('24h涨幅'), 2)} | 5m {format_pct(item.get('5m涨幅'), 2)} | 仓位 {format_pct(item.get('建议仓位'), 1)} | 流动性 {item.get('流动性', '-')}"
            )
    return '\n'.join(lines)


def default_risk_state() -> Dict[str, Any]:
    return {
        'halted': False,
        'halt_reason': '',
        'consecutive_losses': 0,
        'daily_realized_pnl_usdt': 0.0,
        'symbol_cooldowns': {},
        'portfolio_exposure_pct_by_theme': {},
        'portfolio_exposure_pct_by_correlation': {},
    }


def load_risk_state(store: RuntimeStateStore) -> Dict[str, Any]:
    state = store.load_json('risk_state', default_risk_state())
    if not isinstance(state, dict):
        return default_risk_state()
    merged = default_risk_state()
    merged.update(state)
    if not isinstance(merged.get('symbol_cooldowns'), dict):
        merged['symbol_cooldowns'] = {}
    return merged


def log_runtime_event(event_type: str, payload: Dict[str, Any]) -> None:
    print(json.dumps({'event_type': event_type, **payload}, ensure_ascii=False), flush=True)


def load_env_value(key: str, default: str = '') -> str:
    return os.getenv(key, default)


def parse_notification_target(target: str) -> Dict[str, Any]:
    raw = (target or '').strip()
    if not raw or ':' not in raw:
        raise ValueError(f'invalid notification target: {target}')
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
        return f"开单 {symbol} entry={payload.get('entry_price')} stop={payload.get('stop_price')} qty={payload.get('quantity')} profile={profile}".strip()
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
            from hermes.platforms.weixin.direct import send_message as direct_send_message
        except Exception as exc:
            raise RuntimeError('weixin direct adapter unavailable') from exc
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
        if parsed['platform'] == 'telegram':
            bot_token = load_env_value(getattr(args, 'telegram_bot_token_env', 'TELEGRAM_BOT_TOKEN'))
            results.append(send_telegram_notification(bot_token, parsed['chat_id'], message, thread_id=parsed.get('thread_id'), post_func=post_func))
        elif parsed['platform'] == 'weixin':
            results.append(send_weixin_notification(parsed['chat_id'], message))
        else:
            results.append({'ok': False, 'platform': parsed['platform'], 'error': 'unsupported_platform'})
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


def fetch_open_positions(client: Any):
    if hasattr(client, 'signed_get'):
        rows = client.signed_get('/fapi/v2/positionRisk', params={})
        return [row for row in rows if abs(_to_float(row.get('positionAmt'))) > 0]
    return []


def resolve_position_protection_status(client: Any, symbol: str, expected_stop_order: Optional[Dict[str, Any]] = None, allow_missing_when_flat: bool = True, side: Any = POSITION_SIDE_LONG) -> Dict[str, Any]:
    position_side = normalize_position_side(side)
    positions = fetch_open_positions(client)
    active = next((row for row in positions if row.get('symbol') == symbol and normalize_position_side(row.get('positionSide')) == position_side and abs(_to_float(row.get('positionAmt'))) > 0), None)
    expected_order_id = expected_stop_order.get('orderId') if isinstance(expected_stop_order, dict) else None
    if active is None:
        return {'status': 'flat', 'active_position': None, 'expected_order_id': expected_order_id, 'side': position_side}
    open_orders = fetch_open_orders(client, symbol)
    matched = next((row for row in open_orders if row.get('orderId') == expected_order_id), None) if expected_order_id is not None else (open_orders[0] if open_orders else None)
    if matched is None:
        return {'status': 'missing', 'active_position': active, 'expected_order_id': expected_order_id, 'side': position_side}
    return {'status': 'protected', 'active_position': active, 'expected_order_id': expected_order_id, 'stop_order': matched, 'side': position_side}

def repair_missing_protection(client: Any, symbol: str, tracked: Optional[Dict[str, Any]], active_position: Optional[Dict[str, Any]], meta: Optional[SymbolMeta] = None) -> Dict[str, Any]:
    tracked = tracked if isinstance(tracked, dict) else {}
    active_position = active_position if isinstance(active_position, dict) else {}
    quantity = abs(_to_float(active_position.get('positionAmt')))
    stop_price = _to_float(tracked.get('stop_price'))
    side = normalize_position_side(tracked.get('side') or active_position.get('positionSide'))
    if quantity <= 0 or stop_price <= 0:
        return {
            'ok': False,
            'symbol': symbol,
            'side': side,
            'status': 'repair_failed',
            'message': 'missing stop_price or active quantity for repair',
            'repair_attempted': False,
        }
    if meta is None:
        meta = load_exchange_info(client).get(symbol)
    if meta is None:
        return {
            'ok': False,
            'symbol': symbol,
            'side': side,
            'status': 'repair_failed',
            'message': 'missing symbol meta for repair',
            'repair_attempted': False,
        }
    stop_order = place_stop_market_order(client, symbol, stop_price, quantity, meta, side=side)
    return {
        'ok': True,
        'symbol': symbol,
        'side': side,
        'status': 'protected',
        'stop_order': stop_order,
        'stop_price': stop_price,
        'quantity': quantity,
        'repair_attempted': True,
    }


def sync_tracked_positions_with_exchange(store: RuntimeStateStore, exchange_positions: Sequence[Dict[str, Any]], protected_symbols: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    raw_positions_state = store.load_json('positions', {})
    if not isinstance(raw_positions_state, dict):
        raw_positions_state = {}
    original_keys = {build_position_key(value.get('symbol') or split_position_key(key)[0], value.get('side') or split_position_key(key)[1]): str(key).upper() for key, value in raw_positions_state.items() if isinstance(value, dict)}
    positions_state = migrate_positions_state(raw_positions_state)
    protected_keys = {str(symbol).upper() for symbol in list(protected_symbols or []) if symbol}
    protected_symbol_names = {split_position_key(symbol)[0] for symbol in protected_keys}
    exchange_map = {
        build_position_key(row.get('symbol'), row.get('positionSide')): row
        for row in list(exchange_positions or [])
        if isinstance(row, dict) and row.get('symbol')
    }
    closed_symbols: List[str] = []
    refreshed_symbols: List[str] = []
    orphan_symbols: List[str] = []
    normalized_positions: Dict[str, Any] = {}
    saw_side_aware_keys = any(':' in str(key) for key in raw_positions_state.keys())
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
            if tracked.get('status') not in {'closed', 'orphan'}:
                tracked['status'] = 'closed'
                tracked['remaining_quantity'] = 0.0
                tracked['stop_order_id'] = None
                tracked['protection_status'] = 'flat'
                closed_symbols.append(report_key)
            normalized_positions[position_key] = tracked
            continue
        qty = abs(_to_float(exchange_row.get('positionAmt')))
        tracked['quantity'] = qty
        tracked['remaining_quantity'] = qty
        tracked['protection_status'] = 'protected' if position_key in protected_keys or symbol in protected_symbol_names else tracked.get('protection_status')
        if tracked.get('status') == 'orphan':
            orphan_symbols.append(report_key)
        refreshed_symbols.append(report_key)
        normalized_positions[position_key] = tracked
    materialized_positions = materialize_positions_state(normalized_positions, original_keys, include_legacy_alias=not saw_side_aware_keys)
    store.save_json('positions', materialized_positions)
    return {
        'closed_symbols': closed_symbols,
        'refreshed_symbols': refreshed_symbols,
        'orphan_symbols': orphan_symbols,
    }


def reconcile_runtime_state(client: Any, store: RuntimeStateStore, halt_on_orphan_position: bool = False, repair_missing_protection_enabled: bool = True) -> Dict[str, Any]:
    positions_state = store.load_json('positions', {})
    if not isinstance(positions_state, dict):
        positions_state = {}
    exchange_positions = fetch_open_positions(client)
    orphan_positions = []
    positions_missing_protection = []
    protected_symbols = []
    protection_repairs = []
    for row in exchange_positions:
        symbol = row.get('symbol')
        if not symbol:
            continue
        side = normalize_position_side(row.get('positionSide'))
        position_key, tracked = get_position_by_symbol_side(positions_state, symbol, side)
        tracked = tracked if tracked else None
        protection = resolve_position_protection_status(client, symbol, expected_stop_order={'orderId': tracked.get('stop_order_id')} if isinstance(tracked, dict) and tracked.get('stop_order_id') else None, side=side)
        if protection.get('status') == 'protected':
            protected_symbols.append(position_key)
        if tracked is None:
            orphan_positions.append(symbol)
            positions_state, _ = upsert_position_record(positions_state, {'symbol': symbol, 'side': side, 'status': 'orphan'}, key=position_key)
        elif protection.get('status') != 'protected':
            repair_result = None
            if repair_missing_protection_enabled:
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
                tracked['stop_order_id'] = repair_result.get('stop_order', {}).get('orderId')
                tracked['status'] = tracked.get('status') or 'monitoring'
                positions_state, _ = upsert_position_record(positions_state, tracked, key=position_key)
            else:
                positions_missing_protection.append(symbol if side == POSITION_SIDE_LONG else position_key)
                tracked['protection_status'] = protection.get('status')
    store.save_json('positions', positions_state)
    sync_result = sync_tracked_positions_with_exchange(store, exchange_positions, protected_symbols=protected_symbols)
    result = {
        'ok': True,
        'orphan_positions': orphan_positions,
        'positions_missing_protection': positions_missing_protection,
        'protection_repairs': protection_repairs,
        'exchange_position_count': len(exchange_positions),
        'closed_tracked_positions': sync_result.get('closed_symbols', []),
        'refreshed_tracked_positions': sync_result.get('refreshed_symbols', []),
    }
    if halt_on_orphan_position and orphan_positions:
        risk_state = load_risk_state(store)
        risk_state['halted'] = True
        risk_state['halt_reason'] = f"orphan_positions:{','.join(orphan_positions)}"
        store.save_json('risk_state', risk_state)
        store.append_event('reconcile', result)
        result['ok'] = False
    return result


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
    snapshot = build_position_exposure_snapshot(open_positions)
    reasons: List[str] = []
    candidate_symbol = str(getattr(candidate, 'symbol', '') or '').upper()
    candidate_side = normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG)) if candidate is not None else POSITION_SIDE_LONG
    candidate_notional = abs(_to_float(getattr(candidate, 'notional', 0.0) or getattr(candidate, 'planned_notional', 0.0)))
    if candidate_notional <= 0:
        candidate_notional = abs(_to_float(getattr(candidate, 'entry_price', 0.0) or getattr(candidate, 'last_price', 0.0))) * abs(_to_float(getattr(candidate, 'quantity', 0.0)))

    if candidate is not None:
        if candidate_side == POSITION_SIDE_LONG and max_long_positions > 0 and snapshot['long_count'] >= max_long_positions:
            reasons.append('max_long_positions_reached')
        if candidate_side == POSITION_SIDE_SHORT and max_short_positions > 0 and snapshot['short_count'] >= max_short_positions:
            reasons.append('max_short_positions_reached')
        if per_symbol_single_side_only and candidate_symbol:
            active_sides = snapshot['symbol_sides'].get(candidate_symbol, [])
            if active_sides and candidate_side not in active_sides:
                reasons.append('per_symbol_single_side_only_violation')
            if opposite_side_flip_cooldown_minutes > 0 and active_sides and candidate_side not in active_sides:
                reasons.append('opposite_side_flip_cooldown_active')
        projected_net = snapshot['net_exposure_usdt'] + (candidate_notional if candidate_side == POSITION_SIDE_LONG else -candidate_notional)
        projected_gross = snapshot['gross_exposure_usdt'] + candidate_notional
        if max_net_exposure_usdt > 0 and abs(projected_net) >= max_net_exposure_usdt:
            reasons.append('max_net_exposure_reached')
        if max_gross_exposure_usdt > 0 and projected_gross >= max_gross_exposure_usdt:
            reasons.append('max_gross_exposure_reached')
    snapshot['candidate_symbol'] = candidate_symbol
    snapshot['candidate_side'] = candidate_side
    snapshot['candidate_notional_usdt'] = candidate_notional
    return {'allowed': not reasons, 'reasons': reasons, 'snapshot': snapshot}


def evaluate_risk_guards(symbol: Optional[str] = None, risk_state: Optional[Dict[str, Any]] = None, candidate: Any = None, now_ts: Optional[int] = None, daily_max_loss_usdt: float = 0.0, max_consecutive_losses: int = 0, symbol_cooldown_minutes: int = 0, **kwargs) -> Dict[str, Any]:
    normalized = default_risk_state()
    if isinstance(risk_state, dict):
        normalized.update(risk_state)
    if not isinstance(normalized.get('symbol_cooldowns'), dict):
        normalized['symbol_cooldowns'] = {}
    if not isinstance(normalized.get('portfolio_exposure_pct_by_theme'), dict):
        normalized['portfolio_exposure_pct_by_theme'] = {}
    if not isinstance(normalized.get('portfolio_exposure_pct_by_correlation'), dict):
        normalized['portfolio_exposure_pct_by_correlation'] = {}
    reasons = []
    if normalized.get('halted'):
        reasons.append('strategy_halted')
    pnl = abs(_to_float(normalized.get('daily_realized_pnl_usdt')))
    if daily_max_loss_usdt > 0 and pnl >= daily_max_loss_usdt:
        reasons.append('daily_max_loss_reached')
    if max_consecutive_losses > 0 and int(normalized.get('consecutive_losses', 0) or 0) >= max_consecutive_losses:
        reasons.append('max_consecutive_losses_reached')
    cooldown_until = None
    if symbol:
        cooldown_until = normalized['symbol_cooldowns'].get(symbol)
        ts = int(time.time()) if now_ts is None else int(now_ts)
        if cooldown_until and ts < int(cooldown_until):
            reasons.append('symbol_cooldown_active')
    if candidate is not None:
        state = getattr(candidate, 'state', '')
        execution_slippage_r = compute_expected_slippage_r(candidate)
        execution_liquidity_grade = classify_execution_liquidity_grade(getattr(candidate, 'book_depth_fill_ratio', 0.0), execution_slippage_r)
        if state == 'distribution':
            reasons.append('candidate_distribution_risk')
        if _to_float(getattr(candidate, 'cvd_delta', 0.0)) < 0 and _to_float(getattr(candidate, 'cvd_zscore', 0.0)) <= -2.0:
            reasons.append('candidate_cvd_divergence')
        if _to_float(getattr(candidate, 'oi_change_pct_5m', 0.0)) < 0:
            reasons.append('candidate_oi_reversal')
        if execution_slippage_r > 0.15:
            reasons.append('candidate_execution_slippage_risk')
        if execution_liquidity_grade == 'C' and _to_float(getattr(candidate, 'book_depth_fill_ratio', 0.0)) < 0.5:
            reasons.append('candidate_execution_liquidity_poor')
        position_size_pct = max(_to_float(getattr(candidate, 'position_size_pct', 0.0)), 0.0)
        portfolio_narrative_bucket = str(kwargs.get('portfolio_narrative_bucket') or '').strip()
        portfolio_correlation_group = str(kwargs.get('portfolio_correlation_group') or '').strip()
        max_theme = max(_to_float(kwargs.get('max_portfolio_exposure_pct_per_theme', 0.0)), 0.0)
        max_corr = max(_to_float(kwargs.get('max_portfolio_exposure_pct_per_correlation_group', 0.0)), 0.0)
        if portfolio_narrative_bucket and max_theme > 0 and position_size_pct > 0:
            current_theme = _to_float(normalized['portfolio_exposure_pct_by_theme'].get(portfolio_narrative_bucket))
            if current_theme + position_size_pct >= max_theme:
                reasons.append('candidate_portfolio_theme_overexposure')
        if portfolio_correlation_group and max_corr > 0 and position_size_pct > 0:
            current_corr = _to_float(normalized['portfolio_exposure_pct_by_correlation'].get(portfolio_correlation_group))
            if current_corr + position_size_pct >= max_corr:
                reasons.append('candidate_portfolio_correlation_overexposure')
    return {'allowed': not reasons, 'reasons': reasons, 'cooldown_until': cooldown_until, 'normalized_risk_state': normalized}


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


def recover_unknown_entry_order(client: Any, candidate: Candidate, quantity: float, quantity_precision: int) -> Dict[str, Any]:
    position_side = normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG))
    submit_side = 'SELL' if position_side == POSITION_SIDE_SHORT else 'BUY'
    try:
        response = client.signed_post('/fapi/v1/order', {
            'symbol': candidate.symbol,
            'side': submit_side,
            'positionSide': position_side,
            'type': 'MARKET',
            'quantity': format_decimal(quantity, quantity_precision),
            'newOrderRespType': 'RESULT',
        })
    except Exception as retry_exc:
        raise BinanceAPIError(f'entry order status remained unknown after timeout recovery attempt: {retry_exc}') from retry_exc
    order_id = response.get('orderId') if isinstance(response, dict) else None
    client_order_id = response.get('clientOrderId') if isinstance(response, dict) else None
    try:
        confirmed = query_order(client, candidate.symbol, order_id=order_id, client_order_id=client_order_id)
    except Exception as confirm_exc:
        raise BinanceAPIError(f'entry order status remained unknown after timeout recovery attempt: {confirm_exc}') from confirm_exc
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


def place_live_trade(client: Any, candidate: Candidate, leverage: int, meta: SymbolMeta, args: argparse.Namespace) -> Dict[str, Any]:
    position_side = normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG))
    position_key = build_position_key(candidate.symbol, position_side)
    profile = getattr(args, 'profile', 'default')
    client.signed_post('/fapi/v1/leverage', {'symbol': candidate.symbol, 'leverage': int(leverage)})

    open_positions = fetch_open_positions(client)
    has_existing_position = any(
        isinstance(row, dict)
        and str(row.get('symbol', '')).upper() == candidate.symbol.upper()
        and normalize_position_side(row.get('positionSide')) == position_side
        and abs(_to_float(row.get('positionAmt'))) > 0
        for row in list(open_positions or [])
    )
    if has_existing_position:
        error_payload = {
            'symbol': candidate.symbol,
            'side': position_side,
            'position_key': position_key,
            'profile': profile,
            'preflight_reason': 'existing_position_open',
            'message': 'preflight hard gate: existing_position_open',
        }
        log_runtime_event('error', error_payload)
        emit_notification(args, 'error', error_payload)
        raise BinanceAPIError('preflight hard gate: existing_position_open')

    open_orders = fetch_open_orders(client, candidate.symbol)
    has_existing_open_orders = any(
        isinstance(row, dict)
        and str(row.get('symbol', '')).upper() == candidate.symbol.upper()
        for row in list(open_orders or [])
    )
    if has_existing_open_orders:
        error_payload = {
            'symbol': candidate.symbol,
            'side': position_side,
            'position_key': position_key,
            'profile': profile,
            'preflight_reason': 'existing_open_orders',
            'message': 'preflight hard gate: existing_open_orders',
            'open_order_ids': [row.get('orderId') for row in list(open_orders or []) if isinstance(row, dict)],
        }
        log_runtime_event('error', error_payload)
        emit_notification(args, 'error', error_payload)
        raise BinanceAPIError('preflight hard gate: existing_open_orders')

    execution_quality = compute_execution_quality_size_adjustment(candidate)
    step_size = float(getattr(meta, 'step_size', 0.0) or 0.0)
    quantity_precision = int(getattr(meta, 'quantity_precision', 0) or 0)
    min_qty = float(getattr(meta, 'min_qty', 0.0) or 0.0)
    base_quantity = round_step(candidate.quantity, step_size, quantity_precision)
    scaled_quantity = round_step(base_quantity * float(execution_quality['size_multiplier']), step_size, quantity_precision)
    quantity = scaled_quantity if scaled_quantity >= min_qty else scaled_quantity
    entry_order_error: Optional[Exception] = None
    try:
        entry_order = client.signed_post('/fapi/v1/order', {
            'symbol': candidate.symbol,
            'side': 'SELL' if position_side == POSITION_SIDE_SHORT else 'BUY',
            'positionSide': position_side,
            'type': 'MARKET',
            'quantity': format_decimal(quantity, quantity_precision),
        })
    except Exception as exc:
        entry_order_error = exc
        error_message = str(exc)
        if '-1007' in error_message and 'unknown' in error_message.lower():
            try:
                entry_order = recover_unknown_entry_order(client, candidate, quantity, quantity_precision)
            except Exception as recovery_exc:
                message = str(recovery_exc)
                error_payload = {
                    'symbol': candidate.symbol,
                    'message': message,
                    'profile': profile,
                }
                log_runtime_event('error', error_payload)
                emit_notification(args, 'error', error_payload)
                raise BinanceAPIError(message) from recovery_exc
        else:
            raise
    entry_price = _to_float(entry_order.get('avgPrice')) or float(candidate.last_price)
    filled_quantity = _to_float(entry_order.get('executedQty'), default=quantity)
    entry_order_feedback = {
        'order_id': entry_order.get('orderId'),
        'client_order_id': entry_order.get('clientOrderId'),
        'status': entry_order.get('status'),
        'avg_price': entry_price,
        'executed_qty': filled_quantity,
        'cum_quote': _to_float(entry_order.get('cumQuote')),
        'update_time': entry_order.get('updateTime'),
        'recovered_from_unknown_timeout': bool(entry_order_error is not None),
    }
    plan = build_trade_management_plan(
        entry_price=entry_price,
        stop_price=float(candidate.stop_price),
        quantity=filled_quantity,
        tp1_r=float(getattr(args, 'tp1_r', 1.5)),
        tp1_close_pct=float(getattr(args, 'tp1_close_pct', 0.3)),
        tp2_r=float(getattr(args, 'tp2_r', 2.0)),
        tp2_close_pct=float(getattr(args, 'tp2_close_pct', 0.4)),
        breakeven_r=float(getattr(args, 'breakeven_r', 1.0)),
        atr_stop_distance=float(getattr(candidate, 'atr_stop_distance', 0.0) or 0.0),
        side=normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG)),
    )
    payload = {
        'symbol': candidate.symbol,
        'side': normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG)),
        'position_key': build_position_key(candidate.symbol, getattr(candidate, 'side', POSITION_SIDE_LONG)),
        'entry_price': entry_price,
        'filled_quantity': filled_quantity,
        'stop_price': float(candidate.stop_price),
        'quantity': filled_quantity,
        'entry_order_id': entry_order_feedback['order_id'],
        'entry_client_order_id': entry_order_feedback['client_order_id'],
        'entry_order_status': entry_order_feedback['status'],
        'entry_cum_quote': entry_order_feedback['cum_quote'],
        'entry_update_time': entry_order_feedback['update_time'],
        'profile': getattr(args, 'profile', 'default'),
        'leverage': int(leverage),
    }
    log_runtime_event('entry_filled', payload)
    emit_notification(args, 'entry_filled', payload)
    try:
        stop_order = place_stop_market_order(client, candidate.symbol, float(candidate.stop_price), filled_quantity, meta, side=normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG)))
    except Exception as exc:
        error_payload = {
            'symbol': candidate.symbol,
            'message': f'开仓成功，但挂止损失败: {exc}',
            'profile': getattr(args, 'profile', 'default'),
        }
        log_runtime_event('error', error_payload)
        emit_notification(args, 'error', error_payload)
        raise
    protection = resolve_position_protection_status(
        client,
        candidate.symbol,
        expected_stop_order=stop_order,
        allow_missing_when_flat=True,
        side=normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG)),
    )
    if protection.get('status') == 'missing':
        error_payload = {
            'symbol': candidate.symbol,
            'message': '开仓成功，但止损单未被交易所开放订单确认 (not confirmed by exchange open orders)',
            'expected_stop_order_id': protection.get('expected_order_id'),
            'profile': getattr(args, 'profile', 'default'),
        }
        log_runtime_event('error', error_payload)
        emit_notification(args, 'error', error_payload)
        raise BinanceAPIError('stop order not confirmed by exchange open orders')
    stop_payload = {
        'symbol': candidate.symbol,
        'side': normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG)),
        'position_key': build_position_key(candidate.symbol, getattr(candidate, 'side', POSITION_SIDE_LONG)),
        'stop_price': float(candidate.stop_price),
        'quantity': filled_quantity,
        'stop_order_id': stop_order.get('orderId'),
        'protection_status': protection.get('status'),
        'profile': getattr(args, 'profile', 'default'),
    }
    log_runtime_event('initial_stop_placed', stop_payload)
    emit_notification(args, 'initial_stop_placed', stop_payload)
    return {
        'symbol': candidate.symbol,
        'side': normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG)),
        'position_key': build_position_key(candidate.symbol, getattr(candidate, 'side', POSITION_SIDE_LONG)),
        'entry_order': entry_order,
        'entry_price': entry_price,
        'filled_quantity': filled_quantity,
        'entry_order_feedback': entry_order_feedback,
        'stop_order': stop_order,
        'protection_check': protection,
        'trade_management_plan': asdict(plan),
    }


def monitor_live_trade(client: Any, symbol: str, meta: SymbolMeta, args: argparse.Namespace, trade: Dict[str, Any], store: RuntimeStateStore) -> Dict[str, Any]:
    entry_price = _to_float(trade.get('entry_price'))
    stop_order = trade.get('stop_order') if isinstance(trade.get('stop_order'), dict) else None
    protection_check = trade.get('protection_check') if isinstance(trade.get('protection_check'), dict) else {}
    plan_payload = trade.get('trade_management_plan') if isinstance(trade.get('trade_management_plan'), dict) else {}
    trade_side = normalize_position_side(trade.get('side') or plan_payload.get('side'))
    plan_payload.setdefault('side', trade_side)
    plan_payload.setdefault('position_side', trade_side)
    plan = TradeManagementPlan(**plan_payload)
    positions = store.load_json('positions', {})
    if not isinstance(positions, dict):
        positions = {}
    position_key, tracked = get_position_by_symbol_side(positions, symbol, trade_side)
    state = TradeManagementState(
        symbol=symbol,
        side=position_side_to_trade_side(trade_side),
        position_side=trade_side,
        position_key=position_key,
        initial_quantity=_to_float(tracked.get('quantity') or plan.quantity or trade.get('quantity')),
        remaining_quantity=_to_float(tracked.get('remaining_quantity') or tracked.get('quantity') or plan.quantity),
        current_stop_price=_to_float(tracked.get('stop_price') or tracked.get('current_stop_price') or plan.stop_price, default=plan.stop_price),
        moved_to_breakeven=bool(tracked.get('moved_to_breakeven', False)),
        tp1_hit=bool(tracked.get('tp1_hit', False)),
        tp2_hit=bool(tracked.get('tp2_hit', False)),
        highest_price_seen=_to_float(tracked.get('highest_price_seen') or entry_price, default=entry_price),
        lowest_price_seen=_to_float(tracked.get('lowest_price_seen') or entry_price, default=entry_price),
    )

    def persist_position(status: str, protection_status: Optional[str], active_stop_order: Optional[Dict[str, Any]], exit_reason: Optional[str] = None) -> Dict[str, Any]:
        position_payload = dict(tracked)
        position_payload.update({
            'symbol': symbol,
            'side': state.side,
            'position_key': build_position_key(symbol, state.side),
            'status': status,
            'quantity': round(state.initial_quantity, 10),
            'remaining_quantity': round(state.remaining_quantity, 10),
            'entry_price': round(entry_price, 10),
            'stop_price': round(state.current_stop_price, 10) if state.current_stop_price is not None else None,
            'current_stop_price': round(state.current_stop_price, 10) if state.current_stop_price is not None else None,
            'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) else None,
            'protection_status': protection_status,
            'moved_to_breakeven': state.moved_to_breakeven,
            'tp1_hit': state.tp1_hit,
            'tp2_hit': state.tp2_hit,
            'highest_price_seen': round(state.highest_price_seen, 10) if state.highest_price_seen is not None else None,
            'trade_management_plan': asdict(plan),
            'profile': getattr(args, 'profile', 'default'),
            'exit_reason': exit_reason or position_payload.get('exit_reason'),
        })
        storage_key_hint = str(state.position_key or tracked.get('position_key') or tracked.get('symbol') or build_position_key(symbol, state.side)).upper()
        updated_positions, resolved_key = upsert_position_record(positions, position_payload, key=storage_key_hint)
        positions.clear()
        positions.update(updated_positions)
        state.position_key = resolved_key
        position_payload['position_key'] = resolved_key
        persisted_payload = dict(positions.get(resolved_key, position_payload))
        store.save_json('positions', materialize_positions_state(
            positions,
            {resolved_key: str(tracked.get('position_key') or tracked.get('symbol') or resolved_key).upper()},
            include_legacy_alias=True,
        ))
        tracked.clear()
        tracked.update(persisted_payload)
        return persisted_payload

    def record_event(event_type: str, payload: Dict[str, Any], notify: bool = True) -> Dict[str, Any]:
        event_payload = {
            'symbol': symbol,
            'side': state.side,
            'position_key': state.position_key or build_position_key(symbol, state.side),
            'profile': getattr(args, 'profile', 'default'),
            **payload,
        }
        log_runtime_event(event_type, event_payload)
        row = store.append_event(event_type, event_payload)
        if notify:
            emit_notification(args, event_type, event_payload)
        return row

    persist_position(status='monitoring', protection_status=protection_check.get('status'), active_stop_order=stop_order)
    record_event('entry_filled', {
        'entry_price': round(entry_price, 10),
        'stop_price': round(plan.stop_price, 10),
        'quantity': round(state.initial_quantity, 10),
    })
    record_event('protection_confirmed', {
        'protection_status': protection_check.get('status'),
        'stop_order_id': stop_order.get('orderId') if isinstance(stop_order, dict) else None,
    })

    active_stop_order = stop_order
    protection_status = protection_check.get('status')
    max_cycles = max(int(getattr(args, 'max_monitor_cycles', 20) or 20), 1)
    if getattr(args, 'monitor_poll_interval_sec', None) in (None, 0, 0.0, '0', '0.0') and getattr(args, 'max_monitor_cycles', None) is None:
        max_cycles = max(max_cycles, 50)
    trailing_buffer_pct = float(getattr(args, 'trailing_buffer_pct', 0.02) or 0.02)
    for _ in range(max_cycles):
        if state.remaining_quantity <= 0:
            break
        klines = fetch_klines(client, symbol, '5m', 21)
        positions = store.load_json('positions', {})
        if not isinstance(positions, dict):
            positions = {}
        loop_position_key, tracked = get_position_by_symbol_side(positions, symbol, state.side)
        state.position_key = loop_position_key
        closes = extract_closes(klines)
        highs = extract_highs(klines)
        lows = extract_lows(klines)
        current_price = tracked.get('_debug_current_price')
        if current_price is None:
            current_price = max(_to_float(row[2]) for row in klines) if klines else entry_price
        ema5m = tracked.get('_debug_ema5m')
        if ema5m is None:
            ema5m = closes[-1] if closes else current_price
        trailing_reference = tracked.get('_debug_trailing_reference')
        if trailing_reference is None:
            trailing_reference = min(lows) if lows else min(state.lowest_price_seen or current_price, current_price)
        actions = evaluate_management_actions(
            state,
            plan,
            current_price=current_price,
            ema5m=ema5m,
            trailing_reference=trailing_reference,
            trailing_buffer_pct=trailing_buffer_pct,
            allow_runner_exit=True,
        )
        debug_payload = {'symbol': symbol, 'current_price': current_price, 'ema5m': ema5m, 'trailing_reference': trailing_reference, 'actions': actions, 'remaining_quantity': state.remaining_quantity, 'tracked': tracked, 'max_cycles': max_cycles}
        store.save_json('monitor_debug', debug_payload)
        if not actions:
            time.sleep(float(getattr(args, 'monitor_poll_interval_sec', 2) or 2))
            continue
        for action in actions:
            state, active_stop_order, action_result = apply_management_action(client, symbol, meta, state, action, active_stop_order)
            if state.remaining_quantity <= 0:
                protection_status = 'flat'
            elif action['type'] == 'runner_exit':
                protection_status = 'flat'
            else:
                protection_status = 'protected'
            if action['type'] == 'move_stop_to_breakeven':
                record_event('breakeven_moved', {
                    'new_stop_price': round(action['new_stop_price'], 10),
                    'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) else None,
                    'confirmation_mode': action.get('confirmation_mode', plan.breakeven_confirmation_mode),
                })
            elif action['type'] == 'take_profit_1':
                action.setdefault('exit_reason', 'tp1')
                record_event('tp1_hit', {
                    'close_qty': round(action['close_qty'], 10),
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'new_stop_price': round(action.get('new_stop_price'), 10) if action.get('new_stop_price') is not None else None,
                    'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) else None,
                    'exit_reason': action.get('exit_reason', 'tp1'),
                })
            elif action['type'] == 'take_profit_2':
                action.setdefault('exit_reason', 'tp2')
                record_event('tp2_hit', {
                    'close_qty': round(action['close_qty'], 10),
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'new_stop_price': round(action.get('new_stop_price'), 10) if action.get('new_stop_price') is not None else None,
                    'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) else None,
                    'exit_reason': action.get('exit_reason', 'tp2'),
                })
            elif action['type'] == 'runner_exit':
                action.setdefault('exit_reason', 'runner')
                record_event('runner_exited', {
                    'close_qty': round(action['close_qty'], 10),
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'trailing_floor': round(action.get('trailing_floor'), 10) if action.get('trailing_floor') is not None else None,
                    'exit_reason': action.get('exit_reason', 'runner'),
                })
            if protection_status == 'flat':
                final_exit_reason = action.get('exit_reason', 'flat')
                record_event('trade_invalidated', {
                    'exit_reason': final_exit_reason,
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'protection_status': protection_status,
                }, notify=False)
            persist_position(status='closed' if protection_status == 'flat' else 'monitoring', protection_status=protection_status, active_stop_order=active_stop_order, exit_reason=action.get('exit_reason') if protection_status == 'flat' else None)
            tracked['exit_reason'] = action.get('exit_reason') if protection_status == 'flat' else tracked.get('exit_reason')
            if protection_status == 'flat':
                break
        if protection_status == 'flat':
            break
        time.sleep(float(getattr(args, 'monitor_poll_interval_sec', 2) or 2))

    final_status = 'closed' if state.remaining_quantity <= 0 or protection_status == 'flat' else 'monitoring'
    final_exit_reason = tracked.get('exit_reason')
    persist_position(status=final_status, protection_status='flat' if final_status == 'closed' else protection_status, active_stop_order=active_stop_order if final_status != 'closed' else None, exit_reason=final_exit_reason if final_status == 'closed' else None)
    return {
        'ok': True,
        'mode': 'foreground',
        'symbol': symbol,
        'status': final_status,
        'remaining_quantity': round(state.remaining_quantity, 10),
        'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) and final_status != 'closed' else None,
        'protection_status': 'flat' if final_status == 'closed' else protection_status,
        'exit_reason': final_exit_reason if final_status == 'closed' else None,
    }


def start_trade_monitor_thread(*args, **kwargs):
    thread = threading.Thread(target=monitor_live_trade, kwargs=kwargs, daemon=True, name=f"trade-monitor-{kwargs.get('symbol') or (args[1] if len(args) > 1 else 'unknown')}")
    thread.start()
    return thread


def resolve_auto_loop_book_ticker_symbols(client: BinanceFuturesClient, args: argparse.Namespace) -> List[str]:
    scan_top_n = int(getattr(args, 'top_n', 5) or 5)
    top_gainers = int(getattr(args, 'top_gainers', 20) or 20)
    try:
        square_rows = fetch_square_hot_symbols(client, limit=scan_top_n)
    except Exception:
        square_rows = []
    square_symbols = [str(row.get('symbol', '')).upper() for row in list(square_rows or []) if str(row.get('symbol', '')).strip()]
    try:
        tickers = fetch_24h_tickers(client)
    except Exception:
        tickers = []
    merged_symbols, _, _ = merged_candidate_symbols(
        square_symbols=square_symbols,
        tickers=tickers,
        top_gainers=top_gainers,
    )
    symbols = [str(symbol).upper() for symbol in list(merged_symbols or []) if str(symbol).strip()]
    if symbols:
        return symbols
    if square_symbols:
        return square_symbols
    return ['BTCUSDT']


def run_loop(client: Any, args: argparse.Namespace) -> Dict[str, Any]:
    store = get_runtime_state_store(args)
    reconcile = reconcile_runtime_state(
        client,
        store,
        halt_on_orphan_position=getattr(args, 'halt_on_orphan_position', False),
        repair_missing_protection_enabled=getattr(args, 'repair_missing_protection', True),
    )
    if getattr(args, 'reconcile_only', False):
        return {'mode': 'reconcile_only', 'ok': bool(reconcile.get('ok', True)), 'reconcile': reconcile, 'cycles': []}
    result: Dict[str, Any] = {'ok': bool(reconcile.get('ok', True)), 'cycles': []}
    cycle: Dict[str, Any] = {'reconcile': reconcile}
    result['cycles'].append(cycle)
    if getattr(args, 'auto_loop', False):
        ws_module = globals().get('websocket')
        if ws_module is not None:
            book_ticker_symbols = resolve_auto_loop_book_ticker_symbols(client, args)
            book_ticker_summary = run_book_ticker_websocket_supervisor(
                store,
                initial_symbols=book_ticker_symbols,
                symbol_provider=lambda: resolve_auto_loop_book_ticker_symbols(client, args),
                ws_module=ws_module,
                max_supervisor_cycles=1,
            )
            book_ticker_health = store.load_json('book_ticker_ws_status', {})
            if not isinstance(book_ticker_health, dict):
                book_ticker_health = {}
            cycle['book_ticker_websocket'] = dict(book_ticker_summary, health=book_ticker_health)
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
        return result
    scan_result, best_candidate, meta_map = run_scan_once(client, args)
    cycle['scan'] = scan_result
    if best_candidate is None:
        risk_state = load_risk_state(store)
        cycle['risk_guard'] = evaluate_risk_guards(
            risk_state=risk_state,
            daily_max_loss_usdt=float(getattr(args, 'daily_max_loss_usdt', 0.0) or 0.0),
            max_consecutive_losses=int(getattr(args, 'max_consecutive_losses', 0) or 0),
            symbol_cooldown_minutes=int(getattr(args, 'symbol_cooldown_minutes', 0) or 0),
        )
        return result
    risk_state = load_risk_state(store)
    risk_guard = evaluate_risk_guards(
        symbol=best_candidate.symbol,
        risk_state=risk_state,
        candidate=best_candidate,
        daily_max_loss_usdt=float(getattr(args, 'daily_max_loss_usdt', 0.0) or 0.0),
        max_consecutive_losses=int(getattr(args, 'max_consecutive_losses', 0) or 0),
        symbol_cooldown_minutes=int(getattr(args, 'symbol_cooldown_minutes', 0) or 0),
    )
    open_positions = fetch_open_positions(client) if getattr(args, 'live', False) else []
    portfolio_risk_guard = evaluate_portfolio_risk_guards(
        open_positions=open_positions,
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
    cycle['scan_only'] = bool(getattr(args, 'scan_only', False))
    cycle['live_requested'] = bool(getattr(args, 'live', False))
    if not getattr(args, 'live', False):
        return result
    if not risk_guard['allowed']:
        cycle['live_skipped_due_to_risk_guard'] = risk_guard['reasons']
        append_candidate_rejected_event(store, best_candidate, risk_guard['reasons'])
        return result
    max_open_positions = int(getattr(args, 'max_open_positions', 1) or 1)
    if len(open_positions) >= max_open_positions:
        cycle['live_skipped_due_to_existing_positions'] = open_positions
        append_candidate_rejected_event(store, best_candidate, ['max_open_positions_reached'], {'open_positions': open_positions})
        return result
    meta = meta_map.get(best_candidate.symbol)
    if meta is None:
        raise ValueError(f'missing symbol meta for {best_candidate.symbol}')
    live_execution = place_live_trade(client, best_candidate, int(getattr(args, 'leverage', best_candidate.recommended_leverage) or best_candidate.recommended_leverage), meta, args)
    cycle['live_execution'] = live_execution
    positions_state = store.load_json('positions', {})
    if not isinstance(positions_state, dict):
        positions_state = {}
    live_side = normalize_position_side(live_execution.get('side') or 'LONG')
    position_payload = {
        'symbol': best_candidate.symbol,
        'side': live_side,
        'status': 'open',
        'quantity': live_execution.get('trade_management_plan', {}).get('quantity', live_execution.get('filled_quantity', best_candidate.quantity)),
        'filled_quantity': live_execution.get('filled_quantity', live_execution.get('trade_management_plan', {}).get('quantity', best_candidate.quantity)),
        'entry_price': live_execution.get('entry_price'),
        'stop_price': float(best_candidate.stop_price),
        'stop_order_id': live_execution.get('stop_order', {}).get('orderId'),
        'protection_status': live_execution.get('protection_check', {}).get('status'),
        'entry_order_id': live_execution.get('entry_order_feedback', {}).get('order_id'),
        'entry_client_order_id': live_execution.get('entry_order_feedback', {}).get('client_order_id'),
        'entry_order_status': live_execution.get('entry_order_feedback', {}).get('status'),
        'entry_cum_quote': live_execution.get('entry_order_feedback', {}).get('cum_quote'),
        'entry_update_time': live_execution.get('entry_order_feedback', {}).get('update_time'),
    }
    positions_state, position_key = upsert_position_record(
        positions_state,
        position_payload,
        key=build_position_key(best_candidate.symbol, live_side),
    )
    store.save_json('positions', positions_state)
    if getattr(args, 'auto_loop', False):
        uds_monitor = run_user_data_stream_monitor_cycle(client=client, store=store, symbol=best_candidate.symbol)
        positions_state = store.load_json('positions', {})
        if not isinstance(positions_state, dict):
            positions_state = {}
        _, position_state = get_position_by_symbol_side(positions_state, best_candidate.symbol, live_execution.get('side') or 'LONG')
        if not isinstance(position_state, dict):
            position_state = {}
        position_state['status'] = 'monitoring'
        position_state['monitor_mode'] = 'background_thread'
        position_state['user_data_stream'] = {
            'status': uds_monitor.get('status'),
            'listen_key': uds_monitor.get('listen_key'),
            'health': uds_monitor.get('health', {}),
            'action': uds_monitor.get('action'),
            'now_utc': uds_monitor.get('now_utc'),
        }
        if isinstance(cycle.get('book_ticker_websocket'), dict):
            position_state['book_ticker_websocket'] = cycle['book_ticker_websocket'].get('health', {})
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
        store.save_json('positions', positions_state)
        store.append_event('buy_fill_confirmed', {
            'symbol': best_candidate.symbol,
            'entry_price': live_execution.get('entry_price'),
            'side': positions_state[position_key].get('side'),
            'position_key': positions_state[position_key].get('position_key'),
            'quantity': positions_state[position_key]['quantity'],
            'filled_quantity': positions_state[position_key].get('filled_quantity'),
            'stop_price': positions_state[position_key]['stop_price'],
            'stop_order_id': positions_state[position_key].get('stop_order_id'),
            'protection_status': positions_state[position_key].get('protection_status'),
            'entry_order_id': positions_state[position_key].get('entry_order_id'),
            'entry_client_order_id': positions_state[position_key].get('entry_client_order_id'),
            'entry_order_status': positions_state[position_key].get('entry_order_status'),
            'entry_cum_quote': positions_state[position_key].get('entry_cum_quote'),
            'entry_update_time': positions_state[position_key].get('entry_update_time'),
            'monitor_mode': 'background_thread',
            'monitor_thread_name': positions_state[position_key]['monitor_thread_name'],
            'listen_key': positions_state[position_key]['user_data_stream'].get('listen_key'),
        })
        if uds_monitor.get('status') in {'refresh_failed', 'disconnected'}:
            health = uds_monitor.get('health', {}) if isinstance(uds_monitor.get('health'), dict) else {}
            emit_notification(args, 'user_data_stream_alert', {
                'symbol': best_candidate.symbol,
                'listen_key': uds_monitor.get('listen_key'),
                'status': uds_monitor.get('status'),
                'action': uds_monitor.get('action'),
                'error': uds_monitor.get('error') or health.get('detail'),
                'detail': health.get('detail'),
                'disconnect_count': health.get('disconnect_count', 0),
                'refresh_failure_count': health.get('refresh_failure_count', 0),
                'reconnect_count': health.get('reconnect_count', 0),
                'started_at': health.get('started_at'),
                'last_refresh_at': health.get('last_refresh_at'),
                'updated_at': health.get('updated_at'),
            })
        cycle['trade_management'] = {
            'mode': 'background_thread',
            'thread_name': getattr(thread, 'name', 'trade-monitor'),
            'user_data_stream': uds_monitor,
        }
    else:
        cycle['trade_management'] = monitor_live_trade(client=client, symbol=best_candidate.symbol, meta=meta, args=args, trade=live_execution)
    return result


def print_scan_output(result: Dict[str, Any], output_format: str) -> None:
    if output_format == 'json':
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    print(render_cn_scan_summary(result))


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = apply_runtime_profile(parse_args(argv))
    client = BinanceFuturesClient(
        base_url=args.base_url,
        api_key=os.getenv('BINANCE_FUTURES_API_KEY', ''),
        api_secret=os.getenv('BINANCE_FUTURES_API_SECRET', ''),
    )
    run_loop_fn = globals().get('run_loop')
    if not callable(run_loop_fn):
        result, _, _ = run_scan_once(client, args)
        print_scan_output(result, args.output_format)
        return 0

    if getattr(args, 'auto_loop', False):
        max_cycles = int(getattr(args, 'max_scan_cycles', 0) or 0)
        poll_interval = max(0, int(getattr(args, 'poll_interval_sec', 60) or 60))
        cycle_no = 0
        last_result: Dict[str, Any] = {'ok': True, 'cycles': []}
        try:
            while max_cycles == 0 or cycle_no < max_cycles:
                cycle_no += 1
                last_result = run_loop_fn(client, args)
                if isinstance(last_result, dict):
                    last_result = dict(last_result)
                    last_result['cycle_no'] = cycle_no
                    last_result['auto_loop'] = True
                print_scan_output(last_result, args.output_format)
                if max_cycles and cycle_no >= max_cycles:
                    break
                time.sleep(poll_interval)
        except KeyboardInterrupt:
            interrupted = {'ok': True, 'interrupted': True, 'cycle_no': cycle_no, 'auto_loop': True}
            print_scan_output(interrupted, args.output_format)
            return 0
        return 0

    result = run_loop_fn(client, args)
    print_scan_output(result, args.output_format)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

