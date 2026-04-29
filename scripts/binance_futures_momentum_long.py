from __future__ import annotations

import argparse
import base64
import datetime
import hashlib
import hmac
import json
import math
import os
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlencode

import requests

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


class BinanceFuturesClient:
    def __init__(
        self,
        base_url: str,
        api_key: str = '',
        api_secret: str = '',
        session: Optional[requests.Session] = None,
        max_get_retries: int = 3,
        get_retry_sleep_sec: float = 0.5,
        data_base_url: str = '',
    ):
        self.base_url = base_url.rstrip('/')
        self.data_base_url = (data_base_url or os.getenv('BINANCE_FUTURES_DATA_BASE_URL', 'https://fapi.binance.com')).rstrip('/')
        self.api_key = api_key or ''
        self.api_secret = api_secret or ''
        self.session = session or requests.Session()
        self.max_get_retries = max(1, int(max_get_retries or 1))
        self.get_retry_sleep_sec = max(0.0, float(get_retry_sleep_sec or 0.0))
        self._server_time_offset_ms: Optional[int] = None
        if self.api_key:
            self.session.headers.setdefault('X-MBX-APIKEY', self.api_key)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15):
        last_exc: Optional[BaseException] = None
        base_url = self.data_base_url if str(path or '').startswith('/futures/data/') else self.base_url
        url = f'{base_url}{path}'
        for attempt in range(self.max_get_retries):
            try:
                response = self.session.get(url, params=params or {}, timeout=timeout)
                self._raise_for_status(response)
                return response.json()
            except requests.RequestException as exc:
                last_exc = exc
                if attempt + 1 >= self.max_get_retries:
                    break
                time.sleep(self.get_retry_sleep_sec * (2 ** attempt))
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
        payload = dict(params)
        payload.setdefault('timestamp', self._timestamp_ms())
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

    def _timestamp_ms(self) -> int:
        if self._server_time_offset_ms is None:
            self.sync_server_time()
        return int(time.time() * 1000) + int(self._server_time_offset_ms or 0)

    def sync_server_time(self) -> int:
        try:
            local_before_ms = int(time.time() * 1000)
            response = self.session.get(f'{self.base_url}/fapi/v1/time', timeout=5)
            local_after_ms = int(time.time() * 1000)
            self._raise_for_status(response)
            server_time_ms = int(response.json().get('serverTime'))
            local_midpoint_ms = int((local_before_ms + local_after_ms) / 2)
            self._server_time_offset_ms = server_time_ms - local_midpoint_ms - 1000
        except Exception:
            self._server_time_offset_ms = 0
        return int(self._server_time_offset_ms or 0)

    @staticmethod
    def _raise_for_status(response):
        if response.status_code >= 400:
            try:
                payload = response.json()
            except Exception:
                payload = response.text
            raise BinanceAPIError(f'Binance API error {response.status_code}: {payload}')


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

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15):
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


def fetch_okx_open_positions(client: OKXClient, inst_id: str = '') -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {'instType': 'SWAP'}
    if inst_id:
        params['instId'] = str(inst_id).upper()
    payload = client.get('/api/v5/account/positions', params)
    rows = payload.get('data', []) if isinstance(payload, dict) else []
    positions: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        qty = abs(_to_float(row.get('pos') or row.get('availPos') or row.get('availPosCcy')))
        if qty <= 0:
            continue
        positions.append(row)
    return positions


def okx_position_matches_symbol_side(
    position: Dict[str, Any],
    symbol: str,
    side: Any = 'LONG',
    account_snapshot: Optional[Dict[str, Any]] = None,
) -> bool:
    if not isinstance(position, dict):
        return False
    inst_id = str(position.get('instId') or '').upper()
    if inst_id != normalize_okx_swap_inst_id(symbol):
        return False
    expected_side = normalize_position_side(side)
    pos_side = str(position.get('posSide') or '').strip().lower()
    quantity = _to_float(position.get('pos') or position.get('availPos') or position.get('availPosCcy'))
    if pos_side in {'long', 'short'}:
        return pos_side == ('short' if expected_side == POSITION_SIDE_SHORT else 'long')
    if str((account_snapshot or {}).get('position_mode') or '').lower() == 'net_mode' or pos_side == 'net':
        return quantity < 0 if expected_side == POSITION_SIDE_SHORT else quantity > 0
    return quantity < 0 if expected_side == POSITION_SIDE_SHORT else quantity > 0


def okx_position_exists_for_symbol_side(
    client: OKXClient,
    symbol: str,
    side: Any = 'LONG',
    account_snapshot: Optional[Dict[str, Any]] = None,
) -> bool:
    inst_id = normalize_okx_swap_inst_id(symbol)
    rows = fetch_okx_open_positions(client, inst_id=inst_id)
    return any(okx_position_matches_symbol_side(row, symbol, side, account_snapshot=account_snapshot) for row in rows)


def build_okx_account_snapshot(client: OKXClient) -> Dict[str, Any]:
    config = fetch_okx_account_config(client)
    balance = fetch_okx_account_balance(client)
    details = balance.get('details', []) if isinstance(balance, dict) else []
    usdt = next((row for row in details if str(row.get('ccy', '')).upper() == 'USDT'), {}) if isinstance(details, list) else {}
    acct_lv = str(config.get('acctLv') or '')
    return {
        'exchange': 'OKX',
        'simulated': bool(client.simulated_trading),
        'account_mode': acct_lv,
        'account_mode_label': okx_account_mode_label(acct_lv),
        'position_mode': config.get('posMode'),
        'api_label': config.get('label'),
        'api_permissions': config.get('perm'),
        'total_wallet_balance': _to_float(balance.get('totalEq')),
        'account_total_margin_balance': _to_float(balance.get('totalEq')),
        'account_available_balance': _to_float(usdt.get('availEq') or usdt.get('availBal') or usdt.get('cashBal')),
        'available_balance': _to_float(usdt.get('availEq') or usdt.get('availBal') or usdt.get('cashBal')),
        'usdt_equity': _to_float(usdt.get('eq')),
        'usdt_cash_balance': _to_float(usdt.get('cashBal')),
        'usdt_available_equity': _to_float(usdt.get('availEq')),
        'supports_swap_trading': acct_lv in {'2', '3', '4'},
        'mode_error': 'okx_account_mode_not_supported' if acct_lv == '1' else '',
        'mode_help': 'OKX 合约模拟盘需要 Futures/Multi-currency/Portfolio 账户模式；Spot mode 会触发 51010。第一次切换必须在 OKX 网页或 App 的模拟盘账户模式里完成。',
        'updated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


def fetch_okx_ticker_last(client: OKXClient, inst_id: str) -> float:
    payload = client.public_get('/api/v5/market/ticker', {'instId': inst_id})
    rows = payload.get('data', []) if isinstance(payload, dict) else []
    row = rows[0] if rows and isinstance(rows[0], dict) else {}
    return _to_float(row.get('last') or row.get('markPx') or row.get('idxPx'), default=0.0)


def build_okx_reduce_only_order(
    position: Dict[str, Any],
    quantity: float,
    args: argparse.Namespace,
    instrument: Dict[str, Any],
    account_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    symbol = str(position.get('symbol') or '').upper()
    inst_id = str(instrument.get('instId') or '').upper() or normalize_okx_swap_inst_id(symbol)
    position_side = normalize_position_side(position.get('side') or position.get('position_side'))
    order_side = 'buy' if position_side == POSITION_SIDE_SHORT else 'sell'
    contract_value = abs(_to_float(instrument.get('ctVal'), default=1.0)) or 1.0
    raw_contracts = abs(float(quantity or 0.0)) / contract_value
    lot_size = abs(_to_float(instrument.get('lotSz'), default=1.0)) or 1.0
    contracts = _round_down_to_step(raw_contracts, lot_size)
    if contracts <= 0:
        raise OKXAPIError(f'invalid OKX reduce quantity for {symbol}: {quantity}')
    td_mode = str(position.get('margin_type') or getattr(args, 'margin_type', 'ISOLATED') or 'ISOLATED').lower()
    td_mode = 'cross' if td_mode in {'crossed', 'cross'} else 'isolated'
    order = {
        'instId': inst_id,
        'tdMode': td_mode,
        'side': order_side,
        'ordType': 'market',
        'sz': _format_decimal(contracts),
        'reduceOnly': 'true',
    }
    if str((account_snapshot or {}).get('position_mode') or '').lower() != 'net_mode':
        order['posSide'] = 'short' if position_side == POSITION_SIDE_SHORT else 'long'
    return order


def place_okx_reduce_only_market(
    okx_client: OKXClient,
    position: Dict[str, Any],
    quantity: float,
    args: argparse.Namespace,
    account_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    symbol = str(position.get('symbol') or '').upper()
    inst_id = normalize_okx_swap_inst_id(symbol)
    instrument = fetch_okx_swap_instrument(okx_client, inst_id)
    order = build_okx_reduce_only_order(position, quantity, args, instrument, account_snapshot)
    response = okx_client.post('/api/v5/trade/order', order)
    order_row = (response.get('data') or [{}])[0] if isinstance(response, dict) else {}
    return {
        'exchange': 'OKX',
        'simulated': True,
        'symbol': symbol,
        'inst_id': order.get('instId'),
        'side': normalize_position_side(position.get('side') or position.get('position_side')),
        'closed_quantity': float(quantity or 0.0),
        'okx_order': order,
        'okx_response': response,
        'order_feedback': {
            'order_id': order_row.get('ordId'),
            'client_order_id': order_row.get('clOrdId'),
            'status': order_row.get('sCode', response.get('code')),
            'message': order_row.get('sMsg', ''),
        },
    }


def is_binance_simulated_trading(args: argparse.Namespace) -> bool:
    return bool(getattr(args, 'binance_simulated_trading', False))


def execution_exchange_label(args: argparse.Namespace) -> str:
    if bool(getattr(args, 'okx_simulated_trading', False)):
        return 'OKX_SIMULATED'
    if is_binance_simulated_trading(args):
        return 'BINANCE_SIMULATED'
    return 'BINANCE'


def resolve_okx_simulated_api_credentials() -> Tuple[str, str, str]:
    api_key = os.getenv('OKX_SIMULATED_API_KEY') or os.getenv('OKX_API_KEY', '')
    api_secret = os.getenv('OKX_SIMULATED_SECRET_KEY') or os.getenv('OKX_SECRET_KEY', '')
    passphrase = os.getenv('OKX_SIMULATED_PASSPHRASE') or os.getenv('OKX_PASSPHRASE', '')
    return api_key, api_secret, passphrase


def resolve_binance_api_credentials(args: argparse.Namespace) -> Tuple[str, str]:
    if is_binance_simulated_trading(args):
        api_key = os.getenv('BINANCE_FUTURES_TESTNET_API_KEY') or os.getenv('BINANCE_FUTURES_API_KEY', '')
        api_secret = os.getenv('BINANCE_FUTURES_TESTNET_API_SECRET') or os.getenv('BINANCE_FUTURES_API_SECRET', '')
        return api_key, api_secret
    return os.getenv('BINANCE_FUTURES_API_KEY', ''), os.getenv('BINANCE_FUTURES_API_SECRET', '')


def build_okx_simulated_order(
    candidate: Any,
    leverage: int,
    args: argparse.Namespace,
    instrument: Dict[str, Any],
    account_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    inst_id = str(instrument.get('instId') or '').upper() or normalize_okx_swap_inst_id(getattr(candidate, 'symbol', ''))
    position_side = normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG))
    side = 'sell' if position_side == POSITION_SIDE_SHORT else 'buy'
    pos_side = 'short' if position_side == POSITION_SIDE_SHORT else 'long'
    base_quantity = abs(_to_float(getattr(candidate, 'quantity', 0.0)))
    contract_value = abs(_to_float(instrument.get('ctVal'), default=1.0)) or 1.0
    raw_contracts = base_quantity / contract_value
    lot_size = abs(_to_float(instrument.get('lotSz'), default=1.0)) or 1.0
    min_size = abs(_to_float(instrument.get('minSz'), default=lot_size)) or lot_size
    contracts = _round_down_to_step(raw_contracts, lot_size)
    if contracts < min_size:
        contracts = min_size
    td_mode = 'cross' if str(getattr(args, 'margin_type', 'ISOLATED')).upper() == 'CROSSED' else 'isolated'
    order = {
        'instId': inst_id,
        'tdMode': td_mode,
        'side': side,
        'ordType': 'market',
        'sz': _format_decimal(contracts),
        'lever': str(int(leverage)),
    }
    if str((account_snapshot or {}).get('position_mode') or '').lower() != 'net_mode':
        order['posSide'] = pos_side
    return order


def place_okx_simulated_trade(okx_client: OKXClient, candidate: Any, leverage: int, args: argparse.Namespace) -> Dict[str, Any]:
    inst_id = normalize_okx_swap_inst_id(getattr(candidate, 'symbol', ''))
    if not inst_id:
        raise OKXAPIError('missing OKX instrument id')
    account_snapshot = build_okx_account_snapshot(okx_client)
    if not bool(account_snapshot.get('supports_swap_trading')):
        raise OKXAPIError(
            'OKX API error 51010: current account mode does not support SWAP trading; '
            f"acctLv={account_snapshot.get('account_mode') or 'unknown'} "
            f"({account_snapshot.get('account_mode_label')}); "
            'switch Demo Trading account mode to Futures/Multi-currency/Portfolio on OKX Web/App first.'
        )
    instrument = fetch_okx_swap_instrument(okx_client, inst_id)
    order = build_okx_simulated_order(candidate, leverage, args, instrument, account_snapshot)
    response = okx_client.post('/api/v5/trade/order', order)
    order_row = (response.get('data') or [{}])[0] if isinstance(response, dict) else {}
    position_side = normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG))
    entry_price = float(getattr(candidate, 'last_price', 0.0) or 0.0)
    filled_quantity = float(getattr(candidate, 'quantity', 0.0) or 0.0)
    plan = build_trade_management_plan(
        entry_price=entry_price,
        stop_price=float(getattr(candidate, 'stop_price')),
        quantity=filled_quantity,
        tp1_r=float(getattr(args, 'tp1_r', 1.5)),
        tp1_close_pct=float(getattr(args, 'tp1_close_pct', 0.3)),
        tp2_r=float(getattr(args, 'tp2_r', 2.0)),
        tp2_close_pct=float(getattr(args, 'tp2_close_pct', 0.4)),
        breakeven_r=float(getattr(args, 'breakeven_r', 1.0)),
        atr_stop_distance=float(getattr(candidate, 'atr_stop_distance', 0.0) or 0.0),
        side=position_side,
        breakeven_confirmation_mode=str(getattr(args, 'breakeven_confirmation_mode', 'ema_support') or 'ema_support'),
        breakeven_min_buffer_pct=float(getattr(args, 'breakeven_min_buffer_pct', 0.001) or 0.0),
    )
    return {
        'exchange': 'OKX',
        'simulated': True,
        'symbol': str(getattr(candidate, 'symbol', '')).upper(),
        'inst_id': inst_id,
        'side': position_side,
        'entry_price': entry_price,
        'filled_quantity': filled_quantity,
        'margin_type': order.get('tdMode', '').upper(),
        'leverage': int(leverage),
        'entry_order_feedback': {
            'order_id': order_row.get('ordId'),
            'client_order_id': order_row.get('clOrdId'),
            'status': order_row.get('sCode', response.get('code')),
            'message': order_row.get('sMsg', ''),
        },
        'okx_order': order,
        'okx_response': response,
        'okx_account': account_snapshot,
        'trade_management_plan': asdict(plan),
        'stop_order': {},
        'protection_check': {'status': 'simulated', 'side': position_side},
    }




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
        row = {'event_type': event_type, 'recorded_at': _isoformat_utc(_utc_now()), **normalize_runtime_event_payload(payload or {})}
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
    state_name = 'event_rate_limit_state'
    state = store.load_json(state_name, {})
    if not isinstance(state, dict):
        state = {}
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
    should_append = last_event_at is None or (now - last_event_at).total_seconds() >= float(min_interval_seconds or 0.0)
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
        store.save_json(state_name, state)
        return row
    event_state[normalized_key] = {
        **bucket,
        'suppressed_since_last': suppressed_since_last + 1,
        'last_suppressed_at': _isoformat_utc(now),
        'last_payload': payload or {},
    }
    store.save_json(state_name, state)
    return None


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


def build_user_data_stream_position_payload(uds_monitor: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'status': uds_monitor.get('status'),
        'listen_key': uds_monitor.get('listen_key'),
        'health': uds_monitor.get('health', {}),
        'action': uds_monitor.get('action'),
        'now_utc': uds_monitor.get('now_utc'),
    }


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


def emit_user_data_stream_alert_if_needed(args: argparse.Namespace, symbol: Optional[str], uds_monitor: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if uds_monitor.get('status') not in {'refresh_failed', 'disconnected'}:
        return None
    health = uds_monitor.get('health', {}) if isinstance(uds_monitor.get('health'), dict) else {}
    payload = {
        'symbol': symbol or health.get('symbol'),
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
    }
    emit_notification(args, 'user_data_stream_alert', payload)
    return payload


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
        'side': getattr(candidate, 'side', getattr(candidate, 'position_side', '')),
        'position_side': getattr(candidate, 'position_side', ''),
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
        'stop_model': getattr(candidate, 'stop_model', 'structure'),
        'stop_distance_pct': round(float(getattr(candidate, 'stop_distance_pct', 0.0) or 0.0), 4),
        'stop_too_tight_flag': bool(getattr(candidate, 'stop_too_tight_flag', False)),
        'stop_too_wide_flag': bool(getattr(candidate, 'stop_too_wide_flag', False)),
        'candle_extension_pct': round(float(candidate.candle_extension_pct or 0.0), 4),
        'recent_3bar_runup_pct': round(float(candidate.recent_3bar_runup_pct or 0.0), 4),
        'overextension_flag': candidate.overextension_flag,
        'entry_pattern': candidate.entry_pattern,
        'trend_regime': candidate.trend_regime,
        'liquidity_grade': candidate.liquidity_grade,
        'setup_ready': bool(candidate.setup_ready),
        'trigger_fired': bool(candidate.trigger_fired),
        'candidate_stage': getattr(candidate, 'candidate_stage', 'watch_candidate'),
        'setup_missing': list(getattr(candidate, 'setup_missing', []) or []),
        'trigger_missing': list(getattr(candidate, 'trigger_missing', []) or []),
        'trade_missing': list(getattr(candidate, 'trade_missing', []) or []),
        'trigger_confirmation_flags': dict(getattr(candidate, 'trigger_confirmation_flags', {}) or {}),
        'trigger_confirmation_count': int(getattr(candidate, 'trigger_confirmation_count', 0) or 0),
        'trigger_min_confirmations': int(getattr(candidate, 'trigger_min_confirmations', 0) or 0),
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
    return {
        'opened_at': state.opened_at,
        'first_1r_at': state.first_1r_at,
        'closed_at': _isoformat_utc(effective_closed_at) if closed_at is not None else None,
        'mfe_r': mfe_r,
        'mae_r': mae_r,
        'time_to_1r': time_to_1r_minutes,
        'time_to_1r_minutes': time_to_1r_minutes,
        'time_in_trade_minutes': time_in_trade_minutes,
        'realized_r': round(_to_float(state.realized_r, default=0.0), 4),
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


def build_trade_management_plan(entry_price: float, stop_price: float, quantity: float, tp1_r: float, tp1_close_pct: float, tp2_r: float, tp2_close_pct: float, breakeven_r: float = 1.0, atr_stop_distance: Optional[float] = None, side: str = POSITION_SIDE_LONG, breakeven_confirmation_mode: str = 'price_only', breakeven_min_buffer_pct: float = 0.0) -> TradeManagementPlan:
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
        breakeven_confirmation_mode=str(breakeven_confirmation_mode or 'price_only'),
        breakeven_min_buffer_pct=max(float(breakeven_min_buffer_pct or 0.0), 0.0),
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
    return client.signed_post('/fapi/v1/order/cancel', params)


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
    }
    if should_send_position_side(client):
        params['positionSide'] = position_side
        params['reduceOnly'] = 'true'
    try:
        return client.signed_post('/fapi/v1/algoOrder', params)
    except Exception as exc:
        if 'reduceOnly' in params and is_reduce_only_not_required_error(exc):
            params.pop('reduceOnly', None)
            try:
                return client.signed_post('/fapi/v1/algoOrder', params)
            except Exception as retry_exc:
                exc = retry_exc
        if not should_send_position_side(client) or not is_position_side_mode_error(exc):
            raise
        mark_one_way_position_mode(client)
        params.pop('positionSide', None)
        params.pop('reduceOnly', None)
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


def build_local_open_positions_for_risk(store: RuntimeStateStore) -> List[Dict[str, Any]]:
    positions_state = store.load_json('positions', {})
    rows: List[Dict[str, Any]] = []
    for _key, position in iter_canonical_open_positions(positions_state):
        side = normalize_position_side(position.get('side') or position.get('position_side'))
        quantity = abs(_to_float(position.get('remaining_quantity') or position.get('quantity') or position.get('filled_quantity')))
        entry_price = abs(_to_float(position.get('entry_price')))
        rows.append({
            'symbol': str(position.get('symbol') or '').upper(),
            'side': side,
            'positionSide': side,
            'quantity': quantity,
            'positionAmt': quantity if side == POSITION_SIDE_LONG else -quantity,
            'entryPrice': entry_price,
            'notional': abs(_to_float(position.get('notional'))) or quantity * entry_price,
        })
    return rows


def build_trade_management_plan_from_position(position: Dict[str, Any], args: argparse.Namespace) -> TradeManagementPlan:
    plan_payload = position.get('trade_management_plan')
    if isinstance(plan_payload, dict) and plan_payload:
        payload = dict(plan_payload)
        payload.setdefault('side', normalize_position_side(position.get('side') or position.get('position_side')))
        return TradeManagementPlan(**payload)
    entry_price = _to_float(position.get('entry_price'))
    stop_price = _to_float(position.get('stop_price') or position.get('current_stop_price'))
    quantity = _to_float(position.get('quantity') or position.get('filled_quantity') or position.get('remaining_quantity'))
    return build_trade_management_plan(
        entry_price=entry_price,
        stop_price=stop_price,
        quantity=quantity,
        tp1_r=float(getattr(args, 'tp1_r', 1.5)),
        tp1_close_pct=float(getattr(args, 'tp1_close_pct', 0.3)),
        tp2_r=float(getattr(args, 'tp2_r', 2.0)),
        tp2_close_pct=float(getattr(args, 'tp2_close_pct', 0.4)),
        breakeven_r=float(getattr(args, 'breakeven_r', 1.0)),
        atr_stop_distance=float(position.get('atr_stop_distance') or 0.0),
        side=normalize_position_side(position.get('side') or position.get('position_side')),
        breakeven_confirmation_mode=str(getattr(args, 'breakeven_confirmation_mode', 'ema_support') or 'ema_support'),
        breakeven_min_buffer_pct=float(getattr(args, 'breakeven_min_buffer_pct', 0.001) or 0.0),
    )


def manage_okx_simulated_positions(store: RuntimeStateStore, args: argparse.Namespace, okx_client: OKXClient) -> Dict[str, Any]:
    positions_state = store.load_json('positions', {})
    if not isinstance(positions_state, dict):
        positions_state = {}
    canonical_positions = migrate_positions_state(positions_state)
    if not iter_canonical_open_positions(canonical_positions):
        store.save_json('positions', materialize_positions_state(canonical_positions, include_legacy_alias=True))
        return {'ok': True, 'actions': [], 'errors': [], 'tracked_positions': 0}
    actions_taken: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    try:
        account_snapshot = build_okx_account_snapshot(okx_client)
    except Exception:
        account_snapshot = {}
    for position_key, position in iter_canonical_open_positions(canonical_positions):
        symbol = str(position.get('symbol') or '').upper()
        side = normalize_position_side(position.get('side') or position.get('position_side'))
        remaining = abs(_to_float(position.get('remaining_quantity') or position.get('quantity') or position.get('filled_quantity')))
        if not symbol or remaining <= 0:
            continue
        try:
            current_price = fetch_okx_ticker_last(okx_client, normalize_okx_swap_inst_id(symbol))
            if current_price <= 0:
                continue
            plan = build_trade_management_plan_from_position(position, args)
            state = TradeManagementState(
                symbol=symbol,
                side=side,
                position_side=side,
                position_key=position_key,
                initial_quantity=_to_float(position.get('quantity') or position.get('filled_quantity') or plan.quantity),
                remaining_quantity=remaining,
                current_stop_price=_to_float(position.get('current_stop_price') or position.get('stop_price') or plan.stop_price, default=plan.stop_price),
                moved_to_breakeven=bool(position.get('moved_to_breakeven', False)),
                tp1_hit=bool(position.get('tp1_hit', False)),
                tp2_hit=bool(position.get('tp2_hit', False)),
                highest_price_seen=_to_float(position.get('highest_price_seen') or position.get('entry_price'), default=_to_float(position.get('entry_price'))),
                lowest_price_seen=_to_float(position.get('lowest_price_seen') or position.get('entry_price'), default=_to_float(position.get('entry_price'))),
            )
            stop_hit = current_price <= state.current_stop_price if side == POSITION_SIDE_LONG else current_price >= state.current_stop_price
            actions = [{'type': 'stop_exit', 'close_qty': remaining, 'exit_reason': 'stop'}] if stop_hit else evaluate_management_actions(
                state,
                plan,
                current_price=current_price,
                ema5m=current_price,
                trailing_reference=current_price,
                trailing_buffer_pct=float(getattr(args, 'trailing_buffer_pct', 0.02) or 0.02),
                allow_runner_exit=True,
            )
            if not actions:
                position.update({
                    'highest_price_seen': state.highest_price_seen,
                    'lowest_price_seen': state.lowest_price_seen,
                    'last_management_price': current_price,
                    'last_management_at': _isoformat_utc(_utc_now()),
                    'monitor_mode': 'okx_simulated_loop',
                })
                canonical_positions, _ = upsert_position_record(canonical_positions, position, key=position_key)
                continue
            for action in actions:
                action_type = str(action.get('type') or '')
                if action_type == 'move_stop_to_breakeven':
                    state.current_stop_price = _to_float(action.get('new_stop_price'), default=state.current_stop_price or plan.entry_price)
                    state.moved_to_breakeven = True
                    position.update({
                        'current_stop_price': state.current_stop_price,
                        'moved_to_breakeven': True,
                        'last_management_price': current_price,
                        'last_management_at': _isoformat_utc(_utc_now()),
                        'monitor_mode': 'okx_simulated_loop',
                    })
                    canonical_positions, _ = upsert_position_record(canonical_positions, position, key=position_key)
                    store.append_event('okx_breakeven_moved', {
                        'symbol': symbol,
                        'side': side,
                        'position_key': position_key,
                        'new_stop_price': state.current_stop_price,
                        'profile': getattr(args, 'profile', 'default'),
                    })
                    continue
                close_qty = min(abs(_to_float(action.get('close_qty'))), state.remaining_quantity)
                if close_qty <= 0:
                    continue
                try:
                    close_result = place_okx_reduce_only_market(okx_client, position, close_qty, args, account_snapshot)
                except OKXAPIError as exc:
                    if is_okx_reduce_position_missing_error(exc):
                        exchange_position_exists = True
                        try:
                            exchange_position_exists = okx_position_exists_for_symbol_side(
                                okx_client,
                                symbol,
                                side,
                                account_snapshot=account_snapshot,
                            )
                        except Exception:
                            exchange_position_exists = True
                        if not exchange_position_exists:
                            exit_reason = str(action.get('exit_reason') or ('stop' if action_type == 'stop_exit' else 'exchange_position_missing'))
                            position.update({
                                'remaining_quantity': 0.0,
                                'current_stop_price': _to_float(action.get('new_stop_price'), default=state.current_stop_price or plan.stop_price),
                                'moved_to_breakeven': state.moved_to_breakeven,
                                'tp1_hit': state.tp1_hit,
                                'tp2_hit': state.tp2_hit,
                                'last_management_price': current_price,
                                'last_management_at': _isoformat_utc(_utc_now()),
                                'monitor_mode': 'okx_simulated_loop',
                                'status': 'closed',
                                'protection_status': 'flat',
                                'exit_reason': exit_reason,
                                'exchange_reconcile_reason': 'okx_position_missing_after_reduce_failure',
                            })
                            canonical_positions, _ = upsert_position_record(canonical_positions, position, key=position_key)
                            event = store.append_event('okx_position_reconciled_closed', {
                                'symbol': symbol,
                                'side': side,
                                'position_key': position_key,
                                'close_qty': close_qty,
                                'remaining_quantity': 0.0,
                                'current_price': current_price,
                                'exit_reason': exit_reason,
                                'reconcile_reason': 'okx_position_missing_after_reduce_failure',
                                'error': str(exc),
                                'profile': getattr(args, 'profile', 'default'),
                            })
                            actions_taken.append(event)
                            state.remaining_quantity = 0.0
                            break
                    raise
                state.remaining_quantity = round(max(state.remaining_quantity - close_qty, 0.0), 10)
                if action_type == 'take_profit_1':
                    state.tp1_hit = True
                elif action_type == 'take_profit_2':
                    state.tp2_hit = True
                exit_reason = str(action.get('exit_reason') or ('stop' if action_type == 'stop_exit' else action_type))
                position.update({
                    'remaining_quantity': state.remaining_quantity,
                    'current_stop_price': _to_float(action.get('new_stop_price'), default=state.current_stop_price or plan.stop_price),
                    'moved_to_breakeven': state.moved_to_breakeven,
                    'tp1_hit': state.tp1_hit,
                    'tp2_hit': state.tp2_hit,
                    'last_management_price': current_price,
                    'last_management_at': _isoformat_utc(_utc_now()),
                    'last_reduce_order_id': close_result.get('order_feedback', {}).get('order_id'),
                    'monitor_mode': 'okx_simulated_loop',
                    'status': 'closed' if state.remaining_quantity <= 0 else 'open',
                    'protection_status': 'flat' if state.remaining_quantity <= 0 else position.get('protection_status', 'simulated'),
                    'exit_reason': exit_reason if state.remaining_quantity <= 0 else position.get('exit_reason'),
                })
                canonical_positions, _ = upsert_position_record(canonical_positions, position, key=position_key)
                event_type = {
                    'take_profit_1': 'okx_tp1_hit',
                    'take_profit_2': 'okx_tp2_hit',
                    'runner_exit': 'okx_runner_exited',
                    'stop_exit': 'okx_stop_exited',
                }.get(action_type, 'okx_position_reduced')
                event = store.append_event(event_type, {
                    'symbol': symbol,
                    'side': side,
                    'position_key': position_key,
                    'close_qty': close_qty,
                    'remaining_quantity': state.remaining_quantity,
                    'current_price': current_price,
                    'exit_reason': exit_reason,
                    'order_id': close_result.get('order_feedback', {}).get('order_id'),
                    'profile': getattr(args, 'profile', 'default'),
                })
                actions_taken.append(event)
                if state.remaining_quantity <= 0:
                    break
        except Exception as exc:
            error_payload = {
                'symbol': symbol,
                'side': side,
                'position_key': position_key,
                'message': str(exc),
                'profile': getattr(args, 'profile', 'default'),
            }
            errors.append(error_payload)
            store.append_event('okx_management_action_failed', error_payload)
    store.save_json('positions', materialize_positions_state(canonical_positions, include_legacy_alias=True))
    return {'ok': not errors, 'actions': actions_taken, 'errors': errors, 'tracked_positions': len(iter_canonical_open_positions(canonical_positions))}


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


def derive_regime_entry_thresholds(side: str, regime_label: str, min_5m_change_pct: float, base_acceleration_ratio: float = 1.5) -> Dict[str, float]:
    trade_side = normalize_trade_side(side)
    regime = str(regime_label or 'neutral').strip().lower()
    change_threshold = float(min_5m_change_pct or 0.0)
    acceleration_threshold = float(base_acceleration_ratio or 0.0)

    if trade_side == TRADE_SIDE_LONG:
        if regime == 'risk_on':
            change_threshold *= 0.85
            acceleration_threshold = max(1.2, acceleration_threshold - 0.15)
        elif regime == 'caution':
            change_threshold *= 1.1
            acceleration_threshold += 0.15
        elif regime == 'risk_off':
            change_threshold *= 1.25
            acceleration_threshold += 0.35
    else:
        if regime == 'risk_off':
            change_threshold *= 0.85
            acceleration_threshold = max(1.2, acceleration_threshold - 0.15)
        elif regime == 'caution':
            change_threshold *= 1.05
            acceleration_threshold += 0.1
        elif regime == 'risk_on':
            change_threshold *= 1.25
            acceleration_threshold += 0.35

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


def compute_market_regime_filter(btc_klines: Optional[Sequence[Sequence[Any]]] = None, sol_klines: Optional[Sequence[Sequence[Any]]] = None) -> Dict[str, Any]:
    reasons: List[str] = []
    score_multiplier = 1.0

    def evaluate(label: str, klines: Optional[Sequence[Sequence[Any]]]) -> Tuple[bool, bool, bool, bool]:
        if not klines or len(klines) < 5:
            return False, False, False, False
        closes = extract_closes(klines)
        price = closes[-1]
        ema_length = min(20, len(closes))
        ema20 = compute_ema(closes, ema_length)
        trend_down = price < ema20
        trend_up = price > ema20
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
        return trend_down, momentum_breakdown, trend_up, momentum_breakout

    btc_trend_down, btc_momo_down, btc_trend_up, btc_momo_up = evaluate('btc', btc_klines)
    sol_trend_down, sol_momo_down, sol_trend_up, sol_momo_up = evaluate('sol', sol_klines)
    btc_bad = btc_trend_down or btc_momo_down
    sol_bad = sol_trend_down or sol_momo_down
    if btc_bad:
        score_multiplier *= 0.7
    if sol_bad:
        score_multiplier *= 0.8
    if (btc_trend_down and sol_trend_down) or (btc_momo_down and sol_momo_down):
        label = 'risk_off'
        score_multiplier = min(score_multiplier, 0.55)
    elif btc_bad or sol_bad:
        label = 'caution'
        score_multiplier = min(score_multiplier, 0.85)
    elif btc_trend_up and sol_trend_up and (btc_momo_up or sol_momo_up):
        label = 'risk_on'
        score_multiplier = 1.05
        if btc_momo_up:
            score_multiplier += 0.05
        if sol_momo_up:
            score_multiplier += 0.05
    else:
        label = 'neutral'
    return {
        'risk_on': label == 'risk_on',
        'score_multiplier': max(0.35, min(score_multiplier, 1.15)),
        'reasons': reasons,
        'label': label,
        'trend_flags': {'btc': btc_trend_down, 'sol': sol_trend_down},
        'momentum_flags': {'btc': btc_momo_down, 'sol': sol_momo_down},
        'bullish_trend_flags': {'btc': btc_trend_up, 'sol': sol_trend_up},
        'bullish_momentum_flags': {'btc': btc_momo_up, 'sol': sol_momo_up},
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
    loser_rank: Optional[int] = None,
    okx_sentiment_score: float = 0.0,
    okx_sentiment_acceleration: float = 0.0,
    sector_resonance_score: float = 0.0,
    smart_money_flow_score: float = 0.0,
    microstructure_inputs: Optional[Dict[str, Any]] = None,
    max_notional_usdt: float = 0.0,
    side: str = TRADE_SIDE_LONG,
    **legacy_kwargs: Any,
) -> Optional[Candidate]:
    early_reject_stats = legacy_kwargs.get('early_reject_stats')

    def early_reject(reason: str) -> None:
        if not isinstance(early_reject_stats, dict):
            return
        by_reason = early_reject_stats.setdefault('by_reason', {})
        by_side = early_reject_stats.setdefault('by_side', {})
        reason_text = str(reason or 'unknown')
        by_reason[reason_text] = int(by_reason.get(reason_text, 0) or 0) + 1
        side_text = normalize_trade_side(side)
        side_bucket = by_side.setdefault(side_text, {})
        side_bucket[reason_text] = int(side_bucket.get(reason_text, 0) or 0) + 1
        early_reject_stats['total'] = int(early_reject_stats.get('total', 0) or 0) + 1

    if len(klines_5m) < max(lookback_bars + 2, swing_bars + 20, 30):
        early_reject('insufficient_5m_klines')
        return None
    if len(klines_15m) < 20 or len(klines_1h) < 25 or len(klines_4h) < 25:
        early_reject('insufficient_higher_tf_klines')
        return None

    trade_side = normalize_trade_side(side)
    position_side = trade_side_to_position_side(trade_side)
    higher_timeframe_bias = trade_side
    regime_payload = legacy_kwargs.get('market_regime') or {}
    regime_label = str(regime_payload.get('label', 'neutral') or 'neutral')
    entry_thresholds = derive_regime_entry_thresholds(trade_side, regime_label, min_5m_change_pct, base_acceleration_ratio=1.5)
    effective_min_5m_change_pct = float(entry_thresholds.get('min_5m_change_pct', min_5m_change_pct) or 0.0)
    effective_acceleration_threshold = float(entry_thresholds.get('acceleration_ratio', 1.5) or 1.5)
    setup_breakout_tolerance_pct = max(_to_float(legacy_kwargs.get('setup_breakout_tolerance_pct'), default=0.0), 0.0)
    watch_breakout_tolerance_pct = max(
        _to_float(legacy_kwargs.get('watch_breakout_tolerance_pct'), default=setup_breakout_tolerance_pct),
        0.0,
    )
    external_setup = derive_external_setup_params(
        legacy_kwargs.get('external_signal'),
        enabled=bool(legacy_kwargs.get('use_external_setup_relaxation')),
    )
    if external_setup.get('enabled'):
        effective_min_5m_change_pct *= float(external_setup.get('min_5m_change_pct_multiplier', 1.0) or 1.0)
        min_volume_multiple *= float(external_setup.get('min_volume_multiple_multiplier', 1.0) or 1.0)
        min_quote_volume = min(float(min_quote_volume or 0.0), float(external_setup.get('min_quote_volume', min_quote_volume) or min_quote_volume))

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
    if breakout_level:
        entry_distance_from_breakout_pct = (((last_price / breakout_level) - 1.0) * 100) if trade_side == TRADE_SIDE_LONG else (((breakout_level / last_price) - 1.0) * 100)
    else:
        entry_distance_from_breakout_pct = 0.0
    near_external_breakout_setup = bool(
        external_setup.get('enabled')
        and entry_distance_from_breakout_pct >= -float(external_setup.get('max_breakout_distance_pct', 0.0) or 0.0)
    )
    near_configured_watch_setup = bool(
        watch_breakout_tolerance_pct > 0.0
        and entry_distance_from_breakout_pct >= -watch_breakout_tolerance_pct
    )
    near_configured_setup = bool(
        setup_breakout_tolerance_pct > 0.0
        and entry_distance_from_breakout_pct >= -setup_breakout_tolerance_pct
    )
    near_breakout_setup = near_external_breakout_setup or near_configured_watch_setup
    stop_price = round_price(stop_price_raw, meta.tick_size, meta.price_precision)
    structure_stop_price = stop_price
    stop_model = 'structure'
    if stop_price <= 0:
        early_reject('invalid_stop_price')
        return None
    if trade_side == TRADE_SIDE_SHORT and stop_price <= last_price:
        early_reject('invalid_short_stop_distance')
        return None
    if trade_side == TRADE_SIDE_LONG and stop_price >= last_price:
        early_reject('invalid_long_stop_distance')
        return None
    risk_per_unit = abs(last_price - stop_price)
    if risk_per_unit <= 0:
        early_reject('invalid_risk_per_unit')
        return None
    quantity = round_step(risk_usdt / risk_per_unit, meta.step_size, meta.quantity_precision)
    if max_notional_usdt > 0:
        max_qty_by_notional = round_step(max_notional_usdt / last_price, meta.step_size, meta.quantity_precision)
        quantity = min(quantity, max_qty_by_notional)
    if quantity < meta.min_qty or quantity <= 0:
        early_reject('quantity_below_min_qty')
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
            prior_stop_price = stop_price
            stop_price = max(stop_price, atr_stop_price) if trade_side == TRADE_SIDE_LONG else min(stop_price, atr_stop_price)
            if abs(stop_price - atr_stop_price) <= max(float(meta.tick_size or 0.0), 1e-12) and abs(prior_stop_price - atr_stop_price) > max(float(meta.tick_size or 0.0), 1e-12):
                stop_model = 'atr'
            elif abs(stop_price - structure_stop_price) > max(float(meta.tick_size or 0.0), 1e-12):
                stop_model = 'blended'
            risk_per_unit = abs(last_price - stop_price)
            if risk_per_unit <= 0:
                early_reject('invalid_atr_risk_per_unit')
                return None
            quantity = round_step(risk_usdt / risk_per_unit, meta.step_size, meta.quantity_precision)
            if max_notional_usdt > 0:
                max_qty_by_notional = round_step(max_notional_usdt / last_price, meta.step_size, meta.quantity_precision)
                quantity = min(quantity, max_qty_by_notional)
            if quantity < meta.min_qty or quantity <= 0:
                early_reject('atr_quantity_below_min_qty')
                return None
    stop_distance_pct = (risk_per_unit / last_price) * 100 if last_price else 0.0
    stop_too_tight_flag = bool(stop_distance_pct > 0 and stop_distance_pct < 0.08)
    stop_too_wide_flag = bool(stop_distance_pct > max(8.0, min(float(max_distance_from_vwap_pct or 0.0) * 1.5, 12.0)))

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

    if trade_side == TRADE_SIDE_LONG and last_price <= breakout_level and not near_breakout_setup:
        early_reject('long_breakout_not_confirmed')
        return None
    if trade_side == TRADE_SIDE_SHORT and last_price >= breakout_level and not near_breakout_setup:
        early_reject('short_breakdown_not_confirmed')
        return None
    if recent_5m_change_pct < effective_min_5m_change_pct and not near_breakout_setup:
        early_reject('recent_5m_change_below_gate')
        return None
    if quote_volume_24h < min_quote_volume:
        early_reject('quote_volume_below_gate')
        return None
    if not higher_tf_allowed and not near_breakout_setup:
        early_reject('higher_timeframe_not_allowed')
        return None
    if volume_multiple < min_volume_multiple and not near_breakout_setup:
        early_reject('volume_multiple_below_gate')
        return None
    if trade_side == TRADE_SIDE_LONG:
        if funding_rate is not None and funding_rate > funding_rate_threshold:
            early_reject('long_funding_rate_above_gate')
            return None
        if funding_rate_avg is not None and funding_rate_avg > funding_rate_avg_threshold:
            early_reject('long_funding_rate_avg_above_gate')
            return None
    else:
        if funding_rate is not None and funding_rate < (-funding_rate_threshold):
            early_reject('short_funding_rate_below_gate')
            return None
        if funding_rate_avg is not None and funding_rate_avg < (-funding_rate_avg_threshold):
            early_reject('short_funding_rate_avg_below_gate')
            return None
    if not structure_break and not near_breakout_setup:
        early_reject('micro_structure_break_not_confirmed')
        return None
    if trade_side == TRADE_SIDE_LONG and macd_5m['hist'] <= macd_5m['prev_hist'] and not near_breakout_setup:
        early_reject('long_macd_hist_not_accelerating')
        return None
    if trade_side == TRADE_SIDE_SHORT and macd_5m['hist'] >= macd_5m['prev_hist'] and not near_breakout_setup:
        early_reject('short_macd_hist_not_accelerating')
        return None
    if acceleration_ratio < effective_acceleration_threshold and not near_breakout_setup:
        early_reject('acceleration_ratio_below_gate')
        return None

    reasons: List[str] = []
    score = 0.0
    reasons.append(f'min_5m_change_gate={effective_min_5m_change_pct:.2f}')
    reasons.append(f'acceleration_ratio_gate={effective_acceleration_threshold:.2f}')
    if external_setup.get('enabled'):
        reasons.append('external_accumulation_setup_relaxed')
        reasons.append(f"external_setup_score={float(external_setup.get('score', 0.0) or 0.0):.1f}")
        reasons.append(f"external_max_breakout_distance_pct={float(external_setup.get('max_breakout_distance_pct', 0.0) or 0.0):.2f}")
    if near_configured_watch_setup:
        reasons.append('configured_near_breakout_watch')
        reasons.append(f'watch_breakout_tolerance_pct={watch_breakout_tolerance_pct:.2f}')
    if near_configured_setup:
        reasons.append('configured_near_breakout_setup')
        reasons.append(f'setup_breakout_tolerance_pct={setup_breakout_tolerance_pct:.2f}')
    if hot_rank is not None:
        score += max(0.0, 1 - ((hot_rank - 1) / 10)) * 40
        reasons.append(f'square_hot_rank={hot_rank}')
    directional_rank = loser_rank if trade_side == TRADE_SIDE_SHORT else gainer_rank
    directional_rank_label = 'loser_rank' if trade_side == TRADE_SIDE_SHORT else 'gainer_rank'
    if directional_rank is not None:
        score += max(0.0, 1 - ((directional_rank - 1) / 20)) * 60
        reasons.append(f'{directional_rank_label}={directional_rank}')
    if hot_rank is not None and directional_rank is not None:
        score += 20
        reasons.append('hot_directional_mover_intersection')
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
    reasons.append(f'stop_model={stop_model}')
    reasons.append(f'stop_distance_pct={stop_distance_pct:.2f}')
    if stop_too_tight_flag:
        score -= 8.0
        reasons.append('stop_too_tight_flag')
    if stop_too_wide_flag:
        score -= 10.0
        reasons.append('stop_too_wide_flag')

    sentiment_bonus_payload = compute_sentiment_resonance_bonus(okx_sentiment_score, okx_sentiment_acceleration, sector_resonance_score, smart_money_flow_score, side=trade_side)
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
    leading_payload = compute_leading_sentiment_signal(okx_sentiment_score, okx_sentiment_acceleration, side=trade_side)
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
        side=trade_side,
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
        side=trade_side,
    )
    score += squeeze_payload['score']
    reasons.extend(squeeze_payload['reasons'])

    control_risk_payload = compute_control_risk_score(
        short_bias=float(oi_features.get('short_bias', 0.0) or 0.0),
        oi_notional_percentile=float(oi_features.get('oi_notional_percentile', 0.0) or 0.0),
        smart_money_flow_score=smart_money_effective,
        side=trade_side,
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
    trigger_confirmation = evaluate_trigger_confirmation(
        structure_break=structure_break,
        price_above_vwap=price_above_vwap,
        distance_from_ema20_5m_pct=distance_from_ema20_5m_pct,
        distance_from_vwap_15m_pct=distance_from_vwap_15m_pct,
        taker_buy_ratio=oi_features.get('taker_buy_ratio'),
        oi_change_pct_5m=oi_features.get('oi_change_pct_5m', 0.0),
        oi_change_pct_15m=oi_features.get('oi_change_pct_15m', 0.0),
        funding_rate=funding_rate,
        funding_rate_threshold=funding_rate_threshold,
        funding_rate_avg=funding_rate_avg,
        funding_rate_avg_threshold=funding_rate_avg_threshold,
        cvd_delta=oi_features.get('cvd_delta', 0.0),
        cvd_zscore=oi_features.get('cvd_zscore', 0.0),
        state=state_payload['state'],
        overextension_flag=overextension_flag,
        side=trade_side,
        min_confirmations=2,
        long_short_ratio=microstructure_inputs.get('long_short_ratio'),
        price_change_pct_24h=price_change_pct_24h,
        recent_5m_change_pct=recent_5m_change_pct,
    )
    setup_ready = bool(trigger_confirmation['setup_ready'])
    trigger_fired = bool(trigger_confirmation['trigger_fired'])
    waiting_breakout = bool(
        near_breakout_setup
        and (
            (trade_side == TRADE_SIDE_LONG and last_price <= breakout_level)
            or (trade_side == TRADE_SIDE_SHORT and last_price >= breakout_level)
            or not structure_break
        )
    )
    if waiting_breakout:
        trigger_fired = False
        trigger_confirmation['trigger_fired'] = False
        trigger_confirmation['flags']['waiting_breakout'] = True
        if not (near_external_breakout_setup or near_configured_setup):
            setup_ready = False
            trigger_confirmation['setup_ready'] = False
            trigger_confirmation['flags']['watch_only_breakout_distance'] = True
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
        stop_model=stop_model,
        stop_distance_pct=stop_distance_pct,
        stop_too_tight_flag=stop_too_tight_flag,
        stop_too_wide_flag=stop_too_wide_flag,
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
        loser_rank=loser_rank,
        trigger_confirmation_flags=dict(trigger_confirmation['flags']),
        trigger_confirmation_count=int(trigger_confirmation['confirmation_count']),
        trigger_min_confirmations=int(trigger_confirmation['min_confirmations']),
        oi_hard_reversal_threshold_pct=float(legacy_kwargs.get('oi_hard_reversal_threshold_pct', 0.8) or 0.8),
        portfolio_narrative_bucket='',
        portfolio_correlation_group='',
    )
    candidate.must_pass_flags = {
        **dict(candidate.must_pass_flags or {}),
        **dict(trigger_confirmation['flags']),
        'setup_ready': setup_ready,
        'trigger_fired': trigger_fired,
    }
    candidate.reasons.append(f"trigger_confirmation_count={candidate.trigger_confirmation_count}")
    candidate.reasons.append(f"trigger_min_confirmations={candidate.trigger_min_confirmations}")
    if waiting_breakout:
        candidate.reasons.append('waiting_breakout')
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
    append_rate_limited_runtime_event(store, 'book_ticker_cache_miss', {
        'event_source': 'book_ticker_cache',
        'symbol': symbol,
        'requested_sample_count': max(int(sample_count or 0), 0),
        'cache_max_age_seconds': float(cache_max_age_seconds),
        'fallback': 'rest_polling',
    }, key='global', min_interval_seconds=60.0)
    samples: List[Dict[str, Any]] = []
    sample_total = max(int(sample_count or 0), 0)
    for idx in range(sample_total):
        payload = client.get('/fapi/v1/ticker/bookTicker', params={'symbol': symbol})
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
    return client.get('/futures/data/topLongShortAccountRatio', params={'symbol': symbol, 'period': period, 'limit': limit})


def merged_candidate_symbols(**kwargs) -> Tuple[List[str], Dict[str, int], Dict[str, int], Dict[str, int]]:
    square_symbols = kwargs.get('square_symbols', [])
    tickers = kwargs.get('tickers', [])
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
    if candidate.price_change_pct_24h >= 15.0 and candidate.state in {'chase', 'momentum_extension', 'overheated'}:
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


def derive_directional_score_multiplier(side: str, regime_label: str, base_multiplier: float) -> float:
    normalized_side = normalize_position_side(side)
    normalized_regime = str(regime_label or 'neutral').strip().lower()
    base = max(float(base_multiplier or 1.0), 0.0)
    if normalized_regime == 'risk_off' and normalized_side == POSITION_SIDE_SHORT:
        return max(base, 1.0)
    if normalized_regime == 'risk_on' and normalized_side == POSITION_SIDE_SHORT:
        return min(base, 1.0)
    return base


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
        'candle_extension_pct': round(float(candidate.candle_extension_pct or 0.0), 4),
        'recent_3bar_runup_pct': round(float(candidate.recent_3bar_runup_pct or 0.0), 4),
        'overextension_flag': candidate.overextension_flag,
        'entry_pattern': candidate.entry_pattern,
        'trend_regime': candidate.trend_regime,
        'liquidity_grade': candidate.liquidity_grade,
        'setup_ready': bool(candidate.setup_ready),
        'trigger_fired': bool(candidate.trigger_fired),
        'candidate_stage': candidate.candidate_stage,
        'setup_missing': list(candidate.setup_missing or []),
        'trigger_missing': list(candidate.trigger_missing or []),
        'trade_missing': list(candidate.trade_missing or []),
        'trigger_confirmation_flags': dict(candidate.trigger_confirmation_flags or {}),
        'trigger_confirmation_count': int(candidate.trigger_confirmation_count or 0),
        'trigger_min_confirmations': int(candidate.trigger_min_confirmations or 0),
        'portfolio_narrative_bucket': candidate.portfolio_narrative_bucket,
        'portfolio_correlation_group': candidate.portfolio_correlation_group,
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


def run_scan_once(client: Optional[BinanceFuturesClient], args: argparse.Namespace, explicit_square_symbols: Optional[Sequence[str]] = None):
    store = get_runtime_state_store(args)
    okx_simulated_trading = bool(getattr(args, 'okx_simulated_trading', False))
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
    merged_payload = merged_candidate_symbols(
        square_symbols=square_symbols,
        tickers=tickers,
        top_gainers=getattr(args, 'top_gainers', 20),
        top_losers=getattr(args, 'top_losers', getattr(args, 'top_gainers', 20)),
    )
    if len(merged_payload) == 4:
        merged_symbols, hot_rank_map, gainer_rank_map, loser_rank_map = merged_payload
    else:
        merged_symbols, hot_rank_map, gainer_rank_map = merged_payload
        loser_rank_map = {}
    raw_merged_symbol_count = len(merged_symbols)
    okx_unavailable_symbols: List[str] = []
    okx_available_inst_count = 0
    if okx_simulated_trading:
        okx_skip_symbols = load_okx_sim_skip_symbols(store)
        try:
            okx_filter_client = OKXClient(
                base_url=getattr(args, 'okx_base_url', 'https://www.okx.com'),
                simulated_trading=True,
            )
            okx_inst_ids = fetch_okx_swap_inst_ids(okx_filter_client)
            okx_available_inst_count = len(okx_inst_ids)
            filtered_symbols = []
            for symbol in merged_symbols:
                normalized_symbol = normalize_symbol(symbol)
                if normalized_symbol in okx_skip_symbols:
                    okx_unavailable_symbols.append(symbol)
                elif normalize_okx_swap_inst_id(symbol) in okx_inst_ids:
                    filtered_symbols.append(symbol)
                else:
                    okx_unavailable_symbols.append(symbol)
            merged_symbols = filtered_symbols
        except Exception as exc:
            okx_unavailable_symbols = [f'okx_instrument_filter_failed:{exc}']
            if okx_skip_symbols:
                filtered_symbols = []
                for symbol in merged_symbols:
                    if normalize_symbol(symbol) in okx_skip_symbols:
                        okx_unavailable_symbols.append(symbol)
                    else:
                        filtered_symbols.append(symbol)
                merged_symbols = filtered_symbols
    ticker_map = {row['symbol']: row for row in tickers}

    regime_payload = compute_market_regime_filter(
        btc_klines=fetch_klines(client, 'BTCUSDT', '15m', 30) if client else None,
        sol_klines=fetch_klines(client, 'SOLUSDT', '15m', 30) if client else None,
    )

    rejected_events: List[Dict[str, Any]] = []
    early_reject_stats: Dict[str, Any] = {'total': 0, 'by_reason': {}, 'by_side': {}}
    candidates: List[Candidate] = []
    built_candidates: List[Candidate] = []
    candidate_alerts: List[Dict[str, Any]] = []
    max_candidates = int(getattr(args, 'max_candidates', 8) or 8)
    evaluated_symbols = merged_symbols[: max(max_candidates * 2, max_candidates)]
    evaluated_side_count = 0
    for symbol in evaluated_symbols:
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
                execution_slippage_hard_veto_r=float(getattr(args, 'execution_slippage_hard_veto_r', 0.25) or 0.25),
                execution_slippage_risk_threshold_r=float(getattr(args, 'execution_slippage_risk_threshold_r', 0.15) or 0.15),
                side=candidate_side,
                early_reject_stats=early_reject_stats,
                **micro,
            )
            if candidate is None:
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
            candidate.regime_label = regime_label
            candidate.regime_multiplier = regime_multiplier
            candidate.side_risk_multiplier = side_multiplier
            candidate.reasons.append(f'market_regime_multiplier={regime_multiplier:.2f}')
            candidate.reasons.append(f'directional_score_multiplier={directional_score_multiplier:.2f}')
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
            apply_candidate_diagnostics(candidate)
            candidates.append(candidate)

    candidates.sort(key=lambda item: item.score, reverse=True)
    candidate_alerts = [build_standardized_alert(item, regime_payload) for item in candidates]
    execution_priority = sorted(
        candidates,
        key=lambda item: (
            2 if bool(getattr(item, 'trigger_fired', False)) else (1 if bool(getattr(item, 'setup_ready', False)) else 0),
            float(getattr(item, 'score', 0.0) or 0.0),
        ),
        reverse=True,
    )
    best = execution_priority[0] if execution_priority else None
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
    funnel = {
        'raw_scan_symbol_count': raw_merged_symbol_count,
        'evaluated_symbol_count': len(evaluated_symbols),
        'evaluated_side_count': evaluated_side_count,
        'okx_available_inst_count': okx_available_inst_count,
        'okx_unavailable_symbol_count': len(okx_unavailable_symbols),
        'okx_unavailable_symbols_sample': okx_unavailable_symbols[:12],
        'early_filter_passed_count': len(built_candidates),
        'candidate_pool_count': len(candidates),
        'setup_ready_count': sum(1 for item in built_candidates if bool(item.setup_ready)),
        'trigger_fired_count': sum(1 for item in built_candidates if bool(item.trigger_fired)),
        'hard_rejected_count': len(rejected_events),
        'stage_counts': stage_counts,
        'top_setup_missing': dict(sorted(setup_missing_counts.items(), key=lambda item: item[1], reverse=True)[:8]),
        'top_trigger_missing': dict(sorted(trigger_missing_counts.items(), key=lambda item: item[1], reverse=True)[:8]),
        'top_trade_missing': dict(sorted(trade_missing_counts.items(), key=lambda item: item[1], reverse=True)[:8]),
    }
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
            'triggered_but_risk_rejected': triggered_but_risk_rejected,
        },
        'early_rejected_stats': early_reject_stats,
        'funnel': funnel,
    }
    return payload, best, metas


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', default='')
    parser.add_argument('--square-symbols', default='')
    parser.add_argument('--square-symbols-file', default='')
    parser.add_argument('--use-square-page', action='store_true')
    parser.add_argument('--top-gainers', type=int, default=20)
    parser.add_argument('--top-losers', type=int, default=20)
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
    parser.add_argument('--watch-breakout-tolerance-pct', type=float, default=0.0, help='Allow near-breakout watch candidates within this percent into candidate scoring.')
    parser.add_argument('--setup-breakout-tolerance-pct', type=float, default=0.0, help='Treat near-breakout candidates within this percent as eligible for setup readiness; execution still requires trigger confirmation.')
    parser.add_argument('--oi-hard-reversal-threshold-pct', type=float, default=0.8, help='Directional 5m OI reversal threshold that remains a hard veto.')
    parser.add_argument('--sim-probe-entry-enabled', action='store_true', help='Allow OKX simulated trading to submit a small probe when setup is ready but full trigger has not fired.')
    parser.add_argument('--sim-probe-size-ratio', type=float, default=0.2)
    parser.add_argument('--sim-probe-min-score', type=float, default=62.0)
    parser.add_argument('--sim-probe-max-breakout-distance-pct', type=float, default=0.35)
    parser.add_argument('--execution-slippage-hard-veto-r', type=float, default=0.25)
    parser.add_argument('--execution-slippage-risk-threshold-r', type=float, default=0.15)
    parser.add_argument('--max-funding-rate', type=float, default=0.0005)
    parser.add_argument('--max-funding-rate-avg', type=float, default=0.0003)
    parser.add_argument('--leverage', type=int, default=5)
    parser.add_argument('--margin-type', choices=['ISOLATED', 'CROSSED', 'isolated', 'crossed'], default='ISOLATED')
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--scan-only', action='store_true')
    parser.add_argument('--profile', default='default')
    parser.add_argument('--tp1-r', type=float, default=1.5)
    parser.add_argument('--tp1-close-pct', type=float, default=0.3)
    parser.add_argument('--tp2-r', type=float, default=2.0)
    parser.add_argument('--tp2-close-pct', type=float, default=0.4)
    parser.add_argument('--breakeven-r', type=float, default=1.0)
    parser.add_argument('--breakeven-confirmation-mode', choices=['price_only', 'ema_support'], default='ema_support')
    parser.add_argument('--breakeven-min-buffer-pct', type=float, default=0.001)
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
    parser.add_argument('--okx-simulated-trading', action='store_true', help='Execute selected live candidate on OKX simulated trading instead of Binance live trading.')
    parser.add_argument('--okx-base-url', default=os.getenv('OKX_BASE_URL', 'https://www.okx.com'))
    parser.add_argument('--binance-simulated-trading', action='store_true', help='Use Binance USDT-M Futures Testnet / Mock Trading instead of Binance production futures.')
    parser.add_argument('--external-signal-json', default='')
    parser.add_argument('--use-external-setup-relaxation', action='store_true', help='Let strong external accumulation signals relax early scan gates; live entries still require setup/trigger risk guards.')
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
    parser.add_argument('--gross-heat-cap-r', type=float, default=0.0, help='Maximum total portfolio heat in R units (0 disables).')
    parser.add_argument('--same-theme-heat-cap-r', type=float, default=0.0, help='Maximum heat in R units for the same narrative/theme bucket (0 disables).')
    parser.add_argument('--same-correlation-heat-cap-r', type=float, default=0.0, help='Maximum heat in R units for the same correlation bucket (0 disables).')
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
            'max_notional_usdt': 500.0,
            'leverage': 5,
            'breakeven_r': 0.8,
            'tp1_r': 1.2,
            'tp1_close_pct': 0.5,
            'tp2_r': 1.8,
            'tp2_close_pct': 0.3,
            'min_quote_volume': 20_000_000,
            'top_gainers': 12,
            'top_losers': 12,
            'max_candidates': 5,
            'max_rsi_5m': 76.0,
            'min_volume_multiple': 2.4,
            'min_5m_change_pct': 2.5,
            'max_distance_from_ema_pct': 6.0,
            'max_distance_from_vwap_pct': 5.0,
            'max_funding_rate': 0.0004,
            'max_funding_rate_avg': 0.00025,
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
            'lookback_bars': 6,
            'swing_bars': 5,
            'min_quote_volume': 10_000_000,
            'top_gainers': 25,
            'top_losers': 25,
            'max_candidates': 10,
            'max_rsi_5m': 82.0,
            'min_volume_multiple': 1.25,
            'min_5m_change_pct': 0.8,
            'max_distance_from_ema_pct': 7.0,
            'max_distance_from_vwap_pct': 6.0,
            'max_funding_rate': 0.0008,
            'max_funding_rate_avg': 0.0005,
        }
    elif profile in {'okx-sim-active', 'binance-sim-active'}:
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
        if profile == 'okx-sim-active':
            profile_overrides['okx_simulated_trading'] = True
        else:
            profile_overrides.update({
                'binance_simulated_trading': True,
                'base_url': 'https://testnet.binancefuture.com',
            })
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
        'portfolio_heat_open_r': 0.0,
        'portfolio_heat_pending_r': 0.0,
        'portfolio_heat_r_by_theme': {},
        'portfolio_heat_r_by_correlation': {},
    }


def load_risk_state(store: RuntimeStateStore) -> Dict[str, Any]:
    state = store.load_json('risk_state', default_risk_state())
    if not isinstance(state, dict):
        return default_risk_state()
    merged = default_risk_state()
    merged.update(state)
    if not isinstance(merged.get('symbol_cooldowns'), dict):
        merged['symbol_cooldowns'] = {}
    if not isinstance(merged.get('portfolio_exposure_pct_by_theme'), dict):
        merged['portfolio_exposure_pct_by_theme'] = {}
    if not isinstance(merged.get('portfolio_exposure_pct_by_correlation'), dict):
        merged['portfolio_exposure_pct_by_correlation'] = {}
    if not isinstance(merged.get('portfolio_heat_r_by_theme'), dict):
        merged['portfolio_heat_r_by_theme'] = {}
    if not isinstance(merged.get('portfolio_heat_r_by_correlation'), dict):
        merged['portfolio_heat_r_by_correlation'] = {}
    heat_snapshot = compute_positions_heat_snapshot(store.load_json('positions', {}))
    if int(heat_snapshot.get('tracked_positions', 0) or 0) > 0:
        merged['portfolio_heat_open_r'] = heat_snapshot.get('open_heat_r', 0.0)
        if heat_snapshot.get('heat_r_by_theme'):
            merged['portfolio_heat_r_by_theme'] = heat_snapshot['heat_r_by_theme']
        if heat_snapshot.get('heat_r_by_correlation'):
            merged['portfolio_heat_r_by_correlation'] = heat_snapshot['heat_r_by_correlation']
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
    position_side = normalize_position_side(side)
    positions = fetch_open_positions(client)
    active = next((row for row in positions if position_row_matches_symbol_side(row, symbol, position_side)), None)
    expected_order_id = expected_stop_order.get('orderId') if isinstance(expected_stop_order, dict) else None
    expected_client_algo_id = expected_stop_order.get('clientAlgoId') if isinstance(expected_stop_order, dict) else None
    if active is None:
        return {'status': 'flat', 'active_position': None, 'expected_order_id': expected_order_id, 'side': position_side}
    open_orders = fetch_open_orders(client, symbol)
    matched = next((row for row in open_orders if row.get('orderId') == expected_order_id), None) if expected_order_id is not None else (open_orders[0] if open_orders else None)
    if matched is None:
        open_algo_orders = fetch_open_algo_orders(client, symbol)
        if expected_client_algo_id:
            matched = next((row for row in open_algo_orders if row.get('clientAlgoId') == expected_client_algo_id), None)
        elif open_algo_orders:
            matched = next((row for row in open_algo_orders if str(row.get('orderType') or '').upper() == 'STOP_MARKET'), open_algo_orders[0])
    if matched is None:
        return {'status': 'missing', 'active_position': active, 'expected_order_id': expected_order_id, 'expected_client_algo_id': expected_client_algo_id, 'side': position_side}
    return {'status': 'protected', 'active_position': active, 'expected_order_id': expected_order_id, 'expected_client_algo_id': expected_client_algo_id, 'stop_order': matched, 'side': position_side}

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
        meta = fetch_exchange_meta(client).get(symbol)
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
        build_position_key(row.get('symbol'), position_side_from_exchange_position(row)): row
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
        tracked.update(exchange_position_runtime_fields(exchange_row))
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
        side = position_side_from_exchange_position(row)
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
    if not isinstance(normalized.get('portfolio_heat_r_by_theme'), dict):
        normalized['portfolio_heat_r_by_theme'] = {}
    if not isinstance(normalized.get('portfolio_heat_r_by_correlation'), dict):
        normalized['portfolio_heat_r_by_correlation'] = {}
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
        if not bool(getattr(candidate, 'setup_ready', False)):
            reasons.append('candidate_setup_not_ready')
        elif not bool(getattr(candidate, 'trigger_fired', False)):
            reasons.append('candidate_trigger_not_fired')
        execution_slippage_r = compute_expected_slippage_r(candidate)
        execution_liquidity_grade = classify_execution_liquidity_grade(getattr(candidate, 'book_depth_fill_ratio', 0.0), execution_slippage_r)
        if state == 'distribution':
            reasons.append('candidate_distribution_risk')
        if _to_float(getattr(candidate, 'cvd_delta', 0.0)) < 0 and _to_float(getattr(candidate, 'cvd_zscore', 0.0)) <= -2.0:
            reasons.append('candidate_cvd_divergence')
        oi_hard_reversal_threshold = abs(_to_float(getattr(candidate, 'oi_hard_reversal_threshold_pct', 0.8), default=0.8))
        if _to_float(getattr(candidate, 'oi_change_pct_5m', 0.0)) <= -oi_hard_reversal_threshold:
            reasons.append('candidate_oi_reversal')
        risk_slippage_r = max(_to_float(getattr(candidate, 'execution_slippage_risk_threshold_r', 0.15), default=0.15), 0.0)
        if execution_slippage_r > risk_slippage_r:
            reasons.append('candidate_execution_slippage_risk')
        if execution_liquidity_grade == 'C' and _to_float(getattr(candidate, 'book_depth_fill_ratio', 0.0)) < 0.5:
            reasons.append('candidate_execution_liquidity_poor')
        position_size_pct = max(_to_float(getattr(candidate, 'position_size_pct', 0.0)), 0.0)
        portfolio_narrative_bucket = str(kwargs.get('portfolio_narrative_bucket') or getattr(candidate, 'portfolio_narrative_bucket', '') or '').strip()
        portfolio_correlation_group = str(kwargs.get('portfolio_correlation_group') or getattr(candidate, 'portfolio_correlation_group', '') or '').strip()
        max_theme = max(_to_float(kwargs.get('max_portfolio_exposure_pct_per_theme', 0.0)), 0.0)
        max_corr = max(_to_float(kwargs.get('max_portfolio_exposure_pct_per_correlation_group', 0.0)), 0.0)
        base_risk_usdt = max(_to_float(kwargs.get('base_risk_usdt', 0.0)), 0.0)
        candidate_heat_r = estimate_candidate_heat_r(candidate, base_risk_usdt=base_risk_usdt)
        current_open_heat_r = max(_to_float(normalized.get('portfolio_heat_open_r', 0.0)), 0.0)
        current_pending_heat_r = max(_to_float(normalized.get('portfolio_heat_pending_r', 0.0)), 0.0)
        gross_heat_cap_r = max(_to_float(kwargs.get('gross_heat_cap_r', 0.0)), 0.0)
        same_theme_heat_cap_r = max(_to_float(kwargs.get('same_theme_heat_cap_r', 0.0)), 0.0)
        same_correlation_heat_cap_r = max(_to_float(kwargs.get('same_correlation_heat_cap_r', 0.0)), 0.0)
        if gross_heat_cap_r > 0 and (current_open_heat_r + current_pending_heat_r + candidate_heat_r) >= gross_heat_cap_r:
            reasons.append('candidate_portfolio_heat_overexposure')
        if portfolio_narrative_bucket and max_theme > 0 and position_size_pct > 0:
            current_theme = _to_float(normalized['portfolio_exposure_pct_by_theme'].get(portfolio_narrative_bucket))
            if current_theme + position_size_pct >= max_theme:
                reasons.append('candidate_portfolio_theme_overexposure')
        if portfolio_correlation_group and max_corr > 0 and position_size_pct > 0:
            current_corr = _to_float(normalized['portfolio_exposure_pct_by_correlation'].get(portfolio_correlation_group))
            if current_corr + position_size_pct >= max_corr:
                reasons.append('candidate_portfolio_correlation_overexposure')
        if portfolio_narrative_bucket and same_theme_heat_cap_r > 0 and candidate_heat_r > 0:
            current_theme_heat = _to_float(normalized['portfolio_heat_r_by_theme'].get(portfolio_narrative_bucket))
            if current_theme_heat + candidate_heat_r >= same_theme_heat_cap_r:
                reasons.append('candidate_same_theme_heat_overexposure')
        if portfolio_correlation_group and same_correlation_heat_cap_r > 0 and candidate_heat_r > 0:
            current_corr_heat = _to_float(normalized['portfolio_heat_r_by_correlation'].get(portfolio_correlation_group))
            if current_corr_heat + candidate_heat_r >= same_correlation_heat_cap_r:
                reasons.append('candidate_same_correlation_heat_overexposure')
    return {'allowed': not reasons, 'reasons': reasons, 'cooldown_until': cooldown_until, 'normalized_risk_state': normalized}


SIM_PROBE_ALLOWED_RISK_REASONS = {'candidate_trigger_not_fired'}
SIM_PROBE_HARD_RISK_REASONS = {
    'candidate_setup_not_ready',
    'candidate_distribution_risk',
    'candidate_cvd_divergence',
    'candidate_oi_reversal',
    'candidate_execution_slippage_risk',
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
    return replace(
        candidate,
        quantity=max(float(candidate.quantity or 0.0) * ratio, 0.0),
        position_size_pct=round(float(candidate.position_size_pct or 0.0) * ratio, 4),
        reasons=list(candidate.reasons or []) + [f'sim_probe_size_ratio={ratio:.2f}'],
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


def recover_unknown_entry_order(client: Any, candidate: Candidate, quantity: float, quantity_precision: int) -> Dict[str, Any]:
    position_side = normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG))
    submit_side = 'SELL' if position_side == POSITION_SIDE_SHORT else 'BUY'
    params = {
        'symbol': candidate.symbol,
        'side': submit_side,
        'type': 'MARKET',
        'quantity': format_decimal(quantity, quantity_precision),
        'newOrderRespType': 'RESULT',
    }
    if should_send_position_side(client):
        params['positionSide'] = position_side
    try:
        response = client.signed_post('/fapi/v1/order', params)
    except Exception as retry_exc:
        if not should_send_position_side(client) or not is_position_side_mode_error(retry_exc):
            raise BinanceAPIError(f'entry order status remained unknown after timeout recovery attempt: {retry_exc}') from retry_exc
        mark_one_way_position_mode(client)
        params.pop('positionSide', None)
        try:
            response = client.signed_post('/fapi/v1/order', params)
        except Exception as one_way_retry_exc:
            raise BinanceAPIError(f'entry order status remained unknown after timeout recovery attempt: {one_way_retry_exc}') from one_way_retry_exc
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


def ensure_symbol_margin_type(client: Any, symbol: str, margin_type: str = 'ISOLATED') -> Dict[str, Any]:
    normalized_margin_type = str(margin_type or 'ISOLATED').strip().upper()
    if normalized_margin_type not in {'ISOLATED', 'CROSSED'}:
        normalized_margin_type = 'ISOLATED'
    try:
        response = client.signed_post('/fapi/v1/marginType', {
            'symbol': str(symbol or '').upper(),
            'marginType': normalized_margin_type,
        })
        return {
            'ok': True,
            'requested': normalized_margin_type,
            'actual': normalized_margin_type,
            'response': response if isinstance(response, dict) else {},
            'already_set': False,
        }
    except BinanceAPIError as exc:
        message = str(exc)
        if '-4046' in message or 'No need to change margin type' in message:
            return {
                'ok': True,
                'requested': normalized_margin_type,
                'actual': normalized_margin_type,
                'response': {'message': message},
                'already_set': True,
            }
        raise


def place_live_trade(client: Any, candidate: Candidate, leverage: int, meta: SymbolMeta, args: argparse.Namespace) -> Dict[str, Any]:
    position_side = normalize_position_side(getattr(candidate, 'side', POSITION_SIDE_LONG))
    position_key = build_position_key(candidate.symbol, position_side)
    profile = getattr(args, 'profile', 'default')
    requested_margin_type = str(getattr(args, 'margin_type', 'ISOLATED') or 'ISOLATED').strip().upper()
    requested_leverage = int(leverage)

    open_positions = fetch_open_positions(client)
    has_existing_position = any(
        isinstance(row, dict)
        and position_row_matches_symbol_side(row, candidate.symbol, position_side)
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
    open_algo_orders = fetch_open_algo_orders(client, candidate.symbol)
    has_existing_open_orders = any(
        isinstance(row, dict)
        and str(row.get('symbol', '')).upper() == candidate.symbol.upper()
        for row in list(open_orders or []) + list(open_algo_orders or [])
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
            'open_algo_order_ids': [row.get('algoId') or row.get('clientAlgoId') for row in list(open_algo_orders or []) if isinstance(row, dict)],
        }
        log_runtime_event('error', error_payload)
        emit_notification(args, 'error', error_payload)
        raise BinanceAPIError('preflight hard gate: existing_open_orders')

    margin_type_check = ensure_symbol_margin_type(client, candidate.symbol, requested_margin_type)
    leverage_response = client.signed_post('/fapi/v1/leverage', {'symbol': candidate.symbol, 'leverage': requested_leverage})
    actual_leverage = int(_to_float(leverage_response.get('leverage'), default=requested_leverage)) if isinstance(leverage_response, dict) else requested_leverage
    if actual_leverage != requested_leverage:
        error_payload = {
            'symbol': candidate.symbol,
            'side': position_side,
            'position_key': position_key,
            'profile': profile,
            'preflight_reason': 'leverage_mismatch',
            'requested_leverage': requested_leverage,
            'actual_leverage': actual_leverage,
            'message': f'preflight hard gate: leverage_mismatch requested={requested_leverage} actual={actual_leverage}',
        }
        log_runtime_event('error', error_payload)
        emit_notification(args, 'error', error_payload)
        raise BinanceAPIError(error_payload['message'])

    execution_quality = compute_execution_quality_size_adjustment(candidate)
    step_size = float(getattr(meta, 'step_size', 0.0) or 0.0)
    quantity_precision = int(getattr(meta, 'quantity_precision', 0) or 0)
    min_qty = float(getattr(meta, 'min_qty', 0.0) or 0.0)
    base_quantity = round_step(candidate.quantity, step_size, quantity_precision)
    scaled_quantity = round_step(base_quantity * float(execution_quality['size_multiplier']), step_size, quantity_precision)
    quantity = scaled_quantity if scaled_quantity >= min_qty else scaled_quantity
    entry_order_error: Optional[Exception] = None
    entry_position_mode = 'HEDGE' if should_send_position_side(client) else 'ONE_WAY'
    entry_params = {
        'symbol': candidate.symbol,
        'side': 'SELL' if position_side == POSITION_SIDE_SHORT else 'BUY',
        'type': 'MARKET',
        'quantity': format_decimal(quantity, quantity_precision),
        'newOrderRespType': 'RESULT',
    }
    if should_send_position_side(client):
        entry_params['positionSide'] = position_side
    try:
        entry_order = client.signed_post('/fapi/v1/order', entry_params)
    except Exception as exc:
        entry_order_error = exc
        error_message = str(exc)
        if is_position_side_mode_error(exc) and should_send_position_side(client):
            mark_one_way_position_mode(client)
            entry_params.pop('positionSide', None)
            entry_order = client.signed_post('/fapi/v1/order', entry_params)
            entry_position_mode = 'ONE_WAY'
        elif '-1007' in error_message and 'unknown' in error_message.lower():
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
    filled_quantity = _to_float(entry_order.get('executedQty'), default=0.0)
    if filled_quantity <= 0 or str(entry_order.get('status') or '').upper() not in {'FILLED', 'PARTIALLY_FILLED'}:
        order_id = entry_order.get('orderId') if isinstance(entry_order, dict) else None
        client_order_id = entry_order.get('clientOrderId') if isinstance(entry_order, dict) else None
        for _ in range(3):
            if order_id is None and not client_order_id:
                break
            time.sleep(0.4)
            try:
                confirmed_entry = query_order(client, candidate.symbol, order_id=order_id, client_order_id=client_order_id)
            except Exception:
                continue
            confirmed_qty = _to_float(confirmed_entry.get('executedQty'), default=0.0)
            if confirmed_qty > 0:
                entry_order = confirmed_entry
                entry_price = _to_float(entry_order.get('avgPrice')) or _to_float(entry_order.get('price')) or entry_price
                filled_quantity = confirmed_qty
                break
        if filled_quantity <= 0:
            active_position = next((row for row in fetch_open_positions(client) if position_row_matches_symbol_side(row, candidate.symbol, position_side)), None)
            if isinstance(active_position, dict):
                filled_quantity = abs(_to_float(active_position.get('positionAmt')))
                entry_price = _to_float(active_position.get('entryPrice')) or entry_price
    if filled_quantity <= 0:
        raise BinanceAPIError(f'entry order not filled yet; stop placement skipped for {candidate.symbol}')
    entry_order_feedback = {
        'order_id': entry_order.get('orderId'),
        'client_order_id': entry_order.get('clientOrderId'),
        'status': entry_order.get('status'),
        'avg_price': entry_price,
        'executed_qty': filled_quantity,
        'cum_quote': _to_float(entry_order.get('cumQuote')),
        'update_time': entry_order.get('updateTime'),
        'position_mode': entry_position_mode,
        'recovered_from_unknown_timeout': bool(entry_order_error is not None and '-1007' in str(entry_order_error)),
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
        breakeven_confirmation_mode=str(getattr(args, 'breakeven_confirmation_mode', 'ema_support') or 'ema_support'),
        breakeven_min_buffer_pct=float(getattr(args, 'breakeven_min_buffer_pct', 0.001) or 0.0),
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
        'margin_type': requested_margin_type,
        'margin_type_check': margin_type_check,
        'leverage': requested_leverage,
        'leverage_check': {
            'requested': requested_leverage,
            'actual': actual_leverage,
            'response': leverage_response if isinstance(leverage_response, dict) else {},
        },
        'position_mode': entry_position_mode,
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
        'position_mode': 'ONE_WAY' if not should_send_position_side(client) else entry_position_mode,
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
        'position_mode': 'ONE_WAY' if not should_send_position_side(client) else entry_position_mode,
        'margin_type': requested_margin_type,
        'margin_type_check': margin_type_check,
        'leverage': requested_leverage,
        'leverage_check': {
            'requested': requested_leverage,
            'actual': actual_leverage,
            'response': leverage_response if isinstance(leverage_response, dict) else {},
        },
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
        opened_at=str(tracked.get('opened_at') or _isoformat_utc(_utc_now())),
        first_1r_at=str(tracked.get('first_1r_at') or '') or None,
        realized_r=_to_float(tracked.get('realized_r'), default=0.0),
    )
    selection_context = dict(tracked)

    def persist_position(
        status: str,
        protection_status: Optional[str],
        active_stop_order: Optional[Dict[str, Any]],
        exit_reason: Optional[str] = None,
        closed_at: Optional[datetime.datetime] = None,
    ) -> Dict[str, Any]:
        position_payload = dict(tracked)
        analytics_snapshot = build_trade_analytics_snapshot(state, plan, closed_at=closed_at)
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
            'lowest_price_seen': round(state.lowest_price_seen, 10) if state.lowest_price_seen is not None else None,
            'opened_at': state.opened_at,
            'first_1r_at': state.first_1r_at,
            'realized_r': analytics_snapshot['realized_r'],
            'mfe_r': analytics_snapshot['mfe_r'],
            'mae_r': analytics_snapshot['mae_r'],
            'time_to_1r': analytics_snapshot['time_to_1r'],
            'time_to_1r_minutes': analytics_snapshot['time_to_1r_minutes'],
            'time_in_trade_minutes': analytics_snapshot['time_in_trade_minutes'],
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
        selection_context.update(persisted_payload)
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
    book_ticker_cache_max_age_seconds = max(float(getattr(args, 'monitor_poll_interval_sec', 2) or 2), 3.0)
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
        current_price_source = 'debug_override' if current_price is not None else ''
        current_price_snapshot = None
        if current_price is None:
            fallback_price = closes[-1] if closes else entry_price
            price_resolution = resolve_monitor_current_price(
                store,
                symbol,
                state.position_side,
                fallback_price=fallback_price,
                cache_max_age_seconds=book_ticker_cache_max_age_seconds,
            )
            current_price = price_resolution['price']
            current_price_source = str(price_resolution.get('source') or 'kline_close_fallback')
            current_price_snapshot = price_resolution.get('snapshot')
        ema5m = tracked.get('_debug_ema5m')
        if ema5m is None:
            ema5m = closes[-1] if closes else current_price
        trailing_reference = tracked.get('_debug_trailing_reference')
        if trailing_reference is None:
            if state.position_side == POSITION_SIDE_SHORT:
                trailing_reference = min(lows) if lows else min(state.lowest_price_seen or current_price, current_price)
            else:
                trailing_reference = max(highs) if highs else max(state.highest_price_seen or current_price, current_price)
        actions = evaluate_management_actions(
            state,
            plan,
            current_price=current_price,
            ema5m=ema5m,
            trailing_reference=trailing_reference,
            trailing_buffer_pct=trailing_buffer_pct,
            allow_runner_exit=True,
        )
        update_trade_progress_metrics(state, plan, current_price=current_price, observed_at=_utc_now())
        debug_payload = {
            'symbol': symbol,
            'position_side': state.position_side,
            'current_price': current_price,
            'current_price_source': current_price_source or 'kline_close_fallback',
            'current_price_cache_max_age_seconds': book_ticker_cache_max_age_seconds,
            'book_ticker_snapshot': current_price_snapshot,
            'ema5m': ema5m,
            'trailing_reference': trailing_reference,
            'actions': actions,
            'remaining_quantity': state.remaining_quantity,
            'tracked': tracked,
            'max_cycles': max_cycles,
        }
        store.save_json('monitor_debug', debug_payload)
        if not actions:
            persist_position(status='monitoring', protection_status=protection_status, active_stop_order=active_stop_order)
            time.sleep(float(getattr(args, 'monitor_poll_interval_sec', 2) or 2))
            continue
        for action in actions:
            try:
                state, active_stop_order, action_result = apply_management_action(client, symbol, meta, state, action, active_stop_order)
            except BinanceAPIError as exc:
                message = str(exc)
                record_event('management_action_failed', {
                    'action': action.get('type'),
                    'message': message,
                    'current_price': round(float(current_price), 10),
                    'requested_stop_price': action.get('new_stop_price'),
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'kept_existing_stop': True,
                })
                persist_position(status='monitoring', protection_status=protection_status, active_stop_order=active_stop_order)
                continue
            action_exit_price = None
            if action.get('type') in {'take_profit_1', 'take_profit_2', 'runner_exit'}:
                action_exit_price = resolve_reduce_order_exit_price(action_result.get('reduce_order', {}), current_price)
                state.realized_r += compute_trade_realized_r_increment(
                    entry_price=plan.entry_price,
                    exit_price=action_exit_price,
                    initial_risk_per_unit=plan.initial_risk_per_unit,
                    close_qty=action.get('close_qty'),
                    initial_quantity=state.initial_quantity,
                    side=state.position_side,
                )
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
                    'exit_price': round(action_exit_price, 10) if action_exit_price is not None else None,
                    'realized_r_after_action': round(state.realized_r, 4),
                })
            elif action['type'] == 'take_profit_2':
                action.setdefault('exit_reason', 'tp2')
                record_event('tp2_hit', {
                    'close_qty': round(action['close_qty'], 10),
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'new_stop_price': round(action.get('new_stop_price'), 10) if action.get('new_stop_price') is not None else None,
                    'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) else None,
                    'exit_reason': action.get('exit_reason', 'tp2'),
                    'exit_price': round(action_exit_price, 10) if action_exit_price is not None else None,
                    'realized_r_after_action': round(state.realized_r, 4),
                })
            elif action['type'] == 'runner_exit':
                action.setdefault('exit_reason', 'runner')
                record_event('runner_exited', {
                    'close_qty': round(action['close_qty'], 10),
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'trailing_floor': round(action.get('trailing_floor'), 10) if action.get('trailing_floor') is not None else None,
                    'exit_reason': action.get('exit_reason', 'runner'),
                    'exit_price': round(action_exit_price, 10) if action_exit_price is not None else None,
                    'realized_r_after_action': round(state.realized_r, 4),
                })
            if protection_status == 'flat':
                final_exit_reason = action.get('exit_reason', 'flat')
                closed_at = _utc_now()
                analytics_snapshot = build_trade_analytics_snapshot(state, plan, closed_at=closed_at)
                selected_score = _to_float(
                    tracked.get('selected_score', tracked.get('score', selection_context.get('selected_score', selection_context.get('score')))),
                    default=0.0,
                )
                record_event('trade_invalidated', {
                    'exit_reason': final_exit_reason,
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'protection_status': protection_status,
                    'exit_price': round(action_exit_price, 10) if action_exit_price is not None else None,
                    'score': round(selected_score, 4),
                    'score_decile': str(tracked.get('score_decile') or selection_context.get('score_decile') or score_to_decile_label(selected_score)),
                    'state': str(tracked.get('selected_state') or tracked.get('state') or selection_context.get('selected_state') or selection_context.get('state') or ''),
                    'alert_tier': str(tracked.get('selected_alert_tier') or tracked.get('alert_tier') or selection_context.get('selected_alert_tier') or selection_context.get('alert_tier') or ''),
                    'candidate_stage': str(tracked.get('candidate_stage') or selection_context.get('candidate_stage') or ''),
                    'trigger_class': str(tracked.get('trigger_class') or selection_context.get('trigger_class') or resolve_trigger_class(selection_context)),
                    'market_regime_label': str(tracked.get('market_regime_label') or selection_context.get('market_regime_label') or ''),
                    'market_regime_multiplier': round(_to_float(tracked.get('market_regime_multiplier', selection_context.get('market_regime_multiplier')), default=0.0), 4),
                    'setup_ready': bool(tracked.get('setup_ready', selection_context.get('setup_ready', False))),
                    'trigger_fired': bool(tracked.get('trigger_fired', selection_context.get('trigger_fired', False))),
                    **analytics_snapshot,
                }, notify=False)
            persist_position(
                status='closed' if protection_status == 'flat' else 'monitoring',
                protection_status=protection_status,
                active_stop_order=active_stop_order,
                exit_reason=action.get('exit_reason') if protection_status == 'flat' else None,
                closed_at=closed_at if protection_status == 'flat' else None,
            )
            tracked['exit_reason'] = action.get('exit_reason') if protection_status == 'flat' else tracked.get('exit_reason')
            if protection_status == 'flat':
                break
        if protection_status == 'flat':
            break
        time.sleep(float(getattr(args, 'monitor_poll_interval_sec', 2) or 2))

    final_status = 'closed' if state.remaining_quantity <= 0 or protection_status == 'flat' else 'monitoring'
    final_exit_reason = tracked.get('exit_reason')
    persist_position(
        status=final_status,
        protection_status='flat' if final_status == 'closed' else protection_status,
        active_stop_order=active_stop_order if final_status != 'closed' else None,
        exit_reason=final_exit_reason if final_status == 'closed' else None,
        closed_at=_utc_now() if final_status == 'closed' else None,
    )
    return {
        'ok': True,
        'mode': 'foreground',
        'symbol': symbol,
        'status': final_status,
        'remaining_quantity': round(state.remaining_quantity, 10),
        'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) and final_status != 'closed' else None,
        'protection_status': 'flat' if final_status == 'closed' else protection_status,
        'exit_reason': final_exit_reason if final_status == 'closed' else None,
        'realized_r': round(state.realized_r, 4),
    }


def start_trade_monitor_thread(*args, **kwargs):
    thread = threading.Thread(target=monitor_live_trade, kwargs=kwargs, daemon=True, name=f"trade-monitor-{kwargs.get('symbol') or (args[1] if len(args) > 1 else 'unknown')}")
    thread.start()
    return thread


def resolve_auto_loop_book_ticker_symbols(client: BinanceFuturesClient, args: argparse.Namespace) -> List[str]:
    scan_top_n = int(getattr(args, 'top_n', 5) or 5)
    top_gainers = int(getattr(args, 'top_gainers', 20) or 20)
    top_losers = int(getattr(args, 'top_losers', top_gainers) or 0)
    try:
        square_rows = fetch_square_hot_symbols(client, limit=scan_top_n)
    except Exception:
        square_rows = []
    square_symbols = [str(row.get('symbol', '')).upper() for row in list(square_rows or []) if str(row.get('symbol', '')).strip()]
    try:
        tickers = fetch_24h_tickers(client)
    except Exception:
        tickers = []
    merged_payload = merged_candidate_symbols(
        square_symbols=square_symbols,
        tickers=tickers,
        top_gainers=top_gainers,
        top_losers=top_losers,
    )
    merged_symbols = merged_payload[0]
    symbols = [str(symbol).upper() for symbol in list(merged_symbols or []) if str(symbol).strip()]
    if symbols:
        return symbols
    if square_symbols:
        return square_symbols
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
    return store.append_event('buy_fill_confirmed', {
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
    })


def run_loop(client: Any, args: argparse.Namespace) -> Dict[str, Any]:
    store = get_runtime_state_store(args)
    okx_simulated_trading = bool(getattr(args, 'okx_simulated_trading', False))
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

    if okx_simulated_trading:
        reconcile = {
            'ok': True,
            'skipped': True,
            'skip_reason': 'okx_simulated_trading',
            'orphan_positions': [],
            'positions_missing_protection': [],
            'protection_repairs': [],
        }
    elif binance_simulated_trading:
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
            else:
                raise
    if getattr(args, 'reconcile_only', False):
        reconcile_payload = {'reconcile': reconcile}
        persist_cycle_snapshot(reconcile_payload)
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
        else:
            cycle['book_ticker_websocket'] = {
                'status': 'unavailable',
                'reason': 'websocket_client_missing',
            }
            append_rate_limited_runtime_event(store, 'book_ticker_ws_unavailable', {
                'event_source': 'book_ticker_websocket',
                'reason': 'websocket_client_missing',
            }, key='global', min_interval_seconds=3600.0)
        if okx_simulated_trading:
            uds_monitor = {
                'status': 'skipped',
                'action': 'skipped',
                'exchange': 'OKX_SIMULATED',
                'listen_key': '',
                'detail': 'okx_simulated_trading_uses_okx_private_state_not_binance_listen_key',
                'now_utc': _isoformat_utc(_utc_now()),
                'refresh_failure_count': 0,
                'disconnect_count': 0,
            }
            store.save_json('user_data_stream', uds_monitor)
            cycle['user_data_stream_monitor'] = uds_monitor
        else:
            existing_uds_state = store.load_json('user_data_stream', {})
            if isinstance(existing_uds_state, dict) and existing_uds_state.get('listen_key'):
                uds_monitor = run_user_data_stream_monitor_cycle(
                    client=client,
                    store=store,
                    symbol=existing_uds_state.get('symbol'),
                    refresh_interval_minutes=float(getattr(args, 'user_stream_refresh_interval_minutes', 30.0) or 30.0),
                    disconnect_timeout_minutes=float(getattr(args, 'user_stream_disconnect_timeout_minutes', 65.0) or 65.0),
                )
                persist_user_data_stream_monitor_to_positions(store, uds_monitor)
                cycle['user_data_stream_monitor'] = uds_monitor
                alert_payload = emit_user_data_stream_alert_if_needed(args, existing_uds_state.get('symbol'), uds_monitor)
                if alert_payload is not None:
                    cycle['user_data_stream_alert'] = alert_payload
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
    okx_client_for_management: Optional[OKXClient] = None
    if okx_simulated_trading:
        okx_api_key, okx_api_secret, okx_passphrase = resolve_okx_simulated_api_credentials()
        okx_client_for_management = OKXClient(
            base_url=getattr(args, 'okx_base_url', 'https://www.okx.com'),
            api_key=okx_api_key,
            api_secret=okx_api_secret,
            passphrase=okx_passphrase,
            simulated_trading=True,
        )
        cycle['okx_position_management'] = manage_okx_simulated_positions(store, args, okx_client_for_management)
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
    risk_state = load_risk_state(store)
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
    )
    if getattr(args, 'live', False) and okx_simulated_trading:
        open_positions = build_local_open_positions_for_risk(store)
    elif getattr(args, 'live', False) and not binance_simulated_trading:
        open_positions = fetch_open_positions(client)
    else:
        open_positions = []
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
    max_open_positions = int(getattr(args, 'max_open_positions', 1) or 1)
    if len(open_positions) >= max_open_positions:
        cycle['live_skipped_due_to_existing_positions'] = open_positions
        append_candidate_rejected_event(store, best_candidate, ['max_open_positions_reached'], {'open_positions': open_positions})
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
        probe_entry = evaluate_sim_probe_entry(best_candidate, risk_guard, args) if (okx_simulated_trading or binance_simulated_trading) else {'allowed': False, 'reasons': ['not_simulated_trading']}
        cycle['sim_probe_entry'] = probe_entry
        if not bool(probe_entry.get('allowed', False)):
            cycle['live_skipped_due_to_risk_guard'] = risk_guard['reasons']
            append_candidate_rejected_event(store, best_candidate, risk_guard['reasons'])
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
    meta = meta_map.get(best_candidate.symbol)
    if meta is None:
        raise ValueError(f'missing symbol meta for {best_candidate.symbol}')
    requested_leverage = int(getattr(args, 'leverage', best_candidate.recommended_leverage) or best_candidate.recommended_leverage)
    if okx_simulated_trading:
        okx_client = okx_client_for_management
        if okx_client is None:
            okx_api_key, okx_api_secret, okx_passphrase = resolve_okx_simulated_api_credentials()
            okx_client = OKXClient(
                base_url=getattr(args, 'okx_base_url', 'https://www.okx.com'),
                api_key=okx_api_key,
                api_secret=okx_api_secret,
                passphrase=okx_passphrase,
                simulated_trading=True,
            )
        try:
            live_execution = place_okx_simulated_trade(okx_client, best_candidate, requested_leverage, args)
        except OKXAPIError as exc:
            if is_non_retryable_okx_symbol_error(exc):
                skip_symbols = load_okx_sim_skip_symbols(store)
                skip_symbols.add(normalize_symbol(best_candidate.symbol))
                save_okx_sim_skip_symbols(store, skip_symbols)
            cycle['live_execution_error'] = {
                'exchange': 'OKX',
                'simulated': True,
                'symbol': best_candidate.symbol,
                'side': getattr(best_candidate, 'side', getattr(best_candidate, 'position_side', '')),
                'error': str(exc),
                'entry_mode': 'sim_probe' if bool(probe_entry.get('allowed', False)) else 'full',
            }
            append_candidate_rejected_event(store, best_candidate, ['okx_execution_preflight_failed'], cycle['live_execution_error'])
            persist_cycle_snapshot(cycle)
            return result
        if bool(probe_entry.get('allowed', False)):
            live_execution['entry_mode'] = 'sim_probe'
            live_execution['probe_size_ratio'] = float(probe_entry.get('size_ratio', 0.2) or 0.2)
    else:
        try:
            live_execution = place_live_trade(client, best_candidate, requested_leverage, meta, args)
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
    scan_funnel = cycle.get('scan', {}).get('funnel')
    if isinstance(scan_funnel, dict):
        scan_funnel['order_submitted_count'] = 1
    positions_state, position_key = persist_live_open_position(store, best_candidate, live_execution)
    if okx_simulated_trading:
        store.append_event('okx_simulated_order_submitted', {
            'symbol': best_candidate.symbol,
            'side': live_execution.get('side'),
            'position_key': position_key,
            'entry_price': live_execution.get('entry_price'),
            'quantity': live_execution.get('filled_quantity'),
            'inst_id': live_execution.get('inst_id'),
            'order_id': live_execution.get('entry_order_feedback', {}).get('order_id'),
            'entry_mode': live_execution.get('entry_mode', 'full'),
            'probe_size_ratio': live_execution.get('probe_size_ratio'),
            'profile': getattr(args, 'profile', 'default'),
        })
        cycle['trade_management'] = {
            'mode': 'okx_simulated',
            'status': 'submitted',
            'message': 'OKX simulated order submitted; Binance live monitor is skipped.',
        }
        persist_cycle_snapshot(cycle)
        return result
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
        append_buy_fill_confirmed_event(store, best_candidate.symbol, positions_state, position_key)
        alert_payload = emit_user_data_stream_alert_if_needed(args, best_candidate.symbol, uds_monitor)
        if alert_payload is not None:
            cycle['user_data_stream_alert'] = alert_payload
        cycle['trade_management'] = {
            'mode': 'background_thread',
            'thread_name': getattr(thread, 'name', 'trade-monitor'),
            'user_data_stream': uds_monitor,
        }
    else:
        cycle['trade_management'] = monitor_live_trade(client=client, symbol=best_candidate.symbol, meta=meta, args=args, trade=live_execution)
    persist_cycle_snapshot(cycle)
    return result


def print_scan_output(result: Dict[str, Any], output_format: str) -> None:
    if output_format == 'json':
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    print(render_cn_scan_summary(result))


def main(argv: Optional[Sequence[str]] = None) -> int:
    load_dotenv()
    args = apply_runtime_profile(parse_args(argv))
    if is_binance_simulated_trading(args) and 'base_url' not in set(getattr(args, '_explicit_cli_dests', set()) or set()):
        args.base_url = 'https://testnet.binancefuture.com'
    binance_api_key, binance_api_secret = resolve_binance_api_credentials(args)
    client = BinanceFuturesClient(
        base_url=args.base_url,
        api_key=binance_api_key,
        api_secret=binance_api_secret,
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
