#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

import yaobiradar_v2_output_writer as writer


DEFAULT_ENGINE = 'accumulation_radar'
DEFAULT_BASE_URL = 'https://fapi.binance.com'
EXCLUDED_COINS = {'USDC', 'USDP', 'TUSD', 'FDUSD', 'BTCDOM', 'DEFI', 'USDM'}


@dataclass(frozen=True)
class RadarConfig:
    min_sideways_days: int = 45
    max_range_pct: float = 80.0
    max_avg_vol_usd: float = 20_000_000.0
    min_data_days: int = 50
    vol_breakout_mult: float = 3.0
    min_oi_usd: float = 2_000_000.0
    oi_bonus_threshold_pct: float = 3.0


class BinancePublicClient:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        session: Optional[requests.Session] = None,
        max_get_retries: int = 3,
        get_retry_sleep_sec: float = 0.5,
    ):
        self.base_url = base_url.rstrip('/')
        self.session = session or requests.Session()
        self.max_get_retries = max(1, int(max_get_retries or 1))
        self.get_retry_sleep_sec = max(0.0, float(get_retry_sleep_sec or 0.0))

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15) -> Any:
        last_exc: Optional[BaseException] = None
        url = f'{self.base_url}{path}'
        for attempt in range(self.max_get_retries):
            try:
                response = self.session.get(url, params=params or {}, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_exc = exc
                if attempt + 1 >= self.max_get_retries:
                    break
                time.sleep(self.get_retry_sleep_sec * (2 ** attempt))
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f'GET request failed without response: {path}')


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_symbol(raw: Any) -> str:
    return writer.normalize_symbol(raw)


def coin_from_symbol(symbol: str) -> str:
    symbol = normalize_symbol(symbol)
    return symbol[:-4] if symbol.endswith('USDT') else symbol


def format_usd(value: float) -> str:
    value = float(value or 0.0)
    if abs(value) >= 1e9:
        return f'${value / 1e9:.1f}B'
    if abs(value) >= 1e6:
        return f'${value / 1e6:.1f}M'
    if abs(value) >= 1e3:
        return f'${value / 1e3:.0f}K'
    return f'${value:.0f}'


def parse_kline_rows(klines: Sequence[Sequence[Any]]) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for row in klines:
        if len(row) < 8:
            continue
        rows.append({
            'open': _to_float(row[1]),
            'high': _to_float(row[2]),
            'low': _to_float(row[3]),
            'close': _to_float(row[4]),
            'quote_volume': _to_float(row[7]),
        })
    return rows


def linear_slope_pct(closes: Sequence[float]) -> float:
    if len(closes) < 2 or closes[0] <= 0:
        return 0.0
    n = len(closes)
    x_mean = (n - 1) / 2
    y_mean = sum(closes) / n
    denom = sum((idx - x_mean) ** 2 for idx in range(n))
    if denom <= 0:
        return 0.0
    slope = sum((idx - x_mean) * (price - y_mean) for idx, price in enumerate(closes)) / denom
    return slope * n / closes[0] * 100


def analyze_accumulation(symbol: str, klines: Sequence[Sequence[Any]], config: RadarConfig = RadarConfig()) -> Optional[Dict[str, Any]]:
    symbol = normalize_symbol(symbol)
    coin = coin_from_symbol(symbol)
    if not symbol or coin in EXCLUDED_COINS:
        return None

    data = parse_kline_rows(klines)
    if len(data) < config.min_data_days:
        return None

    recent_7d = data[-7:]
    prior = data[:-7]
    if not recent_7d or not prior:
        return None
    recent_avg_price = sum(row['close'] for row in recent_7d) / len(recent_7d)
    prior_avg_price = sum(row['close'] for row in prior) / len(prior)
    if prior_avg_price > 0 and (recent_avg_price - prior_avg_price) / prior_avg_price > 3.0:
        return None

    best: Optional[Dict[str, Any]] = None
    max_window = min(180, len(data))
    for window in range(config.min_sideways_days, max_window + 1):
        rows = data[-window:]
        highs = [row['high'] for row in rows]
        lows = [row['low'] for row in rows]
        closes = [row['close'] for row in rows]
        low_price = min(lows)
        high_price = max(highs)
        if low_price <= 0:
            continue
        range_pct = (high_price - low_price) / low_price * 100
        if range_pct > config.max_range_pct:
            continue
        avg_vol = sum(row['quote_volume'] for row in rows) / len(rows)
        if avg_vol > config.max_avg_vol_usd:
            continue
        slope_pct = linear_slope_pct(closes)
        if abs(slope_pct) > 20:
            continue
        best = {
            'sideways_days': window,
            'range_pct': range_pct,
            'low_price': low_price,
            'high_price': high_price,
            'avg_vol': avg_vol,
            'slope_pct': slope_pct,
        }

    if not best:
        return None

    best_avg_vol = float(best['avg_vol'])
    recent_vol = sum(row['quote_volume'] for row in recent_7d) / len(recent_7d)
    vol_breakout = recent_vol / best_avg_vol if best_avg_vol > 0 else 0.0
    estimated_mcap = data[-1]['close'] * best_avg_vol * 30

    days_score = min(float(best['sideways_days']) / 90.0, 1.0) * 25
    range_score = max(0.0, 1 - float(best['range_pct']) / config.max_range_pct) * 20
    vol_score = max(0.0, 1 - best_avg_vol / config.max_avg_vol_usd) * 20
    breakout_score = min(vol_breakout / config.vol_breakout_mult, 1.0) * 15
    if estimated_mcap < 50_000_000:
        mcap_score = 20
    elif estimated_mcap < 100_000_000:
        mcap_score = 15
    elif estimated_mcap < 200_000_000:
        mcap_score = 10
    elif estimated_mcap < 500_000_000:
        mcap_score = 5
    else:
        mcap_score = 0
    flatness_bonus = max(0.0, 1 - abs(float(best['slope_pct'])) / 20.0) * 5
    score = days_score + range_score + vol_score + breakout_score + mcap_score + flatness_bonus

    status = 'volume_breakout' if vol_breakout >= config.vol_breakout_mult else 'volume_warming' if vol_breakout >= 1.5 else 'accumulating'
    return {
        'symbol': symbol,
        'coin': coin,
        'sideways_days': int(best['sideways_days']),
        'range_pct': round(float(best['range_pct']), 4),
        'slope_pct': round(float(best['slope_pct']), 4),
        'low_price': float(best['low_price']),
        'high_price': float(best['high_price']),
        'avg_vol': best_avg_vol,
        'current_price': data[-1]['close'],
        'recent_vol': recent_vol,
        'vol_breakout': round(vol_breakout, 4),
        'estimated_mcap': estimated_mcap,
        'accumulation_score': round(score, 4),
        'status': status,
        'data_days': len(data),
    }


def classify_tier(score: float, status: str = '') -> str:
    if score >= 90 or status == 'volume_breakout':
        return 'critical'
    if score >= 75 or status == 'volume_warming':
        return 'high'
    return 'watch'


def position_size_pct_for_tier(tier: str) -> float:
    return {'critical': 3.0, 'high': 2.0, 'watch': 1.0}.get(str(tier).lower(), 1.0)


def fetch_all_perp_symbols(client: Any) -> List[str]:
    payload = client.get('/fapi/v1/exchangeInfo')
    symbols = []
    for row in payload.get('symbols', []) if isinstance(payload, dict) else []:
        if row.get('quoteAsset') == 'USDT' and row.get('contractType') == 'PERPETUAL' and row.get('status') == 'TRADING':
            symbols.append(str(row.get('symbol', '')).upper())
    return symbols


def fetch_ticker_map(client: Any) -> Dict[str, Dict[str, float]]:
    payload = client.get('/fapi/v1/ticker/24hr')
    rows = payload if isinstance(payload, list) else [payload]
    result: Dict[str, Dict[str, float]] = {}
    for row in rows:
        symbol = normalize_symbol(row.get('symbol')) if isinstance(row, dict) else ''
        if symbol.endswith('USDT'):
            result[symbol] = {
                'price_change_pct': _to_float(row.get('priceChangePercent')),
                'quote_volume': _to_float(row.get('quoteVolume')),
                'last_price': _to_float(row.get('lastPrice')),
            }
    return result


def fetch_funding_map(client: Any) -> Dict[str, float]:
    payload = client.get('/fapi/v1/premiumIndex')
    rows = payload if isinstance(payload, list) else [payload]
    result: Dict[str, float] = {}
    for row in rows:
        symbol = normalize_symbol(row.get('symbol')) if isinstance(row, dict) else ''
        if symbol.endswith('USDT'):
            result[symbol] = _to_float(row.get('lastFundingRate'))
    return result


def fetch_oi_snapshot(client: Any, symbol: str, config: RadarConfig = RadarConfig()) -> Dict[str, float]:
    try:
        rows = client.get('/futures/data/openInterestHist', {'symbol': normalize_symbol(symbol), 'period': '1h', 'limit': 6})
    except Exception:
        return {}
    if not isinstance(rows, list) or len(rows) < 2:
        return {}
    current = _to_float(rows[-1].get('sumOpenInterestValue') if isinstance(rows[-1], dict) else None)
    prev_1h = _to_float(rows[-2].get('sumOpenInterestValue') if isinstance(rows[-2], dict) else None)
    prev_6h = _to_float(rows[0].get('sumOpenInterestValue') if isinstance(rows[0], dict) else None)
    if current < config.min_oi_usd:
        return {'oi_usd': current, 'oi_1h_pct': 0.0, 'oi_6h_pct': 0.0}
    return {
        'oi_usd': current,
        'oi_1h_pct': ((current - prev_1h) / prev_1h * 100) if prev_1h > 0 else 0.0,
        'oi_6h_pct': ((current - prev_6h) / prev_6h * 100) if prev_6h > 0 else 0.0,
    }


def scan_pool(
    client: Any,
    symbols: Optional[Sequence[str]] = None,
    config: RadarConfig = RadarConfig(),
    sleep_every: int = 10,
    sleep_seconds: float = 0.2,
) -> List[Dict[str, Any]]:
    scan_symbols = [normalize_symbol(symbol) for symbol in symbols] if symbols else fetch_all_perp_symbols(client)
    results: List[Dict[str, Any]] = []
    for index, symbol in enumerate([symbol for symbol in scan_symbols if symbol], start=1):
        try:
            klines = client.get('/fapi/v1/klines', {'symbol': symbol, 'interval': '1d', 'limit': 180})
        except Exception:
            continue
        if isinstance(klines, list):
            result = analyze_accumulation(symbol, klines, config)
            if result:
                results.append(result)
        if sleep_every > 0 and index % sleep_every == 0:
            time.sleep(max(0.0, sleep_seconds))
    return sorted(results, key=lambda row: (-float(row.get('accumulation_score', 0.0)), str(row.get('symbol', ''))))


def build_external_rows(
    pool_results: Sequence[Dict[str, Any]],
    oi_map: Optional[Dict[str, Dict[str, float]]] = None,
    ticker_map: Optional[Dict[str, Dict[str, float]]] = None,
    funding_map: Optional[Dict[str, float]] = None,
    top: int = 30,
    config: RadarConfig = RadarConfig(),
) -> List[Dict[str, Any]]:
    oi_map = oi_map or {}
    ticker_map = ticker_map or {}
    funding_map = funding_map or {}
    rows: List[Dict[str, Any]] = []
    for result in pool_results:
        symbol = normalize_symbol(result.get('symbol'))
        if not symbol:
            continue
        oi = oi_map.get(symbol, {})
        ticker = ticker_map.get(symbol, {})
        funding = float(funding_map.get(symbol, 0.0) or 0.0)
        score = float(result.get('accumulation_score', 0.0) or 0.0)
        oi_6h_pct = float(oi.get('oi_6h_pct', 0.0) or 0.0)
        px_chg = float(ticker.get('price_change_pct', 0.0) or 0.0)
        if abs(oi_6h_pct) >= config.oi_bonus_threshold_pct:
            score += min(abs(oi_6h_pct) * 1.2, 15.0)
        if funding < 0:
            score += min(abs(funding) * 10_000, 8.0)
        if oi_6h_pct > 2 and abs(px_chg) < 5:
            score += 5.0
        score = min(round(score, 4), 100.0)
        tier = classify_tier(score, str(result.get('status', '')))

        reasons = [
            f"accumulation_days={int(result.get('sideways_days', 0) or 0)}",
            f"range_pct={float(result.get('range_pct', 0.0) or 0.0):.1f}",
            f"avg_vol={format_usd(float(result.get('avg_vol', 0.0) or 0.0))}",
            f"status={result.get('status', '')}",
        ]
        if abs(oi_6h_pct) >= config.oi_bonus_threshold_pct:
            reasons.append(f"oi_6h_pct={oi_6h_pct:+.1f}")
        if oi_6h_pct > 2 and abs(px_chg) < 5:
            reasons.append('dark_flow_oi_up_price_flat')
        if funding < 0:
            reasons.append(f"negative_funding={funding * 100:.4f}%")

        rows.append({
            'symbol': symbol,
            'external_signal_score': score,
            'external_signal_tier': tier,
            'external_position_size_pct': position_size_pct_for_tier(tier),
            'external_reasons': reasons,
            'portfolio_narrative_bucket': 'accumulation',
            'portfolio_correlation_group': 'accumulation',
        })

    return sorted(rows, key=lambda row: (-float(row.get('external_signal_score', 0.0)), str(row.get('symbol', ''))))[:top]


def scan_external_signals(
    client: Any,
    symbols: Optional[Sequence[str]] = None,
    top: int = 30,
    config: RadarConfig = RadarConfig(),
) -> Dict[str, Any]:
    pool = scan_pool(client, symbols=symbols, config=config)
    pool = pool[:max(top * 2, top)]
    ticker_map = fetch_ticker_map(client)
    funding_map = fetch_funding_map(client)
    oi_map = {row['symbol']: fetch_oi_snapshot(client, row['symbol'], config) for row in pool}
    rows = build_external_rows(pool, oi_map=oi_map, ticker_map=ticker_map, funding_map=funding_map, top=top, config=config)
    return writer.build_payload(rows, engine=DEFAULT_ENGINE)


def parse_symbol_list(value: str) -> List[str]:
    return [normalize_symbol(part) for part in str(value or '').replace('\n', ',').split(',') if normalize_symbol(part)]


def load_symbols_file(path: str) -> List[str]:
    if not path:
        return []
    return parse_symbol_list(Path(path).read_text(encoding='utf-8'))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Generate accumulation-radar external signals for the Binance strategy.')
    parser.add_argument('--mode', choices=['pool', 'external'], default='external')
    parser.add_argument('--base-url', default=DEFAULT_BASE_URL)
    parser.add_argument('--symbols', default='', help='Comma-separated symbols. Empty means scan all USDT perpetuals.')
    parser.add_argument('--symbols-file', default='', help='Optional newline/comma separated symbol file.')
    parser.add_argument('--top', type=int, default=30)
    parser.add_argument('--min-sideways-days', type=int, default=45)
    parser.add_argument('--max-range-pct', type=float, default=80.0)
    parser.add_argument('--max-avg-vol-usd', type=float, default=20_000_000.0)
    parser.add_argument('--min-oi-usd', type=float, default=2_000_000.0)
    parser.add_argument('--symbols-output', default=str(writer.SYMBOLS_PATH))
    parser.add_argument('--external-json-output', default=str(writer.EXTERNAL_JSON_PATH))
    parser.add_argument('--print-json', action='store_true')
    parser.add_argument('--no-write', action='store_true')
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    config = RadarConfig(
        min_sideways_days=args.min_sideways_days,
        max_range_pct=args.max_range_pct,
        max_avg_vol_usd=args.max_avg_vol_usd,
        min_oi_usd=args.min_oi_usd,
    )
    symbols = parse_symbol_list(args.symbols) + load_symbols_file(args.symbols_file)
    symbols = list(dict.fromkeys(symbols)) or None
    client = BinancePublicClient(args.base_url)

    if args.mode == 'pool':
        pool = scan_pool(client, symbols=symbols, config=config)
        print(json.dumps(pool[:args.top], ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    payload = scan_external_signals(client, symbols=symbols, top=args.top, config=config)
    if not args.no_write:
        writer.write_outputs(
            payload,
            symbols_path=Path(args.symbols_output),
            external_json_path=Path(args.external_json_output),
        )
    if args.print_json or args.no_write:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"wrote {len(payload.get('symbols', []))} accumulation symbols to {args.symbols_output}")
        print(f"wrote external signal payload to {args.external_json_output}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
