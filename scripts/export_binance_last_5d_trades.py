#!/usr/bin/env python3
"""Export recent Binance USDT-M Futures trading records.

Creates event-style JSONL plus JSON/Markdown summaries for Git archival.
"""
import hashlib
import hmac
import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "runtime-state"
ENV_PATH = Path("/root/.hermes/.env")
DAYS = 5


def load_env():
    if ENV_PATH.exists():
        for raw in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def signed_get(path, params=None):
    key = os.environ["BINANCE_FUTURES_API_KEY"]
    secret = os.environ["BINANCE_FUTURES_API_SECRET"]
    base = os.environ.get("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com").rstrip("/")
    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)
    params.setdefault("recvWindow", 60000)
    qs = urlencode(params, doseq=True)
    sig = hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"{base}{path}?{qs}&signature={sig}"
    response = requests.get(url, headers={"X-MBX-APIKEY": key}, timeout=30)
    response.raise_for_status()
    return response.json()


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def num(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def fetch_income(start_ms, end_ms):
    rows = []
    cursor = start_ms
    while cursor < end_ms:
        batch = signed_get("/fapi/v1/income", {"startTime": cursor, "endTime": end_ms, "limit": 1000})
        if not batch:
            break
        rows.extend(batch)
        max_time = max(int(row["time"]) for row in batch)
        if len(batch) < 1000 or max_time <= cursor:
            break
        cursor = max_time + 1
        time.sleep(0.15)
    return rows


def fetch_symbol_pages(path, symbol, start_ms, end_ms):
    rows = []
    cursor = start_ms
    while cursor < end_ms:
        batch = signed_get(path, {"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": 1000})
        if not batch:
            break
        rows.extend(batch)
        time_key = "time" if "time" in batch[-1] else "updateTime"
        max_time = max(int(row.get(time_key, row.get("time", row.get("updateTime", 0)))) for row in batch)
        if len(batch) < 1000 or max_time <= cursor:
            break
        cursor = max_time + 1
        time.sleep(0.15)
    return rows


def main():
    load_env()
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=DAYS)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    income = fetch_income(start_ms, end_ms)
    positions = signed_get("/fapi/v2/positionRisk")
    symbols = {row.get("symbol") for row in income if row.get("symbol")}
    symbols.update(row["symbol"] for row in positions if num(row.get("positionAmt")) != 0)
    symbols = sorted(s for s in symbols if s)

    trades_by_symbol = {}
    orders_by_symbol = {}
    for symbol in symbols:
        trades_by_symbol[symbol] = fetch_symbol_pages("/fapi/v1/userTrades", symbol, start_ms, end_ms)
        orders_by_symbol[symbol] = fetch_symbol_pages("/fapi/v1/allOrders", symbol, start_ms, end_ms)

    records = []
    for row in income:
        records.append({
            "recorded_at": iso(int(row["time"])),
            "source": "binance_futures_income",
            "event_type": row.get("incomeType"),
            "symbol": row.get("symbol"),
            "income": row.get("income"),
            "asset": row.get("asset"),
            "info": row.get("info"),
            "tran_id": row.get("tranId"),
            "trade_id": row.get("tradeId"),
        })
    for symbol, trades in trades_by_symbol.items():
        for row in trades:
            records.append({
                "recorded_at": iso(int(row["time"])),
                "source": "binance_futures_user_trades",
                "event_type": "USER_TRADE",
                "symbol": symbol,
                "side": "BUY" if row.get("buyer") else "SELL",
                "position_side": row.get("positionSide"),
                "order_id": row.get("orderId"),
                "trade_id": row.get("id"),
                "quantity": row.get("qty"),
                "price": row.get("price"),
                "realized_pnl": row.get("realizedPnl"),
                "commission": row.get("commission"),
                "commission_asset": row.get("commissionAsset"),
                "maker": row.get("maker"),
            })
    for symbol, orders in orders_by_symbol.items():
        for row in orders:
            records.append({
                "recorded_at": iso(int(row.get("updateTime") or row.get("time") or 0)),
                "source": "binance_futures_orders",
                "event_type": "ORDER",
                "symbol": symbol,
                "side": row.get("side"),
                "position_side": row.get("positionSide"),
                "order_id": row.get("orderId"),
                "status": row.get("status"),
                "type": row.get("type"),
                "orig_qty": row.get("origQty"),
                "executed_qty": row.get("executedQty"),
                "avg_price": row.get("avgPrice"),
                "stop_price": row.get("stopPrice"),
                "reduce_only": row.get("reduceOnly"),
                "close_position": row.get("closePosition"),
            })

    records.sort(key=lambda item: item.get("recorded_at") or "")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    jsonl_path = OUT_DIR / "trade-records-last-5d.jsonl"
    summary_json_path = OUT_DIR / "trade-records-last-5d-summary.json"
    summary_md_path = OUT_DIR / "trade-records-last-5d-summary.md"

    with jsonl_path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    source_counts = Counter(r["source"] for r in records)
    event_counts = Counter(str(r.get("event_type")) for r in records)
    symbol_counts = Counter(r.get("symbol") or "" for r in records)
    realized_by_symbol = defaultdict(float)
    commission_by_symbol = defaultdict(float)
    income_by_type = defaultdict(float)
    for row in income:
        income_by_type[row.get("incomeType") or ""] += num(row.get("income"))
        if row.get("incomeType") == "REALIZED_PNL":
            realized_by_symbol[row.get("symbol") or ""] += num(row.get("income"))
        if row.get("incomeType") == "COMMISSION":
            commission_by_symbol[row.get("symbol") or ""] += num(row.get("income"))

    summary = {
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "window_start": start.isoformat().replace("+00:00", "Z"),
        "window_end": now.isoformat().replace("+00:00", "Z"),
        "source": "Binance USDT-M Futures REST API: income, userTrades, allOrders",
        "symbols": symbols,
        "record_count": len(records),
        "source_counts": dict(source_counts),
        "event_type_counts": dict(event_counts),
        "symbol_record_counts": dict(symbol_counts),
        "income_by_type_usdt": dict(sorted(income_by_type.items())),
        "realized_pnl_by_symbol_usdt": dict(sorted(realized_by_symbol.items())),
        "commission_by_symbol_usdt": dict(sorted(commission_by_symbol.items())),
        "open_position_symbols_at_export": [row["symbol"] for row in positions if num(row.get("positionAmt")) != 0],
    }
    summary_json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    lines = [
        "# Binance Futures trade records — last 5 days",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Window start: `{summary['window_start']}`",
        f"- Window end: `{summary['window_end']}`",
        f"- Source: {summary['source']}",
        f"- Total records: `{len(records)}`",
        f"- Symbols: `{', '.join(symbols) if symbols else 'none'}`",
        "",
        "## Source counts",
    ]
    for key, value in sorted(source_counts.items()):
        lines.append(f"- `{key}`: `{value}`")
    lines += ["", "## Income by type (USDT)"]
    for key, value in sorted(income_by_type.items()):
        lines.append(f"- `{key}`: `{value:.8f}`")
    lines += ["", "## Realized PnL by symbol (USDT)"]
    for key, value in sorted(realized_by_symbol.items()):
        lines.append(f"- `{key}`: `{value:.8f}`")
    lines += ["", "## Commission by symbol (USDT)"]
    for key, value in sorted(commission_by_symbol.items()):
        lines.append(f"- `{key}`: `{value:.8f}`")
    lines.append("")
    summary_md_path.write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps({"jsonl": str(jsonl_path), "summary_json": str(summary_json_path), "summary_md": str(summary_md_path), "record_count": len(records), "symbols": symbols}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
