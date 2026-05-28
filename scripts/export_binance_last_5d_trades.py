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


def rounded(value):
    return round(float(value), 10)


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


def compute_order_quality(trades_by_symbol, orders_by_symbol):
    market_order_count = 0
    limit_order_count = 0
    canceled_limit_count = 0
    cancel_replace_count_by_symbol = defaultdict(int)
    taker_trade_count = 0
    maker_trade_count = 0
    taker_fee = 0.0
    total_fee = 0.0
    avg_hold_seconds_by_symbol = {}
    fast_stop_count_by_symbol = defaultdict(int)

    for symbol, orders in (orders_by_symbol or {}).items():
        ordered = sorted(orders or [], key=lambda row: int(row.get("updateTime") or row.get("time") or 0))
        last_canceled_limit = False
        for row in ordered:
            order_type = str(row.get("type") or "").upper()
            status = str(row.get("status") or "").upper()
            if order_type == "MARKET":
                market_order_count += 1
            if "LIMIT" in order_type:
                limit_order_count += 1
                if status == "CANCELED":
                    canceled_limit_count += 1
                    last_canceled_limit = True
                    continue
            if last_canceled_limit and status in {"NEW", "PARTIALLY_FILLED", "FILLED"}:
                cancel_replace_count_by_symbol[symbol] += 1
            last_canceled_limit = False

    for symbol, trades in (trades_by_symbol or {}).items():
        ordered = sorted(trades or [], key=lambda row: int(row.get("time") or 0))
        entry_time = None
        hold_seconds = []
        for row in ordered:
            fee = abs(num(row.get("commission")))
            total_fee += fee
            if bool(row.get("maker")):
                maker_trade_count += 1
            else:
                taker_trade_count += 1
                taker_fee += fee
            realized = num(row.get("realizedPnl", row.get("realized_pnl")))
            ts = int(row.get("time") or 0)
            if abs(realized) < 1e-12 and entry_time is None:
                entry_time = ts
            elif realized != 0 and entry_time is not None:
                held = max((ts - entry_time) / 1000.0, 0.0)
                hold_seconds.append(held)
                if realized < 0 and held <= 300:
                    fast_stop_count_by_symbol[symbol] += 1
                entry_time = None
        if hold_seconds:
            avg_hold_seconds_by_symbol[symbol] = rounded(sum(hold_seconds) / len(hold_seconds))

    return {
        "market_order_count": market_order_count,
        "limit_order_count": limit_order_count,
        "canceled_limit_count": canceled_limit_count,
        "cancel_replace_count_by_symbol": dict(sorted(cancel_replace_count_by_symbol.items())),
        "taker_trade_count": taker_trade_count,
        "maker_trade_count": maker_trade_count,
        "taker_fee_ratio": rounded(taker_fee / total_fee) if total_fee else 0.0,
        "avg_hold_seconds_by_symbol": dict(sorted(avg_hold_seconds_by_symbol.items())),
        "fast_stop_count": int(sum(fast_stop_count_by_symbol.values())),
        "fast_stop_count_by_symbol": dict(sorted(fast_stop_count_by_symbol.items())),
    }


def build_summary(*, now, start, records, income, positions, trades_by_symbol, orders_by_symbol, symbols):
    source_counts = Counter(r["source"] for r in records)
    event_counts = Counter(str(r.get("event_type")) for r in records)
    symbol_counts = Counter(r.get("symbol") or "" for r in records)
    realized_by_symbol = defaultdict(float)
    commission_by_symbol = defaultdict(float)
    income_by_type = defaultdict(float)
    for row in income:
        income_type = row.get("incomeType") or ""
        value = num(row.get("income"))
        income_by_type[income_type] += value
        if income_type == "REALIZED_PNL":
            realized_by_symbol[row.get("symbol") or ""] += value
        if income_type == "COMMISSION":
            commission_by_symbol[row.get("symbol") or ""] += value

    open_positions = [row for row in positions if num(row.get("positionAmt")) != 0]
    unrealized_by_symbol = {
        row["symbol"]: rounded(num(row.get("unRealizedProfit", row.get("unrealizedProfit"))))
        for row in open_positions
    }
    realized_pnl = income_by_type.get("REALIZED_PNL", 0.0)
    commission = income_by_type.get("COMMISSION", 0.0)
    funding_fee = income_by_type.get("FUNDING_FEE", 0.0)
    open_unrealized = sum(unrealized_by_symbol.values())
    return {
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "window_start": start.isoformat().replace("+00:00", "Z"),
        "window_end": now.isoformat().replace("+00:00", "Z"),
        "source": "Binance USDT-M Futures REST API: income, userTrades, allOrders",
        "symbols": symbols,
        "record_count": len(records),
        "source_counts": dict(source_counts),
        "event_type_counts": dict(event_counts),
        "symbol_record_counts": dict(symbol_counts),
        "income_by_type_usdt": dict(sorted((k, rounded(v)) for k, v in income_by_type.items())),
        "realized_pnl_by_symbol_usdt": dict(sorted((k, rounded(v)) for k, v in realized_by_symbol.items())),
        "commission_by_symbol_usdt": dict(sorted((k, rounded(v)) for k, v in commission_by_symbol.items())),
        "realized_pnl_usdt": rounded(realized_pnl),
        "commission_usdt": rounded(commission),
        "funding_fee_usdt": rounded(funding_fee),
        "net_realized_after_fee_usdt": rounded(realized_pnl + commission + funding_fee),
        "open_position_symbols": [row["symbol"] for row in open_positions],
        "open_position_symbols_at_export": [row["symbol"] for row in open_positions],
        "unrealized_pnl_by_symbol_usdt": unrealized_by_symbol,
        "open_position_unrealized_pnl_usdt": rounded(open_unrealized),
        "estimated_total_pnl_usdt": rounded(realized_pnl + commission + funding_fee + open_unrealized),
        "order_quality": compute_order_quality(trades_by_symbol, orders_by_symbol),
    }


def render_markdown(summary):
    lines = [
        "# Binance Futures trade records — last 5 days",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Window start: `{summary['window_start']}`",
        f"- Window end: `{summary['window_end']}`",
        f"- Source: {summary['source']}",
        f"- Total records: `{summary['record_count']}`",
        f"- Symbols: `{', '.join(summary['symbols']) if summary['symbols'] else 'none'}`",
        f"- realized_pnl: `{summary['realized_pnl_usdt']:.8f}`",
        f"- commission: `{summary['commission_usdt']:.8f}`",
        f"- funding_fee: `{summary['funding_fee_usdt']:.8f}`",
        f"- net_realized_after_fee: `{summary['net_realized_after_fee_usdt']:.8f}`",
        f"- open_position_symbols: `{', '.join(summary['open_position_symbols']) if summary['open_position_symbols'] else 'none'}`",
        f"- open_position_unrealized_pnl: `{summary['open_position_unrealized_pnl_usdt']:.8f}`",
        f"- estimated_total_pnl: `{summary['estimated_total_pnl_usdt']:.8f}`",
        "",
        "## Source counts",
    ]
    for key, value in sorted(summary["source_counts"].items()):
        lines.append(f"- `{key}`: `{value}`")
    lines += ["", "## Income by type (USDT)"]
    for key, value in sorted(summary["income_by_type_usdt"].items()):
        lines.append(f"- `{key}`: `{value:.8f}`")
    lines += ["", "## Realized PnL by symbol (USDT)"]
    for key, value in sorted(summary["realized_pnl_by_symbol_usdt"].items()):
        lines.append(f"- `{key}`: `{value:.8f}`")
    lines += ["", "## Commission by symbol (USDT)"]
    for key, value in sorted(summary["commission_by_symbol_usdt"].items()):
        lines.append(f"- `{key}`: `{value:.8f}`")
    lines += ["", "## Open position unrealized PnL (USDT)"]
    for key, value in sorted(summary["unrealized_pnl_by_symbol_usdt"].items()):
        lines.append(f"- `{key}`: `{value:.8f}`")
    lines += ["", "## Order quality"]
    quality = summary["order_quality"]
    for key in ["market_order_count", "limit_order_count", "canceled_limit_count", "taker_trade_count", "maker_trade_count", "taker_fee_ratio", "fast_stop_count"]:
        lines.append(f"- `{key}`: `{quality.get(key)}`")
    lines.append("- `cancel_replace_count_by_symbol`:")
    for key, value in sorted(quality.get("cancel_replace_count_by_symbol", {}).items()):
        lines.append(f"  - `{key}`: `{value}`")
    lines.append("- `avg_hold_seconds_by_symbol`:")
    for key, value in sorted(quality.get("avg_hold_seconds_by_symbol", {}).items()):
        lines.append(f"  - `{key}`: `{value}`")
    lines.append("")
    return "\n".join(lines)


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

    summary = build_summary(
        now=now,
        start=start,
        records=records,
        income=income,
        positions=positions,
        trades_by_symbol=trades_by_symbol,
        orders_by_symbol=orders_by_symbol,
        symbols=symbols,
    )
    summary_json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_md_path.write_text(render_markdown(summary), encoding="utf-8")

    print(json.dumps({"jsonl": str(jsonl_path), "summary_json": str(summary_json_path), "summary_md": str(summary_md_path), "record_count": len(records), "symbols": symbols}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
