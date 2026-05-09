from __future__ import annotations

import argparse
import copy
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Sequence


SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import binance_futures_momentum_long as strategy


def build_strategy_args(argv: Sequence[str]) -> argparse.Namespace:
    args = strategy.apply_runtime_profile(strategy.parse_args(list(argv)))
    if strategy.is_binance_simulated_trading(args) and 'base_url' not in set(getattr(args, '_explicit_cli_dests', set()) or set()):
        args.base_url = 'https://testnet.binancefuture.com'
    return args


def build_client(args: argparse.Namespace) -> strategy.BinanceFuturesClient:
    api_key, api_secret = strategy.resolve_binance_api_credentials(args)
    return strategy.BinanceFuturesClient(
        base_url=getattr(args, 'base_url', 'https://fapi.binance.com'),
        api_key=api_key,
        api_secret=api_secret,
    )


def execute_with_cached_scan(
    client: strategy.BinanceFuturesClient,
    args: argparse.Namespace,
    scan_result: Dict[str, Any],
    best_candidate: Any,
    meta_map: Dict[str, Any],
) -> Dict[str, Any]:
    original_run_scan_once = strategy.run_scan_once

    def cached_run_scan_once(_client: Any, _args: argparse.Namespace, explicit_square_symbols: Any = None):
        return copy.deepcopy(scan_result), copy.deepcopy(best_candidate), copy.deepcopy(meta_map)

    strategy.run_scan_once = cached_run_scan_once
    try:
        return strategy.run_loop(client, args)
    finally:
        strategy.run_scan_once = original_run_scan_once


def sync_binance_positions_for_dashboard(client: strategy.BinanceFuturesClient, runtime_state_dir: str) -> Dict[str, Any]:
    store = strategy.RuntimeStateStore(runtime_state_dir)
    return strategy.reconcile_runtime_state(
        client,
        store,
        halt_on_orphan_position=False,
        repair_missing_protection_enabled=False,
    )


def sync_binance_account_for_dashboard(client: strategy.BinanceFuturesClient, runtime_state_dir: str) -> Dict[str, Any]:
    store = strategy.RuntimeStateStore(runtime_state_dir)
    account = client.signed_get('/fapi/v2/account', params={})
    assets = account.get('assets', []) if isinstance(account, dict) else []
    usdt = next((row for row in assets if str(row.get('asset', '')).upper() == 'USDT'), {}) if isinstance(assets, list) else {}
    positions = store.load_json('positions', {})
    unique: Dict[str, Dict[str, Any]] = {}
    if isinstance(positions, dict):
        for row in positions.values():
            if not isinstance(row, dict):
                continue
            key = str(row.get('position_key') or f"{row.get('symbol')}:{row.get('side')}")
            unique.setdefault(key, row)
    total_unrealized = sum(float(row.get('unrealized_pnl_usdt') or 0.0) for row in unique.values())
    total_notional = sum(abs(float(row.get('position_notional') or 0.0)) for row in unique.values())
    payload = {
        'asset': 'USDT',
        'total_wallet_balance': strategy._to_float(usdt.get('walletBalance')),
        'available_balance': strategy._to_float(usdt.get('availableBalance')),
        'cross_wallet_balance': strategy._to_float(usdt.get('crossWalletBalance')),
        'cross_unrealized_pnl': strategy._to_float(usdt.get('crossUnPnl')),
        'account_total_wallet_balance': strategy._to_float(account.get('totalWalletBalance')) if isinstance(account, dict) else 0.0,
        'account_total_margin_balance': strategy._to_float(account.get('totalMarginBalance')) if isinstance(account, dict) else 0.0,
        'account_available_balance': strategy._to_float(account.get('availableBalance')) if isinstance(account, dict) else 0.0,
        'account_total_unrealized_pnl': strategy._to_float(account.get('totalUnrealizedProfit')) if isinstance(account, dict) else 0.0,
        'positions_unrealized_pnl': total_unrealized,
        'positions_notional': total_notional,
        'open_position_count': len(unique),
        'updated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    }
    return store.save_json('account', payload)


def parse_args(argv: Any = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Shared scan loop for Binance Futures testnet execution.')
    parser.add_argument('--poll-interval-sec', type=int, default=60)
    parser.add_argument('--max-cycles', type=int, default=0, help='Set 0 for infinite loop.')
    parser.add_argument('--output-format', choices=['json', 'text'], default='json')
    parser.add_argument('--binance-runtime-state-dir', default='runtime-state-binance-futures-testnet')
    return parser.parse_args(argv)


def main(argv: Any = None) -> int:
    strategy.load_dotenv()
    args = parse_args(argv)
    binance_args = build_strategy_args([
        '--profile', 'binance-sim-active',
        '--live',
        '--binance-simulated-trading',
        '--auto-loop',
        '--max-scan-cycles', '0',
        '--poll-interval-sec', str(args.poll_interval_sec),
        '--output-format', 'json',
        '--runtime-state-dir', args.binance_runtime_state_dir,
    ])
    scan_client = build_client(binance_args)
    binance_client = build_client(binance_args)
    cycle_no = 0
    while True:
        cycle_no += 1
        scan_result, best_candidate, meta_map = strategy.run_scan_once(scan_client, binance_args)
        binance_result = execute_with_cached_scan(binance_client, binance_args, scan_result, best_candidate, meta_map)
        try:
            binance_position_sync = sync_binance_positions_for_dashboard(binance_client, args.binance_runtime_state_dir)
        except Exception as sync_exc:
            binance_position_sync = {'ok': False, 'error': str(sync_exc)}
        try:
            binance_account = sync_binance_account_for_dashboard(binance_client, args.binance_runtime_state_dir)
        except Exception as account_exc:
            binance_account = {'ok': False, 'error': str(account_exc)}
        payload = {
            'ok': True,
            'shared_scan': True,
            'cycle_no': cycle_no,
            'binance': binance_result,
            'binance_position_sync': binance_position_sync,
            'binance_account': binance_account,
        }
        print(json.dumps(payload, ensure_ascii=False) if args.output_format == 'json' else str(payload), flush=True)
        if args.max_cycles and cycle_no >= args.max_cycles:
            break
        time.sleep(max(1, int(args.poll_interval_sec)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
