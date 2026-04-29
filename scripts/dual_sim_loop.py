from __future__ import annotations

import argparse
import copy
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Sequence, Set


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


def build_okx_client(args: argparse.Namespace) -> strategy.OKXClient:
    api_key, api_secret, passphrase = strategy.resolve_okx_simulated_api_credentials()
    return strategy.OKXClient(
        base_url=getattr(args, 'okx_base_url', 'https://www.okx.com'),
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        simulated_trading=True,
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


def load_okx_skip_symbols(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return set()
    if isinstance(payload, list):
        return {str(item or '').upper() for item in payload if item}
    if isinstance(payload, dict):
        return {str(item or '').upper() for item in payload.get('symbols', []) if item}
    return set()


def save_okx_skip_symbols(path: Path, symbols: Set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'symbols': sorted(symbols)}, ensure_ascii=False, indent=2), encoding='utf-8')


def is_non_retryable_okx_symbol_error(error: str) -> bool:
    text = str(error or '')
    return any(code in text for code in ('51001', '51087', '51155'))


def is_okx_account_mode_error(error: str) -> bool:
    return '51010' in str(error or '')


def hide_okx_blacklisted_candidate(scan_result: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    adjusted = copy.deepcopy(scan_result)
    candidates = adjusted.get('candidates')
    if isinstance(candidates, list):
        adjusted['candidates'] = [row for row in candidates if str(row.get('symbol', '')).upper() != symbol]
        adjusted['candidate_count'] = len(adjusted['candidates'])
    selected = adjusted.get('selected')
    if isinstance(selected, dict) and str(selected.get('symbol', '')).upper() == symbol:
        adjusted['selected'] = None
        adjusted['selected_alert'] = None
    funnel = adjusted.get('funnel')
    if isinstance(funnel, dict):
        sample = list(funnel.get('okx_unavailable_symbols_sample') or [])
        if symbol not in sample:
            sample.append(symbol)
        funnel['okx_unavailable_symbols_sample'] = sample[-12:]
        funnel['okx_unavailable_symbol_count'] = int(funnel.get('okx_unavailable_symbol_count') or 0) + 1
    return adjusted


def hide_okx_execution(scan_result: Dict[str, Any], reason: str) -> Dict[str, Any]:
    adjusted = copy.deepcopy(scan_result)
    adjusted['selected'] = None
    adjusted['selected_alert'] = None
    adjusted['candidate_count'] = 0
    adjusted['candidates'] = []
    funnel = adjusted.get('funnel')
    if isinstance(funnel, dict):
        funnel['selected_risk_allowed_count'] = 0
        funnel['order_submitted_count'] = 0
    adjusted['okx_execution_paused_reason'] = reason
    return adjusted


def okx_execution_paused(runtime_state_dir: str) -> bool:
    store = strategy.RuntimeStateStore(runtime_state_dir)
    risk_state = strategy.load_risk_state(store)
    return bool(risk_state.get('halted')) and str(risk_state.get('halt_reason') or '') == 'okx_account_mode_not_supported'


def sync_okx_account_for_dashboard(client: strategy.OKXClient, runtime_state_dir: str) -> Dict[str, Any]:
    store = strategy.RuntimeStateStore(runtime_state_dir)
    snapshot = strategy.build_okx_account_snapshot(client)
    if snapshot.get('supports_swap_trading'):
        risk_state = strategy.load_risk_state(store)
        if str(risk_state.get('halt_reason') or '') == 'okx_account_mode_not_supported':
            risk_state['halted'] = False
            risk_state['halt_reason'] = ''
            store.save_json('risk_state', risk_state)
    else:
        risk_state = strategy.load_risk_state(store)
        risk_state['halted'] = True
        risk_state['halt_reason'] = 'okx_account_mode_not_supported'
        risk_state['halt_detail'] = snapshot.get('mode_help')
        store.save_json('risk_state', risk_state)
    return store.save_json('account', snapshot)


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
    parser = argparse.ArgumentParser(description='Shared scan loop for OKX simulated and Binance Futures testnet execution.')
    parser.add_argument('--poll-interval-sec', type=int, default=60)
    parser.add_argument('--max-cycles', type=int, default=0, help='Set 0 for infinite loop.')
    parser.add_argument('--output-format', choices=['json', 'text'], default='json')
    parser.add_argument('--okx-runtime-state-dir', default='runtime-state')
    parser.add_argument('--binance-runtime-state-dir', default='runtime-state-binance-futures-testnet')
    parser.add_argument('--okx-skip-symbols-file', default='runtime-state/okx-sim-skip-symbols.json')
    return parser.parse_args(argv)


def main(argv: Any = None) -> int:
    strategy.load_dotenv()
    args = parse_args(argv)
    okx_args = build_strategy_args([
        '--profile', 'okx-sim-active',
        '--live',
        '--okx-simulated-trading',
        '--auto-loop',
        '--max-scan-cycles', '0',
        '--poll-interval-sec', str(args.poll_interval_sec),
        '--output-format', 'json',
        '--runtime-state-dir', args.okx_runtime_state_dir,
    ])
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
    scan_client = build_client(okx_args)
    okx_client = build_client(okx_args)
    okx_private_client = build_okx_client(okx_args)
    binance_client = build_client(binance_args)
    okx_skip_path = Path(args.okx_skip_symbols_file)
    cycle_no = 0
    while True:
        cycle_no += 1
        try:
            okx_account = sync_okx_account_for_dashboard(okx_private_client, args.okx_runtime_state_dir)
        except Exception as account_exc:
            okx_account = {'ok': False, 'error': str(account_exc)}
        scan_result, best_candidate, meta_map = strategy.run_scan_once(scan_client, okx_args)
        okx_skip_symbols = load_okx_skip_symbols(okx_skip_path)
        okx_symbol = str(getattr(best_candidate, 'symbol', '') or '').upper() if best_candidate is not None else ''
        if okx_execution_paused(args.okx_runtime_state_dir):
            okx_scan_result = hide_okx_execution(scan_result, 'okx_account_mode_not_supported')
            okx_best_candidate = None
        elif okx_symbol and okx_symbol in okx_skip_symbols:
            okx_scan_result = hide_okx_blacklisted_candidate(scan_result, okx_symbol)
            okx_best_candidate = None
        else:
            okx_scan_result = scan_result
            okx_best_candidate = best_candidate
        okx_result = execute_with_cached_scan(okx_client, okx_args, okx_scan_result, okx_best_candidate, meta_map)
        okx_cycle = ((okx_result.get('cycles') or [{}])[0] if isinstance(okx_result, dict) else {})
        okx_error = ((okx_cycle or {}).get('live_execution_error') or {}).get('error', '')
        if is_non_retryable_okx_symbol_error(okx_error):
            failed_symbol = str(((okx_cycle or {}).get('live_execution_error') or {}).get('symbol') or okx_symbol).upper()
            if failed_symbol:
                okx_skip_symbols.add(failed_symbol)
                save_okx_skip_symbols(okx_skip_path, okx_skip_symbols)
        elif is_okx_account_mode_error(okx_error):
            okx_store = strategy.RuntimeStateStore(args.okx_runtime_state_dir)
            okx_store.save_json('risk_state', {
                **strategy.load_risk_state(okx_store),
                'halted': True,
                'halt_reason': 'okx_account_mode_not_supported',
            })
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
            'okx': okx_result,
            'binance': binance_result,
            'okx_account': okx_account,
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
