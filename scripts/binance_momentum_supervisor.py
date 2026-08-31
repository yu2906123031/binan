from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

APP_HOME = Path(os.path.expanduser('~/.hermes/binance-futures-momentum-long'))
RUNTIME_STATE_DIR = APP_HOME / 'runtime-state'
BOT_SCRIPT = Path(__file__).resolve().parents[1] / 'main.py'
SQUARE_SYMBOLS_FILE = APP_HOME / 'square-symbols.txt'
EXTERNAL_SIGNAL_JSON = APP_HOME / 'external-signal.json'
RUNTIME_CONFIG_FILE = APP_HOME / 'runtime-config.json'
POSITIONS_STATE_FILE = RUNTIME_STATE_DIR / 'positions.json'
HALT_MARKER_FILE = RUNTIME_STATE_DIR / 'supervisor_halt.json'
SINGLE_INSTANCE_LOCK_FILE = RUNTIME_STATE_DIR / 'supervisor.lock'
SINGLE_INSTANCE_LOCK_HANDLE: Any = None
SHUTDOWN_REQUESTED = False
POLL_INTERVAL = 5.0
RESTART_DELAY = 5.0


class BinanceTemporaryBan(RuntimeError):
    def __init__(self, message: str, *, ban_until_ms: int = 0):
        super().__init__(message)
        self.ban_until_ms = int(ban_until_ms or 0)


def load_env(_path: Path) -> None:
    return None


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f'.{path.name}.tmp')
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(temp, path)


def resolve_strategy_python() -> str:
    configured = os.getenv('BINANCE_STRATEGY_PYTHON', '').strip()
    return configured or sys.executable


def fetch_account_state() -> dict[str, Any]:
    """Fail-safe default used when no exchange adapter is injected.

    Production launchers may replace this with the authenticated account probe;
    the supervisor itself never guesses account state from stale local files.
    """
    return {'positions': [], 'risk_positions': [], 'position_mismatch': False, 'open_orders': [], 'ignored_open_orders': []}


def _runtime_positions() -> dict[str, Any]:
    payload = _read_json(POSITIONS_STATE_FILE, {})
    if isinstance(payload, dict) and isinstance(payload.get('positions'), dict):
        payload = payload['positions']
    return payload if isinstance(payload, dict) else {}


def _position_symbol_side(row: dict[str, Any]) -> tuple[str, str]:
    symbol = str(row.get('symbol') or '').upper()
    side = str(row.get('position_side') or row.get('side') or '').upper()
    if side in {'SELL', 'SHORT'}:
        side = 'SHORT'
    else:
        side = 'LONG'
    return symbol, side


def _runtime_can_recover(account_state: dict[str, Any]) -> bool:
    positions = account_state.get('positions') or account_state.get('risk_positions') or []
    if not positions:
        return True
    runtime = _runtime_positions()
    if not runtime:
        return False
    runtime_rows = [row for row in runtime.values() if isinstance(row, dict)]
    for account_row in positions:
        if not isinstance(account_row, dict):
            continue
        symbol = str(account_row.get('symbol') or '').upper()
        amount = float(account_row.get('positionAmt') or 0.0)
        if not symbol or abs(amount) <= 0:
            continue
        side = 'SHORT' if amount < 0 else 'LONG'
        matched = False
        for row in runtime_rows:
            rsym, rside = _position_symbol_side(row)
            remaining = float(row.get('remaining_quantity') or row.get('quantity') or 0.0)
            status = str(row.get('status') or '').lower()
            if rsym == symbol and rside == side and remaining > 0 and status in {
                'recovery_pending', 'reconciled', 'monitoring', 'open', 'protected'
            }:
                matched = True
                break
        if not matched:
            return False
    return True


def _config(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = _read_json(RUNTIME_CONFIG_FILE, {})
    cfg = dict(cfg) if isinstance(cfg, dict) else {}
    if overrides:
        cfg.update(overrides)
    return cfg


def build_command(overrides: dict[str, Any] | None = None) -> list[str]:
    cfg = _config(overrides)
    defaults: dict[str, Any] = {
        'profile': 'five-usdt-scalp-v2',
        'risk_usdt': 1.0,
        'target_notional_usdt': 80.0,
        'min_notional_usdt': 60.0,
        'max_notional_usdt': 90.0,
        'tp1_profit_usdt': 1.5,
        'tp2_profit_usdt': 3.0,
        'tp1_close_pct': 0.4,
        'tp2_close_pct': 0.4,
        'max_open_positions': 3,
        'max_long_positions': 3,
        'max_short_positions': 3,
        'min_target_net_profit_usdt': 0.1,
        'max_loss_usdt': 1.5,
        'min_expected_rr': 0.7,
        'trigger_min_confirmations': 1,
        'micro_scalp_time_stop_sec': 1800,
        'daily_max_loss_usdt': 4.0,
        'max_consecutive_losses': 2,
        'consecutive_loss_pause_minutes': 180,
        'leverage': 10,
        'margin_type': 'ISOLATED',
        'trigger_relax_min_score': 78,
        'trigger_relax_min_points': 3,
        'execution_slippage_hard_veto_r': 450,
        'execution_slippage_risk_threshold_r': 450,
        'execution_timeout_seconds': 120,
        'maker_post_only_fallback_min_score': 92,
        'maker_post_only_fallback_min_liquidity_grade': 'A',
        'maker_post_only_fallback_max_spread_bps': 6,
        'scanner_kline_rest_fallback_min_interval_seconds': 0,
    }
    defaults.update(cfg)
    cmd = [resolve_strategy_python(), '-u', str(BOT_SCRIPT), '--live', '--auto-loop']
    flags = {
        'profile': '--profile', 'risk_usdt': '--risk-usdt',
        'target_notional_usdt': '--target-notional-usdt', 'min_notional_usdt': '--min-notional-usdt',
        'max_notional_usdt': '--max-notional-usdt', 'tp1_profit_usdt': '--tp1-profit-usdt',
        'tp2_profit_usdt': '--tp2-profit-usdt', 'tp1_close_pct': '--tp1-close-pct',
        'tp2_close_pct': '--tp2-close-pct', 'max_open_positions': '--max-open-positions',
        'max_long_positions': '--max-long-positions', 'max_short_positions': '--max-short-positions',
        'min_target_net_profit_usdt': '--min-target-net-profit-usdt', 'max_loss_usdt': '--max-loss-usdt',
        'min_expected_rr': '--min-expected-rr', 'trigger_min_confirmations': '--trigger-min-confirmations',
        'micro_scalp_time_stop_sec': '--micro-scalp-time-stop-sec', 'daily_max_loss_usdt': '--daily-max-loss-usdt',
        'max_consecutive_losses': '--max-consecutive-losses', 'consecutive_loss_pause_minutes': '--consecutive-loss-pause-minutes',
        'leverage': '--leverage', 'margin_type': '--margin-type', 'trigger_relax_min_score': '--trigger-relax-min-score',
        'trigger_relax_min_points': '--trigger-relax-min-points', 'execution_slippage_hard_veto_r': '--execution-slippage-hard-veto-r',
        'execution_slippage_risk_threshold_r': '--execution-slippage-risk-threshold-r', 'execution_timeout_seconds': '--execution-timeout-seconds',
        'maker_post_only_fallback_min_score': '--maker-post-only-fallback-min-score',
        'maker_post_only_fallback_min_liquidity_grade': '--maker-post-only-fallback-min-liquidity-grade',
        'maker_post_only_fallback_max_spread_bps': '--maker-post-only-fallback-max-spread-bps',
        'scanner_kline_rest_fallback_min_interval_seconds': '--scanner-kline-rest-fallback-min-interval-seconds',
    }
    for key, flag in flags.items():
        cmd.extend([flag, str(defaults[key])])
    if bool(defaults.get('allow_post_only_taker_fallback')):
        cmd.append('--allow-post-only-taker-fallback')
    if bool(defaults.get('scanner_rest_fallback')):
        cmd.extend(['--scanner-rest-fallback', '--scanner-kline-rest-fallback'])
    return cmd


def classify_child_exit(*, return_code: int, consecutive_failures: int) -> dict[str, Any]:
    if SHUTDOWN_REQUESTED or return_code in {-signal.SIGTERM, -signal.SIGINT, 0}:
        return {'action': 'exit', 'reason': 'shutdown_requested', 'consecutive_failures': consecutive_failures}
    failures = max(int(consecutive_failures or 0), 0) + 1
    return {'action': 'restart', 'reason': 'child_failure', 'consecutive_failures': failures}


def _emit_block(reason: str, **extra: Any) -> None:
    payload = {'reason': reason, **extra}
    print(json.dumps(payload, ensure_ascii=False))
    _write_json(HALT_MARKER_FILE, payload)


def _install_signal_handlers() -> None:
    def handler(_signum: int, _frame: Any) -> None:
        global SHUTDOWN_REQUESTED
        SHUTDOWN_REQUESTED = True
    try:
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)
    except Exception:
        pass


def main() -> int:
    _install_signal_handlers()
    RUNTIME_STATE_DIR.mkdir(parents=True, exist_ok=True)
    consecutive_failures = 0
    while not SHUTDOWN_REQUESTED:
        try:
            account = fetch_account_state()
        except BinanceTemporaryBan as exc:
            now_ms = int(time.time() * 1000)
            remaining = max((exc.ban_until_ms - now_ms) / 1000.0, 60.0) if exc.ban_until_ms else 60.0
            _emit_block('binance_temporary_ban', ban_until_ms=exc.ban_until_ms)
            time.sleep(remaining)
            continue
        if bool(account.get('position_mismatch')) or not _runtime_can_recover(account):
            _emit_block('account_not_flat')
            time.sleep(max(POLL_INTERVAL, 0.0))
            continue
        try:
            HALT_MARKER_FILE.unlink(missing_ok=True)
        except OSError:
            pass
        child = subprocess.Popen(build_command())
        rc = child.wait()
        decision = classify_child_exit(return_code=rc, consecutive_failures=consecutive_failures)
        consecutive_failures = int(decision['consecutive_failures'])
        if decision['action'] == 'exit':
            return 0
        time.sleep(max(RESTART_DELAY, 0.0))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
