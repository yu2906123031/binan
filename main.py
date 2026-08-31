from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

strategy_module = __import__('binance_futures_momentum_long')
runtime_store_module = __import__('runtime_store')
install_lifecycle_snapshot_hooks = __import__('lifecycle_snapshot').install_lifecycle_snapshot_hooks
install_reduce_only_risk_guard = __import__('risk_exit_guard').install_reduce_only_risk_guard
install_runtime_state_hardening = __import__('runtime_state_hardening').install_runtime_state_hardening
install_slippage_calibration_hooks = __import__('slippage_calibration_policy').install_slippage_calibration_hooks
install_runtime_state_hardening(runtime_store_module)
install_lifecycle_snapshot_hooks(strategy_module)
install_reduce_only_risk_guard(strategy_module)
install_slippage_calibration_hooks(strategy_module)
strategy_main = strategy_module.main


if __name__ == '__main__':
    raise SystemExit(strategy_main())
