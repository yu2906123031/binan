from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

strategy_module = __import__('binance_futures_momentum_long')
request_manager_module = __import__('binance_request_manager')
slippage_calibration_policy_module = __import__('slippage_calibration_policy')
relative_selection_policy_module = __import__('relative_selection_policy')
install_startup_hardening = __import__('startup_bootstrap').install_startup_hardening

install_startup_hardening(
    strategy_module=strategy_module,
    request_manager_module=request_manager_module,
    slippage_calibration_policy_module=slippage_calibration_policy_module,
    relative_selection_policy_module=relative_selection_policy_module,
)
strategy_main = strategy_module.main


if __name__ == '__main__':
    raise SystemExit(strategy_main())
