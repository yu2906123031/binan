from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

strategy_module = __import__('binance_futures_momentum_long')
from lifecycle_snapshot import install_lifecycle_snapshot_hooks

install_lifecycle_snapshot_hooks(strategy_module)
strategy_main = strategy_module.main


if __name__ == '__main__':
    raise SystemExit(strategy_main())
