from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

strategy_main = __import__('binance_futures_momentum_long').main


if __name__ == '__main__':
    raise SystemExit(strategy_main())
