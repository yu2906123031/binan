from __future__ import annotations

import trade_bucket_analysis as trade_bucket_module
from trade_bucket_slippage_hardening import install_trade_bucket_slippage_hardening


def main() -> int:
    install_trade_bucket_slippage_hardening(trade_bucket_module)
    return int(trade_bucket_module.main())


if __name__ == '__main__':
    raise SystemExit(main())
