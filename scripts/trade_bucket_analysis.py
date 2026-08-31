from __future__ import annotations

import trade_bucket_analysis_impl as _impl
from trade_bucket_slippage_hardening import install_trade_bucket_slippage_hardening

install_trade_bucket_slippage_hardening(_impl)

# Preserve the historical module API, including helper names used by tests and
# calibration code, while keeping the implementation in an unwrapped module.
for _name in dir(_impl):
    if not _name.startswith('__'):
        globals()[_name] = getattr(_impl, _name)


def main() -> int:
    return int(_impl.main())


if __name__ == '__main__':
    raise SystemExit(main())
