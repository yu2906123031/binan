from __future__ import annotations

import trade_bucket_analysis_impl as _impl
from trade_bucket_analysis_impl import *  # noqa: F403
from trade_bucket_slippage_hardening import install_trade_bucket_slippage_hardening

install_trade_bucket_slippage_hardening(_impl)

# Keep underscored compatibility helpers available while public names remain
# statically visible to type checkers through the import above.
for _name in dir(_impl):
    if _name.startswith('_') and not _name.startswith('__'):
        globals()[_name] = getattr(_impl, _name)


def main() -> int:
    return int(_impl.main())


if __name__ == '__main__':
    raise SystemExit(main())
