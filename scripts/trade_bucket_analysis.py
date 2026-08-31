from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import trade_bucket_analysis_impl as _impl
from trade_bucket_slippage_hardening import install_trade_bucket_slippage_hardening

install_trade_bucket_slippage_hardening(_impl)
from trade_bucket_analysis_impl import *  # noqa: E402,F403

# Explicit canonical API surface.  The preserved implementation remains
# regression-locked while callers/type-checkers depend on these stable exports.
def load_events(events_path: Any, limit: int = 5000) -> List[Dict[str, Any]]:
    return _impl.load_events(events_path, limit=limit)


def build_trade_bucket_analysis_payload(
    rows: Iterable[Dict[str, Any]],
    symbol: str = '',
    lookback_days: int = 0,
    now: Optional[Any] = None,
) -> Dict[str, Any]:
    return _impl.build_trade_bucket_analysis_payload(
        rows,
        symbol=symbol,
        lookback_days=lookback_days,
        now=now,
    )


def main() -> int:
    return int(_impl.main())


# Keep underscored compatibility helpers available to runtime/tests.
for _name in dir(_impl):
    if _name.startswith('_') and not _name.startswith('__'):
        globals()[_name] = getattr(_impl, _name)


if __name__ == '__main__':
    raise SystemExit(main())
