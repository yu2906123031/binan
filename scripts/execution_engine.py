from __future__ import annotations

import execution_engine_impl as _impl
from execution_feedback_hardening import install_execution_feedback_hardening

install_execution_feedback_hardening(_impl)

# Preserve the historical public and helper API while making the canonical
# execution module return normalized submit-time, directional fill feedback.
for _name in dir(_impl):
    if not _name.startswith('__'):
        globals()[_name] = getattr(_impl, _name)
