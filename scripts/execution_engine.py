from __future__ import annotations

import execution_engine_impl as _impl
from execution_feedback_hardening import install_execution_feedback_hardening

install_execution_feedback_hardening(_impl)
from execution_engine_impl import *  # noqa: E402,F403

# Keep underscored compatibility helpers available to runtime/tests while public
# names remain statically visible to type checkers through the import above.
for _name in dir(_impl):
    if _name.startswith('_') and not _name.startswith('__'):
        globals()[_name] = getattr(_impl, _name)
