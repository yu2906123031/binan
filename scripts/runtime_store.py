from __future__ import annotations

import runtime_store_impl as _impl
from runtime_state_backup_hardening import install_runtime_state_backup_hardening
from runtime_state_hardening import install_runtime_state_hardening

install_runtime_state_hardening(_impl)
install_runtime_state_backup_hardening(_impl)
from runtime_store_impl import *  # noqa: E402,F403

# Keep underscored compatibility helpers available while public names are
# statically visible to type checkers through the import above.
for _name in dir(_impl):
    if _name.startswith('_') and not _name.startswith('__'):
        globals()[_name] = getattr(_impl, _name)
