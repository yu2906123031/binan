from __future__ import annotations

import runtime_store_impl as _impl
from runtime_state_backup_hardening import install_runtime_state_backup_hardening
from runtime_state_hardening import install_runtime_state_hardening

install_runtime_state_hardening(_impl)
install_runtime_state_backup_hardening(_impl)

# Re-export the existing runtime-store API. The canonical RuntimeStateStore class
# is now hardened at import time, so direct module consumers and main.py share
# identical durability and corruption-recovery semantics.
for _name in dir(_impl):
    if not _name.startswith('__'):
        globals()[_name] = getattr(_impl, _name)
