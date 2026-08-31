from __future__ import annotations

import datetime
import os
from pathlib import Path
from typing import Any, Dict


def _coerce_opened_at(value: Any) -> str | None:
    if value in (None, ''):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000.0
        if number <= 0:
            return None
        return datetime.datetime.fromtimestamp(number, datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return _coerce_opened_at(float(text))
    return text


def infer_opened_at(position: Dict[str, Any]) -> str | None:
    for key in (
        'opened_at',
        'entry_fill_recorded_at',
        'entry_recorded_at',
        'prediction_recorded_at',
        'entry_time',
        'entry_timestamp',
        'created_at',
    ):
        inferred = _coerce_opened_at(position.get(key))
        if inferred:
            return inferred
    snapshot = position.get('entry_prediction_snapshot')
    if isinstance(snapshot, dict):
        for key in ('entry_fill_recorded_at', 'prediction_recorded_at', 'recorded_at'):
            inferred = _coerce_opened_at(snapshot.get(key))
            if inferred:
                return inferred
    return None


def _fsync_path_and_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        fd = None
    if fd is not None:
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    try:
        dir_fd = os.open(path.parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def install_runtime_state_hardening(runtime_store_module: Any) -> None:
    store_cls = getattr(runtime_store_module, 'RuntimeStateStore', None)
    if store_cls is not None:
        original_atomic_write = getattr(store_cls, '_atomic_write_json', None)
        if callable(original_atomic_write) and not getattr(original_atomic_write, '_durability_hardened', False):
            fsync_path_and_directory = _fsync_path_and_directory

            def durable_atomic_write(
                self: Any,
                path: Path,
                payload: Any,
                _fsync_path_and_directory=fsync_path_and_directory,
            ):
                result = original_atomic_write(self, path, payload)
                _fsync_path_and_directory(Path(path))
                return result

            durable_atomic_write._durability_hardened = True  # type: ignore[attr-defined]
            store_cls._atomic_write_json = durable_atomic_write

    original_restore = getattr(runtime_store_module, 'restore_position_lifecycle_fields', None)
    if callable(original_restore) and not getattr(original_restore, '_opened_at_hardened', False):
        infer_opened_at_for_install = infer_opened_at

        def restore_with_opened_at(position: Dict[str, Any], *args: Any, **kwargs: Any):
            normalized = dict(position or {})
            if not normalized.get('opened_at'):
                inferred = infer_opened_at_for_install(normalized)
                if inferred:
                    normalized['opened_at'] = inferred
            return original_restore(normalized, *args, **kwargs)

        restore_with_opened_at._opened_at_hardened = True  # type: ignore[attr-defined]
        runtime_store_module.restore_position_lifecycle_fields = restore_with_opened_at
