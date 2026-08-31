from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any, Dict, Tuple


def _backup_path(path: Path) -> Path:
    return path.with_name(f'{path.name}.bak')


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


def _write_backup(path: Path, payload: Any) -> None:
    backup = _backup_path(path)
    backup.parent.mkdir(parents=True, exist_ok=True)
    temp = backup.parent / f'.{backup.name}.{uuid.uuid4().hex}.tmp'
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    try:
        with temp.open('w', encoding='utf-8') as fh:
            fh.write(serialized)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temp, backup)
        _fsync_path_and_directory(backup)
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


def _load_backup(path: Path) -> Tuple[Any, Dict[str, Any] | None]:
    backup = _backup_path(path)
    if not backup.exists():
        return None, None
    try:
        payload = json.loads(backup.read_text(encoding='utf-8'))
    except Exception as exc:
        return None, {
            'backup_file': backup.name,
            'backup_error_type': exc.__class__.__name__,
            'backup_error': str(exc),
        }
    return payload, {
        'recovered_from_backup': True,
        'backup_file': backup.name,
    }


def install_runtime_state_backup_hardening(runtime_store_module: Any) -> None:
    store_cls = getattr(runtime_store_module, 'RuntimeStateStore', None)
    if store_cls is None:
        return

    original_atomic_write = getattr(store_cls, '_atomic_write_json', None)
    if callable(original_atomic_write) and not getattr(original_atomic_write, '_backup_hardened', False):
        def atomic_write_with_backup(self: Any, path: Path, payload: Any):
            result = original_atomic_write(self, path, payload)
            _write_backup(Path(path), payload)
            return result

        atomic_write_with_backup._backup_hardened = True  # type: ignore[attr-defined]
        store_cls._atomic_write_json = atomic_write_with_backup

    original_load_with_error = getattr(store_cls, 'load_json_with_error', None)
    if callable(original_load_with_error) and not getattr(original_load_with_error, '_backup_hardened', False):
        def load_json_with_backup(self: Any, name: str, default: Any = None):
            payload, error = original_load_with_error(self, name, default)
            if error is None:
                return payload, None

            path = self._json_path(name)
            with self._file_lock(path):
                backup_payload, backup_meta = _load_backup(path)
            if backup_meta is None or not backup_meta.get('recovered_from_backup'):
                combined = dict(error)
                if backup_meta:
                    combined.update(backup_meta)
                return default, combined

            if name == 'positions':
                migrated = runtime_store_module.migrate_positions_state(backup_payload)
                backup_payload = runtime_store_module.materialize_positions_state(
                    migrated,
                    include_legacy_alias=False,
                )
            recovery = dict(error)
            recovery.update(backup_meta)
            return backup_payload, recovery

        load_json_with_backup._backup_hardened = True  # type: ignore[attr-defined]
        store_cls.load_json_with_error = load_json_with_backup

    original_load_json = getattr(store_cls, 'load_json', None)
    if callable(original_load_json) and not getattr(original_load_json, '_backup_hardened', False):
        def load_json_with_backup_fallback(self: Any, name: str, default: Any = None):
            payload, _error = self.load_json_with_error(name, default)
            return payload

        load_json_with_backup_fallback._backup_hardened = True  # type: ignore[attr-defined]
        store_cls.load_json = load_json_with_backup_fallback
