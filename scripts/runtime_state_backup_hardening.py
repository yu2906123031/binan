from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any, Dict, Tuple

BACKUP_GENERATIONS = 3


def _backup_path(path: Path, generation: int = 0) -> Path:
    suffix = '.bak' if generation <= 0 else f'.bak.{generation}'
    return path.with_name(f'{path.name}{suffix}')


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


def _rotate_backups(path: Path) -> None:
    for generation in range(BACKUP_GENERATIONS - 1, 0, -1):
        source = _backup_path(path, generation - 1)
        target = _backup_path(path, generation)
        if not source.exists():
            continue
        try:
            os.replace(source, target)
        except OSError:
            continue
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
        _rotate_backups(path)
        os.replace(temp, backup)
        _fsync_path_and_directory(backup)
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


def _load_backup(path: Path) -> Tuple[Any, Dict[str, Any] | None]:
    errors: list[dict[str, Any]] = []
    for generation in range(BACKUP_GENERATIONS):
        backup = _backup_path(path, generation)
        if not backup.exists():
            continue
        try:
            payload = json.loads(backup.read_text(encoding='utf-8'))
        except Exception as exc:
            errors.append({
                'backup_file': backup.name,
                'backup_error_type': exc.__class__.__name__,
                'backup_error': str(exc),
            })
            continue
        return payload, {
            'recovered_from_backup': True,
            'backup_file': backup.name,
            'backup_generation': generation,
        }
    if errors:
        result = dict(errors[0])
        if len(errors) > 1:
            result['backup_errors'] = errors
        return None, result
    return None, None


def install_runtime_state_backup_hardening(runtime_store_module: Any) -> None:
    store_cls = getattr(runtime_store_module, 'RuntimeStateStore', None)
    if store_cls is None:
        return

    original_atomic_write = getattr(store_cls, '_atomic_write_json', None)
    if callable(original_atomic_write) and not getattr(original_atomic_write, '_backup_hardened', False):
        write_backup_for_install = _write_backup

        def atomic_write_with_backup(self: Any, path: Path, payload: Any):
            result = original_atomic_write(self, path, payload)
            write_backup_for_install(Path(path), payload)
            return result

        atomic_write_with_backup._backup_hardened = True  # type: ignore[attr-defined]
        store_cls._atomic_write_json = atomic_write_with_backup

    original_load = getattr(store_cls, 'load', None)
    if callable(original_load) and not getattr(original_load, '_backup_hardened', False):
        load_backup_for_install = _load_backup

        def load_with_backup(self: Any):
            path = self._path()
            with self._file_lock(path):
                if not path.exists():
                    return {}
                try:
                    payload = json.loads(path.read_text(encoding='utf-8'))
                    return payload if isinstance(payload, dict) else {}
                except Exception:
                    backup_payload, backup_meta = load_backup_for_install(path)
            if backup_meta and backup_meta.get('recovered_from_backup') and isinstance(backup_payload, dict):
                return backup_payload
            return {}

        load_with_backup._backup_hardened = True  # type: ignore[attr-defined]
        store_cls.load = load_with_backup

    original_load_with_error = getattr(store_cls, 'load_json_with_error', None)
    if callable(original_load_with_error) and not getattr(original_load_with_error, '_backup_hardened', False):
        load_backup_for_json = _load_backup

        def load_json_with_backup(self: Any, name: str, default: Any = None):
            payload, error = original_load_with_error(self, name, default)
            if error is None:
                return payload, None

            path = self._json_path(name)
            with self._file_lock(path):
                backup_payload, backup_meta = load_backup_for_json(path)
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
