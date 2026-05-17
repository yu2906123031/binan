from __future__ import annotations

import datetime
import contextlib
import fcntl
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

CANONICAL_RUNTIME_STATE_DIR = Path('~/.hermes/binance-futures-momentum-long/runtime-state').expanduser()
LEGACY_RUNTIME_STATE_DIR = Path('/root/runtime-state')

POSITION_SIDE_LONG = 'LONG'
POSITION_SIDE_SHORT = 'SHORT'
TRADE_SIDE_LONG = 'long'
TRADE_SIDE_SHORT = 'short'


def validate_runtime_state_layout(
    configured_dir: Any,
    canonical_dir: Path = CANONICAL_RUNTIME_STATE_DIR,
    legacy_dir: Path = LEGACY_RUNTIME_STATE_DIR,
) -> Path:
    canonical_path = Path(canonical_dir).expanduser()
    canonical_path.mkdir(parents=True, exist_ok=True)
    if not canonical_path.is_dir():
        raise RuntimeError(f'canonical runtime-state path is not a directory: {canonical_path}')
    if not os.access(canonical_path, os.W_OK):
        raise RuntimeError(f'canonical runtime-state path is not writable: {canonical_path}')

    legacy_path = Path(legacy_dir).expanduser()
    if legacy_path.is_symlink():
        try:
            legacy_resolved = legacy_path.resolve(strict=True)
        except FileNotFoundError as exc:
            raise RuntimeError(f'legacy runtime-state symlink is broken: {legacy_path}') from exc
        if legacy_resolved != canonical_path.resolve():
            raise RuntimeError(
                f'legacy runtime-state symlink points to {legacy_resolved}, expected {canonical_path}'
            )
    elif legacy_path.exists():
        raise RuntimeError(f'legacy runtime-state path is a real directory or file: {legacy_path}')

    configured_path = Path(configured_dir).expanduser()
    try:
        configured_resolved = configured_path.resolve(strict=False)
    except RuntimeError as exc:
        raise RuntimeError(f'configured runtime-state path could not be resolved: {configured_path}') from exc
    if configured_resolved != canonical_path.resolve():
        raise RuntimeError(
            f'runtime-state-dir must resolve to canonical path {canonical_path}; got {configured_path}'
        )
    return canonical_path


RebuildTradeManagementPlan = Callable[[Dict[str, Any], Any], Any]


def normalize_trade_side(side: Any, default: str = TRADE_SIDE_LONG) -> str:
    normalized = str(side or '').strip().lower()
    return TRADE_SIDE_SHORT if normalized == TRADE_SIDE_SHORT else default


def position_side_to_trade_side(side: Any, default: str = TRADE_SIDE_LONG) -> str:
    normalized = normalize_position_side(side, POSITION_SIDE_SHORT if str(default).lower() == TRADE_SIDE_SHORT else POSITION_SIDE_LONG)
    return TRADE_SIDE_SHORT if normalized == POSITION_SIDE_SHORT else TRADE_SIDE_LONG


def trade_side_to_position_side(side: Any, default: str = POSITION_SIDE_LONG) -> str:
    normalized = normalize_trade_side(side, TRADE_SIDE_SHORT if str(default).upper() == POSITION_SIDE_SHORT else TRADE_SIDE_LONG)
    return POSITION_SIDE_SHORT if normalized == TRADE_SIDE_SHORT else POSITION_SIDE_LONG


def normalize_position_side(side: Any, default: str = POSITION_SIDE_LONG) -> str:
    normalized = str(side or '').strip().upper()
    fallback = str(default or POSITION_SIDE_LONG).strip().upper()
    if normalized in {POSITION_SIDE_SHORT, TRADE_SIDE_SHORT.upper(), 'SELL'}:
        return POSITION_SIDE_SHORT
    if normalized in {POSITION_SIDE_LONG, TRADE_SIDE_LONG.upper(), 'BUY'}:
        return POSITION_SIDE_LONG
    return POSITION_SIDE_SHORT if fallback in {POSITION_SIDE_SHORT, TRADE_SIDE_SHORT.upper(), 'SELL'} else POSITION_SIDE_LONG


def build_position_key(symbol: str, side: Any = POSITION_SIDE_LONG) -> str:
    return f"{str(symbol or '').upper()}:{normalize_position_side(side)}"


def split_position_key(position_key: Any) -> Tuple[str, str]:
    text = str(position_key or '').strip().upper()
    if ':' in text:
        symbol, side = text.split(':', 1)
        return symbol, normalize_position_side(side)
    return text, POSITION_SIDE_LONG


def is_legacy_position_key(position_key: Any) -> bool:
    text = str(position_key or '').strip().upper()
    return bool(text) and ':' not in text


def position_matches_symbol_side(position: Any, symbol: str, side: Any = POSITION_SIDE_LONG) -> bool:
    if not isinstance(position, dict):
        return False
    return str(position.get('symbol') or '').upper() == str(symbol or '').upper() and normalize_position_side(position.get('side')) == normalize_position_side(side)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def upsert_position_record(positions_state: Any, position: Dict[str, Any], key: Optional[str] = None) -> Tuple[Dict[str, Any], str]:
    if not isinstance(positions_state, dict):
        positions_state = {}
    normalized = dict(position or {})
    symbol = str(normalized.get('symbol') or '').upper()
    position_side = normalize_position_side(normalized.get('position_side') or normalized.get('side'))
    trade_side = normalize_trade_side(normalized.get('side'), position_side_to_trade_side(position_side))
    position_key = build_position_key(symbol, position_side)
    normalized['symbol'] = symbol
    normalized['side'] = trade_side
    normalized['position_side'] = position_side
    normalized['position_key'] = position_key
    quantity = _to_float(normalized.get('quantity'), default=0.0)
    if 'remaining_quantity' not in normalized:
        normalized['remaining_quantity'] = quantity
    normalized.setdefault('current_stop_price', normalized.get('stop_price'))
    normalized.setdefault('moved_to_breakeven', False)
    normalized.setdefault('tp1_hit', False)
    normalized.setdefault('tp2_hit', False)
    normalized.setdefault('highest_price_seen', None)
    normalized.setdefault('lowest_price_seen', None)
    normalized.setdefault('opened_at', None)
    normalized.setdefault('first_1r_at', None)
    normalized.setdefault('realized_r', 0.0)
    normalized.setdefault('mfe_r', 0.0)
    normalized.setdefault('mae_r', 0.0)
    normalized.setdefault('time_to_1r', None)
    normalized.setdefault('time_to_1r_minutes', None)
    normalized.setdefault('time_in_trade_minutes', None)
    normalized.setdefault('selected_score', None)
    normalized.setdefault('selected_state', '')
    normalized.setdefault('selected_alert_tier', '')
    normalized.setdefault('trigger_class', '')
    normalized.setdefault('score_decile', '')
    normalized.setdefault('market_regime_label', '')
    normalized.setdefault('market_regime_multiplier', 0.0)
    normalized.setdefault('monitor_mode', 'trade_management')

    explicit_key = str(key or '').strip().upper()
    key_candidates: List[str] = []
    for candidate in [explicit_key, position_key, symbol]:
        candidate_text = str(candidate or '').strip().upper()
        if candidate_text and candidate_text not in key_candidates:
            key_candidates.append(candidate_text)
    for existing_key, existing_value in list(positions_state.items()):
        existing_key_text = str(existing_key or '').strip().upper()
        if existing_key_text and existing_key_text not in key_candidates and position_matches_symbol_side(existing_value, symbol, position_side):
            key_candidates.append(existing_key_text)

    for candidate_key in list(key_candidates):
        existing = positions_state.get(candidate_key)
        if isinstance(existing, dict) and position_matches_symbol_side(existing, symbol, position_side):
            positions_state.pop(candidate_key, None)

    positions_state[position_key] = normalized
    return positions_state, position_key


def get_position_storage_aliases(position_key: str, symbol: Any, side: Any, prefer_legacy: bool = False, include_legacy_alias: bool = True) -> List[str]:
    aliases: List[str] = []
    canonical = str(position_key or '').upper()
    symbol_text = str(symbol or '').upper()
    normalized_side = normalize_position_side(side)
    if prefer_legacy and symbol_text and normalized_side == POSITION_SIDE_LONG and include_legacy_alias:
        aliases.append(symbol_text)
    if canonical:
        aliases.append(canonical)
    if symbol_text and normalized_side == POSITION_SIDE_LONG and include_legacy_alias:
        aliases.append(symbol_text)
    unique_aliases: List[str] = []
    seen = set()
    for alias in aliases:
        if alias and alias not in seen:
            unique_aliases.append(alias)
            seen.add(alias)
    return unique_aliases


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _isoformat_utc(value: datetime.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')


def _parse_iso8601_utc(value: Any) -> Optional[datetime.datetime]:
    text = str(value or '').strip()
    if not text:
        return None
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)


def restore_position_lifecycle_fields(
    position: Dict[str, Any],
    args: Any = None,
    rebuild_trade_management_plan_from_position: Optional[RebuildTradeManagementPlan] = None,
) -> Dict[str, Any]:
    restored = dict(position or {})
    entry_price = _to_float(restored.get('entry_price'))
    current_stop_price = _to_float(restored.get('current_stop_price') or restored.get('stop_price'))
    quantity = abs(_to_float(restored.get('quantity') or restored.get('filled_quantity') or restored.get('remaining_quantity')))
    position_side = normalize_position_side(restored.get('side') or restored.get('position_side'))
    if entry_price <= 0 or quantity <= 0:
        return restored

    protection_status = str(restored.get('protection_status') or '').lower()
    has_valid_stop = current_stop_price > 0 and abs(current_stop_price - entry_price) > 1e-12
    if current_stop_price <= 0:
        current_stop_price = entry_price
        restored['current_stop_price'] = entry_price
        restored.setdefault('stop_price', entry_price)

    plan_payload = restored.get('trade_management_plan') if isinstance(restored.get('trade_management_plan'), dict) else None
    plan_stop_price = _to_float(plan_payload.get('stop_price')) if plan_payload else 0.0
    plan_risk = abs(_to_float(plan_payload.get('initial_risk_per_unit'))) if plan_payload else 0.0
    plan_side = normalize_position_side((plan_payload or {}).get('position_side') or (plan_payload or {}).get('side') or position_side)
    plan_invalid = (
        plan_payload is None
        or not plan_payload
        or plan_side != position_side
        or plan_stop_price <= 0
        or abs(plan_stop_price - entry_price) <= 1e-12
        or plan_risk <= 0
    )

    rebuilt_plan = None
    rebuild_error = None
    if plan_invalid and args is not None and has_valid_stop and rebuild_trade_management_plan_from_position is not None:
        try:
            rebuilt_plan = rebuild_trade_management_plan_from_position({
                **restored,
                'position_side': position_side,
                'side': position_side_to_trade_side(position_side),
                'stop_price': current_stop_price,
                'current_stop_price': current_stop_price,
                'quantity': quantity,
                'remaining_quantity': abs(_to_float(restored.get('remaining_quantity'), default=quantity)) or quantity,
            }, args)
        except ValueError as exc:
            rebuild_error = str(exc)

    if rebuilt_plan is not None:
        normalized_plan = dict(rebuilt_plan) if isinstance(rebuilt_plan, dict) else dict(getattr(rebuilt_plan, '__dict__', {}))
        restored['trade_management_plan'] = normalized_plan
        restored['trade_management_plan']['side'] = position_side_to_trade_side(position_side)
        restored['trade_management_plan']['position_side'] = position_side
        restored.pop('recovery_incomplete', None)
        restored.pop('recovery_reason', None)
        if restored.get('status') in {'protected_recovery_pending', 'recovery_pending'}:
            restored['status'] = 'monitoring' if protection_status == 'protected' else restored.get('status')
    elif plan_invalid:
        restored['recovery_incomplete'] = True
        restored['recovery_reason'] = 'missing_valid_stop_distance'
        if rebuild_error:
            restored['recovery_detail'] = rebuild_error
        restored['trade_management_plan'] = None
        restored['current_stop_price'] = current_stop_price
        restored['stop_price'] = current_stop_price
        if protection_status == 'protected':
            restored['status'] = 'protected_recovery_pending'
        else:
            restored['status'] = 'recovery_pending'
    else:
        normalized_plan = dict(plan_payload or {})
        normalized_plan['side'] = position_side_to_trade_side(position_side)
        normalized_plan['position_side'] = position_side
        restored['trade_management_plan'] = normalized_plan
        restored.pop('recovery_incomplete', None)
        restored.pop('recovery_reason', None)
        restored.pop('recovery_detail', None)

    if not restored.get('opened_at'):
        restored['opened_at'] = _isoformat_utc(_utc_now())
    restored['monitor_mode'] = restored.get('monitor_mode') or 'trade_management'
    restored['status'] = restored.get('status') or 'monitoring'
    return restored


def materialize_positions_state(positions_state: Dict[str, Any], original_keys: Optional[Dict[str, str]] = None, include_legacy_alias: bool = False) -> Dict[str, Any]:
    materialized: Dict[str, Any] = {}
    key_hints = dict(original_keys or {})
    for position_key, tracked in list((positions_state or {}).items()):
        if not isinstance(tracked, dict):
            continue
        symbol = str(tracked.get('symbol') or split_position_key(position_key)[0]).upper()
        side = normalize_position_side(tracked.get('position_side') or tracked.get('side') or split_position_key(position_key)[1])
        canonical_key = build_position_key(symbol, side)
        normalized = restore_position_lifecycle_fields(dict(tracked))
        normalized['symbol'] = symbol
        normalized['side'] = position_side_to_trade_side(side)
        normalized['position_side'] = side
        normalized['position_key'] = canonical_key
        plan_payload = normalized.get('trade_management_plan')
        if isinstance(plan_payload, dict) and plan_payload:
            plan_payload = dict(plan_payload)
            plan_payload['position_side'] = side
            plan_payload['side'] = position_side_to_trade_side(side)
            normalized['trade_management_plan'] = plan_payload
        materialized[canonical_key] = normalized
        prefer_legacy = is_legacy_position_key(key_hints.get(canonical_key))
        if include_legacy_alias:
            for alias in get_position_storage_aliases(canonical_key, symbol, side, prefer_legacy=prefer_legacy, include_legacy_alias=True):
                if alias != canonical_key:
                    materialized[alias] = normalized
    return materialized


def migrate_positions_state(positions_state: Any) -> Dict[str, Any]:
    if not isinstance(positions_state, dict):
        return {}
    migrated: Dict[str, Any] = {}
    for key, value in positions_state.items():
        if not isinstance(value, dict):
            continue
        raw_symbol = str(value.get('symbol') or '').upper()
        raw_side = value.get('side')
        if not raw_symbol:
            raw_symbol, inferred_side = split_position_key(key)
            raw_side = raw_side or inferred_side
        migrated, _ = upsert_position_record(migrated, dict(value, symbol=raw_symbol, side=normalize_position_side(raw_side)), key=str(key or ''))
    return migrated


def normalize_runtime_event_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(payload or {})
    symbol = str(row.get('symbol') or '').upper()
    inferred_symbol, inferred_side = split_position_key(row.get('position_key')) if row.get('position_key') else ('', POSITION_SIDE_LONG)
    if not symbol:
        symbol = inferred_symbol
    position_side = normalize_position_side(row.get('position_side') or row.get('side'), inferred_side if inferred_symbol else POSITION_SIDE_LONG)
    if symbol:
        row['symbol'] = symbol
        row['side'] = position_side
        row['position_side'] = position_side
        row['position_key'] = build_position_key(symbol, position_side)
    return row


@dataclass
class RuntimeStateStore:
    runtime_state_dir: str

    def _dir(self) -> Path:
        return Path(self.runtime_state_dir).expanduser()

    def _path(self) -> Path:
        return self._dir() / 'runtime_state.json'

    def _json_path(self, name: str) -> Path:
        return self._dir() / f'{name}.json'

    def _events_path(self) -> Path:
        return self._dir() / 'events.jsonl'

    def _lock_path(self, path: Path) -> Path:
        return path.parent / f'.{path.name}.lock'

    @contextlib.contextmanager
    def _file_lock(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock_path(path).open('a+', encoding='utf-8') as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

    def _atomic_write_json(self, path: Path, payload: Any) -> Any:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.parent / f'.{path.name}.{uuid.uuid4().hex}.tmp'
        serialized = json.dumps(payload, ensure_ascii=False, indent=2)
        try:
            temp_path.write_text(serialized, encoding='utf-8')
            os.replace(temp_path, path)
        finally:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception:
                pass
        return payload

    def load(self) -> Dict[str, Any]:
        path = self._path()
        with self._file_lock(path):
            if not path.exists():
                return {}
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except Exception:
                return {}

    def save(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        path = self._path()
        with self._file_lock(path):
            return self._atomic_write_json(path, payload)

    def load_json(self, name: str, default: Any = None) -> Any:
        path = self._json_path(name)
        with self._file_lock(path):
            if not path.exists():
                return default
            try:
                payload = json.loads(path.read_text(encoding='utf-8'))
            except Exception:
                return default
        if name == 'positions':
            migrated = migrate_positions_state(payload)
            materialized = materialize_positions_state(migrated, include_legacy_alias=False)
            return materialized
        return payload

    def load_json_with_error(self, name: str, default: Any = None) -> Tuple[Any, Optional[Dict[str, Any]]]:
        path = self._json_path(name)
        with self._file_lock(path):
            if not path.exists():
                return default, None
            try:
                payload = json.loads(path.read_text(encoding='utf-8'))
            except Exception as exc:
                return default, {
                    'state_key': str(name or ''),
                    'state_file': path.name,
                    'error_type': exc.__class__.__name__,
                    'error': str(exc),
                }
        if name == 'positions':
            migrated = migrate_positions_state(payload)
            materialized = materialize_positions_state(migrated, include_legacy_alias=False)
            return materialized, None
        return payload, None

    def save_json(self, name: str, payload: Any) -> Any:
        path = self._json_path(name)
        normalized_payload = materialize_positions_state(migrate_positions_state(payload), include_legacy_alias=False) if name == 'positions' else payload
        with self._file_lock(path):
            return self._atomic_write_json(path, normalized_payload)

    def append_event(self, event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        path = self._events_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        row = {'event_type': event_type, 'recorded_at': _isoformat_utc(_utc_now()), **normalize_runtime_event_payload(payload or {})}
        with self._file_lock(path):
            with path.open('a', encoding='utf-8') as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + '\n')
                fh.flush()
                os.fsync(fh.fileno())
        return row

    def read_events(self, limit: int = 1000) -> List[Dict[str, Any]]:
        path = self._events_path()
        if not path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        max_rows = max(int(limit or 0), 1)
        try:
            with path.open('r', encoding='utf-8') as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(row, dict):
                        rows.append(row)
        except Exception:
            return []
        if len(rows) <= max_rows:
            return rows
        return rows[-max_rows:]


def load_positions_state(store: RuntimeStateStore) -> Dict[str, Any]:
    positions_state = store.load_json('positions', {})
    return positions_state if isinstance(positions_state, dict) else {}


def save_positions_state(store: RuntimeStateStore, positions_state: Any) -> Dict[str, Any]:
    normalized = positions_state if isinstance(positions_state, dict) else {}
    store.save_json('positions', normalized)
    persisted = store.load_json('positions', {})
    return persisted if isinstance(persisted, dict) else {}
