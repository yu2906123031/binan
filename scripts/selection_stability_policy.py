from __future__ import annotations

import threading
from typing import Any, Iterable

_STATE = threading.local()
STATE_NAME = 'selection-stability'
MAX_MISSING_SCANS = 12
MAX_TRACKED_SYMBOLS = 300


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _symbol(candidate: Any) -> str:
    return str(getattr(candidate, 'symbol', '') or '').strip().upper()


def _normalize_state(raw: Any) -> dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    try:
        scan_id = max(int(payload.get('scan_id') or 0), 0)
    except (TypeError, ValueError):
        scan_id = 0
    symbols: dict[str, dict[str, Any]] = {}
    source = payload.get('symbols')
    if isinstance(source, dict):
        for raw_symbol, raw_row in source.items():
            symbol = str(raw_symbol or '').strip().upper()
            if not symbol or not isinstance(raw_row, dict):
                continue
            try:
                last_seen_scan = max(int(raw_row.get('last_seen_scan') or 0), 0)
                streak = max(int(raw_row.get('streak') or 0), 0)
                appearances = max(int(raw_row.get('appearances') or 0), 0)
            except (TypeError, ValueError):
                continue
            ema = _num(raw_row.get('ema_percentile'))
            if ema is not None:
                ema = max(0.0, min(ema, 1.0))
            symbols[symbol] = {
                'last_seen_scan': last_seen_scan,
                'streak': streak,
                'appearances': appearances,
                'ema_percentile': ema,
            }
    return {'scan_id': scan_id, 'symbols': symbols}


def _projected_streak(history: dict[str, Any] | None, scan_id: int) -> int:
    if not history:
        return 1
    try:
        last_seen = int(history.get('last_seen_scan') or 0)
        streak = max(int(history.get('streak') or 0), 0)
    except (TypeError, ValueError):
        return 1
    return streak + 1 if last_seen == scan_id - 1 else 1


def compute_stability_multiplier(candidate: Any, history: dict[str, Any] | None, scan_id: int) -> tuple[float, int]:
    """Reward persistent high-quality candidates without blocking fresh signals."""
    streak = _projected_streak(history, scan_id)
    multiplier = 1.0

    if streak >= 4:
        multiplier += 0.025
    elif streak == 3:
        multiplier += 0.015
    elif streak == 2:
        multiplier += 0.005

    historical_percentile = _num((history or {}).get('ema_percentile'))
    if streak >= 2 and historical_percentile is not None:
        if historical_percentile >= 0.75:
            multiplier += 0.015
        elif historical_percentile >= 0.60:
            multiplier += 0.0075
        elif streak >= 3 and historical_percentile <= 0.25:
            multiplier -= 0.005

    current_percentile = _num(getattr(candidate, 'relative_selection_percentile', None))
    if (
        streak == 2
        and historical_percentile is not None
        and current_percentile is not None
        and current_percentile - historical_percentile >= 0.50
    ):
        multiplier -= 0.01

    return round(max(0.98, min(multiplier, 1.05)), 4), streak


def apply_selection_stability(candidates: Iterable[Any], state: dict[str, Any], scan_id: int) -> list[Any]:
    cohort = list(candidates)
    history_rows = state.get('symbols') if isinstance(state, dict) else {}
    if not isinstance(history_rows, dict):
        history_rows = {}

    for candidate in cohort:
        base = float(getattr(candidate, 'score', 0.0) or 0.0)
        candidate.stability_base_score = round(base, 4)
        symbol = _symbol(candidate)
        history = history_rows.get(symbol) if symbol else None
        multiplier, streak = compute_stability_multiplier(candidate, history, scan_id)
        historical_percentile = _num((history or {}).get('ema_percentile')) if isinstance(history, dict) else None
        candidate.selection_stability_streak = streak
        candidate.selection_stability_historical_percentile = (
            round(historical_percentile, 4) if historical_percentile is not None else None
        )
        candidate.selection_stability_multiplier = multiplier
        candidate.score = round(base * multiplier, 4)
        reasons = [
            reason for reason in list(getattr(candidate, 'reasons', []) or [])
            if not str(reason).startswith('selection_stability=')
        ]
        reasons.append(
            'selection_stability='
            f'streak={streak}:historical_percentile='
            f'{historical_percentile if historical_percentile is not None else "-"}:'
            f'multiplier={multiplier:.4f}'
        )
        candidate.reasons = reasons
    return cohort


def update_stability_state(state: dict[str, Any], candidates: Iterable[Any], scan_id: int) -> dict[str, Any]:
    normalized = _normalize_state(state)
    previous = normalized['symbols']
    updated: dict[str, dict[str, Any]] = dict(previous)
    for candidate in candidates:
        symbol = _symbol(candidate)
        if not symbol:
            continue
        old = previous.get(symbol, {})
        streak = _projected_streak(old, scan_id)
        current_percentile = _num(getattr(candidate, 'relative_selection_percentile', None), default=0.5)
        if current_percentile is None:
            current_percentile = 0.5
        current_percentile = max(0.0, min(float(current_percentile), 1.0))
        previous_ema = _num(old.get('ema_percentile')) if isinstance(old, dict) else None
        ema = current_percentile if previous_ema is None else (0.65 * previous_ema) + (0.35 * current_percentile)
        try:
            appearances = max(int(old.get('appearances') or 0), 0) + 1
        except (TypeError, ValueError, AttributeError):
            appearances = 1
        updated[symbol] = {
            'last_seen_scan': scan_id,
            'streak': streak,
            'appearances': appearances,
            'ema_percentile': round(max(0.0, min(ema, 1.0)), 4),
        }

    min_scan = max(scan_id - MAX_MISSING_SCANS, 0)
    kept = [
        (symbol, row)
        for symbol, row in updated.items()
        if int(row.get('last_seen_scan') or 0) >= min_scan
    ]
    kept.sort(key=lambda item: (int(item[1].get('last_seen_scan') or 0), int(item[1].get('appearances') or 0)), reverse=True)
    return {
        'scan_id': scan_id,
        'symbols': dict(kept[:MAX_TRACKED_SYMBOLS]),
    }


def install_selection_stability_hook(relative_selection_module: Any, strategy_module: Any) -> None:
    original_rerank = getattr(relative_selection_module, 'rerank_candidate_cohort', None)
    original_run_scan = getattr(strategy_module, 'run_scan_once', None)
    get_store = getattr(strategy_module, 'get_runtime_state_store', None)
    if not callable(original_rerank) or not callable(original_run_scan) or not callable(get_store):
        return
    if getattr(original_run_scan, '_selection_stability_hook', False):
        return

    def rerank_with_stability(candidates: Iterable[Any]):
        cohort = original_rerank(candidates)
        if getattr(_STATE, 'active', False):
            state = getattr(_STATE, 'history', {'scan_id': 0, 'symbols': {}})
            scan_id = int(getattr(_STATE, 'scan_id', 1) or 1)
            cohort = apply_selection_stability(cohort, state, scan_id)
            _STATE.latest_cohort = list(cohort)
        return cohort

    def run_scan_with_stability(*args: Any, **kwargs: Any):
        scan_args = kwargs.get('args')
        if scan_args is None and len(args) >= 2:
            scan_args = args[1]
        if scan_args is None:
            return original_run_scan(*args, **kwargs)

        store = get_store(scan_args)
        try:
            history = _normalize_state(store.load_json(STATE_NAME, {}))
        except Exception:
            history = {'scan_id': 0, 'symbols': {}}
        scan_id = int(history.get('scan_id') or 0) + 1

        previous_active = getattr(_STATE, 'active', False)
        previous_history = getattr(_STATE, 'history', None)
        previous_scan_id = getattr(_STATE, 'scan_id', None)
        previous_cohort = getattr(_STATE, 'latest_cohort', None)
        _STATE.active = True
        _STATE.history = history
        _STATE.scan_id = scan_id
        _STATE.latest_cohort = []
        try:
            result = original_run_scan(*args, **kwargs)
            latest = list(getattr(_STATE, 'latest_cohort', []) or [])
            next_state = update_stability_state(history, latest, scan_id)
            try:
                store.save_json(STATE_NAME, next_state)
            except Exception:
                pass
            return result
        finally:
            _STATE.active = previous_active
            _STATE.history = previous_history
            _STATE.scan_id = previous_scan_id
            _STATE.latest_cohort = previous_cohort

    rerank_with_stability._selection_stability_hook = True  # type: ignore[attr-defined]
    run_scan_with_stability._selection_stability_hook = True  # type: ignore[attr-defined]
    relative_selection_module.rerank_candidate_cohort = rerank_with_stability
    strategy_module.run_scan_once = run_scan_with_stability
