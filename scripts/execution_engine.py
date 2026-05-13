from __future__ import annotations

import argparse
import copy
import datetime
import threading
from typing import Any, Callable, Dict, Optional


MONITOR_EVENT_ROW_VOLATILE_FIELDS = frozenset({'recorded_at', 'opened_at', 'closed_at', 'consumer'})
TRADE_INVALIDATED_VOLATILE_FIELDS = frozenset({'time_in_trade_minutes'})
MONITOR_EVENT_PAYLOAD_VOLATILE_FIELDS = frozenset({'recorded_at', 'opened_at', 'closed_at'})


def normalize_monitor_event_row(row: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = {k: v for k, v in row.items() if k not in MONITOR_EVENT_ROW_VOLATILE_FIELDS}
    if cleaned.get('event_type') == 'trade_invalidated':
        for field in TRADE_INVALIDATED_VOLATILE_FIELDS:
            cleaned.pop(field, None)
    payload = cleaned.get('payload')
    if isinstance(payload, dict):
        cleaned['payload'] = {
            k: v for k, v in payload.items() if k not in MONITOR_EVENT_PAYLOAD_VOLATILE_FIELDS
        }
    return cleaned


def normalize_monitor_event_rows(rows: Any) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, dict):
            normalized.append(normalize_monitor_event_row(row))
    return normalized


def resolve_position_protection_status(
    client: Any,
    symbol: str,
    *,
    expected_stop_order: Optional[Dict[str, Any]] = None,
    allow_missing_when_flat: bool = True,
    side: Any,
    position_side_long: str,
    normalize_position_side: Callable[..., str],
    fetch_open_positions: Callable[..., Any],
    fetch_open_orders: Callable[..., Any],
    fetch_open_algo_orders: Callable[..., Any],
    position_row_matches_symbol_side: Callable[..., bool],
    _to_float: Callable[..., float],
) -> Dict[str, Any]:
    position_side = normalize_position_side(side)
    positions = fetch_open_positions(client)
    active = next((row for row in positions if position_row_matches_symbol_side(row, symbol, position_side)), None)
    expected_order_id = expected_stop_order.get('orderId') if isinstance(expected_stop_order, dict) else None
    expected_client_algo_id = expected_stop_order.get('clientAlgoId') if isinstance(expected_stop_order, dict) else None
    expected_trigger_price = _to_float(expected_stop_order.get('triggerPrice')) if isinstance(expected_stop_order, dict) else 0.0
    expected_quantity = abs(_to_float(expected_stop_order.get('quantity') or expected_stop_order.get('origQty'))) if isinstance(expected_stop_order, dict) else 0.0
    if active is None:
        return {
            'status': 'flat',
            'active_position': None,
            'expected_order_id': expected_order_id,
            'expected_client_algo_id': expected_client_algo_id,
            'matched_via': 'flat',
            'side': position_side,
        }
    open_orders = fetch_open_orders(client, symbol)
    matched = None
    matched_via = 'unmatched'
    matched_trigger_price = None
    matched_quantity = None
    if expected_order_id is not None:
        matched = next((row for row in open_orders if row.get('orderId') == expected_order_id), None)
        if matched is not None:
            matched_via = 'open_orders'
    elif open_orders:
        matched = open_orders[0]
        matched_via = 'open_orders'
    if matched is None:
        open_algo_orders = fetch_open_algo_orders(client, symbol)
        if expected_client_algo_id:
            candidates = [row for row in open_algo_orders if row.get('clientAlgoId') == expected_client_algo_id]
            if expected_trigger_price > 0:
                candidates = [row for row in candidates if abs(_to_float(row.get('triggerPrice')) - expected_trigger_price) <= 1e-9]
            if expected_quantity > 0:
                candidates = [row for row in candidates if abs(abs(_to_float(row.get('quantity') or row.get('origQty'))) - expected_quantity) <= 1e-9]
            matched = candidates[0] if candidates else None
            if matched is not None:
                matched_via = 'open_algo_orders'
        elif open_algo_orders:
            matched = next((row for row in open_algo_orders if str(row.get('orderType') or '').upper() == 'STOP_MARKET'), open_algo_orders[0])
            matched_via = 'open_algo_orders'
    if matched is None:
        return {
            'status': 'missing',
            'active_position': active,
            'expected_order_id': expected_order_id,
            'expected_client_algo_id': expected_client_algo_id,
            'matched_via': matched_via,
            'side': position_side,
        }
    matched_trigger_price = _to_float(matched.get('triggerPrice')) if isinstance(matched, dict) else None
    matched_quantity = abs(_to_float(matched.get('quantity') or matched.get('origQty'))) if isinstance(matched, dict) else None
    return {
        'status': 'protected',
        'active_position': active,
        'expected_order_id': expected_order_id,
        'expected_client_algo_id': expected_client_algo_id,
        'stop_order': matched,
        'matched_via': matched_via,
        'matched_trigger_price': matched_trigger_price,
        'matched_quantity': matched_quantity,
        'side': position_side,
    }


def repair_missing_protection(
    client: Any,
    symbol: str,
    *,
    tracked: Optional[Dict[str, Any]],
    active_position: Optional[Dict[str, Any]],
    meta: Optional[Any] = None,
    normalize_position_side: Callable[..., str],
    place_stop_market_order: Callable[..., Dict[str, Any]],
    fetch_exchange_meta: Callable[..., Any],
    _to_float: Callable[..., float],
) -> Dict[str, Any]:
    tracked = tracked if isinstance(tracked, dict) else {}
    active_position = active_position if isinstance(active_position, dict) else {}
    quantity = abs(_to_float(active_position.get('positionAmt')))
    stop_price = _to_float(tracked.get('stop_price'))
    side = normalize_position_side(tracked.get('side') or active_position.get('positionSide'))
    if quantity <= 0 or stop_price <= 0:
        return {
            'ok': False,
            'symbol': symbol,
            'side': side,
            'status': 'repair_failed',
            'message': 'missing stop_price or active quantity for repair',
            'repair_attempted': False,
        }
    if meta is None:
        meta = fetch_exchange_meta(client).get(symbol)
    if meta is None:
        return {
            'ok': False,
            'symbol': symbol,
            'side': side,
            'status': 'repair_failed',
            'message': 'missing symbol meta for repair',
            'repair_attempted': False,
        }
    stop_order = place_stop_market_order(client, symbol, stop_price, quantity, meta, side=side)
    return {
        'ok': True,
        'symbol': symbol,
        'side': side,
        'status': 'protected',
        'stop_order': stop_order,
        'stop_price': stop_price,
        'quantity': quantity,
        'repair_attempted': True,
    }


def ensure_symbol_margin_type(
    client: Any,
    symbol: str,
    *,
    binance_api_error,
    margin_type: str = 'ISOLATED',
) -> Dict[str, Any]:
    normalized_margin_type = str(margin_type or 'ISOLATED').strip().upper()
    if normalized_margin_type not in {'ISOLATED', 'CROSSED'}:
        normalized_margin_type = 'ISOLATED'
    try:
        response = client.signed_post('/fapi/v1/marginType', {
            'symbol': str(symbol or '').upper(),
            'marginType': normalized_margin_type,
        })
        return {
            'ok': True,
            'requested': normalized_margin_type,
            'actual': normalized_margin_type,
            'response': response if isinstance(response, dict) else {},
            'already_set': False,
            'applied': normalized_margin_type == 'ISOLATED',
            'multi_assets_mode': False,
        }
    except binance_api_error as exc:
        message = str(exc)
        if '-4046' in message or 'No need to change margin type' in message:
            return {
                'ok': True,
                'requested': normalized_margin_type,
                'actual': normalized_margin_type,
                'response': {'message': message},
                'already_set': True,
                'applied': normalized_margin_type == 'ISOLATED',
                'multi_assets_mode': False,
            }
        if normalized_margin_type == 'ISOLATED' and ('-4168' in message or 'Multi-Assets mode' in message):
            return {
                'ok': True,
                'requested': normalized_margin_type,
                'actual': 'CROSSED',
                'response': {'message': message},
                'already_set': False,
                'applied': False,
                'multi_assets_mode': True,
                'fallback_reason': 'binance_multi_assets_mode_blocks_isolated',
            }
        raise


def place_initial_stop_with_retries(
    client: Any,
    candidate: Any,
    meta: Any,
    args: argparse.Namespace,
    *,
    filled_quantity: float,
    position_side: str,
    fetch_open_positions: Callable[..., Any],
    position_row_matches_symbol_side: Callable[..., bool],
    place_stop_market_order: Callable[..., Dict[str, Any]],
    log_runtime_event: Callable[..., Any],
    emit_notification: Callable[..., Any],
    binance_api_error,
    _to_float: Callable[..., float],
    time_module,
) -> Dict[str, Any]:
    max_attempts = max(int(getattr(args, 'initial_stop_max_attempts', 3) or 3), 1)
    retry_sleep_sec = max(float(getattr(args, 'initial_stop_retry_sleep_sec', 0.7) or 0.0), 0.0)
    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        attempt_quantity = filled_quantity
        active_position = next(
            (
                row
                for row in fetch_open_positions(client)
                if position_row_matches_symbol_side(row, candidate.symbol, position_side)
            ),
            None,
        )
        if isinstance(active_position, dict):
            live_quantity = abs(_to_float(active_position.get('positionAmt')))
            if live_quantity > 0:
                attempt_quantity = live_quantity

        try:
            stop = place_stop_market_order(
                client,
                candidate.symbol,
                float(candidate.stop_price),
                attempt_quantity,
                meta,
                side=position_side,
            )
            success_payload = {
                'symbol': candidate.symbol,
                'side': position_side,
                'attempt': attempt,
                'max_attempts': max_attempts,
                'quantity': attempt_quantity,
                'stop_order_id': stop.get('orderId') if isinstance(stop, dict) else None,
                'profile': getattr(args, 'profile', 'default'),
            }
            log_runtime_event('initial_stop_place_attempt_succeeded', success_payload)
            if attempt > 1:
                emit_notification(args, 'initial_stop_place_attempt_succeeded', success_payload)
            return stop
        except Exception as exc:
            last_error = exc
            retry_payload = {
                'symbol': candidate.symbol,
                'side': position_side,
                'attempt': attempt,
                'max_attempts': max_attempts,
                'quantity': attempt_quantity,
                'message': f'initial stop placement failed: {exc}',
                'profile': getattr(args, 'profile', 'default'),
            }
            log_runtime_event('initial_stop_place_attempt_failed', retry_payload)
            emit_notification(args, 'initial_stop_place_attempt_failed', retry_payload)
            if attempt < max_attempts and retry_sleep_sec > 0:
                time_module.sleep(retry_sleep_sec)

    exhausted_payload = {
        'symbol': candidate.symbol,
        'side': position_side,
        'max_attempts': max_attempts,
        'message': f'开仓成功，但初始止损重挂全部失败: {last_error}',
        'profile': getattr(args, 'profile', 'default'),
    }
    log_runtime_event('initial_stop_retry_exhausted', exhausted_payload)
    emit_notification(args, 'initial_stop_retry_exhausted', exhausted_payload)
    raise binance_api_error(exhausted_payload['message']) from last_error


def place_live_trade(
    client: Any,
    candidate: Any,
    *,
    leverage: int,
    meta: Any,
    args: argparse.Namespace,
    binance_api_error,
    ensure_symbol_margin_type_fn: Callable[..., Dict[str, Any]],
    round_step: Callable[..., float],
    format_decimal: Callable[..., str],
    should_send_position_side: Callable[..., bool],
    is_position_side_mode_error: Callable[..., bool],
    mark_one_way_position_mode: Callable[..., None],
    build_trade_management_plan: Callable[..., Any],
    fetch_open_positions: Callable[..., Any],
    fetch_open_orders: Callable[..., Any],
    fetch_open_algo_orders: Callable[..., Any],
    place_stop_market_order: Callable[..., Dict[str, Any]],
    place_take_profit_market_order: Optional[Callable[..., Dict[str, Any]]],
    resolve_position_protection_status: Callable[..., Dict[str, Any]],
    recover_unknown_entry_order: Callable[..., Dict[str, Any]],
    query_order: Callable[..., Dict[str, Any]],
    log_runtime_event: Callable[..., Any],
    emit_notification: Callable[..., Any],
    normalize_position_side: Callable[..., str],
    build_position_key: Callable[..., str],
    position_row_matches_symbol_side: Callable[..., bool],
    _to_float: Callable[..., float],
    compute_execution_quality_size_adjustment: Callable[..., Dict[str, Any]],
    asdict: Callable[[Any], Dict[str, Any]],
    position_side_long: str,
    time_module,
) -> Dict[str, Any]:
    position_side = normalize_position_side(getattr(candidate, 'side', position_side_long))
    position_key = build_position_key(candidate.symbol, position_side)
    profile = getattr(args, 'profile', 'default')
    requested_margin_type = str(getattr(args, 'margin_type', 'ISOLATED') or 'ISOLATED').strip().upper()
    requested_leverage = int(leverage)

    open_positions = fetch_open_positions(client)
    has_existing_position = any(
        isinstance(row, dict)
        and position_row_matches_symbol_side(row, candidate.symbol, position_side)
        for row in list(open_positions or [])
    )
    if has_existing_position:
        error_payload = {
            'symbol': candidate.symbol,
            'side': position_side,
            'position_key': position_key,
            'profile': profile,
            'preflight_reason': 'existing_position_open',
            'message': 'preflight hard gate: existing_position_open',
        }
        log_runtime_event('error', error_payload)
        emit_notification(args, 'error', error_payload)
        raise binance_api_error('preflight hard gate: existing_position_open')

    open_orders = fetch_open_orders(client, candidate.symbol)
    open_algo_orders = fetch_open_algo_orders(client, candidate.symbol)
    has_existing_open_orders = any(
        isinstance(row, dict)
        and str(row.get('symbol', '')).upper() == candidate.symbol.upper()
        for row in list(open_orders or []) + list(open_algo_orders or [])
    )
    if has_existing_open_orders:
        error_payload = {
            'symbol': candidate.symbol,
            'side': position_side,
            'position_key': position_key,
            'profile': profile,
            'preflight_reason': 'existing_open_orders',
            'message': 'preflight hard gate: existing_open_orders',
            'open_order_ids': [row.get('orderId') for row in list(open_orders or []) if isinstance(row, dict)],
            'open_algo_order_ids': [row.get('algoId') or row.get('clientAlgoId') for row in list(open_algo_orders or []) if isinstance(row, dict)],
        }
        log_runtime_event('error', error_payload)
        emit_notification(args, 'error', error_payload)
        raise binance_api_error('preflight hard gate: existing_open_orders')

    margin_type_check = ensure_symbol_margin_type_fn(client, candidate.symbol, margin_type=requested_margin_type)
    leverage_response = client.signed_post('/fapi/v1/leverage', {'symbol': candidate.symbol, 'leverage': requested_leverage})
    actual_leverage = int(_to_float(leverage_response.get('leverage'), default=requested_leverage)) if isinstance(leverage_response, dict) else requested_leverage
    if actual_leverage != requested_leverage:
        error_payload = {
            'symbol': candidate.symbol,
            'side': position_side,
            'position_key': position_key,
            'profile': profile,
            'preflight_reason': 'leverage_mismatch',
            'requested_leverage': requested_leverage,
            'actual_leverage': actual_leverage,
            'message': f'preflight hard gate: leverage_mismatch requested={requested_leverage} actual={actual_leverage}',
        }
        log_runtime_event('error', error_payload)
        emit_notification(args, 'error', error_payload)
        raise binance_api_error(error_payload['message'])

    if getattr(candidate, 'probe_entry', False):
        probe_max_leverage = int(getattr(args, 'probe_max_leverage', 2) or 2)
        probe_max_leverage = max(probe_max_leverage, 1)
        if requested_leverage > probe_max_leverage:
            requested_leverage = probe_max_leverage
            client.signed_post('/fapi/v1/leverage', {'symbol': candidate.symbol, 'leverage': requested_leverage})

    execution_quality = compute_execution_quality_size_adjustment(candidate)
    step_size = float(getattr(meta, 'step_size', 0.0) or 0.0)
    quantity_precision = int(getattr(meta, 'quantity_precision', 0) or 0)
    min_qty = float(getattr(meta, 'min_qty', 0.0) or 0.0)
    base_quantity = round_step(candidate.quantity, step_size, quantity_precision)
    scaled_quantity = round_step(base_quantity * float(execution_quality['size_multiplier']), step_size, quantity_precision)
    quantity = scaled_quantity if scaled_quantity >= min_qty else scaled_quantity
    if getattr(candidate, 'probe_entry', False):
        probe_size_ratio = float(getattr(args, 'sim_probe_size_ratio', 0.2) or 0.2)
        quantity = max(quantity * probe_size_ratio, min_qty)
    if quantity < min_qty:
        error_payload = {
            'symbol': candidate.symbol,
            'side': position_side,
            'position_key': position_key,
            'profile': profile,
            'preflight_reason': 'quantity_below_min_qty',
            'base_quantity': base_quantity,
            'scaled_quantity': scaled_quantity,
            'min_qty': min_qty,
            'message': 'preflight hard gate: quantity_below_min_qty',
        }
        log_runtime_event('error', error_payload)
        emit_notification(args, 'error', error_payload)
        raise binance_api_error(error_payload['message'])
    entry_order_error: Optional[Exception] = None
    entry_position_mode = 'HEDGE' if should_send_position_side(client) else 'ONE_WAY'
    entry_params = {
        'symbol': candidate.symbol,
        'side': 'SELL' if position_side != position_side_long else 'BUY',
        'type': 'MARKET',
        'quantity': format_decimal(quantity, quantity_precision),
        'newOrderRespType': 'RESULT',
    }
    if should_send_position_side(client):
        entry_params['positionSide'] = position_side
    try:
        entry_order = client.signed_post('/fapi/v1/order', entry_params)
    except Exception as exc:
        entry_order_error = exc
        error_message = str(exc)
        if is_position_side_mode_error(exc) and should_send_position_side(client):
            mark_one_way_position_mode(client)
            entry_params.pop('positionSide', None)
            entry_order = client.signed_post('/fapi/v1/order', entry_params)
            entry_position_mode = 'ONE_WAY'
        elif '-1007' in error_message and 'unknown' in error_message.lower():
            try:
                entry_order = recover_unknown_entry_order(client, candidate, quantity, quantity_precision)
            except Exception as recovery_exc:
                message = str(recovery_exc)
                error_payload = {
                    'symbol': candidate.symbol,
                    'message': message,
                    'profile': profile,
                }
                log_runtime_event('error', error_payload)
                emit_notification(args, 'error', error_payload)
                raise binance_api_error(message) from recovery_exc
        else:
            raise
    entry_price = _to_float(entry_order.get('avgPrice')) or float(candidate.last_price)
    filled_quantity = _to_float(entry_order.get('executedQty'), default=0.0)
    if filled_quantity <= 0 or str(entry_order.get('status') or '').upper() not in {'FILLED', 'PARTIALLY_FILLED'}:
        order_id = entry_order.get('orderId') if isinstance(entry_order, dict) else None
        client_order_id = entry_order.get('clientOrderId') if isinstance(entry_order, dict) else None
        for _ in range(3):
            if order_id is None and not client_order_id:
                break
            time_module.sleep(0.4)
            try:
                confirmed_entry = query_order(client, candidate.symbol, order_id=order_id, client_order_id=client_order_id)
            except Exception:
                continue
            confirmed_qty = _to_float(confirmed_entry.get('executedQty'), default=0.0)
            if confirmed_qty > 0:
                entry_order = confirmed_entry
                entry_price = _to_float(entry_order.get('avgPrice')) or _to_float(entry_order.get('price')) or entry_price
                filled_quantity = confirmed_qty
                break
        if filled_quantity <= 0:
            active_position = next((row for row in fetch_open_positions(client) if position_row_matches_symbol_side(row, candidate.symbol, position_side)), None)
            if isinstance(active_position, dict):
                filled_quantity = abs(_to_float(active_position.get('positionAmt')))
                entry_price = _to_float(active_position.get('entryPrice')) or entry_price
    if filled_quantity <= 0:
        raise binance_api_error(f'entry order not filled yet; stop placement skipped for {candidate.symbol}')
    entry_order_feedback = {
        'order_id': entry_order.get('orderId'),
        'client_order_id': entry_order.get('clientOrderId'),
        'status': entry_order.get('status'),
        'avg_price': entry_price,
        'executed_qty': filled_quantity,
        'cum_quote': _to_float(entry_order.get('cumQuote')),
        'update_time': entry_order.get('updateTime'),
        'position_mode': entry_position_mode,
        'recovered_from_unknown_timeout': bool(entry_order_error is not None and '-1007' in str(entry_order_error)),
    }
    plan = build_trade_management_plan(
        entry_price=entry_price,
        stop_price=float(candidate.stop_price),
        quantity=filled_quantity,
        tp1_r=float(getattr(args, 'tp1_r', 1.5)),
        tp1_close_pct=float(getattr(args, 'tp1_close_pct', 0.3)),
        tp2_r=float(getattr(args, 'tp2_r', 2.0)),
        tp2_close_pct=float(getattr(args, 'tp2_close_pct', 0.4)),
        breakeven_r=float(getattr(args, 'breakeven_r', 1.0)),
        atr_stop_distance=float(getattr(candidate, 'atr_stop_distance', 0.0) or 0.0),
        side=normalize_position_side(getattr(candidate, 'side', position_side_long)),
        breakeven_confirmation_mode=str(getattr(args, 'breakeven_confirmation_mode', 'ema_support') or 'ema_support'),
        breakeven_min_buffer_pct=float(getattr(args, 'breakeven_min_buffer_pct', 0.001) or 0.0),
        tp1_profit_usdt=float(getattr(args, 'tp1_profit_usdt', 0.0) or 0.0),
        tp2_profit_usdt=float(getattr(args, 'tp2_profit_usdt', 0.0) or 0.0),
    )
    payload = {
        'symbol': candidate.symbol,
        'side': normalize_position_side(getattr(candidate, 'side', position_side_long)),
        'position_key': build_position_key(candidate.symbol, getattr(candidate, 'side', position_side_long)),
        'entry_price': entry_price,
        'filled_quantity': filled_quantity,
        'stop_price': float(candidate.stop_price),
        'quantity': filled_quantity,
        'entry_order_id': entry_order_feedback['order_id'],
        'entry_client_order_id': entry_order_feedback['client_order_id'],
        'entry_order_status': entry_order_feedback['status'],
        'entry_cum_quote': entry_order_feedback['cum_quote'],
        'entry_update_time': entry_order_feedback['update_time'],
        'profile': getattr(args, 'profile', 'default'),
        'margin_type': requested_margin_type,
        'margin_type_check': margin_type_check,
        'leverage': requested_leverage,
        'leverage_check': {
            'requested': requested_leverage,
            'actual': actual_leverage,
            'response': leverage_response if isinstance(leverage_response, dict) else {},
        },
        'position_mode': entry_position_mode,
    }
    log_runtime_event('entry_filled', payload)
    emit_notification(args, 'entry_filled', payload)
    stop_order = place_initial_stop_with_retries(
        client=client,
        candidate=candidate,
        meta=meta,
        args=args,
        filled_quantity=filled_quantity,
        position_side=position_side,
        fetch_open_positions=fetch_open_positions,
        position_row_matches_symbol_side=position_row_matches_symbol_side,
        place_stop_market_order=place_stop_market_order,
        log_runtime_event=log_runtime_event,
        emit_notification=emit_notification,
        binance_api_error=binance_api_error,
        _to_float=_to_float,
        time_module=time_module,
    )
    protection = resolve_position_protection_status(
        client,
        candidate.symbol,
        expected_stop_order=stop_order,
        allow_missing_when_flat=True,
        side=normalize_position_side(getattr(candidate, 'side', position_side_long)),
    )
    if protection.get('status') == 'missing':
        error_payload = {
            'symbol': candidate.symbol,
            'message': '开仓成功，但止损单未被交易所开放订单确认 (not confirmed by exchange open orders)',
            'expected_stop_order_id': protection.get('expected_order_id'),
            'profile': getattr(args, 'profile', 'default'),
        }
        log_runtime_event('error', error_payload)
        emit_notification(args, 'error', error_payload)
        raise binance_api_error('stop order not confirmed by exchange open orders')
    stop_payload = {
        'symbol': candidate.symbol,
        'side': normalize_position_side(getattr(candidate, 'side', position_side_long)),
        'position_key': build_position_key(candidate.symbol, getattr(candidate, 'side', position_side_long)),
        'stop_price': float(candidate.stop_price),
        'quantity': filled_quantity,
        'stop_order_id': stop_order.get('orderId'),
        'protection_status': protection.get('status'),
        'profile': getattr(args, 'profile', 'default'),
        'position_mode': 'ONE_WAY' if not should_send_position_side(client) else entry_position_mode,
    }
    log_runtime_event('initial_stop_placed', stop_payload)
    emit_notification(args, 'initial_stop_placed', stop_payload)
    position_side = normalize_position_side(getattr(candidate, 'side', position_side_long))
    tp1_order = None
    if place_take_profit_market_order and plan.tp1_close_qty > 0:
        tp1_order = place_take_profit_market_order(
            client,
            candidate.symbol,
            float(plan.tp1_trigger_price),
            float(plan.tp1_close_qty),
            meta,
            side=position_side,
        )
    tp2_order = None
    tp2_close_qty = float(plan.tp2_close_qty) + float(plan.runner_qty)
    if place_take_profit_market_order and tp2_close_qty > 0:
        tp2_order = place_take_profit_market_order(
            client,
            candidate.symbol,
            float(plan.tp2_trigger_price),
            tp2_close_qty,
            meta,
            side=position_side,
        )
    return {
        'symbol': candidate.symbol,
        'side': normalize_position_side(getattr(candidate, 'side', position_side_long)),
        'position_key': build_position_key(candidate.symbol, getattr(candidate, 'side', position_side_long)),
        'entry_order': entry_order,
        'entry_price': entry_price,
        'filled_quantity': filled_quantity,
        'entry_order_feedback': entry_order_feedback,
        'position_mode': 'ONE_WAY' if not should_send_position_side(client) else entry_position_mode,
        'margin_type': requested_margin_type,
        'margin_type_check': margin_type_check,
        'leverage': requested_leverage,
        'leverage_check': {
            'requested': requested_leverage,
            'actual': actual_leverage,
            'response': leverage_response if isinstance(leverage_response, dict) else {},
        },
        'stop_order': stop_order,
        'tp1_order': tp1_order,
        'tp2_order': tp2_order,
        'protection_check': protection,
        'trade_management_plan': asdict(plan),
    }


def monitor_live_trade(
    client: Any,
    symbol: str,
    meta: Any,
    args: argparse.Namespace,
    trade: Dict[str, Any],
    store: Any,
    *,
    initial_positions_state: Optional[Dict[str, Any]] = None,
    trade_management_plan_type,
    trade_management_state_type,
    position_side_long: str,
    position_side_short: str,
    binance_api_error,
    _to_float: Callable[..., float],
    normalize_position_side: Callable[..., str],
    position_side_to_trade_side: Callable[..., str],
    build_position_key: Callable[..., str],
    get_position_by_symbol_side: Callable[..., Any],
    build_trade_analytics_snapshot: Callable[..., Dict[str, Any]],
    upsert_position_record: Callable[..., Any],
    materialize_positions_state: Callable[..., Dict[str, Any]],
    asdict: Callable[[Any], Dict[str, Any]],
    log_runtime_event: Callable[..., Any],
    emit_notification: Callable[..., Any],
    fetch_klines: Callable[..., Any],
    extract_closes: Callable[..., Any],
    extract_highs: Callable[..., Any],
    extract_lows: Callable[..., Any],
    resolve_monitor_current_price: Callable[..., Dict[str, Any]],
    evaluate_management_actions: Callable[..., Any],
    update_trade_progress_metrics: Callable[..., Any],
    apply_management_action: Callable[..., Any],
    resolve_reduce_order_exit_price: Callable[..., Any],
    compute_trade_realized_r_increment: Callable[..., float],
    score_to_decile_label: Callable[..., str],
    resolve_trigger_class: Callable[..., str],
    utc_now: Callable[[], datetime.datetime],
    isoformat_utc: Callable[[datetime.datetime], str],
    time_module,
) -> Dict[str, Any]:
    entry_price = _to_float(trade.get('entry_price'))
    stop_order = trade.get('stop_order') if isinstance(trade.get('stop_order'), dict) else None
    protection_check = trade.get('protection_check') if isinstance(trade.get('protection_check'), dict) else {}
    raw_plan_payload = trade.get('trade_management_plan')
    plan_payload: Dict[str, Any] = copy.deepcopy(raw_plan_payload) if isinstance(raw_plan_payload, dict) else {}
    trade_side = normalize_position_side(trade.get('side') or plan_payload.get('side'))
    plan_payload.setdefault('side', trade_side)
    plan_payload.setdefault('position_side', trade_side)
    positions = copy.deepcopy(initial_positions_state) if isinstance(initial_positions_state, dict) else store.load_json('positions', {})
    position_state_source = 'injected' if isinstance(initial_positions_state, dict) else 'store'
    if not isinstance(positions, dict):
        positions = {}
    position_key, tracked = get_position_by_symbol_side(positions, symbol, trade_side)
    tracked_stop_price = _to_float(tracked.get('current_stop_price') or tracked.get('stop_price'), default=0.0)
    if tracked_stop_price > 0:
        plan_payload['stop_price'] = tracked_stop_price
    plan = trade_management_plan_type(**plan_payload)
    state = trade_management_state_type(
        symbol=symbol,
        side=position_side_to_trade_side(trade_side),
        position_side=trade_side,
        position_key=position_key,
        initial_quantity=_to_float(tracked.get('quantity') or plan.quantity or trade.get('quantity')),
        remaining_quantity=_to_float(tracked.get('remaining_quantity') or tracked.get('quantity') or plan.quantity),
        current_stop_price=_to_float(tracked.get('current_stop_price') or tracked.get('stop_price') or plan.stop_price, default=plan.stop_price),
        moved_to_breakeven=bool(tracked.get('moved_to_breakeven', False)),
        tp1_hit=bool(tracked.get('tp1_hit', False)),
        tp2_hit=bool(tracked.get('tp2_hit', False)),
        highest_price_seen=_to_float(tracked.get('highest_price_seen') or entry_price, default=entry_price),
        lowest_price_seen=_to_float(tracked.get('lowest_price_seen') or entry_price, default=entry_price),
        opened_at=str(tracked.get('opened_at') or isoformat_utc(utc_now())),
        first_1r_at=str(tracked.get('first_1r_at') or '') or None,
        realized_r=_to_float(tracked.get('realized_r'), default=0.0),
    )
    selection_context = dict(tracked)

    def persist_position(
        status: str,
        protection_status: Optional[str],
        active_stop_order: Optional[Dict[str, Any]],
        exit_reason: Optional[str] = None,
        closed_at: Optional[datetime.datetime] = None,
    ) -> Dict[str, Any]:
        position_payload = dict(tracked)
        analytics_snapshot = build_trade_analytics_snapshot(state, plan, closed_at=closed_at)
        normalized_opened_at = state.opened_at
        normalized_closed_at = isoformat_utc(closed_at) if closed_at else None
        position_payload.update({
            'symbol': symbol,
            'side': state.side,
            'position_key': build_position_key(symbol, state.side),
            'status': status,
            'quantity': round(state.initial_quantity, 10),
            'remaining_quantity': round(state.remaining_quantity, 10),
            'entry_price': round(entry_price, 10),
            'stop_price': round(state.current_stop_price, 10) if state.current_stop_price is not None else None,
            'current_stop_price': round(state.current_stop_price, 10) if state.current_stop_price is not None else None,
            'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) else None,
            'protection_status': protection_status,
            'moved_to_breakeven': state.moved_to_breakeven,
            'tp1_hit': state.tp1_hit,
            'tp2_hit': state.tp2_hit,
            'highest_price_seen': round(state.highest_price_seen, 10) if state.highest_price_seen is not None else None,
            'lowest_price_seen': round(state.lowest_price_seen, 10) if state.lowest_price_seen is not None else None,
            'opened_at': normalized_opened_at,
            'first_1r_at': state.first_1r_at,
            'closed_at': normalized_closed_at,
            'realized_r': analytics_snapshot['realized_r'],
            'mfe_r': analytics_snapshot['mfe_r'],
            'mae_r': analytics_snapshot['mae_r'],
            'time_to_1r': analytics_snapshot['time_to_1r'],
            'time_to_1r_minutes': analytics_snapshot['time_to_1r_minutes'],
            'time_in_trade_minutes': analytics_snapshot['time_in_trade_minutes'],
            'trade_management_plan': asdict(plan),
            'profile': getattr(args, 'profile', 'default'),
            'exit_reason': exit_reason or position_payload.get('exit_reason'),
        })
        storage_key_hint = str(state.position_key or tracked.get('position_key') or tracked.get('symbol') or build_position_key(symbol, state.side)).upper()
        updated_positions, resolved_key = upsert_position_record(positions, position_payload, key=storage_key_hint)
        positions.clear()
        positions.update(updated_positions)
        state.position_key = resolved_key
        position_payload['position_key'] = resolved_key
        persisted_payload = dict(positions.get(resolved_key, position_payload))
        store.save_json('positions', materialize_positions_state(
            positions,
            {resolved_key: str(tracked.get('position_key') or tracked.get('symbol') or resolved_key).upper()},
            include_legacy_alias=True,
        ))
        tracked.clear()
        tracked.update(persisted_payload)
        selection_context.update(persisted_payload)
        return persisted_payload

    def record_event(event_type: str, payload: Dict[str, Any], notify: bool = True) -> Dict[str, Any]:
        event_payload = {
            'symbol': symbol,
            'side': state.side,
            'position_key': state.position_key or build_position_key(symbol, state.side),
            'profile': getattr(args, 'profile', 'default'),
            **payload,
        }
        log_runtime_event(event_type, event_payload)
        row = store.append_event(event_type, event_payload)
        if notify:
            emit_notification(args, event_type, event_payload)
        return row

    persist_position(status='monitoring', protection_status=protection_check.get('status') if isinstance(protection_check, dict) else None, active_stop_order=stop_order)
    record_event('entry_filled', {
        'entry_price': round(entry_price, 10),
        'stop_price': round(plan.stop_price, 10),
        'quantity': round(state.initial_quantity, 10),
    })
    record_event('protection_confirmed', {
        'protection_status': protection_check.get('status') if isinstance(protection_check, dict) else None,
        'stop_order_id': stop_order.get('orderId') if isinstance(stop_order, dict) else None,
    })

    active_stop_order = stop_order
    protection_status = protection_check.get('status') if isinstance(protection_check, dict) else None
    max_cycles = max(int(getattr(args, 'max_monitor_cycles', 20) or 20), 1)
    if getattr(args, 'monitor_poll_interval_sec', None) in (None, 0, 0.0, '0', '0.0') and getattr(args, 'max_monitor_cycles', None) is None:
        max_cycles = max(max_cycles, 50)
    trailing_buffer_pct = float(getattr(args, 'trailing_buffer_pct', 0.02) or 0.02)
    book_ticker_cache_max_age_seconds = max(float(getattr(args, 'monitor_poll_interval_sec', 2) or 2), 3.0)
    for _ in range(max_cycles):
        if state.remaining_quantity <= 0:
            break
        klines = fetch_klines(client, symbol, '5m', 21)
        positions = store.load_json('positions', {})
        if not isinstance(positions, dict):
            positions = {}
        loop_position_key, tracked = get_position_by_symbol_side(positions, symbol, state.side)
        state.position_key = loop_position_key
        closes = extract_closes(klines)
        highs = extract_highs(klines)
        lows = extract_lows(klines)
        current_price = tracked.get('_debug_current_price')
        current_price_source = 'debug_override' if current_price is not None else ''
        current_price_snapshot = None
        if current_price is None:
            fallback_price = closes[-1] if closes else entry_price
            price_resolution = resolve_monitor_current_price(
                store,
                symbol,
                state.position_side,
                fallback_price=fallback_price,
                cache_max_age_seconds=book_ticker_cache_max_age_seconds,
            )
            current_price = price_resolution['price']
            current_price_source = str(price_resolution.get('source') or 'kline_close_fallback')
            current_price_snapshot = price_resolution.get('snapshot')
        ema5m = tracked.get('_debug_ema5m')
        if ema5m is None:
            ema5m = closes[-1] if closes else current_price
        trailing_reference = tracked.get('_debug_trailing_reference')
        if trailing_reference is None:
            if state.position_side == position_side_short:
                trailing_reference = min(lows) if lows else min(state.lowest_price_seen or current_price, current_price)
            else:
                trailing_reference = max(highs) if highs else max(state.highest_price_seen or current_price, current_price)
        actions = evaluate_management_actions(
            state,
            plan,
            current_price=current_price,
            ema5m=ema5m,
            trailing_reference=trailing_reference,
            trailing_buffer_pct=trailing_buffer_pct,
            allow_runner_exit=True,
        )
        update_trade_progress_metrics(state, plan, current_price=current_price, observed_at=utc_now())
        if normalize_position_side(state.position_side) == position_side_short:
            state.lowest_price_seen = min(state.lowest_price_seen or current_price, current_price)
        else:
            state.highest_price_seen = max(state.highest_price_seen or current_price, current_price)
        debug_payload = {
            'symbol': symbol,
            'position_side': state.position_side,
            'position_state_source': position_state_source,
            'current_price': current_price,
            'current_price_source': current_price_source or 'kline_close_fallback',
            'current_price_cache_max_age_seconds': book_ticker_cache_max_age_seconds,
            'book_ticker_snapshot': current_price_snapshot,
            'ema5m': ema5m,
            'trailing_reference': trailing_reference,
            'actions': actions,
            'remaining_quantity': state.remaining_quantity,
            'tracked': tracked,
            'max_cycles': max_cycles,
        }
        store.save_json('monitor_debug', debug_payload)
        if not actions:
            persist_position(status='monitoring', protection_status=protection_status, active_stop_order=active_stop_order)
            time_module.sleep(float(getattr(args, 'monitor_poll_interval_sec', 2) or 2))
            continue
        for action in actions:
            try:
                state, active_stop_order, action_result = apply_management_action(client, symbol, meta, state, action, active_stop_order)
            except binance_api_error as exc:
                message = str(exc)
                record_event('management_action_failed', {
                    'action': action.get('type'),
                    'message': message,
                    'current_price': round(float(current_price), 10),
                    'requested_stop_price': action.get('new_stop_price'),
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'kept_existing_stop': True,
                })
                persist_position(status='monitoring', protection_status=protection_status, active_stop_order=active_stop_order)
                continue
            action_exit_price = None
            if action.get('type') in {'take_profit_1', 'take_profit_2', 'runner_exit'}:
                action_exit_price = resolve_reduce_order_exit_price(action_result.get('reduce_order', {}), current_price)
                state.realized_r += compute_trade_realized_r_increment(
                    entry_price=plan.entry_price,
                    exit_price=action_exit_price,
                    initial_risk_per_unit=plan.initial_risk_per_unit,
                    close_qty=action.get('close_qty'),
                    initial_quantity=state.initial_quantity,
                    side=state.position_side,
                )
            if state.remaining_quantity <= 0:
                protection_status = 'flat'
            elif action['type'] == 'runner_exit':
                protection_status = 'flat'
            else:
                protection_status = 'protected'
            if action['type'] == 'move_stop_to_breakeven':
                record_event('breakeven_moved', {
                    'new_stop_price': round(action['new_stop_price'], 10),
                    'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) else None,
                    'confirmation_mode': action.get('confirmation_mode', plan.breakeven_confirmation_mode),
                })
            elif action['type'] == 'take_profit_1':
                action.setdefault('exit_reason', 'tp1')
                record_event('tp1_hit', {
                    'close_qty': round(action['close_qty'], 10),
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'new_stop_price': round(action.get('new_stop_price'), 10) if action.get('new_stop_price') is not None else None,
                    'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) else None,
                    'exit_reason': action.get('exit_reason', 'tp1'),
                    'exit_price': round(action_exit_price, 10) if action_exit_price is not None else None,
                    'realized_r_after_action': round(state.realized_r, 4),
                })
            elif action['type'] == 'take_profit_2':
                action.setdefault('exit_reason', 'tp2')
                record_event('tp2_hit', {
                    'close_qty': round(action['close_qty'], 10),
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'new_stop_price': round(action.get('new_stop_price'), 10) if action.get('new_stop_price') is not None else None,
                    'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) else None,
                    'exit_reason': action.get('exit_reason', 'tp2'),
                    'exit_price': round(action_exit_price, 10) if action_exit_price is not None else None,
                    'realized_r_after_action': round(state.realized_r, 4),
                })
            elif action['type'] == 'runner_exit':
                action.setdefault('exit_reason', 'runner')
                record_event('runner_exited', {
                    'close_qty': round(action['close_qty'], 10),
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'trailing_floor': round(action.get('trailing_floor'), 10) if action.get('trailing_floor') is not None else None,
                    'protection_status': protection_status,
                    'exit_reason': action.get('exit_reason', 'runner'),
                    'exit_price': round(action_exit_price, 10) if action_exit_price is not None else None,
                    'realized_r_after_action': round(state.realized_r, 4),
                })
            closed_at = None
            if protection_status == 'flat':
                final_exit_reason = action.get('exit_reason', 'flat')
                closed_at = utc_now()
                analytics_snapshot = build_trade_analytics_snapshot(state, plan, closed_at=closed_at)
                analytics_snapshot.pop('opened_at', None)
                analytics_snapshot.pop('closed_at', None)
                selected_score = _to_float(
                    tracked.get('selected_score', tracked.get('score', selection_context.get('selected_score', selection_context.get('score')))),
                    default=0.0,
                )
                record_event('trade_invalidated', {
                    'exit_reason': final_exit_reason,
                    'remaining_quantity': round(state.remaining_quantity, 10),
                    'protection_status': protection_status,
                    'exit_price': round(action_exit_price, 10) if action_exit_price is not None else None,
                    'score': round(selected_score, 4),
                    'score_decile': str(tracked.get('score_decile') or selection_context.get('score_decile') or score_to_decile_label(selected_score)),
                    'state': str(tracked.get('selected_state') or tracked.get('state') or selection_context.get('selected_state') or selection_context.get('state') or ''),
                    'alert_tier': str(tracked.get('selected_alert_tier') or tracked.get('alert_tier') or selection_context.get('selected_alert_tier') or selection_context.get('alert_tier') or ''),
                    'candidate_stage': str(tracked.get('candidate_stage') or selection_context.get('candidate_stage') or ''),
                    'trigger_class': str(tracked.get('trigger_class') or selection_context.get('trigger_class') or resolve_trigger_class(selection_context)),
                    'market_regime_label': str(tracked.get('market_regime_label') or selection_context.get('market_regime_label') or ''),
                    'market_regime_multiplier': round(_to_float(tracked.get('market_regime_multiplier', selection_context.get('market_regime_multiplier')), default=0.0), 4),
                    'setup_ready': bool(tracked.get('setup_ready', selection_context.get('setup_ready', False))),
                    'trigger_fired': bool(tracked.get('trigger_fired', selection_context.get('trigger_fired', False))),
                    **analytics_snapshot,
                }, notify=False)
            persist_position(
                status='closed' if protection_status == 'flat' else 'monitoring',
                protection_status=protection_status,
                active_stop_order=active_stop_order,
                exit_reason=action.get('exit_reason') if protection_status == 'flat' else None,
                closed_at=closed_at if protection_status == 'flat' else None,
            )
            tracked['exit_reason'] = action.get('exit_reason') if protection_status == 'flat' else tracked.get('exit_reason')
            if protection_status == 'flat':
                break
        if protection_status == 'flat':
            break
        time_module.sleep(float(getattr(args, 'monitor_poll_interval_sec', 2) or 2))

    final_status = 'closed' if state.remaining_quantity <= 0 or protection_status == 'flat' else 'monitoring'
    final_exit_reason = tracked.get('exit_reason')
    persist_position(
        status=final_status,
        protection_status='flat' if final_status == 'closed' else protection_status,
        active_stop_order=active_stop_order if final_status != 'closed' else None,
        exit_reason=final_exit_reason if final_status == 'closed' else None,
        closed_at=utc_now() if final_status == 'closed' else None,
    )
    return {
        'ok': True,
        'mode': 'foreground',
        'symbol': symbol,
        'status': final_status,
        'remaining_quantity': round(state.remaining_quantity, 10),
        'stop_order_id': active_stop_order.get('orderId') if isinstance(active_stop_order, dict) and final_status != 'closed' else None,
        'protection_status': 'flat' if final_status == 'closed' else protection_status,
        'exit_reason': final_exit_reason if final_status == 'closed' else None,
        'realized_r': round(state.realized_r, 4),
    }


def start_trade_monitor_thread(
    client: Any,
    symbol: str,
    meta: Any,
    args: argparse.Namespace,
    trade: Dict[str, Any],
    store: Any,
    *,
    monitor_live_trade_fn: Callable[..., Dict[str, Any]],
    thread_factory: Callable[..., threading.Thread] = threading.Thread,
):
    thread_name = f"trade-monitor-{str(symbol or '').upper()}"
    thread = thread_factory(
        target=monitor_live_trade_fn,
        kwargs={
            'client': client,
            'symbol': symbol,
            'meta': meta,
            'args': args,
            'trade': trade,
            'store': store,
        },
        daemon=True,
        name=thread_name,
    )
    thread.start()
    return thread
