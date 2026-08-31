from __future__ import annotations

import threading
from typing import Any, Dict


_ENTRY_SUBMIT_CAPTURE_LOCK = threading.RLock()


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalize_side(value: Any) -> str:
    text = str(value or '').strip().upper()
    if text in {'SELL', 'SHORT'}:
        return 'SHORT'
    return 'LONG'


def _directional_slippage_bps(side: Any, fill_price: float, reference_price: float) -> float | None:
    if fill_price <= 0 or reference_price <= 0:
        return None
    if _normalize_side(side) == 'SHORT':
        return (reference_price - fill_price) / reference_price * 10000.0
    return (fill_price - reference_price) / reference_price * 10000.0


def _candidate_executable_price(candidate: Any, side: Any) -> float:
    if _normalize_side(side) == 'SHORT':
        values = (
            getattr(candidate, 'best_bid_price', None),
            getattr(candidate, 'bid_price', None),
            getattr(candidate, 'best_bid', None),
        )
    else:
        values = (
            getattr(candidate, 'best_ask_price', None),
            getattr(candidate, 'ask_price', None),
            getattr(candidate, 'best_ask', None),
        )
    for value in values:
        price = _to_float(value)
        if price > 0:
            return price
    return _to_float(getattr(candidate, 'last_price', 0.0))


def _capture_submit_reference(client: Any, candidate: Any, order_params: Dict[str, Any]) -> tuple[float, str]:
    symbol = str(order_params.get('symbol') or getattr(candidate, 'symbol', '') or '').upper()
    side = order_params.get('side') or getattr(candidate, 'side', None) or getattr(candidate, 'position_side', None)
    ticker: Any = None
    if symbol:
        get_method = getattr(client, 'get', None)
        if callable(get_method):
            try:
                ticker = get_method(
                    '/fapi/v1/ticker/bookTicker',
                    params={'symbol': symbol},
                    timeout=3,
                    purpose='execution',
                )
            except TypeError:
                try:
                    ticker = get_method('/fapi/v1/ticker/bookTicker', params={'symbol': symbol}, timeout=3)
                except Exception:
                    ticker = None
            except Exception:
                ticker = None
        if ticker is None:
            public_get = getattr(client, 'public_get', None)
            if callable(public_get):
                try:
                    ticker = public_get('/fapi/v1/ticker/bookTicker', params={'symbol': symbol}, timeout=3)
                except Exception:
                    ticker = None
    if isinstance(ticker, dict):
        bid = _to_float(ticker.get('bidPrice') or ticker.get('bid_price'))
        ask = _to_float(ticker.get('askPrice') or ticker.get('ask_price'))
        if _normalize_side(side) == 'SHORT' and bid > 0:
            return bid, 'book_ticker_bid_at_submit'
        if _normalize_side(side) == 'LONG' and ask > 0:
            return ask, 'book_ticker_ask_at_submit'
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0, 'book_ticker_mid_at_submit'

    candidate_price = _candidate_executable_price(candidate, side)
    if candidate_price > 0:
        return candidate_price, 'candidate_executable_fallback'
    return 0.0, 'unavailable'


def _is_entry_order(candidate: Any, path: Any, params: Any) -> bool:
    if str(path or '') != '/fapi/v1/order' or not isinstance(params, dict):
        return False
    if bool(params.get('reduceOnly')) or bool(params.get('closePosition')):
        return False
    if params.get('stopPrice') not in (None, '') or params.get('activationPrice') not in (None, ''):
        return False
    order_type = str(params.get('type') or '').upper()
    if order_type not in {'MARKET', 'LIMIT'}:
        return False
    expected_side = 'SELL' if _normalize_side(getattr(candidate, 'side', getattr(candidate, 'position_side', None))) == 'SHORT' else 'BUY'
    return str(params.get('side') or '').upper() == expected_side


def normalize_entry_order_feedback(candidate: Any, live_execution: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(live_execution or {})
    raw_feedback = normalized.get('entry_order_feedback', {})
    feedback = dict(raw_feedback) if isinstance(raw_feedback, dict) else {}

    fill_price = _to_float(
        normalized.get('entry_price')
        or feedback.get('fill_price')
        or feedback.get('avg_price')
    )
    reference_price = _to_float(
        feedback.get('market_price_at_submit')
        or getattr(candidate, 'last_price', 0.0)
    )
    side = getattr(candidate, 'position_side', None) or getattr(candidate, 'side', None)
    predicted_bps = max(_to_float(feedback.get('predicted_slippage_bps')), 0.0)

    if fill_price > 0:
        feedback['fill_price'] = fill_price
    if reference_price > 0:
        feedback['market_price_at_submit'] = reference_price
    if predicted_bps > 0 and reference_price > 0 and not feedback.get('predicted_fill_price'):
        direction = -1.0 if _normalize_side(side) == 'SHORT' else 1.0
        feedback['predicted_fill_price'] = round(
            reference_price * (1.0 + direction * predicted_bps / 10000.0),
            12,
        )

    directional_bps = _directional_slippage_bps(side, fill_price, reference_price)
    if directional_bps is not None:
        feedback['actual_fill_slippage_bps'] = round(directional_bps, 4)
        feedback['actual_fill_slippage_abs_bps'] = round(abs(directional_bps), 4)
        feedback['slippage_error_bps'] = round(directional_bps - predicted_bps, 4)
        feedback['within_expected_slippage'] = directional_bps <= predicted_bps

    liquidity_grade = feedback.get('liquidity_grade_at_entry') or feedback.get('liquidity_grade')
    if liquidity_grade not in (None, ''):
        feedback['liquidity_grade_at_entry'] = liquidity_grade

    normalized['entry_order_feedback'] = feedback
    return normalized


def install_execution_feedback_hardening(strategy_module: Any) -> None:
    original = getattr(strategy_module, 'place_live_trade', None)
    if not callable(original) or getattr(original, '_execution_feedback_hardening_hook', False):
        return

    def place_live_trade_with_feedback(*args: Any, **kwargs: Any):
        candidate = kwargs.get('candidate')
        if candidate is None and len(args) >= 2:
            candidate = args[1]
        client = kwargs.get('client')
        if client is None and args:
            client = args[0]

        captured: Dict[str, Any] = {}
        signed_post = getattr(client, 'signed_post', None) if client is not None else None
        patched = False
        if candidate is not None and callable(signed_post):
            def signed_post_with_submit_reference(path: Any, params: Any = None, *post_args: Any, **post_kwargs: Any):
                if _is_entry_order(candidate, path, params):
                    price, source = _capture_submit_reference(client, candidate, dict(params))
                    if price > 0:
                        captured['market_price_at_submit'] = price
                        captured['market_price_source'] = source
                return signed_post(path, params, *post_args, **post_kwargs)

            try:
                with _ENTRY_SUBMIT_CAPTURE_LOCK:
                    setattr(client, 'signed_post', signed_post_with_submit_reference)
                    patched = True
                    result = original(*args, **kwargs)
            finally:
                if patched:
                    setattr(client, 'signed_post', signed_post)
        else:
            result = original(*args, **kwargs)

        if not isinstance(result, dict) or candidate is None:
            return result
        normalized = dict(result)
        raw_feedback = normalized.get('entry_order_feedback', {})
        feedback = dict(raw_feedback) if isinstance(raw_feedback, dict) else {}
        if captured.get('market_price_at_submit'):
            feedback['market_price_at_submit'] = round(float(captured['market_price_at_submit']), 12)
            feedback['market_price_source'] = captured.get('market_price_source')
            normalized['entry_order_feedback'] = feedback
        return normalize_entry_order_feedback(candidate, normalized)

    place_live_trade_with_feedback._execution_feedback_hardening_hook = True  # type: ignore[attr-defined]
    strategy_module.place_live_trade = place_live_trade_with_feedback
