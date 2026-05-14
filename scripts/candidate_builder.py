from __future__ import annotations

from typing import Any, Dict, List, Optional


def finalize_candidate_construction(
    *,
    Candidate,
    symbol: str,
    last_price: float,
    price_change_pct_24h: float,
    quote_volume_24h: float,
    hot_rank: Optional[int],
    gainer_rank: Optional[int],
    funding_rate: Optional[float],
    funding_rate_avg: Optional[float],
    recent_5m_change_pct: float,
    acceleration_ratio: float,
    breakout_level: float,
    recent_swing_low: float,
    stop_price: float,
    quantity: float,
    risk_per_unit: float,
    recommended_leverage: int,
    rsi_5m: float,
    volume_multiple: float,
    distance_from_ema20_5m_pct: float,
    distance_from_vwap_15m_pct: float,
    trend_1h: Dict[str, Any],
    trend_4h: Dict[str, Any],
    score: float,
    reasons: List[str],
    trade_side: str,
    position_side: str,
    higher_timeframe_bias: str,
    oi_features: Dict[str, Any],
    microstructure_inputs: Dict[str, Any],
    atr_stop_distance: float,
    stop_model: str,
    stop_distance_pct: float,
    stop_too_tight_flag: bool,
    stop_too_wide_flag: bool,
    state_payload: Dict[str, Any],
    okx_sentiment_score: float,
    okx_sentiment_acceleration: float,
    sector_resonance_score: float,
    smart_money_effective: float,
    leading_payload: Dict[str, Any],
    squeeze_payload: Dict[str, Any],
    control_risk_payload: Dict[str, Any],
    initial_alert_tier: str,
    initial_position_size_pct: float,
    regime_label: str,
    regime_multiplier: float,
    onchain_smart_money_score: float,
    smart_money_merge: Dict[str, Any],
    entry_distance_from_breakout_pct: float,
    entry_distance_from_vwap_pct: float,
    overextension_flag: bool,
    setup_ready: bool,
    trigger_fired: bool,
    expected_slippage_pct: float,
    book_depth_fill_ratio: float,
    liquidity_grade: str,
    funding_rate_threshold: float,
    tradeability_score: float,
    loser_rank: Optional[int],
    trigger_confirmation: Dict[str, Any],
    legacy_kwargs: Dict[str, Any],
    waiting_breakout: bool,
):
    candidate = Candidate(
        symbol=symbol,
        last_price=last_price,
        price_change_pct_24h=price_change_pct_24h,
        quote_volume_24h=quote_volume_24h,
        hot_rank=hot_rank,
        gainer_rank=gainer_rank,
        funding_rate=funding_rate,
        funding_rate_avg=funding_rate_avg,
        recent_5m_change_pct=recent_5m_change_pct,
        acceleration_ratio_5m_vs_15m=acceleration_ratio,
        breakout_level=breakout_level,
        recent_swing_low=recent_swing_low,
        stop_price=stop_price,
        quantity=quantity,
        risk_per_unit=risk_per_unit,
        recommended_leverage=recommended_leverage,
        rsi_5m=rsi_5m,
        volume_multiple=volume_multiple,
        distance_from_ema20_5m_pct=distance_from_ema20_5m_pct,
        distance_from_vwap_15m_pct=distance_from_vwap_15m_pct,
        higher_tf_summary={'1h': trend_1h, '4h': trend_4h},
        score=score,
        reasons=reasons,
        side=trade_side,
        position_side=position_side,
        trigger_type='breakout',
        higher_timeframe_bias=higher_timeframe_bias,
        oi_change_pct_5m=oi_features['oi_change_pct_5m'],
        oi_change_pct_15m=oi_features['oi_change_pct_15m'],
        oi_acceleration_ratio=oi_features['oi_acceleration_ratio'],
        taker_buy_ratio=oi_features.get('taker_buy_ratio'),
        long_short_ratio=microstructure_inputs.get('long_short_ratio'),
        short_bias=float(microstructure_inputs.get('short_bias', 0.0) or 0.0),
        oi_zscore_5m=oi_features.get('oi_zscore_5m', 0.0),
        volume_zscore_5m=oi_features.get('volume_zscore_5m', 0.0),
        bollinger_bandwidth_pct=oi_features.get('bollinger_bandwidth_pct', 0.0),
        price_above_vwap=oi_features.get('price_above_vwap', False),
        funding_rate_percentile_hint=oi_features.get('funding_rate_percentile_hint'),
        cvd_delta=oi_features.get('cvd_delta', 0.0),
        cvd_zscore=oi_features.get('cvd_zscore', 0.0),
        atr_stop_distance=atr_stop_distance,
        stop_model=stop_model,
        stop_distance_pct=stop_distance_pct,
        stop_too_tight_flag=stop_too_tight_flag,
        stop_too_wide_flag=stop_too_wide_flag,
        state=state_payload['state'],
        state_reasons=state_payload['state_reasons'],
        setup_score=state_payload['setup_score'],
        exhaustion_score=state_payload['exhaustion_score'],
        okx_sentiment_score=okx_sentiment_score,
        okx_sentiment_acceleration=okx_sentiment_acceleration,
        sector_resonance_score=sector_resonance_score,
        smart_money_flow_score=smart_money_effective,
        leading_sentiment_delta=leading_payload['score'],
        squeeze_score=squeeze_payload['score'],
        control_risk_score=control_risk_payload['score'],
        alert_tier=initial_alert_tier,
        position_size_pct=initial_position_size_pct,
        regime_label=regime_label,
        regime_multiplier=regime_multiplier,
        onchain_smart_money_score=onchain_smart_money_score,
        smart_money_veto=bool(smart_money_merge['veto'] or control_risk_payload['veto']),
        smart_money_veto_reason=smart_money_merge['veto_reason'] or control_risk_payload['veto_reason'],
        smart_money_sources=list(smart_money_merge['sources']),
        entry_distance_from_breakout_pct=entry_distance_from_breakout_pct,
        entry_distance_from_vwap_pct=entry_distance_from_vwap_pct,
        overextension_flag=overextension_flag,
        setup_ready=setup_ready,
        trigger_fired=trigger_fired,
        expected_slippage_pct=expected_slippage_pct,
        book_depth_fill_ratio=book_depth_fill_ratio,
        liquidity_grade=liquidity_grade,
        loser_rank=loser_rank,
        trigger_confirmation_flags=dict(trigger_confirmation['flags']),
        trigger_confirmation_count=int(trigger_confirmation['confirmation_count']),
        trigger_min_confirmations=int(trigger_confirmation['min_confirmations']),
        oi_hard_reversal_threshold_pct=float(legacy_kwargs.get('oi_hard_reversal_threshold_pct', 0.8) or 0.8),
        portfolio_narrative_bucket='',
        portfolio_correlation_group='',
        tradeability_score=round(max(0.0, min(tradeability_score, 1.0)), 4),
    )
    candidate.must_pass_flags = {
        **dict(candidate.must_pass_flags or {}),
        **dict(trigger_confirmation['flags']),
        'setup_ready': setup_ready,
        'trigger_fired': trigger_fired,
    }
    candidate.reasons.append(f"trigger_confirmation_count={candidate.trigger_confirmation_count}")
    candidate.reasons.append(f"trigger_min_confirmations={candidate.trigger_min_confirmations}")
    if waiting_breakout:
        candidate.reasons.append('waiting_breakout')
    candidate.reasons.append(f'alert_tier={candidate.alert_tier}')
    candidate.reasons.append(f'position_size_pct={candidate.position_size_pct}')
    return candidate


def build_candidate(
    symbol: str,
    ticker: Dict[str, Any],
    klines_5m: List[List[Any]],
    klines_15m: List[List[Any]],
    klines_1h: List[List[Any]],
    klines_4h: List[List[Any]],
    meta,
    hot_rank: Optional[int],
    gainer_rank: Optional[int],
    risk_usdt: float,
    lookback_bars: int,
    swing_bars: int,
    min_5m_change_pct: float,
    min_quote_volume: float,
    stop_buffer_pct: float,
    max_rsi_5m: float,
    min_volume_multiple: float,
    max_distance_from_ema_pct: float,
    funding_rate: Optional[float],
    funding_rate_threshold: float,
    funding_rate_avg: Optional[float] = None,
    funding_rate_avg_threshold: float = 0.0003,
    max_distance_from_vwap_pct: float = 10.0,
    max_leverage: int = 5,
    base_acceleration_ratio: float = 1.25,
    trigger_min_confirmations: int = 2,
    loser_rank: Optional[int] = None,
    okx_sentiment_score: float = 0.0,
    okx_sentiment_acceleration: float = 0.0,
    sector_resonance_score: float = 0.0,
    smart_money_flow_score: float = 0.0,
    microstructure_inputs: Optional[Dict[str, Any]] = None,
    max_notional_usdt: float = 0.0,
    side: str = 'long',
    *,
    Candidate,
    TRADE_SIDE_LONG: str,
    TRADE_SIDE_SHORT: str,
    normalize_trade_side,
    trade_side_to_position_side,
    derive_regime_entry_thresholds,
    _to_float,
    derive_external_setup_params,
    extract_closes,
    extract_highs,
    extract_lows,
    extract_volumes,
    round_price,
    round_step,
    compute_rsi,
    compute_ema,
    compute_vwap,
    compute_atr,
    compute_zscore,
    compute_bollinger_bandwidth_pct,
    evaluate_higher_timeframe_trend,
    compute_macd,
    compute_sentiment_resonance_bonus,
    compute_leading_sentiment_signal,
    merge_smart_money_scores,
    compute_relative_oi_features,
    compute_squeeze_signal,
    compute_control_risk_score,
    classify_candidate_state,
    recommend_leverage,
    evaluate_trigger_confirmation,
    clamp,
    classify_alert_tier,
    recommended_position_size_pct,
    build_trade_management_plan,
    **legacy_kwargs: Any,
):
    early_reject_stats = legacy_kwargs.get('early_reject_stats')

    def early_reject(reason: str) -> None:
        if not isinstance(early_reject_stats, dict):
            return
        by_reason = early_reject_stats.setdefault('by_reason', {})
        by_side = early_reject_stats.setdefault('by_side', {})
        reason_text = str(reason or 'unknown')
        by_reason[reason_text] = int(by_reason.get(reason_text, 0) or 0) + 1
        side_text = normalize_trade_side(side)
        side_bucket = by_side.setdefault(side_text, {})
        side_bucket[reason_text] = int(side_bucket.get(reason_text, 0) or 0) + 1
        early_reject_stats['total'] = int(early_reject_stats.get('total', 0) or 0) + 1

    if len(klines_5m) < max(lookback_bars + 2, swing_bars + 20, 30):
        early_reject('insufficient_5m_klines')
        return None
    if len(klines_15m) < 20 or len(klines_1h) < 25 or len(klines_4h) < 25:
        early_reject('insufficient_higher_tf_klines')
        return None

    trade_side = normalize_trade_side(side)
    position_side = trade_side_to_position_side(trade_side)
    higher_timeframe_bias = trade_side
    regime_payload = legacy_kwargs.get('market_regime') or {}
    regime_label = str(regime_payload.get('label', 'neutral') or 'neutral')
    entry_thresholds = derive_regime_entry_thresholds(trade_side, regime_label, min_5m_change_pct, base_acceleration_ratio=base_acceleration_ratio)
    effective_min_5m_change_pct = float(entry_thresholds.get('min_5m_change_pct', min_5m_change_pct) or 0.0)
    effective_acceleration_threshold = float(entry_thresholds.get('acceleration_ratio', 1.5) or 1.5)
    setup_breakout_tolerance_pct = max(_to_float(legacy_kwargs.get('setup_breakout_tolerance_pct'), default=0.0), 0.0)
    watch_breakout_tolerance_pct = max(
        _to_float(legacy_kwargs.get('watch_breakout_tolerance_pct'), default=setup_breakout_tolerance_pct),
        0.0,
    )
    external_setup = derive_external_setup_params(
        legacy_kwargs.get('external_signal'),
        enabled=bool(legacy_kwargs.get('use_external_setup_relaxation')),
    )
    if external_setup.get('enabled'):
        effective_min_5m_change_pct *= float(external_setup.get('min_5m_change_pct_multiplier', 1.0) or 1.0)
        min_volume_multiple *= float(external_setup.get('min_volume_multiple_multiplier', 1.0) or 1.0)
        min_quote_volume = min(float(min_quote_volume or 0.0), float(external_setup.get('min_quote_volume', min_quote_volume) or min_quote_volume))

    closes_5m = extract_closes(klines_5m)
    highs_5m = extract_highs(klines_5m)
    lows_5m = extract_lows(klines_5m)
    volumes_5m = extract_volumes(klines_5m)
    closes_15m = extract_closes(klines_15m)

    last_price = closes_5m[-1]
    prev_close = closes_5m[-2]
    recent_5m_change_pct_raw = ((last_price / prev_close) - 1.0) * 100 if prev_close else 0.0
    recent_5m_change_pct = recent_5m_change_pct_raw if trade_side == TRADE_SIDE_LONG else -recent_5m_change_pct_raw
    if trade_side == TRADE_SIDE_SHORT:
        breakout_level = min(lows_5m[-(lookback_bars + 1):-1])
        recent_swing_low = max(highs_5m[-(swing_bars + 1):-1])
        stop_price_raw = recent_swing_low * (1.0 + stop_buffer_pct)
    else:
        breakout_level = max(highs_5m[-(lookback_bars + 1):-1])
        recent_swing_low = min(lows_5m[-(swing_bars + 1):-1])
        stop_price_raw = recent_swing_low * (1.0 - stop_buffer_pct)
    if breakout_level:
        entry_distance_from_breakout_pct = (((last_price / breakout_level) - 1.0) * 100) if trade_side == TRADE_SIDE_LONG else (((breakout_level / last_price) - 1.0) * 100)
    else:
        entry_distance_from_breakout_pct = 0.0
    near_external_breakout_setup = bool(
        external_setup.get('enabled')
        and entry_distance_from_breakout_pct >= -float(external_setup.get('max_breakout_distance_pct', 0.0) or 0.0)
    )
    near_configured_watch_setup = bool(
        watch_breakout_tolerance_pct > 0.0
        and entry_distance_from_breakout_pct >= -watch_breakout_tolerance_pct
    )
    near_configured_setup = bool(
        setup_breakout_tolerance_pct > 0.0
        and entry_distance_from_breakout_pct >= -setup_breakout_tolerance_pct
    )
    near_breakout_setup = near_external_breakout_setup or near_configured_watch_setup
    stop_price = round_price(stop_price_raw, meta.tick_size, meta.price_precision)
    structure_stop_price = stop_price
    stop_model = 'structure'
    if stop_price <= 0:
        early_reject('invalid_stop_price')
        return None
    if trade_side == TRADE_SIDE_SHORT and stop_price <= last_price:
        early_reject('invalid_short_stop_distance')
        return None
    if trade_side == TRADE_SIDE_LONG and stop_price >= last_price:
        early_reject('invalid_long_stop_distance')
        return None
    risk_per_unit = abs(last_price - stop_price)
    if risk_per_unit <= 0:
        early_reject('invalid_risk_per_unit')
        return None
    quantity = round_step(risk_usdt / risk_per_unit, meta.step_size, meta.quantity_precision)
    if max_notional_usdt > 0:
        max_qty_by_notional = round_step(max_notional_usdt / last_price, meta.step_size, meta.quantity_precision)
        quantity = min(quantity, max_qty_by_notional)
    if quantity < meta.min_qty or quantity <= 0:
        early_reject('quantity_below_min_qty')
        return None

    rsi_5m = compute_rsi(closes_5m, period=14)
    avg_volume_20 = sum(volumes_5m[-21:-1]) / 20
    volume_multiple = (volumes_5m[-1] / avg_volume_20) if avg_volume_20 > 0 else 0.0
    ema20_5m = compute_ema(closes_5m, 20)
    distance_from_ema20_5m_pct_raw = ((last_price / ema20_5m) - 1.0) * 100 if ema20_5m else 0.0
    distance_from_ema20_5m_pct = distance_from_ema20_5m_pct_raw if trade_side == TRADE_SIDE_LONG else -distance_from_ema20_5m_pct_raw
    vwap_15m = compute_vwap(klines_15m[-20:])
    distance_from_vwap_15m_pct_raw = ((last_price / vwap_15m) - 1.0) * 100 if vwap_15m else 0.0
    distance_from_vwap_15m_pct = distance_from_vwap_15m_pct_raw if trade_side == TRADE_SIDE_LONG else -distance_from_vwap_15m_pct_raw
    atr_stop_distance = compute_atr(klines_5m, period=14) * 1.5
    oi_change_samples_5m = []
    if len(closes_5m) >= 22:
        for idx in range(1, min(len(closes_5m), 22)):
            prev_close_i = closes_5m[-(idx + 1)]
            curr_close_i = closes_5m[-idx]
            if prev_close_i:
                oi_change_samples_5m.append(((curr_close_i / prev_close_i) - 1.0) * 100)
    volume_samples_5m = volumes_5m[-21:-1] if len(volumes_5m) >= 21 else volumes_5m[:-1]
    oi_zscore_price_proxy = compute_zscore(recent_5m_change_pct, oi_change_samples_5m)
    volume_zscore_price_proxy = compute_zscore(volumes_5m[-1], volume_samples_5m)
    bollinger_bandwidth_pct = compute_bollinger_bandwidth_pct(closes_5m, period=20)
    price_above_vwap = bool(vwap_15m and (last_price >= vwap_15m if trade_side == TRADE_SIDE_LONG else last_price <= vwap_15m))

    if atr_stop_distance > 0:
        atr_stop_price_raw = last_price - atr_stop_distance if trade_side == TRADE_SIDE_LONG else last_price + atr_stop_distance
        atr_stop_price = round_price(atr_stop_price_raw, meta.tick_size, meta.price_precision)
        atr_stop_valid = 0 < atr_stop_price < last_price if trade_side == TRADE_SIDE_LONG else atr_stop_price > last_price
        if atr_stop_valid:
            prior_stop_price = stop_price
            stop_price = max(stop_price, atr_stop_price) if trade_side == TRADE_SIDE_LONG else min(stop_price, atr_stop_price)
            if abs(stop_price - atr_stop_price) <= max(float(meta.tick_size or 0.0), 1e-12) and abs(prior_stop_price - atr_stop_price) > max(float(meta.tick_size or 0.0), 1e-12):
                stop_model = 'atr'
            elif abs(stop_price - structure_stop_price) > max(float(meta.tick_size or 0.0), 1e-12):
                stop_model = 'blended'
            risk_per_unit = abs(last_price - stop_price)
            if risk_per_unit <= 0:
                early_reject('invalid_atr_risk_per_unit')
                return None
            quantity = round_step(risk_usdt / risk_per_unit, meta.step_size, meta.quantity_precision)
            if max_notional_usdt > 0:
                max_qty_by_notional = round_step(max_notional_usdt / last_price, meta.step_size, meta.quantity_precision)
                quantity = min(quantity, max_qty_by_notional)
            if quantity < meta.min_qty or quantity <= 0:
                early_reject('atr_quantity_below_min_qty')
                return None
    stop_distance_pct = (risk_per_unit / last_price) * 100 if last_price else 0.0
    stop_too_tight_flag = bool(stop_distance_pct > 0 and stop_distance_pct < 0.08)
    stop_too_wide_flag = bool(stop_distance_pct > max(8.0, min(float(max_distance_from_vwap_pct or 0.0) * 1.5, 12.0)))

    trend_1h = evaluate_higher_timeframe_trend(klines_1h, side=trade_side)
    trend_4h = evaluate_higher_timeframe_trend(klines_4h, side=trade_side)
    higher_tf_allowed = trend_1h['allowed'] or trend_4h['allowed']
    macd_5m = compute_macd(closes_5m)
    structure_break = last_price > max(closes_5m[-6:-1]) if trade_side == TRADE_SIDE_LONG else last_price < min(closes_5m[-6:-1])
    avg_15m_change_pct = 0.0
    if len(closes_15m) >= 5:
        pct_changes_15m = []
        for prev, curr in zip(closes_15m[-5:-1], closes_15m[-4:]):
            if prev:
                pct_changes_15m.append(((curr / prev) - 1.0) * 100)
        avg_15m_change_pct = sum(pct_changes_15m) / len(pct_changes_15m) if pct_changes_15m else 0.0
    if trade_side == TRADE_SIDE_SHORT:
        avg_15m_signal = -avg_15m_change_pct
        acceleration_ratio = recent_5m_change_pct / avg_15m_signal if avg_15m_signal > 0 else (99.0 if recent_5m_change_pct > 0 else 0.0)
    else:
        acceleration_ratio = recent_5m_change_pct / avg_15m_change_pct if avg_15m_change_pct > 0 else (99.0 if recent_5m_change_pct > 0 else 0.0)
    quote_volume_24h = _to_float(ticker.get('quoteVolume', 0.0))
    price_change_pct_24h = _to_float(ticker.get('priceChangePercent', 0.0))

    if trade_side == TRADE_SIDE_LONG and last_price <= breakout_level and not near_breakout_setup:
        early_reject('long_breakout_not_confirmed')
        return None
    if trade_side == TRADE_SIDE_SHORT and last_price >= breakout_level and not near_breakout_setup:
        early_reject('short_breakdown_not_confirmed')
        return None
    if recent_5m_change_pct < effective_min_5m_change_pct and not near_breakout_setup:
        early_reject('recent_5m_change_below_gate')
        return None
    if quote_volume_24h < min_quote_volume:
        early_reject('quote_volume_below_gate')
        return None
    if not higher_tf_allowed and not near_breakout_setup:
        early_reject('higher_timeframe_not_allowed')
        return None
    if volume_multiple < min_volume_multiple and not near_breakout_setup:
        early_reject('volume_multiple_below_gate')
        return None
    if trade_side == TRADE_SIDE_LONG:
        if funding_rate is not None and funding_rate > funding_rate_threshold:
            early_reject('long_funding_rate_above_gate')
            return None
        if funding_rate_avg is not None and funding_rate_avg > funding_rate_avg_threshold:
            early_reject('long_funding_rate_avg_above_gate')
            return None
    else:
        if funding_rate is not None and funding_rate < (-funding_rate_threshold):
            early_reject('short_funding_rate_below_gate')
            return None
        if funding_rate_avg is not None and funding_rate_avg < (-funding_rate_avg_threshold):
            early_reject('short_funding_rate_avg_below_gate')
            return None
    if not structure_break and not near_breakout_setup:
        early_reject('micro_structure_break_not_confirmed')
        return None
    if trade_side == TRADE_SIDE_LONG and macd_5m['hist'] <= macd_5m['prev_hist'] and not near_breakout_setup:
        early_reject('long_macd_hist_not_accelerating')
        return None
    if trade_side == TRADE_SIDE_SHORT and macd_5m['hist'] >= macd_5m['prev_hist'] and not near_breakout_setup:
        early_reject('short_macd_hist_not_accelerating')
        return None
    if acceleration_ratio < effective_acceleration_threshold and not near_breakout_setup:
        early_reject('acceleration_ratio_below_gate')
        return None

    reasons: List[str] = []
    score = 0.0
    reasons.append(f'min_5m_change_gate={effective_min_5m_change_pct:.2f}')
    reasons.append(f'acceleration_ratio_gate={effective_acceleration_threshold:.2f}')
    if external_setup.get('enabled'):
        reasons.append('external_accumulation_setup_relaxed')
        reasons.append(f"external_setup_score={float(external_setup.get('score', 0.0) or 0.0):.1f}")
        reasons.append(f"external_max_breakout_distance_pct={float(external_setup.get('max_breakout_distance_pct', 0.0) or 0.0):.2f}")
    if near_configured_watch_setup:
        reasons.append('configured_near_breakout_watch')
        reasons.append(f'watch_breakout_tolerance_pct={watch_breakout_tolerance_pct:.2f}')
    if near_configured_setup:
        reasons.append('configured_near_breakout_setup')
        reasons.append(f'setup_breakout_tolerance_pct={setup_breakout_tolerance_pct:.2f}')
    if hot_rank is not None:
        score += max(0.0, 1 - ((hot_rank - 1) / 10)) * 40
        reasons.append(f'square_hot_rank={hot_rank}')
    directional_rank = loser_rank if trade_side == TRADE_SIDE_SHORT else gainer_rank
    directional_rank_label = 'loser_rank' if trade_side == TRADE_SIDE_SHORT else 'gainer_rank'
    if directional_rank is not None:
        score += max(0.0, 1 - ((directional_rank - 1) / 20)) * 60
        reasons.append(f'{directional_rank_label}={directional_rank}')
    if hot_rank is not None and directional_rank is not None:
        score += 20
        reasons.append('hot_directional_mover_intersection')
    score += min(recent_5m_change_pct * 6, 20)
    reasons.append(f'recent_5m_change_pct={recent_5m_change_pct:.2f}')
    score += min(volume_multiple * 8, 20)
    reasons.append(f'volume_multiple={volume_multiple:.2f}')
    score += min(acceleration_ratio * 5, 15)
    reasons.append(f'acceleration_ratio={acceleration_ratio:.2f}')
    price_change_signal_24h = price_change_pct_24h if trade_side == TRADE_SIDE_LONG else -price_change_pct_24h
    score += min(max(price_change_signal_24h, 0.0), 15)
    reasons.append(f'price_change_24h={price_change_pct_24h:.2f}')
    reasons.extend([f'{trade_side}_breakout_confirmed', f'rsi_5m={rsi_5m:.2f}', f'distance_from_ema20_5m_pct={distance_from_ema20_5m_pct:.2f}', f'distance_from_vwap_15m_pct={distance_from_vwap_15m_pct:.2f}'])
    if funding_rate is not None:
        reasons.append(f'funding_rate={funding_rate:.5f}')
        funding_headroom = (funding_rate_threshold - funding_rate) if trade_side == TRADE_SIDE_LONG else (funding_rate + funding_rate_threshold)
        score += max(0.0, funding_headroom * 10000)
    if funding_rate_avg is not None:
        reasons.append(f'funding_rate_avg={funding_rate_avg:.5f}')
        funding_avg_headroom = (funding_rate_avg_threshold - funding_rate_avg) if trade_side == TRADE_SIDE_LONG else (funding_rate_avg + funding_rate_avg_threshold)
        score += max(0.0, funding_avg_headroom * 10000)
    reasons.append(f'stop_model={stop_model}')
    reasons.append(f'stop_distance_pct={stop_distance_pct:.2f}')
    if stop_too_tight_flag:
        score -= 8.0
        reasons.append('stop_too_tight_flag')
    if stop_too_wide_flag:
        score -= 10.0
        reasons.append('stop_too_wide_flag')

    okx_sentiment_payload = dict(legacy_kwargs.pop('okx_sentiment', {}) or {})
    smart_money_context_payload = dict(legacy_kwargs.pop('smart_money_context', {}) or {})
    okx_sentiment_score = float(okx_sentiment_payload.get('okx_sentiment_score', okx_sentiment_score) or 0.0)
    okx_sentiment_acceleration = float(okx_sentiment_payload.get('okx_sentiment_acceleration', okx_sentiment_acceleration) or 0.0)
    sector_resonance_score = float(okx_sentiment_payload.get('sector_resonance_score', sector_resonance_score) or 0.0)
    smart_money_flow_score = float(smart_money_context_payload.get('smart_money_flow_score', smart_money_flow_score) or 0.0)

    sentiment_bonus_payload = compute_sentiment_resonance_bonus(okx_sentiment_score, okx_sentiment_acceleration, sector_resonance_score, smart_money_flow_score, side=trade_side)
    score += sentiment_bonus_payload['net']
    reasons.extend(sentiment_bonus_payload['reasons'])
    if okx_sentiment_score:
        reasons.append(f'okx_sentiment_score={okx_sentiment_score:.2f}')
    if okx_sentiment_acceleration:
        reasons.append(f'okx_sentiment_acceleration={okx_sentiment_acceleration:.2f}')
    if sector_resonance_score:
        reasons.append(f'sector_resonance_score={sector_resonance_score:.2f}')
    if smart_money_flow_score:
        reasons.append(f'smart_money_flow_score={smart_money_flow_score:.2f}')
    leading_payload = compute_leading_sentiment_signal(okx_sentiment_score, okx_sentiment_acceleration, side=trade_side)
    score += leading_payload['score']
    reasons.extend(leading_payload['reasons'])

    microstructure_inputs = dict(microstructure_inputs or {})
    if legacy_kwargs:
        for key, value in legacy_kwargs.items():
            if key == 'market_regime':
                continue
            microstructure_inputs.setdefault(key, value)
    onchain_smart_money_score_raw = legacy_kwargs.get('onchain_smart_money_score')
    onchain_smart_money_score = float(onchain_smart_money_score_raw or 0.0)
    smart_money_merge = merge_smart_money_scores(
        exchange_score=smart_money_flow_score,
        onchain_score=onchain_smart_money_score_raw if onchain_smart_money_score_raw is not None else None,
        side=trade_side,
    )
    smart_money_effective = float(smart_money_merge['score'])

    if microstructure_inputs.get('long_short_ratio') is not None:
        reasons.append(f"long_short_ratio={float(microstructure_inputs['long_short_ratio']):.2f}")
    if microstructure_inputs.get('short_bias', 0.0) > 0:
        reasons.append(f"short_bias={float(microstructure_inputs['short_bias']):.2f}")
    if trend_1h['allowed']:
        score += 12
        reasons.append(f'trend_1h_{trade_side}')
    if trend_4h['allowed']:
        score += 12
        reasons.append(f'trend_4h_{trade_side}')
    if (trade_side == TRADE_SIDE_LONG and macd_5m['hist'] > 0) or (trade_side == TRADE_SIDE_SHORT and macd_5m['hist'] < 0):
        score += 8
        reasons.append(f'macd_hist_{trade_side}')
    if structure_break:
        score += 6
        reasons.append(f'micro_structure_break_{trade_side}')

    oi_features = compute_relative_oi_features(
        oi_now=microstructure_inputs.get('oi_now'),
        oi_5m_ago=microstructure_inputs.get('oi_5m_ago'),
        oi_15m_ago=microstructure_inputs.get('oi_15m_ago'),
        taker_buy_ratio=microstructure_inputs.get('taker_buy_ratio'),
        funding_rate=funding_rate,
        funding_rate_avg=funding_rate_avg,
        oi_change_samples_5m=microstructure_inputs.get('oi_change_samples_5m'),
        volume_samples_5m=microstructure_inputs.get('volume_samples_5m'),
        latest_volume_5m=microstructure_inputs.get('latest_volume_5m'),
        cvd_samples=microstructure_inputs.get('cvd_samples'),
        cvd_delta=float(microstructure_inputs.get('cvd_delta', 0.0) or 0.0),
        bollinger_bandwidth_pct=bollinger_bandwidth_pct,
        price_above_vwap=price_above_vwap,
        oi_notional_history=microstructure_inputs.get('oi_notional_history'),
        short_bias=float(microstructure_inputs.get('short_bias', 0.0) or 0.0),
    )
    oi_features.update({
        'oi_zscore_5m': max(float(oi_features.get('oi_zscore_5m', 0.0) or 0.0), oi_zscore_price_proxy),
        'volume_zscore_5m': max(float(oi_features.get('volume_zscore_5m', 0.0) or 0.0), volume_zscore_price_proxy),
        'bollinger_bandwidth_pct': bollinger_bandwidth_pct,
        'price_above_vwap': price_above_vwap,
    })

    squeeze_payload = compute_squeeze_signal(
        funding_rate=funding_rate,
        funding_rate_avg=funding_rate_avg,
        short_bias=float(oi_features.get('short_bias', 0.0) or 0.0),
        oi_zscore_5m=float(oi_features.get('oi_zscore_5m', 0.0) or 0.0),
        cvd_delta=float(oi_features.get('cvd_delta', 0.0) or 0.0),
        cvd_zscore=float(oi_features.get('cvd_zscore', 0.0) or 0.0),
        recent_5m_change_pct=recent_5m_change_pct,
        side=trade_side,
    )
    score += squeeze_payload['score']
    reasons.extend(squeeze_payload['reasons'])

    control_risk_payload = compute_control_risk_score(
        short_bias=float(oi_features.get('short_bias', 0.0) or 0.0),
        oi_notional_percentile=float(oi_features.get('oi_notional_percentile', 0.0) or 0.0),
        smart_money_flow_score=smart_money_effective,
        side=trade_side,
    )
    score -= control_risk_payload['score']
    reasons.extend(control_risk_payload['reasons'])

    state_payload = classify_candidate_state(
        recent_5m_change_pct=recent_5m_change_pct,
        volume_multiple=volume_multiple,
        acceleration_ratio=acceleration_ratio,
        oi_features=oi_features,
        rsi_5m=rsi_5m,
        distance_from_ema20_5m_pct=distance_from_ema20_5m_pct,
        distance_from_vwap_15m_pct=distance_from_vwap_15m_pct,
        funding_rate=funding_rate,
        funding_rate_avg=funding_rate_avg,
        higher_tf_allowed=higher_tf_allowed,
        price_change_pct_24h=price_change_pct_24h,
        side=trade_side,
    )
    squeeze_reason = 'launch_short_squeeze' if trade_side == TRADE_SIDE_LONG else 'launch_long_squeeze'
    if squeeze_payload['score'] >= 12 and state_payload['state'] in {'squeeze', 'overheated', 'momentum_extension'}:
        state_payload = {
            **state_payload,
            'state': 'launch',
            'state_reasons': list(state_payload.get('state_reasons', [])) + [squeeze_reason],
            'exhaustion_score': min(float(state_payload.get('exhaustion_score', 0.0) or 0.0), max(float(state_payload.get('setup_score', 0.0) or 0.0) - 0.5, 0.0)),
        }
    score += state_payload['setup_score'] - (state_payload['exhaustion_score'] * 0.5)
    reasons.extend(smart_money_merge['sources'])

    regime_multiplier = float(regime_payload.get('score_multiplier', 1.0) or 1.0)
    recommended_leverage = recommend_leverage(last_price, stop_price, max_leverage=max_leverage)
    if breakout_level:
        entry_distance_from_breakout_pct = (((last_price / breakout_level) - 1.0) * 100) if trade_side == TRADE_SIDE_LONG else (((breakout_level / last_price) - 1.0) * 100)
    else:
        entry_distance_from_breakout_pct = 0.0
    entry_distance_from_vwap_pct = abs(distance_from_vwap_15m_pct)
    short_squeeze_launch = squeeze_reason in state_payload.get('state_reasons', [])
    overextension_flag = bool(
        state_payload['state'] in {'overheated', 'momentum_extension'}
        or (short_squeeze_launch is False and entry_distance_from_breakout_pct >= max(min(max_distance_from_ema_pct * 0.5, 3.0), 0.75))
        or (short_squeeze_launch is False and entry_distance_from_vwap_pct >= max(min(max_distance_from_vwap_pct * 0.5, 3.0), 0.75))
    )
    trigger_confirmation = evaluate_trigger_confirmation(
        structure_break=structure_break,
        price_above_vwap=price_above_vwap,
        distance_from_ema20_5m_pct=distance_from_ema20_5m_pct,
        distance_from_vwap_15m_pct=distance_from_vwap_15m_pct,
        taker_buy_ratio=oi_features.get('taker_buy_ratio'),
        oi_change_pct_5m=oi_features.get('oi_change_pct_5m', 0.0),
        oi_change_pct_15m=oi_features.get('oi_change_pct_15m', 0.0),
        funding_rate=funding_rate,
        funding_rate_threshold=funding_rate_threshold,
        funding_rate_avg=funding_rate_avg,
        funding_rate_avg_threshold=funding_rate_avg_threshold,
        cvd_delta=oi_features.get('cvd_delta', 0.0),
        cvd_zscore=oi_features.get('cvd_zscore', 0.0),
        state=state_payload['state'],
        overextension_flag=overextension_flag,
        side=trade_side,
        min_confirmations=max(int(trigger_min_confirmations or 2), 1),
        long_short_ratio=microstructure_inputs.get('long_short_ratio'),
        price_change_pct_24h=price_change_pct_24h,
        recent_5m_change_pct=recent_5m_change_pct,
    )
    setup_ready = bool(trigger_confirmation['setup_ready'])
    trigger_fired = bool(trigger_confirmation['trigger_fired'])
    waiting_breakout = bool(
        near_breakout_setup
        and (
            (trade_side == TRADE_SIDE_LONG and last_price <= breakout_level)
            or (trade_side == TRADE_SIDE_SHORT and last_price >= breakout_level)
            or structure_break is False
        )
    )
    if waiting_breakout:
        trigger_fired = False
        trigger_confirmation['trigger_fired'] = False
        trigger_confirmation['flags']['waiting_breakout'] = True
        if near_external_breakout_setup is False and near_configured_setup is False:
            setup_ready = False
            trigger_confirmation['setup_ready'] = False
            trigger_confirmation['flags']['watch_only_breakout_distance'] = True
    expected_slippage_pct = round(max(entry_distance_from_breakout_pct, 0.0) * 0.35, 4)
    book_depth_fill_ratio = round(clamp(1.0 - (expected_slippage_pct / 2.0), 0.0, 1.0), 4)
    if book_depth_fill_ratio >= 0.85 and expected_slippage_pct <= 0.2:
        liquidity_grade = 'A'
    elif book_depth_fill_ratio >= 0.6 and expected_slippage_pct <= 0.5:
        liquidity_grade = 'B'
    else:
        liquidity_grade = 'C'
    tradeability_score = round(max(0.0, min((1.0 - max(expected_slippage_pct, 0.0)) * book_depth_fill_ratio, 1.0)), 4)
    initial_alert_tier = classify_alert_tier(score, state_payload['state'], regime_label)
    initial_position_size_pct = recommended_position_size_pct(score, initial_alert_tier, regime_multiplier)
    return finalize_candidate_construction(
        Candidate=Candidate,
        symbol=symbol,
        last_price=last_price,
        price_change_pct_24h=price_change_pct_24h,
        quote_volume_24h=quote_volume_24h,
        hot_rank=hot_rank,
        gainer_rank=gainer_rank,
        funding_rate=funding_rate,
        funding_rate_avg=funding_rate_avg,
        recent_5m_change_pct=recent_5m_change_pct,
        acceleration_ratio=acceleration_ratio,
        breakout_level=breakout_level,
        recent_swing_low=recent_swing_low,
        stop_price=stop_price,
        quantity=quantity,
        risk_per_unit=risk_per_unit,
        recommended_leverage=recommended_leverage,
        rsi_5m=rsi_5m,
        volume_multiple=volume_multiple,
        distance_from_ema20_5m_pct=distance_from_ema20_5m_pct,
        distance_from_vwap_15m_pct=distance_from_vwap_15m_pct,
        trend_1h=trend_1h,
        trend_4h=trend_4h,
        score=score,
        reasons=reasons,
        trade_side=trade_side,
        position_side=position_side,
        higher_timeframe_bias=higher_timeframe_bias,
        oi_features=oi_features,
        microstructure_inputs=microstructure_inputs,
        atr_stop_distance=atr_stop_distance,
        stop_model=stop_model,
        stop_distance_pct=stop_distance_pct,
        stop_too_tight_flag=stop_too_tight_flag,
        stop_too_wide_flag=stop_too_wide_flag,
        state_payload=state_payload,
        okx_sentiment_score=okx_sentiment_score,
        okx_sentiment_acceleration=okx_sentiment_acceleration,
        sector_resonance_score=sector_resonance_score,
        smart_money_effective=smart_money_effective,
        leading_payload=leading_payload,
        squeeze_payload=squeeze_payload,
        control_risk_payload=control_risk_payload,
        initial_alert_tier=initial_alert_tier,
        initial_position_size_pct=initial_position_size_pct,
        regime_label=regime_label,
        regime_multiplier=regime_multiplier,
        onchain_smart_money_score=onchain_smart_money_score,
        smart_money_merge=smart_money_merge,
        entry_distance_from_breakout_pct=entry_distance_from_breakout_pct,
        entry_distance_from_vwap_pct=entry_distance_from_vwap_pct,
        overextension_flag=overextension_flag,
        setup_ready=setup_ready,
        trigger_fired=trigger_fired,
        expected_slippage_pct=expected_slippage_pct,
        book_depth_fill_ratio=book_depth_fill_ratio,
        liquidity_grade=liquidity_grade,
        funding_rate_threshold=funding_rate_threshold,
        tradeability_score=tradeability_score,
        loser_rank=loser_rank,
        trigger_confirmation=trigger_confirmation,
        legacy_kwargs=legacy_kwargs,
        waiting_breakout=waiting_breakout,
    )
