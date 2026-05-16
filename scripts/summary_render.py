from typing import Any, Dict, Iterable, List, Sequence


def build_cn_scan_summary_data(result: Dict[str, Any], mask_sensitive_token):
    return build_cn_scan_summary(result, mask_sensitive_token)


def render_cn_scan_summary_text(summary: Dict[str, Any], format_num, format_pct):
    return render_cn_scan_summary(summary, format_num, format_pct)


def top_dict_items(data: Dict[str, Any], limit: int = 4) -> List[tuple[str, Any]]:
    if not isinstance(data, dict):
        return []
    return sorted(data.items(), key=lambda item: item[1], reverse=True)[:limit]


def build_runtime_health_summary(cycle: Dict[str, Any], mask_sensitive_token) -> Dict[str, Any]:
    book_ticker = cycle.get('book_ticker_websocket', {}) if isinstance(cycle, dict) else {}
    user_data_stream_monitor = cycle.get('user_data_stream_monitor', {}) if isinstance(cycle, dict) else {}
    user_data_stream_alert = cycle.get('user_data_stream_alert', {}) if isinstance(cycle, dict) else {}

    runtime_summary: Dict[str, Dict[str, Any]] = {
        'BookTicker WS': {},
        'User Data Stream': {},
        '告警': {},
    }

    if isinstance(book_ticker, dict) and book_ticker:
        book_ticker_health = book_ticker.get('health', {}) if isinstance(book_ticker.get('health'), dict) else {}
        runtime_summary['BookTicker WS'] = {
            '状态': book_ticker.get('status', '-'),
            '原因': book_ticker.get('reason', ''),
            '活跃流': book_ticker_health.get('active_streams', book_ticker.get('active_streams', [])),
            '订阅版本': book_ticker_health.get('subscription_version', book_ticker.get('subscription_version')),
        }

    if isinstance(user_data_stream_monitor, dict) and user_data_stream_monitor:
        uds_health = user_data_stream_monitor.get('health', {}) if isinstance(user_data_stream_monitor.get('health'), dict) else {}
        runtime_summary['User Data Stream'] = {
            '状态': user_data_stream_monitor.get('status', '-'),
            '动作': user_data_stream_monitor.get('action', ''),
            'listen_key': mask_sensitive_token(user_data_stream_monitor.get('listen_key', ''), prefix=2, suffix=2),
            '断线次数': uds_health.get('disconnect_count', user_data_stream_monitor.get('disconnect_count')),
            '续期失败次数': uds_health.get('refresh_failure_count', user_data_stream_monitor.get('refresh_failure_count')),
            '重连次数': uds_health.get('reconnect_count', user_data_stream_monitor.get('reconnect_count')),
            '更新时间': uds_health.get('updated_at', user_data_stream_monitor.get('updated_at')),
            '最近错误': user_data_stream_monitor.get('error', uds_health.get('last_error', uds_health.get('detail', ''))),
        }

    if isinstance(user_data_stream_alert, dict) and user_data_stream_alert:
        runtime_summary['告警'] = {
            '级别': user_data_stream_alert.get('level', user_data_stream_alert.get('status', '-')),
            '消息': user_data_stream_alert.get('message') or user_data_stream_alert.get('error', ''),
            'listen_key': mask_sensitive_token(user_data_stream_alert.get('listen_key', '')) if user_data_stream_alert.get('listen_key') else '',
            '更新时间': user_data_stream_alert.get('updated_at'),
        }

    return runtime_summary


def build_selected_summary(selected: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if not isinstance(selected, dict):
        return None
    return {
        '交易对': selected.get('symbol', '-'),
        '评级': selected.get('alert_tier', '-'),
        '状态': selected.get('state', '-'),
        '得分': selected.get('score'),
        '建议仓位': selected.get('position_size_pct'),
        '入场价': selected.get('entry_price', selected.get('last_price')),
        '止损价': selected.get('stop_price'),
        '预期滑点R': selected.get('expected_slippage_r'),
        '流动性': selected.get('execution_liquidity_grade', selected.get('liquidity_grade')),
        '理由': selected.get('reasons', [])[:6],
    }


def _build_candidate_row(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        '交易对': item.get('symbol', '-'),
        '评级': item.get('alert_tier', '-'),
        '状态': item.get('state', '-'),
        '得分': item.get('score'),
        '24h涨幅': item.get('price_change_pct_24h'),
        '5m涨幅': item.get('recent_5m_change_pct'),
        '建议仓位': item.get('position_size_pct'),
        '流动性': item.get('execution_liquidity_grade', item.get('liquidity_grade')),
    }


def _build_tradeability_block_row(item: Dict[str, Any]) -> Dict[str, Any]:
    blocked_reasons = item.get('blocked_reasons') or []
    if not isinstance(blocked_reasons, list):
        blocked_reasons = [str(blocked_reasons)] if blocked_reasons else []
    return {
        '交易对': item.get('symbol', '-'),
        '原因': item.get('reject_label', item.get('reason', '-')),
        '可交易分': item.get('tradeability_score'),
        '阻断明细': [str(reason) for reason in blocked_reasons[:4]],
    }


def populate_tradeability_blocks(summary: Dict[str, Any], scan: Dict[str, Any]) -> None:
    blocked_tradeability = scan.get('blocked_tradeability') or []
    if not isinstance(blocked_tradeability, list):
        return
    rows = summary['扫描概览']['可交易性拦截列表']
    for item in blocked_tradeability[:5]:
        if not isinstance(item, dict):
            continue
        rows.append(_build_tradeability_block_row(item))


def populate_candidate_sections(summary: Dict[str, Any], candidates: Sequence[Dict[str, Any]]) -> None:
    stage_counts = summary['扫描概览']['阶段分布']
    grouped_rows = {
        'setup_candidate': summary['setup候选列表'],
        'watch_candidate': summary['watch候选列表'],
        'trade_candidate': summary['trade候选列表'],
        'other': summary['其他候选列表'],
    }
    for index, item in enumerate(list(candidates)):
        if not isinstance(item, dict):
            continue
        row = _build_candidate_row(item)
        stage = str(item.get('candidate_stage') or '').strip().lower()
        bucket = stage if stage in {'setup_candidate', 'watch_candidate', 'trade_candidate'} else 'other'
        stage_counts[bucket] += 1
        if index < 5:
            summary['候选列表'].append(row)
            grouped_rows[bucket].append(row)


def build_cn_scan_summary(result: Dict[str, Any], mask_sensitive_token) -> Dict[str, Any]:
    cycles = result.get('cycles') if isinstance(result, dict) else None
    cycle = cycles[0] if isinstance(cycles, list) and cycles else {}
    scan = cycle.get('scan', {}) if isinstance(cycle, dict) else {}
    selected = scan.get('selected_alert') or scan.get('selected')
    market_regime = scan.get('market_regime', {}) if isinstance(scan, dict) else {}
    rejected_stats = scan.get('rejected_stats', {}) if isinstance(scan, dict) else {}
    candidates = scan.get('candidate_alerts') or scan.get('candidates') or []
    cycle_mode = 'live' if bool(result.get('live')) else 'dry-run'
    if isinstance(cycle, dict):
        if cycle.get('scan_only'):
            cycle_mode = 'scan-only'
        elif cycle.get('live_requested') or cycle.get('live_execution') or cycle.get('live_skipped_due_to_risk_guard') or cycle.get('live_skipped_due_to_existing_positions'):
            cycle_mode = 'live'

    summary = {
        'ok': result.get('ok', True) if isinstance(result, dict) else True,
        '模式': cycle_mode,
        '市场状态': {
            '标签': market_regime.get('label', '-'),
            '乘数': market_regime.get('score_multiplier', 1.0),
            '原因': market_regime.get('reasons', []),
        },
        '扫描概览': {
            '候选数': scan.get('candidate_count', 0),
            '拒绝数': rejected_stats.get('total', 0),
            '主要拒绝原因': [
                {'原因': key, '数量': value}
                for key, value in top_dict_items(rejected_stats.get('by_reject_label', {}), limit=4)
            ],
            '可交易性拦截列表': [],
            '阶段分布': {
                'setup_candidate': 0,
                'watch_candidate': 0,
                'trade_candidate': 0,
                'other': 0,
            },
        },
        '运行监控': build_runtime_health_summary(cycle, mask_sensitive_token) if isinstance(cycle, dict) else {
            'BookTicker WS': {},
            'User Data Stream': {},
            '告警': {},
        },
        '首选标的': build_selected_summary(selected),
        '候选列表': [],
        'setup候选列表': [],
        'watch候选列表': [],
        'trade候选列表': [],
        '其他候选列表': [],
    }
    populate_tradeability_blocks(summary, scan)
    populate_candidate_sections(summary, candidates)
    return summary


def render_runtime_health_lines(runtime_health: Dict[str, Any]) -> list[str]:
    lines: list[str] = []
    book_ticker = runtime_health.get('BookTicker WS', {}) or {}
    if book_ticker:
        book_ticker_parts = [f"BookTicker WS: {book_ticker.get('状态', '-')}"]
        if book_ticker.get('原因'):
            book_ticker_parts.append(f"原因 {book_ticker.get('原因')}")
        active_streams = book_ticker.get('活跃流') or []
        if active_streams:
            book_ticker_parts.append(f"活跃流 {', '.join(str(x) for x in active_streams[:4])}")
        if book_ticker.get('订阅版本') is not None:
            book_ticker_parts.append(f"订阅版本 {book_ticker.get('订阅版本')}")
        lines.append(' | '.join(book_ticker_parts))

    user_data_stream = runtime_health.get('User Data Stream', {}) or {}
    if user_data_stream:
        uds_parts = [f"User Data Stream: {user_data_stream.get('状态', '-')}"]
        if user_data_stream.get('动作'):
            uds_parts.append(f"动作 {user_data_stream.get('动作')}")
        if user_data_stream.get('断线次数') is not None:
            uds_parts.append(f"断线 {user_data_stream.get('断线次数')}")
        if user_data_stream.get('续期失败次数') is not None:
            uds_parts.append(f"续期失败 {user_data_stream.get('续期失败次数')}")
        if user_data_stream.get('重连次数') is not None:
            uds_parts.append(f"重连 {user_data_stream.get('重连次数')}")
        if user_data_stream.get('更新时间'):
            uds_parts.append(f"更新时间 {user_data_stream.get('更新时间')}")
        if user_data_stream.get('最近错误'):
            uds_parts.append(f"错误 {user_data_stream.get('最近错误')}")
        lines.append(' | '.join(uds_parts))

    user_data_stream_alert = runtime_health.get('告警', {}) or {}
    if user_data_stream_alert:
        alert_parts = [f"运行告警: {user_data_stream_alert.get('级别', '-')}"]
        if user_data_stream_alert.get('消息'):
            alert_parts.append(str(user_data_stream_alert.get('消息')))
        if user_data_stream_alert.get('listen_key'):
            alert_parts.append(f"listen_key {user_data_stream_alert.get('listen_key')}")
        if user_data_stream_alert.get('更新时间'):
            alert_parts.append(f"更新时间 {user_data_stream_alert.get('更新时间')}")
        lines.append(' | '.join(alert_parts))

    return lines


def _append_selected_lines(lines: List[str], selected: Dict[str, Any] | None, format_num, format_pct) -> None:
    if not selected:
        return
    lines.extend([
        '',
        '首选标的',
        f"- {selected.get('交易对')} | {selected.get('评级')} | {selected.get('状态')} | 得分 {format_num(selected.get('得分'), 1)}",
        f"- 入场 {format_num(selected.get('入场价'), 6)} | 止损 {format_num(selected.get('止损价'), 6)} | 建议仓位 {format_pct(selected.get('建议仓位'), 1)}",
        f"- 执行质量 {selected.get('流动性', '-')} | 预期滑点R {format_num(selected.get('预期滑点R'), 3)}",
    ])
    selected_reasons = selected.get('理由', []) or []
    if selected_reasons:
        lines.append(f"- 关键信号: {'，'.join(str(x) for x in selected_reasons[:5])}")


def _append_candidate_rows(lines: List[str], title: str, rows: Iterable[Dict[str, Any]], format_num, format_pct) -> None:
    rows = list(rows)
    if not rows:
        return
    lines.extend(['', title])
    for idx, item in enumerate(rows, start=1):
        lines.append(
            f"{idx}. {item.get('交易对')} | {item.get('评级')} | {item.get('状态')} | 得分 {format_num(item.get('得分'), 1)} | 24h {format_pct(item.get('24h涨幅'), 2)} | 5m {format_pct(item.get('5m涨幅'), 2)} | 仓位 {format_pct(item.get('建议仓位'), 1)} | 流动性 {item.get('流动性', '-')}"
        )


def _append_tradeability_block_rows(lines: List[str], rows: Iterable[Dict[str, Any]], format_num) -> None:
    rows = list(rows)
    if not rows:
        return
    lines.extend(['', '可交易性拦截:'])
    for item in rows:
        blocked_reasons = item.get('阻断明细') or []
        reason_text = ', '.join(str(reason) for reason in blocked_reasons[:4]) if blocked_reasons else '-'
        lines.append(
            f"- {item.get('交易对')} | {item.get('原因')} | 分数 {format_num(item.get('可交易分'), 1)} | {reason_text}"
        )


def render_cn_scan_summary(summary: Dict[str, Any], format_num, format_pct) -> str:
    market = summary.get('市场状态', {})
    overview = summary.get('扫描概览', {})
    selected = summary.get('首选标的')
    lines = [
        f"扫描模式: {summary.get('模式', 'dry-run')}",
    ]
    lines.append(f"扫描结果: 候选 {overview.get('候选数', 0)} 个 | 拒绝 {overview.get('拒绝数', 0)} 个")
    reject_items = overview.get('主要拒绝原因', []) or []
    if reject_items:
        reject_text = '，'.join(f"{item['原因']} {item['数量']}" for item in reject_items)
        lines.append(f"主要拦截: {reject_text}")
    stage_distribution = overview.get('阶段分布', {}) or {}
    if stage_distribution:
        lines.append(
            "阶段分布: "
            f"setup {int(stage_distribution.get('setup_candidate', 0) or 0)} | "
            f"watch {int(stage_distribution.get('watch_candidate', 0) or 0)} | "
            f"trade {int(stage_distribution.get('trade_candidate', 0) or 0)} | "
            f"other {int(stage_distribution.get('other', 0) or 0)}"
        )
    _append_tradeability_block_rows(lines, overview.get('可交易性拦截列表', []) or [], format_num)
    runtime_health = summary.get('运行监控', {}) or {}
    lines.extend(render_runtime_health_lines(runtime_health))
    _append_selected_lines(lines, selected, format_num, format_pct)
    _append_candidate_rows(lines, '候选列表', summary.get('候选列表', []) or [], format_num, format_pct)
    grouped_sections = [
        ('Trade 候选', summary.get('trade候选列表', []) or []),
        ('Setup 候选', summary.get('setup候选列表', []) or []),
        ('Watch 候选', summary.get('watch候选列表', []) or []),
        ('其他候选', summary.get('其他候选列表', []) or []),
    ]
    for title, rows in grouped_sections:
        _append_candidate_rows(lines, title, rows, format_num, format_pct)
    return '\n'.join(lines)
