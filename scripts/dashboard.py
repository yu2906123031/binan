from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse


SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from binance_futures_momentum_long import RuntimeStateStore, load_dotenv


DEFAULT_RUNTIME_DIR = os.path.expanduser('~/.hermes/binance-futures-momentum-long/runtime-state')


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def file_meta(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {'exists': False}
    stat = path.stat()
    return {
        'exists': True,
        'size': stat.st_size,
        'modified_at': dt.datetime.fromtimestamp(stat.st_mtime, dt.timezone.utc).isoformat(),
    }


def load_dashboard_state(runtime_state_dir: str, event_limit: int = 200) -> Dict[str, Any]:
    store = RuntimeStateStore(runtime_state_dir)
    base_dir = store._dir()
    events = store.read_events(limit=event_limit)
    return {
        'ok': True,
        'now': utc_now(),
        'runtime_state_dir': str(base_dir),
        'files': {
            'last_cycle': file_meta(base_dir / 'last_cycle.json'),
            'positions': file_meta(base_dir / 'positions.json'),
            'account': file_meta(base_dir / 'account.json'),
            'risk_state': file_meta(base_dir / 'risk_state.json'),
            'events': file_meta(base_dir / 'events.jsonl'),
            'user_data_stream': file_meta(base_dir / 'user_data_stream.json'),
        },
        'last_cycle': store.load_json('last_cycle', {}),
        'account': store.load_json('account', {}),
        'positions': store.load_json('positions', {}),
        'risk_state': store.load_json('risk_state', {}),
        'user_data_stream': store.load_json('user_data_stream', {}),
        'events': events,
        'event_count_loaded': len(events),
    }


def parse_panel(raw: str) -> Dict[str, str]:
    if '=' in raw:
        name, runtime_dir = raw.split('=', 1)
        return {'name': name.strip() or runtime_dir.strip(), 'runtime_state_dir': runtime_dir.strip()}
    runtime_dir = raw.strip()
    return {'name': Path(runtime_dir).name or runtime_dir, 'runtime_state_dir': runtime_dir}


def parse_panels(values: List[str], fallback_dir: str) -> List[Dict[str, str]]:
    raw_values: List[str] = []
    for value in values:
        raw_values.extend([part for part in value.split(';') if part.strip()])
    if not raw_values:
        raw_values = [f'Default={fallback_dir}']
    panels = [parse_panel(value) for value in raw_values]
    return [panel for panel in panels if panel['runtime_state_dir']]


def load_multi_state(panels: List[Dict[str, str]], event_limit: int = 200) -> Dict[str, Any]:
    loaded = []
    for panel in panels:
        state = load_dashboard_state(panel['runtime_state_dir'], event_limit=event_limit)
        state['panel_name'] = panel['name']
        loaded.append(state)
    return {'ok': True, 'now': utc_now(), 'panels': loaded}


HTML = r'''<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>量化交易运行终端</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #080b0f;
      --panel: #111820;
      --panel2: #151f29;
      --panel3: #0d131a;
      --line: #243140;
      --line2: #334456;
      --text: #edf3f8;
      --muted: #8e9baa;
      --faint: #5f6d7b;
      --good: #18c77a;
      --good-bg: rgba(24, 199, 122, .1);
      --warn: #e5b74f;
      --warn-bg: rgba(229, 183, 79, .12);
      --bad: #f05252;
      --bad-bg: rgba(240, 82, 82, .1);
      --info: #4aa8ff;
      --info-bg: rgba(74, 168, 255, .1);
      --neutral: #9aa4b2;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: "Segoe UI", "Microsoft YaHei", Arial, sans-serif;
      letter-spacing: 0;
    }
    header {
      position: sticky;
      top: 0;
      z-index: 2;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      padding: 12px 18px;
      background: rgba(9, 13, 18, .96);
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(10px);
    }
    h1 { margin: 0; font-size: 17px; font-weight: 800; }
    main { padding: 14px; display: grid; gap: 14px; }
    .status { color: var(--muted); font-size: 13px; }
    .dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 8px; background: var(--warn); }
    .dot.good { background: var(--good); }
    .dot.bad { background: var(--bad); }
    .panel {
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 8px;
      overflow: hidden;
    }
    .panel-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      background: #0d131a;
    }
    .panel-title { font-weight: 800; font-size: 16px; }
    .panel-sub { color: var(--muted); font-size: 12px; margin-top: 4px; overflow-wrap: anywhere; }
    .terminal-top { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 8px; padding: 12px; border-bottom: 1px solid var(--line); background: #0b1016; }
    .status-card { min-width: 0; padding: 9px 10px; background: var(--panel2); border: 1px solid var(--line); border-radius: 6px; }
    .status-card span, .metric span { display: block; color: var(--muted); font-size: 11px; margin-bottom: 5px; }
    .status-card strong { display: block; font-size: 15px; overflow-wrap: anywhere; }
    .body { padding: 12px; display: grid; gap: 12px; }
    .decision-grid { display: grid; grid-template-columns: minmax(260px, 1.2fr) minmax(320px, 2fr) minmax(240px, 1fr); gap: 12px; align-items: stretch; }
    .combined-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 12px; align-items: start; }
    .exchange-card { border: 1px solid var(--line); background: var(--panel3); border-radius: 8px; min-width: 0; overflow: hidden; }
    .exchange-head { display: flex; justify-content: space-between; gap: 10px; align-items: center; padding: 10px 12px; border-bottom: 1px solid var(--line); background: #0b1016; }
    .exchange-name { font-size: 15px; font-weight: 850; }
    .exchange-body { padding: 12px; display: grid; gap: 10px; }
    .compact-decision { display: grid; grid-template-columns: 110px 1fr; gap: 10px; align-items: stretch; }
    .compact-action { display: grid; place-items: center; border: 1px solid var(--line); border-radius: 6px; background: var(--panel2); font-size: 22px; font-weight: 900; min-height: 86px; }
    .compact-meta { display: grid; gap: 8px; min-width: 0; }
    .combined-stream { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
    .section { border: 1px solid var(--line); background: var(--panel3); border-radius: 8px; padding: 12px; min-width: 0; }
    .section-title { display: flex; justify-content: space-between; align-items: center; gap: 10px; color: var(--muted); font-size: 12px; font-weight: 700; margin-bottom: 10px; }
    .action { font-size: 34px; font-weight: 900; line-height: 1; margin-bottom: 8px; }
    .reason { min-height: 48px; padding: 10px; border: 1px solid var(--line); background: #0b1016; color: var(--text); border-radius: 6px; font-size: 13px; line-height: 1.45; }
    .candidate-list { display: grid; gap: 8px; }
    .candidate { border: 1px solid var(--line); border-left: 3px solid var(--info); background: var(--panel2); border-radius: 6px; padding: 9px; }
    .candidate-head { display: flex; justify-content: space-between; gap: 8px; align-items: baseline; font-weight: 800; }
    .candidate small { color: var(--muted); display: block; margin-top: 5px; line-height: 1.4; }
    .progress-row { display: grid; gap: 8px; }
    .gate { display: grid; grid-template-columns: 96px 1fr 42px; gap: 8px; align-items: center; font-size: 12px; color: var(--muted); }
    .bar { height: 7px; overflow: hidden; background: #0a0f15; border: 1px solid var(--line); border-radius: 999px; }
    .bar > i { display: block; height: 100%; width: var(--w, 0%); background: var(--info); }
    .bar.good > i { background: var(--good); }
    .bar.warn > i { background: var(--warn); }
    .bar.bad > i { background: var(--bad); }
    .funnel { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 8px; }
    .funnel-step { min-width: 0; padding: 9px; border: 1px solid var(--line); border-radius: 6px; background: var(--panel2); }
    .funnel-step b { display: block; font-size: 11px; color: var(--muted); margin-bottom: 4px; }
    .funnel-step strong { display: block; font-size: 20px; }
    .funnel-step small { display: block; color: var(--faint); margin-top: 5px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    details { border-top: 1px solid var(--line); margin-top: 10px; padding-top: 8px; }
    summary { cursor: pointer; color: var(--muted); font-size: 12px; }
    .positions-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 10px; }
    .position-card { border: 1px solid var(--line); border-left: 3px solid var(--neutral); background: var(--panel2); border-radius: 8px; padding: 10px; min-width: 0; }
    .position-card.long { border-left-color: var(--good); }
    .position-card.short { border-left-color: var(--bad); }
    .position-top { display: flex; justify-content: space-between; gap: 8px; align-items: baseline; }
    .symbol { font-size: 16px; font-weight: 850; }
    .pnl { font-size: 24px; font-weight: 900; margin: 8px 0 4px; }
    .mini { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px; color: var(--muted); font-size: 12px; }
    .risk-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
    .metric { min-width: 0; padding: 10px; background: var(--panel2); border: 1px solid var(--line); border-radius: 6px; }
    .metric strong { display: block; font-size: 20px; overflow-wrap: anywhere; }
    .metric small { color: var(--muted); display: block; margin-top: 5px; line-height: 1.35; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { padding: 8px 6px; border-bottom: 1px solid var(--line); vertical-align: top; text-align: left; }
    th { color: var(--muted); background: var(--panel2); }
    td { overflow-wrap: anywhere; }
    .pill { display: inline-flex; padding: 2px 8px; border: 1px solid var(--line2); border-radius: 999px; margin: 1px 2px 1px 0; background: #101821; font-size: 12px; white-space: nowrap; }
    .pill.good { background: var(--good-bg); border-color: rgba(24, 199, 122, .35); }
    .pill.warn { background: var(--warn-bg); border-color: rgba(229, 183, 79, .4); }
    .pill.bad { background: var(--bad-bg); border-color: rgba(240, 82, 82, .38); }
    .pill.info { background: var(--info-bg); border-color: rgba(74, 168, 255, .38); }
    .toolbar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .toggle { display: inline-flex; align-items: center; gap: 6px; color: var(--muted); font-size: 12px; user-select: none; }
    .toggle input { accent-color: var(--info); }
    .table-wrap { overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; }
    .good { color: var(--good); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }
    .info { color: var(--info); }
    .muted { color: var(--muted); }
    .hidden { display: none !important; }
    pre { margin: 0; white-space: pre-wrap; overflow: auto; max-height: 220px; padding: 10px; border: 1px solid var(--line); border-radius: 6px; background: #0c0f12; font-size: 12px; }
    @media (max-width: 1180px) {
      .terminal-top { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .decision-grid { grid-template-columns: 1fr; }
      .combined-stream { grid-template-columns: 1fr; }
      .risk-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .funnel { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }
    @media (max-width: 620px) {
      header, .panel-head { flex-direction: column; align-items: flex-start; }
      main { padding: 10px; }
      .terminal-top, .risk-grid, .funnel { grid-template-columns: 1fr; }
      .action { font-size: 28px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>量化交易运行终端</h1>
    <div class="toolbar">
      <label class="toggle" title="只保留当前候选、持仓和关键事件"><input id="focusToggle" type="checkbox">只看关键标的</label>
      <div class="status"><span id="statusDot" class="dot"></span><span id="statusText">连接中</span></div>
    </div>
  </header>
  <main id="panels"></main>
  <script>
    const apiUrl = '/api/state' + window.location.search;
    const fmt = (v) => v === undefined || v === null || v === '' ? '-' : String(v);
    const esc = (v) => fmt(v).replace(/[&<>"']/g, (m) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
    const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : null; };
    const amount = (v, digits = 6) => { const n = num(v); return n === null ? '-' : n.toLocaleString(undefined, { maximumFractionDigits: digits }); };
    const money = (v) => { const n = num(v); return n === null ? '-' : `${n < 0 ? '-' : ''}$${Math.abs(n).toFixed(Math.abs(n) >= 100 ? 1 : 2)}`; };
    const pill = (text, kind = '') => `<span class="pill ${kind}">${esc(text)}</span>`;
    const zh = (v) => ({
      OKX_SIMULATED: 'OKX 模拟盘',
      BINANCE_SIMULATED: '币安合约模拟盘',
      BINANCE: '币安',
      BINANCE_SPOT_DEMO: '币安现货模拟盘',
      okx_sim_active: 'OKX 模拟活跃档',
      'okx-sim-active': 'OKX 模拟活跃档',
      'binance-sim-active': '币安合约模拟活跃档',
      running: '运行中',
      idle: '待机',
      blocked: '已拦截',
      allowed: '允许',
      neutral: '中性',
      risk_on: '偏强',
      risk_off: '偏弱',
      none: '无明确形态',
      overheated: '过热',
      active: '有效',
      open: '持仓中',
      closed: '已关闭',
      protected: '已保护',
      monitoring: '监控中',
      SPOT_DEMO_BALANCE: '现货模拟资产',
      ASSET: '资产',
      asset: '资产',
      LONG: '做多',
      SHORT: '做空',
      long: '做多',
      short: '做空',
      watch: '观察',
      launch: '启动形态',
      high: '高优先级',
      critical: '极高优先级',
      watch_candidate: '观察候选',
      setup_candidate: '结构候选',
      trigger_candidate: '触发候选',
      trade_candidate: '可交易候选',
      candidate_rejected: '候选被拒绝',
      book_ticker_cache_miss: '盘口缓存未命中',
      book_ticker_cache_miss_summary: '盘口缓存未命中汇总',
      okx_simulated_order_submitted: 'OKX 模拟盘已提交订单',
      entry_filled: '开仓成交',
      initial_stop_placed: '初始止损已挂',
      protection_confirmed: '保护单已确认',
      user_data_stream_started: '账户监听已启动',
      user_data_stream_health: '账户监听健康状态',
      user_data_stream_alert: '账户监听告警',
      candidate_setup_not_ready: '结构还没准备好',
      candidate_trigger_not_fired: '触发条件还没出现',
      okx_execution_preflight_failed: 'OKX 下单前检查失败',
      waiting_breakout: '等待真正突破',
      breakout_close_not_confirmed: '收盘突破未确认',
      retest_not_confirmed: '回踩确认不足',
      oi_taker_not_confirmed: 'OI/主动买卖未确认',
      cvd_not_confirmed: 'CVD 未确认',
      long_crowding_not_ok: '多头拥挤',
      funding_crowding_not_ok: '资金费率拥挤',
      price_extension_too_far: '价格延伸过远',
      state_overheated: '形态过热',
      state_none: '没有明确形态',
      not_okx_simulated: '不是 OKX 模拟盘试探模式',
      not_simulated_trading: '不是模拟盘试探模式',
      sim_probe_entry_allowed: '允许模拟盘试探单',
      sim_probe_disabled: '试探单未开启',
      sim_probe_score_below_min: '试探单分数不足',
      sim_probe_liquidity_not_good: '试探单流动性不足',
      sim_probe_breakout_distance_too_far: '离突破位太远，不试探',
      sim_probe_price_extension_risk: '价格延伸风险过高，不试探',
      full_trigger_already_fired: '完整触发已经出现',
      strategy_halted: '策略已暂停',
      okx_account_mode_not_supported: 'OKX 账户模式不支持合约下单',
      execution_slippage_veto: '预估滑点过高',
      oi_reversal_veto: '持仓量反转风险',
      open_interest_reversal: '持仓量反转',
      execution_slippage: '执行滑点过高',
      rest_polling: '已改用 REST 行情补采',
      btc_above_ema20: 'BTC 位于 EMA20 上方',
      sol_above_ema20: 'SOL 位于 EMA20 上方',
      long_breakout_not_confirmed: '做多突破未确认',
      short_breakdown_not_confirmed: '做空破位未确认',
      quote_volume_below_gate: '成交额不足',
      short_funding_rate_below_gate: '做空资金费率过低',
      short_funding_rate_avg_below_gate: '做空平均资金费率过低',
      extended_chase_veto: '追高风险过高',
      negative_cvd_veto: 'CVD 负向否决',
      price_extension_chase: '价格延伸追高',
      negative_cvd_distribution: 'CVD 负向派发',
      orphan: '交易所已有仓位',
      protected: '已挂保护止损',
      monitoring: '监控中',
      open: '持仓中',
      closed: '已关闭',
    }[String(v)] || fmt(v));
    const firstCycle = (last) => last?.cycle || last?.cycles?.[0] || {};
    const uniquePositions = (positions) => {
      const seen = new Set();
      const rows = [];
      for (const row of Object.values(positions || {})) {
        if (!row || typeof row !== 'object') continue;
        const key = row.position_key || `${row.asset || row.symbol}:${row.side || row.position_side || ''}`;
        if (seen.has(key)) continue;
        seen.add(key);
        rows.push(row);
      }
      return rows;
    };
    const topCounts = (obj, limit = 8) => Object.entries(obj || {}).sort((a, b) => Number(b[1]) - Number(a[1])).slice(0, limit);
    const metric = (name, value, sub = '', cls = '') => `<div class="metric"><span>${esc(name)}</span><strong class="${cls}">${esc(value)}</strong><small>${esc(sub)}</small></div>`;
    const kvRows = (rows) => rows.map(([k, v]) => `<tr><td>${esc(k)}</td><td>${v}</td></tr>`).join('');
    const zhList = (values) => (values || []).map(zh).join('，') || '-';
    const pct = (v, digits = 1) => { const n = num(v); return n === null ? '-' : `${n.toFixed(digits)}%`; };
    const clamp = (v, min = 0, max = 100) => Math.max(min, Math.min(max, Number(v) || 0));
    const sideKind = (side) => {
      const s = String(side || '').toLowerCase();
      if (s.includes('short') || s.includes('空') || s === 'sell') return 'short';
      if (s.includes('long') || s.includes('多') || s === 'buy') return 'long';
      return '';
    };
    const statusCard = (name, value, sub = '', cls = '') => `<div class="status-card"><span>${esc(name)}</span><strong class="${cls}">${esc(value)}</strong><small class="muted">${esc(sub)}</small></div>`;
    const gateRow = (name, value, text, cls = 'info') => `<div class="gate" title="${esc(text)}"><span>${esc(name)}</span><div class="bar ${cls}" style="--w:${clamp(value)}%"><i></i></div><b class="${cls}">${esc(Math.round(clamp(value)))}%</b></div>`;
    const reasonLabel = (reason) => humanError(zh(reason));
    const latestImportantEvent = (events) => importantEvents(events).slice(-1)[0] || null;
    const latestRejected = (events) => [...(events || [])].reverse().find((e) => String(e.event_type || '') === 'candidate_rejected') || null;
    const renderReasons = (counts) => topCounts(counts, 10).map(([k, v]) => `<span class="pill warn" title="${esc(zh(k))}">${esc(zh(k))} ${esc(v)}</span>`).join('') || '<span class="pill">暂无淘汰原因</span>';
    const candidateRows = (scan, selected, events) => {
      const list = [];
      if (selected) list.push(selected);
      for (const e of [...(events || [])].reverse()) {
        if (list.length >= 2) break;
        if (!e.symbol || String(e.event_type || '') !== 'candidate_rejected') continue;
        if (list.some((x) => x.symbol === e.symbol)) continue;
        list.push(e);
      }
      if (!list.length) return '<div class="candidate"><div class="candidate-head"><span>暂无候选</span><span class="pill">WAIT</span></div><small>扫描没有产生可执行标的，系统继续等待。</small></div>';
      return list.slice(0, 2).map((c) => {
        const kind = sideKind(c.side || c.position_side);
        const cls = kind === 'short' ? 'bad' : (kind === 'long' ? 'good' : 'info');
        const score = c.score ?? c.rank_score ?? '-';
        const why = c.reject_reason || c.reject_reason_label || (c.trade_missing || c.trigger_missing || c.setup_missing || [])[0] || c.candidate_stage || '-';
        return `<div class="candidate key-symbol" data-symbol="${esc(c.symbol || '')}" title="${esc(reasonLabel(why))}">
          <div class="candidate-head"><span>${esc(c.symbol || '-')}</span>${pill(zh(c.side || c.position_side || '观察'), cls)}</div>
          <small>分数 ${esc(amount(score, 2))} | 阶段 ${esc(zh(c.candidate_stage || c.event_type || '-'))} | ${esc(reasonLabel(why))}</small>
        </div>`;
      }).join('');
    };
    const triggerProgress = (scan, selected, riskOk) => {
      const funnel = scan.funnel || {};
      const raw = Number(funnel.raw_scan_symbol_count || 0);
      const evaled = Number(funnel.evaluated_symbol_count || 0);
      const candidates = Number(scan.candidate_count || 0);
      const score = Number(selected?.score || 0);
      return [
        gateRow('SCAN', raw ? 100 : 0, `扫描 ${raw} 个标的`, raw ? 'good' : 'bad'),
        gateRow('FILTER', raw ? evaled / Math.max(raw, 1) * 100 : 0, `通过初筛 ${evaled}/${raw}`, evaled ? 'info' : 'warn'),
        gateRow('STRUCTURE', candidates ? 70 : 20, `候选 ${candidates} 个`, candidates ? 'info' : 'warn'),
        gateRow('TRIGGER', selected ? clamp(score) : 15, selected ? `候选分数 ${amount(score, 2)}` : '触发条件未出现', selected ? 'good' : 'warn'),
        gateRow('RISK', riskOk ? 100 : 0, riskOk ? '风控允许' : '风控拦截', riskOk ? 'good' : 'bad'),
      ].join('');
    };
    const funnelStep = (name, count, reason, cls = '') => `<div class="funnel-step"><b>${esc(name)}</b><strong class="${cls}">${esc(count)}</strong><small title="${esc(reason)}">${esc(reason)}</small></div>`;
    const renderFunnel = (scan, cycle, rejectCounts, riskOk) => {
      const f = scan.funnel || {};
      const raw = f.raw_scan_symbol_count || 0;
      const evaled = f.evaluated_symbol_count || 0;
      const candidates = scan.candidate_count || 0;
      const submitted = f.order_submitted_count || (cycle.live_execution ? 1 : 0);
      return `<div class="funnel">
        ${funnelStep('SCAN', raw, '行情扫描池')}
        ${funnelStep('FILTER', evaled, topCounts(rejectCounts, 1).map(([k, v]) => `${zh(k)} ${v}`).join('') || '基础过滤通过')}
        ${funnelStep('STRUCTURE', candidates, '结构候选')}
        ${funnelStep('TRIGGER', scan.selected || scan.selected_alert ? 1 : 0, scan.selected || scan.selected_alert ? '触发接近/已触发' : '等待突破/回踩确认', scan.selected || scan.selected_alert ? 'good' : 'warn')}
        ${funnelStep('RISK', riskOk ? '允许' : '拦截', riskOk ? '风控允许执行' : '存在硬性限制', riskOk ? 'good' : 'bad')}
        ${funnelStep('EXECUTE', submitted, submitted ? '已提交订单' : '未执行', submitted ? 'good' : '')}
      </div><details><summary>展开淘汰原因</summary><div style="margin-top:8px">${renderReasons(rejectCounts)}</div></details>`;
    };
    const humanError = (error) => {
      const text = fmt(error);
      if (text === '-') return '-';
      if (text.includes('okx_account_mode_not_supported')) return 'OKX 模拟盘账户模式不支持合约下单。请在 OKX 网页/App 的模拟盘账户模式中切到 Futures/Multi-currency/Portfolio。';
      if (text.includes('50101') || text.includes('does not match current environment')) return 'API Key 和当前环境不匹配。模拟盘必须使用模拟盘创建的 Key。';
      if (text.includes('50110') || text.includes('IP whitelist')) return '当前出口 IP 不在 API Key 白名单里。';
      if (text.includes('51010') || text.includes('current account mode')) return '当前 OKX 账户模式不支持这个下单请求，需要检查账户模式/保证金模式。';
      if (text.includes('51001') || text.includes('Instrument ID')) return 'OKX 没有这个合约标的，已跳过。';
      if (text.includes('51087') || text.includes('Listing canceled')) return '这个币种在 OKX 已取消上市或不可交易。';
      if (text.includes('51155') || text.includes('local compliance')) return '因地区合规限制，OKX 不允许交易这个标的。';
      if (text.includes('-2015')) return '币安 API Key、IP 或权限不正确。';
      return text;
    };
    const eventSummary = (e) => {
      const type = String(e.event_type || '');
      const side = zh(e.side || e.position_side || '');
      if (type === 'book_ticker_cache_miss') {
        return `盘口快照缓存过期，已用 REST 行情补采。方向：${side}；缓存要求：${fmt(e.cache_max_age_seconds)} 秒内；本次合并隐藏 ${fmt(e.suppressed_since_last || 0)} 条同类事件。`;
      }
      if (type === 'candidate_rejected') {
        const reason = zh(e.reject_reason || e.reject_reason_label || (e.reasons || [])[0]);
        const missing = [...(e.trade_missing || []), ...(e.trigger_missing || []), ...(e.setup_missing || [])].slice(0, 5);
        const bits = [
          `${side}候选未下单：${reason}`,
          `分数 ${amount(e.score, 2)}`,
          `阶段 ${zh(e.candidate_stage || '-')}`,
          `形态 ${zh(e.state || '-')}`,
          `缺少：${zhList(missing)}`,
        ];
        if (e.error) bits.push(`接口返回：${humanError(e.error)}`);
        return bits.join('；');
      }
      if (type === 'okx_simulated_order_submitted') {
        return `OKX 模拟盘订单已提交。方向：${side}；数量：${amount(e.quantity, 8)}；价格：${amount(e.entry_price, 8)}；订单号：${fmt(e.order_id)}。`;
      }
      if (type === 'entry_filled') {
        return `开仓已成交。方向：${side}；成交价：${amount(e.entry_price || e.price, 8)}；数量：${amount(e.quantity || e.filled_quantity, 8)}；订单号：${fmt(e.order_id || e.entry_order_id)}。`;
      }
      if (type === 'initial_stop_placed' || type === 'protection_confirmed') {
        return `保护止损已处理。止损价：${amount(e.stop_price, 8)}；数量：${amount(e.quantity, 8)}；订单号：${fmt(e.order_id || e.stop_order_id)}。`;
      }
      if (type.includes('error')) return humanError(e.error || e.message || e.reason || JSON.stringify(e));
      return e.message || humanError(e.error) || zhList(e.reasons) || JSON.stringify(e);
    };
    const importantEvents = (events) => (events || []).filter((e) => !String(e.event_type || '').startsWith('book_ticker_cache_miss'));
    const eventRows = (events) => importantEvents(events).slice(-30).reverse().map((e, idx) => `<tr class="event-row" data-symbol="${esc(e.symbol || e.asset || '')}"><td>${idx + 1}</td><td>${esc(zh(e.event_type))}</td><td>${esc(e.symbol || e.asset || '-')}</td><td>${esc(eventSummary(e))}</td></tr>`).join('') || '<tr><td colspan="4">暂无重要事件</td></tr>';
    const tradeEvents = (events) => (events || []).filter((e) => ['buy_fill_confirmed', 'entry_filled', 'protection_confirmed', 'initial_stop_placed', 'tp1_hit', 'tp2_hit', 'runner_exited', 'trade_invalidated', 'management_action_failed'].includes(String(e.event_type || '')));
    const tradeRows = (events) => {
      const rows = tradeEvents(events).slice(-40).reverse();
      if (!rows.length) return '<tr><td colspan="8">暂无交易明细</td></tr>';
      return rows.map((e) => {
        const qty = e.quantity ?? e.filled_quantity ?? e.close_qty ?? e.remaining_quantity;
        const price = e.entry_price ?? e.price ?? e.stop_price ?? e.new_stop_price;
        const pnl = e.realized_pnl_usdt ?? e.unrealized_pnl_usdt;
        const status = e.protection_status || e.exit_reason || e.action || e.entry_order_status || '-';
        return `<tr class="event-row" data-symbol="${esc(e.symbol || '')}"><td>${esc(e.recorded_at || e.updated_at || '-')}</td><td>${esc(zh(e.event_type))}</td><td>${esc(e.symbol || '-')}</td><td>${esc(zh(e.side || e.position_side || '-'))}</td><td>${esc(amount(qty, 8))}</td><td>${esc(amount(price, 8))}</td><td>${esc(pnl == null ? '-' : money(pnl))}</td><td>${esc(zh(status))}</td></tr>`;
      }).join('');
    };
    const positionRows = (positions) => {
      const rows = uniquePositions(positions);
      if (!rows.length) return '<tr><td colspan="7">暂无持仓/资产</td></tr>';
      const pnlClass = (v) => Number(v || 0) > 0 ? 'good' : (Number(v || 0) < 0 ? 'bad' : '');
      return rows.map((p) => `<tr><td>${esc(p.asset || p.symbol)}</td><td>${esc(zh(p.side || p.position_side || p.type || '资产'))}</td><td>${esc(amount(p.free ?? p.quantity ?? p.remaining_quantity ?? p.positionAmt, 8))}</td><td>${esc(amount(p.locked ?? p.current_price ?? p.entry_price, 8))}</td><td>${esc(money(p.notional_usdt ?? p.position_notional ?? p.notional ?? p.usdt_value))}</td><td class="${pnlClass(p.unrealized_pnl_usdt)}">${esc(money(p.unrealized_pnl_usdt))}${p.unrealized_pnl_pct == null ? '' : ` / ${esc(amount(p.unrealized_pnl_pct, 2))}%`}</td><td>${esc(zh(p.status || p.protection_status || '-'))}</td></tr>`).join('');
    };
    const positionSummary = (positions) => {
      const rows = uniquePositions(positions);
      const totalPnl = rows.reduce((sum, p) => sum + Number(p.unrealized_pnl_usdt || 0), 0);
      const totalNotional = rows.reduce((sum, p) => sum + Math.abs(Number(p.position_notional || p.notional_usdt || p.notional || 0)), 0);
      const totalMargin = rows.reduce((sum, p) => sum + Math.abs(Number(p.position_margin_usdt || 0)), 0);
      return {rows, totalPnl, totalNotional, totalMargin};
    };
    const positionCards = (positions) => {
      const rows = uniquePositions(positions);
      if (!rows.length) return '<div class="position-card"><div class="position-top"><span class="symbol">暂无持仓</span><span class="pill">WAIT</span></div><div class="pnl muted">$0.00</div><div class="mini"><span>系统没有读取到交易所持仓</span><span>风险：正常</span></div></div>';
      return rows.map((p) => {
        const kind = sideKind(p.side || p.position_side);
        const qty = p.quantity ?? p.remaining_quantity ?? p.positionAmt;
        const notional = Math.abs(Number(p.position_notional || p.notional_usdt || p.notional || 0));
        const pnl = Number(p.unrealized_pnl_usdt || 0);
        const roi = p.unrealized_pnl_pct ?? (notional ? pnl / notional * 100 : null);
        const strength = clamp(notional / 20);
        const riskBad = Math.abs(Number(roi || 0)) >= 10 || String(p.protection_status || p.status || '').includes('missing');
        return `<div class="position-card ${kind} key-symbol" data-symbol="${esc(p.asset || p.symbol || '')}" title="入场 ${esc(amount(p.entry_price, 8))}，当前 ${esc(amount(p.current_price, 8))}">
          <div class="position-top"><span class="symbol">${esc(p.asset || p.symbol || '-')}</span>${pill(zh(p.side || p.position_side || '资产'), kind === 'short' ? 'bad' : (kind === 'long' ? 'good' : ''))}</div>
          <div class="pnl ${pnl > 0 ? 'good' : (pnl < 0 ? 'bad' : 'muted')}">${esc(money(pnl))}</div>
          <div class="mini"><span>ROI ${esc(pct(roi, 2))}</span><span>名义 ${esc(money(notional))}</span><span>数量 ${esc(amount(qty, 8))}</span><span>${esc(zh(p.status || p.protection_status || '正常'))}</span></div>
          <div class="gate" style="grid-template-columns:70px 1fr 48px;margin-top:9px"><span>强度</span><div class="bar ${riskBad ? 'warn' : 'good'}" style="--w:${strength}%"><i></i></div><b class="${riskBad ? 'warn' : 'good'}">${riskBad ? '危险' : '正常'}</b></div>
        </div>`;
      }).join('');
    };
    const riskMetrics = (risk, pos, account) => {
      const totalPnl = Number(account.positions_unrealized_pnl ?? account.account_total_unrealized_pnl ?? pos.totalPnl ?? 0);
      const equity = Number(account.account_total_margin_balance ?? account.total_wallet_balance ?? 0);
      const concentration = pos.totalNotional && pos.rows.length ? Math.max(...pos.rows.map((p) => Math.abs(Number(p.position_notional || p.notional_usdt || p.notional || 0)))) / pos.totalNotional * 100 : 0;
      const drawdown = equity ? Math.min(0, totalPnl) / equity * 100 : 0;
      const score = clamp((risk.allowed === false ? 78 : 20) + concentration / 2 + Math.abs(Math.min(0, drawdown)) * 2);
      return `<div class="risk-grid">
        ${metric('当前风险评分', `${Math.round(score)}/100`, score >= 70 ? '高风险，需要限制' : '正常监控', score >= 70 ? 'bad' : (score >= 45 ? 'warn' : 'good'))}
        ${metric('持仓集中度', pct(concentration, 1), `持仓 ${pos.rows.length} 个`)}
        ${metric('最大回撤估算', pct(drawdown, 2), `按当前浮盈亏估算`, drawdown < -5 ? 'bad' : '')}
        ${metric('风控限制', risk.allowed === false ? '已触发' : '未触发', (risk.reasons || []).map(zh).join('，') || '允许开仓', risk.allowed === false ? 'bad' : 'good')}
      </div>`;
    };
    const panelModel = (state) => {
      const last = state.last_cycle || {};
      const cycle = firstCycle(last);
      const scan = cycle.scan || {};
      const risk = cycle.risk_guard || {};
      const exchange = String(last.execution_exchange || cycle.execution_exchange || '').toUpperCase();
      const selected = scan.selected || scan.selected_alert || null;
      const pos = positionSummary(state.positions);
      const account = state.account || {};
      const wallet = account.account_total_wallet_balance ?? account.total_wallet_balance;
      const marginBalance = account.account_total_margin_balance;
      const available = account.account_available_balance ?? account.available_balance;
      const totalPnl = account.positions_unrealized_pnl ?? account.account_total_unrealized_pnl ?? pos.totalPnl;
      const accountMode = account.account_mode_label || account.account_mode || '-';
      const swapModeOk = account.supports_swap_trading !== false;
      const riskOk = risk.allowed !== false;
      const rejectCounts = {...(scan.early_rejected_stats?.by_reason || {}), ...(scan.rejected_stats?.by_reason || {})};
      const latestReject = latestRejected(state.events);
      const latestEvent = latestImportantEvent(state.events);
      const hardHalt = state.risk_state?.halted || last.halted || account.supports_swap_trading === false || false;
      const action = cycle.live_execution ? 'EXECUTE' : (selected && riskOk && !hardHalt ? 'READY' : 'WAIT');
      const systemStatus = hardHalt ? 'BLOCKED' : (selected ? (riskOk ? 'RUNNING' : 'BLOCKED') : 'NO SIGNAL');
      const noTradeReason = cycle.live_execution ? '已进入执行流程。' : (
        hardHalt ? humanError(state.risk_state?.halt_detail || state.risk_state?.halt_reason || last.halt_reason || account.mode_help || 'strategy_halted') :
        (!riskOk ? (risk.reasons || []).map(reasonLabel).join('，') :
        (latestReject ? eventSummary(latestReject) : (latestEvent ? eventSummary(latestEvent) : '当前没有满足开仓条件的标的。')))
      );
      return {state, last, cycle, scan, risk, exchange, selected, pos, account, wallet, marginBalance, available, totalPnl, riskOk, rejectCounts, hardHalt, action, systemStatus, noTradeReason, accountMode, swapModeOk};
    };
    const renderCombinedDashboard = (states) => {
      const models = states.map(panelModel);
      const totalWallet = models.reduce((sum, m) => sum + Number(m.wallet || 0), 0);
      const totalEquity = models.reduce((sum, m) => sum + Number(m.marginBalance ?? m.wallet ?? 0), 0);
      const totalPnl = models.reduce((sum, m) => sum + Number(m.totalPnl || 0), 0);
      const totalPositions = models.reduce((sum, m) => sum + m.pos.rows.length, 0);
      const anyBlocked = models.some((m) => m.systemStatus === 'BLOCKED' || m.hardHalt);
      const anyReady = models.some((m) => m.action === 'READY' || m.action === 'EXECUTE');
      const systemStatus = anyBlocked ? 'BLOCKED' : (anyReady ? 'RUNNING' : 'NO SIGNAL');
      const maxUpdated = models.map((m) => m.last.updated_at || m.state.now).filter(Boolean).sort().slice(-1)[0] || '-';
      const exchangeCards = models.map((m) => {
        const title = m.state.panel_name || zh(m.exchange || m.last.profile || '策略');
        const cls = m.action === 'EXECUTE' ? 'good' : (m.action === 'READY' ? 'info' : 'muted');
        return `<div class="exchange-card">
          <div class="exchange-head"><div><div class="exchange-name">${esc(title)}</div><div class="panel-sub">${esc(m.last.updated_at || m.state.now)}</div></div>${pill(m.systemStatus, m.systemStatus === 'BLOCKED' ? 'bad' : (m.systemStatus === 'NO SIGNAL' ? '' : 'good'))}</div>
          <div class="exchange-body">
            <div class="compact-decision">
              <div class="compact-action ${cls}">${esc(m.action)}</div>
              <div class="compact-meta">
                <div class="reason" title="${esc(m.noTradeReason)}">${esc(m.noTradeReason)}</div>
                <div>${statusCard('收益/权益', `${money(m.totalPnl)} / ${money(m.marginBalance ?? m.wallet)}`, `模式 ${m.accountMode} | 可用 ${money(m.available)}`, m.swapModeOk ? (Number(m.totalPnl || 0) >= 0 ? 'good' : 'bad') : 'bad')}</div>
              </div>
            </div>
            <div class="candidate-list">${candidateRows(m.scan, m.selected, m.state.events)}</div>
            <div class="progress-row">${triggerProgress(m.scan, m.selected, m.riskOk && !m.hardHalt)}</div>
          </div>
        </div>`;
      }).join('');
      const funnelCards = models.map((m) => `<div class="exchange-card"><div class="exchange-head"><span class="exchange-name">${esc(m.state.panel_name || zh(m.exchange || '策略'))}</span>${pill(m.action)}</div><div class="exchange-body">${renderFunnel(m.scan, m.cycle, m.rejectCounts, m.riskOk && !m.hardHalt)}</div></div>`).join('');
      const positionSections = models.map((m) => `<div class="exchange-card"><div class="exchange-head"><span class="exchange-name">${esc(m.state.panel_name || zh(m.exchange || '策略'))}</span><span class="muted">${esc(m.pos.rows.length)} 个持仓</span></div><div class="exchange-body"><div class="positions-grid">${positionCards(m.state.positions)}</div></div></div>`).join('');
      const streamCards = models.map((m) => `<div class="exchange-card"><div class="exchange-head"><span class="exchange-name">${esc(m.state.panel_name || zh(m.exchange || '策略'))}</span><span class="muted">交易/事件</span></div><div class="exchange-body"><div class="table-wrap"><table><thead><tr><th>时间</th><th>类型</th><th>标的</th><th>方向</th><th>数量</th><th>价格</th><th>盈亏</th><th>状态</th></tr></thead><tbody>${tradeRows(m.state.events)}</tbody></table></div><div class="table-wrap"><table><thead><tr><th>#</th><th>事件</th><th>标的</th><th>摘要</th></tr></thead><tbody>${eventRows(m.state.events)}</tbody></table></div></div></div>`).join('');
      return `<section class="panel">
        <div class="panel-head"><div><div class="panel-title">合并交易看板</div><div class="panel-sub">OKX 模拟盘 + 币安合约模拟盘，同屏对比</div></div><div class="panel-sub">${esc(maxUpdated)}</div></div>
        <div class="terminal-top">
          ${statusCard('系统状态', systemStatus, `交易所 ${models.length} 个`, systemStatus === 'BLOCKED' ? 'bad' : (systemStatus === 'NO SIGNAL' ? 'muted' : 'good'))}
          ${statusCard('当前动作', models.map((m) => `${m.state.panel_name || zh(m.exchange)}:${m.action}`).join(' | '), anyReady ? '有可执行/执行中信号' : '等待信号', anyReady ? 'info' : 'muted')}
          ${statusCard('总浮盈', money(totalPnl), `总权益 ${money(totalEquity)}`, totalPnl >= 0 ? 'good' : 'bad')}
          ${statusCard('总资金', money(totalWallet), `持仓 ${totalPositions} 个`)}
          ${statusCard('风控', anyBlocked ? '限制/禁止' : '允许', anyBlocked ? '至少一个交易所被拦截' : '未发现硬限制', anyBlocked ? 'bad' : 'good')}
        </div>
        <div class="body">
          <div class="section"><div class="section-title"><span>核心决策</span><span class="muted">两个交易所同屏</span></div><div class="combined-grid">${exchangeCards}</div></div>
          <div class="section"><div class="section-title"><span>策略漏斗对比</span><span class="muted">SCAN / FILTER / STRUCTURE / TRIGGER / RISK / EXECUTE</span></div><div class="combined-grid">${funnelCards}</div></div>
          <div class="section"><div class="section-title"><span>合并持仓区</span><span class="muted">按交易所分组，不再上下分散</span></div><div class="combined-grid">${positionSections}</div></div>
          <div class="section"><div class="section-title"><span>合并风险监控</span><span class="muted">跨交易所总览</span></div><div class="risk-grid">${metric('总风险状态', anyBlocked ? '限制' : '允许', anyBlocked ? '检查 BLOCKED 交易所原因' : '两个交易所均可监控', anyBlocked ? 'bad' : 'good')}${metric('总持仓数', `${totalPositions}`, `OKX/币安合计`)}${metric('总浮盈', money(totalPnl), `账户权益合计 ${money(totalEquity)}`, totalPnl >= 0 ? 'good' : 'bad')}${metric('刷新时间', maxUpdated, '自动 2 秒刷新')}</div></div>
          <div class="section"><div class="section-title"><span>交易状态流</span><span class="muted">左右对比，不用下拉找</span></div><div class="combined-stream">${streamCards}</div></div>
        </div>
      </section>`;
    };
    const renderStrategyPanel = (state) => {
      const m = panelModel(state);
      const last = m.last;
      const cycle = m.cycle;
      const scan = m.scan;
      const risk = m.risk;
      const exchange = m.exchange;
      const selected = m.selected;
      const pos = m.pos;
      const account = m.account;
      const wallet = m.wallet;
      const marginBalance = m.marginBalance;
      const available = m.available;
      const totalPnl = m.totalPnl;
      const riskOk = m.riskOk;
      const title = state.panel_name || zh(exchange || last.profile || '策略');
      const rejectCounts = m.rejectCounts;
      const hardHalt = m.hardHalt;
      const action = m.action;
      const systemStatus = m.systemStatus;
      const env = zh(scan.market_regime?.label || scan.market_regime?.state || 'neutral');
      const noTradeReason = m.noTradeReason;
      return `<section class="panel">
        <div class="panel-head"><div><div class="panel-title">${esc(title)}</div><div class="panel-sub">${esc(state.runtime_state_dir)}</div></div><div class="panel-sub">${esc(last.updated_at || state.now)}</div></div>
        <div class="terminal-top">
          ${statusCard('当前模式', zh(exchange || (last.scan_only ? '扫描' : '待机')), `账户 ${m.accountMode} | 配置 ${zh(last.profile || '-')}`, m.swapModeOk ? (exchange ? 'info' : '') : 'bad')}
          ${statusCard('系统状态', systemStatus, hardHalt ? reasonLabel(state.risk_state?.halt_reason || '-') : `事件 ${state.event_count_loaded || 0} 条`, systemStatus === 'BLOCKED' ? 'bad' : (systemStatus === 'NO SIGNAL' ? 'muted' : 'good'))}
          ${statusCard('市场环境', env, (scan.market_regime?.reasons || []).map(zh).slice(0, 2).join('，') || '-')}
          ${statusCard('风控状态', riskOk && !hardHalt ? '允许' : '限制', (risk.reasons || []).map(zh).join('，') || '-', riskOk && !hardHalt ? 'good' : 'bad')}
          ${statusCard('收益', `${money(totalPnl)} / ${money(marginBalance ?? wallet)}`, `可用 ${money(available)}`, Number(totalPnl || 0) >= 0 ? 'good' : 'bad')}
        </div>
        <div class="body">
          <div class="decision-grid">
            <div class="section"><div class="section-title"><span>核心决策</span>${pill(action, action === 'EXECUTE' ? 'good' : (action === 'READY' ? 'info' : ''))}</div><div class="action ${action === 'EXECUTE' ? 'good' : (action === 'READY' ? 'info' : 'muted')}">${action}</div><div class="reason" title="${esc(noTradeReason)}">${esc(noTradeReason)}</div></div>
            <div class="section"><div class="section-title"><span>当前候选标的</span><span class="muted">只显示最关键 1-2 个</span></div><div class="candidate-list">${candidateRows(scan, selected, state.events)}</div></div>
            <div class="section"><div class="section-title"><span>触发条件进度</span><span class="muted">SCAN -> EXECUTE</span></div><div class="progress-row">${triggerProgress(scan, selected, riskOk && !hardHalt)}</div></div>
          </div>
          <div class="section"><div class="section-title"><span>策略漏斗</span><span class="muted">SCAN / FILTER / STRUCTURE / TRIGGER / RISK / EXECUTE</span></div>${renderFunnel(scan, cycle, rejectCounts, riskOk && !hardHalt)}</div>
          <div class="section"><div class="section-title"><span>持仓区</span><span class="muted">方向、浮盈亏、ROI、强度、风险</span></div><div class="positions-grid">${positionCards(state.positions)}</div></div>
          <div class="section"><div class="section-title"><span>风险监控</span><span class="muted">账户 ${money(wallet)} / 权益 ${money(marginBalance)}</span></div>${riskMetrics(risk, pos, account)}</div>
          <div class="section"><div class="section-title"><span>交易状态流</span><span class="muted">成交、保护单、退出、执行错误</span></div><div class="table-wrap"><table><thead><tr><th>时间</th><th>类型</th><th>标的</th><th>方向</th><th>数量</th><th>价格</th><th>盈亏</th><th>状态</th></tr></thead><tbody>${tradeRows(state.events)}</tbody></table></div></div>
          <div class="section"><div class="section-title"><span>重要事件</span><span class="muted">已隐藏盘口缓存噪音</span></div><div class="table-wrap"><table><thead><tr><th>#</th><th>事件</th><th>标的</th><th>摘要</th></tr></thead><tbody>${eventRows(state.events)}</tbody></table></div></div>
        </div>
      </section>`;
    };
    const renderSpotDemoPanel = (state) => {
      const last = state.last_cycle || {};
      const balances = state.positions || {};
      const rows = Object.values(balances).filter((b) => Number(b.free || 0) || Number(b.locked || 0));
      const total = rows.reduce((sum, b) => sum + (Number(b.usdt_value || 0) || 0), 0);
      return `<section class="panel">
        <div class="panel-head"><div><div class="panel-title">${esc(state.panel_name || '币安现货模拟盘')}</div><div class="panel-sub">${esc(state.runtime_state_dir)}</div></div><div class="panel-sub">${esc(last.updated_at || state.now)}</div></div>
        <div class="grid">
          ${metric('运行模式', '币安现货模拟盘', '模拟环境', 'info')}
          ${metric('连接状态', last.ok === false ? '异常' : '运行中', humanError(last.error || last.message || '-'), last.ok === false ? 'bad' : 'good')}
          ${metric('资产数量', `${rows.length}`, `估值 ${money(total)}`)}
          ${metric('自动轮询', last.auto_loop ? '开启' : '单次', `周期 ${last.poll_interval_sec || '-'} 秒`)}
        </div>
        <div class="body">
          <table><thead><tr><th>资产</th><th>类型</th><th>可用</th><th>冻结</th><th>USDT估值</th><th>状态</th></tr></thead><tbody>${positionRows(balances)}</tbody></table>
          <table><thead><tr><th>#</th><th>事件</th><th>资产</th><th>摘要</th></tr></thead><tbody>${eventRows(state.events)}</tbody></table>
        </div>
      </section>`;
    };
    const render = (payload) => {
      const panels = payload.panels || [payload];
      document.getElementById('panels').innerHTML = panels.length > 1 ? renderCombinedDashboard(panels) : panels.map((state) => renderStrategyPanel(state)).join('');
      applyFocusMode();
      document.getElementById('statusDot').className = 'dot good';
      document.getElementById('statusText').textContent = `已刷新 ${new Date().toLocaleTimeString()} | 合并看板 ${panels.length} 个交易所`;
    };
    const applyFocusMode = () => {
      const enabled = document.getElementById('focusToggle')?.checked;
      const keySymbols = new Set([...document.querySelectorAll('.candidate.key-symbol, .position-card.key-symbol')].map((el) => el.dataset.symbol).filter(Boolean));
      document.querySelectorAll('.event-row').forEach((row) => {
        const sym = row.dataset.symbol || '';
        row.classList.toggle('hidden', Boolean(enabled && sym && keySymbols.size && !keySymbols.has(sym)));
      });
    };
    document.getElementById('focusToggle').addEventListener('change', applyFocusMode);
    async function refresh() {
      try {
        const res = await fetch(apiUrl, { cache: 'no-store' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        render(await res.json());
      } catch (err) {
        document.getElementById('statusDot').className = 'dot bad';
        document.getElementById('statusText').textContent = `读取失败: ${err.message}`;
      }
    }
    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
'''


class DashboardHandler(BaseHTTPRequestHandler):
    runtime_state_dir = DEFAULT_RUNTIME_DIR
    panels: List[Dict[str, str]] = []

    def log_message(self, format: str, *args: Any) -> None:
        return

    def send_payload(self, status: int, content_type: str, payload: bytes) -> None:
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(payload)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == '/':
            self.send_payload(200, 'text/html; charset=utf-8', HTML.encode('utf-8'))
            return
        if parsed.path == '/api/state':
            params = parse_qs(parsed.query)
            limit_raw = params.get('event_limit', ['200'])[0]
            try:
                event_limit = int(limit_raw)
            except ValueError:
                event_limit = 200
            panel_params = params.get('panel', [])
            if panel_params:
                panels = parse_panels(panel_params, self.runtime_state_dir)
                state = load_multi_state(panels, event_limit=max(1, min(event_limit, 1000)))
            elif self.panels:
                state = load_multi_state(self.panels, event_limit=max(1, min(event_limit, 1000)))
            else:
                runtime_dir = params.get('runtime_state_dir', [self.runtime_state_dir])[0] or self.runtime_state_dir
                state = load_dashboard_state(runtime_dir, event_limit=max(1, min(event_limit, 1000)))
            self.send_payload(200, 'application/json; charset=utf-8', json.dumps(state, ensure_ascii=False, indent=2).encode('utf-8'))
            return
        self.send_payload(404, 'application/json; charset=utf-8', json.dumps({'ok': False, 'error': 'not found'}, ensure_ascii=False).encode('utf-8'))


def parse_args(argv: Any = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='策略本地运行面板')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--runtime-state-dir', default=DEFAULT_RUNTIME_DIR)
    parser.add_argument('--panel', action='append', default=[], help='Panel definition: name=runtime-state-dir. Can be repeated or separated by semicolon.')
    return parser.parse_args(argv)


def main(argv: Any = None) -> int:
    load_dotenv()
    args = parse_args(argv)
    DashboardHandler.runtime_state_dir = args.runtime_state_dir
    DashboardHandler.panels = parse_panels(args.panel, args.runtime_state_dir)
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    panel_query = '&'.join(f"panel={panel['name']}={panel['runtime_state_dir']}" for panel in DashboardHandler.panels)
    suffix = f'?{panel_query}' if panel_query else f'?runtime_state_dir={args.runtime_state_dir}'
    print(f'Dashboard: http://{args.host}:{args.port}/{suffix}', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())


