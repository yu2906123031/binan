import importlib.util
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

MODULE_PATH = SCRIPTS_DIR / 'binance_futures_momentum_long.py'
SUMMARY_RENDER_PATH = SCRIPTS_DIR / 'summary_render.py'
spec = importlib.util.spec_from_file_location('binance_futures_momentum_long', MODULE_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)

summary_render_spec = importlib.util.spec_from_file_location('summary_render', SUMMARY_RENDER_PATH)
summary_render_mod = importlib.util.module_from_spec(summary_render_spec)
sys.modules[summary_render_spec.name] = summary_render_mod
assert summary_render_spec.loader is not None
summary_render_spec.loader.exec_module(summary_render_mod)


def test_build_cn_scan_summary_marks_live_when_live_requested_true_without_execution_payload():
    result = {
        'ok': True,
        'cycles': [
            {
                'scan_only': False,
                'live_requested': True,
                'scan': {
                    'market_regime': {'label': 'risk-on', 'score_multiplier': 1.2, 'reasons': ['trend_up']},
                    'candidate_count': 1,
                    'rejected_stats': {'total': 0, 'by_reject_label': {}},
                    'candidate_alerts': [],
                },
            }
        ],
    }

    summary = mod.build_cn_scan_summary(result)

    assert summary['模式'] == 'live'


def test_summary_render_module_matches_script_helpers():
    result = {
        'ok': True,
        'cycles': [
            {
                'scan_only': False,
                'live_requested': True,
                'book_ticker_websocket': {
                    'status': 'unavailable',
                    'reason': 'websocket_client_missing',
                    'health': {
                        'active_streams': [],
                        'subscription_version': 0,
                    },
                },
                'user_data_stream_monitor': {
                    'status': 'refresh_failed',
                    'action': 'refresh_failed',
                    'listen_key': 'dummy-listen-key',
                    'health': {
                        'disconnect_count': 2,
                        'refresh_failure_count': 3,
                        'reconnect_count': 1,
                        'updated_at': '2026-04-20T12:31:00Z',
                        'last_error': 'boom',
                    },
                },
                'user_data_stream_alert': {
                    'level': 'warning',
                    'message': 'listen key stale',
                },
                'scan': {
                    'market_regime': {'label': 'risk-on', 'score_multiplier': 1.1, 'reasons': ['btc_above_ema20']},
                    'candidate_count': 1,
                    'rejected_stats': {'total': 0, 'by_reject_label': {}},
                    'candidate_alerts': [
                        {
                            'symbol': 'WIFUSDT',
                            'alert_tier': 'trade',
                            'state': 'armed',
                            'candidate_stage': 'trade_candidate',
                            'score': 78.5,
                            'price_change_pct_24h': 15.7,
                            'recent_5m_change_pct': 2.4,
                            'position_size_pct': 20.0,
                            'execution_liquidity_grade': 'A',
                        }
                    ],
                },
            }
        ],
    }

    script_summary = mod.build_cn_scan_summary(result)
    module_summary = summary_render_mod.build_cn_scan_summary_data(result, mod.mask_sensitive_token)
    assert module_summary == script_summary

    script_rendered = mod.render_cn_scan_summary(result)
    module_rendered = summary_render_mod.render_cn_scan_summary_text(module_summary, mod.format_num, mod.format_pct)
    assert module_rendered == script_rendered


def test_build_cn_scan_summary_splits_setup_watch_and_trade_candidates():
    result = {
        'ok': True,
        'cycles': [
            {
                'scan_only': False,
                'scan': {
                    'market_regime': {'label': 'risk-on', 'score_multiplier': 1.0, 'reasons': []},
                    'candidate_count': 4,
                    'rejected_stats': {'total': 0, 'by_reject_label': {}},
                    'candidate_alerts': [
                        {
                            'symbol': 'DOGEUSDT',
                            'alert_tier': 'high',
                            'state': 'setup',
                            'candidate_stage': 'setup_candidate',
                            'score': 71.2,
                            'price_change_pct_24h': 12.4,
                            'recent_5m_change_pct': 1.8,
                            'position_size_pct': 18.0,
                            'execution_liquidity_grade': 'A',
                        },
                        {
                            'symbol': 'PEPEUSDT',
                            'alert_tier': 'watch',
                            'state': 'watch',
                            'candidate_stage': 'watch_candidate',
                            'score': 66.1,
                            'price_change_pct_24h': 8.4,
                            'recent_5m_change_pct': 1.2,
                            'position_size_pct': 12.0,
                            'execution_liquidity_grade': 'B',
                        },
                        {
                            'symbol': 'WIFUSDT',
                            'alert_tier': 'trade',
                            'state': 'armed',
                            'candidate_stage': 'trade_candidate',
                            'score': 78.5,
                            'price_change_pct_24h': 15.7,
                            'recent_5m_change_pct': 2.4,
                            'position_size_pct': 20.0,
                            'execution_liquidity_grade': 'A',
                        },
                        {
                            'symbol': 'BONKUSDT',
                            'alert_tier': 'info',
                            'state': 'unknown',
                            'candidate_stage': 'cooldown_candidate',
                            'score': 55.0,
                            'price_change_pct_24h': 4.2,
                            'recent_5m_change_pct': 0.8,
                            'position_size_pct': 8.0,
                            'execution_liquidity_grade': 'C',
                        },
                    ],
                },
            }
        ],
    }

    summary = mod.build_cn_scan_summary(result)
    rendered = mod.render_cn_scan_summary(result)

    assert [item['交易对'] for item in summary['setup候选列表']] == ['DOGEUSDT']
    assert [item['交易对'] for item in summary['watch候选列表']] == ['PEPEUSDT']
    assert [item['交易对'] for item in summary['trade候选列表']] == ['WIFUSDT']
    assert [item['交易对'] for item in summary['其他候选列表']] == ['BONKUSDT']
    assert summary['扫描概览']['阶段分布'] == {
        'setup_candidate': 1,
        'watch_candidate': 1,
        'trade_candidate': 1,
        'other': 1,
    }
    assert 'Trade 候选' in rendered
    assert 'Setup 候选' in rendered
    assert 'Watch 候选' in rendered
    assert '其他候选' in rendered
    assert rendered.index('Trade 候选') < rendered.index('Setup 候选') < rendered.index('Watch 候选') < rendered.index('其他候选')
    assert '观察候选' not in rendered
    assert 'DOGEUSDT' in rendered
    assert 'PEPEUSDT' in rendered
    assert 'WIFUSDT' in rendered
    assert 'BONKUSDT' in rendered
    assert '阶段分布: setup 1 | watch 1 | trade 1 | other 1' in rendered


def test_build_cn_scan_summary_stage_distribution_counts_full_candidate_set_but_lists_top_five():
    candidate_alerts = []
    stages = [
        ('AAAUSDT', 'setup_candidate'),
        ('BBBUSDT', 'watch_candidate'),
        ('CCCUSDT', 'trade_candidate'),
        ('DDDUSDT', 'trade_candidate'),
        ('EEEUSDT', 'cooldown_candidate'),
        ('FFFUSDT', 'setup_candidate'),
        ('GGGUSDT', 'watch_candidate'),
    ]
    for idx, (symbol, stage) in enumerate(stages, start=1):
        candidate_alerts.append({
            'symbol': symbol,
            'alert_tier': 'tier',
            'state': f'state-{idx}',
            'candidate_stage': stage,
            'score': 70 + idx,
            'price_change_pct_24h': 10 + idx,
            'recent_5m_change_pct': 1 + idx / 10,
            'position_size_pct': 5 + idx,
            'execution_liquidity_grade': 'A',
        })

    result = {
        'ok': True,
        'cycles': [{
            'scan_only': False,
            'scan': {
                'market_regime': {'label': 'risk-on', 'score_multiplier': 1.0, 'reasons': []},
                'candidate_count': len(candidate_alerts),
                'rejected_stats': {'total': 0, 'by_reject_label': {}},
                'candidate_alerts': candidate_alerts,
            },
        }],
    }

    summary = mod.build_cn_scan_summary(result)

    assert [item['交易对'] for item in summary['候选列表']] == ['AAAUSDT', 'BBBUSDT', 'CCCUSDT', 'DDDUSDT', 'EEEUSDT']
    assert [item['交易对'] for item in summary['setup候选列表']] == ['AAAUSDT']
    assert [item['交易对'] for item in summary['watch候选列表']] == ['BBBUSDT']
    assert [item['交易对'] for item in summary['trade候选列表']] == ['CCCUSDT', 'DDDUSDT']
    assert [item['交易对'] for item in summary['其他候选列表']] == ['EEEUSDT']
    assert summary['扫描概览']['阶段分布'] == {
        'setup_candidate': 2,
        'watch_candidate': 2,
        'trade_candidate': 2,
        'other': 1,
    }


def test_build_cn_scan_summary_marks_live_when_live_requested_false_but_live_flag_true():
    result = {
        'ok': True,
        'live': True,
        'cycles': [
            {
                'scan_only': False,
                'live_requested': False,
                'scan': {
                    'market_regime': {'label': 'risk-on', 'score_multiplier': 1.0, 'reasons': []},
                    'candidate_count': 0,
                    'rejected_stats': {'total': 0, 'by_reject_label': {}},
                    'candidate_alerts': [],
                },
            }
        ],
    }

    summary = mod.build_cn_scan_summary(result)

    assert summary['模式'] == 'live'


def test_build_cn_scan_summary_includes_runtime_health_sections():
    result = {
        'ok': True,
        'cycles': [
            {
                'scan_only': False,
                'live_requested': True,
                'book_ticker_websocket': {
                    'status': 'unavailable',
                    'reason': 'websocket_client_missing',
                    'health': {
                        'active_streams': [],
                        'subscription_version': 0,
                    },
                },
                'user_data_stream_monitor': {
                    'status': 'refresh_failed',
                    'action': 'refresh_failed',
                    'listen_key': 'dummy-listen-key',
                    'health': {
                        'disconnect_count': 2,
                        'refresh_failure_count': 3,
                        'reconnect_count': 1,
                        'updated_at': '2026-04-20T12:31:00Z',
                        'last_error': 'boom',
                    },
                },
                'user_data_stream_alert': {
                    'level': 'warning',
                    'message': 'listen key stale',
                },
                'scan': {
                    'market_regime': {'label': 'risk-on', 'score_multiplier': 1.1, 'reasons': ['btc_above_ema20']},
                    'candidate_count': 0,
                    'rejected_stats': {'total': 0, 'by_reject_label': {}},
                    'candidate_alerts': [],
                },
            }
        ],
    }

    summary = mod.build_cn_scan_summary(result)
    rendered = mod.render_cn_scan_summary(result)

    assert summary['运行监控']['BookTicker WS']['状态'] == 'unavailable'
    assert summary['运行监控']['BookTicker WS']['原因'] == 'websocket_client_missing'
    assert summary['运行监控']['User Data Stream']['状态'] == 'refresh_failed'
    assert summary['运行监控']['User Data Stream']['动作'] == 'refresh_failed'
    assert summary['运行监控']['User Data Stream']['listen_key'] == 'du***ey'
    assert summary['运行监控']['User Data Stream']['断线次数'] == 2
    assert summary['运行监控']['User Data Stream']['续期失败次数'] == 3
    assert summary['运行监控']['User Data Stream']['重连次数'] == 1
    assert summary['运行监控']['User Data Stream']['最近错误'] == 'boom'
    assert summary['运行监控']['告警']['级别'] == 'warning'
    assert summary['运行监控']['告警']['消息'] == 'listen key stale'
    assert 'BookTicker WS: unavailable | 原因 websocket_client_missing' in rendered
    assert 'User Data Stream: refresh_failed | 动作 refresh_failed | 断线 2 | 续期失败 3 | 重连 1 | 更新时间 2026-04-20T12:31:00Z | 错误 boom' in rendered
    assert '运行告警: warning | listen key stale' in rendered
