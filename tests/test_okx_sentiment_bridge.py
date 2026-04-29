import argparse
import importlib.util
import json
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'okx_sentiment_bridge.py'
spec = importlib.util.spec_from_file_location('okx_sentiment_bridge', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_extract_rows_handles_nested_rows_shape():
    payload = {
        'ok': True,
        'data': {
            'data': [
                {
                    'rows': [
                        {'instId': 'DOGE-USDT-SWAP', 'oiDeltaPct': '3.2'},
                        {'instId': 'SUI-USDT-SWAP', 'oiDeltaPct': '2.1'},
                    ]
                }
            ]
        },
    }

    rows = mod._extract_rows(payload)

    assert len(rows) == 2
    assert rows[0]['instId'] == 'DOGE-USDT-SWAP'


def test_merge_sentiment_trend_updates_score_and_acceleration():
    base = {
        'DOGEUSDT': {
            'okx_sentiment_score': 0.1,
            'okx_sentiment_acceleration': 0.0,
            'sector_resonance_score': 0.2,
            'smart_money_flow_score': 0.0,
        }
    }
    payload = {
        'ok': True,
        'data': {
            'data': [
                {
                    'coin': 'DOGEUSDT',
                    'points': [
                        {'bullishRatio': 0.40, 'bearishRatio': 0.30},
                        {'bullishRatio': 0.65, 'bearishRatio': 0.15},
                    ],
                }
            ]
        },
    }

    mod._merge_sentiment_trend(base, payload)

    assert round(base['DOGEUSDT']['okx_sentiment_score'], 4) == 0.5
    assert round(base['DOGEUSDT']['okx_sentiment_acceleration'], 4) == 0.4
    assert base['DOGEUSDT']['sector_resonance_score'] > 0.2


def test_build_bridge_payload_combines_market_and_news_data(monkeypatch):
    args = argparse.Namespace(
        stdio_command='fake-stdio',
        name='okx_bridge',
        symbols='DOGEUSDT,SUIUSDT',
        symbols_file='',
        top_hot=5,
        oi_top=5,
        oi_bar='5m',
        oi_history_limit=6,
        min_oi_usd='1000000',
        min_vol_usd_24h='5000000',
        min_abs_oi_delta_pct='1',
        quote_ccy='USDT',
        settle_ccy='USDT',
        period='1h',
        trend_points=24,
        news_required=False,
        output_format='lines',
    )

    def fake_run(_stdio, _name, tool, tool_args):
        if tool == 'market_filter':
            return {
                'ok': True,
                'data': {
                    'data': [
                        {'rows': [
                            {'instId': 'DOGE-USDT-SWAP'},
                            {'instId': 'SUI-USDT-SWAP'},
                        ]}
                    ]
                },
            }
        if tool == 'market_filter_oi_change':
            return {
                'ok': True,
                'data': {
                    'data': [
                        {'rows': [
                            {'instId': 'DOGE-USDT-SWAP', 'oiDeltaPct': '3.0', 'pxChgPct': '5.0', 'fundingRate': '-0.0020'},
                            {'instId': 'SUI-USDT-SWAP', 'oiDeltaPct': '1.2', 'pxChgPct': '-1.0', 'fundingRate': '0.0010'},
                        ]}
                    ]
                },
            }
        if tool == 'news_get_sentiment_ranking':
            return {
                'ok': True,
                'data': {
                    'data': [
                        {'coin': 'DOGEUSDT', 'bullishRatio': 0.72, 'bearishRatio': 0.18, 'mentionCount': 120},
                        {'coin': 'SUIUSDT', 'bullishRatio': 0.44, 'bearishRatio': 0.31, 'mentionCount': 25},
                    ]
                },
            }
        if tool == 'news_get_coin_sentiment':
            assert tool_args['coins'] == 'DOGEUSDT,SUIUSDT'
            return {
                'ok': True,
                'data': {
                    'data': [
                        {'coin': 'DOGEUSDT', 'points': [
                            {'bullishRatio': 0.50, 'bearishRatio': 0.30},
                            {'bullishRatio': 0.68, 'bearishRatio': 0.18},
                        ]},
                        {'coin': 'SUIUSDT', 'points': [
                            {'bullishRatio': 0.40, 'bearishRatio': 0.35},
                            {'bullishRatio': 0.43, 'bearishRatio': 0.34},
                        ]},
                    ]
                },
            }
        raise AssertionError(f'unexpected tool {tool}')

    monkeypatch.setattr(mod, '_run_mcporter', fake_run)

    payload = mod.build_bridge_payload(args)

    assert set(payload) == {'DOGEUSDT', 'SUIUSDT'}
    assert payload['DOGEUSDT']['okx_sentiment_score'] > payload['SUIUSDT']['okx_sentiment_score']
    assert payload['DOGEUSDT']['okx_sentiment_acceleration'] > 0
    assert payload['DOGEUSDT']['smart_money_flow_score'] > 0
    assert payload['SUIUSDT']['smart_money_flow_score'] < 0


def test_run_mcporter_uses_current_stdio_call_syntax(monkeypatch):
    captured = {}

    class Completed:
        stdout = json.dumps({'ok': True, 'data': {'data': []}})
        stderr = ''

    def fake_run(command, capture_output, text, check):
        captured['command'] = command
        captured['capture_output'] = capture_output
        captured['text'] = text
        captured['check'] = check
        return Completed()

    monkeypatch.setattr(mod.subprocess, 'run', fake_run)
    monkeypatch.setattr(mod, '_mcporter_executable', lambda: 'mcporter')

    payload = mod._run_mcporter('okx-trade-mcp --modules market,skills --read-only --no-log', 'okx_bridge', 'market_filter', {'symbols': 'DOGEUSDT'})

    assert payload['ok'] is True
    assert captured['command'][:4] == ['mcporter', 'call', '--stdio', 'okx-trade-mcp --modules market,skills --read-only --no-log']
    assert '--tool' in captured['command']
    assert 'market_filter' in captured['command']
    assert '--args' in captured['command']
    assert '--output' in captured['command']


def test_build_bridge_payload_falls_back_to_market_only_when_news_unavailable(monkeypatch):
    args = argparse.Namespace(
        stdio_command='fake-stdio',
        name='okx_bridge',
        symbols='DOGEUSDT',
        symbols_file='',
        top_hot=5,
        oi_top=5,
        oi_bar='5m',
        oi_history_limit=6,
        min_oi_usd='1000000',
        min_vol_usd_24h='5000000',
        min_abs_oi_delta_pct='1',
        quote_ccy='USDT',
        settle_ccy='USDT',
        period='1h',
        trend_points=24,
        news_required=False,
        output_format='lines',
    )

    def fake_run(_stdio, _name, tool, _tool_args):
        if tool == 'market_filter':
            return {'ok': True, 'data': {'data': [{'rows': [{'instId': 'DOGE-USDT-SWAP'}]}]}}
        if tool == 'market_filter_oi_change':
            return {'ok': True, 'data': {'data': [{'rows': [{'instId': 'DOGE-USDT-SWAP', 'oiDeltaPct': '2.5', 'pxChgPct': '2.0', 'fundingRate': '-0.0015'}]}]}}
        if tool.startswith('news_'):
            raise RuntimeError('news offline')
        raise AssertionError(f'unexpected tool {tool}')

    monkeypatch.setattr(mod, '_run_mcporter', fake_run)

    payload = mod.build_bridge_payload(args)

    assert payload['DOGEUSDT']['smart_money_flow_score'] > 0
    assert payload['DOGEUSDT']['sector_resonance_score'] > 0


def test_emit_lines_matches_strategy_text_format():
    text = mod.emit_lines(
        {
            'DOGEUSDT': {
                'okx_sentiment_score': 0.7,
                'okx_sentiment_acceleration': 0.2,
                'sector_resonance_score': 0.4,
                'smart_money_flow_score': -0.1,
            }
        }
    )

    parsed = mod.strategy.parse_okx_sentiment_payload(text)

    assert parsed['DOGEUSDT']['okx_sentiment_score'] == 0.7
    assert parsed['DOGEUSDT']['smart_money_flow_score'] == -0.1
