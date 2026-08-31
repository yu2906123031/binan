import importlib.util
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

SCRIPT_PATH = SCRIPTS_DIR / 'trade_bucket_analysis_cli.py'
spec = importlib.util.spec_from_file_location('trade_bucket_analysis_cli_test_mod', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_cli_installs_directional_slippage_before_running(monkeypatch):
    calls = []

    def install(module):
        calls.append(('install', module))

    def run_main():
        calls.append(('main', mod.trade_bucket_module))
        return 7

    monkeypatch.setattr(mod, 'install_trade_bucket_slippage_hardening', install)
    monkeypatch.setattr(mod.trade_bucket_module, 'main', run_main)

    assert mod.main() == 7
    assert calls == [
        ('install', mod.trade_bucket_module),
        ('main', mod.trade_bucket_module),
    ]
