import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
SCRIPT_PATH = SCRIPTS_DIR / 'runtime_state_hardening.py'
spec = importlib.util.spec_from_file_location('runtime_state_hardening', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def test_infer_opened_at_prefers_entry_fill_timestamp():
    position = {
        'entry_fill_recorded_at': '2026-08-30T10:00:00Z',
        'prediction_recorded_at': '2026-08-30T09:59:00Z',
    }
    assert mod.infer_opened_at(position) == '2026-08-30T10:00:00Z'


def test_infer_opened_at_accepts_epoch_milliseconds():
    assert mod.infer_opened_at({'entry_timestamp': 1_700_000_000_000}) == '2023-11-14T22:13:20Z'


def test_restore_hook_injects_historical_opened_at_before_original():
    seen = []

    def restore(position, *args, **kwargs):
        seen.append(dict(position))
        return dict(position)

    runtime_store = SimpleNamespace(restore_position_lifecycle_fields=restore, RuntimeStateStore=None)
    mod.install_runtime_state_hardening(runtime_store)
    result = runtime_store.restore_position_lifecycle_fields({'entry_fill_recorded_at': '2026-08-30T10:00:00Z'})
    assert seen[0]['opened_at'] == '2026-08-30T10:00:00Z'
    assert result['opened_at'] == '2026-08-30T10:00:00Z'


def test_durable_write_fsyncs_file_and_directory(monkeypatch, tmp_path):
    calls = []

    class Store:
        def _atomic_write_json(self, path, payload):
            path.write_text('ok', encoding='utf-8')
            return payload

    runtime_store = SimpleNamespace(RuntimeStateStore=Store, restore_position_lifecycle_fields=lambda position, *a, **k: position)
    monkeypatch.setattr(mod, '_fsync_path_and_directory', lambda path: calls.append(Path(path)))
    mod.install_runtime_state_hardening(runtime_store)
    path = tmp_path / 'state.json'
    result = Store()._atomic_write_json(path, {'x': 1})
    assert result == {'x': 1}
    assert calls == [path]


def test_runtime_hardening_install_is_idempotent():
    class Store:
        def _atomic_write_json(self, path, payload):
            return payload

    runtime_store = SimpleNamespace(RuntimeStateStore=Store, restore_position_lifecycle_fields=lambda position, *a, **k: position)
    mod.install_runtime_state_hardening(runtime_store)
    first_write = Store._atomic_write_json
    first_restore = runtime_store.restore_position_lifecycle_fields
    mod.install_runtime_state_hardening(runtime_store)
    assert Store._atomic_write_json is first_write
    assert runtime_store.restore_position_lifecycle_fields is first_restore
