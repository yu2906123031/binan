from pathlib import Path

from scripts.runtime_store import validate_runtime_state_layout


def test_inaccessible_unused_legacy_path_does_not_block_canonical_store(tmp_path, monkeypatch):
    canonical_dir = tmp_path / 'canonical-runtime-state'
    legacy_dir = Path('/inaccessible/legacy-runtime-state')
    original_is_symlink = Path.is_symlink

    def guarded_is_symlink(path):
        if path == legacy_dir:
            raise PermissionError(str(path))
        return original_is_symlink(path)

    monkeypatch.setattr(Path, 'is_symlink', guarded_is_symlink)

    resolved = validate_runtime_state_layout(
        configured_dir=canonical_dir,
        canonical_dir=canonical_dir,
        legacy_dir=legacy_dir,
    )

    assert resolved == canonical_dir
    assert canonical_dir.is_dir()
