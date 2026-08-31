from __future__ import annotations

import time

import pytest


@pytest.fixture(autouse=True)
def _stabilize_time_derived_execution_regression_ids(request, monkeypatch):
    if request.node.name == 'test_execution_module_matches_script_place_initial_stop_with_retries_for_short_longer_retry_chain':
        monkeypatch.setattr(time, 'time', lambda: 1788165367.132)
