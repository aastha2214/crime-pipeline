"""field_ids_for must wait for Metabase's background scan on a fresh install instead of crashing (StopIteration)."""
import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("setup_metabase", Path(__file__).resolve().parent.parent / "scripts" / "setup_metabase.py")
sm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sm)

TABLE = {"tables": [{"name": "cpd_incidents", "fields": [
    {"name": "date_time", "id": 1}, {"name": "type", "id": 2}, {"name": "location", "id": 3}]}]}


def fake_call_factory(responses):
    calls = []

    def fake_call(s, method, path, **kw):
        calls.append((method, path))
        if method == "GET":
            return responses.pop(0) if len(responses) > 1 else responses[0]
        return None
    return fake_call, calls


def test_waits_until_table_appears(monkeypatch):
    # scan not finished: no tables, then table with no fields yet, then the real thing
    fake, calls = fake_call_factory([{"tables": []}, {"tables": [{"name": "cpd_incidents", "fields": []}]}, TABLE])
    monkeypatch.setattr(sm, "call", fake)
    monkeypatch.setattr(sm.time, "sleep", lambda s: None)
    assert sm.field_ids_for(None, 7) == {"date_time": 1, "type": 2, "location": 3}
    assert sum(1 for m, _ in calls if m == "GET") == 3
    assert [c for c in calls if c[0] == "POST"] == [("POST", "/api/database/7/sync_schema")]  # re-triggers the scan once


def test_gives_clear_error_on_timeout(monkeypatch):
    fake, _ = fake_call_factory([{"tables": []}])
    monkeypatch.setattr(sm, "call", fake)
    monkeypatch.setattr(sm.time, "sleep", lambda s: None)
    clock = iter(range(0, 10_000, 100))
    monkeypatch.setattr(sm.time, "time", lambda: next(clock))
    with pytest.raises(SystemExit):
        sm.field_ids_for(None, 7, timeout=300)


def test_returns_immediately_when_ready(monkeypatch):
    fake, calls = fake_call_factory([TABLE])
    monkeypatch.setattr(sm, "call", fake)
    assert sm.field_ids_for(None, 7)["location"] == 3
    assert calls == [("GET", "/api/database/7/metadata")]
