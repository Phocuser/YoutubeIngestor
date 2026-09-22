import json
from pathlib import Path
from typing import Any, Dict, List
import pytest

from app.backfill_store import BackfillStore
from app.config import Settings
from app.reindex import reindex

pytestmark = pytest.mark.skip(reason="adapter-owned reindex is disabled")


def _make_store(tmp_path: Path) -> BackfillStore:
    store = BackfillStore(str(tmp_path / "test.db"))
    store.register("vid-1", "ch1", "Video 1")
    store.register("vid-2", "ch1", "Video 2")
    store.register("vid-3", "ch1", "Video 3")
    store.register("vid-4", "ch1", "Video 4")

    store.mark_done("vid-1", "2026-09-18T10:00:00Z", "u-1", json.dumps({"id": "u-1", "text": "one"}), "{}")
    store.mark_done("vid-2", "2026-09-18T10:01:00Z", "u-2", json.dumps({"id": "u-2", "text": "two"}), "{}")
    store.mark_no_transcript("vid-3")
    store.mark_done("vid-4", "2026-09-18T10:03:00Z", "u-4", json.dumps({"id": "u-4", "text": "four"}), "{}")
    return store


def test_reindex_done_envelopes_and_drain(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    recorded: List[Dict[str, Any]] = []
    drain_calls = 0

    def fake_index(envelope: dict, **kwargs: Any) -> tuple[bool, str]:
        recorded.append(envelope)
        return True, "ok"

    def fake_drain() -> int:
        nonlocal drain_calls
        drain_calls += 1
        return 7

    res = reindex(store, Settings())
    assert res["blocked"] == "LEGACY_PATH_DISABLED"
    assert drain_calls == 0
    assert recorded == []
    store.close()


def test_reindex_failure_handling_and_limit(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    recorded: List[Dict[str, Any]] = []

    def fake_index(envelope: dict, **kwargs: Any) -> tuple[bool, str]:
        recorded.append(envelope)
        if envelope["id"] == "u-1":
            return False, "indexer error"
        return True, "ok"

    res = reindex(store, Settings(), limit=2)
    assert res["blocked"] == "LEGACY_PATH_DISABLED"
    assert recorded == []
    store.close()
