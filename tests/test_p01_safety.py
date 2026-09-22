import json

from app.store import CaptionsStore


def test_changed_caption_body_is_quarantined(tmp_path):
    store = CaptionsStore(str(tmp_path / "captions.sqlite"))
    assert store.record_video("v", "c", "t", "", True,
                             json.dumps({"id": "v", "raw_content": "one"}))
    assert not store.record_video("v", "c", "t", "", True,
                                  json.dumps({"id": "v", "raw_content": "two"}))
    assert store.get_video("v")["review_state"] == "revision_needed"


def test_backfill_quarantine_is_durable_and_not_todo(tmp_path):
    from app.backfill_store import BackfillStore

    store = BackfillStore(str(tmp_path / "backfill.sqlite"))
    store.register("v", "c", "title")
    store.quarantine("v", "changed_body_quarantined")
    row = store.get("v")
    assert row["review_state"] == "revision_needed"
    assert row["status"] == "quarantined"
    assert store.todo() == []
