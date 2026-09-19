from pathlib import Path
import pytest

from app.backfill_store import BackfillStore


def test_register_and_dedup(tmp_path: Path) -> None:
    db_file = str(tmp_path / "backfill.db")
    store = BackfillStore(db_file)

    assert store.get("vid-1") is None

    inserted = store.register("vid-1", "chan-1", "Video 1", duration=120)
    assert inserted is True

    duplicate = store.register("vid-1", "chan-1", "Video 1 duplicate", duration=120)
    assert duplicate is False

    row = store.get("vid-1")
    assert row is not None
    assert row["video_id"] == "vid-1"
    assert row["channel_id"] == "chan-1"
    assert row["title"] == "Video 1"
    assert row["duration"] == 120
    assert row["status"] == "pending"
    assert row["attempts"] == 0
    assert row["last_error"] is None
    assert row["persisted_at"] is None
    assert row["indexed_at"] is None
    assert row["created_at"] is not None
    assert row["updated_at"] is not None
    store.close()


def test_todo_ordering_limit_and_max_attempts(tmp_path: Path) -> None:
    db_file = str(tmp_path / "backfill.db")
    store = BackfillStore(db_file)

    for i in range(1, 6):
        store.register(f"vid-{i}", "chan-1", f"Video {i}")

    # vid-1: pending (attempts=0)
    # vid-2: error (attempts=1)
    store.mark_error("vid-2", "temporary failure")

    # vid-3: error (attempts=5)
    for _ in range(5):
        store.mark_error("vid-3", "persistent error")

    # vid-4: done
    store.mark_done("vid-4", "2026-09-18T10:00:00Z", "uuid-4", '{"id": 4}', "{}")

    # vid-5: error (attempts=2)
    store.mark_error("vid-5", "transient failure 1")
    store.mark_error("vid-5", "transient failure 2")

    # todo with default max_attempts=5: vid-1 (pending), vid-2 (attempts 1), vid-5 (attempts 2)
    # vid-3 excluded (attempts=5 >= 5), vid-4 excluded (done)
    all_todo = store.todo(limit=0, max_attempts=5)
    assert [r["video_id"] for r in all_todo] == ["vid-1", "vid-2", "vid-5"]

    # limit applies
    limited_todo = store.todo(limit=2, max_attempts=5)
    assert [r["video_id"] for r in limited_todo] == ["vid-1", "vid-2"]

    # stricter max_attempts=2 excludes vid-5 (attempts=2)
    strict_todo = store.todo(limit=0, max_attempts=2)
    assert [r["video_id"] for r in strict_todo] == ["vid-1", "vid-2"]

    store.close()


def test_state_transitions_and_attempts_counting(tmp_path: Path) -> None:
    db_file = str(tmp_path / "backfill.db")
    store = BackfillStore(db_file)

    store.register("vid-1", "chan-1", "Video 1")
    initial = store.get("vid-1")
    assert initial is not None
    assert initial["status"] == "pending"
    assert initial["attempts"] == 0

    # mark_error increments attempts and truncates error to 500 chars
    long_error = "x" * 600
    store.mark_error("vid-1", long_error)
    errored = store.get("vid-1")
    assert errored is not None
    assert errored["status"] == "error"
    assert errored["attempts"] == 1
    assert errored["last_error"] == "x" * 500

    # mark_done clears last_error, increments attempts, updates status
    store.mark_done(
        video_id="vid-1",
        published_at="2026-09-18T12:00:00Z",
        article_uuid="art-uuid-1",
        pending_article_json='{"title": "Video 1"}',
        metadata_json='{"duration": 120}',
    )
    done = store.get("vid-1")
    assert done is not None
    assert done["status"] == "done"
    assert done["attempts"] == 2
    assert done["last_error"] is None
    assert done["published_at"] == "2026-09-18T12:00:00Z"
    assert done["article_uuid"] == "art-uuid-1"
    assert done["pending_article_json"] == '{"title": "Video 1"}'
    assert done["metadata_json"] == '{"duration": 120}'

    # mark_no_transcript increments attempts and sets status
    store.register("vid-2", "chan-1", "Video 2")
    store.mark_no_transcript("vid-2", published_at="2026-09-18T13:00:00Z")
    no_trans = store.get("vid-2")
    assert no_trans is not None
    assert no_trans["status"] == "no_transcript"
    assert no_trans["attempts"] == 1
    assert no_trans["published_at"] == "2026-09-18T13:00:00Z"

    store.close()


def test_undelivered_lists_and_shrink(tmp_path: Path) -> None:
    db_file = str(tmp_path / "backfill.db")
    store = BackfillStore(db_file)

    store.register("vid-1", "chan-1", "Video 1")
    store.register("vid-2", "chan-1", "Video 2")
    store.register("vid-3", "chan-1", "Video 3")

    store.mark_done("vid-1", "2026-09-18T10:00:00Z", "uuid-1", '{"text": "1"}', "{}")
    store.mark_done("vid-2", "2026-09-18T10:01:00Z", "uuid-2", '{"text": "2"}', "{}")
    store.mark_no_transcript("vid-3")

    undelivered_pg = store.undelivered_postgres()
    undelivered_idx = store.undelivered_indexer()
    assert [r["video_id"] for r in undelivered_pg] == ["vid-1", "vid-2"]
    assert [r["video_id"] for r in undelivered_idx] == ["vid-1", "vid-2"]

    # Persisting vid-1 shrinks Postgres list only
    store.mark_persisted("vid-1")
    assert [r["video_id"] for r in store.undelivered_postgres()] == ["vid-2"]
    assert [r["video_id"] for r in store.undelivered_indexer()] == ["vid-1", "vid-2"]

    # Indexing vid-1 shrinks Indexer list only
    store.mark_indexed("vid-1")
    assert [r["video_id"] for r in store.undelivered_postgres()] == ["vid-2"]
    assert [r["video_id"] for r in store.undelivered_indexer()] == ["vid-2"]

    # Persist and index vid-2 leaves both lists empty
    store.mark_persisted("vid-2")
    store.mark_indexed("vid-2")
    assert len(store.undelivered_postgres()) == 0
    assert len(store.undelivered_indexer()) == 0

    # Empty pending_article_json does not appear in undelivered_indexer
    store.register("vid-4", "chan-1", "Video 4")
    store.mark_done("vid-4", "2026-09-18T10:05:00Z", "uuid-4", "", "{}")
    assert [r["video_id"] for r in store.undelivered_postgres()] == ["vid-4"]
    assert len(store.undelivered_indexer()) == 0

    store.close()


def test_no_transcript_never_appears_in_undelivered(tmp_path: Path) -> None:
    db_file = str(tmp_path / "backfill.db")
    store = BackfillStore(db_file)

    store.register("vid-1", "chan-1", "Video 1")
    store.mark_no_transcript("vid-1")

    assert len(store.undelivered_postgres()) == 0
    assert len(store.undelivered_indexer()) == 0
    store.close()


def test_stats_numbers(tmp_path: Path) -> None:
    db_file = str(tmp_path / "backfill.db")
    store = BackfillStore(db_file)

    initial_stats = store.stats()
    assert initial_stats == {
        "total": 0,
        "pending": 0,
        "done": 0,
        "no_transcript": 0,
        "error": 0,
        "persisted": 0,
        "indexed": 0,
        "pending_postgres": 0,
        "pending_indexer": 0,
    }

    store.register("vid-1", "chan-1", "Video 1")
    store.register("vid-2", "chan-1", "Video 2")
    store.register("vid-3", "chan-1", "Video 3")
    store.register("vid-4", "chan-1", "Video 4")

    store.mark_done("vid-2", "2026-09-18T10:00:00Z", "uuid-2", '{"data": 2}', "{}")
    store.mark_no_transcript("vid-3")
    store.mark_error("vid-4", "some error")

    stats = store.stats()
    assert stats["total"] == 4
    assert stats["pending"] == 1
    assert stats["done"] == 1
    assert stats["no_transcript"] == 1
    assert stats["error"] == 1
    assert stats["persisted"] == 0
    assert stats["indexed"] == 0
    assert stats["pending_postgres"] == 1
    assert stats["pending_indexer"] == 1

    store.mark_persisted("vid-2")
    assert store.stats()["persisted"] == 1
    assert store.stats()["pending_postgres"] == 0

    store.mark_indexed("vid-2")
    assert store.stats()["indexed"] == 1
    assert store.stats()["pending_indexer"] == 0

    store.close()


def test_state_get_set(tmp_path: Path) -> None:
    db_file = str(tmp_path / "backfill.db")
    store = BackfillStore(db_file)

    assert store.get_state("cursor") is None
    assert store.get_state("cursor", default="default_cursor") == "default_cursor"

    store.set_state("cursor", "page_1")
    assert store.get_state("cursor") == "page_1"

    store.set_state("cursor", "page_2")
    assert store.get_state("cursor") == "page_2"

    store.close()


def test_reopening_same_db_preserves_rows(tmp_path: Path) -> None:
    db_file = str(tmp_path / "backfill.db")
    store1 = BackfillStore(db_file)
    store1.register("vid-persist", "chan-1", "Persistent Video", duration=300)
    store1.mark_done(
        "vid-persist",
        "2026-09-18T10:00:00Z",
        "uuid-p",
        '{"title": "Persistent"}',
        '{"meta": "data"}',
    )
    store1.set_state("checkpoint", "vid-persist")
    store1.close()

    store2 = BackfillStore(db_file)
    row = store2.get("vid-persist")
    assert row is not None
    assert row["video_id"] == "vid-persist"
    assert row["title"] == "Persistent Video"
    assert row["status"] == "done"
    assert row["duration"] == 300
    assert row["article_uuid"] == "uuid-p"
    assert row["pending_article_json"] == '{"title": "Persistent"}'
    assert store2.get_state("checkpoint") == "vid-persist"

    stats = store2.stats()
    assert stats["total"] == 1
    assert stats["done"] == 1
    assert stats["pending_postgres"] == 1
    store2.close()


def test_memory_database() -> None:
    store = BackfillStore(":memory:")
    assert store.register("vid-mem", "chan-1", "Memory Video") is True
    assert store.get("vid-mem") is not None
    store.close()
