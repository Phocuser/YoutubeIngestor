import sqlite3
import os
import tempfile
import pytest

from app.store import CaptionsStore


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False) as f:
        db_path = f.name
    try:
        yield db_path
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)


def test_record_video_dedupes(temp_db):
    store = CaptionsStore(temp_db)
    assert not store.is_processed("vid-1")

    # First record should succeed
    inserted = store.record_video(
        video_id="vid-1",
        channel_id="chan-1",
        title="Sample Video 1",
        published_at="2026-09-18T10:00:00Z",
        has_transcript=True,
        pending_article_json='{"id": "vid-1"}',
    )
    assert inserted is True
    assert store.is_processed("vid-1") is True

    # Duplicate record should return False
    duplicate = store.record_video(
        video_id="vid-1",
        channel_id="chan-1",
        title="Sample Video 1 duplicate",
        published_at="2026-09-18T10:00:00Z",
        has_transcript=True,
    )
    assert duplicate is False


def test_record_video_without_transcript(temp_db):
    store = CaptionsStore(temp_db)
    inserted = store.record_video(
        video_id="vid-2",
        channel_id="chan-1",
        title="Video Without Captions",
        published_at="2026-09-18T11:00:00Z",
        has_transcript=False,
        pending_article_json=None,
    )
    assert inserted is True
    assert store.is_processed("vid-2") is True

    item = store.get_video("vid-2")
    assert item is not None
    assert item["video_id"] == "vid-2"
    assert item["has_transcript"] is False
    assert item["pending_article_json"] is None


def test_get_recent_and_count(temp_db):
    store = CaptionsStore(temp_db)
    assert store.count() == 0

    for i in range(5):
        store.record_video(
            video_id=f"vid-{i}",
            channel_id="chan-1",
            title=f"Video {i}",
            published_at="2026-09-18T10:00:00Z",
            has_transcript=True,
            pending_article_json=f'{{"id": "vid-{i}"}}',
        )

    assert store.count() == 5
    recent = store.get_recent(limit=3)
    assert len(recent) == 3


def test_state_operations(temp_db):
    store = CaptionsStore(temp_db)
    assert store.get_state("last_poll_status") is None

    store.set_state("last_poll_status", "ok")
    assert store.get_state("last_poll_status") == "ok"

    store.set_state("last_poll_status", "error: something broke")
    assert store.get_state("last_poll_status") == "error: something broke"


def test_set_states_rolls_back_related_service_state_on_failure(temp_db):
    store = CaptionsStore(temp_db)
    store.connection.executescript(
        """
        CREATE TRIGGER reject_transcript_cooldown
        BEFORE INSERT ON service_state
        WHEN NEW.key = 'youtube_transcript_cooldown_until'
        BEGIN
            SELECT RAISE(ABORT, 'forced cooldown write failure');
        END;
        """
    )

    with pytest.raises(sqlite3.IntegrityError, match="forced cooldown write failure"):
        store.set_states(
            {
                "youtube_transcript_block_streak": "1",
                "youtube_transcript_cooldown_until": "2026-09-29T10:30:00+00:00",
                "last_poll_status": "paused: YouTube transcript requests blocked",
            }
        )

    assert store.get_state("youtube_transcript_block_streak") is None
    assert store.get_state("youtube_transcript_cooldown_until") is None
    assert store.get_state("last_poll_status") is None


def test_mark_indexed(temp_db):
    store = CaptionsStore(temp_db)
    store.record_video(
        video_id="vid-1",
        channel_id="chan-1",
        title="Sample Video",
        published_at="2026-09-18T10:00:00Z",
        has_transcript=True,
        pending_article_json='{"id": "vid-1"}',
    )
    rec = store.get_video("vid-1")
    assert rec["indexed_at"] is None

    store.mark_indexed("vid-1", indexed_at="2026-09-18T10:05:00Z")
    rec_after = store.get_video("vid-1")
    assert rec_after["indexed_at"] == "2026-09-18T10:05:00Z"


def test_undelivered_with_transcript(temp_db):
    store = CaptionsStore(temp_db)

    # 1. Video with transcript, not indexed
    store.record_video(
        video_id="vid-1",
        channel_id="chan-1",
        title="Video 1",
        published_at="2026-09-18T10:00:00Z",
        has_transcript=True,
        pending_article_json='{"id": "vid-1"}',
    )
    # 2. Video without transcript
    store.record_video(
        video_id="vid-2",
        channel_id="chan-1",
        title="Video 2 (no transcript)",
        published_at="2026-09-18T10:01:00Z",
        has_transcript=False,
        pending_article_json=None,
    )
    # 3. Video with transcript, already indexed
    store.record_video(
        video_id="vid-3",
        channel_id="chan-1",
        title="Video 3",
        published_at="2026-09-18T10:02:00Z",
        has_transcript=True,
        pending_article_json='{"id": "vid-3"}',
        indexed_at="2026-09-18T10:05:00Z",
    )

    undelivered = store.undelivered_with_transcript(limit=25)
    assert len(undelivered) == 2
    assert {row["video_id"] for row in undelivered} == {"vid-1", "vid-3"}
    assert undelivered[0]["has_transcript"] is True
    assert {row["pending_article_json"] for row in undelivered} == {'{"id": "vid-1"}', '{"id": "vid-3"}'}
    assert all(row["indexed_at"] in {None, "2026-09-18T10:05:00Z"} for row in undelivered)

    store.connection.execute("UPDATE processed_videos SET review_state = 'revision_needed' WHERE video_id = 'vid-1'")
    assert [row["video_id"] for row in store.undelivered_with_transcript()] == ["vid-3"]

    # Mark vid-1 as indexed
    store.mark_indexed("vid-1")
    assert [row["video_id"] for row in store.undelivered_with_transcript()] == ["vid-3"]
