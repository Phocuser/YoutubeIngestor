import json
import os
import tempfile
from unittest.mock import MagicMock
import pytest

from app.config import Settings
from app.service import CaptionsService
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


@pytest.fixture
def service_and_store(temp_db):
    settings = Settings(
        youtube_channel_ids=["channel_a", "channel_b"],
        database_path=temp_db,
    )
    store = CaptionsStore(temp_db)
    client = MagicMock()
    service = CaptionsService(settings, store, client)
    return service, store, client


@pytest.mark.asyncio
async def test_poll_once_dedupes_across_two_calls(service_and_store):
    """poll_once() dedupes correctly across two calls."""
    service, store, client = service_and_store

    channel_feed = [
        {
            "video_id": "vid-1",
            "title": "Video 1",
            "published_at": "2026-09-18T12:00:00Z",
            "channel_name": "Channel Alpha",
            "channel_id": "channel_a",
        },
        {
            "video_id": "vid-2",
            "title": "Video 2",
            "published_at": "2026-09-18T12:05:00Z",
            "channel_name": "Channel Alpha",
            "channel_id": "channel_a",
        },
    ]

    def mock_fetch_feed(channel_id):
        if channel_id == "channel_a":
            return channel_feed
        return []

    client.fetch_channel_feed.side_effect = mock_fetch_feed
    client.fetch_transcript.return_value = "This is the transcript content."

    # First poll: processes both videos
    processed_1 = await service.poll_once()
    assert len(processed_1) == 2
    assert store.is_processed("vid-1")
    assert store.is_processed("vid-2")
    assert client.fetch_transcript.call_count == 2

    # Second poll with the same feed: skips both videos
    processed_2 = await service.poll_once()
    assert len(processed_2) == 0
    # fetch_transcript should NOT have been called again
    assert client.fetch_transcript.call_count == 2


@pytest.mark.asyncio
async def test_poll_once_continues_to_next_channel_after_failure(service_and_store):
    """poll_once() continues to the next channel after one channel's feed fetch raises."""
    service, store, client = service_and_store

    def mock_fetch_feed(channel_id):
        if channel_id == "channel_a":
            raise RuntimeError("Feed fetch failed for channel_a")
        if channel_id == "channel_b":
            return [
                {
                    "video_id": "vid-b1",
                    "title": "Channel B Video",
                    "published_at": "2026-09-18T12:10:00Z",
                    "channel_name": "Channel Beta",
                    "channel_id": "channel_b",
                }
            ]
        return []

    client.fetch_channel_feed.side_effect = mock_fetch_feed
    client.fetch_transcript.return_value = "Beta transcript text."

    processed = await service.poll_once()

    # Channel A failed, but Channel B succeeded
    assert len(processed) == 1
    assert processed[0]["video_id"] == "vid-b1"
    assert store.is_processed("vid-b1")
    assert service.last_error == "Feed fetch failed for channel_a"
    assert "error:" in (store.get_state("last_poll_status") or "")


@pytest.mark.asyncio
async def test_videos_without_transcripts_recorded_and_not_retried(service_and_store):
    """videos without transcripts still get recorded (not retried forever)."""
    service, store, client = service_and_store

    client.fetch_channel_feed.return_value = [
        {
            "video_id": "no-sub-vid",
            "title": "Silent Video",
            "published_at": "2026-09-18T12:00:00Z",
            "channel_name": "Channel Alpha",
            "channel_id": "channel_a",
        }
    ]
    # Simulate video having no transcripts
    client.fetch_transcript.return_value = None

    # First poll
    processed = await service.poll_once()
    assert len(processed) == 1
    assert processed[0]["video_id"] == "no-sub-vid"
    assert processed[0]["has_transcript"] is False

    # Check store: recorded with has_transcript=False and no pending_article_json
    rec = store.get_video("no-sub-vid")
    assert rec is not None
    assert rec["has_transcript"] is False
    assert rec["pending_article_json"] is None
    assert store.is_processed("no-sub-vid") is True

    # Second poll: should NOT call fetch_transcript again because video was recorded
    client.fetch_transcript.reset_mock()
    processed_2 = await service.poll_once()
    assert len(processed_2) == 0
    client.fetch_transcript.assert_not_called()


@pytest.mark.asyncio
async def test_article_json_normalization_shape(service_and_store):
    """Verify stored Article JSON matches mycelium pipeline.go Article struct."""
    service, store, client = service_and_store

    client.fetch_channel_feed.return_value = [
        {
            "video_id": "vid-norm",
            "title": "Normalizing Captions into Articles",
            "published_at": "2026-09-18T12:30:00Z",
            "channel_name": "Channel Alpha",
            "channel_id": "channel_a",
        }
    ]
    client.fetch_transcript.return_value = "Full transcript text for pipeline extraction."

    await service.poll_once()

    rec = store.get_video("vid-norm")
    assert rec is not None
    assert rec["has_transcript"] is True
    assert rec["pending_article_json"] is not None

    article = json.loads(rec["pending_article_json"])
    # Target Article JSON shape confirmed from mycelium/cmd/indexer/pipeline.go:
    # {"id": "<youtube video id>", "name": "<video title>", "source_agency": "<channel name or id>", "published_at": "...", "raw_content": "..."}
    assert article["id"] == "vid-norm"
    assert article["name"] == "Normalizing Captions into Articles"
    assert article["source_agency"] == "Channel Alpha"
    assert article["published_at"] == "2026-09-18T12:30:00Z"
    assert article["raw_content"] == "Full transcript text for pipeline extraction."


@pytest.mark.asyncio
async def test_genuine_transcript_error_does_not_mark_as_processed(service_and_store):
    """Verify genuine network/API error on transcript fetch does not record video, allowing retry."""
    service, store, client = service_and_store

    client.fetch_channel_feed.return_value = [
        {
            "video_id": "vid-retry",
            "title": "Retry Video",
            "published_at": "2026-09-18T12:00:00Z",
            "channel_name": "Channel Alpha",
            "channel_id": "channel_a",
        }
    ]
    # Simulate network failure during transcript fetch
    client.fetch_transcript.side_effect = ConnectionError("Network down")

    processed = await service.poll_once()
    assert len(processed) == 0
    # Video must NOT be recorded as processed so it can be retried later
    assert not store.is_processed("vid-retry")
