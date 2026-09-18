import json
import os
import subprocess
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch
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


@pytest.fixture(autouse=True)
def default_mock_subprocess():
    with patch("app.indexer_client.subprocess.run") as mock_run:
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "indexed successfully"
        mock_proc.stderr = ""
        mock_run.return_value = mock_proc
        yield mock_run


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


@pytest.mark.asyncio
async def test_poll_once_successful_indexing_marks_indexed(
    service_and_store, default_mock_subprocess
):
    """Successful run of indexer marks the video indexed."""
    service, store, client = service_and_store

    client.fetch_channel_feed.return_value = [
        {
            "video_id": "vid-indexed",
            "title": "Indexed Video",
            "published_at": "2026-09-18T12:00:00Z",
            "channel_name": "Channel Alpha",
            "channel_id": "channel_a",
        }
    ]
    client.fetch_transcript.return_value = "Transcript for indexing."
    default_mock_subprocess.return_value.returncode = 0
    default_mock_subprocess.return_value.stdout = "candidates: 5"

    processed = await service.poll_once()
    assert len(processed) == 1
    assert store.is_processed("vid-indexed")

    rec = store.get_video("vid-indexed")
    assert rec["has_transcript"] is True
    assert rec["indexed_at"] is not None
    assert len(store.undelivered_with_transcript()) == 0
    assert default_mock_subprocess.call_count == 1


@pytest.mark.asyncio
async def test_poll_once_indexer_nonzero_exit_leaves_undelivered(
    service_and_store, default_mock_subprocess
):
    """Non-zero exit of indexer leaves video undelivered for retry."""
    service, store, client = service_and_store

    client.fetch_channel_feed.return_value = [
        {
            "video_id": "vid-fail",
            "title": "Failed Video",
            "published_at": "2026-09-18T12:00:00Z",
            "channel_name": "Channel Alpha",
            "channel_id": "channel_a",
        }
    ]
    client.fetch_transcript.return_value = "Transcript for failed indexing."
    default_mock_subprocess.return_value.returncode = 1
    default_mock_subprocess.return_value.stderr = "indexer crash"

    processed = await service.poll_once()
    assert len(processed) == 1
    assert store.is_processed("vid-fail")

    rec = store.get_video("vid-fail")
    assert rec["has_transcript"] is True
    assert rec["indexed_at"] is None

    undelivered = store.undelivered_with_transcript()
    assert len(undelivered) == 1
    assert undelivered[0]["video_id"] == "vid-fail"


@pytest.mark.asyncio
async def test_poll_once_indexer_filenotfound_or_timeout_handled_gracefully(
    service_and_store, default_mock_subprocess
):
    """FileNotFoundError and TimeoutExpired are handled gracefully without raising, leaving undelivered."""
    service, store, client = service_and_store

    client.fetch_channel_feed.return_value = [
        {
            "video_id": "vid-missing-bin",
            "title": "Missing Bin Video",
            "published_at": "2026-09-18T12:00:00Z",
            "channel_name": "Channel Alpha",
            "channel_id": "channel_a",
        }
    ]
    client.fetch_transcript.return_value = "Some transcript."
    default_mock_subprocess.side_effect = FileNotFoundError(
        "No such file or directory: bin/indexer"
    )

    # Should not raise
    processed = await service.poll_once()
    assert len(processed) == 1
    assert store.is_processed("vid-missing-bin")
    assert store.get_video("vid-missing-bin")["indexed_at"] is None

    # Test TimeoutExpired
    client.fetch_channel_feed.return_value = [
        {
            "video_id": "vid-timeout",
            "title": "Timeout Video",
            "published_at": "2026-09-18T12:01:00Z",
            "channel_name": "Channel Alpha",
            "channel_id": "channel_a",
        }
    ]
    default_mock_subprocess.side_effect = subprocess.TimeoutExpired(
        cmd=["indexer"], timeout=30.0
    )

    processed_2 = await service.poll_once()
    assert len(processed_2) == 1
    assert store.is_processed("vid-timeout")
    assert store.get_video("vid-timeout")["indexed_at"] is None


@pytest.mark.asyncio
async def test_retry_sweep_reattempts_and_marks_delivered_on_success(
    service_and_store, default_mock_subprocess
):
    """Retry sweep re-attempts undelivered videos at start of poll_once and marks them delivered on success."""
    service, store, client = service_and_store

    # Seed an undelivered video
    store.record_video(
        video_id="vid-pending",
        channel_id="channel_a",
        title="Pending Video",
        published_at="2026-09-18T10:00:00Z",
        has_transcript=True,
        pending_article_json=json.dumps(
            {
                "id": "vid-pending",
                "name": "Pending Video",
                "source_agency": "Channel Alpha",
                "published_at": "2026-09-18T10:00:00Z",
                "raw_content": "Transcript to retry.",
            }
        ),
    )
    assert len(store.undelivered_with_transcript()) == 1

    # Empty feed for this poll
    client.fetch_channel_feed.return_value = []
    default_mock_subprocess.return_value.returncode = 0
    default_mock_subprocess.return_value.stdout = "indexed"

    await service.poll_once()

    rec = store.get_video("vid-pending")
    assert rec["indexed_at"] is not None
    assert len(store.undelivered_with_transcript()) == 0
    assert default_mock_subprocess.call_count == 1


@pytest.mark.asyncio
async def test_retry_sweep_exception_does_not_block_new_polling(
    service_and_store, default_mock_subprocess
):
    """A retry sweep failure never blocks polling for new videos."""
    service, store, client = service_and_store

    client.fetch_channel_feed.return_value = [
        {
            "video_id": "vid-new",
            "title": "New Video",
            "published_at": "2026-09-18T12:00:00Z",
            "channel_name": "Channel Alpha",
            "channel_id": "channel_a",
        }
    ]
    client.fetch_transcript.return_value = "Transcript for new video."

    # Make retry_undelivered_articles raise an exception
    with patch.object(
        service, "retry_undelivered_articles", side_effect=RuntimeError("Sweep exploded")
    ):
        processed = await service.poll_once()

    # The new video was still polled and processed successfully
    assert len(processed) == 1
    assert processed[0]["video_id"] == "vid-new"
    assert store.is_processed("vid-new")
