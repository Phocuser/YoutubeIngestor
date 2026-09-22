import asyncio
import json
import os
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from app.config import Settings
from app.service import CaptionsService
from app.store import CaptionsStore


TIMED_CAPTIONS = [("Opening analysis", 0.0, 2.0), ("The raw caption track is retained.", 2.0, 3.0)]


class ReceiptSubmitter:
    """Test-only durable boundary; production uses MyceliumPg.submit()."""
    def __init__(self, state="queued"):
        self.state, self.calls = state, []

    def submit(self, article, metadata):
        self.calls.append((article, metadata))
        return SimpleNamespace(state=self.state)


class TestYoutubeClient:
    """Small synchronous double safe to pass through asyncio.to_thread."""
    def __init__(self):
        self.fetch_channel_feed_calls = []
        self.fetch_channel_feed_return_value = None
        self.fetch_channel_feed_side_effect = None
        self.fetch_timed_transcript_calls = []
        self.fetch_timed_transcript_return_value = None
        self.fetch_timed_transcript_side_effect = None
        self.fetch_transcript_calls = []

    @staticmethod
    def _result(calls, args, return_value, side_effect):
        calls.append(args)
        if side_effect is not None:
            if isinstance(side_effect, BaseException):
                raise side_effect
            return side_effect(*args)
        return return_value

    def fetch_channel_feed(self, channel):
        return self._result(self.fetch_channel_feed_calls, (channel,),
                            self.fetch_channel_feed_return_value,
                            self.fetch_channel_feed_side_effect)

    def fetch_timed_transcript(self, video_id):
        return self._result(self.fetch_timed_transcript_calls, (video_id,),
                            self.fetch_timed_transcript_return_value,
                            self.fetch_timed_transcript_side_effect)

    def fetch_transcript(self, video_id):
        self.fetch_transcript_calls.append((video_id,))
        return None


@pytest.fixture(autouse=True)
def inline_to_thread(monkeypatch):
    async def _inline(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", _inline)


@pytest.fixture
def service_and_store(tmp_path):
    db = str(tmp_path / "captions.sqlite3")
    settings = Settings(youtube_channel_ids=["channel_a", "channel_b"], database_path=db)
    store, client, submitter = CaptionsStore(db), TestYoutubeClient(), ReceiptSubmitter()
    return CaptionsService(settings, store, client, submitter), store, client, submitter


def _video(video_id="vid-1", title="Video 1"):
    return {"video_id": video_id, "title": title, "published_at": "2026-09-18T12:00:00Z",
            "channel_name": "Channel Alpha", "channel_id": "channel_a"}


@pytest.mark.asyncio
async def test_poll_uses_timed_captions_and_durable_receipt(service_and_store):
    service, store, client, submitter = service_and_store
    client.fetch_channel_feed_side_effect = lambda channel: [_video()] if channel == "channel_a" else []
    client.fetch_timed_transcript_return_value = list(TIMED_CAPTIONS)

    assert [x["video_id"] for x in await service.poll_once()] == ["vid-1"]
    assert client.fetch_timed_transcript_calls == [("vid-1",)]
    assert client.fetch_transcript_calls == []
    assert len(submitter.calls) == 1
    row = store.get_video("vid-1")
    assert row["persisted_at"] is not None
    article = json.loads(row["pending_article_json"])
    assert article["raw_content"] == "Opening analysis The raw caption track is retained."
    assert article["caption_track"]["segments"][1]["text"] == "The raw caption track is retained."
    assert article["cleaning"] == {"policy_version": "ads-v1", "decisions": [], "ranges": [], "sources": []}


@pytest.mark.asyncio
async def test_dedupes_after_durable_persistence(service_and_store):
    service, store, client, submitter = service_and_store
    client.fetch_channel_feed_return_value = [_video()]
    client.fetch_timed_transcript_return_value = list(TIMED_CAPTIONS)
    assert len(await service.poll_once()) == 1
    assert len(await service.poll_once()) == 0
    assert len(client.fetch_timed_transcript_calls) == 1 and len(submitter.calls) == 1
    assert store.get_video("vid-1")["persisted_at"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("captions", [None, "not-a-timed-track", []])
async def test_missing_or_malformed_timed_captions_remain_retryable(service_and_store, captions):
    service, store, client, submitter = service_and_store
    client.fetch_channel_feed_return_value = [_video("retry-caption")]
    client.fetch_timed_transcript_return_value = captions
    assert await service.poll_once() == []
    assert not store.is_processed("retry-caption") and submitter.calls == []


@pytest.mark.asyncio
async def test_failed_timed_caption_fetch_remains_retryable(service_and_store):
    service, store, client, submitter = service_and_store
    client.fetch_channel_feed_return_value = [_video("failed-caption")]
    client.fetch_timed_transcript_side_effect = ConnectionError("caption API unavailable")
    assert await service.poll_once() == []
    assert not store.is_processed("failed-caption") and submitter.calls == []


@pytest.mark.asyncio
async def test_receipt_failure_leaves_persisted_outbox_retryable(service_and_store):
    service, store, client, submitter = service_and_store
    submitter.state = "rejected"
    client.fetch_channel_feed_return_value = [_video("delivery-retry")]
    client.fetch_timed_transcript_return_value = list(TIMED_CAPTIONS)
    assert len(await service.poll_once()) == 1
    row = store.get_video("delivery-retry")
    assert row["pending_article_json"] is not None and row["persisted_at"] is None
    assert [x["video_id"] for x in store.undelivered_with_transcript()] == ["delivery-retry"]


@pytest.mark.asyncio
async def test_retry_sweep_uses_receipt_and_marks_persisted(service_and_store):
    service, store, client, submitter = service_and_store
    store.record_video("pending", "channel_a", "Pending", "2026-09-18T10:00:00Z", True,
                       json.dumps({"id": "pending", "raw_content": "caption text", "caption_track": {"segments": []}}))
    client.fetch_channel_feed_return_value = []
    await service.poll_once()
    assert store.get_video("pending")["persisted_at"] is not None
    assert len(store.undelivered_with_transcript()) == 0 and len(submitter.calls) == 1


@pytest.mark.asyncio
async def test_no_indexer_or_redis_fallback_including_retry_sweep(service_and_store):
    service, store, client, submitter = service_and_store
    client.fetch_channel_feed_return_value = [_video("no-legacy")]
    client.fetch_timed_transcript_return_value = list(TIMED_CAPTIONS)
    with patch("app.indexer_client.subprocess.run") as run_indexer:
        await service.poll_once()
        assert run_indexer.call_count == 0
    assert store.get_video("no-legacy")["persisted_at"] is not None and submitter.calls


@pytest.mark.asyncio
async def test_retry_sweep_failure_does_not_block_new_timed_caption_poll(service_and_store):
    service, store, client, submitter = service_and_store
    client.fetch_channel_feed_return_value = [_video("new-video")]
    client.fetch_timed_transcript_return_value = list(TIMED_CAPTIONS)
    with patch.object(service, "retry_undelivered_articles", new_callable=AsyncMock,
                      side_effect=RuntimeError("sweep failed")):
        processed = await service.poll_once()
    assert [x["video_id"] for x in processed] == ["new-video"]
    assert store.get_video("new-video")["persisted_at"] is not None and submitter.calls
