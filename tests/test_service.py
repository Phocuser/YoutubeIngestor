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


class LinkedReceiptSubmitter(ReceiptSubmitter):
    def submit(self, article, metadata):
        self.calls.append((article, metadata))
        number = len(self.calls)
        return SimpleNamespace(
            state=self.state,
            receipt_id=f"receipt-{number}",
            job_id=f"job-{number}",
            submission_id=f"submission-{number}",
        )


class TestYoutubeClient:
    """Small synchronous double safe to pass through asyncio.to_thread."""
    __test__ = False

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
    assert submitter.calls[0][1]["access_scope"] == "public"
    row = store.get_video("vid-1")
    assert row["persisted_at"] is not None
    article = json.loads(row["pending_article_json"])
    assert article["raw_content"] == "Opening analysis The raw caption track is retained."
    assert article["caption_track"]["segments"][1]["text"] == "The raw caption track is retained."
    assert article["cleaning"]["policy_version"] == "ads-v1"
    assert article["cleaning"]["derived_from"] == "caption_track"
    assert len(article["cleaning"]["decisions"]) == 2


@pytest.mark.asyncio
async def test_dedupes_after_durable_persistence(service_and_store):
    service, store, client, submitter = service_and_store
    client.fetch_channel_feed_return_value = [_video()]
    client.fetch_timed_transcript_return_value = list(TIMED_CAPTIONS)
    assert len(await service.poll_once()) == 1
    assert len(await service.poll_once()) == 0
    # Existing videos are fetched again so timing/track-only revisions cannot
    # be hidden by local prose dedupe; the typed digest keeps the replay local.
    assert len(client.fetch_timed_transcript_calls) == 2 and len(submitter.calls) == 1
    assert store.get_video("vid-1")["persisted_at"] is not None


@pytest.mark.asyncio
async def test_timing_revision_quarantine_is_idempotent_and_newer_revision_reopens(service_and_store):
    service, store, client, _ = service_and_store
    submitter = LinkedReceiptSubmitter()
    service.submitter = submitter
    client.fetch_channel_feed_return_value = [_video("revision-video")]

    first = list(TIMED_CAPTIONS)
    changed = [("Opening analysis", 0.0, 2.0), ("The raw caption track is retained.", 4.0, 3.0)]
    newer = [("Opening analysis", 0.0, 2.0), ("The raw caption track is retained.", 5.0, 3.0)]

    submitter.state = "queued"
    client.fetch_timed_transcript_return_value = first
    assert len(await service.poll_once()) == 1
    assert len(submitter.calls) == 1
    initial = store.get_video("revision-video")
    assert initial["admission_state"] == "queued"
    assert initial["admission_receipt_id"] == "receipt-1"
    assert initial["admission_job_id"] == "job-1"
    assert initial["admission_submission_id"] == "submission-1"
    assert initial["materialization_state"] == "unknown"
    assert initial["indexed_at"] is None

    submitter.state = "quarantined"
    client.fetch_timed_transcript_return_value = changed
    assert len(await service.poll_once()) == 1
    assert len(submitter.calls) == 2
    quarantined = store.get_video("revision-video")
    assert quarantined["review_state"] == "revision_needed"
    assert quarantined["admission_state"] == "quarantined"
    assert quarantined["admission_receipt_id"] == "receipt-2"
    assert quarantined["materialization_state"] == "unknown"
    assert quarantined["indexed_at"] is None

    # The same quarantined bytes remain durable evidence and must not be
    # submitted repeatedly while awaiting explicit revision handling.
    assert await service.poll_once() == []
    assert len(submitter.calls) == 2

    client.fetch_timed_transcript_return_value = newer
    assert len(await service.poll_once()) == 1
    assert len(submitter.calls) == 3
    reopened = store.get_video("revision-video")
    assert reopened["admission_state"] == "quarantined"
    assert reopened["admission_receipt_id"] == "receipt-3"
    assert reopened["materialization_state"] == "unknown"
    assert reopened["indexed_at"] is None


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
async def test_retryable_caption_gap_can_resolve_without_quarantine(service_and_store):
    service, store, client, submitter = service_and_store
    client.fetch_channel_feed_return_value = [_video("gap-resolves")]
    client.fetch_timed_transcript_return_value = None
    assert await service.poll_once() == []
    assert store.get_video("gap-resolves")["review_state"] == "retryable"
    client.fetch_timed_transcript_return_value = list(TIMED_CAPTIONS)
    assert len(await service.poll_once()) == 1
    assert store.get_video("gap-resolves")["review_state"] == "ready"
    assert store.get_video("gap-resolves")["persisted_at"] is not None


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
async def test_direct_durable_submission_normalizes_youtube_alias(service_and_store):
    service, store, client, submitter = service_and_store
    store.record_video(
        "alias-video",
        "channel_a",
        "Alias video",
        "2026-09-18T10:00:00Z",
        True,
        json.dumps({"id": "youtube:alias-video", "raw_content": "caption text"}),
    )

    assert await service.submit_durable({"id": "youtube:alias-video"})
    assert store.get_video("alias-video")["persisted_at"] is not None
    assert len(submitter.calls) == 1


@pytest.mark.asyncio
async def test_no_indexer_or_redis_fallback_including_retry_sweep(service_and_store):
    service, store, client, submitter = service_and_store
    client.fetch_channel_feed_return_value = [_video("no-legacy")]
    client.fetch_timed_transcript_return_value = list(TIMED_CAPTIONS)
    await service.poll_once()
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
