"""Tests for the channel backfill orchestrator."""
import functools
from types import SimpleNamespace

from app.backfill import Backfill
from app.backfill_store import BackfillStore
from app.channel_videos import VideoMeta, VideoRef
from app.config import Settings
from app.mycelium_pg import ALREADY_PRESENT, STORED, article_uuid
from app.sponsor import clean_transcript
from app.youtube_client import TranscriptBlocked

LONG_TRANSCRIPT = [
    ("word alpha beta gamma delta epsilon zeta eta", float(i * 5), 5.0) for i in range(10)
]


class FakePg:
    def __init__(self, outcome=STORED, should_raise=False):
        self.outcome, self.should_raise, self.calls = outcome, should_raise, []

    def insert(self, envelope, metadata):
        if self.should_raise:
            raise RuntimeError("pg down")
        self.calls.append((envelope, metadata))
        return self.outcome


class FakeIndexer:
    def __init__(self, ok=True, output="ok"):
        self.ok, self.output, self.calls = ok, output, []

    def __call__(self, envelope, **kw):
        self.calls.append((envelope, kw))
        return (self.ok, self.output)


def fake_clean(snips, vid):
    return SimpleNamespace(
        text=" ".join(s.text for s in snips), removed_seconds=0.0, removed_snippets=0, sources=[]
    )


def make_backfill(
    tmp_path, *, pg=None, indexer=None, list_videos=None, fetch_meta=None,
    fetch_transcript=None, clean=fake_clean, sleep=lambda s: None, clock=lambda: 0.0, delay=(0, 0),
):
    store = BackfillStore(str(tmp_path / "b.sqlite3"))
    pg = pg or FakePg()
    indexer = indexer or FakeIndexer()
    b = Backfill(
        Settings(), store, pg,
        list_videos=list_videos or (lambda h: [VideoRef("vid00000001", "Title A", 900)]),
        fetch_meta=fetch_meta or (
            lambda v: VideoMeta(v, "Title", "2026-09-15T00:00:00Z", 900, "UCx", "WarFronts", "", "")
        ),
        fetch_transcript=fetch_transcript or (lambda v: list(LONG_TRANSCRIPT)),
        clean=clean, index=indexer, sleep=sleep, clock=clock, delay=delay,
    )
    return b, store, pg, indexer


def test_happy_path(tmp_path):
    b, store, pg, indexer = make_backfill(tmp_path)
    summary = b.run(["@x"])
    row = store.get("vid00000001")
    assert summary.done == 1 and row["status"] == "done"
    assert row["persisted_at"] is not None and row["indexed_at"] is not None
    expected_id = article_uuid("vid00000001")
    assert pg.calls[0][0]["id"] == indexer.calls[0][0]["id"] == expected_id
    assert pg.calls[0][0]["source_agency"] == "WarFronts"
    assert pg.calls[0][0]["published_at"] == "2026-09-15T00:00:00Z"
    metadata = pg.calls[0][1]
    assert metadata["video_id"] == "vid00000001" and "ads_removed_seconds" in metadata


def test_ad_text_removed(tmp_path):
    raw = [
        ("before ad story headline update text line " * 8, 0.0, 10.0),
        ("premium subscriber site special discount visit now", 10.0, 10.0),
        ("after ad story continuation analysis words " * 8, 20.0, 10.0),
    ]
    clean = functools.partial(clean_transcript, fetch=lambda v: [(10.0, 20.0)], use_heuristic=False)
    b, _, pg, _ = make_backfill(tmp_path, fetch_transcript=lambda v: raw, clean=clean)
    summary = b.run(["@x"])
    content = pg.calls[0][0]["raw_content"]
    assert "premium subscriber site" not in content
    assert "before ad story headline" in content and "after ad story continuation" in content
    assert summary.ads_removed_seconds > 0


def test_no_transcript_and_short_transcript(tmp_path):
    videos = [VideoRef("v1", "T1", 900), VideoRef("v2", "T2", 900)]
    transcripts = {"v1": None, "v2": [("too short", 0.0, 1.0)]}
    b, store, pg, indexer = make_backfill(
        tmp_path, list_videos=lambda h: videos, fetch_transcript=lambda v: transcripts[v]
    )
    summary = b.run(["@x"])
    assert summary.no_transcript == 2
    assert store.get("v1")["status"] == "no_transcript"
    assert store.get("v2")["status"] == "no_transcript"
    assert len(pg.calls) == 0 and len(indexer.calls) == 0


def test_throttled_backoff_then_success(tmp_path):
    waits, attempts = [], 0

    def throttled_transcript(v):
        nonlocal attempts
        attempts += 1
        if attempts <= 2:
            raise TranscriptBlocked("rate limited")
        return list(LONG_TRANSCRIPT)

    b, store, _, _ = make_backfill(
        tmp_path, fetch_transcript=throttled_transcript, sleep=waits.append, delay=(0, 0)
    )
    b.run(["@x"])
    assert waits[:2] == [300, 900]
    assert store.get("vid00000001")["status"] == "done"


def test_persistent_block(tmp_path):
    def blocked(v):
        raise TranscriptBlocked("blocked")

    videos = [VideoRef("v1", "T1", 900), VideoRef("v2", "T2", 900)]
    b, store, _, _ = make_backfill(tmp_path, list_videos=lambda h: videos, fetch_transcript=blocked)
    summary = b.run(["@x"])
    assert summary.aborted != ""
    assert store.get("v1")["status"] == "pending" and store.get("v2")["status"] == "pending"
    assert summary.processed == 1


def test_fetch_meta_exception_error_and_retry(tmp_path):
    fail_meta, videos = True, [VideoRef("v1", "T1", 900), VideoRef("v2", "T2", 900)]

    def meta(v):
        if v == "v1" and fail_meta:
            raise RuntimeError("meta failed")
        return VideoMeta(v, "T", "2026-09-15T00:00:00Z", 900, "UCx", "WarFronts", "", "")

    b, store, _, _ = make_backfill(tmp_path, list_videos=lambda h: videos, fetch_meta=meta)
    b.run(["@x"])
    assert store.get("v1")["status"] == "error" and store.get("v1")["attempts"] == 1
    assert store.get("v2")["status"] == "done"

    fail_meta = False
    b.run(["@x"], discover=False)
    assert store.get("v1")["status"] == "done"


def test_postgres_failure_and_retry(tmp_path):
    pg = FakePg(should_raise=True)
    b, store, _, _ = make_backfill(tmp_path, pg=pg)
    summary1 = b.run(["@x"])
    assert store.get("vid00000001")["status"] == "done"
    assert store.get("vid00000001")["persisted_at"] is None
    assert summary1.delivery_failures == 1 and store.stats()["pending_postgres"] == 1

    pg.should_raise = False
    b.run(["@x"], discover=False)
    assert store.stats()["pending_postgres"] == 0
    assert store.get("vid00000001")["persisted_at"] is not None


def test_indexer_failure_and_retry(tmp_path):
    indexer = FakeIndexer(ok=False, output="boom")
    b, store, _, _ = make_backfill(tmp_path, indexer=indexer)
    b.run(["@x"])
    assert store.get("vid00000001")["indexed_at"] is None

    indexer.ok = True
    b.run(["@x"], discover=False)
    assert store.get("vid00000001")["indexed_at"] is not None


def test_live_video_skipped(tmp_path):
    live_meta = lambda v: VideoMeta(v, "T", "2026-09-15T00:00:00Z", 900, "UCx", "WarFronts", "", "is_live")
    b, store, pg, indexer = make_backfill(tmp_path, fetch_meta=live_meta)
    summary = b.run(["@x"])
    assert summary.skipped_live == 1 and store.get("vid00000001")["status"] == "pending"
    assert len(pg.calls) == 0 and len(indexer.calls) == 0


def test_resume_does_nothing(tmp_path):
    calls = []
    b, _, _, _ = make_backfill(
        tmp_path, fetch_transcript=lambda v: (calls.append(v) or list(LONG_TRANSCRIPT))
    )
    b.run(["@x"])
    assert len(calls) == 1
    summary = b.run(["@x"])
    assert summary.processed == 0 and len(calls) == 1


def test_discover_deduplicates_and_handles_exception(tmp_path):
    videos = [VideoRef("v1", "T1", 900), VideoRef("v2", "T2", 900)]
    b, store, _, _ = make_backfill(tmp_path, list_videos=lambda h: videos)
    assert b.run(["@x"]).discovered == 2 and store.stats()["total"] == 2
    assert b.run(["@x"]).discovered == 2 and store.stats()["total"] == 2

    def failing_list(h):
        raise RuntimeError("fail")

    b._list_videos = failing_list
    assert b.run(["@bad"]).discovered == 0


def test_max_hours_limit(tmp_path):
    videos, current = [VideoRef(f"v{i}", f"T{i}", 900) for i in range(5)], [0.0]

    def clock():
        val = current[0]
        current[0] += 3600.0
        return val

    b, _, _, _ = make_backfill(tmp_path, list_videos=lambda h: videos, clock=clock)
    summary = b.run(["@x"], max_hours=1.5)
    assert "1.5h" in summary.aborted and summary.processed < 5


def test_already_present_pg(tmp_path):
    pg = FakePg(outcome=ALREADY_PRESENT)
    b, store, _, _ = make_backfill(tmp_path, pg=pg)
    summary = b.run(["@x"])
    assert summary.already_present == 1 and summary.stored == 0
    assert store.get("vid00000001")["persisted_at"] is not None
