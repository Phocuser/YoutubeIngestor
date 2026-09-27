"""Tests for the channel backfill orchestrator."""
import functools
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.backfill import AlreadyRunning, Backfill, main, single_instance
from app.backfill_store import BackfillStore
from app.channel_videos import VideoMeta, VideoRef
from app.config import Settings
from app.mycelium_pg import ALREADY_PRESENT, STORED, article_uuid
from app.sponsor import clean_transcript
from app.youtube_client import TranscriptBlocked

# Explicitly test-only timed-caption provider data; no live YouTube calls.
LONG_TRANSCRIPT = [
    ("word alpha beta gamma delta epsilon zeta eta", float(i * 5), 5.0) for i in range(10)
]


class FakePg:
    def __init__(self, outcome=STORED, should_raise=False):
        self.outcome, self.should_raise, self.calls = outcome, should_raise, []

    def submit(self, envelope, metadata):
        if self.should_raise:
            raise RuntimeError("pg down")
        self.calls.append((envelope, metadata))
        return SimpleNamespace(state="queued" if self.outcome != ALREADY_PRESENT else "rejected")


class FakeIndexer:
    def __init__(self, ok=True, output="ok"):
        self.ok, self.output, self.calls = ok, output, []

    def __call__(self, envelope, **kw):
        self.calls.append((envelope, kw))
        return (self.ok, self.output)


def fake_clean(snips, vid):
    return SimpleNamespace(
        text=" ".join(s.text for s in snips), removed_seconds=0.0, removed_snippets=0,
        sources=[], ranges=[], decisions=[], policy_version="ads-v1"
    )


def make_backfill(
    tmp_path, *, pg=None, indexer=None, list_videos=None, fetch_meta=None,
    fetch_transcript=None, clean=fake_clean, sleep=lambda s: None, clock=lambda: 0.0,
    wall=lambda: 0.0, delay=(0, 0),
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
        clean=clean, index=indexer, sleep=sleep, clock=clock, wall=wall, delay=delay,
    )
    return b, store, pg, indexer


def test_happy_path(tmp_path):
    b, store, pg, indexer = make_backfill(tmp_path)
    summary = b.run(["@x"])
    row = store.get("vid00000001")
    assert summary.done == 1 and row["status"] == "done"
    assert row["persisted_at"] is not None
    assert pg.calls[0][0]["id"] == "youtube:vid00000001"
    assert pg.calls[0][0]["source_agency"] == "WarFronts"
    assert pg.calls[0][0]["published_at"] == "2026-09-15T00:00:00Z"
    metadata = pg.calls[0][1]
    assert metadata["video_id"] == "vid00000001" and "ads_removed_seconds" in metadata
    assert metadata["access_scope"] == "public"


def test_ad_text_removed(tmp_path):
    raw = [
        ("before ad story headline update text line " * 8, 0.0, 10.0),
        ("premium subscriber site special discount visit now", 10.0, 10.0),
        ("after ad story continuation analysis words " * 8, 20.0, 10.0),
    ]
    clean = functools.partial(clean_transcript, fetch=lambda v: [(10.0, 20.0)], use_heuristic=False)
    b, _, pg, _ = make_backfill(tmp_path, fetch_transcript=lambda v: raw, clean=clean)
    summary = b.run(["@x"])
    content = pg.calls[0][0]["editorial_content"]
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
    assert summary.no_transcript == 1
    assert store.get("v1")["status"] == "no_transcript"
    assert store.get("v2")["status"] == "done"
    assert len(pg.calls) == 1 and len(indexer.calls) == 0


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
    pg = FakePg(should_raise=True)
    b, store, _, _ = make_backfill(tmp_path, pg=pg)
    b.run(["@x"])
    assert store.get("vid00000001")["persisted_at"] is None

    pg.should_raise = False
    b.run(["@x"], discover=False)
    assert store.get("vid00000001")["persisted_at"] is not None


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


def test_discovery_limit_is_passed_per_channel_and_enforced_across_channels(tmp_path):
    refs = {
        "@one": [VideoRef("v1", "T1", 900), VideoRef("v2", "T2", 900)],
        "@two": [VideoRef("v3", "T3", 900), VideoRef("v4", "T4", 900)],
    }
    calls = []

    def bounded_list(handle, *, limit=None):
        calls.append((handle, limit))
        return refs[handle]

    b, store, _, _ = make_backfill(tmp_path, list_videos=bounded_list)
    summary = b.run(["@one", "@two"], limit=3)

    assert calls == [("@one", 3), ("@two", 1)]
    assert summary.discovered == 3
    assert store.stats()["total"] == 3


def test_discovery_limit_defends_against_legacy_enumerator_ignoring_bound(tmp_path):
    refs = [VideoRef(f"v{i}", f"T{i}", 900) for i in range(5)]
    calls = []

    def unbounded_list(handle):
        calls.append(handle)
        return refs

    b, store, _, _ = make_backfill(tmp_path, list_videos=unbounded_list)
    summary = b.run(["@one", "@two"], limit=2)

    assert calls == ["@one"]
    assert summary.discovered == 2
    assert store.stats()["total"] == 2


def test_discovery_without_limit_preserves_legacy_enumerator_behavior(tmp_path):
    refs = [VideoRef("v1", "T1", 900), VideoRef("v2", "T2", 900)]
    calls = []

    def legacy_list(handle):
        calls.append(handle)
        return refs

    b, store, _, _ = make_backfill(tmp_path, list_videos=legacy_list)
    summary = b.run(["@one"], limit=0)

    assert calls == ["@one"]
    assert summary.discovered == 2
    assert store.stats()["total"] == 2


def test_backfill_days_filters_before_processing_and_fails_closed_on_unknown_dates(tmp_path):
    now = datetime(2026, 9, 27, tzinfo=timezone.utc).timestamp()
    refs = [
        VideoRef("recent", "Recent", 900, "2026-09-25T00:00:00Z"),
        VideoRef("old", "Old", 900, "2026-09-01T00:00:00Z"),
        VideoRef("unknown", "Unknown", 900),
    ]
    metadata_calls, transcript_calls = [], []

    def fetch_meta(video_id):
        metadata_calls.append(video_id)
        return VideoMeta(video_id, video_id, "2026-09-25T00:00:00Z", 900, "UCx", "WarFronts", "", "")

    b, store, _, _ = make_backfill(
        tmp_path,
        list_videos=lambda handle: refs,
        fetch_meta=fetch_meta,
        fetch_transcript=lambda video_id: transcript_calls.append(video_id) or list(LONG_TRANSCRIPT),
        wall=lambda: now,
    )
    summary = b.run(["@one"], limit=3, backfill_days=7)

    assert summary.discovered == 3
    assert summary.out_of_window == 1
    assert summary.unknown_dates == 1
    assert summary.incomplete is True
    assert summary.aborted == ""
    assert store.stats()["total"] == 1
    assert store.get("recent")["published_at"] == "2026-09-25T00:00:00Z"
    assert metadata_calls == ["recent"]
    assert transcript_calls == ["recent"]


def test_date_window_bounds_existing_rows_without_discovery(tmp_path):
    now = datetime(2026, 9, 27, tzinfo=timezone.utc).timestamp()
    metadata_calls, transcript_calls = [], []
    b, store, _, _ = make_backfill(
        tmp_path,
        list_videos=lambda handle: (_ for _ in ()).throw(AssertionError("discovery called")),
        fetch_meta=lambda video_id: (
            metadata_calls.append(video_id)
            or VideoMeta(video_id, video_id, "2026-09-25T00:00:00Z", 900, "UCx", "WarFronts", "", "")
        ),
        fetch_transcript=lambda video_id: transcript_calls.append(video_id) or list(LONG_TRANSCRIPT),
        wall=lambda: now,
    )
    store.register("recent", "@one", "Recent", 900, "2026-09-25T00:00:00Z")
    store.register("old", "@one", "Old", 900, "2026-09-01T00:00:00Z")
    store.register("unknown", "@one", "Unknown", 900)
    store.register("refresh", "@one", "Refresh", 900)
    assert store.register("refresh", "@one", "Refresh", 900, "2026-09-25T00:00:00Z") is False

    summary = b.run([], limit=3, discover=False, backfill_days=7)

    assert summary.out_of_window == 1
    assert summary.unknown_dates == 1
    assert summary.incomplete is True
    assert metadata_calls == ["recent", "refresh"]
    assert transcript_calls == ["recent", "refresh"]
    assert store.get("old")["status"] == "pending"
    assert store.get("unknown")["status"] == "pending"
    assert store.get("refresh")["published_at"] == "2026-09-25T00:00:00Z"


def test_date_window_does_not_retry_out_of_window_or_unknown_outbox_rows(tmp_path):
    now = datetime(2026, 9, 27, tzinfo=timezone.utc).timestamp()
    pg = FakePg()
    b, store, _, _ = make_backfill(tmp_path, pg=pg, wall=lambda: now)
    for video_id, published_at in (
        ("old", "2026-09-01T00:00:00Z"),
        ("unknown", None),
    ):
        store.register(video_id, "@one", video_id, 900, published_at)
        store.mark_done(video_id, published_at, "article", "{}", "{}")

    summary = b.run([], limit=1, discover=False, backfill_days=7)

    assert pg.calls == []
    assert summary.out_of_window == 1
    assert summary.unknown_dates == 1
    assert summary.incomplete is True
    assert store.get("old")["persisted_at"] is None
    assert store.get("unknown")["persisted_at"] is None


@pytest.mark.parametrize("backfill_days", [0, 91])
def test_backfill_days_bounds_fail_before_enumeration(tmp_path, backfill_days):
    calls = []
    b, _, _, _ = make_backfill(
        tmp_path,
        list_videos=lambda handle: calls.append(handle),
    )

    with pytest.raises(ValueError, match="1 through 90"):
        b.run(["@one"], limit=1, backfill_days=backfill_days)

    assert calls == []


def test_date_bounded_discovery_requires_a_positive_item_limit(tmp_path):
    calls = []
    b, _, _, _ = make_backfill(
        tmp_path,
        list_videos=lambda handle: calls.append(handle),
    )

    with pytest.raises(ValueError, match="requires a limit"):
        b.run(["@one"], backfill_days=7)

    assert calls == []


def test_item_limit_upper_bound_fails_before_enumeration_but_legacy_zero_is_unbounded(tmp_path):
    calls = []
    b, _, _, _ = make_backfill(
        tmp_path,
        list_videos=lambda handle: calls.append(handle) or [],
    )

    with pytest.raises(ValueError, match="1 through 1000"):
        b.run(["@one"], limit=1001)
    assert calls == []

    b.run(["@one"], limit=0)
    assert calls == ["@one"]


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
    assert summary.delivery_failures == 1 and summary.stored == 0
    assert store.get("vid00000001")["persisted_at"] is None


def test_persisted_cooldown_sleeps_before_fetch(tmp_path):
    events, now = [], [1000.0]
    b, store, _, _ = make_backfill(
        tmp_path,
        fetch_transcript=lambda v: events.append("fetch") or list(LONG_TRANSCRIPT),
        sleep=lambda seconds: events.append(("sleep", seconds)),
    )
    b._wall = lambda: now[0]
    store.set_state("blocked_until", "1100")
    b._transcript_with_backoff("vid00000001", b.run([]))
    assert events[0] == ("sleep", 100.0) and events[1] == "fetch"


def test_persisted_block_count_escalates_and_success_resets(tmp_path):
    store = BackfillStore(str(tmp_path / "b.sqlite3"))
    waits, now = [], [1000.0]
    first = iter([TranscriptBlocked("blocked"), RuntimeError("stop")])

    def first_fetch(video_id):
        result = next(first)
        if isinstance(result, Exception):
            raise result
        return result

    b1, _, _, _ = make_backfill(tmp_path, fetch_transcript=first_fetch, sleep=waits.append)
    b1._wall = lambda: now[0]
    with pytest.raises(RuntimeError):
        b1._transcript_with_backoff("v1", b1.run([]))
    now[0] = 1300.0
    calls = iter([TranscriptBlocked("blocked"), list(LONG_TRANSCRIPT)])

    def second_fetch(video_id):
        result = next(calls)
        if isinstance(result, Exception):
            raise result
        return result

    b2 = Backfill(
        Settings(), store, FakePg(), fetch_transcript=second_fetch,
        clean=fake_clean, sleep=waits.append, wall=lambda: now[0], index=FakeIndexer(),
    )
    b2._transcript_with_backoff("v1", b2.run([]))
    assert waits[-1] == 900 and store.get_state("block_count") == "0"


def test_cooldown_in_past_does_not_sleep(tmp_path):
    calls = []
    b, store, _, _ = make_backfill(
        tmp_path, fetch_transcript=lambda v: calls.append(v) or list(LONG_TRANSCRIPT),
        sleep=lambda seconds: calls.append(seconds),
    )
    b._wall = lambda: 1000.0
    store.set_state("blocked_until", "999")
    b._transcript_with_backoff("vid00000001", b.run([]))
    assert calls == ["vid00000001"]


def test_single_instance_releases_and_main_reports_running(tmp_path, capsys):
    lock = str(tmp_path / "nested" / "backfill.lock")
    with single_instance(lock):
        with pytest.raises(AlreadyRunning):
            with single_instance(lock):
                pass
    with single_instance(lock):
        pass

    database = str(tmp_path / "backfill.sqlite3")
    settings_factory = lambda: Settings(backfill_database_path=os.environ["BACKFILL_DATABASE_PATH"])
    with patch.dict(os.environ, {"BACKFILL_DATABASE_PATH": database}), patch(
        "app.backfill.Settings", side_effect=settings_factory
    ), patch("app.backfill.MyceliumPg"):
        with single_instance(database + ".lock"):
            assert main(["--no-postgres"]) == 3
    assert "already running" in capsys.readouterr().err


def test_cooldown_is_honored_before_the_metadata_request_too(tmp_path):
    """No YouTube request of any kind (yt-dlp metadata included) during a cooldown."""
    order = []
    now = 1_000.0
    b, store, _, _ = make_backfill(
        tmp_path,
        fetch_meta=lambda v: (order.append("meta"), VideoMeta(v, "T", "2026-09-15T00:00:00Z", 900, "UC", "WarFronts", "", ""))[1],
        fetch_transcript=lambda v: (order.append("transcript"), list(LONG_TRANSCRIPT))[1],
        sleep=lambda s: order.append(("sleep", round(s))),
    )
    b._wall = lambda: now
    store.set_state("blocked_until", str(now + 120))
    b.run(["@x"], discover=True)
    assert order[0] == ("sleep", 120)
    assert order.index("meta") > 0 and order.count(("sleep", 120)) == 1
