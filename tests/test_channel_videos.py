import json
from pathlib import Path
from unittest.mock import MagicMock
import pytest

from app.channel_videos import (
    channel_url,
    fetch_video_meta,
    is_upcoming_or_live,
    list_channel_videos,
    VideoMeta,
    VideoRef,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class FakeYDLContext:
    def __init__(self, return_data=None, side_effect=None):
        self.return_data = return_data
        self.side_effect = side_effect
        self.recorded_opts = None
        self.recorded_calls = []

    def __call__(self, opts):
        self.recorded_opts = opts
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def extract_info(self, url, download=False):
        self.recorded_calls.append((url, download))
        if self.side_effect:
            raise self.side_effect
        return self.return_data


class OrderingYDLContext(FakeYDLContext):
    def __init__(self, return_data=None):
        super().__init__(return_data=return_data)
        self.events = []

    def __call__(self, opts):
        self.events.append(("factory", opts.copy()))
        return super().__call__(opts)

    def extract_info(self, url, download=False):
        self.events.append(("extract_info", url, download))
        return super().extract_info(url, download=download)


def test_channel_url_variants():
    # Handle with leading @
    assert (
        channel_url("@warographics643")
        == "https://www.youtube.com/@warographics643/videos"
    )
    # Handle without leading @
    assert (
        channel_url("warographics643")
        == "https://www.youtube.com/@warographics643/videos"
    )
    # 24-character UC channel ID
    channel_id = "UC9h8BDcXwkhZtnqoQJ7PggA"
    assert len(channel_id) == 24 and channel_id.startswith("UC")
    assert (
        channel_url(channel_id)
        == "https://www.youtube.com/channel/UC9h8BDcXwkhZtnqoQJ7PggA/videos"
    )
    # Full handle URL lacking /videos
    assert (
        channel_url("https://www.youtube.com/@warographics643")
        == "https://www.youtube.com/@warographics643/videos"
    )
    assert (
        channel_url("https://www.youtube.com/@warographics643/")
        == "https://www.youtube.com/@warographics643/videos"
    )
    # Full handle URL already having /videos
    assert (
        channel_url("https://www.youtube.com/@warographics643/videos")
        == "https://www.youtube.com/@warographics643/videos"
    )
    # Full channel URL lacking /videos
    assert (
        channel_url("https://www.youtube.com/channel/UC9h8BDcXwkhZtnqoQJ7PggA")
        == "https://www.youtube.com/channel/UC9h8BDcXwkhZtnqoQJ7PggA/videos"
    )
    assert (
        channel_url("https://www.youtube.com/channel/UC9h8BDcXwkhZtnqoQJ7PggA/")
        == "https://www.youtube.com/channel/UC9h8BDcXwkhZtnqoQJ7PggA/videos"
    )
    # Full channel URL already having /videos
    assert (
        channel_url("https://www.youtube.com/channel/UC9h8BDcXwkhZtnqoQJ7PggA/videos")
        == "https://www.youtube.com/channel/UC9h8BDcXwkhZtnqoQJ7PggA/videos"
    )
    # Non-channel / non-handle URL unchanged
    playlist_url = "https://www.youtube.com/playlist?list=PL12345"
    assert channel_url(playlist_url) == playlist_url


def test_list_channel_videos_with_fixture():
    fixture_path = FIXTURES_DIR / "ytdlp_flat_warfronts.json"
    fixture_data = json.loads(fixture_path.read_text(encoding="utf-8"))

    fake_factory = FakeYDLContext(return_data=fixture_data)
    results = list_channel_videos("@warographics643", ydl_factory=fake_factory)

    assert fake_factory.recorded_opts == {
        "extract_flat": True,
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
    }
    assert fake_factory.recorded_calls == [
        ("https://www.youtube.com/@warographics643/videos", False)
    ]
    assert len(results) == 14
    assert results[0] == VideoRef(
        video_id="SzYKwX3Tz9A",
        title="How Saudi Arabia Botched Yemen.",
        duration=1120,
    )
    assert results[-1] == VideoRef(
        video_id="xh-vlIHxDtY",
        title="Israel Has Lost Control of the West Bank.",
        duration=1133,
    )


def test_list_channel_videos_duration_filter():
    entries = [
        {"id": "vid_short", "title": "Too Short", "duration": 45},
        {"id": "vid_none", "title": "No Duration", "duration": None},
        {"id": "vid_120", "title": "Exact Cutoff", "duration": 120},
        {"id": "vid_long", "title": "Long Video", "duration": 850},
        {"id": "vid_under", "title": "Under Cutoff", "duration": 119},
    ]
    fake_factory = FakeYDLContext(return_data={"entries": entries})
    results = list_channel_videos("warographics643", min_duration=120, ydl_factory=fake_factory)

    assert [r.video_id for r in results] == ["vid_none", "vid_120", "vid_long"]
    assert results[0].duration is None
    assert results[1].duration == 120
    assert results[2].duration == 850


def test_list_channel_videos_preserves_order_and_drops_duplicates():
    entries = [
        {"id": "vid_1", "title": "First", "duration": 300},
        {"id": "vid_2", "title": "Second", "duration": 400},
        {"id": "vid_1", "title": "First Duplicate", "duration": 300},
        {"id": "vid_3", "title": "Third", "duration": 500},
        {"id": "vid_2", "title": "Second Duplicate", "duration": 400},
    ]
    fake_factory = FakeYDLContext(return_data={"entries": entries})
    results = list_channel_videos("warographics643", ydl_factory=fake_factory)

    assert [r.video_id for r in results] == ["vid_1", "vid_2", "vid_3"]
    assert [r.title for r in results] == ["First", "Second", "Third"]


def test_list_channel_videos_nested_playlist_flattening():
    entries = [
        {"id": "vid_top1", "title": "Top 1", "duration": 200},
        {
            "title": "Sub Playlist",
            "entries": [
                {"id": "vid_sub1", "title": "Sub 1", "duration": 210},
                {"id": "vid_sub2", "title": "Sub 2", "duration": 220},
            ],
        },
        {"id": "vid_top2", "title": "Top 2", "duration": 230},
    ]
    fake_factory = FakeYDLContext(return_data={"entries": entries})
    results = list_channel_videos("warographics643", ydl_factory=fake_factory)

    assert [r.video_id for r in results] == [
        "vid_top1",
        "vid_sub1",
        "vid_sub2",
        "vid_top2",
    ]


def test_list_channel_videos_skips_entries_without_id():
    entries = [
        {"title": "No ID", "duration": 300},
        {"id": "", "title": "Empty ID", "duration": 300},
        {"id": "valid_id", "title": "Valid Video", "duration": 300},
    ]
    fake_factory = FakeYDLContext(return_data={"entries": entries})
    results = list_channel_videos("warographics643", ydl_factory=fake_factory)

    assert len(results) == 1
    assert results[0].video_id == "valid_id"


def test_list_channel_videos_limit_is_passed_before_extraction():
    entries = [{"id": "vid_1", "title": "First", "duration": 300}]
    fake_factory = OrderingYDLContext(return_data={"entries": entries})

    results = list_channel_videos(
        "warographics643", limit=3, ydl_factory=fake_factory
    )

    assert [result.video_id for result in results] == ["vid_1"]
    assert fake_factory.recorded_opts["playlistend"] == 3
    assert fake_factory.recorded_opts["playlistend"] is not None
    assert fake_factory.recorded_calls == [
        ("https://www.youtube.com/@warographics643/videos", False)
    ]
    assert [event[0] for event in fake_factory.events] == ["factory", "extract_info"]


@pytest.mark.parametrize("invalid_limit", [0, -1, True, 1.5, "3"])
def test_list_channel_videos_rejects_invalid_limits(invalid_limit):
    fake_factory = FakeYDLContext(return_data={"entries": []})

    with pytest.raises(ValueError, match="positive integer"):
        list_channel_videos(
            "warographics643", limit=invalid_limit, ydl_factory=fake_factory
        )

    assert fake_factory.recorded_opts is None


def test_list_channel_videos_limit_is_enforced_after_flattening_filtering_and_deduplication():
    entries = [
        {"id": "vid_1", "title": "First", "duration": 300},
        {
            "title": "Nested",
            "entries": [
                {"id": "vid_2", "title": "Second", "duration": 400},
                {"id": "vid_1", "title": "Duplicate", "duration": 300},
                {"id": "vid_3", "title": "Third", "duration": 500},
            ],
        },
    ]
    fake_factory = FakeYDLContext(return_data={"entries": entries})

    results = list_channel_videos(
        "warographics643", min_duration=120, limit=2, ydl_factory=fake_factory
    )

    assert [result.video_id for result in results] == ["vid_1", "vid_2"]
    assert len(results) <= 2


def test_short_candidates_at_raw_bound_are_marked_truncated():
    entries = [
        {"id": "short-1", "title": "Short", "duration": 45},
        {"id": "long-1", "title": "Long one", "duration": 300},
        {"id": "long-2", "title": "Long two", "duration": 400},
    ]
    fake_factory = FakeYDLContext(return_data={"entries": entries})

    results = list_channel_videos(
        "warographics643", min_duration=120, limit=3, ydl_factory=fake_factory
    )

    assert [result.video_id for result in results] == ["long-1", "long-2"]
    assert results.truncated is True


def test_list_channel_videos_limit_does_not_materialize_ignored_listing_tail():
    def entries():
        for index in range(3):
            yield {"id": f"vid_{index}", "title": f"Video {index}", "duration": 300}
        raise AssertionError("listing was consumed past the local bound")

    fake_factory = FakeYDLContext(return_data={"entries": entries()})

    results = list_channel_videos("warographics643", limit=3, ydl_factory=fake_factory)

    assert [result.video_id for result in results] == ["vid_0", "vid_1", "vid_2"]


@pytest.mark.parametrize("raw_date", [None, "", "not-a-date", "20240230"])
def test_list_channel_videos_represents_missing_or_malformed_dates_as_unknown(raw_date):
    fake_factory = FakeYDLContext(
        return_data={"entries": [{"id": "vid_1", "title": "Video", "duration": 300, "upload_date": raw_date}]}
    )

    results = list_channel_videos("warographics643", ydl_factory=fake_factory)

    assert results[0].published_at is None


def test_list_channel_videos_preserves_structured_publication_date():
    fake_factory = FakeYDLContext(
        return_data={
            "entries": [
                {
                    "id": "vid_1",
                    "title": "Video",
                    "duration": 300,
                    "upload_date": "20240229",
                }
            ]
        }
    )

    results = list_channel_videos("warographics643", ydl_factory=fake_factory)

    assert results[0].published_at == "2024-02-29T00:00:00Z"


def test_list_channel_videos_without_limit_keeps_existing_provider_options_and_behavior():
    entries = [
        {"id": "vid_1", "title": "First", "duration": 300},
        {"id": "vid_2", "title": "Second", "duration": 400},
    ]
    fake_factory = FakeYDLContext(return_data={"entries": entries})

    results = list_channel_videos("warographics643", ydl_factory=fake_factory)

    assert [result.video_id for result in results] == ["vid_1", "vid_2"]
    assert "playlistend" not in fake_factory.recorded_opts


def test_fetch_video_meta():
    fake_info = {
        "id": "G_9WlR5TAzk",
        "title": "China is Preparing a Mass Mobilization. Why?",
        "upload_date": "20260915",
        "duration": 882,
        "channel_id": "UC9h8BDcXwkhZtnqoQJ7PggA",
        "channel": "WarFronts",
        "description": "Video description text.",
        "live_status": "was_live",
    }
    fake_factory = FakeYDLContext(return_data=fake_info)
    meta = fetch_video_meta("G_9WlR5TAzk", ydl_factory=fake_factory)

    assert fake_factory.recorded_opts == {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
    }
    assert fake_factory.recorded_calls == [
        ("https://www.youtube.com/watch?v=G_9WlR5TAzk", False)
    ]
    assert meta == VideoMeta(
        video_id="G_9WlR5TAzk",
        title="China is Preparing a Mass Mobilization. Why?",
        published_at="2026-09-15T00:00:00Z",
        duration=882,
        channel_id="UC9h8BDcXwkhZtnqoQJ7PggA",
        channel_name="WarFronts",
        description="Video description text.",
        live_status="was_live",
    )


def test_fetch_video_meta_date_and_channel_fallbacks():
    # Missing upload_date, uploader fallback when channel is missing
    fake_info_1 = {
        "id": "vid_123",
        "title": "Fallback Test",
        "upload_date": None,
        "duration": 600,
        "channel_id": "chan_123",
        "uploader": "UploaderFallbackName",
        "description": "",
        "live_status": "none",
    }
    fake_factory = FakeYDLContext(return_data=fake_info_1)
    meta_1 = fetch_video_meta("vid_123", ydl_factory=fake_factory)

    assert meta_1.published_at == ""
    assert meta_1.channel_name == "UploaderFallbackName"

    # Invalid upload_date format
    fake_info_2 = {
        "id": "vid_456",
        "title": "Invalid Date",
        "upload_date": "not-a-date",
        "channel_id": "chan_123",
        "channel": "ChannelName",
    }
    fake_factory_2 = FakeYDLContext(return_data=fake_info_2)
    meta_2 = fetch_video_meta("vid_456", ydl_factory=fake_factory_2)
    assert meta_2.published_at == ""
    assert meta_2.channel_name == "ChannelName"


def test_fetch_video_meta_propagates_ytdlp_exceptions():
    fake_factory = FakeYDLContext(side_effect=RuntimeError("yt-dlp extraction failed"))
    with pytest.raises(RuntimeError, match="yt-dlp extraction failed"):
        fetch_video_meta("error_vid", ydl_factory=fake_factory)


def test_is_upcoming_or_live():
    base_meta = VideoMeta(
        video_id="vid",
        title="title",
        published_at="",
        duration=None,
        channel_id="cid",
        channel_name="cname",
        description="",
        live_status="",
    )

    for status in ("is_upcoming", "is_live", "post_live"):
        meta = VideoMeta(
            video_id=base_meta.video_id,
            title=base_meta.title,
            published_at=base_meta.published_at,
            duration=base_meta.duration,
            channel_id=base_meta.channel_id,
            channel_name=base_meta.channel_name,
            description=base_meta.description,
            live_status=status,
        )
        assert is_upcoming_or_live(meta) is True

    for status in ("was_live", "none", "", "completed", "unknown"):
        meta = VideoMeta(
            video_id=base_meta.video_id,
            title=base_meta.title,
            published_at=base_meta.published_at,
            duration=base_meta.duration,
            channel_id=base_meta.channel_id,
            channel_name=base_meta.channel_name,
            description=base_meta.description,
            live_status=status,
        )
        assert is_upcoming_or_live(meta) is False
