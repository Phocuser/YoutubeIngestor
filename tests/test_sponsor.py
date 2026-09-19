"""Tests for app.sponsor module."""

import json
import urllib.error
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.sponsor import (
    DEFAULT_CATEGORIES,
    CleanResult,
    Snippet,
    SponsorBlockUnavailable,
    clean_transcript,
    effective_duration,
    effective_end,
    fetch_sponsor_segments,
    heuristic_ad_ranges,
    join_text,
    merge_ranges,
    strip_ranges,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_transcript_fixture():
    with open(FIXTURES_DIR / "transcript_G_9WlR5TAzk.json", "r", encoding="utf-8") as f:
        return json.load(f)


def _load_sponsorblock_fixture():
    with open(FIXTURES_DIR / "sponsorblock_G_9WlR5TAzk.json", "r", encoding="utf-8") as f:
        return json.load(f)


def test_clean_transcript_sponsorblock_real_video():
    """Real-video test using SponsorBlock segments."""
    transcript = _load_transcript_fixture()
    result = clean_transcript(
        transcript,
        "9WlR5TAzk",
        fetch=lambda vid: [(485.5, 528.3)],
    )

    assert isinstance(result, CleanResult)
    text_lower = result.text.lower()
    assert "premium subscriber" not in text_lower
    assert "$50 a year" not in result.text
    assert "fronts.co" not in text_lower
    assert "catalog here on warfronts" not in text_lower

    assert "Beijing spying" in result.text
    assert "Decipher the purpose" in result.text

    assert 35.0 <= result.removed_seconds <= 55.0
    assert result.sources == ["sponsorblock"]
    assert result.removed_snippets > 0
    assert result.total_snippets == len(transcript)
    assert result.ranges == [(485.5, 528.3)]


def test_clean_transcript_heuristic_real_video():
    """Real-video fallback test when SponsorBlock is unavailable."""
    transcript = _load_transcript_fixture()

    def failing_fetch(vid: str):
        raise SponsorBlockUnavailable("SponsorBlock connection timed out")

    result = clean_transcript(transcript, "9WlR5TAzk", fetch=failing_fetch)

    assert isinstance(result, CleanResult)
    text_lower = result.text.lower()
    assert "premium subscriber" not in text_lower
    assert "$50 a year" not in result.text

    assert "Beijing spying" in result.text
    assert "Decipher the purpose" in result.text

    assert result.sources == ["heuristic", "sponsorblock_unavailable"]
    assert 35.0 <= result.removed_seconds <= 55.0
    assert result.removed_snippets > 0


def test_clean_transcript_no_ads():
    """Empty fetch on clean video returns unchanged text."""
    snippets = [
        Snippet("History of naval aviation begins with early experiments.", 0.0, 4.0),
        Snippet("Pioneering pilots demonstrated takeoffs from modified cruisers.", 4.0, 5.0),
        Snippet("These developments transformed maritime strategy.", 9.0, 4.0),
    ]
    result = clean_transcript(snippets, "clean_video", fetch=lambda vid: [])

    assert result.text == (
        "History of naval aviation begins with early experiments. "
        "Pioneering pilots demonstrated takeoffs from modified cruisers. "
        "These developments transformed maritime strategy."
    )
    assert result.removed_snippets == 0
    assert result.removed_seconds == 0.0
    assert result.sources == []
    assert result.ranges == []
    assert result.total_snippets == 3


def test_fetch_sponsor_segments_404():
    """HTTP 404 from SponsorBlock means no segments, returns empty list."""
    mock_opener = MagicMock(
        side_effect=urllib.error.HTTPError("http://example.com", 404, "Not Found", {}, None)
    )
    segments = fetch_sponsor_segments("test_video_id", opener=mock_opener)
    assert segments == []


def test_fetch_sponsor_segments_500():
    """HTTP 500 error raises SponsorBlockUnavailable."""
    mock_opener = MagicMock(
        side_effect=urllib.error.HTTPError("http://example.com", 500, "Server Error", {}, None)
    )
    with pytest.raises(SponsorBlockUnavailable):
        fetch_sponsor_segments("test_video_id", opener=mock_opener)


def test_fetch_sponsor_segments_invalid_json():
    """Invalid JSON response raises SponsorBlockUnavailable."""
    mock_opener = MagicMock(return_value=b"<html>502 Bad Gateway</html>")
    with pytest.raises(SponsorBlockUnavailable):
        fetch_sponsor_segments("test_video_id", opener=mock_opener)


def test_fetch_sponsor_segments_filtering_and_sorting():
    """Segments are filtered by category, actionType, validity, and sorted."""
    payload = [
        {"category": "sponsor", "actionType": "skip", "segment": [100.0, 120.0]},
        {"category": "sponsor", "actionType": "full", "segment": [0.0, 0.0]},
        {"category": "outro", "actionType": "skip", "segment": [200.0, 210.0]},
        {"category": "selfpromo", "actionType": "mute", "segment": [10.0, 30.0]},
        {"category": "interaction", "segment": [40.0, 50.0]},
        {"category": "sponsor", "actionType": "skip", "segment": [60.0, 50.0]},
        {"category": "sponsor", "actionType": "skip", "segment": [0.0, -5.0]},
    ]
    mock_opener = MagicMock(return_value=json.dumps(payload).encode("utf-8"))
    res = fetch_sponsor_segments(
        "any_vid",
        categories=("sponsor", "selfpromo", "interaction"),
        opener=mock_opener,
    )
    assert res == [(10.0, 30.0), (40.0, 50.0), (100.0, 120.0)]


def test_fetch_sponsor_segments_real_fixture():
    """Parse real fixture payload from SponsorBlock."""
    fixture_data = _load_sponsorblock_fixture()
    mock_opener = MagicMock(return_value=json.dumps(fixture_data).encode("utf-8"))
    res = fetch_sponsor_segments("9WlR5TAzk", opener=mock_opener)
    assert res == [(485.5, 528.3), (485.5, 528.3)]


def test_merge_ranges():
    """Test merge_ranges behavior for overlapping and near-adjacent intervals."""
    # Overlapping
    assert merge_ranges([(10.0, 20.0), (18.0, 25.0)]) == [(10.0, 25.0)]
    # Near adjacent within gap=1.0
    assert merge_ranges([(10.0, 20.0), (20.5, 30.0)], gap=1.0) == [(10.0, 30.0)]
    # Separated beyond gap
    assert merge_ranges([(10.0, 20.0), (22.0, 30.0)], gap=1.0) == [(10.0, 20.0), (22.0, 30.0)]
    # Empty and single
    assert merge_ranges([]) == []
    assert merge_ranges([(5.0, 10.0)]) == [(5.0, 10.0)]
    # Unsorted input
    assert merge_ranges([(30.0, 40.0), (10.0, 20.0)]) == [(10.0, 20.0), (30.0, 40.0)]


def test_effective_end_and_duration():
    """Test effective end bounded by next snippet start."""
    s1 = Snippet("first", start=10.0, duration=5.0)
    s2 = Snippet("second", start=12.0, duration=4.0)
    assert effective_end(s1, s2) == 12.0
    assert effective_duration(s1, s2) == 2.0

    assert effective_end(s2, None) == 16.0
    assert effective_duration(s2, None) == 4.0

    s3 = Snippet("third", start=20.0, duration=2.0)
    s4 = Snippet("fourth", start=30.0, duration=2.0)
    assert effective_end(s3, s4) == 22.0
    assert effective_duration(s3, s4) == 2.0


def test_join_text_cleaning():
    """Test bracket tag removal, leading >> removal, and whitespace normalization."""
    snippets = [
        Snippet("[Music] >> Welcome to our broadcast.", 0.0, 2.0),
        Snippet(">> We are discussing [Applause] the newest findings.", 2.0, 3.0),
        Snippet("More details to follow. [foreign speech]", 5.0, 3.0),
    ]
    cleaned = join_text(snippets)
    assert cleaned == (
        "Welcome to our broadcast. "
        "We are discussing the newest findings. "
        "More details to follow."
    )
    assert join_text([]) == ""


def test_heuristic_single_stray_vs_promo_code():
    """Single stray mention is ignored; single promo code mention triggers an ad."""
    stray = [Snippet("Be sure to subscribe to the channel if you enjoyed.", 100.0, 4.0)]
    assert heuristic_ad_ranges(stray) == []

    promo = [Snippet("Use promo code DISCOVER at checkout for a discount.", 100.0, 5.0)]
    ranges = heuristic_ad_ranges(promo)
    assert len(ranges) == 1
    start, end = ranges[0]
    assert start <= 100.0 and end >= 105.0  # covers the promo-code sentence
    assert end - start < 40.0  # and stays tight around it


def test_heuristic_cluster_weak_hits():
    """Weak hits cluster within 30s, but not when separated by > 30s."""
    s1 = Snippet("Our friends at Nord make online privacy easy.", 10.0, 3.0)
    s2 = Snippet("Sign up for a free trial today.", 25.0, 3.0)
    ranges = heuristic_ad_ranges([s1, s2])
    assert len(ranges) == 1
    assert ranges[0][0] <= 10.0 and ranges[0][1] >= 28.0  # one range covering both hits

    s3 = Snippet("Our friends at Nord make online privacy easy.", 10.0, 3.0)
    s4 = Snippet("Sign up for a free trial today.", 60.0, 3.0)
    assert heuristic_ad_ranges([s3, s4]) == []


def test_strip_ranges():
    """Test snippet stripping with midpoint and overlap criteria."""
    s1 = Snippet("before ad", start=0.0, duration=8.0)
    s2 = Snippet("inside ad", start=11.0, duration=5.0)
    s3 = Snippet("after ad", start=25.0, duration=5.0)

    kept, removed_sec, removed_count = strip_ranges([s1, s2, s3], [(10.0, 20.0)])
    assert len(kept) == 2
    assert kept[0].text == "before ad"
    assert kept[1].text == "after ad"
    assert removed_count == 1
    assert removed_sec == 5.0


def test_heuristic_lands_on_the_real_ad_window():
    """Anchored on spoken transitions: close to SponsorBlock's community label."""
    transcript = _load_transcript_fixture()
    (start, end), = heuristic_ad_ranges(transcript)
    assert abs(start - 485.5) <= 2.0
    assert abs(end - 528.3) <= 3.5


def test_one_stray_subscriber_mention_is_not_double_counted():
    stray = [
        Snippet("Premium subscribers of the newspaper were angry about the policy.", 100.0, 4.0),
        Snippet("The minister refused to comment on that.", 104.0, 4.0),
    ]
    assert heuristic_ad_ranges(stray) == []
