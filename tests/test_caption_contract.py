from app.models import timed_track
from app.capture import build_capture, canonical_timed_bytes
import hashlib


def test_poll_and_backfill_timed_contract_preserves_raw_order_and_unknown_time():
    track = timed_track("video", [("first", 0, 1), ("late", None, None)])
    assert [s.original_index for s in track.segments] == [0, 1]
    assert track.segments[1].start_ms is None
    payload = track.to_dict()
    assert payload["segments"][1]["segment_id"] == "video:unknown:1"
    assert payload["segments"][1]["end_ms"] is None


def test_shared_poll_backfill_envelope_and_metadata_are_identical():
    rows = [{"segment_id": "a", "text": "before", "start": 0, "duration": 1},
            {"segment_id": "b", "text": "this video is sponsored by a premium subscriber offer", "start": 1, "duration": 2},
            {"segment_id": "c", "text": "after", "start": 3, "duration": 1}]
    args = dict(title="Title", channel_id="UC1", channel_name="Channel",
                published_at="2026-09-18T00:00:00Z")
    poll = build_capture("v", rows, **args)
    backfill = build_capture("v", rows, **args)
    assert poll == backfill
    envelope, metadata = poll
    assert envelope["id"] == "youtube:v"
    assert "sponsored by" in envelope["raw_content"]
    assert envelope["editorial_content"] != envelope["raw_content"]
    assert envelope["raw_timed_captions"]["segments"][1]["segment_id"] == "b"
    assert metadata["provider"] == "youtube"
    assert metadata["raw_timed_sha256"] == hashlib.sha256(
        canonical_timed_bytes(timed_track("v", rows, source_metadata={"provider": "youtube"}))
    ).hexdigest()


def test_unknown_timing_is_retained_and_uncertain_decision_is_reversible():
    envelope, metadata = build_capture("v", [("unknown ad text", None, None)])
    segment = envelope["raw_timed_captions"]["segments"][0]
    assert segment["start_ms"] is None and segment["end_ms"] is None
    assert envelope["cleaning"]["decisions"][0]["decision"] == "uncertain"
    assert envelope["cleaning"]["decisions"][0]["segment_id"] == segment["segment_id"]
    assert metadata["ad_removal_sources"] == ["timing_unknown"]
