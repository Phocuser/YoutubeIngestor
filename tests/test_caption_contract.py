from app.models import timed_track


def test_poll_and_backfill_timed_contract_preserves_raw_order_and_unknown_time():
    track = timed_track("video", [("first", 0, 1), ("late", None, None)])
    assert [s.original_index for s in track.segments] == [0, 1]
    assert track.segments[1].start_ms is None
    payload = track.to_dict()
    assert payload["segments"][1]["segment_id"] == "video:unknown:1"
    assert payload["segments"][1]["end_ms"] is None
