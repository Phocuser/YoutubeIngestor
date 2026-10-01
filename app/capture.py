"""Shared, provider-neutral YouTube caption capture representation.

This module deliberately has no network or database dependencies.  Both the
poller and the backfill use it so a timed track has one deterministic envelope
and one reversible editorial view.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Callable, Mapping

from . import sponsor
from .models import CaptionTrack, timed_track


def canonical_timed_bytes(track: CaptionTrack) -> bytes:
    """Return the exact UTF-8 evidence bytes submitted to the capture boundary."""
    return json.dumps(track.to_dict(), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _clean_view(track: CaptionTrack, cleaner: Callable | None, video_id: str) -> sponsor.CleanResult:
    snippets = [
        sponsor.Snippet(s.text, s.start_ms / 1000.0, (s.end_ms - s.start_ms) / 1000.0)
        for s in track.segments
        if s.start_ms is not None and s.end_ms is not None
    ]
    if len(snippets) != len(track.segments):
        return sponsor.CleanResult(
            text=sponsor.join_text(track.segments), removed_seconds=0.0,
            removed_snippets=0, total_snippets=len(track.segments),
            sources=["timing_unknown"], ranges=[], policy_version="ads-v1",
            decisions=[{"segment_id": s.segment_id, "segment_index": s.original_index,
                        "decision": "uncertain", "reason": "caption_timing_unknown",
                        "source": "none", "start_ms": s.start_ms, "end_ms": s.end_ms}
                       for s in track.segments],
        )
    # The default view is local and deterministic.  An injected cleaner is
    # still supported for offline tests or an explicitly supplied decision set.
    result = (cleaner or (lambda rows, vid: sponsor.clean_transcript(
        rows, vid, fetch=lambda _video_id: [], use_heuristic=True)))(snippets, video_id)
    decisions = []
    for segment, decision in zip(track.segments, result.decisions or []):
        item = dict(decision)
        item.update({"segment_id": segment.segment_id, "segment_index": segment.original_index,
                     "start_ms": segment.start_ms, "end_ms": segment.end_ms})
        decisions.append(item)
    return sponsor.CleanResult(result.text, result.removed_seconds, result.removed_snippets,
                               getattr(result, "total_snippets", len(track.segments)),
                               list(result.sources), list(result.ranges),
                               decisions, result.policy_version)


def build_capture(
    video_id: str,
    transcript: Any,
    *,
    title: str = "",
    channel_id: str = "",
    channel_name: str = "YouTube",
    published_at: str | None = None,
    track_id: str = "unknown",
    language: str = "unknown",
    caption_kind: str = "unknown",
    cleaner: Callable | None = None,
    duration: int | None = None,
    is_public_channel_feed: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    track = timed_track(video_id, transcript, track_id=track_id, language=language,
                        caption_kind=caption_kind,
                        source_metadata={"provider": "youtube"})
    view = _clean_view(track, cleaner, video_id)
    raw_text = " ".join(segment.text for segment in track.segments)
    raw_bytes = canonical_timed_bytes(track)
    digest = hashlib.sha256(raw_bytes).hexdigest()
    source_alias = f"youtube:{video_id}"
    cleaning = {"policy_version": view.policy_version, "decisions": view.decisions or [],
                "ranges": [list(r) for r in view.ranges], "sources": list(view.sources),
                "derived_from": "caption_track"}
    envelope = {
        # This is a provider alias, never a graph UUID.  Graph identity is
        # assigned by the durable Mycelium capture boundary.
        "id": source_alias,
        "name": title,
        "source_agency": channel_name,
        "published_at": published_at,
        "raw_content": raw_text,
        "editorial_content": view.text,
        "raw_timed_captions": track.to_dict(),
        "caption_track": track.to_dict(),
        "cleaning": cleaning,
    }
    metadata = {
        "video_id": video_id, "source_alias": source_alias, "title": title,
        "channel_id": channel_id, "channel_name": channel_name,
        "published_at": published_at, "duration": duration,
        "transcript_source": "youtube-captions", "provider": "youtube",
        "track_id": track.track_id, "language": track.language,
        "caption_kind": track.caption_kind, "raw_timed_sha256": digest,
        "raw_timed_byte_length": len(raw_bytes), "raw_timed_encoding": "utf-8",
        "ads_removed_seconds": round(view.removed_seconds, 1),
        "ads_removed_snippets": view.removed_snippets,
        "ad_removal_sources": list(view.sources), "ad_ranges": [list(r) for r in view.ranges],
        "ad_decisions": view.decisions or [], "ad_policy_version": view.policy_version,
    }
    # A channel ID alone is not an authorization signal. Only ingestion paths
    # that enumerated this item from a public channel feed may set a public ACL.
    if is_public_channel_feed:
        metadata["access_scope"] = "public"
    return envelope, metadata
