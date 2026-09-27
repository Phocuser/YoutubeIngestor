"""Conversion from the YouTube envelope to Mycelium's typed capture wire form."""
from __future__ import annotations

import base64
import hashlib
import json
from typing import Any, Mapping


def managed_timed_capture(envelope: Mapping[str, Any], metadata: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Convert the existing typed caption envelope to the released wire contract."""
    track = envelope.get("raw_timed_captions") or envelope.get("caption_track")
    if not isinstance(track, Mapping) or not isinstance(track.get("segments"), list) or not track["segments"]:
        raise ValueError("capture requires a non-empty typed timed-caption track")
    if track.get("coverage_state", "complete") != "complete":
        raise ValueError("partial or unknown caption coverage cannot be submitted as complete")
    video_id = track.get("video_id")
    if not isinstance(video_id, str) or not video_id:
        raise ValueError("typed caption track requires video_id")
    segments = []
    for ordinal, item in enumerate(track["segments"]):
        if not isinstance(item, Mapping) or not isinstance(item.get("text"), str) or not item["text"].strip():
            raise ValueError("caption segments must contain non-empty text")
        if item.get("original_index", ordinal) != ordinal:
            raise ValueError("caption segment order must be contiguous")
        segment_id = item.get("segment_id") or f"{video_id}:{ordinal}"
        if not isinstance(segment_id, str) or not segment_id or segment_id in {part["segment_id"] for part in segments}:
            raise ValueError("CAPTURE_INVALID")
        precision = item.get("precision", "unknown")
        source = item.get("source", "youtube")
        if precision not in {"segment", "unknown"} or source != "youtube":
            raise ValueError("CAPTURE_INVALID")
        segments.append({
            "segment_id": segment_id, "ordinal": ordinal, "text": item["text"],
            "start_ms": item.get("start_ms"), "end_ms": item.get("end_ms"),
            "precision": precision, "source": source,
        })
    typed = {
        "schema_version": "youtube.timed-caption.v1",
        "media_type": "application/vnd.mycelium.youtube-timed-caption+json",
        "identity_key": f"youtube:{video_id}", "video_id": video_id,
        "track_id": track.get("track_id", "unknown"), "language": track.get("language", "unknown"),
        "caption_kind": track.get("caption_kind", "unknown"), "transcript_kind": "caption",
        "segments": segments, "source_metadata": dict(track.get("source_metadata") or {}),
        "coverage_state": "complete",
    }
    raw = json.dumps(typed, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    url = f"https://www.youtube.com/watch?v={video_id}"
    capture = {
        "capture_type": "supplied_capture", "content_base64": base64.b64encode(raw).decode("ascii"),
        "byte_length": len(raw), "sha256": digest, "media_type": typed["media_type"],
        "retrieval_metadata": {"provider": "youtube", "video_id": video_id},
    }
    output_metadata = {**dict(metadata), "video_id": video_id, "raw_timed_sha256": digest,
                       "raw_timed_byte_length": len(raw), "raw_timed_encoding": "utf-8",
                       "timed_caption_schema": typed["schema_version"], "timed_caption_identity": typed["identity_key"]}
    return capture, output_metadata, url
