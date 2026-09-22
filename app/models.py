"""Provider-neutral YouTube caption representations."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class TimedCaption:
    segment_id: str
    text: str
    start_ms: int | None
    end_ms: int | None
    original_index: int
    precision: str = "segment"
    source: str = "youtube"


@dataclass(frozen=True)
class CaptionTrack:
    video_id: str
    track_id: str
    language: str
    caption_kind: str
    segments: tuple[TimedCaption, ...]
    source_metadata: Mapping[str, Any] = field(default_factory=dict)
    coverage_state: str = "complete"

    def to_dict(self) -> dict[str, Any]:
        return {
            "video_id": self.video_id, "track_id": self.track_id,
            "language": self.language, "caption_kind": self.caption_kind,
            "coverage_state": self.coverage_state,
            "source_metadata": dict(self.source_metadata),
            "segments": [{"segment_id": s.segment_id, "text": s.text,
                          "start_ms": s.start_ms, "end_ms": s.end_ms,
                          "original_index": s.original_index, "precision": s.precision,
                          "source": s.source} for s in self.segments],
        }


def timed_track(video_id: str, rows, *, track_id: str = "unknown", language: str = "unknown", caption_kind: str = "unknown", source_metadata: Mapping[str, Any] | None = None) -> CaptionTrack:
    track_id = getattr(rows, "track_id", track_id)
    language = getattr(rows, "language", language)
    caption_kind = getattr(rows, "caption_kind", caption_kind)
    source_metadata = {**getattr(rows, "source_metadata", {}), **(source_metadata or {})}
    segments = []
    for index, row in enumerate(rows):
        if isinstance(row, dict):
            text, start, duration = row.get("text", ""), row.get("start"), row.get("duration")
            segment_id = row.get("segment_id")
            precision = row.get("precision", "segment")
        else:
            text, start, duration = row[0], row[1], row[2]
            segment_id, precision = None, "segment"
        start_ms = None if start is None else round(float(start) * 1000)
        end_ms = None if start is None or duration is None else round((float(start) + float(duration)) * 1000)
        if start_ms is not None and start_ms < 0 or end_ms is not None and end_ms < 0:
            raise ValueError("caption timing must be non-negative")
        if start_ms is not None and end_ms is not None and end_ms < start_ms:
            raise ValueError("caption end precedes start")
        if start_ms is None or end_ms is None:
            precision = "unknown"
        segments.append(TimedCaption(segment_id or f"{video_id}:{track_id}:{index}", str(text), start_ms, end_ms, index, precision))
    return CaptionTrack(video_id, track_id, language, caption_kind, tuple(segments), source_metadata or {})
