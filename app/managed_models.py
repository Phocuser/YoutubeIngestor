"""Typed boundaries shared by the bounded managed YouTube worker."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol

from .channel_videos import VideoMeta, VideoRef


class Acquisition(Protocol):
    def list_videos(self, resource_ref: str, *, limit: int) -> Iterable[VideoRef | Mapping[str, Any]]: ...

    def fetch_meta(self, video_id: str) -> VideoMeta | Mapping[str, Any]: ...

    def fetch_transcript(self, video_id: str) -> Any: ...


def video_value(video: VideoRef | Mapping[str, Any], key: str, default: Any = None) -> Any:
    return video.get(key, default) if isinstance(video, Mapping) else getattr(video, key, default)


def error_code(message: str) -> str:
    return message if re.fullmatch(r"[A-Z0-9_]{1,64}", message or "") else "CAPTURE_INVALID"


@dataclass
class ItemOutcome:
    video_id: str
    submitted: bool
    stop_reason: str | None = None


@dataclass
class JobOutcome:
    job_id: str
    status: str
    processed_items: int
    stop_reason: str | None
    completed: bool
    item_outcomes: list[ItemOutcome] = field(default_factory=list)
    duplicates_skipped: int = 0
