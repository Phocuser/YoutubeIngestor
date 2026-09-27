"""Production YouTube acquisition boundary for managed source jobs."""
from __future__ import annotations

from typing import Any, Iterable

from .channel_videos import VideoMeta, VideoRef, fetch_video_meta, list_channel_videos


class DefaultYouTubeAcquisition:
    """Production adapter; tests inject this boundary and never call providers."""

    def list_videos(self, resource_ref: str, *, limit: int) -> Iterable[VideoRef]:
        # Ask the provider helper for one sentinel item so the worker can
        # distinguish an exact bound from a provider-side truncation.
        return list_channel_videos(resource_ref, limit=limit + 1)

    def fetch_meta(self, video_id: str) -> VideoMeta:
        return fetch_video_meta(video_id)

    def fetch_transcript(self, video_id: str) -> Any:
        from .youtube_client import fetch_timed_transcript
        return fetch_timed_transcript(video_id)
