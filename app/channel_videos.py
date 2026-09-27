from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Any, Callable, Dict, List, Optional

try:  # Keep entrypoint imports clean when the optional provider tool is absent.
    import yt_dlp
except ImportError:  # pragma: no cover - exercised by dependency-free smoke tests
    yt_dlp = None

LOGGER = logging.getLogger(__name__)

UPCOMING_OR_LIVE_STATUSES = {"is_upcoming", "is_live", "post_live"}


@dataclass(frozen=True)
class VideoRef:
    video_id: str
    title: str
    duration: Optional[int]
    published_at: Optional[str] = None


@dataclass(frozen=True)
class VideoMeta:
    video_id: str
    title: str
    published_at: str
    duration: Optional[int]
    channel_id: str
    channel_name: str
    description: str
    live_status: str


def channel_url(handle_or_url: str) -> str:
    """Normalize a channel handle, channel ID, or URL into a channel videos URL."""
    val = handle_or_url.strip()
    if val.startswith("http://") or val.startswith("https://"):
        norm = val.rstrip("/")
        if ("/@" in norm or "/channel/" in norm) and not norm.endswith("/videos"):
            return f"{norm}/videos"
        return val
    if val.startswith("UC") and len(val) == 24:
        return f"https://www.youtube.com/channel/{val}/videos"
    if val.startswith("@"):
        return f"https://www.youtube.com/{val}/videos"
    return f"https://www.youtube.com/@{val}/videos"


def _parse_upload_date(raw_date: Any) -> str:
    if not isinstance(raw_date, str) or len(raw_date) != 8 or not raw_date.isdigit():
        return ""
    try:
        dt = datetime.strptime(raw_date, "%Y%m%d")
        return dt.strftime("%Y-%m-%dT00:00:00Z")
    except ValueError:
        return ""


def list_channel_videos(
    handle_or_url: str,
    min_duration: int = 120,
    ydl_factory: Optional[Callable[..., Any]] = None,
    limit: Optional[int] = None,
) -> List[VideoRef]:
    """Extract flat video references from a YouTube channel."""
    if limit is not None and (
        isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0
    ):
        raise ValueError("limit must be a positive integer when supplied")

    url = channel_url(handle_or_url)
    opts = {
        "extract_flat": True,
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
    }
    if limit is not None:
        opts["playlistend"] = limit
    if ydl_factory is None and yt_dlp is None:
        raise RuntimeError("yt-dlp dependency is required for channel discovery")
    factory = ydl_factory or yt_dlp.YoutubeDL
    with factory(opts) as ydl:
        info = ydl.extract_info(url, download=False)

    raw_entries = info.get("entries") if isinstance(info, dict) else None
    if not raw_entries:
        return []

    def iter_flat_entries():
        """Yield flat entries without retaining the provider listing."""
        for entry in raw_entries:
            if not isinstance(entry, dict):
                continue
            nested = entry.get("entries")
            if isinstance(nested, list):
                for sub in nested:
                    if isinstance(sub, dict):
                        yield sub
            else:
                yield entry

    seen_ids = set()
    results: List[VideoRef] = []
    raw_candidates = 0
    for entry in iter_flat_entries():
        if limit is not None and raw_candidates >= limit:
            break
        raw_candidates += 1
        vid_id = entry.get("id")
        if not vid_id:
            continue
        video_id = str(vid_id).strip()
        if not video_id or video_id in seen_ids:
            continue

        raw_duration = entry.get("duration")
        dur_int: Optional[int] = None
        if raw_duration is not None:
            try:
                dur_int = int(raw_duration)
            except (ValueError, TypeError):
                dur_int = None

            if dur_int is not None and dur_int < min_duration:
                continue

        seen_ids.add(video_id)
        title = str(entry.get("title") or "").strip()
        published_at = _parse_upload_date(entry.get("upload_date")) or None
        results.append(
            VideoRef(
                video_id=video_id,
                title=title,
                duration=dur_int,
                published_at=published_at,
            )
        )
        if limit is not None and len(results) >= limit:
            break

    return results


def fetch_video_meta(
    video_id: str,
    ydl_factory: Optional[Callable[..., Any]] = None,
) -> VideoMeta:
    """Fetch metadata for a single YouTube video."""
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
    }
    url = f"https://www.youtube.com/watch?v={video_id}"
    if ydl_factory is None and yt_dlp is None:
        raise RuntimeError("yt-dlp dependency is required for video metadata")
    factory = ydl_factory or yt_dlp.YoutubeDL
    with factory(opts) as ydl:
        info = ydl.extract_info(url, download=False) or {}

    raw_duration = info.get("duration")
    dur_int: Optional[int] = None
    if raw_duration is not None:
        try:
            dur_int = int(raw_duration)
        except (ValueError, TypeError):
            dur_int = None

    channel_name = str(info.get("channel") or info.get("uploader") or "")

    return VideoMeta(
        video_id=video_id,
        title=str(info.get("title") or ""),
        published_at=_parse_upload_date(info.get("upload_date")),
        duration=dur_int,
        channel_id=str(info.get("channel_id") or ""),
        channel_name=channel_name,
        description=str(info.get("description") or ""),
        live_status=str(info.get("live_status") or ""),
    )


def is_upcoming_or_live(meta: VideoMeta) -> bool:
    """Check if video is upcoming, currently live, or post-live."""
    return meta.live_status in UPCOMING_OR_LIVE_STATUSES
