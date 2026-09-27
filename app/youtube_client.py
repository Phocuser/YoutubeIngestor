import logging
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

import httpx
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    NoTranscriptFound,
    NotTranslatable,
    TranscriptsDisabled,
    TranslationLanguageNotAvailable,
)

from .transcript_http import TRANSCRIPT_HTTP_TIMEOUT_SECONDS, TranscriptTimeoutSession

LOGGER = logging.getLogger(__name__)


class TimedTranscript(list):
    """List-compatible captions plus provider track provenance."""

    def __init__(self, rows, *, track_id="unknown", language="unknown", caption_kind="unknown", source_metadata=None):
        super().__init__(rows)
        self.track_id = track_id
        self.language = language
        self.caption_kind = caption_kind
        self.source_metadata = source_metadata or {}

ATOM_NAMESPACES = {
    "atom": "http://www.w3.org/2005/Atom",
    "yt": "http://www.youtube.com/xml/schemas/2015",
    "media": "http://search.yahoo.com/mrss/",
}

NO_TRANSCRIPT_EXCEPTIONS = (
    TranscriptsDisabled,
    NoTranscriptFound,
    NotTranslatable,
    TranslationLanguageNotAvailable,
)


def parse_feed_xml(xml_content: str, default_channel_id: str = "") -> List[Dict[str, str]]:
    """Parse YouTube Atom XML feed into a list of video dictionaries."""
    root = ET.fromstring(xml_content)

    feed_author_el = root.find("atom:author/atom:name", ATOM_NAMESPACES)
    feed_channel_name = (
        feed_author_el.text.strip()
        if feed_author_el is not None and feed_author_el.text
        else ""
    )

    feed_cid_el = root.find("yt:channelId", ATOM_NAMESPACES)
    feed_channel_id = (
        feed_cid_el.text.strip()
        if feed_cid_el is not None and feed_cid_el.text
        else default_channel_id
    )

    entries: List[Dict[str, str]] = []
    for entry in root.findall("atom:entry", ATOM_NAMESPACES):
        vid_el = entry.find("yt:videoId", ATOM_NAMESPACES)
        if vid_el is not None and vid_el.text:
            video_id = vid_el.text.strip()
        else:
            id_el = entry.find("atom:id", ATOM_NAMESPACES)
            raw_id = id_el.text.strip() if id_el is not None and id_el.text else ""
            video_id = raw_id.split(":")[-1] if raw_id else ""

        if not video_id:
            continue

        cid_el = entry.find("yt:channelId", ATOM_NAMESPACES)
        channel_id = (
            cid_el.text.strip()
            if cid_el is not None and cid_el.text
            else feed_channel_id
        )

        title_el = entry.find("atom:title", ATOM_NAMESPACES)
        title = title_el.text.strip() if title_el is not None and title_el.text else ""

        pub_el = entry.find("atom:published", ATOM_NAMESPACES)
        published_at = (
            pub_el.text.strip() if pub_el is not None and pub_el.text else ""
        )

        author_el = entry.find("atom:author/atom:name", ATOM_NAMESPACES)
        channel_name = (
            author_el.text.strip()
            if author_el is not None and author_el.text
            else feed_channel_name
        )

        entries.append(
            {
                "video_id": video_id,
                "title": title,
                "published_at": published_at,
                "channel_name": channel_name,
                "channel_id": channel_id,
            }
        )

    return entries


def fetch_channel_feed(channel_id: str, timeout: float = 30.0) -> List[Dict[str, str]]:
    """Fetch and parse YouTube channel Atom feed."""
    url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    with httpx.Client(timeout=timeout) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return parse_feed_xml(resp.text, default_channel_id=channel_id)


def fetch_transcript(video_id: str) -> Optional[str]:
    """Fetch transcript for a video ID using youtube-transcript-api.

    Returns plain text joined transcript, or None if no captions/transcript exist.
    Genuine network/API errors propagate to caller.
    """
    session = TranscriptTimeoutSession()
    try:
        api = YouTubeTranscriptApi(http_client=session)
        if hasattr(api, "fetch"):
            data = api.fetch(video_id)
        else:
            data = api.get_transcript(video_id)
    except NO_TRANSCRIPT_EXCEPTIONS:
        return None
    except Exception as exc:
        exc_name = exc.__class__.__name__
        if exc_name in (
            "TranscriptsDisabled",
            "NoTranscriptFound",
            "NotTranslatable",
            "TranslationLanguageNotAvailable",
            "NoTranscriptAvailable",
        ):
            return None
        raise
    finally:
        session.close()

    if not data:
        return None

    snippets: List[str] = []
    for item in data:
        if isinstance(item, dict):
            text = item.get("text", "")
        else:
            text = getattr(item, "text", str(item))
        if text:
            snippets.append(text)

    joined = " ".join(snippets).strip()
    return joined if joined else None


BLOCK_EXCEPTION_NAMES = ("RequestBlocked", "IpBlocked", "TooManyRequests")


class TranscriptBlocked(Exception):
    """YouTube is rate-limiting/blocking transcript requests from this IP."""


def fetch_timed_transcript(
    video_id: str, languages: tuple = ("en", "en-US", "en-GB")
) -> Optional[List[tuple]]:
    """Return ``[(text, start, duration), ...]`` or None when no captions exist.

    Raises :class:`TranscriptBlocked` when YouTube throttles this IP, so callers can
    back off; other genuine errors propagate.
    """
    session = TranscriptTimeoutSession()
    try:
        data = YouTubeTranscriptApi(http_client=session).fetch(video_id, languages=list(languages))
    except NO_TRANSCRIPT_EXCEPTIONS:
        return None
    except Exception as exc:
        name = exc.__class__.__name__
        if name in BLOCK_EXCEPTION_NAMES:
            raise TranscriptBlocked(name) from exc
        if name in ("VideoUnavailable", "VideoUnplayable", "AgeRestricted", "InvalidVideoId"):
            return None
        raise
    finally:
        session.close()
    track = getattr(data, "snippets", data)
    rows = [(item.text, float(item.start), float(item.duration)) for item in track if item.text]
    return TimedTranscript(rows, track_id=str(getattr(data, "track_id", "unknown")),
                           language=str(getattr(data, "language_code", "unknown")),
                           caption_kind="asr" if bool(getattr(data, "is_generated", False)) else "manual",
                           source_metadata={"provider": "youtube"}) or None


class YouTubeClient:
    """Client for YouTube RSS feed and transcript fetching."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    def fetch_channel_feed(self, channel_id: str) -> List[Dict[str, str]]:
        return fetch_channel_feed(channel_id, timeout=self.timeout)

    def fetch_transcript(self, video_id: str) -> Optional[str]:
        return fetch_transcript(video_id)

    def fetch_timed_transcript(self, video_id: str) -> Optional[List[tuple]]:
        return fetch_timed_transcript(video_id)
