"""Submit YouTube caption captures through Mycelium's typed receipt boundary."""
import logging
import sys
import uuid
import base64
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


LOGGER = logging.getLogger(__name__)

UUID_PREFIX = "mycelium.youtube.v1/"
STORED = "stored"
ALREADY_PRESENT = "already_present"
QUARANTINED = "revision_needed"


def article_uuid(video_id: str) -> str:
    """One id for both Postgres ``articles.id`` and the indexer envelope
    (``event_nodes.article_id`` is a foreign key to it)."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{UUID_PREFIX}{video_id}"))


def watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


class MyceliumPg:
    def __init__(self, pg_url: str, mycelium_dir: str, redis_addr: str = "127.0.0.1:6381"):
        root = str(Path(mycelium_dir).resolve())
        if root not in sys.path:
            sys.path.insert(0, root)
        from persistence import Database  # noqa: PLC0415
        self._db = Database(pg_url)
        host, _, port = redis_addr.partition(":")
        self._redis = (host, int(port or 6379))
        self.last_stored = None

    def submit(self, envelope: Dict[str, Any], metadata: Dict[str, Any]):
        """Submit poll and backfill output through the common receipt boundary."""
        from contracts.capture import Submission  # noqa: PLC0415
        from contracts.timed_caption import (  # noqa: PLC0415
            TIMED_CAPTION_MEDIA_TYPE,
            canonical_timed_caption_bytes,
            capture_from_youtube_track,
        )
        from persistence.ingest import admit  # noqa: PLC0415
        video_id = str(metadata["video_id"])
        track = envelope.get("raw_timed_captions") or envelope.get("caption_track")
        if not track:
            raise ValueError("capture requires raw timed captions")
        # The durable evidence is the canonical timed track, not a derived
        # editorial string.  This preserves exact UTF-8 bytes and ordering.
        from .models import CaptionTrack, TimedCaption
        segments = tuple(TimedCaption(**segment) for segment in track["segments"])
        raw_track = CaptionTrack(track["video_id"], track["track_id"], track["language"],
                                  track["caption_kind"], segments,
                                  track.get("source_metadata", {}), track.get("coverage_state", "complete"))
        # The typed contract is the source of truth for both bytes and media
        # type.  Keep this conversion adjacent to admission so a future
        # producer-shape change cannot silently turn typed evidence into prose.
        capture = capture_from_youtube_track(raw_track.to_dict())
        raw = canonical_timed_caption_bytes(capture)
        digest = hashlib.sha256(raw).hexdigest()
        submission = Submission(
            schema_version="capture.v1", submission_id=uuid.uuid4(), idempotency_key=f"youtube:{video_id}:{digest}",
            adapter="youtube", adapter_version="capture-v1", submission_kind="capture", correlation_id=uuid.uuid4(),
            source={"source_id": "youtube", "scope": f"video:{video_id}"}, requested_url=watch_url(video_id), canonical_url=watch_url(video_id), final_url=watch_url(video_id),
            metadata={**metadata, "video_id": video_id, "source_alias": f"youtube:{video_id}",
                      "content_scope": "full_text", "access_state": "allowed",
                      "caption_track": envelope.get("caption_track"),
                      "cleaning": envelope.get("cleaning", {}),
                      "raw_timed_sha256": digest, "raw_timed_byte_length": len(raw),
                      "raw_timed_encoding": "utf-8", "timed_caption_schema": capture.schema_version,
                      "timed_caption_identity": capture.identity_key},
            capture={"capture_type": "supplied_capture", "content_base64": base64.b64encode(raw).decode("ascii"), "byte_length": len(raw), "sha256": digest, "media_type": TIMED_CAPTION_MEDIA_TYPE, "retrieval_metadata": {"provider": "youtube", "video_id": video_id}},
        )
        return admit(self._db, submission, principal_id="youtube-adapter")

    def drain_worker(self, max_idle_batches: int = 1, max_batches: int = 500) -> int:
        """Run Mycelium's consumer-group worker until the candidate stream is idle;
        returns the number of candidates handled."""
        LOGGER.warning("legacy importer worker drain disabled; use the central Mycelium worker")
        return 0

    def insert(self, envelope: Dict[str, Any], metadata: Dict[str, Any]) -> str:
        raise RuntimeError("LEGACY_PATH_DISABLED: use submit()")
