"""Mycelium 2 HTTP capture.v1 boundary for timed YouTube captions."""
from __future__ import annotations

import base64
import hashlib
import json
import math
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

MAX_REQUEST_BYTES = 8_000_000
MAX_RESPONSE_BYTES = 1_000_000
TIMED_CAPTION_MEDIA_TYPE = "application/vnd.mycelium.youtube-timed-caption+json"
ALREADY_PRESENT = "already_present"
STORED = "stored"


def watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


@dataclass(frozen=True)
class CaptureReceipt:
    state: str
    capture_id: str
    replayed: bool
    receipt_id: str


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Keep a bearer token and capture body bound to the configured origin."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def capture_bytes(track: dict[str, Any]) -> bytes:
    """Build the Mycelium timed-caption contract using only stdlib JSON."""
    video_id = track["video_id"]
    contract = {
        "schema_version": "youtube.timed-caption.v1",
        "media_type": TIMED_CAPTION_MEDIA_TYPE,
        "identity_key": f"youtube:{video_id}",
        "video_id": video_id,
        "track_id": track["track_id"],
        "language": track["language"],
        "caption_kind": track["caption_kind"],
        "transcript_kind": "caption",
        "coverage_state": track.get("coverage_state", "complete"),
        "source_metadata": track.get("source_metadata", {}),
        "segments": [
            {
                "segment_id": item["segment_id"],
                "ordinal": item["original_index"],
                "text": item["text"],
                "start_ms": item["start_ms"],
                "end_ms": item["end_ms"],
                "precision": item.get("precision", "unknown"),
                "source": item.get("source", "youtube"),
            }
            for item in track["segments"]
        ],
    }
    return json.dumps(contract, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


class Mycelium2HTTP:
    def __init__(self, base_url: str, api_token: str, timeout: float = 30.0):
        self.base_url = base_url.strip().rstrip("/")
        self.api_token = api_token
        timeout = float(timeout)
        if not math.isfinite(timeout):
            raise ValueError("MYCELIUM2_TIMEOUT_SECONDS must be finite")
        self.timeout = max(1.0, min(timeout, 120.0))
        parsed = urlsplit(self.base_url)
        try:
            parsed.port
        except ValueError:
            raise ValueError("MYCELIUM2_URL must be an HTTP(S) origin without credentials") from None
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ValueError("MYCELIUM2_URL must be an HTTP(S) origin without credentials")
        if not self.api_token:
            raise ValueError("MYCELIUM2_API_TOKEN is required")

    @staticmethod
    def payload(envelope: dict[str, Any], metadata: dict[str, Any]) -> tuple[dict, str, int]:
        video_id = str(metadata["video_id"])
        track = envelope.get("raw_timed_captions") or envelope.get("caption_track")
        if not isinstance(track, dict):
            raise ValueError("capture requires raw timed captions")
        raw = capture_bytes(track)
        if not raw:
            raise ValueError("capture content is empty")
        digest = hashlib.sha256(raw).hexdigest()
        url = watch_url(video_id)
        # Keep the submission metadata bounded. The exact track is in capture
        # bytes, while the complete local envelope remains in the retry spool.
        retained_metadata = {
            key: metadata[key] for key in (
                "title", "channel_id", "channel_name", "published_at", "duration",
                "transcript_source", "provider", "track_id", "language", "caption_kind",
                "ads_removed_seconds", "ads_removed_snippets", "ad_removal_sources",
                "ad_policy_version", "access_scope",
            ) if key in metadata
        }
        retained_metadata.update({
            "video_id": video_id, "source_alias": f"youtube:{video_id}",
            "content_scope": "full_text", "access_state": "allowed",
            "raw_timed_sha256": digest, "raw_timed_byte_length": len(raw),
            "raw_timed_encoding": "utf-8", "timed_caption_schema": "youtube.timed-caption.v1",
            "timed_caption_identity": f"youtube:{video_id}",
            "cleaning": {key: envelope.get("cleaning", {}).get(key)
                         for key in ("policy_version", "sources")
                         if key in envelope.get("cleaning", {})},
        })
        payload = {
            "schema_version": "capture.v1",
            "submission_id": str(uuid.uuid4()),
            "idempotency_key": f"youtube:{video_id}:{digest}",
            "adapter": "youtube", "adapter_version": "capture-v1",
            "submission_kind": "capture", "correlation_id": str(uuid.uuid4()),
            "source": {"source_id": "youtube", "scope": f"video:{video_id}"},
            "requested_url": url, "canonical_url": url, "final_url": url,
            "metadata": retained_metadata,
            "capture": {
                "capture_type": "supplied_capture",
                "content_base64": base64.b64encode(raw).decode("ascii"),
                "byte_length": len(raw), "sha256": digest,
                "media_type": TIMED_CAPTION_MEDIA_TYPE,
                "retrieval_metadata": {"provider": "youtube", "video_id": video_id},
            },
        }
        return payload, digest, len(raw)

    def submit(self, envelope: dict[str, Any], metadata: dict[str, Any]) -> CaptureReceipt:
        payload, digest, byte_length = self.payload(envelope, metadata)
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(body) > MAX_REQUEST_BYTES:
            raise ValueError("capture request exceeds 8 MB")
        request = urllib.request.Request(
            self.base_url + "/api/captures?receipt_only=true", data=body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.api_token}"},
            method="POST",
        )
        try:
            opener = urllib.request.build_opener(_NoRedirect)
            with opener.open(request, timeout=self.timeout) as response:
                status = response.status
                response_body = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"Mycelium 2 capture request failed (HTTP {exc.code})") from None
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RuntimeError(f"Mycelium 2 capture request failed ({type(exc).__name__})") from None
        if status < 200 or status >= 300:
            raise RuntimeError(f"Mycelium 2 capture request failed (HTTP {status})")
        if len(response_body) > MAX_RESPONSE_BYTES:
            raise RuntimeError("Mycelium 2 response exceeds size limit")
        try:
            result = json.loads(response_body)
            capture = result["capture"]
            capture_id, replayed = capture["id"], capture["replayed"]
        except (ValueError, TypeError, KeyError):
            raise RuntimeError("Mycelium 2 returned an invalid capture receipt") from None
        if (not isinstance(capture_id, str) or not capture_id or len(capture_id) > 256
                or not isinstance(replayed, bool)
                or result.get("sha256") != digest or result.get("byte_length") != byte_length):
            raise RuntimeError("Mycelium 2 returned an invalid capture receipt")
        return CaptureReceipt(ALREADY_PRESENT if replayed else STORED, capture_id, replayed, capture_id)
