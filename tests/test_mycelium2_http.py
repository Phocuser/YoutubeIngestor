import base64
import hashlib
import json

import pytest

from app.capture import build_capture
from app.mycelium2_http import ALREADY_PRESENT, STORED, Mycelium2HTTP, _NoRedirect, capture_bytes


def capture():
    return build_capture(
        "video-1", [("Opening analysis —", 0.0, 1.25), ("Caption with unknown end", 2.0, None)],
        title="A title", channel_id="UC-1", channel_name="Channel",
        published_at="2026-09-18T12:00:00Z", track_id="manual-en", language="en",
        caption_kind="manual", is_public_channel_feed=True,
    )


@pytest.mark.parametrize("replayed,state", [(False, STORED), (True, ALREADY_PRESENT)])
def test_posts_canonical_timed_capture_v1_and_maps_receipt(monkeypatch, replayed, state):
    envelope, metadata = capture()
    expected, digest, length = Mycelium2HTTP.payload(envelope, metadata)
    response_data = json.dumps({"capture": {"id": "stable-capture-id", "replayed": replayed},
                                "sha256": digest, "byte_length": length}).encode()
    observed = {}

    class Response:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self, limit):
            observed["read_limit"] = limit
            return response_data

    def fake_open(request, timeout):
        observed["url"] = request.full_url
        observed["authorization"] = request.get_header("Authorization")
        observed["request"] = json.loads(request.data)
        observed["timeout"] = timeout
        return Response()

    monkeypatch.setattr("app.mycelium2_http.urllib.request.build_opener",
                        lambda *_handlers: SimpleOpener(fake_open))
    receipt = Mycelium2HTTP("https://mycelium.example", "owner-token", 12).submit(envelope, metadata)

    assert observed["url"] == "https://mycelium.example/api/captures?receipt_only=true"
    assert observed["authorization"] == "Bearer owner-token"
    assert observed["timeout"] == 12
    assert observed["request"]["schema_version"] == "capture.v1"
    assert observed["request"]["source"] == {"source_id": "youtube", "scope": "video:video-1"}
    assert observed["request"]["idempotency_key"] == expected["idempotency_key"]
    raw = base64.b64decode(observed["request"]["capture"]["content_base64"], validate=True)
    assert raw == capture_bytes(envelope["raw_timed_captions"])
    decoded = json.loads(raw)
    assert decoded["segments"][1]["start_ms"] == 2000
    assert decoded["segments"][1]["end_ms"] is None
    assert hashlib.sha256(raw).hexdigest() == observed["request"]["capture"]["sha256"] == digest
    assert receipt.state == state and receipt.capture_id == "stable-capture-id"
    assert receipt.receipt_id == "stable-capture-id" and receipt.replayed is replayed
    # Request timing is bounded and response reads stop at the configured cap.
    assert 1 <= observed["timeout"] <= 120
    assert observed["read_limit"] == 1_000_001


def test_http_failure_hides_credentials_and_does_not_return_a_receipt(monkeypatch):
    import urllib.error

    def fail(*_args, **_kwargs):
        raise urllib.error.HTTPError("url", 403, "denied", {}, None)

    monkeypatch.setattr("app.mycelium2_http.urllib.request.build_opener",
                        lambda *_handlers: SimpleOpener(fail))
    with pytest.raises(RuntimeError) as exc:
        envelope, metadata = capture()
        Mycelium2HTTP("https://mycelium.example", "top-secret").submit(envelope, metadata)
    assert "top-secret" not in str(exc.value)
    assert "403" in str(exc.value)


@pytest.mark.asyncio
async def test_http_accepted_receipt_marks_local_outbox_persisted(tmp_path, monkeypatch):
    import asyncio
    from app.config import Settings
    from app.service import CaptionsService
    from app.store import CaptionsStore

    envelope, metadata = capture()
    digest = hashlib.sha256(capture_bytes(envelope["raw_timed_captions"])).hexdigest()
    byte_length = len(capture_bytes(envelope["raw_timed_captions"]))
    response_data = json.dumps({"capture": {"id": "durable-id", "replayed": False},
                                "sha256": digest, "byte_length": byte_length}).encode()

    class Response:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self, _): return response_data

    monkeypatch.setattr("app.mycelium2_http.urllib.request.build_opener",
                        lambda *_handlers: SimpleOpener(lambda *_a, **_k: Response()))
    path = str(tmp_path / "captions.sqlite3")
    store = CaptionsStore(path)
    settings = Settings(database_path=path)
    service = CaptionsService(settings, store, submitter=Mycelium2HTTP("http://fake", "token"))
    store.record_video("video-1", "UC-1", "A title", "2026-09-18T12:00:00Z", True,
                       json.dumps(envelope), json.dumps(metadata))

    assert await service.submit_durable(envelope, video_id="video-1")
    row = store.get_video("video-1")
    assert row["persisted_at"] is not None
    assert row["admission_state"] == STORED
    assert row["admission_receipt_id"] == "durable-id"


def test_invalid_receipt_is_rejected_without_idempotency_false_success(monkeypatch):
    envelope, metadata = capture()

    class Response:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self, _): return b'{"capture":{"id":"stable","replayed":false},"sha256":"bad","byte_length":0}'

    monkeypatch.setattr("app.mycelium2_http.urllib.request.build_opener",
                        lambda *_handlers: SimpleOpener(lambda *_a, **_k: Response()))
    with pytest.raises(RuntimeError, match="invalid capture receipt"):
        Mycelium2HTTP("http://fake", "token").submit(envelope, metadata)


class SimpleOpener:
    def __init__(self, open_fn):
        self.open_fn = open_fn

    def open(self, request, timeout):
        return self.open_fn(request, timeout)


@pytest.mark.parametrize("url", ["", "file:///tmp/api", "https:///path", "https://u:p@host", "https://host/path?x=1"])
def test_rejects_non_origin_or_credential_bearing_base_urls(url):
    with pytest.raises(ValueError, match="MYCELIUM2_URL"):
        Mycelium2HTTP(url, "token")


def test_redirects_are_disabled_to_prevent_bearer_forwarding():
    handler = _NoRedirect()
    assert handler.redirect_request(None, None, 302, "found", {}, "https://other.example/") is None
