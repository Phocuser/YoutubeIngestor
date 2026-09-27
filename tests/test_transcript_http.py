from unittest.mock import MagicMock, patch

from app.transcript_http import TRANSCRIPT_HTTP_TIMEOUT_SECONDS, TranscriptTimeoutSession


def test_transcript_session_sets_explicit_request_timeout_without_network():
    session = TranscriptTimeoutSession()
    response = MagicMock()
    with patch("requests.Session.request", return_value=response) as request:
        assert session.get("https://example.invalid/captions") is response
        assert request.call_args.kwargs["timeout"] == TRANSCRIPT_HTTP_TIMEOUT_SECONDS
        session.get("https://example.invalid/captions", timeout=None)
        assert request.call_args.kwargs["timeout"] == TRANSCRIPT_HTTP_TIMEOUT_SECONDS
        session.get("https://example.invalid/captions", timeout=2.5)
        assert request.call_args.kwargs["timeout"] == 2.5
    session.close()
