"""HTTP boundary for youtube-transcript-api requests."""
from __future__ import annotations

import requests


TRANSCRIPT_HTTP_TIMEOUT_SECONDS = 15.0


class TranscriptTimeoutSession(requests.Session):
    """Requests session that gives every transcript HTTP call a deadline."""

    def __init__(self, timeout: float = TRANSCRIPT_HTTP_TIMEOUT_SECONDS):
        super().__init__()
        self.timeout = timeout

    def request(self, method, url, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = self.timeout
        return super().request(method, url, **kwargs)
