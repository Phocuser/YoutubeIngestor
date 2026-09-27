"""HTTP control client for Mycelium managed source jobs."""
from __future__ import annotations

from typing import Any, Mapping, Protocol
from urllib.parse import urlsplit

import httpx

from .discovery_deadline import ControlPlaneTimeout, run_with_deadline

CONTROL_PLANE_TIMEOUT_SECONDS = 5.0


class LeaseLost(RuntimeError):
    """The control plane fenced this worker or its lease expired."""


class SubmissionConflict(RuntimeError):
    """Mycelium rejected a submission because its idempotency identity conflicts."""


class ControlPlaneError(RuntimeError):
    """A non-fencing control-plane failure."""


class LeaseResponseError(ControlPlaneError):
    """The control plane returned an unusable lease response."""


class ControlPlane(Protocol):
    def lease_jobs(self, *, worker_id: str, limit: int, lease_seconds: int) -> list[dict[str, Any]]: ...

    def submit_capture(self, job: Mapping[str, Any], *, worker_id: str, envelope: Mapping[str, Any], metadata: Mapping[str, Any], idempotency_key: str) -> Mapping[str, Any]: ...

    def renew_job(self, job: Mapping[str, Any], *, worker_id: str, lease_seconds: int) -> Mapping[str, Any]: ...

    def complete_job(self, job: Mapping[str, Any], *, worker_id: str, status: str, processed_items: int, error_code: str | None) -> Mapping[str, Any]: ...


def _response_payload(response: httpx.Response) -> Mapping[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise ControlPlaneError(f"control plane returned non-JSON HTTP {response.status_code}") from exc
    if not isinstance(payload, dict):
        raise ControlPlaneError("control plane returned a non-object JSON response")
    return payload


def _error_message(body: Mapping[str, Any]) -> str:
    return str(body.get("detail") or body.get("error") or "request rejected")


_LEASE_CONFLICTS = frozenset({
    "managed source lease is missing or inactive",
    "managed source lease is owned by another worker",
    "managed source lease is expired",
    "job lease is missing, expired, or owned by another worker",
})
_SUBMISSION_CONFLICTS = frozenset({
    "idempotency key is bound to a different canonical payload",
    "idempotency key is bound to a different managed source job",
})


def _lease_fields(job: Mapping[str, Any]) -> tuple[str, str]:
    job_id, lease_token = job.get("job_id"), job.get("lease_token")
    if not isinstance(job_id, str) or not job_id or not isinstance(lease_token, str) or not lease_token:
        raise ValueError("leased job must contain job_id and lease_token")
    return job_id, lease_token


def _validated_base_url(base_url: str) -> str:
    if not isinstance(base_url, str) or not base_url.strip():
        raise ValueError("Mycelium source-control URL is required")
    candidate = base_url.strip()
    try:
        parsed = urlsplit(candidate)
        hostname = parsed.hostname
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("Mycelium source-control URL is malformed") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or hostname is None:
        raise ValueError("Mycelium source-control URL must include an HTTP(S) host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Mycelium source-control URL must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("Mycelium source-control URL must not contain query or fragment")
    if parsed.path not in {"", "/"}:
        raise ValueError("Mycelium source-control URL must not contain a path")
    if parsed.scheme == "http" and hostname.lower().rstrip(".") not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("HTTP source-control URLs are allowed only for local loopback targets")
    return candidate.rstrip("/")


class MyceliumSourceControlClient:
    """Small HTTP client for the released managed YouTube job contract."""

    def __init__(self, base_url: str, token: str, *, timeout: float = CONTROL_PLANE_TIMEOUT_SECONDS, client: httpx.Client | None = None):
        base_url = _validated_base_url(base_url)
        if not token.strip():
            raise ValueError("Mycelium source-control token is required")
        if not 0 < timeout <= CONTROL_PLANE_TIMEOUT_SECONDS:
            raise ValueError(f"control-plane timeout must be between 0 and {CONTROL_PLANE_TIMEOUT_SECONDS} seconds")
        self._timeout = timeout
        self._client = client or httpx.Client(
            base_url=base_url, timeout=timeout,
            headers={"Authorization": f"Bearer {token}"},
        )
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _post(self, path: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        try:
            response = run_with_deadline(
                lambda: self._client.post(path, json=dict(payload)),
                self._timeout,
                timeout_type=ControlPlaneTimeout,
            )
        except ControlPlaneTimeout as exc:
            raise ControlPlaneError("control plane request exceeded its wall-clock deadline") from exc
        except httpx.HTTPError as exc:
            raise ControlPlaneError(f"control plane request failed: {type(exc).__name__}") from exc
        body = _response_payload(response)
        if response.status_code == 409:
            message = _error_message(body)
            if path.endswith("/submit") and message in _SUBMISSION_CONFLICTS:
                raise SubmissionConflict(message)
            if message in _LEASE_CONFLICTS:
                raise LeaseLost(message)
            raise ControlPlaneError(f"control plane HTTP 409: {message}")
        if response.status_code >= 400:
            raise ControlPlaneError(f"control plane HTTP {response.status_code}: {_error_message(body)}")
        return body

    def lease_jobs(self, *, worker_id: str, limit: int, lease_seconds: int) -> list[dict[str, Any]]:
        body = self._post("/api/v1/ingest-control/youtube/jobs/lease", {
            "worker_id": worker_id, "limit": limit, "lease_seconds": lease_seconds,
        })
        jobs = body.get("jobs")
        if not isinstance(jobs, list) or any(not isinstance(job, dict) for job in jobs):
            raise ControlPlaneError("lease response did not contain object jobs")
        if len(jobs) > limit:
            raise LeaseResponseError("lease response exceeded requested limit")
        return jobs

    def submit_capture(self, job: Mapping[str, Any], *, worker_id: str, envelope: Mapping[str, Any], metadata: Mapping[str, Any], idempotency_key: str) -> Mapping[str, Any]:
        from .managed_capture import managed_timed_capture
        job_id, lease_token = _lease_fields(job)
        capture, extra_metadata, requested_url = managed_timed_capture(envelope, metadata)
        return self._post(f"/api/v1/ingest-control/youtube/jobs/{job_id}/submit", {
            "worker_id": worker_id, "lease_token": lease_token,
            "idempotency_key": idempotency_key,
            "requested_url": requested_url, "canonical_url": requested_url, "final_url": requested_url,
            "capture": capture, "metadata": extra_metadata,
        })

    def complete_job(self, job: Mapping[str, Any], *, worker_id: str, status: str, processed_items: int, error_code: str | None) -> Mapping[str, Any]:
        job_id, lease_token = _lease_fields(job)
        return self._post(f"/api/v1/ingest-control/youtube/jobs/{job_id}/complete", {
            "worker_id": worker_id, "lease_token": lease_token,
            "status": status, "processed_items": processed_items, "error_code": error_code,
        })

    def renew_job(self, job: Mapping[str, Any], *, worker_id: str, lease_seconds: int) -> Mapping[str, Any]:
        job_id, lease_token = _lease_fields(job)
        return self._post(f"/api/v1/ingest-control/youtube/jobs/{job_id}/renew", {
            "worker_id": worker_id, "lease_token": lease_token, "lease_seconds": lease_seconds,
        })
