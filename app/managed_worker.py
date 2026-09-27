"""Bounded YouTube worker for Mycelium managed source jobs.

The worker owns provider acquisition and the HTTP control-plane calls. It does
not claim that Mycelium indexed or materialized a capture: a successful item
means only that the managed submission returned durable evidence.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import Any, Mapping

from .capture import build_capture
from .discovery_deadline import DiscoveryTimeout, MetadataTimeout, TranscriptTimeout, run_with_deadline
from .managed_capture import managed_timed_capture
from .managed_acquisition import DefaultYouTubeAcquisition
from .managed_models import Acquisition, ItemOutcome, JobOutcome, error_code as _error_code, video_value as _video_value
from .source_control import CONTROL_PLANE_TIMEOUT_SECONDS, ControlPlane, ControlPlaneError, LeaseLost, LeaseResponseError, MyceliumSourceControlClient, SubmissionConflict, _lease_fields

MIN_LEASE_SECONDS = 60
DISCOVERY_TIMEOUT_CAP_SECONDS = 30.0
DISCOVERY_LEASE_FRACTION = 0.25
TRANSCRIPT_TIMEOUT_CAP_SECONDS = 20.0
METADATA_TIMEOUT_CAP_SECONDS = 20.0


class LeaseBudgetExceeded(RuntimeError):
    """The old lease is too close to expiry for another control request."""


class ManagedYouTubeWorker:
    def __init__(self, control: ControlPlane, *, worker_id: str, acquisition: Acquisition | None = None, lease_seconds: int = 120, batch_limit: int = 1, clock: Any = time.monotonic, utc_now: Any = lambda: datetime.now(timezone.utc)):
        if not isinstance(worker_id, str) or not worker_id:
            raise ValueError("worker_id is required")
        if not MIN_LEASE_SECONDS <= lease_seconds <= 600:
            raise ValueError(f"lease_seconds must be between {MIN_LEASE_SECONDS} and 600")
        if isinstance(batch_limit, bool) or not isinstance(batch_limit, int) or batch_limit != 1:
            raise ValueError("batch_limit must be 1; process one leased job per pass")
        self.control, self.worker_id = control, worker_id
        self.acquisition, self.lease_seconds, self.batch_limit = acquisition or DefaultYouTubeAcquisition(), lease_seconds, batch_limit
        self.clock, self.utc_now = clock, utc_now

    def run_once(self) -> list[JobOutcome]:
        try:
            jobs = self.control.lease_jobs(worker_id=self.worker_id, limit=self.batch_limit, lease_seconds=self.lease_seconds)
        except LeaseResponseError:
            return [JobOutcome("<lease-response>", "control_error", 0, "LEASE_RESPONSE_OVER_LIMIT", False)]
        except ControlPlaneError:
            return [JobOutcome("<lease-response>", "control_error", 0, "CONTROL_PLANE_ERROR", False)]
        if not isinstance(jobs, list) or len(jobs) > self.batch_limit:
            return [JobOutcome("<lease-response>", "control_error", 0, "LEASE_RESPONSE_OVER_LIMIT", False)]
        outcomes = []
        for job in jobs:
            try:
                outcomes.append(self.process_job(job))
            except ValueError as exc:
                outcomes.append(self._invalid_job(job, _error_code(str(exc))))
        return outcomes

    def _invalid_job(self, job: Mapping[str, Any], reason: str) -> JobOutcome:
        job_id = str(job.get("job_id") or "<invalid>")
        outcomes: list[ItemOutcome] = []
        if job.get("adapter") == "youtube":
            try:
                _lease_fields(job)
                return self._complete(job, "failed", 0, reason, outcomes)
            except (ValueError, LeaseLost, ControlPlaneError):
                pass
        return JobOutcome(job_id, "invalid_job", 0, reason, False, outcomes)

    @staticmethod
    def _publication_datetime(value: Any) -> datetime | None:
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(timezone.utc)

    def _date_reason(self, published_at: Any, *, now: datetime, backfill_days: int) -> str | None:
        published = self._publication_datetime(published_at)
        if published is None:
            return "PUBLISHED_AT_UNKNOWN"
        if published < now - timedelta(days=backfill_days) or published > now:
            return "PUBLISHED_AT_OUT_OF_WINDOW"
        return None

    def _renew_if_due(self, job: Mapping[str, Any], last_renewed: float) -> float:
        """Renew before work if discovery or an item consumed half the lease."""
        elapsed = self.clock() - last_renewed
        if elapsed < self.lease_seconds / 2:
            return last_renewed
        # The renewal request itself must finish before the old lease expires.
        # The renewed lease then covers the submit and completion calls.
        if elapsed + CONTROL_PLANE_TIMEOUT_SECONDS >= self.lease_seconds:
            raise LeaseBudgetExceeded("lease lacks time for a safe renewal")
        self.control.renew_job(job, worker_id=self.worker_id, lease_seconds=self.lease_seconds)
        return self.clock()

    def _discovery_budget(self) -> float:
        return min(DISCOVERY_TIMEOUT_CAP_SECONDS, self.lease_seconds * DISCOVERY_LEASE_FRACTION)

    def _transcript_budget(self) -> float:
        return min(TRANSCRIPT_TIMEOUT_CAP_SECONDS, self.lease_seconds * DISCOVERY_LEASE_FRACTION)

    def _metadata_budget(self) -> float:
        return min(METADATA_TIMEOUT_CAP_SECONDS, self.lease_seconds * DISCOVERY_LEASE_FRACTION)

    def process_job(self, job: Mapping[str, Any]) -> JobOutcome:
        job_id, _ = _lease_fields(job)
        if job.get("adapter") != "youtube" or job.get("state") != "leased":
            raise ValueError("worker received a non-youtube or non-leased job")
        resource_ref, max_items, backfill_days = job.get("resource_ref"), job.get("max_items"), job.get("backfill_days")
        if not isinstance(resource_ref, str) or not resource_ref.strip():
            raise ValueError("leased YouTube job lacks resource_ref")
        if not isinstance(max_items, int) or isinstance(max_items, bool) or not 1 <= max_items <= 1000:
            raise ValueError("leased YouTube job has invalid max_items")
        if not isinstance(backfill_days, int) or isinstance(backfill_days, bool) or not 1 <= backfill_days <= 90:
            raise ValueError("leased YouTube job has invalid backfill_days")
        now = self.utc_now()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("UTC clock must return an aware datetime")
        now = now.astimezone(timezone.utc)
        # The lease clock includes provider discovery. A slow listing must not
        # consume the lease silently before the first metadata request.
        last_renewed = self.clock()
        try:
            def discover_bounded() -> tuple[bool, list[Any]]:
                discovered = self.acquisition.list_videos(resource_ref, limit=max_items)
                return bool(getattr(discovered, "truncated", False)), list(islice(iter(discovered), max_items + 1))

            discovery_truncated, videos = run_with_deadline(discover_bounded, self._discovery_budget())
        except DiscoveryTimeout:
            return self._complete(job, "failed", 0, "DISCOVERY_TIMEOUT", [], last_renewed=last_renewed)
        except Exception:
            try:
                last_renewed = self._renew_if_due(job, last_renewed)
            except LeaseLost:
                return JobOutcome(job_id, "lease_lost", 0, "LEASE_LOST", False, [])
            except ControlPlaneError:
                return JobOutcome(job_id, "control_error", 0, "CONTROL_PLANE_ERROR", False, [])
            except LeaseBudgetExceeded:
                return JobOutcome(job_id, "lease_lost", 0, "LEASE_BUDGET_EXHAUSTED", False, [])
            return self._complete(job, "failed", 0, "DISCOVERY_FAILED", [], last_renewed=last_renewed)

        try:
            last_renewed = self._renew_if_due(job, last_renewed)
        except LeaseLost:
            return JobOutcome(job_id, "lease_lost", 0, "LEASE_LOST", False, [])
        except ControlPlaneError:
            return JobOutcome(job_id, "control_error", 0, "CONTROL_PLANE_ERROR", False, [])
        except LeaseBudgetExceeded:
            return JobOutcome(job_id, "lease_lost", 0, "LEASE_BUDGET_EXHAUSTED", False, [])

        videos = list(videos)
        if not videos:
            return self._complete(job, "failed", 0, "NO_VIDEOS_FOUND", [], last_renewed=last_renewed)
        outcomes, processed = [], 0
        first_error = "ITEM_LIMIT_REACHED" if len(videos) > max_items or discovery_truncated else None
        seen_ids: set[str] = set()
        duplicates_skipped = 0
        for video in videos[:max_items]:
            try:
                last_renewed = self._renew_if_due(job, last_renewed)
            except LeaseLost:
                return JobOutcome(job_id, "lease_lost", processed, "LEASE_LOST", False, outcomes)
            except ControlPlaneError:
                return JobOutcome(job_id, "control_error", processed, "CONTROL_PLANE_ERROR", False, outcomes)
            except LeaseBudgetExceeded:
                return JobOutcome(job_id, "lease_lost", processed, "LEASE_BUDGET_EXHAUSTED", False, outcomes)
            video_id = _video_value(video, "video_id")
            if not isinstance(video_id, str) or not video_id.strip():
                first_error = first_error or "VIDEO_ID_INVALID"
                continue
            video_id = video_id.strip()
            if video_id in seen_ids:
                duplicates_skipped += 1
                outcomes.append(ItemOutcome(video_id, False, "DUPLICATE_VIDEO_ID"))
                continue
            seen_ids.add(video_id)
            try:
                try:
                    meta = run_with_deadline(
                        lambda: self.acquisition.fetch_meta(video_id),
                        self._metadata_budget(),
                        timeout_type=MetadataTimeout,
                    )
                except MetadataTimeout:
                    first_error = first_error or "METADATA_TIMEOUT"
                    outcomes.append(ItemOutcome(video_id, False, "METADATA_TIMEOUT"))
                    return self._complete(
                        job,
                        "partial" if processed else "failed",
                        processed,
                        first_error,
                        outcomes,
                        duplicates_skipped,
                        last_renewed,
                    )
                published_at = _video_value(video, "published_at") or _video_value(meta, "published_at")
                date_reason = self._date_reason(published_at, now=now, backfill_days=backfill_days)
                if date_reason is not None:
                    first_error = first_error or date_reason
                    outcomes.append(ItemOutcome(video_id, False, date_reason))
                    continue
                try:
                    transcript = run_with_deadline(
                        lambda: self.acquisition.fetch_transcript(video_id),
                        self._transcript_budget(),
                        timeout_type=TranscriptTimeout,
                    )
                except TranscriptTimeout:
                    first_error = first_error or "TRANSCRIPT_TIMEOUT"
                    outcomes.append(ItemOutcome(video_id, False, "TRANSCRIPT_TIMEOUT"))
                    return self._complete(
                        job,
                        "partial" if processed else "failed",
                        processed,
                        first_error,
                        outcomes,
                        duplicates_skipped,
                        last_renewed,
                    )
                if not isinstance(transcript, (list, tuple)) or not transcript:
                    raise ValueError("CAPTIONS_MISSING")
                envelope, metadata = build_capture(
                    video_id, transcript, title=_video_value(meta, "title", _video_value(video, "title", "")) or "",
                    channel_id=_video_value(meta, "channel_id", ""), channel_name=_video_value(meta, "channel_name", "YouTube"),
                    published_at=published_at, duration=_video_value(meta, "duration"), is_public_channel_feed=False,
                )
                capture, submit_metadata, _ = managed_timed_capture(envelope, metadata)
                # Recheck immediately before the control-plane write. Provider
                # work may have consumed the half-lease threshold since the
                # per-item check at loop entry.
                try:
                    last_renewed = self._renew_if_due(job, last_renewed)
                except LeaseLost:
                    return JobOutcome(job_id, "lease_lost", processed, "LEASE_LOST", False, outcomes)
                except ControlPlaneError:
                    return JobOutcome(job_id, "control_error", processed, "CONTROL_PLANE_ERROR", False, outcomes)
                except LeaseBudgetExceeded:
                    return JobOutcome(job_id, "lease_lost", processed, "LEASE_BUDGET_EXHAUSTED", False, outcomes)
                receipt = self.control.submit_capture(job, worker_id=self.worker_id, envelope=envelope, metadata=submit_metadata, idempotency_key=f"youtube:{video_id}:{capture['sha256']}")
                if receipt.get("evidence_durable") is not True:
                    raise ValueError("CAPTURE_NOT_DURABLE")
                processed += 1
                outcomes.append(ItemOutcome(video_id, True))
            except LeaseLost:
                return JobOutcome(job_id, "lease_lost", processed, "LEASE_LOST", False, outcomes)
            except TranscriptTimeout:
                first_error = first_error or "TRANSCRIPT_TIMEOUT"
                outcomes.append(ItemOutcome(video_id, False, "TRANSCRIPT_TIMEOUT"))
                return self._complete(job, "partial" if processed else "failed", processed, first_error, outcomes, duplicates_skipped, last_renewed)
            except MetadataTimeout:
                first_error = first_error or "METADATA_TIMEOUT"
                outcomes.append(ItemOutcome(video_id, False, "METADATA_TIMEOUT"))
                return self._complete(job, "partial" if processed else "failed", processed, first_error, outcomes, duplicates_skipped, last_renewed)
            except DiscoveryTimeout:
                first_error = first_error or "TRANSCRIPT_TIMEOUT"
                outcomes.append(ItemOutcome(video_id, False, "TRANSCRIPT_TIMEOUT"))
                return self._complete(job, "partial" if processed else "failed", processed, first_error, outcomes, duplicates_skipped, last_renewed)
            except SubmissionConflict:
                first_error = first_error or "SUBMISSION_CONFLICT"
                outcomes.append(ItemOutcome(video_id, False, "SUBMISSION_CONFLICT"))
                return self._complete(job, "partial" if processed else "failed", processed, first_error, outcomes, duplicates_skipped, last_renewed)
            except ControlPlaneError:
                return JobOutcome(job_id, "control_error", processed, "CONTROL_PLANE_ERROR", False, outcomes)
            except ValueError as exc:
                code = _error_code(str(exc))
                first_error = first_error or code
                outcomes.append(ItemOutcome(video_id, False, code))
            except Exception:
                first_error = first_error or "ITEM_FAILED"
                outcomes.append(ItemOutcome(video_id, False, "ITEM_FAILED"))

        if first_error is None:
            return self._complete(job, "succeeded", processed, None, outcomes, duplicates_skipped, last_renewed)
        return self._complete(job, "partial" if processed else "failed", processed, first_error, outcomes, duplicates_skipped, last_renewed)

    def _complete(
        self,
        job: Mapping[str, Any],
        status: str,
        processed: int,
        stop_reason: str | None,
        outcomes: list[ItemOutcome],
        duplicates_skipped: int = 0,
        last_renewed: float | None = None,
    ) -> JobOutcome:
        if last_renewed is not None:
            try:
                self._renew_if_due(job, last_renewed)
            except LeaseLost:
                return JobOutcome(str(job["job_id"]), "lease_lost", processed, "LEASE_LOST", False, outcomes)
            except ControlPlaneError:
                return JobOutcome(str(job["job_id"]), "control_error", processed, "CONTROL_PLANE_ERROR", False, outcomes)
            except LeaseBudgetExceeded:
                return JobOutcome(str(job["job_id"]), "lease_lost", processed, "LEASE_BUDGET_EXHAUSTED", False, outcomes)
        try:
            self.control.complete_job(job, worker_id=self.worker_id, status=status, processed_items=processed, error_code=stop_reason)
        except LeaseLost:
            return JobOutcome(str(job["job_id"]), "lease_lost", processed, "LEASE_LOST", False, outcomes)
        except ControlPlaneError:
            return JobOutcome(str(job["job_id"]), "control_error", processed, "CONTROL_PLANE_ERROR", False, outcomes)
        return JobOutcome(str(job["job_id"]), status, processed, stop_reason, True, outcomes, duplicates_skipped)
