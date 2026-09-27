import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from app.managed_worker import (ControlPlaneError, LeaseLost, LeaseResponseError, ManagedYouTubeWorker,
                                DefaultYouTubeAcquisition,
                                SubmissionConflict,
                                MyceliumSourceControlClient, managed_timed_capture)


TRACK = [
    {"text": "Opening analysis", "start": 0.0, "duration": 2.0},
    {"text": "The evidence remains typed.", "start": 2.0, "duration": 3.0},
]


@dataclass
class Video:
    video_id: str
    title: str = "Video"
    published_at: str | None = None


@dataclass
class Meta:
    video_id: str
    title: str = "Video"
    channel_id: str = "UC123"
    channel_name: str = "Channel"
    published_at: str = "2026-09-27T00:00:00Z"
    duration: int = 60


class FakeAcquisition:
    def __init__(self, videos=None, transcripts=None, fail_discovery=False, meta_published_at=None):
        self.videos = [Video("vid-1")] if videos is None else videos
        self.transcripts = transcripts or {"vid-1": TRACK}
        self.fail_discovery = fail_discovery
        self.meta_published_at = meta_published_at or {}
        self.calls = []

    def list_videos(self, resource_ref, *, limit):
        self.calls.append(("list", resource_ref, limit))
        if self.fail_discovery:
            raise RuntimeError("provider disabled")
        return self.videos

    def fetch_meta(self, video_id):
        self.calls.append(("meta", video_id))
        return Meta(video_id, published_at=self.meta_published_at.get(video_id, "2026-09-27T00:00:00Z"))

    def fetch_transcript(self, video_id):
        self.calls.append(("captions", video_id))
        return self.transcripts.get(video_id)


class FakeControl:
    def __init__(self, jobs=None, receipts=None, lease_lost_on_submit=False, lease_lost_on_complete=False, control_error_on_submit=False, lease_lost_on_renew=False, submission_conflict=False):
        self.jobs = jobs or [{"job_id": "job-1", "adapter": "youtube", "state": "leased", "lease_token": "token-1", "resource_ref": "@channel", "max_items": 10, "backfill_days": 30}]
        self.receipts = receipts or [{"evidence_durable": True}]
        self.lease_lost_on_submit, self.lease_lost_on_complete = lease_lost_on_submit, lease_lost_on_complete
        self.control_error_on_submit = control_error_on_submit
        self.lease_lost_on_renew = lease_lost_on_renew
        self.submission_conflict = submission_conflict
        self.submissions, self.completions = [], []
        self.renewals = []

    def lease_jobs(self, **kwargs):
        return self.jobs

    def submit_capture(self, job, **kwargs):
        if self.lease_lost_on_submit:
            raise LeaseLost("stale lease")
        if self.control_error_on_submit:
            raise ControlPlaneError("uncertain response")
        if self.submission_conflict:
            raise SubmissionConflict("idempotency conflict")
        self.submissions.append(kwargs)
        return self.receipts[min(len(self.submissions) - 1, len(self.receipts) - 1)]

    def complete_job(self, job, **kwargs):
        if self.lease_lost_on_complete:
            raise LeaseLost("stale lease")
        self.completions.append(kwargs)
        return {"state": kwargs["status"]}

    def renew_job(self, job, **kwargs):
        if self.lease_lost_on_renew:
            raise LeaseLost("stale lease")
        self.renewals.append(kwargs)
        return {"job_id": job["job_id"]}


def test_wire_capture_is_exact_typed_bytes_and_digest():
    from app.capture import build_capture
    envelope, metadata = build_capture("vid-1", TRACK)
    capture, output_metadata, url = managed_timed_capture(envelope, metadata)
    raw = base64.b64decode(capture["content_base64"])
    assert capture["byte_length"] == len(raw)
    assert capture["sha256"]
    assert output_metadata["timed_caption_schema"] == "youtube.timed-caption.v1"
    assert url == "https://www.youtube.com/watch?v=vid-1"


def test_worker_submits_durable_capture_then_completes_success():
    control = FakeControl()
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=FakeAcquisition()).run_once()[0]
    assert outcome.status == "succeeded" and outcome.processed_items == 1 and outcome.completed
    assert control.completions == [{"worker_id": "yt-worker", "status": "succeeded", "processed_items": 1, "error_code": None}]
    assert control.submissions[0]["idempotency_key"].startswith("youtube:vid-1:")


def test_missing_captions_never_claims_success():
    control = FakeControl()
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=FakeAcquisition(transcripts={"vid-1": None})).run_once()[0]
    assert outcome.status == "failed" and outcome.stop_reason == "CAPTIONS_MISSING"
    assert control.submissions == []
    assert control.completions[0]["status"] == "failed"


def test_quarantined_or_non_durable_receipt_is_failed():
    control = FakeControl(receipts=[{"evidence_durable": False}])
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=FakeAcquisition()).run_once()[0]
    assert outcome.status == "failed" and outcome.stop_reason == "CAPTURE_NOT_DURABLE"
    assert control.completions[0]["error_code"] == "CAPTURE_NOT_DURABLE"


def test_stale_submit_lease_is_not_finalized_by_old_worker():
    control = FakeControl(lease_lost_on_submit=True)
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=FakeAcquisition()).run_once()[0]
    assert outcome.status == "lease_lost" and not outcome.completed
    assert control.completions == []


def test_stale_completion_lease_is_reported_without_false_success():
    control = FakeControl(lease_lost_on_complete=True)
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=FakeAcquisition()).run_once()[0]
    assert outcome.status == "lease_lost" and outcome.stop_reason == "LEASE_LOST"
    assert outcome.processed_items == 1 and not outcome.completed


def test_partial_batch_reports_only_durable_items():
    videos = [Video("vid-1"), Video("vid-2")]
    acquisition = FakeAcquisition(videos=videos, transcripts={"vid-1": TRACK, "vid-2": None})
    control = FakeControl()
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=acquisition).run_once()[0]
    assert outcome.status == "partial" and outcome.processed_items == 1
    assert control.completions[0]["status"] == "partial"
    assert control.completions[0]["processed_items"] == 1


def test_backfill_window_skips_unknown_malformed_old_and_future_publication_dates():
    videos = [
        Video("recent", title="Recent"),
        Video("old", title="Old",),
        Video("future", title="Future"),
        Video("malformed", title="Malformed"),
        Video("unknown", title="Unknown"),
    ]
    videos[0].published_at = "2026-09-25T12:00:00Z"
    videos[1].published_at = "2026-08-01T12:00:00Z"
    videos[2].published_at = "2026-09-28T12:00:00Z"
    videos[3].published_at = "not-a-timestamp"
    videos[4].published_at = None
    acquisition = FakeAcquisition(
        videos=videos,
        transcripts={"recent": TRACK, "old": TRACK, "future": TRACK, "malformed": TRACK, "unknown": TRACK},
        meta_published_at={"unknown": ""},
    )
    control = FakeControl(jobs=[{"job_id": "job-1", "adapter": "youtube", "state": "leased", "lease_token": "token-1", "resource_ref": "@channel", "max_items": 10, "backfill_days": 7}])
    outcome = ManagedYouTubeWorker(
        control,
        worker_id="yt-worker",
        acquisition=acquisition,
        utc_now=lambda: datetime(2026, 9, 27, 12, tzinfo=timezone.utc),
    ).run_once()[0]
    assert outcome.status == "partial" and outcome.processed_items == 1
    assert [call[1] for call in acquisition.calls if call[0] == "captions"] == ["recent"]
    assert {item.stop_reason for item in outcome.item_outcomes if not item.submitted} == {
        "PUBLISHED_AT_OUT_OF_WINDOW", "PUBLISHED_AT_UNKNOWN"
    }


def test_submission_conflict_is_not_misclassified_as_lease_loss():
    control = FakeControl(submission_conflict=True)
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=FakeAcquisition()).run_once()[0]
    assert outcome.status == "failed"
    assert outcome.stop_reason == "SUBMISSION_CONFLICT"
    assert control.completions[0]["error_code"] == "SUBMISSION_CONFLICT"


def test_partial_typed_track_is_rejected_before_submission():
    from app.capture import build_capture
    envelope, metadata = build_capture("vid-1", TRACK)
    envelope["raw_timed_captions"]["coverage_state"] = "partial"
    try:
        managed_timed_capture(envelope, metadata)
    except ValueError as exc:
        assert "partial or unknown" in str(exc)
    else:
        raise AssertionError("partial caption coverage was accepted")


def test_discovered_video_ids_are_deduplicated_before_provider_fetch():
    videos = [Video("vid-1"), Video("vid-1"), Video("vid-2")]
    acquisition = FakeAcquisition(videos=videos, transcripts={"vid-1": TRACK, "vid-2": TRACK})
    control = FakeControl()
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=acquisition).run_once()[0]
    assert outcome.status == "succeeded" and outcome.processed_items == 2
    assert outcome.duplicates_skipped == 1
    assert [call[1] for call in acquisition.calls if call[0] == "meta"] == ["vid-1", "vid-2"]


def test_duplicate_segment_ids_are_rejected_before_submission():
    from app.capture import build_capture
    envelope, metadata = build_capture("vid-1", TRACK)
    envelope["raw_timed_captions"]["segments"][1]["segment_id"] = envelope["raw_timed_captions"]["segments"][0]["segment_id"]
    try:
        managed_timed_capture(envelope, metadata)
    except ValueError as exc:
        assert str(exc) == "CAPTURE_INVALID"
    else:
        raise AssertionError("duplicate segment IDs were accepted")


def test_validation_failure_uses_contract_safe_error_code():
    control = FakeControl()
    acquisition = FakeAcquisition(transcripts={"vid-1": [{"text": "", "start": 0.0, "duration": 1.0}]})
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=acquisition).run_once()[0]
    assert outcome.status == "failed"
    assert outcome.stop_reason == "CAPTURE_INVALID"
    assert control.completions[0]["error_code"] == "CAPTURE_INVALID"


def test_uncertain_control_plane_result_is_left_for_reconciliation():
    control = FakeControl(control_error_on_submit=True)
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=FakeAcquisition()).run_once()[0]
    assert outcome.status == "control_error" and not outcome.completed
    assert outcome.stop_reason == "CONTROL_PLANE_ERROR"
    assert control.completions == []


def test_empty_discovery_has_explicit_stop_reason():
    control = FakeControl()
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=FakeAcquisition(videos=[])).run_once()[0]
    assert outcome.status == "failed" and outcome.stop_reason == "NO_VIDEOS_FOUND"
    assert control.completions[0]["error_code"] == "NO_VIDEOS_FOUND"


def test_renewal_happens_between_slow_items():
    control = FakeControl()
    acquisition = FakeAcquisition(videos=[Video("vid-1"), Video("vid-2")], transcripts={"vid-1": TRACK, "vid-2": TRACK})
    ticks = iter([0.0, 0.0, 61.0, 61.0, 61.0])
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=acquisition, lease_seconds=120, clock=lambda: next(ticks)).run_once()[0]
    assert outcome.status == "succeeded"
    assert len(control.renewals) == 1


def test_discovery_latency_renews_before_first_item():
    class FakeClock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = FakeClock()

    class SlowDiscovery(FakeAcquisition):
        def list_videos(self, resource_ref, *, limit):
            clock.value = 61.0
            return super().list_videos(resource_ref, limit=limit)

    control = FakeControl()
    outcome = ManagedYouTubeWorker(
        control,
        worker_id="yt-worker",
        acquisition=SlowDiscovery(),
        lease_seconds=120,
        clock=clock,
    ).run_once()[0]
    assert outcome.status == "succeeded"
    assert len(control.renewals) == 1


def test_worker_rejects_multi_job_batch_limit():
    try:
        ManagedYouTubeWorker(FakeControl(), worker_id="yt-worker", batch_limit=2)
    except ValueError as exc:
        assert str(exc) == "batch_limit must be 1; process one leased job per pass"
    else:
        raise AssertionError("worker accepted a multi-job batch limit")


def test_invalid_single_job_is_completed_without_acquisition():
    control = FakeControl(jobs=[{
        "job_id": "bad", "adapter": "youtube", "state": "leased", "lease_token": "token-bad",
        "resource_ref": "", "max_items": 10, "backfill_days": 30,
    }])
    outcomes = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=FakeAcquisition()).run_once()
    assert [item.job_id for item in outcomes] == ["bad"]
    assert outcomes[0].status == "failed" and outcomes[0].stop_reason == "CAPTURE_INVALID"
    assert control.completions[0]["status"] == "failed"


def test_over_limit_lease_response_fails_closed_without_processing():
    class OverLimitControl(FakeControl):
        def lease_jobs(self, **kwargs):
            return [
                {"job_id": "job-1", "adapter": "youtube", "state": "leased", "lease_token": "token-1", "resource_ref": "@channel", "max_items": 1, "backfill_days": 30},
                {"job_id": "job-2", "adapter": "youtube", "state": "leased", "lease_token": "token-2", "resource_ref": "@channel", "max_items": 1, "backfill_days": 30},
            ]

    acquisition = FakeAcquisition()
    control = OverLimitControl()
    outcomes = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=acquisition).run_once()
    assert len(outcomes) == 1
    assert outcomes[0].status == "control_error"
    assert outcomes[0].stop_reason == "LEASE_RESPONSE_OVER_LIMIT"
    assert not outcomes[0].completed
    assert acquisition.calls == []
    assert control.submissions == []
    assert control.completions == []


def test_discovery_over_bound_is_truncated_and_not_called_complete():
    videos = [Video(f"vid-{index}") for index in range(3)]
    acquisition = FakeAcquisition(videos=videos, transcripts={video.video_id: TRACK for video in videos})
    control = FakeControl(jobs=[{"job_id": "job-1", "adapter": "youtube", "state": "leased", "lease_token": "token-1", "resource_ref": "@channel", "max_items": 2, "backfill_days": 30}])
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=acquisition).run_once()[0]
    assert outcome.status == "partial" and outcome.processed_items == 2
    assert outcome.stop_reason == "ITEM_LIMIT_REACHED"


def test_default_acquisition_requests_one_sentinel_item(monkeypatch):
    calls = []
    monkeypatch.setattr("app.managed_acquisition.list_channel_videos", lambda resource_ref, limit: calls.append((resource_ref, limit)) or [])
    list(DefaultYouTubeAcquisition().list_videos("@channel", limit=2))
    assert calls == [("@channel", 3)]


def test_default_acquisition_extra_item_is_reported_and_not_submitted(monkeypatch):
    videos = [Video("vid-1"), Video("vid-2"), Video("vid-3")]
    monkeypatch.setattr("app.managed_acquisition.list_channel_videos", lambda resource_ref, limit: videos[:limit])
    acquisition = DefaultYouTubeAcquisition()
    acquisition.fetch_meta = lambda video_id: Meta(video_id)
    acquisition.fetch_transcript = lambda video_id: TRACK
    control = FakeControl(jobs=[{"job_id": "job-1", "adapter": "youtube", "state": "leased", "lease_token": "token-1", "resource_ref": "@channel", "max_items": 2, "backfill_days": 30}])
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=acquisition).run_once()[0]
    assert outcome.status == "partial" and outcome.stop_reason == "ITEM_LIMIT_REACHED"
    assert outcome.processed_items == 2
    assert [call["metadata"]["video_id"] for call in control.submissions] == ["vid-1", "vid-2"]


def test_truncated_filtered_discovery_is_not_reported_as_complete():
    from app.channel_videos import VideoDiscovery

    videos = VideoDiscovery(
        [Video("vid-1"), Video("vid-2")],
        truncated=True,
    )
    acquisition = FakeAcquisition(videos=videos, transcripts={"vid-1": TRACK, "vid-2": TRACK})
    control = FakeControl(jobs=[{"job_id": "job-1", "adapter": "youtube", "state": "leased", "lease_token": "token-1", "resource_ref": "@channel", "max_items": 2, "backfill_days": 30}])
    outcome = ManagedYouTubeWorker(control, worker_id="yt-worker", acquisition=acquisition).run_once()[0]
    assert outcome.status == "partial"
    assert outcome.stop_reason == "ITEM_LIMIT_REACHED"
    assert outcome.processed_items == 2


def test_http_client_uses_released_routes_and_exact_capture_contract():
    seen = []

    def handler(request):
        seen.append(request)
        body = json.loads(request.content)
        if request.url.path.endswith("/jobs/lease"):
            return httpx.Response(200, json={"jobs": []})
        if request.url.path.endswith("/submit"):
            raw = base64.b64decode(body["capture"]["content_base64"])
            typed = json.loads(raw)
            assert body["capture"]["byte_length"] == len(raw)
            assert body["capture"]["sha256"]
            assert typed["schema_version"] == "youtube.timed-caption.v1"
            return httpx.Response(200, json={"evidence_durable": True, "state": "queued"})
        return httpx.Response(200, json={"state": "succeeded"})

    transport = httpx.MockTransport(handler)
    client = httpx.Client(base_url="http://127.0.0.1", transport=transport, headers={"Authorization": "Bearer source-token"})
    control = MyceliumSourceControlClient("http://127.0.0.1", "source-token", client=client)
    job = {"job_id": "job-1", "lease_token": "token-1"}
    from app.capture import build_capture
    envelope, metadata = build_capture("vid-1", TRACK)
    control.lease_jobs(worker_id="worker", limit=1, lease_seconds=10)
    control.submit_capture(job, worker_id="worker", envelope=envelope, metadata=metadata, idempotency_key="youtube:vid-1:digest")
    control.complete_job(job, worker_id="worker", status="succeeded", processed_items=1, error_code=None)
    assert [request.url.path for request in seen] == [
        "/api/v1/ingest-control/youtube/jobs/lease",
        "/api/v1/ingest-control/youtube/jobs/job-1/submit",
        "/api/v1/ingest-control/youtube/jobs/job-1/complete",
    ]
    assert all(request.headers["authorization"] == "Bearer source-token" for request in seen)


def test_http_409_error_envelopes_distinguish_lease_and_submission_conflicts():
    messages = {
        "lease": "managed source lease is expired",
        "submission": "idempotency key is bound to a different canonical payload",
        "unknown": "some other conflict",
    }
    for kind, message in messages.items():
        client = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(lambda request, message=message: httpx.Response(409, json={"error": message, "code": "API_ERROR"})),
        )
        control = MyceliumSourceControlClient("http://127.0.0.1", "source-token", client=client)
        try:
            control._post("/api/v1/ingest-control/youtube/jobs/job-1/submit", {})
        except LeaseLost:
            assert kind == "lease"
        except SubmissionConflict:
            assert kind == "submission"
        except ControlPlaneError:
            assert kind == "unknown"
        else:
            raise AssertionError("409 response was accepted")
        client.close()


def test_http_client_rejects_over_limit_lease_response():
    client = httpx.Client(
        base_url="http://127.0.0.1",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"jobs": [{"job_id": "1"}, {"job_id": "2"}]})),
    )
    control = MyceliumSourceControlClient("http://127.0.0.1", "source-token", client=client)
    try:
        control.lease_jobs(worker_id="worker", limit=1, lease_seconds=10)
    except LeaseResponseError as exc:
        assert str(exc) == "lease response exceeded requested limit"
    else:
        raise AssertionError("over-limit lease response was accepted")
    client.close()


def test_source_control_url_requires_tls_for_remote_hosts_and_rejects_ambiguous_urls():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"jobs": []}))
    for url in ("http://localhost", "http://127.0.0.1:18000", "http://[::1]:18000", "https://mycelium.example"):
        client = httpx.Client(base_url=url, transport=transport)
        control = MyceliumSourceControlClient(url, "source-token", client=client)
        control.close()
        client.close()

    for url in (
        "http://mycelium.example",
        "https://user:secret@mycelium.example",
        "https://mycelium.example/path",
        "https://mycelium.example?token=secret",
        "https://mycelium.example#fragment",
        "not-a-url",
        "https://",
    ):
        try:
            MyceliumSourceControlClient(url, "source-token")
        except ValueError:
            pass
        else:
            raise AssertionError(f"unsafe or malformed URL was accepted: {url}")


def test_one_shot_cli_requires_explicit_control_configuration(capsys):
    from app import managed_worker_cli
    try:
        managed_worker_cli.main(["--worker-id", "worker"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("CLI accepted missing URL and token")
    assert "--mycelium-url" in capsys.readouterr().err
    assert "--source-token" not in managed_worker_cli.build_parser().format_help()

    result = managed_worker_cli.main(["--mycelium-url", "http://staging.invalid", "--worker-id", "worker"])
    assert result == 2
    assert "MYCELIUM_YOUTUBE_SOURCE_TOKEN" in capsys.readouterr().err

    result = managed_worker_cli.main([
        "--mycelium-url", "http://staging.invalid", "--worker-id", "worker", "--batch-limit", "2",
    ])
    assert result == 2
    assert "--batch-limit must be 1" in capsys.readouterr().err


def test_one_shot_cli_runs_once_and_closes_client(monkeypatch, capsys):
    from app import managed_worker_cli
    from app.managed_worker import JobOutcome

    calls = {}

    class FakeClient:
        def __init__(self, url, token):
            calls["client"] = (url, token)

        def close(self):
            calls["closed"] = True

    class FakeWorker:
        def __init__(self, control, **kwargs):
            calls["worker"] = kwargs

        def run_once(self):
            return [JobOutcome("job-1", "succeeded", 1, None, True)]

    monkeypatch.setattr(managed_worker_cli, "MyceliumSourceControlClient", FakeClient)
    monkeypatch.setattr(managed_worker_cli, "ManagedYouTubeWorker", FakeWorker)
    monkeypatch.setenv("MYCELIUM_YOUTUBE_SOURCE_TOKEN", "test-token")
    result = managed_worker_cli.main([
        "--mycelium-url", "http://staging.invalid",
        "--worker-id", "worker-1",
    ])
    assert result == 0
    assert calls["client"] == ("http://staging.invalid", "test-token")
    assert calls["worker"] == {"worker_id": "worker-1", "lease_seconds": 120, "batch_limit": 1}
    assert calls["closed"] is True
    assert '"status": "succeeded"' in capsys.readouterr().out
