import asyncio
from datetime import datetime, timedelta, timezone
import json
import logging
from typing import Any, Callable, Dict, List, Optional

from .config import Settings
from .capture import build_capture
from .store import CaptionsStore
from .youtube_client import TranscriptBlocked, YouTubeClient

LOGGER = logging.getLogger(__name__)

TRANSCRIPT_COOLDOWN_UNTIL = "youtube_transcript_cooldown_until"
TRANSCRIPT_BLOCK_STREAK = "youtube_transcript_block_streak"
TRANSCRIPT_COOLDOWN_BASE_SECONDS = 30 * 60
TRANSCRIPT_COOLDOWN_MAX_SECONDS = 6 * 60 * 60


class CaptionsService:
    def __init__(
        self,
        settings: Settings,
        store: CaptionsStore,
        client: Optional[YouTubeClient] = None,
        submitter=None,
        clock: Optional[Callable[[], datetime]] = None,
    ):
        self.settings = settings
        self.store = store
        self.client = client or YouTubeClient()
        self.submitter = submitter
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.last_poll_at: Optional[str] = self.store.get_state("last_poll_at")
        self.last_source_check_at: Optional[str] = self.store.get_state("last_source_check_at")
        self.last_error: Optional[str] = None
        self.last_poll_status: Optional[str] = self.store.get_state("last_poll_status")
        self.is_polling: bool = False

    def _now(self) -> datetime:
        now = self._clock()
        return now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)

    def _cooldown_until(self) -> Optional[datetime]:
        value = self.store.get_state(TRANSCRIPT_COOLDOWN_UNTIL)
        if not value:
            return None
        try:
            until = datetime.fromisoformat(value)
        except ValueError:
            LOGGER.warning("Ignoring malformed transcript cooldown state")
            return None
        return until if until.tzinfo is not None else until.replace(tzinfo=timezone.utc)

    @staticmethod
    def _paused_status(until: datetime) -> str:
        return f"paused: YouTube transcript requests blocked until {until.isoformat()}"

    @staticmethod
    def _is_provider_blocked(exc: Exception) -> bool:
        return isinstance(exc, TranscriptBlocked) or exc.__class__.__name__ in {
            "RequestBlocked",
            "IpBlocked",
            "TooManyRequests",
        }

    def _set_cooldown(self, now: datetime) -> datetime:
        try:
            prior_streak = int(self.store.get_state(TRANSCRIPT_BLOCK_STREAK) or "0")
        except ValueError:
            prior_streak = 0
        streak = max(1, prior_streak + 1)
        seconds = min(
            TRANSCRIPT_COOLDOWN_BASE_SECONDS * (2 ** (streak - 1)),
            TRANSCRIPT_COOLDOWN_MAX_SECONDS,
        )
        until = now + timedelta(seconds=seconds)
        status = self._paused_status(until)
        self.store.set_states(
            {
                TRANSCRIPT_BLOCK_STREAK: str(streak),
                TRANSCRIPT_COOLDOWN_UNTIL: until.isoformat(),
                "last_poll_status": status,
            }
        )
        self.last_poll_status = status
        return until

    def _clear_cooldown(self, poll_status: Optional[str] = None) -> None:
        updates: Dict[str, str] = {}
        if self.store.get_state(TRANSCRIPT_BLOCK_STREAK) is not None:
            updates[TRANSCRIPT_BLOCK_STREAK] = "0"
        if self.store.get_state(TRANSCRIPT_COOLDOWN_UNTIL) is not None:
            updates[TRANSCRIPT_COOLDOWN_UNTIL] = ""
        if poll_status is not None:
            updates["last_poll_status"] = poll_status
        self.store.set_states(updates)

    def check_health(self) -> Dict[str, Any]:
        cooldown_until = self._cooldown_until()
        now = self._now()
        paused = cooldown_until is not None and cooldown_until > now
        poll_status = (
            self._paused_status(cooldown_until)
            if paused
            else self.last_poll_status or self.store.get_state("last_poll_status")
        )
        return {
            "status": "paused" if paused else ("healthy" if not self.last_error else "degraded"),
            "channels_configured": len(self.settings.youtube_channel_ids),
            "durable_ingestion_enabled": self.submitter is not None,
            "polling_enabled": self.submitter is not None and bool(self.settings.youtube_channel_ids),
            "last_poll_at": self.last_poll_at,
            "last_source_check_at": self.last_source_check_at,
            "last_error": self.last_error,
            "last_poll_status": poll_status,
            "paused_until": cooldown_until.isoformat() if paused else None,
            "total_processed_videos": self.store.count(),
        }

    async def submit_durable(
        self, article: dict, video_id: Optional[str] = None
    ) -> bool:
        target_id = video_id
        if target_id is None:
            article_id = article.get("id")
            target_id = (
                article_id.removeprefix("youtube:")
                if isinstance(article_id, str) and article_id.startswith("youtube:")
                else article_id
            )
        stored = self.store.get_video(target_id) if target_id else None
        if not stored or not stored.get("pending_article_json") or stored.get("review_state") not in {"ready", "revision_needed"}:
            LOGGER.warning("refusing durable receipt submission before caption persistence for %s", target_id)
            return False
        try:
            article = json.loads(stored["pending_article_json"])
        except (TypeError, json.JSONDecodeError):
            LOGGER.warning("refusing malformed durable caption outbox for %s", target_id)
            return False
        try:
            if self.submitter is not None:
                stored_metadata = json.loads(stored.get("pending_metadata_json") or "{}")
                receipt = await asyncio.to_thread(self.submitter.submit, article, stored_metadata or {"video_id": target_id, "transcript_source": "youtube-captions"})
                state = getattr(getattr(receipt, "state", None), "value", getattr(receipt, "state", None))
                if state not in {"queued", "leased", "retry_wait", "succeeded", "quarantined"}:
                    return False
                if target_id:
                    self.store.record_admission(target_id, receipt, state=state)
                return True
            LOGGER.warning("no common durable receipt boundary configured for video %s", target_id)
            return False
        except Exception as exc:
            LOGGER.warning(
                "Could not submit capture receipt for video %s: %s",
                target_id,
                exc,
            )
            return False

    async def retry_undelivered_articles(self, limit: int = 25) -> None:
        try:
            undelivered = self.store.undelivered_with_transcript(limit=limit)
            if not undelivered:
                return

            for item in undelivered:
                try:
                    video_id = item["video_id"]
                    payload = None
                    if item.get("pending_article_json"):
                        try:
                            payload = json.loads(item["pending_article_json"])
                        except Exception as parse_err:
                            LOGGER.warning(
                                "Could not deserialize pending_article_json for %s: %s",
                                video_id,
                                parse_err,
                            )
                            payload = None

                    if payload:
                        await self.submit_durable(payload, video_id=video_id)
                except Exception as item_err:
                    LOGGER.warning(
                        "Failed retrying capture receipt for %s: %s",
                        item.get("video_id"),
                        item_err,
                    )
        except Exception as exc:
            LOGGER.warning("Error in retry_undelivered_articles sweep: %s", exc)

    async def poll_once(self) -> List[Dict[str, Any]]:
        self.is_polling = True
        now_dt = self._now()
        now = now_dt.isoformat()
        self.last_poll_at = now
        self.store.set_state("last_poll_at", now)
        self.last_error = None
        processed_items: List[Dict[str, Any]] = []
        seen_video_ids: set[str] = set()
        blocked = False

        try:
            cooldown_until = self._cooldown_until()
            if cooldown_until is not None and cooldown_until > now_dt:
                self.last_poll_status = self._paused_status(cooldown_until)
                self.store.set_state("last_poll_status", self.last_poll_status)
                return processed_items

            try:
                await self.retry_undelivered_articles(limit=25)
            except Exception as retry_exc:
                LOGGER.warning("Retry sweep failed: %s", retry_exc)

            for channel_id in self.settings.youtube_channel_ids:
                try:
                    feed_videos = await asyncio.to_thread(
                        self.client.fetch_channel_feed, channel_id
                    )
                    for video_index, video in enumerate(feed_videos):
                        video_id = video.get("video_id")
                        if not video_id:
                            continue
                        if video_id in seen_video_ids:
                            continue
                        seen_video_ids.add(video_id)
                        title = video.get("title", "")
                        published_at = video.get("published_at") or now
                        channel_name = video.get("channel_name") or channel_id
                        actual_channel_id = video.get("channel_id") or channel_id

                        try:
                            fetch_timed = getattr(self.client, "fetch_timed_transcript", None)
                            if not callable(fetch_timed):
                                raise RuntimeError("client lacks timed-caption boundary")
                            transcript = await asyncio.to_thread(fetch_timed, video_id)
                        except Exception as transcript_err:
                            if self._is_provider_blocked(transcript_err):
                                self._set_cooldown(now_dt)
                                blocked = True
                                # Preserve retryable gaps for already discovered
                                # videos without making another provider request.
                                for pending_video in feed_videos[video_index:]:
                                    pending_id = pending_video.get("video_id")
                                    if pending_id and not self.store.is_processed(pending_id):
                                        self.store.record_gap(
                                            pending_id,
                                            pending_video.get("channel_id") or channel_id,
                                            pending_video.get("title", ""),
                                            pending_video.get("published_at") or now,
                                            "caption_fetch_blocked",
                                        )
                                LOGGER.warning(
                                    "YouTube transcript access is blocked; pausing all provider requests until %s",
                                    self._cooldown_until().isoformat(),
                                )
                                break
                            LOGGER.warning(
                                "Network/API error fetching transcript for video %s: %s",
                                video_id,
                                transcript_err,
                            )
                            # Record a retryable gap without claiming successful processing.
                            self.store.record_gap(video_id, actual_channel_id, title, published_at,
                                                  f"caption_fetch_retryable:{type(transcript_err).__name__}")
                            continue

                        has_transcript = transcript is not None
                        pending_article_json = None
                        article = None
                        if has_transcript:
                            if not isinstance(transcript, (list, tuple)) or not transcript:
                                LOGGER.warning("malformed or empty captions remain retryable for %s", video_id)
                                self.store.record_gap(video_id, actual_channel_id, title, published_at,
                                                      "caption_missing_or_malformed")
                                continue
                            article, metadata = build_capture(
                                video_id, transcript, title=title, channel_id=actual_channel_id,
                                channel_name=channel_name, published_at=published_at,
                                is_public_channel_feed=True,
                            )
                            if not any(segment["text"] for segment in article["caption_track"]["segments"]):
                                LOGGER.warning("empty captions remain retryable for %s", video_id)
                                self.store.record_gap(video_id, actual_channel_id, title, published_at,
                                                      "caption_empty")
                                continue
                            pending_article_json = json.dumps(article)
                        else:
                            LOGGER.warning("missing timed captions remain retryable for %s", video_id)
                            self.store.record_gap(video_id, actual_channel_id, title, published_at,
                                                  "caption_missing")
                            continue

                        recorded = self.store.record_video(
                            video_id=video_id,
                            channel_id=actual_channel_id,
                            title=title,
                            published_at=published_at,
                            has_transcript=has_transcript,
                            pending_article_json=pending_article_json,
                            pending_metadata_json=json.dumps(metadata) if metadata else None,
                        )
                        revision_pending = self.store.get_video(video_id)
                        if recorded or (
                            revision_pending
                            and revision_pending.get("review_state") == "revision_needed"
                            and revision_pending.get("admission_state") == "pending"
                        ):
                            processed_items.append(
                                {
                                    "video_id": video_id,
                                    "channel_id": actual_channel_id,
                                    "title": title,
                                    "has_transcript": has_transcript,
                                }
                            )
                            LOGGER.info(
                                "Ingested video [%s] from %s (has_transcript=%s)",
                                video_id,
                                actual_channel_id,
                                has_transcript,
                            )
                            if has_transcript and article:
                                await self.submit_durable(
                                    article, video_id=video_id
                                )
                    if blocked:
                        break
                except Exception as channel_err:
                    if self._is_provider_blocked(channel_err):
                        self._set_cooldown(now_dt)
                        blocked = True
                        LOGGER.warning(
                            "YouTube provider access is blocked; pausing all provider requests until %s",
                            self._cooldown_until().isoformat(),
                        )
                        break
                    LOGGER.warning(
                        "Error processing channel feed for %s: %s",
                        channel_id,
                        channel_err,
                    )
                    self.last_error = str(channel_err)
                if blocked:
                    break

            if blocked:
                # _set_cooldown wrote the aggregate status and avoids a
                # per-video blocked error.
                pass
            elif self.last_error:
                self.last_poll_status = f"error: {self.last_error}"
                self.store.set_state("last_poll_status", self.last_poll_status)
            else:
                if self.settings.youtube_channel_ids:
                    self.last_source_check_at = datetime.now(timezone.utc).isoformat()
                    self.store.set_state("last_source_check_at", self.last_source_check_at)
                self.last_poll_status = "ok"
                self._clear_cooldown(poll_status=self.last_poll_status)

        finally:
            self.is_polling = False

        return processed_items

    async def run_loop(self) -> None:
        LOGGER.info(
            "Starting YouTube captions polling loop: channels=%d, interval=%ds",
            len(self.settings.youtube_channel_ids),
            self.settings.poll_interval_seconds,
        )
        while True:
            try:
                await self.poll_once()
            except Exception as exc:
                LOGGER.error("Unexpected error in captions poll loop: %s", exc)
            await asyncio.sleep(max(10, self.settings.poll_interval_seconds))
