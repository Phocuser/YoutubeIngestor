import asyncio
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional

from .config import Settings
from .capture import build_capture
from .store import CaptionsStore
from .youtube_client import YouTubeClient

LOGGER = logging.getLogger(__name__)


class CaptionsService:
    def __init__(
        self,
        settings: Settings,
        store: CaptionsStore,
        client: Optional[YouTubeClient] = None,
        submitter=None,
    ):
        self.settings = settings
        self.store = store
        self.client = client or YouTubeClient()
        self.submitter = submitter
        self.last_poll_at: Optional[str] = None
        self.last_error: Optional[str] = None
        self.is_polling: bool = False

    def check_health(self) -> Dict[str, Any]:
        return {
            "status": "healthy" if not self.last_error else "degraded",
            "channels_configured": len(self.settings.youtube_channel_ids),
            "durable_ingestion_enabled": self.submitter is not None,
            "polling_enabled": self.submitter is not None and bool(self.settings.youtube_channel_ids),
            "last_poll_at": self.last_poll_at,
            "last_error": self.last_error,
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
        now = datetime.now(timezone.utc).isoformat()
        self.last_poll_at = now
        self.store.set_state("last_poll_at", now)
        self.last_error = None
        processed_items: List[Dict[str, Any]] = []
        seen_video_ids: set[str] = set()

        try:
            try:
                await self.retry_undelivered_articles(limit=25)
            except Exception as retry_exc:
                LOGGER.warning("Retry sweep failed: %s", retry_exc)

            for channel_id in self.settings.youtube_channel_ids:
                try:
                    feed_videos = await asyncio.to_thread(
                        self.client.fetch_channel_feed, channel_id
                    )
                    for video in feed_videos:
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
                except Exception as channel_err:
                    LOGGER.warning(
                        "Error processing channel feed for %s: %s",
                        channel_id,
                        channel_err,
                    )
                    self.last_error = str(channel_err)

            if self.last_error:
                self.store.set_state("last_poll_status", f"error: {self.last_error}")
            else:
                self.store.set_state("last_poll_status", "ok")

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
