import asyncio
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional

from .config import Settings
from .store import CaptionsStore
from .youtube_client import YouTubeClient

LOGGER = logging.getLogger(__name__)


class CaptionsService:
    def __init__(
        self,
        settings: Settings,
        store: CaptionsStore,
        client: Optional[YouTubeClient] = None,
    ):
        self.settings = settings
        self.store = store
        self.client = client or YouTubeClient()
        self.last_poll_at: Optional[str] = None
        self.last_error: Optional[str] = None
        self.is_polling: bool = False

    def check_health(self) -> Dict[str, Any]:
        return {
            "status": "healthy" if not self.last_error else "degraded",
            "channels_configured": len(self.settings.youtube_channel_ids),
            "last_poll_at": self.last_poll_at,
            "last_error": self.last_error,
            "total_processed_videos": self.store.count(),
        }

    async def poll_once(self) -> List[Dict[str, Any]]:
        self.is_polling = True
        now = datetime.now(timezone.utc).isoformat()
        self.last_poll_at = now
        self.store.set_state("last_poll_at", now)
        self.last_error = None
        processed_items: List[Dict[str, Any]] = []

        try:
            for channel_id in self.settings.youtube_channel_ids:
                try:
                    feed_videos = await asyncio.to_thread(
                        self.client.fetch_channel_feed, channel_id
                    )
                    for video in feed_videos:
                        video_id = video.get("video_id")
                        if not video_id:
                            continue
                        if self.store.is_processed(video_id):
                            continue

                        title = video.get("title", "")
                        published_at = video.get("published_at") or now
                        channel_name = video.get("channel_name") or channel_id
                        actual_channel_id = video.get("channel_id") or channel_id

                        try:
                            transcript = await asyncio.to_thread(
                                self.client.fetch_transcript, video_id
                            )
                        except Exception as transcript_err:
                            LOGGER.warning(
                                "Network/API error fetching transcript for video %s: %s",
                                video_id,
                                transcript_err,
                            )
                            # Let genuine network error leave video unrecorded so it can be retried
                            continue

                        has_transcript = transcript is not None
                        pending_article_json = None
                        if has_transcript:
                            # Confirmed Article shape from mycelium/cmd/indexer/pipeline.go
                            article = {
                                "id": video_id,
                                "name": title,
                                "source_agency": channel_name,
                                "published_at": published_at,
                                "raw_content": transcript,
                            }
                            pending_article_json = json.dumps(article)

                        recorded = self.store.record_video(
                            video_id=video_id,
                            channel_id=actual_channel_id,
                            title=title,
                            published_at=published_at,
                            has_transcript=has_transcript,
                            pending_article_json=pending_article_json,
                        )
                        if recorded:
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
