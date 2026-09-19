"""Resumable channel backfill: enumerate a channel, fetch timed transcripts, cut the
ad reads, and deliver the spoken text to Mycelium (Postgres + indexer).

    python -m app.backfill [--limit N] [--max-hours H] [--channels @handle,...]

State lives in SQLite, so an interrupted or throttled run simply resumes next time.
"""
import argparse
import errno
import fcntl
import json
import logging
import random
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from . import channel_videos, sponsor
from .backfill_store import BackfillStore
from .config import Settings
from .indexer_client import run_indexer
from .mycelium_pg import ALREADY_PRESENT, MyceliumPg, article_uuid
from .youtube_client import TranscriptBlocked, fetch_timed_transcript

LOGGER = logging.getLogger(__name__)

MIN_WORDS = 60
BLOCK_BACKOFF_SECONDS = (300, 900, 1800, 1800, 3600)


class AlreadyRunning(RuntimeError):
    """Raised when another backfill process owns the single-instance lock."""


@contextmanager
def single_instance(path: str):
    lock_path = Path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise AlreadyRunning(f"backfill lock is held: {path}") from exc
            raise
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


@dataclass
class Summary:
    discovered: int = 0
    processed: int = 0
    done: int = 0
    no_transcript: int = 0
    errors: int = 0
    skipped_live: int = 0
    stored: int = 0
    already_present: int = 0
    indexed: int = 0
    delivery_failures: int = 0
    ads_removed_seconds: float = 0.0
    aborted: str = ""


class Backfill:
    def __init__(
        self,
        settings: Settings,
        store: BackfillStore,
        pg: Optional[MyceliumPg],
        *,
        list_videos: Callable = channel_videos.list_channel_videos,
        fetch_meta: Callable = channel_videos.fetch_video_meta,
        fetch_transcript: Callable = fetch_timed_transcript,
        clean: Callable = sponsor.clean_transcript,
        index: Callable = run_indexer,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], float] = time.time,
        delay: tuple = (3.0, 6.0),
    ) -> None:
        self.settings = settings
        self.store = store
        self.pg = pg
        self._list_videos = list_videos
        self._fetch_meta = fetch_meta
        self._fetch_transcript = fetch_transcript
        self._clean = clean
        self._index = index
        self._sleep = sleep
        self._clock = clock
        self._wall = wall
        self._delay = delay

    # -- discovery ----------------------------------------------------------

    def discover(self, channels: List[str], summary: Summary) -> None:
        for handle in channels:
            try:
                refs = self._list_videos(handle)
            except Exception as exc:  # noqa: BLE001 - keep going with what we already know
                LOGGER.warning("could not enumerate %s: %s", handle, exc)
                continue
            new = sum(
                1 for r in refs if self.store.register(r.video_id, handle, r.title, r.duration)
            )
            summary.discovered += len(refs)
            LOGGER.info("%s: %d videos listed, %d new", handle, len(refs), new)

    # -- delivery -----------------------------------------------------------

    def _deliver_pg(self, row: Dict[str, Any], summary: Summary) -> None:
        if self.pg is None:
            return
        envelope = json.loads(row["pending_article_json"])
        metadata = json.loads(row["metadata_json"] or "{}")
        try:
            outcome = self.pg.insert(envelope, metadata)
        except Exception as exc:  # noqa: BLE001 - stays in the outbox for the next sweep
            LOGGER.warning("postgres insert failed for %s: %s", row["video_id"], exc)
            summary.delivery_failures += 1
            return
        self.store.mark_persisted(row["video_id"])
        if outcome == ALREADY_PRESENT:
            summary.already_present += 1
        else:
            summary.stored += 1

    def _deliver_indexer(self, row: Dict[str, Any], summary: Summary) -> None:
        s = self.settings
        ok, output = self._index(
            json.loads(row["pending_article_json"]),
            indexer_bin=s.indexer_bin,
            dict_path=s.indexer_dict_path,
            markers_path=s.indexer_markers_path,
            redis_addr=s.mycelium_redis_addr,
            timeout=120.0,
        )
        if ok:
            self.store.mark_indexed(row["video_id"])
            summary.indexed += 1
        else:
            LOGGER.warning("indexer failed for %s: %s", row["video_id"], output[:200])
            summary.delivery_failures += 1

    def retry_sweep(self, summary: Summary) -> None:
        for row in self.store.undelivered_postgres():
            self._deliver_pg(row, summary)
        for row in self.store.undelivered_indexer():
            self._deliver_indexer(row, summary)

    # -- one video ----------------------------------------------------------

    def _wait_out_cooldown(self) -> None:
        """Sleep through a persisted YouTube cooldown (survives restarts) so no
        request of any kind is sent while we know we are being throttled."""
        blocked_until = float(self.store.get_state("blocked_until", "0") or 0)
        remaining = blocked_until - self._wall()
        if remaining > 0:
            LOGGER.warning("YouTube cooldown active; sleeping %.1fs", remaining)
            self._sleep(remaining)
            self.store.set_state("blocked_until", "0")

    def _transcript_with_backoff(self, video_id: str, summary: Summary):
        """Fetch a transcript, sleeping through YouTube throttling. Returns
        ``(snippets_or_None, ok)``; ok=False means we gave up (still blocked)."""
        self._wait_out_cooldown()

        for attempt in range(len(BLOCK_BACKOFF_SECONDS) + 1):
            try:
                transcript = self._fetch_transcript(video_id)
                self.store.set_state("block_count", "0")
                return transcript, True
            except TranscriptBlocked:
                block_count = int(self.store.get_state("block_count", "0") or 0) + 1
                wait = BLOCK_BACKOFF_SECONDS[
                    min(block_count - 1, len(BLOCK_BACKOFF_SECONDS) - 1)
                ]
                self.store.set_state("block_count", str(block_count))
                self.store.set_state("blocked_until", str(self._wall() + wait))
                if attempt == len(BLOCK_BACKOFF_SECONDS):
                    return None, False
                LOGGER.warning("YouTube throttling us (attempt %d); sleeping %ds", attempt + 1, wait)
                self._sleep(wait)
        return None, False

    def process_video(self, row: Dict[str, Any], summary: Summary) -> bool:
        """Returns False when the run should stop (persistent throttling)."""
        video_id = row["video_id"]
        try:
            self._wait_out_cooldown()
            meta = self._fetch_meta(video_id)
            if channel_videos.is_upcoming_or_live(meta):
                summary.skipped_live += 1
                return True
            raw, ok = self._transcript_with_backoff(video_id, summary)
            if not ok:
                summary.aborted = "YouTube kept throttling transcript requests"
                return False
            if raw is None:
                self.store.mark_no_transcript(video_id, meta.published_at or None)
                summary.no_transcript += 1
                return True
            snippets = [sponsor.Snippet(text=t, start=s, duration=d) for t, s, d in raw]
            cleaned = self._clean(snippets, video_id)
            if len(cleaned.text.split()) < MIN_WORDS:
                self.store.mark_no_transcript(video_id, meta.published_at or None)
                summary.no_transcript += 1
                return True

            envelope = {
                "id": article_uuid(video_id),
                "name": meta.title or row["title"],
                "source_agency": meta.channel_name or "YouTube",
                "published_at": meta.published_at,
                "raw_content": cleaned.text,
            }
            metadata = {
                "video_id": video_id,
                "title": meta.title or row["title"],
                "channel_name": meta.channel_name,
                "channel_id": meta.channel_id,
                "duration": meta.duration,
                "transcript_source": "youtube-captions",
                "ads_removed_seconds": round(cleaned.removed_seconds, 1),
                "ads_removed_snippets": cleaned.removed_snippets,
                "ad_removal_sources": cleaned.sources,
            }
            self.store.mark_done(
                video_id, meta.published_at, envelope["id"], json.dumps(envelope), json.dumps(metadata)
            )
            summary.done += 1
            summary.ads_removed_seconds += cleaned.removed_seconds
            fresh = self.store.get(video_id)
            self._deliver_pg(fresh, summary)
            self._deliver_indexer(fresh, summary)
        except Exception as exc:  # noqa: BLE001 - one bad video must not stop the night
            LOGGER.warning("video %s failed: %s", video_id, exc)
            self.store.mark_error(video_id, f"{type(exc).__name__}: {exc}")
            summary.errors += 1
        return True

    # -- run ----------------------------------------------------------------

    def run(
        self,
        channels: List[str],
        *,
        limit: int = 0,
        max_hours: float = 0.0,
        discover: bool = True,
    ) -> Summary:
        summary = Summary()
        started = self._clock()
        self._wait_out_cooldown()
        if discover:
            self.discover(channels, summary)
        self.retry_sweep(summary)
        for row in self.store.todo(limit=limit):
            if max_hours and self._clock() - started > max_hours * 3600:
                summary.aborted = f"stopped after {max_hours}h (resume by re-running)"
                break
            summary.processed += 1
            if not self.process_video(row, summary):
                break
            if summary.processed % 25 == 0:
                LOGGER.info("progress: %s", asdict(summary))
            self._sleep(random.uniform(*self._delay))
        self.store.set_state("last_run_summary", json.dumps(asdict(summary)))
        return summary


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="app.backfill", description=__doc__.splitlines()[0])
    parser.add_argument("--limit", type=int, default=0, help="process at most N videos")
    parser.add_argument("--max-hours", type=float, default=0.0, help="stop after H hours")
    parser.add_argument("--channels", default="", help="comma-separated @handles/UC ids (default: BACKFILL_CHANNELS)")
    parser.add_argument("--no-discover", action="store_true", help="skip re-listing the channel(s)")
    parser.add_argument("--no-postgres", action="store_true", help="skip the Postgres write (indexer only)")
    parser.add_argument("--delay-min", type=float, default=12.0, help="min seconds between videos")
    parser.add_argument("--delay-max", type=float, default=20.0, help="max seconds between videos")
    parser.add_argument("--stats", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    settings = Settings()
    store = BackfillStore(settings.backfill_database_path)
    if args.stats:
        print(json.dumps(store.stats(), indent=2))
        return 0
    try:
        with single_instance(settings.backfill_database_path + ".lock"):
            pg = None if args.no_postgres else MyceliumPg(settings.mycelium_pg_url, settings.mycelium_dir)
            channels = [c.strip() for c in args.channels.split(",") if c.strip()] or settings.backfill_channels
            summary = Backfill(settings, store, pg, delay=(args.delay_min, args.delay_max)).run(
                channels, limit=args.limit, max_hours=args.max_hours, discover=not args.no_discover
            )
    except AlreadyRunning:
        print("backfill already running; exiting", file=sys.stderr)
        return 3
    print(json.dumps(asdict(summary), indent=2))
    return 1 if summary.aborted else 0


if __name__ == "__main__":
    raise SystemExit(main())
