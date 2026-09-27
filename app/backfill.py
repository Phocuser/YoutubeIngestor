"""Resumable channel backfill: enumerate a channel, fetch timed transcripts, and
deliver a reversible caption capture to Mycelium.

    python -m app.backfill [--limit N] [--backfill-days N] [--max-hours H] [--channels @handle,...]

State lives in SQLite, so an interrupted or throttled run simply resumes next time.
"""
import argparse
import errno
import fcntl
import inspect
import json
import logging
import random
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from . import channel_videos, sponsor
from .backfill_store import BackfillStore
from .config import Settings
from .capture import build_capture
from .mycelium_pg import ALREADY_PRESENT, QUARANTINED, MyceliumPg
from .youtube_client import TranscriptBlocked, fetch_timed_transcript

LOGGER = logging.getLogger(__name__)

BLOCK_BACKOFF_SECONDS = (300, 900, 1800, 1800, 3600)
MIN_BACKFILL_DAYS = 1
MAX_BACKFILL_DAYS = 90
MIN_DISCOVERY_LIMIT = 1
MAX_DISCOVERY_LIMIT = 1000


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
    out_of_window: int = 0
    unknown_dates: int = 0
    incomplete: bool = False
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
        clean: Callable | None = None,
        index: Callable | None = None,
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
        # Kept in the constructor for snapshot compatibility; adapter-owned
        # indexing is intentionally not invoked.
        self._index = None
        self._sleep = sleep
        self._clock = clock
        self._wall = wall
        self._delay = delay

    # -- discovery ----------------------------------------------------------

    @staticmethod
    def _validate_discovery_bounds(limit: int, backfill_days: Optional[int]) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("limit must be an integer")
        if limit != 0 and not (MIN_DISCOVERY_LIMIT <= limit <= MAX_DISCOVERY_LIMIT):
            raise ValueError("limit must be 0 or an integer from 1 through 1000")
        if backfill_days is None:
            return
        if isinstance(backfill_days, bool) or not isinstance(backfill_days, int):
            raise ValueError("backfill_days must be an integer from 1 through 90")
        if not (MIN_BACKFILL_DAYS <= backfill_days <= MAX_BACKFILL_DAYS):
            raise ValueError("backfill_days must be an integer from 1 through 90")
        if limit == 0:
            raise ValueError("backfill_days requires a limit from 1 through 1000")

    def _list_channel_videos(self, handle: str, limit: Optional[int]):
        """Call injected enumerators with a bound while keeping old adapters usable."""
        if limit is None:
            return self._list_videos(handle)

        try:
            parameters = inspect.signature(self._list_videos).parameters.values()
            supports_limit = any(
                parameter.name == "limit" or parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in parameters
            )
        except (TypeError, ValueError):
            supports_limit = True

        if supports_limit:
            return self._list_videos(handle, limit=limit)
        return self._list_videos(handle)

    def _publication_date(self, value: Any) -> Optional[datetime]:
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _mark_incomplete(self, summary: Summary) -> None:
        summary.incomplete = True

    def _window_bounds(self, backfill_days: Optional[int]):
        if backfill_days is None:
            return None
        now = datetime.fromtimestamp(self._wall(), timezone.utc)
        return now - timedelta(days=backfill_days), now

    def _row_in_window(self, row: Dict[str, Any], summary: Summary, window) -> bool:
        if window is None:
            return True
        cutoff, now = window
        published_at = self._publication_date(row.get("published_at"))
        if published_at is None:
            summary.unknown_dates += 1
            self._mark_incomplete(summary)
            return False
        if published_at < cutoff or published_at > now:
            summary.out_of_window += 1
            return False
        return True

    def discover(
        self,
        channels: List[str],
        summary: Summary,
        *,
        limit: int = 0,
        backfill_days: Optional[int] = None,
        window=None,
    ) -> None:
        self._validate_discovery_bounds(limit, backfill_days)
        remaining = limit if limit > 0 else None
        window = self._window_bounds(backfill_days) if window is None else window

        for handle in channels:
            if remaining is not None and remaining <= 0:
                break
            try:
                refs = self._list_channel_videos(handle, remaining)
            except Exception as exc:  # noqa: BLE001 - keep going with what we already know
                LOGGER.warning("could not enumerate %s: %s", handle, exc)
                continue

            listed = 0
            new = 0
            for ref in refs:
                if remaining is not None and listed >= remaining:
                    break
                listed += 1
                summary.discovered += 1

                published_at = getattr(ref, "published_at", None)
                if window is not None and not self._row_in_window(
                    {"published_at": published_at}, summary, window
                ):
                    continue

                if self.store.register(
                    ref.video_id, handle, ref.title, ref.duration, published_at
                ):
                    new += 1

            if remaining is not None:
                remaining -= listed
            LOGGER.info("%s: %d videos listed, %d new", handle, listed, new)

    # -- delivery -----------------------------------------------------------

    def _deliver_pg(self, row: Dict[str, Any], summary: Summary) -> None:
        if self.pg is None:
            summary.delivery_failures += 1
            LOGGER.warning("no explicit Mycelium capture boundary configured for %s", row["video_id"])
            return
        envelope = json.loads(row["pending_article_json"])
        metadata = json.loads(row["metadata_json"] or "{}")
        try:
            if hasattr(self.pg, "submit"):
                receipt = self.pg.submit(envelope, metadata)
                state = getattr(getattr(receipt, "state", None), "value", getattr(receipt, "state", None))
                if state not in {"queued", "leased", "retry_wait", "succeeded"}:
                    self.store.quarantine(row["video_id"], "central receipt requires review")
                    summary.delivery_failures += 1
                    return
                self.store.mark_persisted(row["video_id"])
                summary.stored += 1
                return
            raise RuntimeError("injected boundary must provide submit()")
        except Exception as exc:  # noqa: BLE001 - stays in the outbox for the next sweep
            LOGGER.warning("postgres insert failed for %s: %s", row["video_id"], exc)
            summary.delivery_failures += 1
            return

    def retry_sweep(self, summary: Summary, *, window=None) -> None:
        for row in self.store.undelivered_postgres():
            if not self._row_in_window(row, summary, window):
                continue
            self._deliver_pg(row, summary)

    def _todo_rows(self, limit: int, summary: Summary, window):
        if window is None:
            return self.store.todo(limit=limit)
        rows = []
        for row in self.store.todo(limit=0):
            if not self._row_in_window(row, summary, window):
                continue
            rows.append(row)
            if limit > 0 and len(rows) >= limit:
                break
        return rows

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
            if not isinstance(raw, (list, tuple)) or not raw:
                self.store.mark_error(video_id, "malformed_or_empty_timed_captions")
                summary.errors += 1
                return True
            envelope, metadata = build_capture(
                video_id, raw, title=meta.title or row["title"],
                channel_id=meta.channel_id or row["channel_id"],
                channel_name=meta.channel_name or "YouTube",
                published_at=meta.published_at, cleaner=self._clean,
                duration=meta.duration,
                is_public_channel_feed=True,
            )
            self.store.mark_done(
                video_id, meta.published_at, envelope["id"], json.dumps(envelope), json.dumps(metadata)
            )
            summary.done += 1
            summary.ads_removed_seconds += float(metadata["ads_removed_seconds"])
            fresh = self.store.get(video_id)
            self._deliver_pg(fresh, summary)
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
        backfill_days: Optional[int] = None,
    ) -> Summary:
        self._validate_discovery_bounds(limit, backfill_days)

        summary = Summary()
        started = self._clock()
        self._wait_out_cooldown()
        window = self._window_bounds(backfill_days)
        if discover:
            self.discover(
                channels,
                summary,
                limit=limit,
                backfill_days=backfill_days,
                window=window,
            )
        self.retry_sweep(summary, window=window)
        for row in self._todo_rows(limit, summary, window):
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
    parser.add_argument("--backfill-days", type=int, default=None, help="only discover videos published in the last N days")
    parser.add_argument("--max-hours", type=float, default=0.0, help="stop after H hours")
    parser.add_argument("--channels", default="", help="comma-separated @handles/UC ids (default: BACKFILL_CHANNELS)")
    parser.add_argument("--no-discover", action="store_true", help="skip re-listing the channel(s)")
    parser.add_argument("--no-postgres", action="store_true", help="legacy compatibility flag; disable the durable capture boundary")
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
                channels,
                limit=args.limit,
                max_hours=args.max_hours,
                discover=not args.no_discover,
                backfill_days=args.backfill_days,
            )
    except AlreadyRunning:
        print("backfill already running; exiting", file=sys.stderr)
        return 3
    print(json.dumps(asdict(summary), indent=2))
    return 1 if (summary.aborted or summary.incomplete or summary.delivery_failures or summary.errors) else 0


if __name__ == "__main__":
    raise SystemExit(main())
