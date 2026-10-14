import sqlite3
import threading
from pathlib import Path
from typing import Any

from . import backfill_store_queries as queries
from .backfill_store_schema import initialize_backfill_schema


class BackfillStore:
    """SQLite store tracking per-video backfill processing and delivery state."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        if path != ":memory:":
            self.connection.execute("PRAGMA journal_mode=WAL")
        self._init_db()

    def _init_db(self) -> None:
        initialize_backfill_schema(self.connection, self._lock)

    def get(self, video_id: str) -> dict[str, Any] | None:
        return queries.get(self, video_id)

    def register(
        self,
        video_id: str,
        channel_id: str,
        title: str,
        duration: int | None = None,
        published_at: str | None = None,
    ) -> bool:
        return queries.register(self, video_id, channel_id, title, duration, published_at)

    def todo(self, limit: int = 0, max_attempts: int = 5) -> list[dict[str, Any]]:
        return queries.todo(self, limit, max_attempts)

    def mark_done(
        self,
        video_id: str,
        published_at: str | None,
        article_uuid: str | None,
        pending_article_json: str | None,
        metadata_json: str | None,
    ) -> None:
        queries.mark_done(
            self,
            video_id,
            published_at,
            article_uuid,
            pending_article_json,
            metadata_json,
        )

    def mark_no_transcript(
        self, video_id: str, published_at: str | None = None
    ) -> None:
        queries.mark_no_transcript(self, video_id, published_at)

    def mark_error(self, video_id: str, error: str) -> None:
        queries.mark_error(self, video_id, error)

    def quarantine(self, video_id: str, reason: str) -> None:
        queries.quarantine(self, video_id, reason)

    def replace_pending(self, video_id: str, envelope: dict[str, Any]) -> None:
        queries.replace_pending(self, video_id, envelope)

    def mark_persisted(
        self, video_id: str, persisted_at: str | None = None
    ) -> None:
        queries.mark_persisted(self, video_id, persisted_at)

    def mark_indexed(
        self, video_id: str, indexed_at: str | None = None
    ) -> None:
        queries.mark_indexed(self, video_id, indexed_at)

    def undelivered_postgres(self, limit: int = 100) -> list[dict[str, Any]]:
        return queries.undelivered_postgres(self, limit)

    def undelivered_indexer(self, limit: int = 100) -> list[dict[str, Any]]:
        return queries.undelivered_indexer(self, limit)

    def done_with_envelope(self, limit: int = 0) -> list[dict[str, Any]]:
        return queries.done_with_envelope(self, limit)

    def get_state(self, key: str, default: str | None = None) -> str | None:
        return queries.get_state(self, key, default)

    def set_state(self, key: str, value: str) -> None:
        queries.set_state(self, key, value)

    def stats(self) -> dict[str, int]:
        return queries.stats(self)

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    def __enter__(self) -> "BackfillStore":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
