import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

LOGGER = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        with self._lock:
            with self.connection:
                self.connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS backfill_videos (
                        video_id TEXT PRIMARY KEY,
                        channel_id TEXT NOT NULL,
                        title TEXT NOT NULL,
                        published_at TEXT,
                        duration INTEGER,
                        status TEXT NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        last_error TEXT,
                        article_uuid TEXT,
                        pending_article_json TEXT,
                        metadata_json TEXT,
                        persisted_at TEXT,
                        indexed_at TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                self.connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS backfill_state (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                    """
                )

    def get(self, video_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cursor = self.connection.execute(
                "SELECT * FROM backfill_videos WHERE video_id = ?",
                (video_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def register(
        self,
        video_id: str,
        channel_id: str,
        title: str,
        duration: Optional[int] = None,
    ) -> bool:
        now = _now_iso()
        with self._lock:
            with self.connection:
                cursor = self.connection.execute(
                    """
                    INSERT OR IGNORE INTO backfill_videos (
                        video_id,
                        channel_id,
                        title,
                        duration,
                        status,
                        attempts,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)
                    """,
                    (video_id, channel_id, title, duration, now, now),
                )
                return cursor.rowcount > 0

    def todo(self, limit: int = 0, max_attempts: int = 5) -> List[Dict[str, Any]]:
        with self._lock:
            if limit > 0:
                cursor = self.connection.execute(
                    """
                    SELECT * FROM backfill_videos
                    WHERE status = 'pending' OR (status = 'error' AND attempts < ?)
                    ORDER BY created_at ASC, rowid ASC
                    LIMIT ?
                    """,
                    (max_attempts, limit),
                )
            else:
                cursor = self.connection.execute(
                    """
                    SELECT * FROM backfill_videos
                    WHERE status = 'pending' OR (status = 'error' AND attempts < ?)
                    ORDER BY created_at ASC, rowid ASC
                    """,
                    (max_attempts,),
                )
            return [dict(row) for row in cursor.fetchall()]

    def mark_done(
        self,
        video_id: str,
        published_at: Optional[str],
        article_uuid: Optional[str],
        pending_article_json: Optional[str],
        metadata_json: Optional[str],
    ) -> None:
        now = _now_iso()
        with self._lock:
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE backfill_videos
                    SET status = 'done',
                        published_at = COALESCE(?, published_at),
                        article_uuid = ?,
                        pending_article_json = ?,
                        metadata_json = ?,
                        last_error = NULL,
                        attempts = attempts + 1,
                        updated_at = ?
                    WHERE video_id = ?
                    """,
                    (
                        published_at,
                        article_uuid,
                        pending_article_json,
                        metadata_json,
                        now,
                        video_id,
                    ),
                )

    def mark_no_transcript(
        self, video_id: str, published_at: Optional[str] = None
    ) -> None:
        now = _now_iso()
        with self._lock:
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE backfill_videos
                    SET status = 'no_transcript',
                        published_at = COALESCE(?, published_at),
                        attempts = attempts + 1,
                        updated_at = ?
                    WHERE video_id = ?
                    """,
                    (published_at, now, video_id),
                )

    def mark_error(self, video_id: str, error: str) -> None:
        now = _now_iso()
        err_msg = str(error)[:500] if error is not None else None
        with self._lock:
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE backfill_videos
                    SET status = 'error',
                        last_error = ?,
                        attempts = attempts + 1,
                        updated_at = ?
                    WHERE video_id = ?
                    """,
                    (err_msg, now, video_id),
                )

    def mark_persisted(
        self, video_id: str, persisted_at: Optional[str] = None
    ) -> None:
        now = persisted_at or _now_iso()
        updated = _now_iso()
        with self._lock:
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE backfill_videos
                    SET persisted_at = ?,
                        updated_at = ?
                    WHERE video_id = ?
                    """,
                    (now, updated, video_id),
                )

    def mark_indexed(
        self, video_id: str, indexed_at: Optional[str] = None
    ) -> None:
        now = indexed_at or _now_iso()
        updated = _now_iso()
        with self._lock:
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE backfill_videos
                    SET indexed_at = ?,
                        updated_at = ?
                    WHERE video_id = ?
                    """,
                    (now, updated, video_id),
                )

    def undelivered_postgres(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            if limit > 0:
                cursor = self.connection.execute(
                    """
                    SELECT * FROM backfill_videos
                    WHERE status = 'done' AND persisted_at IS NULL
                    ORDER BY created_at ASC, rowid ASC
                    LIMIT ?
                    """,
                    (limit,),
                )
            else:
                cursor = self.connection.execute(
                    """
                    SELECT * FROM backfill_videos
                    WHERE status = 'done' AND persisted_at IS NULL
                    ORDER BY created_at ASC, rowid ASC
                    """
                )
            return [dict(row) for row in cursor.fetchall()]

    def undelivered_indexer(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            if limit > 0:
                cursor = self.connection.execute(
                    """
                    SELECT * FROM backfill_videos
                    WHERE status = 'done'
                      AND indexed_at IS NULL
                      AND pending_article_json IS NOT NULL
                      AND pending_article_json != ''
                    ORDER BY created_at ASC, rowid ASC
                    LIMIT ?
                    """,
                    (limit,),
                )
            else:
                cursor = self.connection.execute(
                    """
                    SELECT * FROM backfill_videos
                    WHERE status = 'done'
                      AND indexed_at IS NULL
                      AND pending_article_json IS NOT NULL
                      AND pending_article_json != ''
                    ORDER BY created_at ASC, rowid ASC
                    """
                )
            return [dict(row) for row in cursor.fetchall()]

    def done_with_envelope(self, limit: int = 0) -> List[Dict[str, Any]]:
        with self._lock:
            if limit > 0:
                cursor = self.connection.execute(
                    """
                    SELECT * FROM backfill_videos
                    WHERE status = 'done'
                      AND pending_article_json IS NOT NULL
                      AND pending_article_json != ''
                    ORDER BY created_at ASC, rowid ASC
                    LIMIT ?
                    """,
                    (limit,),
                )
            else:
                cursor = self.connection.execute(
                    """
                    SELECT * FROM backfill_videos
                    WHERE status = 'done'
                      AND pending_article_json IS NOT NULL
                      AND pending_article_json != ''
                    ORDER BY created_at ASC, rowid ASC
                    """
                )
            return [dict(row) for row in cursor.fetchall()]

    def get_state(self, key: str, default: Optional[str] = None) -> Optional[str]:
        with self._lock:
            cursor = self.connection.execute(
                "SELECT value FROM backfill_state WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
            return str(row["value"]) if row is not None else default

    def set_state(self, key: str, value: str) -> None:
        with self._lock:
            with self.connection:
                self.connection.execute(
                    "INSERT OR REPLACE INTO backfill_state (key, value) VALUES (?, ?)",
                    (key, str(value)),
                )

    def stats(self) -> Dict[str, int]:
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending,
                    COUNT(CASE WHEN status = 'done' THEN 1 END) AS done,
                    COUNT(CASE WHEN status = 'no_transcript' THEN 1 END) AS no_transcript,
                    COUNT(CASE WHEN status = 'error' THEN 1 END) AS error,
                    COUNT(CASE WHEN persisted_at IS NOT NULL THEN 1 END) AS persisted,
                    COUNT(CASE WHEN indexed_at IS NOT NULL THEN 1 END) AS indexed,
                    COUNT(CASE WHEN status = 'done' AND persisted_at IS NULL THEN 1 END) AS pending_postgres,
                    COUNT(CASE WHEN status = 'done' AND indexed_at IS NULL AND pending_article_json IS NOT NULL AND pending_article_json != '' THEN 1 END) AS pending_indexer
                FROM backfill_videos
                """
            )
            row = cursor.fetchone()
            if not row:
                return {
                    "total": 0,
                    "pending": 0,
                    "done": 0,
                    "no_transcript": 0,
                    "error": 0,
                    "persisted": 0,
                    "indexed": 0,
                    "pending_postgres": 0,
                    "pending_indexer": 0,
                }
            return {
                "total": int(row["total"]),
                "pending": int(row["pending"]),
                "done": int(row["done"]),
                "no_transcript": int(row["no_transcript"]),
                "error": int(row["error"]),
                "persisted": int(row["persisted"]),
                "indexed": int(row["indexed"]),
                "pending_postgres": int(row["pending_postgres"]),
                "pending_indexer": int(row["pending_indexer"]),
            }

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    def __enter__(self) -> "BackfillStore":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
