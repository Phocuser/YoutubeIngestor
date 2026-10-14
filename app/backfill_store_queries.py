import json
from datetime import datetime, UTC
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def get(store: Any, video_id: str) -> dict[str, Any] | None:
    with store._lock:
        cursor = store.connection.execute(
            "SELECT * FROM backfill_videos WHERE video_id = ?",
            (video_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None


def register(
    store: Any,
    video_id: str,
    channel_id: str,
    title: str,
    duration: int | None = None,
    published_at: str | None = None,
) -> bool:
    now = _now_iso()
    with store._lock:
        with store.connection:
            cursor = store.connection.execute(
                """
                INSERT OR IGNORE INTO backfill_videos (
                    video_id,
                    channel_id,
                    title,
                    published_at,
                    duration,
                    status,
                    attempts,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?)
                """,
                (video_id, channel_id, title, published_at, duration, now, now),
            )
            inserted = cursor.rowcount > 0
            if published_at is not None:
                store.connection.execute(
                    """
                    UPDATE backfill_videos
                    SET published_at = ?, updated_at = ?
                    WHERE video_id = ?
                    """,
                    (published_at, now, video_id),
                )
            return inserted


def todo(store: Any, limit: int = 0, max_attempts: int = 5) -> list[dict[str, Any]]:
    with store._lock:
        if limit > 0:
            cursor = store.connection.execute(
                """
                SELECT * FROM backfill_videos
                WHERE review_state = 'ready'
                  AND (status = 'pending' OR (status IN ('no_transcript', 'error') AND attempts < ?))
                ORDER BY created_at ASC, rowid ASC
                LIMIT ?
                """,
                (max_attempts, limit),
            )
        else:
            cursor = store.connection.execute(
                """
                SELECT * FROM backfill_videos
                WHERE review_state = 'ready'
                  AND (status = 'pending' OR (status IN ('no_transcript', 'error') AND attempts < ?))
                ORDER BY created_at ASC, rowid ASC
                """,
                (max_attempts,),
            )
        return [dict(row) for row in cursor.fetchall()]


def mark_done(
    store: Any,
    video_id: str,
    published_at: str | None,
    article_uuid: str | None,
    pending_article_json: str | None,
    metadata_json: str | None,
) -> None:
    now = _now_iso()
    with store._lock:
        with store.connection:
            store.connection.execute(
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
    store: Any, video_id: str, published_at: str | None = None
) -> None:
    now = _now_iso()
    with store._lock:
        with store.connection:
            store.connection.execute(
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


def mark_error(store: Any, video_id: str, error: str) -> None:
    now = _now_iso()
    err_msg = str(error)[:500] if error is not None else None
    with store._lock:
        with store.connection:
            store.connection.execute(
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


def quarantine(store: Any, video_id: str, reason: str) -> None:
    now = _now_iso()
    with store._lock:
        with store.connection:
            store.connection.execute(
                """UPDATE backfill_videos
                   SET review_state = 'revision_needed', review_reason = ?,
                       status = 'quarantined', last_error = ?, updated_at = ?
                   WHERE video_id = ?""",
                (reason, reason, now, video_id),
            )


def replace_pending(store: Any, video_id: str, envelope: dict[str, Any]) -> None:
    with store._lock, store.connection:
        store.connection.execute(
            "UPDATE backfill_videos SET pending_article_json = ?, updated_at = ? WHERE video_id = ? AND review_state = 'ready'",
            (json.dumps(envelope), _now_iso(), video_id),
        )


def mark_persisted(
    store: Any, video_id: str, persisted_at: str | None = None
) -> None:
    now = persisted_at or _now_iso()
    updated = _now_iso()
    with store._lock:
        with store.connection:
            store.connection.execute(
                """
                UPDATE backfill_videos
                SET persisted_at = ?,
                    updated_at = ?
                WHERE video_id = ?
                """,
                (now, updated, video_id),
            )


def mark_indexed(
    store: Any, video_id: str, indexed_at: str | None = None
) -> None:
    now = indexed_at or _now_iso()
    updated = _now_iso()
    with store._lock:
        with store.connection:
            store.connection.execute(
                """
                UPDATE backfill_videos
                SET indexed_at = ?,
                    updated_at = ?
                WHERE video_id = ?
                """,
                (now, updated, video_id),
            )


def undelivered_postgres(store: Any, limit: int = 100) -> list[dict[str, Any]]:
    with store._lock:
        if limit > 0:
            cursor = store.connection.execute(
                """
                SELECT * FROM backfill_videos
                WHERE status = 'done' AND persisted_at IS NULL AND review_state = 'ready'
                ORDER BY created_at ASC, rowid ASC
                LIMIT ?
                """,
                (limit,),
            )
        else:
            cursor = store.connection.execute(
                """
                SELECT * FROM backfill_videos
                WHERE status = 'done' AND persisted_at IS NULL AND review_state = 'ready'
                ORDER BY created_at ASC, rowid ASC
                """
            )
        return [dict(row) for row in cursor.fetchall()]


def undelivered_indexer(store: Any, limit: int = 100) -> list[dict[str, Any]]:
    with store._lock:
        if limit > 0:
            cursor = store.connection.execute(
                """
                SELECT * FROM backfill_videos
                WHERE status = 'done'
                  AND indexed_at IS NULL
                  AND persisted_at IS NOT NULL
                  AND review_state = 'ready'
                  AND pending_article_json IS NOT NULL
                  AND pending_article_json != ''
                ORDER BY created_at ASC, rowid ASC
                LIMIT ?
                """,
                (limit,),
            )
        else:
            cursor = store.connection.execute(
                """
                SELECT * FROM backfill_videos
                WHERE status = 'done'
                  AND indexed_at IS NULL
                  AND persisted_at IS NOT NULL
                  AND review_state = 'ready'
                  AND pending_article_json IS NOT NULL
                  AND pending_article_json != ''
                ORDER BY created_at ASC, rowid ASC
                """
            )
        return [dict(row) for row in cursor.fetchall()]


def done_with_envelope(store: Any, limit: int = 0) -> list[dict[str, Any]]:
    with store._lock:
        if limit > 0:
            cursor = store.connection.execute(
                """
                SELECT * FROM backfill_videos
                WHERE status = 'done'
                  AND persisted_at IS NOT NULL
                  AND review_state = 'ready'
                  AND pending_article_json IS NOT NULL
                  AND pending_article_json != ''
                ORDER BY created_at ASC, rowid ASC
                LIMIT ?
                """,
                (limit,),
            )
        else:
            cursor = store.connection.execute(
                """
                SELECT * FROM backfill_videos
                WHERE status = 'done'
                  AND persisted_at IS NOT NULL
                  AND review_state = 'ready'
                  AND pending_article_json IS NOT NULL
                  AND pending_article_json != ''
                ORDER BY created_at ASC, rowid ASC
                """
            )
        return [dict(row) for row in cursor.fetchall()]


def get_state(store: Any, key: str, default: str | None = None) -> str | None:
    with store._lock:
        cursor = store.connection.execute(
            "SELECT value FROM backfill_state WHERE key = ?",
            (key,),
        )
        row = cursor.fetchone()
        return str(row["value"]) if row is not None else default


def set_state(store: Any, key: str, value: str) -> None:
    with store._lock:
        with store.connection:
            store.connection.execute(
                "INSERT OR REPLACE INTO backfill_state (key, value) VALUES (?, ?)",
                (key, str(value)),
            )


def stats(store: Any) -> dict[str, int]:
    with store._lock:
        cursor = store.connection.execute(
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
