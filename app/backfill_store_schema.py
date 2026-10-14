import sqlite3
import threading


def initialize_backfill_schema(connection: sqlite3.Connection, lock: threading.Lock) -> None:
    with lock:
        with connection:
            connection.execute(
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
            for column, definition in (
                ("review_state", "TEXT NOT NULL DEFAULT 'ready'"),
                ("review_reason", "TEXT"),
            ):
                try:
                    connection.execute(f"ALTER TABLE backfill_videos ADD COLUMN {column} {definition}")
                except sqlite3.OperationalError:
                    pass
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS backfill_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
