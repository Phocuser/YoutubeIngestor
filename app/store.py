import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


class CaptionsStore:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        with self.connection:
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_videos (
                    video_id TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    has_transcript BOOLEAN NOT NULL,
                    pending_article_json TEXT DEFAULT NULL,
                    persisted_at TEXT DEFAULT NULL,
                    indexed_at TEXT DEFAULT NULL,
                    review_state TEXT NOT NULL DEFAULT 'ready',
                    review_reason TEXT
                )
                """
            )
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS service_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cursor = self.connection.execute("PRAGMA table_info(processed_videos)")
            cols = {row["name"] for row in cursor.fetchall()}
            if "pending_article_json" not in cols:
                self.connection.execute(
                    "ALTER TABLE processed_videos ADD COLUMN pending_article_json TEXT DEFAULT NULL"
                )
            if "indexed_at" not in cols:
                self.connection.execute(
                    "ALTER TABLE processed_videos ADD COLUMN indexed_at TEXT DEFAULT NULL"
                )
            if "persisted_at" not in cols:
                self.connection.execute(
                    "ALTER TABLE processed_videos ADD COLUMN persisted_at TEXT DEFAULT NULL"
                )
            if "review_state" not in cols:
                self.connection.execute("ALTER TABLE processed_videos ADD COLUMN review_state TEXT NOT NULL DEFAULT 'ready'")
            if "review_reason" not in cols:
                self.connection.execute("ALTER TABLE processed_videos ADD COLUMN review_reason TEXT")

    def is_processed(self, video_id: str) -> bool:
        cursor = self.connection.execute(
            "SELECT 1 FROM processed_videos WHERE video_id = ?",
            (video_id,),
        )
        return cursor.fetchone() is not None

    def record_video(
        self,
        video_id: str,
        channel_id: str,
        title: str,
        published_at: str,
        has_transcript: bool,
        pending_article_json: Optional[str] = None,
        indexed_at: Optional[str] = None,
    ) -> bool:
        existing = self.get_video(video_id)
        if existing:
            old = json.loads(existing.get("pending_article_json") or "{}").get("raw_content", "")
            new = json.loads(pending_article_json or "{}").get("raw_content", "")
            if old == new:
                return False
            with self.connection:
                self.connection.execute(
                    "UPDATE processed_videos SET review_state = 'revision_needed', review_reason = ? WHERE video_id = ?",
                    ("changed_body_quarantined", video_id),
                )
            return False
        now = datetime.now(timezone.utc).isoformat()
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO processed_videos (
                    video_id, channel_id, title, published_at, processed_at, has_transcript, pending_article_json, indexed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    video_id,
                    channel_id,
                    title,
                    published_at or now,
                    now,
                    1 if has_transcript else 0,
                    pending_article_json,
                    indexed_at,
                ),
            )
        return True

    def mark_indexed(self, video_id: str, indexed_at: Optional[str] = None) -> None:
        now = indexed_at or datetime.now(timezone.utc).isoformat()
        with self.connection:
            self.connection.execute(
                "UPDATE processed_videos SET indexed_at = ? WHERE video_id = ?",
                (now, video_id),
            )

    def mark_persisted(self, video_id: str, persisted_at: Optional[str] = None) -> None:
        now = persisted_at or datetime.now(timezone.utc).isoformat()
        with self.connection:
            self.connection.execute(
                "UPDATE processed_videos SET persisted_at = ? WHERE video_id = ?",
                (now, video_id),
            )

    def undelivered_with_transcript(self, limit: int = 25) -> List[Dict[str, Any]]:
        cursor = self.connection.execute(
            """
            SELECT video_id, channel_id, title, published_at, processed_at, has_transcript, pending_article_json, persisted_at, indexed_at, review_state, review_reason
            FROM processed_videos
            WHERE has_transcript = 1
              AND persisted_at IS NULL
              AND review_state = 'ready'
            ORDER BY processed_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows: List[Dict[str, Any]] = []
        for row in cursor.fetchall():
            d = dict(row)
            d["has_transcript"] = bool(d["has_transcript"])
            rows.append(d)
        return rows

    def get_video(self, video_id: str) -> Optional[Dict[str, Any]]:
        cursor = self.connection.execute(
            """
            SELECT video_id, channel_id, title, published_at, processed_at, has_transcript, pending_article_json, persisted_at, indexed_at, review_state, review_reason
            FROM processed_videos
            WHERE video_id = ?
            """,
            (video_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        d = dict(row)
        d["has_transcript"] = bool(d["has_transcript"])
        return d

    def get_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        cursor = self.connection.execute(
            """
            SELECT video_id, channel_id, title, published_at, processed_at, has_transcript, pending_article_json, indexed_at
            FROM processed_videos
            ORDER BY processed_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows: List[Dict[str, Any]] = []
        for row in cursor.fetchall():
            d = dict(row)
            d["has_transcript"] = bool(d["has_transcript"])
            rows.append(d)
        return rows

    def set_state(self, key: str, value: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection:
            self.connection.execute(
                "INSERT OR REPLACE INTO service_state (key, value, updated_at) VALUES (?, ?, ?)",
                (key, value, now),
            )

    def get_state(self, key: str) -> Optional[str]:
        cursor = self.connection.execute(
            "SELECT value FROM service_state WHERE key = ?",
            (key,),
        )
        row = cursor.fetchone()
        return row["value"] if row else None

    def count(self) -> int:
        cursor = self.connection.execute("SELECT COUNT(*) as cnt FROM processed_videos")
        row = cursor.fetchone()
        return row["cnt"] if row else 0
