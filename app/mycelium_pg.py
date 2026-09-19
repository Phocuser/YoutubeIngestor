"""Persist video transcripts into Mycelium's Postgres ``articles`` via its typed
persistence layer (imported from the Mycelium checkout, so no SQL lives here)."""
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

LOGGER = logging.getLogger(__name__)

UUID_PREFIX = "mycelium.youtube.v1/"
STORED = "stored"
ALREADY_PRESENT = "already_present"


def article_uuid(video_id: str) -> str:
    """One id for both Postgres ``articles.id`` and the indexer envelope
    (``event_nodes.article_id`` is a foreign key to it)."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{UUID_PREFIX}{video_id}"))


def watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


class MyceliumPg:
    def __init__(self, pg_url: str, mycelium_dir: str, redis_addr: str = "127.0.0.1:6381"):
        root = str(Path(mycelium_dir).resolve())
        if root not in sys.path:
            sys.path.insert(0, root)
        from persistence import Article, ArticleStore, Database  # noqa: PLC0415

        self._Article = Article
        self._db = Database(pg_url)
        self._store = ArticleStore(self._db)
        host, _, port = redis_addr.partition(":")
        self._redis = (host, int(port or 6379))

    def drain_worker(self, max_idle_batches: int = 1, max_batches: int = 500) -> int:
        """Run Mycelium's consumer-group worker until the candidate stream is idle;
        returns the number of candidates handled."""
        import redis  # noqa: PLC0415
        from worker.consumer import RedisConsumer  # noqa: PLC0415

        client = redis.Redis(host=self._redis[0], port=self._redis[1], decode_responses=True)
        consumer = RedisConsumer(client, self._db, consumer_name="youtube-captions")
        handled = idle = 0
        for _ in range(max_batches):
            n = consumer.run_once(count=10, block_ms=1000)
            handled += n
            idle = 0 if n else idle + 1
            if idle >= max_idle_batches:
                break
        return handled

    def insert(self, envelope: Dict[str, Any], metadata: Dict[str, Any]) -> str:
        """Store one transcript article; returns STORED or ALREADY_PRESENT."""
        from persistence import DuplicateArticleError  # noqa: PLC0415

        video_id = metadata["video_id"]
        article = self._Article(
            id=envelope["id"],
            raw_content=envelope["raw_content"],
            source_url=watch_url(video_id),
            canonical_url=watch_url(video_id),
            source_id=f"yt:{video_id}",
            published_at=envelope.get("published_at") or None,
            source_agency=envelope["source_agency"],
            extraction_version="youtube-backfill-v1",
            metadata=metadata,
        )
        try:
            stored = self._store.insert(article, retrieved_at=datetime.now(timezone.utc))
        except DuplicateArticleError as exc:
            LOGGER.info("already present with different content: %s", exc)
            return ALREADY_PRESENT
        return STORED if stored.id == envelope["id"] else ALREADY_PRESENT
