import argparse
import json
import logging
from typing import Any, Callable, Dict, List, Optional

try:
    from .backfill_store import BackfillStore
    from .config import Settings
    from .indexer_client import run_indexer
    from .mycelium_pg import MyceliumPg
except ImportError:
    from app.backfill_store import BackfillStore
    from app.config import Settings
    from app.indexer_client import run_indexer
    from app.mycelium_pg import MyceliumPg

LOGGER = logging.getLogger(__name__)


def reindex(
    store: BackfillStore,
    settings: Settings,
    index: Callable = run_indexer,
    drain: Optional[Callable[[], Any]] = None,
    limit: int = 0,
) -> Dict[str, Any]:
    rows = store.done_with_envelope(limit)
    ok_count = 0
    failed_count = 0
    for row in rows:
        video_id = row["video_id"]
        try:
            article = json.loads(row["pending_article_json"])
            res = index(
                article,
                indexer_bin=settings.indexer_bin,
                dict_path=settings.indexer_dict_path,
                markers_path=settings.indexer_markers_path,
                redis_addr=settings.mycelium_redis_addr,
                timeout=120.0,
            )
            if isinstance(res, tuple):
                ok, output = bool(res[0]), (str(res[1]) if len(res) > 1 else "")
            else:
                ok, output = bool(res), ""
        except Exception as exc:
            ok, output = False, str(exc)

        if ok:
            store.mark_indexed(video_id)
            ok_count += 1
        else:
            LOGGER.warning("indexer failed for %s: %s", video_id, output)
            failed_count += 1

    worker_handled = drain() if drain is not None else None
    return {
        "total": len(rows),
        "ok": ok_count,
        "failed": failed_count,
        "worker_handled": worker_handled,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="app.reindex", description="Re-scan stored articles through indexer")
    parser.add_argument("--limit", type=int, default=0, help="maximum number of videos to re-index")
    parser.add_argument("--no-drain", action="store_true", help="skip draining candidate worker")
    args = parser.parse_args(argv)

    settings = Settings()
    store = BackfillStore(settings.backfill_database_path)
    drain = None
    if not args.no_drain:
        drain = MyceliumPg(
            settings.mycelium_pg_url,
            settings.mycelium_dir,
            settings.mycelium_redis_addr,
        ).drain_worker

    result = reindex(store, settings, drain=drain, limit=args.limit)
    print(json.dumps(result))
    return 1 if result["failed"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
