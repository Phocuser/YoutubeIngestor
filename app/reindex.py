import argparse
import json
import logging
from typing import Any
from collections.abc import Callable

try:
    from .backfill_store import BackfillStore
    from .config import Settings
except ImportError:
    from app.backfill_store import BackfillStore
    from app.config import Settings

LOGGER = logging.getLogger(__name__)


def reindex(
    store: BackfillStore,
    settings: Settings,
    index: Callable | None = None,
    drain: Callable[[], Any] | None = None,
    limit: int = 0,
) -> dict[str, Any]:
    return {"total": 0, "ok": 0, "failed": 0,
            "worker_handled": 0, "blocked": "LEGACY_PATH_DISABLED"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="app.reindex", description="Disabled legacy indexer compatibility command")
    parser.add_argument("--limit", type=int, default=0, help="maximum number of videos to re-index")
    parser.add_argument("--no-drain", action="store_true", help="skip draining candidate worker")
    args = parser.parse_args(argv)

    settings = Settings()
    store = BackfillStore(settings.backfill_database_path)
    result = reindex(store, settings, limit=args.limit)
    print(json.dumps(result))
    return 1 if result["failed"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
