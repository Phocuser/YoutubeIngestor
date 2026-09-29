import asyncio
from contextlib import asynccontextmanager
import logging
from typing import Any, Dict

from fastapi import FastAPI

from .config import Settings
from .service import CaptionsService
from .store import CaptionsStore
from .youtube_client import YouTubeClient
from .mycelium_pg import MyceliumPg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
LOGGER = logging.getLogger("mycelium-youtube-captions")

settings = Settings()
store = CaptionsStore(settings.database_path)
client = YouTubeClient()
submitter = None
if settings.mycelium_pg_url.strip():
    try:
        submitter = MyceliumPg(settings.mycelium_pg_url, settings.mycelium_dir)
    except (ImportError, ModuleNotFoundError) as exc:
        # Keep the HTTP entrypoint importable in adapter-only/test environments;
        # delivery remains disabled until the explicit Mycelium boundary exists.
        LOGGER.warning("Mycelium capture boundary unavailable: %s", exc)
service = CaptionsService(settings, store, client, submitter)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # A missing durable boundary is an explicit disabled state, not a reason
    # to start a poll loop that can only accumulate undeliverable work.
    poll_task = asyncio.create_task(service.run_loop()) if service.submitter is not None else None
    yield
    if poll_task is not None:
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="Mycelium YouTube Captions Ingestion Service",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> Dict[str, Any]:
    info = service.check_health()
    return {
        "ok": info["status"] == "healthy",
        "status": info["status"],
        "channels_configured": info["channels_configured"],
        "durable_ingestion_enabled": info["durable_ingestion_enabled"],
        "polling_enabled": info["polling_enabled"],
        "last_poll_at": info["last_poll_at"],
        "last_source_check_at": info["last_source_check_at"],
        "last_error": info["last_error"],
        "last_poll_status": info["last_poll_status"],
        "paused_until": info["paused_until"],
        "total_processed_videos": info["total_processed_videos"],
    }


@app.post("/poll-now")
async def poll_now() -> Dict[str, Any]:
    if service.is_polling:
        return {"status": "busy", "message": "Poll already in progress"}
    processed = await service.poll_once()
    return {
        "status": "completed",
        "processed_count": len(processed),
        "processed_videos": processed,
    }
