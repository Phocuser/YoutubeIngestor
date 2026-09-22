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
submitter = MyceliumPg(settings.mycelium_pg_url, settings.mycelium_dir)
service = CaptionsService(settings, store, client, submitter)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start polling loop task in background
    poll_task = asyncio.create_task(service.run_loop())
    yield
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
        "last_poll_at": info["last_poll_at"],
        "last_error": info["last_error"],
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
