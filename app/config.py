import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _parse_channel_ids(raw: str | None = None) -> list[str]:
    raw_val = os.getenv("YOUTUBE_CHANNEL_IDS", "") if raw is None else raw
    return [cid.strip() for cid in raw_val.split(",") if cid.strip()]


@dataclass(frozen=True)
class Settings:
    youtube_channel_ids: list[str] = field(
        default_factory=lambda: _parse_channel_ids(os.getenv("YOUTUBE_CHANNEL_IDS", ""))
    )
    poll_interval_seconds: int = int(os.getenv("POLL_INTERVAL_SECONDS", "1800"))
    database_path: str = os.getenv("DATABASE_PATH", "data/youtube_captions.sqlite3")
    service_host: str = os.getenv("SERVICE_HOST", "0.0.0.0")
    service_port: int = int(os.getenv("SERVICE_PORT", "8083"))
    mycelium_pg_url: str = field(
        default_factory=lambda: os.getenv("MYCELIUM_PG_URL", "")
    )
    mycelium_dir: str = field(default_factory=lambda: os.getenv("MYCELIUM_DIR", "../mycelium"))
    backfill_channels: list[str] = field(
        default_factory=lambda: _parse_channel_ids(
            os.getenv("BACKFILL_CHANNELS", "@warographics643,@HomeFronts")
        )
    )
    backfill_database_path: str = os.getenv(
        "BACKFILL_DATABASE_PATH", "data/backfill.sqlite3"
    )
