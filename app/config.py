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
    poll_interval_seconds: int = int(os.getenv("POLL_INTERVAL_SECONDS", "900"))
    database_path: str = os.getenv("DATABASE_PATH", "data/youtube_captions.sqlite3")
    service_host: str = os.getenv("SERVICE_HOST", "0.0.0.0")
    service_port: int = int(os.getenv("SERVICE_PORT", "8083"))
    indexer_bin: str = os.getenv("INDEXER_BIN", "../mycelium/bin/indexer")
    indexer_dict_path: str = os.getenv(
        "INDEXER_DICT_PATH", "../mycelium/testdata/entity_dict.json"
    )
    indexer_markers_path: str = os.getenv(
        "INDEXER_MARKERS_PATH", "../mycelium/seeds/incident_markers.json"
    )
    mycelium_redis_addr: str = os.getenv("MYCELIUM_REDIS_ADDR", "127.0.0.1:6381")
