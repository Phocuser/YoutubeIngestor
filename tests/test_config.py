import os
from unittest.mock import patch
from app.config import Settings


def test_settings_defaults():
    with patch.dict(os.environ, {}, clear=True):
        settings = Settings()
        assert settings.indexer_bin == "../mycelium/bin/indexer"
        assert settings.indexer_dict_path == "../mycelium/testdata/entity_dict.json"
        assert settings.indexer_markers_path == "../mycelium/seeds/incident_markers.json"
        assert settings.mycelium_redis_addr == "127.0.0.1:6381"
        assert settings.poll_interval_seconds == 900
        assert settings.database_path == "data/youtube_captions.sqlite3"


def test_settings_custom_env():
    custom_env = {
        "INDEXER_BIN": "/custom/bin/indexer",
        "INDEXER_DICT_PATH": "/custom/dict.json",
        "INDEXER_MARKERS_PATH": "/custom/markers.json",
        "MYCELIUM_REDIS_ADDR": "redis.internal:6379",
        "POLL_INTERVAL_SECONDS": "300",
    }
    with patch.dict(os.environ, custom_env, clear=True):
        settings = Settings(
            poll_interval_seconds=int(os.environ["POLL_INTERVAL_SECONDS"]),
            indexer_bin=os.environ["INDEXER_BIN"],
            indexer_dict_path=os.environ["INDEXER_DICT_PATH"],
            indexer_markers_path=os.environ["INDEXER_MARKERS_PATH"],
            mycelium_redis_addr=os.environ["MYCELIUM_REDIS_ADDR"],
        )
        assert settings.indexer_bin == "/custom/bin/indexer"
        assert settings.indexer_dict_path == "/custom/dict.json"
        assert settings.indexer_markers_path == "/custom/markers.json"
        assert settings.mycelium_redis_addr == "redis.internal:6379"
        assert settings.poll_interval_seconds == 300
