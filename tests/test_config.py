import os
from unittest.mock import patch
from app.config import Settings


def test_settings_defaults():
    with patch.dict(os.environ, {}, clear=True):
        settings = Settings()
        assert settings.poll_interval_seconds == 1800
        assert settings.database_path == "data/youtube_captions.sqlite3"
        assert settings.mycelium_pg_url == ""
        assert settings.mycelium_dir == "../mycelium"
        assert settings.backfill_channels == ["@warographics643", "@HomeFronts"]


def test_settings_custom_env():
    custom_env = {
        "POLL_INTERVAL_SECONDS": "300",
    }
    with patch.dict(os.environ, custom_env, clear=True):
        settings = Settings(
            poll_interval_seconds=int(os.environ["POLL_INTERVAL_SECONDS"]),
        )
        assert settings.poll_interval_seconds == 300


def test_mycelium_url_is_explicit_opt_in():
    with patch.dict(
        os.environ,
        {"MYCELIUM_PG_URL": "postgresql://db.example/mycelium", "MYCELIUM_DIR": "/opt/mycelium"},
        clear=True,
    ):
        settings = Settings()

    assert settings.mycelium_pg_url == "postgresql://db.example/mycelium"
    assert settings.mycelium_dir == "/opt/mycelium"
