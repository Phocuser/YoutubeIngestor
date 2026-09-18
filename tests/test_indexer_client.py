import subprocess
from unittest.mock import MagicMock, patch
import pytest

from app.indexer_client import run_indexer


def test_run_indexer_success():
    article = {
        "id": "vid-123",
        "name": "Test Video",
        "source_agency": "Test Channel",
        "published_at": "2026-09-18T12:00:00Z",
        "raw_content": "Transcript text here.",
    }
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = "candidates: 2"
    mock_proc.stderr = ""

    with patch("app.indexer_client.subprocess.run", return_value=mock_proc) as mock_run:
        success, output = run_indexer(
            article,
            indexer_bin="/path/to/indexer",
            dict_path="/path/to/dict.json",
            markers_path="/path/to/markers.json",
            redis_addr="127.0.0.1:6379",
            timeout=15.0,
        )

        assert success is True
        assert output == "candidates: 2"
        mock_run.assert_called_once()
        args, kwargs = mock_run.call_args
        cmd = args[0]
        assert cmd == [
            "/path/to/indexer",
            "-dict",
            "/path/to/dict.json",
            "-markers",
            "/path/to/markers.json",
            "-redis-addr",
            "127.0.0.1:6379",
        ]
        assert "vid-123" in kwargs["input"]
        assert kwargs["timeout"] == 15.0
        assert kwargs["text"] is True
        assert kwargs["capture_output"] is True


def test_run_indexer_nonzero_exit():
    article = {"id": "vid-bad", "name": "Bad", "raw_content": "text"}
    mock_proc = MagicMock()
    mock_proc.returncode = 1
    mock_proc.stdout = ""
    mock_proc.stderr = "error loading dictionary: invalid character"

    with patch("app.indexer_client.subprocess.run", return_value=mock_proc):
        success, output = run_indexer(
            article,
            indexer_bin="/path/to/indexer",
            dict_path="/path/to/dict.json",
            markers_path="/path/to/markers.json",
            redis_addr="127.0.0.1:6379",
        )

        assert success is False
        assert "invalid character" in output


def test_run_indexer_file_not_found_handled_gracefully():
    article = {"id": "vid-missing", "name": "Missing", "raw_content": "text"}

    with patch("app.indexer_client.subprocess.run", side_effect=FileNotFoundError("No such file")):
        success, output = run_indexer(
            article,
            indexer_bin="/path/to/nonexistent",
            dict_path="/path/to/dict.json",
            markers_path="/path/to/markers.json",
            redis_addr="127.0.0.1:6379",
        )

        assert success is False
        assert "not found" in output.lower()
        assert "/path/to/nonexistent" in output


def test_run_indexer_timeout_handled_gracefully():
    article = {"id": "vid-timeout", "name": "Timeout", "raw_content": "text"}

    with patch(
        "app.indexer_client.subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd=["indexer"], timeout=5.0),
    ):
        success, output = run_indexer(
            article,
            indexer_bin="/path/to/indexer",
            dict_path="/path/to/dict.json",
            markers_path="/path/to/markers.json",
            redis_addr="127.0.0.1:6379",
            timeout=5.0,
        )

        assert success is False
        assert "timed out" in output.lower()
        assert "5.0" in output
