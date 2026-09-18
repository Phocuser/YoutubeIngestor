from unittest.mock import AsyncMock, patch
import pytest
from fastapi.testclient import TestClient

from app.main import app, service


@pytest.fixture
def client():
    # Use TestClient with app, suppressing background lifespan loop during unit testing
    with patch.object(service, "run_loop", new_callable=AsyncMock):
        with TestClient(app) as test_client:
            yield test_client


def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "ok" in data
    assert "status" in data
    assert "channels_configured" in data
    assert "total_processed_videos" in data


def test_poll_now_endpoint(client):
    with patch.object(service, "poll_once", new_callable=AsyncMock) as mock_poll:
        mock_poll.return_value = [
            {"video_id": "v1", "channel_id": "c1", "title": "T1", "has_transcript": True}
        ]
        response = client.post("/poll-now")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "completed"
        assert data["processed_count"] == 1
        assert len(data["processed_videos"]) == 1


def test_poll_now_when_busy(client):
    with patch.object(service, "is_polling", True):
        response = client.post("/poll-now")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "busy"
