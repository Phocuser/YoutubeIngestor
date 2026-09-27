from unittest.mock import MagicMock, patch
import pytest
import httpx
from youtube_transcript_api._errors import (
    NoTranscriptFound,
    TranscriptsDisabled,
    RequestBlocked,
)

from app.youtube_client import (
    TRANSCRIPT_HTTP_TIMEOUT_SECONDS,
    TranscriptTimeoutSession,
    fetch_channel_feed,
    fetch_transcript,
    parse_feed_xml,
    YouTubeClient,
)

SAMPLE_ATOM_FEED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015" xmlns:media="http://search.yahoo.com/mrss/" xmlns="http://www.w3.org/2005/Atom">
 <link rel="self" href="http://www.youtube.com/feeds/videos.xml?channel_id=UC_x5XG1OV2P6uZZ5FSM9Ttw"/>
 <id>yt:channel:UC_x5XG1OV2P6uZZ5FSM9Ttw</id>
 <yt:channelId>UC_x5XG1OV2P6uZZ5FSM9Ttw</yt:channelId>
 <title>Google for Developers</title>
 <link rel="alternate" href="https://www.youtube.com/channel/UC_x5XG1OV2P6uZZ5FSM9Ttw"/>
 <author>
  <name>Google for Developers</name>
  <uri>https://www.youtube.com/channel/UC_x5XG1OV2P6uZZ5FSM9Ttw</uri>
 </author>
 <published>2007-08-23T00:34:43+00:00</published>
 <entry>
  <id>yt:video:FVkp6tc2rNY</id>
  <yt:videoId>FVkp6tc2rNY</yt:videoId>
  <yt:channelId>UC_x5XG1OV2P6uZZ5FSM9Ttw</yt:channelId>
  <title>Gemini Live API in action</title>
  <link rel="alternate" href="https://www.youtube.com/shorts/FVkp6tc2rNY"/>
  <author>
   <name>Google for Developers</name>
   <uri>https://www.youtube.com/channel/UC_x5XG1OV2P6uZZ5FSM9Ttw</uri>
  </author>
  <published>2026-09-15T19:00:17+00:00</published>
  <updated>2026-09-18T05:28:39+00:00</updated>
 </entry>
 <entry>
  <id>yt:video:abc123def45</id>
  <yt:videoId>abc123def45</yt:videoId>
  <yt:channelId>UC_x5XG1OV2P6uZZ5FSM9Ttw</yt:channelId>
  <title>What is Mycelium Knowledge Graph?</title>
  <link rel="alternate" href="https://www.youtube.com/watch?v=abc123def45"/>
  <author>
   <name>Google for Developers</name>
   <uri>https://www.youtube.com/channel/UC_x5XG1OV2P6uZZ5FSM9Ttw</uri>
  </author>
  <published>2026-09-16T10:30:00+00:00</published>
  <updated>2026-09-18T06:00:00+00:00</updated>
 </entry>
</feed>
"""


def test_parse_feed_xml_realistic():
    """Verify parsing a realistic sample Atom+yt: feed XML into expected dict list."""
    items = parse_feed_xml(SAMPLE_ATOM_FEED_XML, default_channel_id="UC_x5XG1OV2P6uZZ5FSM9Ttw")
    assert len(items) == 2

    first = items[0]
    assert first["video_id"] == "FVkp6tc2rNY"
    assert first["title"] == "Gemini Live API in action"
    assert first["published_at"] == "2026-09-15T19:00:17+00:00"
    assert first["channel_name"] == "Google for Developers"
    assert first["channel_id"] == "UC_x5XG1OV2P6uZZ5FSM9Ttw"

    second = items[1]
    assert second["video_id"] == "abc123def45"
    assert second["title"] == "What is Mycelium Knowledge Graph?"
    assert second["published_at"] == "2026-09-16T10:30:00+00:00"
    assert second["channel_name"] == "Google for Developers"
    assert second["channel_id"] == "UC_x5XG1OV2P6uZZ5FSM9Ttw"


def test_fetch_channel_feed_http_mock():
    """Verify fetch_channel_feed calls HTTP GET and parses returned XML."""
    mock_resp = httpx.Response(
        200,
        text=SAMPLE_ATOM_FEED_XML,
        request=httpx.Request("GET", "https://www.youtube.com/feeds/videos.xml?channel_id=UC_x5XG1OV2P6uZZ5FSM9Ttw"),
    )
    with patch("httpx.Client.get", return_value=mock_resp) as mock_get:
        items = fetch_channel_feed("UC_x5XG1OV2P6uZZ5FSM9Ttw")
        mock_get.assert_called_once_with("https://www.youtube.com/feeds/videos.xml?channel_id=UC_x5XG1OV2P6uZZ5FSM9Ttw")
        assert len(items) == 2
        assert items[0]["video_id"] == "FVkp6tc2rNY"


def test_fetch_channel_feed_http_error():
    """Verify fetch_channel_feed propagates HTTP errors."""
    mock_resp = httpx.Response(
        404,
        request=httpx.Request("GET", "https://www.youtube.com/feeds/videos.xml?channel_id=INVALID"),
    )
    with patch("httpx.Client.get", return_value=mock_resp):
        with pytest.raises(httpx.HTTPStatusError):
            fetch_channel_feed("INVALID")


def test_fetch_transcript_success():
    """Verify fetch_transcript returns joined text when captions exist."""
    snippet1 = MagicMock()
    snippet1.text = "Hello world."
    snippet2 = MagicMock()
    snippet2.text = "Welcome to the video."

    mock_api_instance = MagicMock()
    mock_api_instance.fetch.return_value = [snippet1, snippet2]

    with patch("app.youtube_client.YouTubeTranscriptApi", return_value=mock_api_instance) as api:
        result = fetch_transcript("FVkp6tc2rNY")
        assert result == "Hello world. Welcome to the video."
        session = api.call_args.kwargs["http_client"]
        assert session.timeout == TRANSCRIPT_HTTP_TIMEOUT_SECONDS


def test_fetch_transcript_legacy_instance_fallback_uses_injected_session():
    class LegacyApi:
        def get_transcript(self, video_id):
            return [{"text": f"legacy {video_id}"}]

    with patch("app.youtube_client.YouTubeTranscriptApi", return_value=LegacyApi()) as api:
        assert fetch_transcript("legacy-video") == "legacy legacy-video"
        session = api.call_args.kwargs["http_client"]
        assert session.timeout == TRANSCRIPT_HTTP_TIMEOUT_SECONDS


def test_fetch_transcript_no_transcript_disabled():
    """Verify fetch_transcript returns None gracefully when TranscriptsDisabled."""
    mock_api_instance = MagicMock()
    mock_api_instance.fetch.side_effect = TranscriptsDisabled("FVkp6tc2rNY")

    with patch("app.youtube_client.YouTubeTranscriptApi", return_value=mock_api_instance):
        result = fetch_transcript("FVkp6tc2rNY")
        assert result is None


def test_fetch_transcript_no_transcript_found():
    """Verify fetch_transcript returns None gracefully when NoTranscriptFound."""
    mock_api_instance = MagicMock()
    mock_api_instance.fetch.side_effect = NoTranscriptFound("FVkp6tc2rNY", ["en"], None)

    with patch("app.youtube_client.YouTubeTranscriptApi", return_value=mock_api_instance):
        result = fetch_transcript("FVkp6tc2rNY")
        assert result is None


def test_fetch_transcript_genuine_error_propagates():
    """Verify genuine network/API errors propagate out of fetch_transcript."""
    mock_api_instance = MagicMock()
    mock_api_instance.fetch.side_effect = RequestBlocked("FVkp6tc2rNY")

    with patch("app.youtube_client.YouTubeTranscriptApi", return_value=mock_api_instance):
        with pytest.raises(RequestBlocked):
            fetch_transcript("FVkp6tc2rNY")


def test_transcript_session_sets_explicit_request_timeout_without_network():
    session = TranscriptTimeoutSession()
    response = MagicMock()
    with patch("requests.Session.request", return_value=response) as request:
        assert session.get("https://example.invalid/captions") is response
        assert request.call_args.kwargs["timeout"] == TRANSCRIPT_HTTP_TIMEOUT_SECONDS
        session.get("https://example.invalid/captions", timeout=2.5)
        assert request.call_args.kwargs["timeout"] == 2.5
    session.close()


def test_youtube_client_wrapper():
    """Verify YouTubeClient methods delegate properly."""
    client = YouTubeClient()
    with patch("app.youtube_client.fetch_channel_feed", return_value=[{"video_id": "v1"}]) as mock_feed:
        with patch("app.youtube_client.fetch_transcript", return_value="hello") as mock_transcript:
            assert client.fetch_channel_feed("c1") == [{"video_id": "v1"}]
            mock_feed.assert_called_once_with("c1", timeout=30.0)
            assert client.fetch_transcript("v1") == "hello"
            mock_transcript.assert_called_once_with("v1")
