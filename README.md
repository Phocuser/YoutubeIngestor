# mycelium-youtube-captions

Ingestion service for YouTube channel videos and captions, normalizing transcripts into Mycelium Article envelopes for world-knowledge-graph extraction.

This is **Part 1 of 2**: the ingestion half (poll YouTube channels for new videos, fetch auto-generated/manual captions, and deduplicate). Part 2 will wire stored candidate articles to Mycelium's indexer pipeline.

---

## Important Notice: Configuration Required

> [!IMPORTANT]
> **This service does nothing until `YOUTUBE_CHANNEL_IDS` is explicitly configured.**
> By default, `YOUTUBE_CHANNEL_IDS` is empty. Which channels to monitor is an operator decision. Without configured channel IDs, the polling loop runs without performing any network fetches or recording any videos.

---

## Purpose & How It Fits Into Mycelium

Mycelium extracts events, entities, and relationships from unstructured texts using a deterministic scanner and Aho-Corasick automata (implemented in `cmd/indexer/pipeline.go`).

The pipeline expects incoming articles matching the `Article` shape:
```json
{
  "id": "<youtube video id>",
  "name": "<video title>",
  "source_agency": "<channel name or id>",
  "published_at": "<ISO8601 / RFC3339 date string>",
  "raw_content": "<full transcript text>"
}
```

This service:
1. Polls public YouTube Atom feeds (`https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}`) for configured channels without requiring API keys.
2. Checks SQLite store to deduplicate videos by `video_id`.
3. For each unseen video, fetches transcripts using `youtube-transcript-api` (both manual and auto-generated captions).
4. If captions exist, normalizes the video into the `Article` JSON shape and stores it in the `pending_article_json` column.
5. If captions do not exist (a common case for music, silent clips, or disabled captions), records the video with `has_transcript = 0` so it is not polled or retried forever.
6. Genuine network or API errors propagate without recording the video, enabling automated retry on subsequent poll cycles.

---

## Environment Variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `YOUTUBE_CHANNEL_IDS` | `string` | `""` | Comma-separated list of YouTube Channel IDs to monitor (e.g. `UC_x5XG1OV2P6uZZ5FSM9Ttw,UCxxxxxx`). **Must be configured by operator.** |
| `POLL_INTERVAL_SECONDS` | `int` | `900` | Polling interval in seconds (YouTube RSS feeds don't need aggressive polling). |
| `DATABASE_PATH` | `string` | `data/youtube_captions.sqlite3` | SQLite database path for state and deduplication. |
| `SERVICE_HOST` | `string` | `0.0.0.0` | Bind host for HTTP service. |
| `SERVICE_PORT` | `int` | `8083` | Bind port for HTTP service. |

---

## API Endpoints

- `GET /health`: Returns service health status, last poll timestamp, last error (if any), count of configured channels, and total processed videos count.
- `POST /poll-now`: Triggers an immediate poll cycle across all configured channels outside the regular schedule.

---

## Running Locally

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
Create a `.env` file or export environment variables:
```bash
export YOUTUBE_CHANNEL_IDS="UC_x5XG1OV2P6uZZ5FSM9Ttw"
export POLL_INTERVAL_SECONDS=900
export DATABASE_PATH="data/youtube_captions.sqlite3"
export SERVICE_PORT=8083
```

### 3. Run the service
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8083
```

---

## Running with Docker

Build and run the container:
```bash
docker build -t mycelium-youtube-captions .
docker run -d \
  -p 8083:8083 \
  -e YOUTUBE_CHANNEL_IDS="UC_x5XG1OV2P6uZZ5FSM9Ttw" \
  -v $(pwd)/data:/app/data \
  --name mycelium-youtube-captions \
  mycelium-youtube-captions
```

---

## Running Tests

Run test suite with pytest:
```bash
PYTHONPATH=. pytest tests -v
```
