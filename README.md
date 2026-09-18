# mycelium-youtube-captions

Ingestion service for YouTube channel videos and captions, normalizing transcripts into Mycelium Article envelopes and feeding them into Mycelium's `cmd/indexer` extraction pipeline with retryable delivery tracking.

---

## Important Notice: Configuration Required

> [!IMPORTANT]
> **This service does nothing until `YOUTUBE_CHANNEL_IDS` is explicitly configured.**
> By default, `YOUTUBE_CHANNEL_IDS` is empty. Which channels to monitor is an operator decision. Without configured channel IDs, the polling loop runs without performing any network fetches or recording any videos.

> [!WARNING]
> **Entity Dictionary Coverage Limitation**:
> `INDEXER_DICT_PATH` defaults to `../mycelium/testdata/entity_dict.json` (a small working test fixture) rather than `mycelium/fixtures/entity_dictionary.json`. The large dictionary fixture is currently in the wrong JSON shape for `cmd/indexer -dict` and has unresolved duplicate-key data-quality issues upstream in Mycelium. Until Mycelium's duplicate-key data issue is resolved separately, extraction coverage is intentionally limited to the small test fixture to guarantee clean CLI execution. `mycelium/seeds/incident_markers.json` is the confirmed, working markers file.

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
7. Executes `cmd/indexer` as a subprocess (`[indexer_bin, "-dict", dict_path, "-markers", markers_path, "-redis-addr", redis_addr]`), streaming the normalized `Article` JSON via stdin. On returncode 0, marks `indexed_at` timestamp.
8. Runs a retry sweep at the start of each poll cycle to re-attempt any unindexed articles (`has_transcript = 1 AND indexed_at IS NULL`), wrapped so sweep failures never block polling for new videos.

---

## Environment Variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `YOUTUBE_CHANNEL_IDS` | `string` | `""` | Comma-separated list of YouTube Channel IDs to monitor (e.g. `UC_x5XG1OV2P6uZZ5FSM9Ttw,UCxxxxxx`). **Must be configured by operator.** |
| `POLL_INTERVAL_SECONDS` | `int` | `900` | Polling interval in seconds (YouTube RSS feeds don't need aggressive polling). |
| `DATABASE_PATH` | `string` | `data/youtube_captions.sqlite3` | SQLite database path for state and deduplication. |
| `SERVICE_HOST` | `string` | `0.0.0.0` | Bind host for HTTP service. |
| `SERVICE_PORT` | `int` | `8083` | Bind port for HTTP service. |
| `INDEXER_BIN` | `string` | `../mycelium/bin/indexer` | Path to mycelium `cmd/indexer` Go CLI binary. |
| `INDEXER_DICT_PATH` | `string` | `../mycelium/testdata/entity_dict.json` | Path to entity dictionary JSON fixture (see dictionary coverage limitation note above). |
| `INDEXER_MARKERS_PATH` | `string` | `../mycelium/seeds/incident_markers.json` | Path to incident markers JSON seed file. |
| `MYCELIUM_REDIS_ADDR` | `string` | `127.0.0.1:6381` | Redis host:port for mycelium candidate stream publishing (default 6381 matches docker-compose). |

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
