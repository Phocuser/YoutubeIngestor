# mycelium-youtube-captions

Ingestion service for YouTube channel videos and timed captions. Polling and
backfill produce the same reversible capture envelope and submit it through
Mycelium's durable capture boundary. This service does not own a graph UUID,
Postgres schema, Redis stream, extraction worker, or indexer process.

---

## Important Notice: Configuration Required

> [!IMPORTANT]
> **This service does nothing until `YOUTUBE_CHANNEL_IDS` is explicitly configured.**
> By default, `YOUTUBE_CHANNEL_IDS` is empty. Which channels to monitor is an operator decision. Without configured channel IDs, the polling loop runs without performing any network fetches or recording any videos.


---

## Purpose & How It Fits Into Mycelium

Mycelium may extract events, entities, and relationships downstream. This
adapter only captures source evidence and metadata.

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
6. Genuine network or API errors record a retryable caption gap, enabling automated retry on subsequent poll cycles without claiming successful processing.
7. Submits the canonical timed track through the shared durable capture receipt boundary. Failed delivery remains in a retryable local spool.
8. Keeps ad, self-promo, and interaction decisions as a versioned derived view. Raw timed captions are never deleted or overwritten; changed bodies are quarantined.

---

## Environment Variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `YOUTUBE_CHANNEL_IDS` | `string` | `""` | Comma-separated list of YouTube Channel IDs to monitor (e.g. `UC_x5XG1OV2P6uZZ5FSM9Ttw,UCxxxxxx`). **Must be configured by operator.** |
| `POLL_INTERVAL_SECONDS` | `int` | `900` | Polling interval in seconds (YouTube RSS feeds don't need aggressive polling). |
| `DATABASE_PATH` | `string` | `data/youtube_captions.sqlite3` | SQLite database path for state and deduplication. |
| `SERVICE_HOST` | `string` | `0.0.0.0` | Bind host for HTTP service. |
| `SERVICE_PORT` | `int` | `8083` | Bind port for HTTP service. |
| `MYCELIUM_PG_URL` | `string` | unset | Explicit opt-in durable Mycelium Postgres URL. When unset or blank, submission is disabled and no Mycelium database is opened. |
| `MYCELIUM_DIR` | `string` | `../mycelium` | Filesystem path used by the Mycelium adapter if `MYCELIUM_PG_URL` is explicitly configured; this setting alone never enables database access. |

---

## API Endpoints

- `GET /health`: Returns process health, durable-ingestion and polling enablement, last poll timestamp, last error (if any), configured channel count, and total processed video count. A healthy process may still have ingestion disabled; check both enablement fields before accepting data.
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
docker buildx build --build-context mycelium-source=/path/to/clean/mycelium-source \
  --tag mycelium-youtube-captions --load .
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


---

## Channel backfill (transcripts without the ad reads)

`app/backfill.py` ingests a whole channel's spoken content, resumable and unattended:

```bash
.venv/bin/python -m app.backfill --limit 5          # try a few
.venv/bin/python -m app.backfill --limit 100 --backfill-days 30  # recent, bounded window
scripts/overnight.sh                                # everything, re-running through throttling
.venv/bin/python -m app.backfill --stats            # progress
```

`--backfill-days` accepts 1–90 and requires `--limit` from 1–1000. Videos
with missing or malformed publication dates are left unprocessed and make the
run report incomplete; existing queued rows and delivery retries are filtered
to the same date window.

Flow: `yt-dlp` lists the channel -> timed captions are normalized by the same
shared builder used by polling -> a versioned editorial view records any
SponsorBlock/heuristic decisions and ranges without removing source segments ->
the durable Mycelium capture receipt boundary accepts the evidence. The source
alias is `youtube:<video_id>`; it is not a graph UUID. Backfill completion is
not proof that downstream indexing has completed.

Ad removal: SponsorBlock's community segments (`sponsor`, `selfpromo`, `interaction`) are used when they exist; otherwise a conservative keyword heuristic anchored on the spoken transitions ("before we go any further" ... "let's get back to"). What was removed is recorded per article in `metadata` (`ads_removed_seconds`, `ad_removal_sources`).

State is in `data/backfill.sqlite3`; YouTube throttling triggers long backoffs and a resumed round, never lost work. Videos with no captions are recorded as `no_transcript`.

### Managed source one-shot worker

The released managed YouTube control plane can be exercised with one bounded
worker pass. Set the source token in the protected
`MYCELIUM_YOUTUBE_SOURCE_TOKEN` environment variable. The command requires
the Mycelium URL and does not read credentials from source files or process
arguments:

```bash
export MYCELIUM_YOUTUBE_SOURCE_TOKEN
python -m app.managed_worker_cli \
  --mycelium-url "https://mycelium-staging.example.invalid" \
  --worker-id "youtube-worker-1" \
  --batch-limit 1
```

The one-shot runner leases one job per pass, processes only the
leased source's bounded date window, submits typed timed captions, and prints
completion outcomes before exiting. A successful outcome confirms durable
capture admission; it does not confirm downstream indexing or materialization.
