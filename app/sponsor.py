"""Sponsor and ad-read segment detection and removal."""

import json
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Sequence, Tuple

LOGGER = logging.getLogger(__name__)

DEFAULT_CATEGORIES: Tuple[str, ...] = ("sponsor", "selfpromo", "interaction")


@dataclass(frozen=True)
class Snippet:
    """A single caption snippet with text, start timestamp, and duration."""

    text: str
    start: float
    duration: float


@dataclass(frozen=True)
class CleanResult:
    """Result of cleaning transcript snippets of ad/sponsor content."""

    text: str
    removed_seconds: float
    removed_snippets: int
    total_snippets: int
    sources: List[str]
    ranges: List[Tuple[float, float]]
    decisions: List[dict] = None
    policy_version: str = "ads-v1"


class SponsorBlockUnavailable(Exception):
    """Raised when the SponsorBlock API cannot be reached or returns an error."""


def _normalize_snippets(snippets: Sequence[Any]) -> List[Snippet]:
    """Convert input snippets to a list of Snippet dataclasses."""
    result: List[Snippet] = []
    for s in snippets:
        if isinstance(s, Snippet):
            result.append(s)
        elif isinstance(s, dict):
            result.append(
                Snippet(
                    text=str(s.get("text", "")),
                    start=float(s.get("start", 0.0)),
                    duration=float(s.get("duration", 0.0)),
                )
            )
        else:
            result.append(
                Snippet(
                    text=str(getattr(s, "text", "")),
                    start=float(getattr(s, "start", 0.0)),
                    duration=float(getattr(s, "duration", 0.0)),
                )
            )
    return result


def effective_end(snippet: Snippet, next_snippet: Optional[Snippet] = None) -> float:
    """Compute effective end time of a snippet bounded by the next snippet's start."""
    declared_end = snippet.start + snippet.duration
    if next_snippet is not None and next_snippet.start > snippet.start:
        return min(declared_end, next_snippet.start)
    return declared_end


def effective_duration(snippet: Snippet, next_snippet: Optional[Snippet] = None) -> float:
    """Compute effective duration of a snippet."""
    return max(0.0, effective_end(snippet, next_snippet) - snippet.start)


def fetch_sponsor_segments(
    video_id: str,
    categories: Sequence[str] = DEFAULT_CATEGORIES,
    timeout: float = 15.0,
    opener: Optional[Callable[..., Any]] = None,
) -> List[Tuple[float, float]]:
    """Fetch skip/mute segments for video_id from SponsorBlock API."""
    encoded_categories = urllib.parse.quote(json.dumps(list(categories)))
    url = f"https://sponsor.ajay.app/api/skipSegments?videoID={urllib.parse.quote(video_id)}&categories={encoded_categories}"

    try:
        if opener is not None:
            try:
                resp = opener(url, timeout=timeout)
            except TypeError:
                resp = opener(url)
        else:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "mycelium-youtube-captions"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                resp = r.read()

        if hasattr(resp, "read"):
            raw_bytes = resp.read()
        elif isinstance(resp, (bytes, bytearray)):
            raw_bytes = bytes(resp)
        elif isinstance(resp, str):
            raw_bytes = resp.encode("utf-8")
        else:
            raw_bytes = bytes(resp)

        data = json.loads(raw_bytes.decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise SponsorBlockUnavailable(f"SponsorBlock returned HTTP {e.code}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise SponsorBlockUnavailable(f"SponsorBlock connection error: {e}") from e
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise SponsorBlockUnavailable(f"Invalid JSON from SponsorBlock: {e}") from e
    except SponsorBlockUnavailable:
        raise
    except Exception as e:
        if getattr(e, "code", None) == 404 or getattr(e, "status", None) == 404:
            return []
        raise SponsorBlockUnavailable(f"Error fetching SponsorBlock segments: {e}") from e

    if not isinstance(data, list):
        raise SponsorBlockUnavailable("Unexpected SponsorBlock response format")

    results: List[Tuple[float, float]] = []
    category_set = set(categories)
    for item in data:
        if not isinstance(item, dict):
            continue
        action_type = item.get("actionType", "skip")
        if action_type == "full":
            continue
        if action_type not in ("skip", "mute"):
            continue
        cat = item.get("category")
        if cat not in category_set:
            continue
        segment = item.get("segment")
        if not (isinstance(segment, (list, tuple)) and len(segment) >= 2):
            continue
        try:
            start = float(segment[0])
            end = float(segment[1])
        except (ValueError, TypeError):
            continue
        if end <= start or end <= 0:
            continue
        results.append((start, end))

    results.sort(key=lambda r: (r[0], r[1]))
    return results


def merge_ranges(
    ranges: Sequence[Tuple[float, float]], gap: float = 1.0
) -> List[Tuple[float, float]]:
    """Sort and merge overlapping ranges and ranges closer than gap seconds."""
    valid = [(float(s), float(e)) for s, e in ranges if float(e) > float(s)]
    if not valid:
        return []
    sorted_ranges = sorted(valid, key=lambda r: (r[0], r[1]))
    merged: List[Tuple[float, float]] = []
    cur_start, cur_end = sorted_ranges[0]
    for s, e in sorted_ranges[1:]:
        if s <= cur_end + gap:
            cur_end = max(cur_end, e)
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = s, e
    merged.append((cur_start, cur_end))
    return merged


AD_PATTERNS = [
    r"premium\s+subscribers?",
    r"sponsored\s+by",
    r"our\s+sponsor",
    r"brought\s+to\s+you\s+by",
    r"use\s+(?:the\s+)?(?:promo\s+)?code",
    r"promo\s+code",
    r"link\s+in\s+the\s+(?:description|comments)",
    r"%\s*off",
    r"free\s+trial",
    r"our\s+friends\s+at",
    r"patreon",
    r"fronts\.co",
    r"subscribe\s+to\s+(?:our|the)",
]

COMPILED_AD_PATTERNS = [re.compile(p, re.IGNORECASE) for p in AD_PATTERNS]

STRONG_AD_PATTERN = re.compile(
    r"sponsored\s+by|brought\s+to\s+you\s+by|promo\s+code|use\s+(?:the\s+)?(?:promo\s+)?code",
    re.IGNORECASE,
)


MAX_AD_SECONDS = 150.0
AD_INTRO_LOOKBACK = 20.0
AD_OUTRO_LOOKAHEAD = 25.0

# Spoken transitions that open / close an ad read; the cut is anchored on these
# rather than on fixed padding so real content next to the ad survives.
AD_INTRO_CUES = re.compile(
    r"pause (this|the) (episode|video)|before we (go any further|continue|get (in|into|started))"
    r"|quick (word|break|message)|a word from|take a (quick )?moment|we'd like to (tell|talk)"
    r"|want to tell you|let me tell you|this (video|episode) is (sponsored|brought)",
    re.IGNORECASE,
)
AD_OUTRO_CUES = re.compile(
    r"(let's|let us|now,? (we )?(can|will)?) ?get back to|back to (the|our) (video|episode|story|topic)"
    r"|and now,? (let's|back)|on with the (show|video)|without further ado",
    re.IGNORECASE,
)


def _ad_start(norm: List[Snippet], first_idx: int) -> float:
    """Start of the ad: the nearest intro cue shortly before the first hit, else just before it."""
    first = norm[first_idx]
    for j in range(first_idx, -1, -1):
        if first.start - norm[j].start > AD_INTRO_LOOKBACK:
            break
        if AD_INTRO_CUES.search(norm[j].text):
            return norm[j].start
    return max(0.0, first.start - 3.0)


def _ad_end(norm: List[Snippet], last_idx: int) -> float:
    """End of the ad: the transition back to content shortly after the last hit,
    else the end of the sentence following it."""
    n = len(norm)
    last = norm[last_idx]
    for j in range(last_idx, n):
        if norm[j].start - last.start > AD_OUTRO_LOOKAHEAD:
            break
        if AD_OUTRO_CUES.search(norm[j].text):
            return effective_end(norm[j], norm[j + 1] if j + 1 < n else None)
    end_idx = last_idx
    while end_idx + 1 < n and not re.search(r"[.!?]\s*$", norm[end_idx].text):
        end_idx += 1
        if norm[end_idx].start - last.start > 10.0:
            break
    return effective_end(norm[end_idx], norm[end_idx + 1] if end_idx + 1 < n else None)


def heuristic_ad_ranges(snippets: Sequence[Any]) -> List[Tuple[float, float]]:
    """Detect sponsorship / ad ranges using keyword clustering."""
    norm = _normalize_snippets(snippets)
    if not norm:
        return []

    n = len(norm)
    # Detect hits
    hits: List[Tuple[int, Snippet, bool]] = []
    for i, s in enumerate(norm):
        text = s.text
        prev = norm[i - 1].text if i > 0 else ""
        combined = f"{prev} {text}"
        # A pattern counts once: inside this snippet, or spanning the boundary with
        # the previous one (not when the previous snippet already matched by itself).
        matched = [
            p
            for p in COMPILED_AD_PATTERNS
            if p.search(text) or (i > 0 and p.search(combined) and not p.search(prev))
        ]
        if matched:
            is_strong = any(
                STRONG_AD_PATTERN.search(text)
                or (i > 0 and STRONG_AD_PATTERN.search(combined) and not STRONG_AD_PATTERN.search(prev))
                for _ in (0,)
            )
            hits.append((i, s, is_strong))

    if not hits:
        return []

    # Group hits into clusters (hit within 30s of previous hit joins cluster)
    clusters: List[List[Tuple[int, Snippet, bool]]] = []
    for item in hits:
        if not clusters:
            clusters.append([item])
        else:
            prev_snip = clusters[-1][-1][1]
            cur_snip = item[1]
            if cur_snip.start - prev_snip.start <= 30.0:
                clusters[-1].append(item)
            else:
                clusters.append([item])

    raw_ranges: List[Tuple[float, float]] = []
    for cluster in clusters:
        has_strong = any(h[2] for h in cluster)
        if len(cluster) < 2 and not has_strong:
            continue

        first_idx = cluster[0][0]
        last_idx = cluster[-1][0]
        start = _ad_start(norm, first_idx)
        end = _ad_end(norm, last_idx)
        if end - start > MAX_AD_SECONDS:
            end = start + MAX_AD_SECONDS
        raw_ranges.append((start, end))

    return merge_ranges(raw_ranges)


def strip_ranges(
    snippets: Sequence[Any],
    ranges: Sequence[Tuple[float, float]],
    pad_before: float = 1.0,
    pad_after: float = 0.5,
    min_overlap: float = 0.35,
) -> Tuple[List[Snippet], float, int]:
    """Strip snippets falling inside padded ad ranges."""
    norm = _normalize_snippets(snippets)
    if not ranges or not norm:
        return norm, 0.0, 0

    padded_ranges = [(s - pad_before, e + pad_after) for s, e in ranges]
    kept: List[Snippet] = []
    removed_seconds = 0.0
    removed_count = 0

    n = len(norm)
    for i, s in enumerate(norm):
        next_s = norm[i + 1] if i + 1 < n else None
        eff_end = effective_end(s, next_s)
        eff_dur = max(0.0, eff_end - s.start)
        midpoint = (s.start + eff_end) / 2.0

        is_dropped = False
        for p_start, p_end in padded_ranges:
            if p_start <= midpoint <= p_end:
                is_dropped = True
                break
            overlap = max(0.0, min(eff_end, p_end) - max(s.start, p_start))
            if eff_dur > 0 and overlap >= min_overlap * eff_dur:
                is_dropped = True
                break

        if is_dropped:
            removed_seconds += eff_dur
            removed_count += 1
        else:
            kept.append(s)

    return kept, removed_seconds, removed_count


def join_text(snippets: Sequence[Any]) -> str:
    """Join snippet texts into a clean single string."""
    norm = _normalize_snippets(snippets)
    cleaned: List[str] = []
    for s in norm:
        text = s.text
        text = re.sub(r"\[[^\]]*\]", " ", text)
        text = re.sub(r"(?:^|\n)(?:\s*>>\s*)+", " ", text)
        text = re.sub(r"^(?:\s*>>\s*)+", "", text)
        text = text.strip()
        if text:
            cleaned.append(text)
    joined = " ".join(cleaned)
    return re.sub(r"\s+", " ", joined).strip()


def clean_transcript(
    snippets: Sequence[Any],
    video_id: str,
    fetch: Callable[[str], List[Tuple[float, float]]] = fetch_sponsor_segments,
    use_heuristic: bool = True,
) -> CleanResult:
    """Clean transcript snippets using SponsorBlock API with optional heuristic fallback."""
    norm = _normalize_snippets(snippets)
    sources: List[str] = []
    applied_ranges: List[Tuple[float, float]] = []

    sb_ranges: List[Tuple[float, float]] = []
    fetch_failed = False

    try:
        sb_ranges = fetch(video_id)
    except SponsorBlockUnavailable:
        fetch_failed = True
    except Exception as e:
        LOGGER.warning("fetch failed for video %s: %s", video_id, e)
        fetch_failed = True

    if sb_ranges:
        sources.append("sponsorblock")
        applied_ranges = merge_ranges(sb_ranges)
    else:
        if use_heuristic:
            h_ranges = heuristic_ad_ranges(norm)
            if h_ranges:
                sources.append("heuristic")
                applied_ranges = merge_ranges(h_ranges)
        if fetch_failed:
            sources.append("sponsorblock_unavailable")

    kept, removed_seconds, removed_count = strip_ranges(norm, applied_ranges)
    text = join_text(kept)

    return CleanResult(
        text=text,
        removed_seconds=removed_seconds,
        removed_snippets=removed_count,
        total_snippets=len(norm),
        sources=sources,
        ranges=applied_ranges,
        decisions=[
            {"segment_index": i, "decision": "excluded" if any(start <= item.start <= end for start, end in applied_ranges) else "included", "reason": "mapped_ad_range" if applied_ranges else "no_ad_range", "source": ",".join(sources) or "none", "start": item.start, "end": effective_end(item, norm[i + 1] if i + 1 < len(norm) else None)}
            for i, item in enumerate(norm)
        ],
    )
