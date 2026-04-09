"""
Stage 1 — Ingestion

Fetches raw match data from the Riot API and persists it to disk.

Flow:
  1. get_high_elo_players()  — LEAGUE-EXP-V4: Challenger / Grandmaster
  2. resolve_puuids()        — SUMMONER-V4, with local cache
  3. get_match_ids()         — MATCH-V5 list, deduped, skipping processed matches
  4. fetch_match_details()   — MATCH-V5 detail, saved to data/raw/{date}/matches/
  5. run_ingestion()         — orchestrates all of the above
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from collections import deque
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from pipeline import config

logger = logging.getLogger(__name__)


class _RiotRateLimitError(Exception):
    """Raised on HTTP 429 so tenacity can retry it with a distinct predicate."""

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------


class RateLimiter:
    """
    Dual-bucket token limiter: 20 req/1 s AND 100 req/2 min.

    Blocks the calling thread until a request slot is available in both
    windows.  Uses a 50 ms polling interval — low overhead for a single-
    threaded pipeline.

    Also tracks consecutive 403 responses to detect an expired API key;
    raises RuntimeError after MAX_CONSECUTIVE_403S failures.
    """

    MAX_CONSECUTIVE_403S: int = 3

    def __init__(
        self,
        per_second: int = config.RATE_LIMIT_PER_SECOND,
        per_2min: int = config.RATE_LIMIT_PER_2MIN,
    ) -> None:
        self._per_second = per_second
        self._per_2min = per_2min
        self._short_window: deque[float] = deque()  # 1 s window
        self._long_window: deque[float] = deque()   # 120 s window
        self._lock = Lock()
        self.consecutive_403s: int = 0

    def acquire(self) -> None:
        """Block until a request slot is available in both windows.

        Computes the exact time until the next slot opens and sleeps precisely
        that long, avoiding the busy-poll overhead of a fixed 50 ms interval.
        """
        while True:
            with self._lock:
                now = time.monotonic()
                # Prune expired timestamps
                while self._short_window and now - self._short_window[0] >= 1.0:
                    self._short_window.popleft()
                while self._long_window and now - self._long_window[0] >= 120.0:
                    self._long_window.popleft()

                if (
                    len(self._short_window) < self._per_second
                    and len(self._long_window) < self._per_2min
                ):
                    self._short_window.append(now)
                    self._long_window.append(now)
                    return

                # Compute how long until the oldest slot in either window expires
                sleep_until = float("inf")
                if len(self._short_window) >= self._per_second:
                    sleep_until = min(sleep_until, self._short_window[0] + 1.0)
                if len(self._long_window) >= self._per_2min:
                    sleep_until = min(sleep_until, self._long_window[0] + 120.0)

            delay = max(0.0, sleep_until - time.monotonic())
            time.sleep(delay)

    def record_success(self) -> None:
        """Reset the 403 counter after a successful response."""
        self.consecutive_403s = 0

    def record_403(self) -> None:
        """Increment the 403 counter; raise if the key appears dead."""
        self.consecutive_403s += 1
        if self.consecutive_403s >= self.MAX_CONSECUTIVE_403S:
            raise RuntimeError(
                f"API key appears to be expired or invalid "
                f"({self.consecutive_403s} consecutive 403 responses). "
                "Regenerate your key at https://developer.riotgames.com"
            )


# Module-level limiter shared across all API calls
_limiter = RateLimiter()

# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((requests.exceptions.RequestException, _RiotRateLimitError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _api_get(url: str, params: dict[str, Any] | None = None) -> Any:
    """Make a rate-limited, retrying GET request to the Riot API."""
    _limiter.acquire()
    resp = requests.get(
        url,
        headers={"X-Riot-Token": config.RIOT_API_KEY},
        params=params,
        timeout=10,
    )

    if resp.status_code == 403:
        _limiter.record_403()
        raise requests.exceptions.HTTPError(response=resp)

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 5))
        logger.warning("Rate limited by Riot API. Sleeping %ds.", retry_after)
        time.sleep(retry_after)
        raise _RiotRateLimitError(f"429 from Riot API; retrying after {retry_after}s")

    resp.raise_for_status()
    _limiter.record_success()
    return resp.json()


# ---------------------------------------------------------------------------
# Ingestion functions
# ---------------------------------------------------------------------------


def get_high_elo_players() -> list[dict[str, Any]]:
    """
    Fetch players in Challenger and Grandmaster tiers (EUW).

    Both tiers fit on a single API page so no pagination is needed.

    Returns a list of league-entry dicts containing at minimum:
      puuid (or summonerId), leaguePoints, tier.
    """
    all_players: list[dict[str, Any]] = []

    for tier in config.TIERS:
        url = config.LEAGUE_ENTRIES_URL.format(tier=tier)
        page = 1
        tier_count_before = len(all_players)
        while True:
            logger.info("Fetching %s players (page %d)...", tier, page)
            entries = _api_get(url, params={"page": page})
            if not entries:
                break
            for e in entries:
                e.setdefault("tier", tier)
            all_players.extend(entries)

            # If the page was full (~205 entries), there may be more pages
            if len(entries) < 205:
                break
            page += 1

        logger.info("  %s: %d entries.", tier, len(all_players) - tier_count_before)

    logger.info("Total players fetched: %d", len(all_players))
    return all_players


def _load_cache(path: Path) -> dict[str, Any]:
    """Load a JSON cache file, returning an empty dict if it doesn't exist."""
    if path.exists():
        with path.open() as f:
            data: dict[str, Any] = json.load(f)
            return data
    return {}


def _save_cache(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(data, f, indent=2)


def _load_processed_matches(path: Path) -> set[str]:
    """Load the set of processed match IDs.

    Supports both the new list format and the legacy dict[str, bool] format
    so existing caches are not broken on first upgrade.
    """
    if not path.exists():
        return set()
    with path.open() as f:
        raw = json.load(f)
    if isinstance(raw, list):
        return set(raw)
    # Legacy dict format: {"matchId": true, ...}
    return set(raw.keys())


def _save_processed_matches(path: Path, match_ids: set[str]) -> None:
    """Persist processed match IDs as a JSON array."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(sorted(match_ids), f, indent=2)


def resolve_puuids(players: list[dict[str, Any]]) -> dict[str, str]:
    """
    Return a mapping of identifier → puuid for all players.

    Modern LEAGUE-EXP-V4 responses include `puuid` directly, so no extra
    API call is needed for those.  For entries that only have `summonerId`
    (older API behaviour), fall back to SUMMONER-V4 with local caching.

    Returns: {identifier: puuid}  (identifier is puuid itself when inline,
                                    summonerId otherwise)
    """
    cache: dict[str, str] = _load_cache(config.PUUID_CACHE_FILE)
    result: dict[str, str] = {}
    to_fetch: list[dict[str, Any]] = []

    for p in players:
        if p.get("puuid"):
            # API already provided the puuid inline — use it directly
            key = p.get("summonerId") or p["puuid"]
            result[key] = p["puuid"]
        elif p.get("summonerId"):
            sid = p["summonerId"]
            if sid in cache:
                result[sid] = cache[sid]
            else:
                to_fetch.append(p)
        else:
            logger.warning("Skipping player entry with no puuid or summonerId: %s", p)

    logger.info(
        "Resolving PUUIDs: %d inline, %d cached, %d to fetch via API.",
        len(result),
        len(cache),
        len(to_fetch),
    )

    for i, player in enumerate(to_fetch):
        sid = player["summonerId"]
        url = config.SUMMONER_BY_ID_URL.format(summoner_id=sid)
        data = _api_get(url)
        puuid = data["puuid"]
        result[sid] = puuid
        cache[sid] = puuid

        if (i + 1) % 100 == 0:
            logger.info("  Resolved %d / %d PUUIDs via API...", i + 1, len(to_fetch))
            _save_cache(config.PUUID_CACHE_FILE, cache)

    if to_fetch:
        _save_cache(config.PUUID_CACHE_FILE, cache)
        logger.info("PUUID cache updated: %d total entries.", len(cache))

    logger.info("Total unique PUUIDs resolved: %d", len(result))
    return result


def get_match_ids(puuid_map: dict[str, str]) -> list[str]:
    """
    Fetch up to MATCHES_PER_PLAYER recent ranked match IDs per player.

    Deduplicates across players and subtracts already-processed match IDs
    from data/cache/processed_matches.json.

    Returns: list of new, unique match IDs to fetch.
    """
    processed: set[str] = _load_processed_matches(config.PROCESSED_MATCHES_FILE)
    all_ids: set[str] = set()

    puuids = list(puuid_map.values())
    start_epoch = int(
        (datetime.now(tz=timezone.utc) - timedelta(days=config.MATCH_LOOKBACK_DAYS))
        .timestamp()
    )
    logger.info(
        "Fetching match IDs for %d players (last %d days)...",
        len(puuids),
        config.MATCH_LOOKBACK_DAYS,
    )

    skipped = 0
    for i, puuid in enumerate(puuids):
        url = config.MATCH_IDS_BY_PUUID_URL.format(puuid=puuid)
        try:
            ids = _api_get(
                url,
                params={
                    "queue": config.QUEUE_ID,
                    "count": config.MATCHES_PER_PLAYER,
                    "startTime": start_epoch,
                },
            )
            all_ids.update(ids)
        except requests.exceptions.RequestException as exc:
            skipped += 1
            puuid_tag = hashlib.sha256(puuid.encode()).hexdigest()[:8]
            logger.warning("Skipping player %s after retries exhausted: %s", puuid_tag, exc)

        if (i + 1) % 200 == 0:
            logger.info("  Processed %d / %d players...", i + 1, len(puuids))

    if skipped:
        logger.warning("Skipped %d / %d players due to network errors.", skipped, len(puuids))

    new_ids = [mid for mid in all_ids if mid not in processed]
    logger.info(
        "Match IDs: %d total, %d already processed, %d new.",
        len(all_ids),
        len(all_ids) - len(new_ids),
        len(new_ids),
    )
    return new_ids


def fetch_match_details(match_ids: list[str], run_date: str) -> list[Path]:
    """
    Fetch full match details for each match ID and save raw JSON to disk.

    Saves to: data/raw/{run_date}/matches/{matchId}.json
    Updates processed_matches.json after each successful save (crash-safe).

    Returns: list of file paths written.
    """
    out_dir = config.RAW_DIR / run_date / "matches"
    out_dir.mkdir(parents=True, exist_ok=True)

    processed: set[str] = _load_processed_matches(config.PROCESSED_MATCHES_FILE)
    saved_paths: list[Path] = []

    logger.info("Fetching details for %d matches...", len(match_ids))

    skipped = 0
    for i, match_id in enumerate(match_ids):
        url = config.MATCH_DETAIL_URL.format(match_id=match_id)
        try:
            match_data = _api_get(url)
        except requests.exceptions.RequestException as exc:
            skipped += 1
            logger.warning("Skipping match %s after retries exhausted: %s", match_id, exc)
            continue

        file_path = out_dir / f"{match_id}.json"
        with file_path.open("w") as f:
            json.dump(match_data, f, indent=2)
        saved_paths.append(file_path)

        # Update cache immediately so a mid-run crash doesn't re-fetch
        processed.add(match_id)
        if (i + 1) % 50 == 0:
            _save_processed_matches(config.PROCESSED_MATCHES_FILE, processed)
            logger.info("  Fetched %d / %d matches...", i + 1, len(match_ids))

    _save_processed_matches(config.PROCESSED_MATCHES_FILE, processed)
    if skipped:
        logger.warning("Skipped %d / %d matches due to network errors.", skipped, len(match_ids))
    logger.info("Saved %d match files to %s.", len(saved_paths), out_dir)
    return saved_paths


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _latest_raw_files() -> list[Path]:
    """Return match JSON files from the most recent date partition.

    Only directories whose name matches YYYY-MM-DD are considered, so
    stray debug folders (e.g. '2099-01-01') or non-date entries are ignored.
    """
    date_dirs = sorted(
        d for d in config.RAW_DIR.iterdir()
        if d.is_dir() and _ISO_DATE_RE.match(d.name)
    )
    if not date_dirs:
        raise RuntimeError(
            "No raw data found in data/raw/. "
            "Run without --dry-run first to ingest data."
        )
    latest = date_dirs[-1]
    return list((latest / "matches").glob("*.json"))


def run_ingestion(dry_run: bool = False) -> list[Path]:
    """
    Orchestrate the full ingestion pipeline.

    If dry_run=True, skip all API calls and return raw files from the
    most recent date directory.
    """
    if dry_run:
        files = _latest_raw_files()
        logger.info("Dry run: using %d existing raw files.", len(files))
        return files

    if not config.RIOT_API_KEY:
        raise RuntimeError(
            "RIOT_API_KEY is not set. "
            "Copy .env.example to .env and fill in your key."
        )

    run_date = date.today().isoformat()
    logger.info("Ingestion run date: %s", run_date)

    players = get_high_elo_players()
    puuid_map = resolve_puuids(players)
    match_ids = get_match_ids(puuid_map)

    if not match_ids:
        logger.info("No new matches to fetch. Pipeline complete.")
        return _latest_raw_files()

    return fetch_match_details(match_ids, run_date)
