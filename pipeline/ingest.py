"""
Stage 1 — Ingestion

Fetches raw match data from the Riot API and persists it to disk.

Flow:
  1. get_high_elo_players()  — LEAGUE-EXP-V4: Challenger / GM / Master
  2. resolve_puuids()        — SUMMONER-V4, with local cache
  3. get_match_ids()         — MATCH-V5 list, deduped, skipping processed matches
  4. fetch_match_details()   — MATCH-V5 detail, saved to data/raw/{date}/matches/
  5. run_ingestion()         — orchestrates all of the above
"""
from __future__ import annotations

import json
import logging
import time
from collections import deque
from datetime import date
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

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------


class RateLimiter:
    """
    Dual-bucket token limiter: 20 req/1 s AND 100 req/2 min.

    Blocks the calling thread until a request slot is available in both
    windows.  Uses a 50 ms polling interval — low overhead for a single-
    threaded pipeline.
    """

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

    def acquire(self) -> None:
        """Block until a request slot is available in both windows."""
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

            time.sleep(0.05)


# Module-level limiter shared across all API calls
_limiter = RateLimiter()

# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

_consecutive_403s = 0
_MAX_CONSECUTIVE_403S = 3


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _api_get(url: str, params: dict[str, Any] | None = None) -> Any:
    """Make a rate-limited, retrying GET request to the Riot API."""
    global _consecutive_403s

    _limiter.acquire()
    resp = requests.get(
        url,
        headers={"X-Riot-Token": config.RIOT_API_KEY},
        params=params,
        timeout=10,
    )

    if resp.status_code == 403:
        _consecutive_403s += 1
        if _consecutive_403s >= _MAX_CONSECUTIVE_403S:
            raise RuntimeError(
                f"API key appears to be expired or invalid "
                f"({_consecutive_403s} consecutive 403 responses). "
                "Regenerate your key at https://developer.riotgames.com"
            )
        raise requests.exceptions.HTTPError(response=resp)

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 5))
        logger.warning("Rate limited by Riot API. Sleeping %ds.", retry_after)
        time.sleep(retry_after)
        raise requests.exceptions.RequestException("429 Rate Limited")

    resp.raise_for_status()
    _consecutive_403s = 0
    return resp.json()


# ---------------------------------------------------------------------------
# Ingestion functions
# ---------------------------------------------------------------------------


_TIER_PRIORITY: dict[str, int] = {"CHALLENGER": 0, "GRANDMASTER": 1, "MASTER": 2}


def get_high_elo_players() -> list[dict[str, Any]]:
    """
    Fetch players in Challenger, Grandmaster, and Master tiers (EUW),
    capped at MAX_PLAYERS total.

    The API returns entries sorted by LP descending, so for Master we stop
    fetching pages as soon as we have enough players to fill the cap — no
    need to pull all 10,000+ Master entries.  Challenger and GM are always
    fetched in full (they each fit on one page).

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

            # For Master: stop early once we've hit the cap
            if tier == "MASTER" and len(all_players) >= config.MAX_PLAYERS:
                logger.info(
                    "  Reached MAX_PLAYERS cap (%d), stopping Master pagination.",
                    config.MAX_PLAYERS,
                )
                break

            # Non-Master tiers fit on one page; Master continues until empty or capped
            if tier != "MASTER" or len(entries) < 205:
                break
            page += 1

        logger.info("  %s: %d entries.", tier, len(all_players) - tier_count_before)

    # Trim to exact cap (last page may have pushed us slightly over)
    if len(all_players) > config.MAX_PLAYERS:
        all_players = all_players[: config.MAX_PLAYERS]

    logger.info("Total players after cap: %d", len(all_players))
    return all_players


def _load_cache(path: Path) -> dict[str, Any]:
    """Load a JSON cache file, returning an empty dict if it doesn't exist."""
    if path.exists():
        with path.open() as f:
            return json.load(f)
    return {}


def _save_cache(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(data, f, indent=2)


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
    processed: dict[str, bool] = _load_cache(config.PROCESSED_MATCHES_FILE)
    all_ids: set[str] = set()

    puuids = list(puuid_map.values())
    logger.info("Fetching match IDs for %d players...", len(puuids))

    skipped = 0
    for i, puuid in enumerate(puuids):
        url = config.MATCH_IDS_BY_PUUID_URL.format(puuid=puuid)
        try:
            ids = _api_get(
                url,
                params={"queue": config.QUEUE_ID, "count": config.MATCHES_PER_PLAYER},
            )
            all_ids.update(ids)
        except requests.exceptions.RequestException as exc:
            skipped += 1
            logger.warning("Skipping player %s after retries exhausted: %s", puuid[:20], exc)

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

    processed: dict[str, bool] = _load_cache(config.PROCESSED_MATCHES_FILE)
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
        processed[match_id] = True
        if (i + 1) % 50 == 0:
            _save_cache(config.PROCESSED_MATCHES_FILE, processed)
            logger.info("  Fetched %d / %d matches...", i + 1, len(match_ids))

    _save_cache(config.PROCESSED_MATCHES_FILE, processed)
    if skipped:
        logger.warning("Skipped %d / %d matches due to network errors.", skipped, len(match_ids))
    logger.info("Saved %d match files to %s.", len(saved_paths), out_dir)
    return saved_paths


def _latest_raw_files() -> list[Path]:
    """Return match JSON files from the most recent date partition."""
    date_dirs = sorted(config.RAW_DIR.iterdir())
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
