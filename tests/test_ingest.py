"""
Unit tests for pipeline/ingest.py.

All API calls are mocked with the `responses` library — no network required.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest
import responses as responses_lib

from pipeline import config
from pipeline.ingest import (
    RateLimiter,
    _load_cache,
    _save_cache,
    fetch_match_details,
    get_high_elo_players,
    get_match_ids,
    resolve_puuids,
)


# ---------------------------------------------------------------------------
# RateLimiter
# ---------------------------------------------------------------------------


class TestRateLimiter:
    def test_allows_requests_within_limit(self) -> None:
        limiter = RateLimiter(per_second=10, per_2min=100)
        # Should not block for the first 10 requests
        start = time.monotonic()
        for _ in range(10):
            limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 1.0, f"First 10 requests should be instant, took {elapsed:.2f}s"

    def test_blocks_after_per_second_limit(self) -> None:
        limiter = RateLimiter(per_second=5, per_2min=100)
        start = time.monotonic()
        for _ in range(10):
            limiter.acquire()
        elapsed = time.monotonic() - start
        # 10 requests at 5/s must take at least 1 second
        assert elapsed >= 1.0, f"Should have been rate-limited, elapsed={elapsed:.2f}s"


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


class TestCacheHelpers:
    def test_load_cache_returns_empty_dict_if_missing(self, tmp_path: Path) -> None:
        result = _load_cache(tmp_path / "nonexistent.json")
        assert result == {}

    def test_save_and_load_roundtrip(self, tmp_path: Path) -> None:
        data = {"key": "value", "num": 42}
        path = tmp_path / "test.json"
        _save_cache(path, data)
        loaded = _load_cache(path)
        assert loaded == data

    def test_save_creates_parent_dirs(self, tmp_path: Path) -> None:
        path = tmp_path / "deep" / "nested" / "cache.json"
        _save_cache(path, {"x": 1})
        assert path.exists()


# ---------------------------------------------------------------------------
# get_high_elo_players
# ---------------------------------------------------------------------------


class TestGetHighEloPlayers:
    @responses_lib.activate
    def test_combines_all_tiers(self) -> None:
        for tier in config.TIERS:
            url = config.LEAGUE_ENTRIES_URL.format(tier=tier)
            responses_lib.add(
                responses_lib.GET,
                url,
                json=[{"summonerId": f"sid-{tier}", "summonerName": f"Player-{tier}",
                       "leaguePoints": 100, "tier": tier}],
            )

        players = get_high_elo_players()
        assert len(players) == 3
        tiers_found = {p["tier"] for p in players}
        assert tiers_found == set(config.TIERS)

    @responses_lib.activate
    def test_paginates_master_tier(self) -> None:
        """Master tier should keep fetching until an empty page is returned."""
        for tier in ["CHALLENGER", "GRANDMASTER"]:
            url = config.LEAGUE_ENTRIES_URL.format(tier=tier)
            responses_lib.add(responses_lib.GET, url, json=[{"summonerId": f"s-{tier}", "tier": tier}])

        master_url = config.LEAGUE_ENTRIES_URL.format(tier="MASTER")
        # Page 1: 205 entries (triggers pagination)
        responses_lib.add(
            responses_lib.GET,
            master_url,
            json=[{"summonerId": f"master-{i}", "tier": "MASTER"} for i in range(205)],
        )
        # Page 2: empty — stops pagination
        responses_lib.add(responses_lib.GET, master_url, json=[])

        players = get_high_elo_players()
        master_players = [p for p in players if p["tier"] == "MASTER"]
        assert len(master_players) == 205


# ---------------------------------------------------------------------------
# resolve_puuids
# ---------------------------------------------------------------------------


class TestResolvePuuids:
    @responses_lib.activate
    def test_fetches_missing_puuids(self, tmp_path: Path) -> None:
        with patch.object(config, "PUUID_CACHE_FILE", tmp_path / "puuids.json"):
            players = [{"summonerId": "sid-1"}, {"summonerId": "sid-2"}]

            for sid in ["sid-1", "sid-2"]:
                url = config.SUMMONER_BY_ID_URL.format(summoner_id=sid)
                responses_lib.add(
                    responses_lib.GET,
                    url,
                    json={"puuid": f"puuid-{sid}", "summonerId": sid},
                )

            result = resolve_puuids(players)
            assert result["sid-1"] == "puuid-sid-1"
            assert result["sid-2"] == "puuid-sid-2"

    @responses_lib.activate
    def test_skips_cached_puuids(self, tmp_path: Path) -> None:
        cache_file = tmp_path / "puuids.json"
        _save_cache(cache_file, {"sid-1": "cached-puuid"})

        with patch.object(config, "PUUID_CACHE_FILE", cache_file):
            players = [{"summonerId": "sid-1"}]
            result = resolve_puuids(players)

        # No HTTP calls should have been made
        assert len(responses_lib.calls) == 0
        assert result["sid-1"] == "cached-puuid"


# ---------------------------------------------------------------------------
# get_match_ids
# ---------------------------------------------------------------------------


class TestGetMatchIds:
    @responses_lib.activate
    def test_deduplicates_match_ids(self, tmp_path: Path) -> None:
        with patch.object(config, "PROCESSED_MATCHES_FILE", tmp_path / "processed.json"):
            puuid_map = {"sid-1": "puuid-1", "sid-2": "puuid-2"}

            for puuid in puuid_map.values():
                url = config.MATCH_IDS_BY_PUUID_URL.format(puuid=puuid)
                # Both players share the same match
                responses_lib.add(
                    responses_lib.GET,
                    url,
                    json=["EUW1_SHARED", f"EUW1_{puuid}_UNIQUE"],
                )

            ids = get_match_ids(puuid_map)
            assert len(ids) == 3  # 1 shared + 2 unique
            assert "EUW1_SHARED" in ids

    @responses_lib.activate
    def test_skips_already_processed(self, tmp_path: Path) -> None:
        processed_file = tmp_path / "processed.json"
        _save_cache(processed_file, {"EUW1_OLD": True})

        with patch.object(config, "PROCESSED_MATCHES_FILE", processed_file):
            puuid_map = {"sid-1": "puuid-1"}
            url = config.MATCH_IDS_BY_PUUID_URL.format(puuid="puuid-1")
            responses_lib.add(responses_lib.GET, url, json=["EUW1_OLD", "EUW1_NEW"])

            ids = get_match_ids(puuid_map)
            assert "EUW1_OLD" not in ids
            assert "EUW1_NEW" in ids


# ---------------------------------------------------------------------------
# fetch_match_details
# ---------------------------------------------------------------------------


class TestFetchMatchDetails:
    @responses_lib.activate
    def test_saves_match_json_files(self, tmp_path: Path, sample_match: dict) -> None:
        with (
            patch.object(config, "RAW_DIR", tmp_path),
            patch.object(config, "PROCESSED_MATCHES_FILE", tmp_path / "processed.json"),
        ):
            match_id = "EUW1_0000000001"
            url = config.MATCH_DETAIL_URL.format(match_id=match_id)
            responses_lib.add(responses_lib.GET, url, json=sample_match)

            paths = fetch_match_details([match_id], "2026-04-04")

            assert len(paths) == 1
            assert paths[0].exists()
            saved = json.loads(paths[0].read_text())
            assert saved["metadata"]["matchId"] == sample_match["metadata"]["matchId"]

    @responses_lib.activate
    def test_updates_processed_cache(self, tmp_path: Path, sample_match: dict) -> None:
        processed_file = tmp_path / "processed.json"
        with (
            patch.object(config, "RAW_DIR", tmp_path),
            patch.object(config, "PROCESSED_MATCHES_FILE", processed_file),
        ):
            match_id = "EUW1_0000000001"
            url = config.MATCH_DETAIL_URL.format(match_id=match_id)
            responses_lib.add(responses_lib.GET, url, json=sample_match)

            fetch_match_details([match_id], "2026-04-04")

            processed = _load_cache(processed_file)
            assert processed.get(match_id) is True
