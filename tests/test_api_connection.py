"""
Smoke test: verify the Riot API key is valid and the platform is reachable.

Skipped automatically when RIOT_API_KEY is not set — safe for offline dev.
Run in CI before the main pipeline to fail fast on expired keys.
"""
from __future__ import annotations

import os

import pytest
import requests

from pipeline import config


@pytest.mark.skipif(
    not os.environ.get("RIOT_API_KEY"),
    reason="RIOT_API_KEY not set — skipping live API test",
)
class TestApiConnection:
    def test_platform_status_returns_200(self) -> None:
        resp = requests.get(
            config.PLATFORM_STATUS_URL,
            headers={"X-Riot-Token": config.RIOT_API_KEY},
            timeout=10,
        )
        assert resp.status_code == 200, (
            f"Platform status returned {resp.status_code}. "
            "Key may be expired — regenerate at https://developer.riotgames.com"
        )

    def test_challenger_list_is_non_empty(self) -> None:
        url = config.LEAGUE_ENTRIES_URL.format(tier="CHALLENGER")
        resp = requests.get(
            url,
            headers={"X-Riot-Token": config.RIOT_API_KEY},
            timeout=10,
        )
        assert resp.status_code == 200
        players = resp.json()
        assert isinstance(players, list)
        assert len(players) > 0, "Challenger list should not be empty"
