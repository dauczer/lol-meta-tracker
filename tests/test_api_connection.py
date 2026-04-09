"""
Smoke test: verify the Riot API key is valid and the platform is reachable.

Behaviour:
  - Locally (no RIOT_API_KEY): skipped — safe for offline dev.
  - In CI (CI env var set, no RIOT_API_KEY): fails loudly — a missing secret
    is a misconfiguration, not a reason to pass silently.
"""
from __future__ import annotations

import os

import pytest
import requests

from pipeline import config

_key_missing = not os.environ.get("RIOT_API_KEY")
_in_ci = bool(os.environ.get("CI"))

if _key_missing and _in_ci:
    pytest.fail("RIOT_API_KEY secret is not configured in CI — add it to repository secrets.")


@pytest.mark.skipif(
    _key_missing,
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
