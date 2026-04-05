"""
Quick smoke test for the Riot API key.

Run: python scripts/test_api_key.py

Verifies:
  1. RIOT_API_KEY is set in the environment / .env file
  2. The EUW1 platform is reachable and returns a 200 status
  3. The LEAGUE-EXP-V4 endpoint returns Challenger players

This is the first script to run after cloning the repo.
"""
from __future__ import annotations

import sys

import requests

# Import config — this also calls load_dotenv()
from pipeline import config


def check_api_key() -> None:
    if not config.RIOT_API_KEY:
        print(
            "ERROR: RIOT_API_KEY is not set.\n"
            "  1. Copy .env.example to .env\n"
            "  2. Fill in your key from https://developer.riotgames.com"
        )
        sys.exit(1)

    # Mask the key for display
    masked = config.RIOT_API_KEY[:8] + "..." + config.RIOT_API_KEY[-4:]
    print(f"API key found: {masked}")


def check_platform_status() -> None:
    print("\nChecking EUW1 platform status...")
    headers = {"X-Riot-Token": config.RIOT_API_KEY}

    try:
        resp = requests.get(config.PLATFORM_STATUS_URL, headers=headers, timeout=10)
    except requests.exceptions.ConnectionError:
        print("ERROR: Could not connect to Riot API. Check your internet connection.")
        sys.exit(1)

    if resp.status_code == 403:
        print(
            "ERROR: API key rejected (403 Forbidden).\n"
            "  Your key may have expired. Personal keys expire every 24 hours.\n"
            "  Regenerate it at: https://developer.riotgames.com"
        )
        sys.exit(1)

    resp.raise_for_status()
    data = resp.json()
    incidents = data.get("incidents", [])
    print(f"  Platform: {data.get('id', 'EUW1')} — OK")
    if incidents:
        print(f"  Active incidents: {len(incidents)}")
    else:
        print("  No active incidents")


def check_challenger_players() -> None:
    print("\nFetching Challenger player list...")
    url = config.LEAGUE_ENTRIES_URL.format(tier="CHALLENGER")
    headers = {"X-Riot-Token": config.RIOT_API_KEY}

    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code == 403:
        print("ERROR: API key rejected. See above for instructions.")
        sys.exit(1)
    resp.raise_for_status()

    players = resp.json()
    print(f"  Challenger players in EUW: {len(players)}")
    if players:
        sample = players[0]
        print(f"  Sample entry: {sample.get('summonerName', 'N/A')} ({sample.get('leaguePoints', 0)} LP)")


def main() -> None:
    print("=== LoL Meta Tracker — API Key Smoke Test ===\n")
    check_api_key()
    check_platform_status()
    check_challenger_players()
    print("\nAll checks passed. You're ready to run the pipeline!")


if __name__ == "__main__":
    main()
