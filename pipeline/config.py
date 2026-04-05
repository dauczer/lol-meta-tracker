from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# API credentials
# ---------------------------------------------------------------------------
RIOT_API_KEY: str = os.environ.get("RIOT_API_KEY", "")

# ---------------------------------------------------------------------------
# Regional routing
# League / Summoner endpoints use the platform host.
# Match endpoints use the regional cluster host.
# Getting this wrong produces confusing 404 errors.
# ---------------------------------------------------------------------------
PLATFORM_HOST: str = "https://euw1.api.riotgames.com"
REGIONAL_HOST: str = "https://europe.api.riotgames.com"

# ---------------------------------------------------------------------------
# Endpoint templates (plain strings, not f-strings)
# ---------------------------------------------------------------------------
LEAGUE_ENTRIES_URL: str = (
    PLATFORM_HOST + "/lol/league-exp/v4/entries/RANKED_SOLO_5x5/{tier}/I"
)
SUMMONER_BY_ID_URL: str = PLATFORM_HOST + "/lol/summoner/v4/summoners/{summoner_id}"
MATCH_IDS_BY_PUUID_URL: str = (
    REGIONAL_HOST + "/lol/match/v5/matches/by-puuid/{puuid}/ids"
)
MATCH_DETAIL_URL: str = REGIONAL_HOST + "/lol/match/v5/matches/{match_id}"
PLATFORM_STATUS_URL: str = PLATFORM_HOST + "/lol/status/v4/platform-data"

# ---------------------------------------------------------------------------
# Ingestion parameters
# ---------------------------------------------------------------------------
TIERS: list[str] = ["CHALLENGER", "GRANDMASTER", "MASTER"]
QUEUE_ID: int = 420  # Ranked Solo/Duo
MATCHES_PER_PLAYER: int = 5
MATCH_LOOKBACK_DAYS: int = 7  # only fetch matches from the last N days
MAX_PLAYERS: int = 1000  # cap total players; keeps all Chall/GM, top-LP Masters fill the rest

# ---------------------------------------------------------------------------
# Rate limits (personal API key)
# ---------------------------------------------------------------------------
RATE_LIMIT_PER_SECOND: int = 20
RATE_LIMIT_PER_2MIN: int = 100

# ---------------------------------------------------------------------------
# Transformation parameters
# ---------------------------------------------------------------------------
MIN_GAME_DURATION: int = 900  # seconds — exclude remakes
MIN_GAMES_THRESHOLD: int = 10  # minimum games for top-champion selection
TOP_N_PER_ROLE: int = 2
ROLES: list[str] = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]

# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
DATA_DIR: Path = PROJECT_ROOT / "data"
RAW_DIR: Path = DATA_DIR / "raw"
OUTPUT_DIR: Path = DATA_DIR / "output"
CACHE_DIR: Path = DATA_DIR / "cache"
PUUID_CACHE_FILE: Path = CACHE_DIR / "puuids.json"
PROCESSED_MATCHES_FILE: Path = CACHE_DIR / "processed_matches.json"
