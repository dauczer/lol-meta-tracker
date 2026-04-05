# LoL Meta Tracker — Project Specification

## Overview

This project is a data engineering portfolio piece: an end-to-end pipeline that ingests League of Legends ranked match data from the Riot API, transforms it into meaningful meta statistics (top champions per role, win rates, pick rates), and outputs clean JSON files consumable by a portfolio website.

The goal is to demonstrate the full data engineering lifecycle (ingestion → storage → transformation → orchestration → delivery) using **100% free tools**, with data refreshed automatically every day.

## Portfolio context

This project showcases three key skills for data engineer / data scientist roles:

1. **End-to-end pipeline ownership** — from raw API to delivered data product
2. **Orchestration and automation** — scheduled jobs that run unattended
3. **Pragmatic tech choices** — using the right tool for the scale, not over-engineering

The repository will contain **two versions** of the pipeline (see "Repository structure" below) to demonstrate both the lightweight portfolio implementation and the enterprise-grade architecture.

## Scope (tight on purpose)

- **Region**: EUW only
- **Tiers**: Challenger + Grandmaster + Master (~1500 players total)
- **Output**: Static JSON files committed to the repo
- **Refresh frequency**: Once per day, automated
- **Game mode**: Ranked Solo/Duo (queueId 420)

Keeping scope tight means we stay well within Riot API rate limits and GitHub Actions free tier.

## Tech stack (100% free)

| Component | Tool | Free tier |
|---|---|---|
| Language | Python 3.11+ | Free |
| HTTP client | `requests` | Free |
| Data manipulation | `pandas` | Free |
| API source | Riot Games API | Personal key (free, renewable every 24h) |
| Champion/item assets | Data Dragon CDN | Free, public |
| Orchestrator | GitHub Actions | 2000 min/month free |
| Storage | Git repository (JSON files) | Free |
| Testing | `pytest` | Free |

**No database, no cloud storage, no paid services.**

## Architecture

### Enterprise vs portfolio mapping

This is a key talking point for interviews. The README should make this explicit:

| Enterprise tool | Portfolio equivalent | Why the swap works at this scale |
|---|---|---|
| Airflow / Prefect | GitHub Actions cron | Both are scheduled orchestrators; Actions handles daily runs fine |
| BigQuery / Snowflake | Python + pandas in memory | Data volume is a few MB, no warehouse needed |
| S3 / GCS data lake | `data/raw/` folder in repo | Git versions raw snapshots naturally |
| dbt | Python transformation scripts | dbt shines on SQL warehouses; here Python is simpler |
| Monitoring (Datadog) | GitHub Actions status + logs | Sufficient for a personal project |

### Repository structure

```
lol-meta-tracker/
├── README.md                    # Project overview, architecture diagram, talking points
├── CLAUDE.md                    # This file
├── .github/
│   └── workflows/
│       └── daily-refresh.yml    # GitHub Actions cron job
├── pipeline/                    # The actual pipeline (what runs in prod)
│   ├── __init__.py
│   ├── config.py                # Constants, API endpoints, tier list
│   ├── ingest.py                # Fetch raw data from Riot API
│   ├── transform.py             # Clean & aggregate into meta stats
│   ├── output.py                # Write final JSONs
│   └── main.py                  # Orchestrates ingest → transform → output
├── pipeline-enterprise/         # "Reference" version — shows enterprise stack
│   ├── README.md                # Explains this is the production-grade design
│   ├── dbt/                     # dbt models (not executed, just demonstrated)
│   ├── airflow/
│   │   └── dags/meta_tracker_dag.py
│   └── docker-compose.yml       # Local Airflow + Postgres setup
├── data/
│   ├── raw/                     # Raw API responses, partitioned by date
│   │   └── YYYY-MM-DD/
│   │       ├── players.json
│   │       └── matches.json
│   └── output/                  # Final consumable JSONs (what the portfolio reads)
│       ├── meta_summary.json
│       ├── champions_by_role.json
│       └── top_champions.json
├── tests/
│   ├── test_ingest.py
│   ├── test_transform.py
│   └── test_api_connection.py
├── scripts/
│   └── test_api_key.py          # Quick smoke test for API credentials
├── requirements.txt
└── .env.example                 # Template for RIOT_API_KEY
```

## Pipeline stages (detailed)

### Stage 1 — Ingestion (`pipeline/ingest.py`)

**Goal**: Fetch raw data from Riot API and save it to disk.

**Steps**:

1. **Get high-elo players list** using LEAGUE-EXP-V4 endpoint:
   - `GET /lol/league-exp/v4/entries/{queue}/{tier}/{division}`
   - Call 3 times: Challenger/I, Grandmaster/I, Master/I
   - Queue: `RANKED_SOLO_5x5`
   - Expected: ~300 Challenger, ~700 GM, ~500 Master

2. **Resolve PUUIDs** for each summoner:
   - `GET /lol/summoner/v4/summoners/{summonerId}`
   - Needed because MATCH-V5 endpoint requires PUUIDs
   - Cache results locally (`data/cache/puuids.json`) — don't re-fetch existing ones

3. **Get match IDs** for each player (last 5 ranked matches per player):
   - `GET /lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&count=5`
   - Deduplicate across all players (same match appears in multiple players' lists)
   - Load already-processed match IDs from `data/cache/processed_matches.json` and skip them

4. **Fetch match details** for each new match:
   - `GET /lol/match/v5/matches/{matchId}`
   - Store raw JSON in `data/raw/{YYYY-MM-DD}/matches/{matchId}.json`

**Regional routing** (important — easy to get wrong):
- League & Summoner endpoints: `euw1.api.riotgames.com`
- Match endpoints: `europe.api.riotgames.com` (regional routing cluster)

### Stage 2 — Transformation (`pipeline/transform.py`)

**Goal**: Turn raw matches into meta statistics.

**Input**: Raw match JSONs from `data/raw/`
**Output**: Pandas DataFrames ready for serialization

**Transformations**:

1. **Parse match participants**: from each match, extract 10 rows (one per player)
   - Fields: `matchId`, `championName`, `teamPosition` (TOP/JUNGLE/MIDDLE/BOTTOM/UTILITY), `win`, `kills`, `deaths`, `assists`, `gameDuration`, `gameVersion` (patch)

2. **Filter valid data**:
   - Exclude matches with `gameDuration < 900s` (remakes)
   - Exclude rows with empty `teamPosition`

3. **Aggregate by champion + role + patch**:
   - `games_played` (count)
   - `wins` (sum)
   - `win_rate` = wins / games_played
   - `pick_rate` = games_played / (total matches on current patch × 2 teams)
   - `avg_kda` = (kills + assists) / max(deaths, 1)

4. **Compute top 2 per role** (minimum 20 games for statistical relevance):
   - For each role (TOP, JUNGLE, MIDDLE, BOTTOM, UTILITY):
     - Filter champions with `games_played >= 20`
     - Sort by `win_rate` desc
     - Take top 2

### Stage 3 — Output (`pipeline/output.py`)

**Goal**: Write final JSON files that the portfolio website will consume.

**Files to generate in `data/output/`**:

**`meta_summary.json`** — hero stats:
```json
{
  "patch": "14.7",
  "region": "EUW",
  "tiers": ["CHALLENGER", "GRANDMASTER", "MASTER"],
  "total_matches": 847,
  "total_champions_tracked": 168,
  "last_updated": "2026-04-04T06:00:00Z"
}
```

**`top_champions.json`** — top 2 per role (portfolio window content):
```json
{
  "TOP": [
    {"champion": "Ambessa", "win_rate": 0.532, "pick_rate": 0.124, "games": 87},
    {"champion": "K'Sante", "win_rate": 0.518, "pick_rate": 0.087, "games": 61}
  ],
  "JUNGLE": [...],
  "MIDDLE": [...],
  "BOTTOM": [...],
  "UTILITY": [...]
}
```

**`champions_by_role.json`** — full breakdown (for detailed views):
```json
{
  "TOP": [
    {"champion": "Ambessa", "win_rate": 0.532, "pick_rate": 0.124, "games": 87, "avg_kda": 3.21},
    ...
  ],
  ...
}
```

### Stage 4 — Orchestration (GitHub Actions)

**File**: `.github/workflows/daily-refresh.yml`

**Schedule**: Every day at 06:00 UTC (cron: `0 6 * * *`)

**Steps**:
1. Checkout repo
2. Setup Python 3.11
3. Install dependencies from `requirements.txt`
4. Run `python -m pipeline.main` (with `RIOT_API_KEY` from GitHub Secrets)
5. Commit and push updated JSONs in `data/output/` if changed
6. On failure: GitHub automatically notifies via email

**Manual trigger**: Also include `workflow_dispatch` so it can be run on-demand from the Actions UI.

## Riot API — practical notes

### Rate limits (personal key)

- **20 requests per 1 second**
- **100 requests per 2 minutes**

The ingestion must implement a **rate limiter** to respect these. A simple token bucket or `time.sleep()` between calls works. The `requests` response headers include `X-Rate-Limit-Count` which you should monitor.

### Volume estimate

- ~1500 players × 5 matches = 7500 match IDs
- After deduplication: ~1500-3000 unique matches per day
- Total API calls per run: ~3000-5000
- Runtime estimate: 10-15 minutes per day (well within GitHub Actions free tier)

### API key management

Personal API keys **expire every 24 hours**. For a portfolio project this is manageable but annoying. Two options:

1. **Request a Personal Key renewal via Riot's developer portal** and refresh the GitHub Secret manually when needed
2. **Apply for a "Personal Project" key** which doesn't expire (takes a few days for Riot to approve) — **strongly recommended for this project**

Store the key in `RIOT_API_KEY` GitHub Secret. Never commit it.

### Endpoints reference

| Endpoint | Base URL | Used for |
|---|---|---|
| LEAGUE-EXP-V4 | `euw1.api.riotgames.com` | Get players in each tier |
| SUMMONER-V4 | `euw1.api.riotgames.com` | Resolve summoner → PUUID |
| MATCH-V5 (list) | `europe.api.riotgames.com` | Get match IDs per player |
| MATCH-V5 (detail) | `europe.api.riotgames.com` | Get match details |

## Testing strategy

### `tests/test_api_connection.py`
- Simple smoke test: hit `/lol/status/v4/platform-data` and check 200 response
- Runs in CI before the main pipeline to fail fast if the key is dead

### `tests/test_transform.py`
- Unit test transformation logic with a fixed sample match JSON (stored in `tests/fixtures/sample_match.json`)
- Verify win rate calculations
- Verify top 2 selection logic
- Verify role assignments

### `tests/test_ingest.py`
- Mock the API responses using `responses` or `pytest-mock`
- Test deduplication logic
- Test rate limiter behavior

### Dry-run mode
- Add a `--dry-run` flag to `pipeline/main.py` that skips API calls and uses cached raw data from `data/raw/` of the last run
- Useful for iterating on transformations without hitting the API

### Local testing script
- `scripts/test_api_key.py` — one-off script to verify your API key works: prints Challenger player count for EUW. Run this first before anything else.

## Success criteria (definition of done)

- [ ] `scripts/test_api_key.py` runs successfully and prints real data
- [ ] `python -m pipeline.main` runs end-to-end locally and generates all 3 output JSONs
- [ ] All unit tests pass (`pytest tests/`)
- [ ] GitHub Actions workflow runs successfully on `workflow_dispatch`
- [ ] Scheduled run triggers at 06:00 UTC and commits updated JSONs
- [ ] README includes architecture diagram, setup instructions, and enterprise-vs-portfolio talking points
- [ ] `pipeline-enterprise/` folder contains working dbt models and Airflow DAG (even if not executed in CI)

## Implementation order (recommended)

1. Setup repo + `requirements.txt` + `.env.example`
2. Write `scripts/test_api_key.py` and verify API access
3. Implement `pipeline/ingest.py` with rate limiting — test against real API
4. Implement `pipeline/transform.py` with sample data
5. Implement `pipeline/output.py`
6. Wire up `pipeline/main.py`
7. Write tests
8. Setup GitHub Actions workflow
9. Write README with architecture diagram
10. Create `pipeline-enterprise/` reference implementation

## Future extensions (post-MVP, not required)

- Historical trends (compare current patch vs previous patches)
- Build path analysis (most common item builds per champion)
- Matchup analysis (which champion beats which)
- Discord webhook alerts when meta shifts significantly
- Add KR region for comparison
- Deploy a FastAPI endpoint on Vercel to serve the JSONs with filtering

## Code style notes

- Type hints everywhere (`from typing import ...`)
- `logging` module, not `print()`
- Constants in `config.py`, no magic strings scattered
- Small, testable functions (each stage is its own module)
- Handle API errors gracefully with retries (use `tenacity` library)
- Idempotent pipeline runs (re-running the same day shouldn't break)