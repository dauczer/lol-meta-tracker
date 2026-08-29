# LoL Meta Tracker

A data pipeline that answers one question every week: **which champions are dominating high-elo League of Legends right now?**

It pulls ranked match data from the Riot API (Challenger + Grandmaster, EUW), crunches the numbers, and publishes static JSON files that my portfolio website reads directly. Everything runs on GitHub Actions. Total infrastructure cost: zero.

This is deliberately a small batch pipeline: one data source, one weekly schedule, an in-memory transform, and versioned static outputs. The implementation focuses on reproducible snapshots and honest failure modes rather than production-scale infrastructure the project does not need.

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                  GitHub Actions (every Monday, 06:00 UTC)       │
│                                                                 │
│  ┌──────────┐    ┌──────────────┐    ┌──────────┐               │
│  │  INGEST  │───>│  TRANSFORM   │───>│  OUTPUT  │               │
│  │          │    │              │    │          │               │
│  │ Riot API │    │ pandas agg   │    │ 4 JSONs  │               │
│  │ -> raw/  │    │ filter+stats │    │ -> output│               │
│  └──────────┘    └──────────────┘    └──────────┘               │
│       │                                    │                    │
│       v                                    v                    │
│  data/raw/YYYY-MM-DD/              data/output/                 │
│  data/cache/puuids.json            ├─ meta_summary.json         │
│  (raw + cache persisted by CI)     ├─ top_champions.json        │
│                                    ├─ champions_by_role.json    │
│                                    └─ portfolio_snapshot.json   │
│                                         │                       │
│                                    git commit + push            │
└─────────────────────────────────────────────────────────────────┘
                                         │
                                         v
                                  Portfolio Website
```

Three stages, each its own module:

1. **Ingest** — Fetches ~1000 high-elo players, resolves their PUUIDs, grabs their last 5 ranked matches (deduped), and saves raw JSON to disk. A dual-bucket rate limiter (20 req/s *and* 100 req/2min) keeps us within Riot's limits, and a 403-counter detects expired API keys within 3 requests instead of burning retries for 30 minutes.

2. **Transform** — Parses 10 participant rows per match, filters remakes (< 15 min) and missing roles, then aggregates by champion + role + patch: win rate, pick rate, and aggregate KDA ratio.

3. **Output** — Writes four JSON files atomically. The portfolio payload ranks three champions per role with a 30-game minimum and the lower bound of a 95% Wilson interval, then compares pick rates with the previous run when both snapshots belong to the same patch.

A cold run normally takes about 60–80 minutes under a Riot Personal API key's 100 requests / 2 minutes limit. A warm rerun can reuse cached raw matches and is faster.

---

## Design Decisions Worth Knowing About

**Complete, rerunnable snapshots.** Every run resolves the complete current set of match IDs. Raw files restored from the Actions cache are reused, missing matches are fetched, and the current date partition is rebuilt before old partitions are pruned. If the cache is unavailable, the same run performs a cold refresh instead of publishing only a partial delta.

**Crash safety.** Cache and raw JSON writes are atomic. The workflow saves `data/cache` and `data/raw` even after a failed pipeline step, allowing a later run to reuse completed downloads when GitHub provides the saved cache.

**Dry-run mode.** `--dry-run` skips all API calls and reuses the most recent local raw partition. A fresh clone has no raw data, so it needs one successful full run before dry-run mode is available.

**No database.** The entire dataset is a few MB. Pandas handles it in memory in seconds. A database would add migrations, backups, and connection management for zero benefit at this scale.

---

## Output

The portfolio site consumes `portfolio_snapshot.json`, a self-contained payload with scope, sample size, methodology, role leaders, freshness, and same-patch weekly movements:

```json
{
  "schema_version": 2,
  "generated_at": "2026-08-24T07:39:28Z",
  "scope": {"patch": "16.16", "region": "EUW", "lookback_days": 7},
  "sample": {"matches": 2354, "champions": 173},
  "roles": {
    "TOP": {
      "leaders": [
        {"rank": 1, "champion": "Gragas", "win_rate": 0.7, "games": 70}
      ]
    }
  }
}
```

The existing files remain available: `meta_summary.json`, `top_champions.json`, and `champions_by_role.json`.

The live payload is available directly at [`data/output/portfolio_snapshot.json`](data/output/portfolio_snapshot.json). Movers require at least 30 games in both snapshots and a pick-rate change of at least 0.5 percentage points.

---

## Quick Start

**Prerequisites**: Python 3.11, a [Riot API key](https://developer.riotgames.com)

> Apply for a *Personal Project* key if you can — it doesn't expire every 24 hours. Takes a few days for Riot to approve.

```bash
git clone <repo-url> && cd lol-meta-tracker
pip install -r requirements.lock

cp .env.example .env
# Edit .env → set RIOT_API_KEY

python scripts/test_api_key.py        # Verify your key works
python -m pipeline.main               # Cold run (~60-80 min with a Personal key)
python -m pipeline.main --dry-run     # Requires raw data from a previous run
pytest tests/ -v                      # Run tests (API test auto-skipped without key)
```

**GitHub Actions**: Push to GitHub, add `RIOT_API_KEY` as a repository secret, then trigger Actions > Weekly Meta Refresh > Run workflow. The cron is scheduled for every Monday at 06:00 UTC; GitHub may start scheduled jobs later during busy periods.

---

## Project Structure

```
lol-meta-tracker/
├── pipeline/                  # The pipeline
│   ├── config.py              # All constants, endpoints, paths, thresholds
│   ├── ingest.py              # Riot API + rate limiting + caching
│   ├── transform.py           # Parse, filter, aggregate with pandas
│   ├── output.py              # Atomic JSON writers
│   └── main.py                # Orchestrator (--dry-run flag)
├── data/
│   ├── raw/                   # Raw API responses, partitioned by date (gitignored)
│   ├── cache/                 # Local PUUID cache (gitignored)
│   └── output/                # Final JSONs (committed by CI)
├── tests/                     # 50 offline tests + 2 live API smoke tests
│   ├── test_ingest.py         # Mocked API, caching and failure tests
│   ├── test_transform.py      # Parsing and aggregation tests
│   ├── test_output.py         # Output schema and comparison tests
│   ├── test_api_connection.py # Live smoke test (skipped without key)
│   └── fixtures/              # Sample match JSON
├── scripts/
│   └── test_api_key.py        # First-run credential check
└── .github/workflows/
    ├── ci.yml                 # PR gate: ruff + mypy + pytest
    └── weekly-refresh.yml     # Cron: run pipeline, commit results
```

---

## Scope

| | |
|---|---|
| **Region** | EUW |
| **Tiers** | Challenger + Grandmaster (~1000 players) |
| **Queue** | Ranked Solo/Duo (queueId 420) |
| **API calls/run** | ~3000–5000 |
| **Unique matches/run** | ~2300–4000 in observed runs |
| **Runtime** | ~60–80 minutes cold; faster when raw matches are reused |

Tight scope is intentional: the pipeline stays within Riot's Personal key limits while producing a useful weekly sample.

---

## Riot Games Notice

LoL Meta Tracker is not endorsed by Riot Games and does not reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games and all associated properties are trademarks or registered trademarks of Riot Games, Inc.

---

## License

MIT
