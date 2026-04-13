# LoL Meta Tracker

A data pipeline that answers one question every week: **which champions are dominating high-elo League of Legends right now?**

It pulls ranked match data from the Riot API (Challenger + Grandmaster, EUW), crunches the numbers, and spits out clean JSON files that my portfolio website reads directly. Everything runs on GitHub Actions. Total cost: zero.

This is a portfolio project, but it's built the way I'd build a production pipeline — modular stages, crash-safe caching, idempotent reruns, automated delivery — just with lighter tools. Every tool choice maps to an enterprise equivalent (see below), which is the actual point: showing I understand what scales and what doesn't.

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                  GitHub Actions (every Monday, 06:00 UTC)       │
│                                                                 │
│  ┌──────────┐    ┌──────────────┐    ┌──────────┐               │
│  │  INGEST  │───>│  TRANSFORM   │───>│  OUTPUT  │               │
│  │          │    │              │    │          │               │
│  │ Riot API │    │ pandas agg   │    │ 3 JSONs  │               │
│  │ -> raw/  │    │ filter+stats │    │ -> output│               │
│  └──────────┘    └──────────────┘    └──────────┘               │
│       │                                    │                    │
│       v                                    v                    │
│  data/raw/YYYY-MM-DD/              data/output/                 │
│  data/cache/puuids.json            ├─ meta_summary.json         │
│  data/cache/processed_matches.json ├─ top_champions.json        │
│                                    └─ champions_by_role.json    │
│                                         │                       │
│                                    git commit + push            │
└─────────────────────────────────────────────────────────────────┘
                                         │
                                         v
                                  Portfolio Website
```

Three stages, each its own module:

1. **Ingest** — Fetches ~1000 high-elo players, resolves their PUUIDs, grabs their last 5 ranked matches (deduped), and saves raw JSON to disk. A dual-bucket rate limiter (20 req/s *and* 100 req/2min) keeps us within Riot's limits, and a 403-counter detects expired API keys within 3 requests instead of burning retries for 30 minutes.

2. **Transform** — Parses 10 participant rows per match, filters remakes (< 15 min) and missing roles, then aggregates by champion + role + patch: win rate, pick rate, avg KDA. Selects the top 2 per role with a minimum 10-game threshold.

3. **Output** — Writes three JSON files with atomic writes (temp file + `os.replace()`) so a crash mid-write can't produce a half-written file.

The whole run takes 10–15 minutes, well within GitHub Actions' free tier.

---

## Design Decisions Worth Knowing About

**Idempotent reruns.** A `processed_matches.json` cache tracks every match already fetched. Re-running the pipeline on the same day doesn't re-fetch anything. The PUUID cache works the same way — PUUIDs never change, so that cache grows forever and never needs invalidation.

**Crash safety.** The PUUID cache saves every 100 lookups, not just at the end. The processed-matches cache updates after each match fetch. If the pipeline dies at minute 14 of a 15-minute run, it picks up where it left off.

**Dry-run mode.** `--dry-run` skips all API calls and re-uses the most recent raw data partition. This is how I iterated on the transform layer without burning API quota — the portfolio equivalent of a staging environment.

**No database.** The entire dataset is a few MB. Pandas handles it in memory in seconds. A database would add migrations, backups, and connection management for zero benefit at this scale.

---

## Enterprise vs Portfolio

The point of this table isn't to apologize for using simple tools. It's to show I know what the production version looks like and why I didn't need it here.

| What I used | What a company would use | Why the swap works |
|---|---|---|
| GitHub Actions cron | Airflow, Prefect, Dagster | Both are scheduled orchestrators. Actions can't do backfills or task-level retries, but handles a single weekly job fine. |
| pandas in-memory | Snowflake / BigQuery + dbt | Data is a few MB. No warehouse needed. The `pipeline-enterprise/dbt/` folder shows what the SQL version looks like. |
| `data/raw/` in Git | S3 / GCS data lake | Git versions files naturally. Would hit repo size limits at production volume. |
| Python transform scripts | dbt models | dbt shines on SQL warehouses with testing and lineage. The enterprise reference includes equivalent dbt models. |
| JSON files committed to Git | API endpoint + database | Works because data updates weekly and the frontend just fetches static files. No query flexibility. |
| `time.sleep()` rate limiter | API gateway, queue with backpressure | In-process, single-node, doesn't survive restarts. Fine for a batch job. |
| `.env` file | Secrets Manager, Vault | GitHub Secrets handles CI. Locally, `.env` is good enough. |
| `logging` to stdout | Datadog, OpenTelemetry | Actions captures stdout. In prod, you'd want searchable, alertable logs. |
| `tenacity` retries | Circuit breakers, dead-letter queues | Failed items should go to a DLQ at scale. Here, in-process retry is sufficient. |

See [`pipeline-enterprise/`](pipeline-enterprise/) for reference implementations: an Airflow DAG, dbt models, and a docker-compose stack.

---

## Output

The portfolio site consumes `top_champions.json`:

```json
{
  "TOP": [
    {"champion": "Ambessa", "win_rate": 0.532, "pick_rate": 0.124, "games": 87, "avg_kda": 3.21},
    {"champion": "K'Sante", "win_rate": 0.518, "pick_rate": 0.087, "games": 61, "avg_kda": 2.94}
  ],
  "JUNGLE": ["..."],
  "MIDDLE": ["..."],
  "BOTTOM": ["..."],
  "UTILITY": ["..."]
}
```

Also generated: `meta_summary.json` (patch, region, match count, timestamp) and `champions_by_role.json` (full stats for all champions, not just top 2).

---

## Quick Start

**Prerequisites**: Python 3.11+, a [Riot API key](https://developer.riotgames.com)

> Apply for a *Personal Project* key if you can — it doesn't expire every 24 hours. Takes a few days for Riot to approve.

```bash
git clone <repo-url> && cd lol-meta-tracker
pip install -r requirements.txt

cp .env.example .env
# Edit .env → set RIOT_API_KEY

python scripts/test_api_key.py        # Verify your key works
python -m pipeline.main               # Full run (~10-15 min)
python -m pipeline.main --dry-run     # Skip API, reuse cached data
pytest tests/ -v                      # Run tests (API test auto-skipped without key)
```

**GitHub Actions**: Push to GitHub, add `RIOT_API_KEY` as a repository secret, trigger manually from Actions > Weekly Meta Refresh > Run workflow. After that, it runs every Monday at 06:00 UTC.

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
├── pipeline-enterprise/       # What this would look like at scale
│   ├── airflow/dags/          # Airflow DAG (ingest >> transform >> output)
│   ├── dbt/models/            # SQL equivalents of transform.py
│   └── docker-compose.yml     # Local Airflow + Postgres
├── data/
│   ├── raw/                   # Raw API responses, partitioned by date (gitignored)
│   ├── cache/                 # PUUID + processed-match caches (gitignored)
│   └── output/                # Final JSONs (committed by CI)
├── tests/                     # 40+ tests
│   ├── test_ingest.py         # 13 mocked API tests (zero network I/O)
│   ├── test_transform.py      # 20+ unit tests with fixture data
│   ├── test_output.py         # 7 serialization tests
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
| **Unique matches/run** | ~1500–3000 |
| **Runtime** | 10–15 minutes |

Tight scope is intentional — it keeps us well within Riot's rate limits and GitHub Actions' 2000 free minutes/month.

---

## License

MIT
