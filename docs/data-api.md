# LoL Meta Tracker -- Data API

## What this data is

Weekly snapshot of the **League of Legends high-elo meta** for the EUW region.

- **Players**: Challenger, Grandmaster, and Master tier (~1,000 players)
- **Game mode**: Ranked Solo/Duo (queue 420)
- **Refresh**: Every Monday at 06:00 UTC via GitHub Actions
- **Source**: Riot Games API (MATCH-V5, LEAGUE-EXP-V4)

Each run fetches the latest 5 matches per player, deduplicates, and computes win rates, pick rates, and KDA for every champion/role combination on the current patch.

---

## Output files

All files live in `data/output/` and are committed to the repository after each run.

### `meta_summary.json`

High-level stats about the current dataset.

```json
{
  "patch": "16.7",
  "region": "EUW",
  "tiers": ["CHALLENGER", "GRANDMASTER", "MASTER"],
  "total_matches": 3485,
  "total_champions_tracked": 172,
  "last_updated": "2026-04-05T11:42:01Z"
}
```

| Field | Type | Description |
|---|---|---|
| `patch` | string | Current game patch (e.g. `"16.7"`) |
| `region` | string | Always `"EUW"` |
| `tiers` | string[] | Tiers included in the sample |
| `total_matches` | int | Number of unique matches analyzed |
| `total_champions_tracked` | int | Number of distinct champions seen on this patch |
| `last_updated` | string | ISO 8601 UTC timestamp of the pipeline run |

### `top_champions.json`

Top 2 champions per role by win rate (minimum 10 games).

```json
{
  "TOP": [
    { "champion": "Illaoi", "win_rate": 0.773, "pick_rate": 0.004, "avg_kda": 1.81, "games": 22 },
    { "champion": "Nasus",  "win_rate": 0.655, "pick_rate": 0.005, "avg_kda": 1.90, "games": 29 }
  ],
  "JUNGLE": [...],
  "MIDDLE": [...],
  "BOTTOM": [...],
  "UTILITY": [...]
}
```

| Field | Type | Description |
|---|---|---|
| `champion` | string | Champion name |
| `win_rate` | float | Wins / games played (0.0 -- 1.0) |
| `pick_rate` | float | Games played / (total matches x 2) |
| `avg_kda` | float | (Kills + Assists) / max(Deaths, 1) |
| `games` | int | Number of games played |

### `champions_by_role.json`

Full breakdown of all champions meeting the minimum games threshold, grouped by role, sorted by win rate descending. Same fields as `top_champions.json` plus:

| Extra field | Type | Description |
|---|---|---|
| `team_position` | string | Role: `TOP`, `JUNGLE`, `MIDDLE`, `BOTTOM`, `UTILITY` |
| `patch` | string | Patch version |
| `wins` | int | Total wins |

---

## Fetching the data

The JSON files are served via GitHub's raw content URL. Replace `{user}` and `{repo}` with your GitHub username and repository name.

**Base URL:**
```
https://raw.githubusercontent.com/{user}/{repo}/main/data/output/
```

**Files:**
- `meta_summary.json`
- `top_champions.json`
- `champions_by_role.json`

### JavaScript example

```js
const BASE = "https://raw.githubusercontent.com/{user}/{repo}/main/data/output";

async function fetchMeta() {
  const [summary, topChamps, allChamps] = await Promise.all([
    fetch(`${BASE}/meta_summary.json`).then(r => r.json()),
    fetch(`${BASE}/top_champions.json`).then(r => r.json()),
    fetch(`${BASE}/champions_by_role.json`).then(r => r.json()),
  ]);

  return { summary, topChamps, allChamps };
}
```

### Champion icons

Champion icons are freely available from Riot's Data Dragon CDN:

```
https://ddragon.leagueoflegends.com/cdn/16.7.1/img/champion/{championName}.png
```

Replace `16.7.1` with the current patch version (add `.1` to the patch from `meta_summary.json`). The `{championName}` matches the `champion` field in the JSON output.

---

## Data freshness

- Data updates every **Monday at ~06:00 UTC**
- Check `meta_summary.json > last_updated` to confirm freshness
- If the pipeline fails (e.g. expired API key), the previous week's data remains in place
- GitHub raw content may be cached for up to 5 minutes; append `?t={timestamp}` to bust the cache if needed

---

## Limitations

- **EUW only** -- single region snapshot
- **High-elo only** -- Challenger/GM/Master meta differs from lower tiers
- **Sample size** -- ~1,500-3,000 matches per week; niche champions may not meet the minimum games threshold
- **Pick rate** -- computed relative to analyzed matches, not the entire player base
