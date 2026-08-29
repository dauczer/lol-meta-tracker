"""
Stage 3 — Output

Writes the three consumable JSON files to data/output/:
  - meta_summary.json      — high-level stats (patch, region, totals)
  - top_champions.json     — top N champions per role
  - champions_by_role.json — full breakdown per role (all threshold-passing champs)
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pandas as pd

from pipeline import config

logger = logging.getLogger(__name__)


def current_utc_timestamp() -> str:
    """Return the canonical timestamp format shared by all output files."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write(path: Path, data: object) -> None:
    """Write JSON atomically: write to .tmp then os.replace to avoid half-written files."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    os.replace(tmp, path)


def _round_floats(record: dict[str, Any]) -> dict[str, Any]:
    """Round floating-point fields to human-readable precision."""
    return {
        **record,
        "win_rate": round(record["win_rate"], 3),
        "pick_rate": round(record["pick_rate"], 3),
        "avg_kda": round(record["avg_kda"], 2),
        "games": int(record["games_played"]),
        "champion": record["champion_name"],
    }


def _clean_record(record: dict[str, Any]) -> dict[str, Any]:
    """Produce a clean output dict with canonical key names."""
    cleaned = _round_floats(record)
    # Remove internal column names
    cleaned.pop("games_played", None)
    cleaned.pop("champion_name", None)
    return cleaned


def _load_json_object(path: Path) -> dict[str, Any] | None:
    """Load a previous output defensively; an invalid file disables comparison."""
    if not path.exists():
        return None
    try:
        raw: Any = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Cannot load previous output %s: %s", path, exc)
        return None
    if not isinstance(raw, dict):
        logger.warning("Previous output %s is not a JSON object.", path)
        return None
    return cast(dict[str, Any], raw)


def load_previous_outputs(
    output_dir: Path,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Load the committed snapshot before the current run overwrites it."""
    return (
        _load_json_object(output_dir / "meta_summary.json"),
        _load_json_object(output_dir / "champions_by_role.json"),
    )


def _wilson_lower_bound(wins: int, games: int, z: float = 1.96) -> float:
    """Lower bound of a 95% Wilson interval for a binomial win rate."""
    if games <= 0:
        return 0.0
    p = wins / games
    denominator = 1 + z**2 / games
    centre = p + z**2 / (2 * games)
    margin = z * math.sqrt((p * (1 - p) + z**2 / (4 * games)) / games)
    return (centre - margin) / denominator


def _portfolio_rows_by_role(
    stats: pd.DataFrame,
    patch: str,
) -> dict[str, list[dict[str, Any]]]:
    """Build statistically ranked, display-ready rows for the portfolio output."""
    eligible = stats[
        (stats["patch"] == patch)
        & (stats["games_played"] >= config.PORTFOLIO_MIN_GAMES)
    ]
    result: dict[str, list[dict[str, Any]]] = {}

    for role in config.ROLES:
        role_rows: list[dict[str, Any]] = []
        records = cast(
            list[dict[str, Any]],
            eligible[eligible["team_position"] == role].to_dict("records"),
        )
        for record in records:
            games = int(record["games_played"])
            wins = int(record["wins"])
            role_rows.append(
                {
                    "champion": str(record["champion_name"]),
                    "win_rate": round(float(record["win_rate"]), 3),
                    "pick_rate": round(float(record["pick_rate"]), 3),
                    "games": games,
                    "kda_ratio": round(float(record["avg_kda"]), 2),
                    "_ranking_score": _wilson_lower_bound(wins, games),
                }
            )
        role_rows.sort(
            key=lambda row: (float(row["_ranking_score"]), int(row["games"])),
            reverse=True,
        )
        result[role] = role_rows

    return result


def _previous_role_index(
    previous_by_role: dict[str, Any],
    role: str,
) -> dict[str, dict[str, Any]]:
    raw_rows: Any = previous_by_role.get(role, [])
    if not isinstance(raw_rows, list):
        return {}

    result: dict[str, dict[str, Any]] = {}
    for raw in raw_rows:
        if not isinstance(raw, dict):
            continue
        row = cast(dict[str, Any], raw)
        champion = row.get("champion")
        games = row.get("games")
        if (
            isinstance(champion, str)
            and isinstance(games, (int, float))
            and int(games) >= config.PORTFOLIO_MIN_GAMES
        ):
            result[champion] = row
    return result


def write_portfolio_snapshot(
    stats: pd.DataFrame,
    total_matches: int,
    patch: str,
    output_dir: Path,
    generated_at: str,
    previous_meta: dict[str, Any] | None = None,
    previous_by_role: dict[str, Any] | None = None,
    total_champions: int | None = None,
) -> Path:
    """Write the self-contained, comparison-aware payload consumed by the portfolio."""
    ranked_rows = _portfolio_rows_by_role(stats, patch)
    missing_roles = [role for role, rows in ranked_rows.items() if not rows]
    if missing_roles:
        raise ValueError(
            "Portfolio snapshot has no statistically eligible champion for roles: "
            + ", ".join(missing_roles)
        )
    same_patch = bool(previous_meta and previous_meta.get("patch") == patch)
    comparison_available = same_patch and previous_by_role is not None
    movement_candidates: list[dict[str, Any]] = []
    role_output: dict[str, dict[str, list[dict[str, Any]]]] = {}

    for role in config.ROLES:
        previous_index = (
            _previous_role_index(previous_by_role, role)
            if comparison_available and previous_by_role is not None
            else {}
        )

        for row in ranked_rows[role]:
            previous = previous_index.get(str(row["champion"]))
            delta: float | None = None
            if previous is not None and isinstance(previous.get("pick_rate"), (int, float)):
                previous_pick_rate = float(previous["pick_rate"])
                delta = round(float(row["pick_rate"]) - previous_pick_rate, 3)
                if abs(delta) >= config.MIN_PICK_RATE_MOVEMENT:
                    movement_candidates.append(
                        {
                            "role": role,
                            "champion": row["champion"],
                            "win_rate": row["win_rate"],
                            "pick_rate": row["pick_rate"],
                            "previous_pick_rate": round(previous_pick_rate, 3),
                            "pick_rate_delta": delta,
                            "games": row["games"],
                        }
                    )
            row["pick_rate_delta"] = delta

        leaders: list[dict[str, Any]] = []
        for rank, row in enumerate(
            ranked_rows[role][: config.PORTFOLIO_TOP_N_PER_ROLE],
            start=1,
        ):
            leaders.append(
                {
                    "rank": rank,
                    **{key: value for key, value in row.items() if key != "_ranking_score"},
                }
            )
        role_output[role] = {"leaders": leaders}

    movers_up = sorted(
        [row for row in movement_candidates if float(row["pick_rate_delta"]) > 0],
        key=lambda row: float(row["pick_rate_delta"]),
        reverse=True,
    )[:3]
    movers_down = sorted(
        [row for row in movement_candidates if float(row["pick_rate_delta"]) < 0],
        key=lambda row: float(row["pick_rate_delta"]),
    )[:3]

    if comparison_available:
        comparison_reason: str | None = None
    elif previous_meta is None or previous_by_role is None:
        comparison_reason = "no_previous_snapshot"
    else:
        comparison_reason = "patch_changed"

    snapshot: dict[str, Any] = {
        "schema_version": 2,
        "generated_at": generated_at,
        "scope": {
            "patch": patch,
            "region": "EUW",
            "tiers": config.TIERS,
            "queue": "RANKED_SOLO_5x5",
            "queue_id": config.QUEUE_ID,
            "lookback_days": config.MATCH_LOOKBACK_DAYS,
        },
        "sample": {
            "matches": total_matches,
            "champions": total_champions
            if total_champions is not None
            else int(stats[stats["patch"] == patch]["champion_name"].nunique()),
        },
        "methodology": {
            "ranking": "wilson_lower_bound_95",
            "minimum_games": config.PORTFOLIO_MIN_GAMES,
            "movement_metric": "pick_rate",
            "minimum_movement": config.MIN_PICK_RATE_MOVEMENT,
        },
        "comparison": {
            "available": comparison_available,
            "same_patch": same_patch,
            "previous_generated_at": (
                previous_meta.get("last_updated") if previous_meta else None
            ),
            "previous_patch": previous_meta.get("patch") if previous_meta else None,
            "reason": comparison_reason,
        },
        "roles": role_output,
        "movers": {
            "up": movers_up if comparison_available else [],
            "down": movers_down if comparison_available else [],
        },
    }

    path = output_dir / "portfolio_snapshot.json"
    _atomic_write(path, snapshot)
    logger.info("Written: %s", path)
    return path


def write_meta_summary(
    stats: pd.DataFrame,
    total_matches: int,
    patch: str,
    output_dir: Path,
    generated_at: str | None = None,
) -> Path:
    """Write meta_summary.json."""
    summary: dict[str, Any] = {
        "patch": patch,
        "region": "EUW",
        "tiers": config.TIERS,
        "total_matches": total_matches,
        "total_champions_tracked": int(
            stats[stats["patch"] == patch]["champion_name"].nunique()
        ),
        "last_updated": generated_at or current_utc_timestamp(),
    }

    path = output_dir / "meta_summary.json"
    _atomic_write(path, summary)
    logger.info("Written: %s", path)
    return path


def write_top_champions(
    top_by_role: dict[str, list[dict[str, Any]]],
    output_dir: Path,
) -> Path:
    """Write top_champions.json — top N per role, rounded for display."""
    cleaned: dict[str, list[dict[str, Any]]] = {}
    for role, champs in top_by_role.items():
        cleaned[role] = [_clean_record(c) for c in champs]

    path = output_dir / "top_champions.json"
    _atomic_write(path, cleaned)
    logger.info("Written: %s", path)
    return path


def write_champions_by_role(
    stats: pd.DataFrame,
    patch: str,
    output_dir: Path,
    min_games: int = config.MIN_GAMES_THRESHOLD,
) -> Path:
    """
    Write champions_by_role.json — all champions meeting the min-games
    threshold on the current patch, grouped by role, sorted by win_rate.
    """
    patch_stats = stats[
        (stats["patch"] == patch) & (stats["games_played"] >= min_games)
    ]

    result: dict[str, list[dict[str, Any]]] = {}
    for role in config.ROLES:
        role_data = patch_stats[patch_stats["team_position"] == role].sort_values(
            "win_rate", ascending=False
        )
        records = cast(list[dict[str, Any]], role_data.to_dict("records"))
        result[role] = [_clean_record(r) for r in records]

    path = output_dir / "champions_by_role.json"
    _atomic_write(path, result)
    logger.info("Written: %s", path)
    return path
