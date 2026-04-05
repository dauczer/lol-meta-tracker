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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline import config

logger = logging.getLogger(__name__)


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


def write_meta_summary(
    stats: pd.DataFrame,
    total_matches: int,
    patch: str,
    output_dir: Path,
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
        "last_updated": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    path = output_dir / "meta_summary.json"
    path.write_text(json.dumps(summary, indent=2))
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
    path.write_text(json.dumps(cleaned, indent=2))
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
        result[role] = [_clean_record(r) for r in role_data.to_dict("records")]

    path = output_dir / "champions_by_role.json"
    path.write_text(json.dumps(result, indent=2))
    logger.info("Written: %s", path)
    return path
