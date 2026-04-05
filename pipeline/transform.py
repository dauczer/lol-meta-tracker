"""
Stage 2 — Transformation

Reads raw match JSONs and produces aggregated meta statistics.

Flow:
  1. parse_matches()          — extract participant rows from raw JSON files
  2. filter_valid()           — remove remakes and missing-role rows
  3. aggregate_stats()        — group by champion + role + patch, compute rates
  4. top_champions_per_role() — top N per role (min games threshold applied)
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline import config

logger = logging.getLogger(__name__)


def parse_matches(match_files: list[Path]) -> pd.DataFrame:
    """
    Load the given match JSON files and return a flat DataFrame with one
    row per match participant.

    Expected schema:
      match_id, champion_name, team_position, win (bool),
      kills, deaths, assists, game_duration (int seconds), patch (str)
    """
    rows: list[dict[str, Any]] = []

    if not match_files:
        logger.warning("No raw match files provided.")
        return pd.DataFrame()

    logger.info("Parsing %d raw match files...", len(match_files))

    for path in match_files:
        try:
            with path.open() as f:
                match = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Skipping %s: %s", path.name, exc)
            continue

        try:
            info = match["info"]
            match_id: str = match["metadata"]["matchId"]
            game_duration: int = info["gameDuration"]
            game_version: str = info.get("gameVersion", "0.0")
            patch = ".".join(game_version.split(".")[:2])

            for p in info["participants"]:
                rows.append(
                    {
                        "match_id": match_id,
                        "champion_name": p["championName"],
                        "team_position": p.get("teamPosition", ""),
                        "win": bool(p["win"]),
                        "kills": int(p["kills"]),
                        "deaths": int(p["deaths"]),
                        "assists": int(p["assists"]),
                        "game_duration": game_duration,
                        "patch": patch,
                    }
                )
        except (KeyError, TypeError) as exc:
            logger.warning("Skipping malformed match %s: %s", path.name, exc)
            continue

    df = pd.DataFrame(rows)
    logger.info("Parsed %d participant rows from %d files.", len(df), len(match_files))
    return df


def filter_valid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows that would skew statistics:
      - Remakes: game_duration < MIN_GAME_DURATION (900 s)
      - Missing role: empty teamPosition string
    """
    if df.empty:
        return df

    before = len(df)
    df = df[
        (df["game_duration"] >= config.MIN_GAME_DURATION)
        & (df["team_position"] != "")
    ].copy()
    removed = before - len(df)

    if removed:
        logger.info("Filtered out %d rows (remakes / missing role).", removed)

    return df


def aggregate_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate participant rows into per-champion-role-patch statistics.

    Computed columns:
      games_played  — number of games
      wins          — number of wins
      win_rate      — wins / games_played
      pick_rate     — games_played / (unique matches on that patch × 2 teams)
      avg_kda       — (kills + assists) / max(deaths, 1)
    """
    if df.empty:
        return pd.DataFrame()

    # Unique match count per patch (denominator for pick_rate)
    patch_match_counts = (
        df.groupby("patch")["match_id"].nunique().rename("patch_matches")
    )

    grouped = (
        df.groupby(["champion_name", "team_position", "patch"])
        .agg(
            games_played=("win", "count"),
            wins=("win", "sum"),
            total_kills=("kills", "sum"),
            total_deaths=("deaths", "sum"),
            total_assists=("assists", "sum"),
        )
        .reset_index()
    )

    # Join patch match counts for pick_rate denominator
    grouped = grouped.merge(patch_match_counts, on="patch", how="left")

    grouped["win_rate"] = grouped["wins"] / grouped["games_played"]
    # 2 teams per match → each match contributes 2 picks per role
    grouped["pick_rate"] = grouped["games_played"] / (grouped["patch_matches"] * 2)
    grouped["avg_kda"] = (grouped["total_kills"] + grouped["total_assists"]) / grouped[
        "total_deaths"
    ].clip(lower=1)

    # Drop helper columns not needed downstream
    grouped = grouped.drop(
        columns=["total_kills", "total_deaths", "total_assists", "patch_matches"]
    )

    logger.info(
        "Aggregated stats: %d champion-role-patch combinations across %d patches.",
        len(grouped),
        grouped["patch"].nunique(),
    )
    return grouped


def current_patch(stats: pd.DataFrame) -> str:
    """Return the lexicographically latest patch string in the stats DataFrame."""
    return stats["patch"].max()


def top_champions_per_role(
    stats: pd.DataFrame,
    patch: str,
    n: int = config.TOP_N_PER_ROLE,
    min_games: int = config.MIN_GAMES_THRESHOLD,
) -> dict[str, list[dict[str, Any]]]:
    """
    For each role, return the top N champions by win_rate on the given patch.

    Champions with fewer than min_games are excluded for statistical relevance.

    Returns: {role: [{"champion": ..., "win_rate": ..., "pick_rate": ...,
                       "games_played": ..., "avg_kda": ...}, ...]}
    """
    patch_stats = stats[stats["patch"] == patch]
    eligible = patch_stats[patch_stats["games_played"] >= min_games]

    result: dict[str, list[dict[str, Any]]] = {}

    for role in config.ROLES:
        role_data = eligible[eligible["team_position"] == role]
        top = role_data.nlargest(n, "win_rate")
        result[role] = top[
            ["champion_name", "win_rate", "pick_rate", "games_played", "avg_kda"]
        ].to_dict("records")

        if not result[role]:
            logger.warning(
                "No champions met the threshold for role %s on patch %s.", role, patch
            )

    return result
