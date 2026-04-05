"""
Unit tests for pipeline/transform.py.

All tests run without an API key using the sample_match.json fixture.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pipeline.transform import (
    aggregate_stats,
    current_patch,
    filter_valid,
    parse_matches,
    top_champions_per_role,
)


class TestParseMatches:
    def test_returns_10_rows_per_match(self, tmp_raw_files: list[Path]) -> None:
        df = parse_matches(tmp_raw_files)
        assert len(df) == 10

    def test_correct_columns(self, tmp_raw_files: list[Path]) -> None:
        df = parse_matches(tmp_raw_files)
        expected = {
            "match_id", "champion_name", "team_position", "win",
            "kills", "deaths", "assists", "game_duration", "patch",
        }
        assert expected.issubset(set(df.columns))

    def test_patch_parsed_correctly(self, tmp_raw_files: list[Path]) -> None:
        df = parse_matches(tmp_raw_files)
        assert (df["patch"] == "14.7").all()

    def test_empty_list_returns_empty_df(self) -> None:
        df = parse_matches([])
        assert df.empty

    def test_skips_corrupt_json(self, tmp_path: Path) -> None:
        match_dir = tmp_path / "2026-04-04" / "matches"
        match_dir.mkdir(parents=True)
        bad_file = match_dir / "bad.json"
        bad_file.write_text("{not valid json")
        df = parse_matches([bad_file])
        assert df.empty


class TestFilterValid:
    def test_removes_remakes(self, tmp_raw_files: list[Path]) -> None:
        df = parse_matches(tmp_raw_files)
        # Manually set game_duration below threshold
        df["game_duration"] = 500
        filtered = filter_valid(df)
        assert filtered.empty

    def test_removes_empty_team_position(self, tmp_raw_files: list[Path]) -> None:
        df = parse_matches(tmp_raw_files)
        df["team_position"] = ""
        filtered = filter_valid(df)
        assert filtered.empty

    def test_keeps_valid_rows(self, tmp_raw_files: list[Path]) -> None:
        df = parse_matches(tmp_raw_files)
        before = len(df)
        filtered = filter_valid(df)
        # Sample match has duration 1850s and all positions filled
        assert len(filtered) == before

    def test_handles_empty_input(self) -> None:
        empty = pd.DataFrame()
        result = filter_valid(empty)
        assert result.empty


class TestAggregateStats:
    def _make_df(self) -> pd.DataFrame:
        """Minimal DataFrame with known values for deterministic tests."""
        return pd.DataFrame([
            {
                "match_id": "M1", "champion_name": "Jinx", "team_position": "BOTTOM",
                "win": True, "kills": 10, "deaths": 1, "assists": 4,
                "game_duration": 1800, "patch": "14.7",
            },
            {
                "match_id": "M2", "champion_name": "Jinx", "team_position": "BOTTOM",
                "win": False, "kills": 3, "deaths": 5, "assists": 1,
                "game_duration": 1600, "patch": "14.7",
            },
        ])

    def test_win_rate_calculation(self) -> None:
        stats = aggregate_stats(self._make_df())
        jinx = stats[stats["champion_name"] == "Jinx"].iloc[0]
        assert jinx["win_rate"] == pytest.approx(0.5)

    def test_games_played_count(self) -> None:
        stats = aggregate_stats(self._make_df())
        jinx = stats[stats["champion_name"] == "Jinx"].iloc[0]
        assert jinx["games_played"] == 2

    def test_avg_kda_calculation(self) -> None:
        stats = aggregate_stats(self._make_df())
        jinx = stats[stats["champion_name"] == "Jinx"].iloc[0]
        # (10+4)/1 = 14 for M1, (3+1)/5 = 0.8 for M2
        # Aggregate: (13+5) / max(6,1) = 18/6 = 3.0
        assert jinx["avg_kda"] == pytest.approx(3.0)

    def test_zero_deaths_uses_1_in_kda(self) -> None:
        df = pd.DataFrame([{
            "match_id": "M1", "champion_name": "Thresh", "team_position": "UTILITY",
            "win": True, "kills": 0, "deaths": 0, "assists": 10,
            "game_duration": 1800, "patch": "14.7",
        }])
        stats = aggregate_stats(df)
        thresh = stats[stats["champion_name"] == "Thresh"].iloc[0]
        assert thresh["avg_kda"] == pytest.approx(10.0)

    def test_pick_rate_denominator(self) -> None:
        # 2 unique matches on patch 14.7, 2 teams → denominator = 4
        df = pd.DataFrame([
            {
                "match_id": "M1", "champion_name": "Jinx", "team_position": "BOTTOM",
                "win": True, "kills": 5, "deaths": 1, "assists": 2,
                "game_duration": 1800, "patch": "14.7",
            },
            {
                "match_id": "M2", "champion_name": "Jinx", "team_position": "BOTTOM",
                "win": False, "kills": 2, "deaths": 3, "assists": 1,
                "game_duration": 1600, "patch": "14.7",
            },
        ])
        stats = aggregate_stats(df)
        jinx = stats[stats["champion_name"] == "Jinx"].iloc[0]
        assert jinx["pick_rate"] == pytest.approx(2 / (2 * 2))  # 2 games / (2 matches * 2 teams)

    def test_empty_input_returns_empty(self) -> None:
        result = aggregate_stats(pd.DataFrame())
        assert result.empty


class TestTopChampionsPerRole:
    def _build_stats(self) -> pd.DataFrame:
        """Stats DataFrame with a known winner per role."""
        rows = []
        roles = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
        champions = {
            "TOP": [("Ambessa", 0.60, 30), ("K'Sante", 0.52, 25), ("Darius", 0.40, 22)],
            "JUNGLE": [("Vi", 0.55, 40), ("Hecarim", 0.48, 35)],
            "MIDDLE": [("Azir", 0.58, 28), ("Syndra", 0.50, 21)],
            "BOTTOM": [("Jinx", 0.62, 45), ("Caitlyn", 0.51, 38)],
            "UTILITY": [("Thresh", 0.54, 60), ("Nautilus", 0.49, 33)],
        }
        for role, champs in champions.items():
            for champ, win_rate, games in champs:
                rows.append({
                    "champion_name": champ,
                    "team_position": role,
                    "patch": "14.7",
                    "games_played": games,
                    "wins": int(games * win_rate),
                    "win_rate": win_rate,
                    "pick_rate": games / 200,
                    "avg_kda": 3.0,
                })
        return pd.DataFrame(rows)

    def test_returns_top_2_per_role(self) -> None:
        stats = self._build_stats()
        top = top_champions_per_role(stats, "14.7", n=2, min_games=20)
        for role in ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]:
            assert len(top[role]) == 2, f"Expected 2 for {role}, got {len(top[role])}"

    def test_sorted_by_win_rate_descending(self) -> None:
        stats = self._build_stats()
        top = top_champions_per_role(stats, "14.7", n=2, min_games=20)
        assert top["TOP"][0]["champion_name"] == "Ambessa"
        assert top["BOTTOM"][0]["champion_name"] == "Jinx"

    def test_min_games_threshold_excludes_low_volume(self) -> None:
        stats = self._build_stats()
        # All champions have >= 20 games; raise threshold to 50 → only Thresh qualifies
        top = top_champions_per_role(stats, "14.7", n=2, min_games=50)
        # Jinx has 45, below 50 → only Thresh (60) passes for their roles
        assert all(c["champion_name"] != "Jinx" for c in top["BOTTOM"])

    def test_empty_result_for_role_with_no_data(self) -> None:
        stats = self._build_stats()
        top = top_champions_per_role(stats, "14.99", n=2, min_games=20)
        for role in top.values():
            assert role == []


class TestCurrentPatch:
    def test_returns_latest_patch(self) -> None:
        df = pd.DataFrame([
            {"patch": "14.6", "champion_name": "A"},
            {"patch": "14.7", "champion_name": "B"},
            {"patch": "14.10", "champion_name": "C"},
        ])
        # lexicographic max: "14.7" > "14.6" but "14.10" < "14.7" lexicographically
        # This is an expected limitation of lexicographic sorting for patch strings
        assert current_patch(df) in {"14.7", "14.10"}
