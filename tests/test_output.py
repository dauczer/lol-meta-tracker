"""
Unit tests for pipeline/output.py.

Covers _clean_record field renaming and the three write_* functions.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pipeline.output import (
    _clean_record,
    write_champions_by_role,
    write_meta_summary,
    write_top_champions,
)


class TestCleanRecord:
    def test_renames_champion_name_to_champion(self) -> None:
        record = {
            "champion_name": "Ambessa",
            "win_rate": 0.532,
            "pick_rate": 0.124,
            "games_played": 87,
            "avg_kda": 3.21,
        }
        result = _clean_record(record)
        assert "champion" in result
        assert result["champion"] == "Ambessa"
        assert "champion_name" not in result

    def test_renames_games_played_to_games(self) -> None:
        record = {
            "champion_name": "Ambessa",
            "win_rate": 0.532,
            "pick_rate": 0.124,
            "games_played": 87,
            "avg_kda": 3.21,
        }
        result = _clean_record(record)
        assert "games" in result
        assert result["games"] == 87
        assert "games_played" not in result

    def test_rounds_floats(self) -> None:
        record = {
            "champion_name": "X",
            "win_rate": 0.532123,
            "pick_rate": 0.123456,
            "games_played": 10,
            "avg_kda": 3.2199,
        }
        result = _clean_record(record)
        assert result["win_rate"] == pytest.approx(0.532)
        assert result["pick_rate"] == pytest.approx(0.123)
        assert result["avg_kda"] == pytest.approx(3.22)

    def test_output_keys_are_canonical(self) -> None:
        record = {
            "champion_name": "X",
            "win_rate": 0.5,
            "pick_rate": 0.1,
            "games_played": 20,
            "avg_kda": 3.0,
        }
        result = _clean_record(record)
        assert set(result.keys()) == {"champion", "win_rate", "pick_rate", "games", "avg_kda"}


class TestWriteMetaSummary:
    def test_writes_correct_schema(self, tmp_path: Path) -> None:
        stats = pd.DataFrame([
            {"patch": "14.7", "champion_name": "Ambessa", "team_position": "TOP",
             "games_played": 50, "wins": 25, "win_rate": 0.5, "pick_rate": 0.1, "avg_kda": 3.0},
        ])
        write_meta_summary(stats, total_matches=100, patch="14.7", output_dir=tmp_path)
        data = json.loads((tmp_path / "meta_summary.json").read_text())
        assert data["patch"] == "14.7"
        assert data["region"] == "EUW"
        assert data["total_matches"] == 100
        assert "last_updated" in data
        assert "tiers" in data


class TestWriteTopChampions:
    def test_applies_clean_record_to_each_champion(self, tmp_path: Path) -> None:
        top_by_role = {
            "TOP": [{"champion_name": "Ambessa", "win_rate": 0.532, "pick_rate": 0.124,
                     "games_played": 87, "avg_kda": 3.21}],
        }
        write_top_champions(top_by_role, output_dir=tmp_path)
        data = json.loads((tmp_path / "top_champions.json").read_text())
        top_entry = data["TOP"][0]
        assert top_entry["champion"] == "Ambessa"
        assert "champion_name" not in top_entry
        assert "games_played" not in top_entry
        assert top_entry["games"] == 87


class TestWriteChampionsByRole:
    def test_filters_by_patch_and_min_games(self, tmp_path: Path) -> None:
        stats = pd.DataFrame([
            {"patch": "14.7", "champion_name": "Ambessa", "team_position": "TOP",
             "games_played": 50, "wins": 25, "win_rate": 0.5, "pick_rate": 0.1, "avg_kda": 3.0},
            {"patch": "14.7", "champion_name": "Teemo", "team_position": "TOP",
             "games_played": 2, "wins": 1, "win_rate": 0.5, "pick_rate": 0.01, "avg_kda": 2.0},
            {"patch": "14.6", "champion_name": "Darius", "team_position": "TOP",
             "games_played": 50, "wins": 30, "win_rate": 0.6, "pick_rate": 0.2, "avg_kda": 3.5},
        ])
        write_champions_by_role(stats, patch="14.7", output_dir=tmp_path, min_games=10)
        data = json.loads((tmp_path / "champions_by_role.json").read_text())
        top_names = [c["champion"] for c in data["TOP"]]
        assert "Ambessa" in top_names
        assert "Teemo" not in top_names   # below min_games
        assert "Darius" not in top_names  # wrong patch
