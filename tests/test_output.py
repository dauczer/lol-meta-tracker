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
    write_portfolio_snapshot,
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


class TestWritePortfolioSnapshot:
    def _stats(self) -> pd.DataFrame:
        rows = []
        for index, role in enumerate(["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]):
            rows.extend([
                {
                    "patch": "14.7",
                    "champion_name": f"{role}Reliable",
                    "team_position": role,
                    "games_played": 100,
                    "wins": 60 - index,
                    "win_rate": (60 - index) / 100,
                    "pick_rate": 0.10 + index * 0.01,
                    "avg_kda": 3.0,
                },
                {
                    "patch": "14.7",
                    "champion_name": f"{role}SmallSample",
                    "team_position": role,
                    "games_played": 10,
                    "wins": 10,
                    "win_rate": 1.0,
                    "pick_rate": 0.01,
                    "avg_kda": 5.0,
                },
            ])
        return pd.DataFrame(rows)

    def _previous_by_role(self) -> dict[str, list[dict[str, object]]]:
        result: dict[str, list[dict[str, object]]] = {}
        roles = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
        for index, role in enumerate(roles):
            current_pick_rate = 0.10 + index * 0.01
            previous_pick_rate = (
                current_pick_rate - 0.02 if index < 3 else current_pick_rate + 0.02
            )
            result[role] = [{
                "champion": f"{role}Reliable",
                "games": 90,
                "pick_rate": previous_pick_rate,
            }]
        return result

    def test_writes_self_contained_schema_and_filters_small_samples(
        self,
        tmp_path: Path,
    ) -> None:
        write_portfolio_snapshot(
            self._stats(),
            total_matches=500,
            patch="14.7",
            output_dir=tmp_path,
            generated_at="2026-04-04T06:00:00Z",
            total_champions=999,
        )

        data = json.loads((tmp_path / "portfolio_snapshot.json").read_text())
        assert data["schema_version"] == 2
        assert data["generated_at"] == "2026-04-04T06:00:00Z"
        assert data["scope"]["patch"] == "14.7"
        assert data["sample"]["matches"] == 500
        assert data["sample"]["champions"] == 999
        assert data["methodology"]["ranking"] == "wilson_lower_bound_95"
        assert data["comparison"]["available"] is False
        assert set(data["roles"]) == {"TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"}
        assert data["roles"]["TOP"]["leaders"][0]["champion"] == "TOPReliable"
        assert "SmallSample" not in json.dumps(data["roles"])

    def test_computes_same_patch_movers(self, tmp_path: Path) -> None:
        write_portfolio_snapshot(
            self._stats(),
            total_matches=500,
            patch="14.7",
            output_dir=tmp_path,
            generated_at="2026-04-11T06:00:00Z",
            previous_meta={"patch": "14.7", "last_updated": "2026-04-04T06:00:00Z"},
            previous_by_role=self._previous_by_role(),
        )

        data = json.loads((tmp_path / "portfolio_snapshot.json").read_text())
        assert data["comparison"]["available"] is True
        assert data["comparison"]["same_patch"] is True
        assert data["movers"]["up"]
        assert data["movers"]["down"]
        assert all(row["pick_rate_delta"] > 0 for row in data["movers"]["up"])
        assert all(row["pick_rate_delta"] < 0 for row in data["movers"]["down"])

    def test_disables_comparison_across_patch_change(self, tmp_path: Path) -> None:
        write_portfolio_snapshot(
            self._stats(),
            total_matches=500,
            patch="14.7",
            output_dir=tmp_path,
            generated_at="2026-04-11T06:00:00Z",
            previous_meta={"patch": "14.6", "last_updated": "2026-04-04T06:00:00Z"},
            previous_by_role=self._previous_by_role(),
        )

        data = json.loads((tmp_path / "portfolio_snapshot.json").read_text())
        assert data["comparison"]["available"] is False
        assert data["comparison"]["reason"] == "patch_changed"
        assert data["movers"] == {"up": [], "down": []}
