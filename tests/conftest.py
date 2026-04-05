from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_match() -> dict:
    with open(FIXTURES_DIR / "sample_match.json") as f:
        return json.load(f)


@pytest.fixture
def tmp_raw_files(tmp_path: Path, sample_match: dict) -> list[Path]:
    """Temporary list of raw match files pre-populated with one sample match."""
    match_dir = tmp_path / "2026-04-04" / "matches"
    match_dir.mkdir(parents=True)
    file_path = match_dir / "EUW1_0000000001.json"
    file_path.write_text(json.dumps(sample_match))
    return [file_path]
