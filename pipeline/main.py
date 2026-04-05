"""
Pipeline orchestrator — runs all stages in sequence.

Usage:
  python -m pipeline.main           # full run (requires RIOT_API_KEY)
  python -m pipeline.main --dry-run # skip API calls, use cached raw data
"""
from __future__ import annotations

import argparse
import logging
import sys

from pipeline import config
from pipeline.ingest import run_ingestion
from pipeline.output import (
    write_champions_by_role,
    write_meta_summary,
    write_top_champions,
)
from pipeline.transform import (
    aggregate_stats,
    current_patch,
    filter_valid,
    parse_matches,
    top_champions_per_role,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="LoL Meta Tracker Pipeline")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip API calls and use cached raw data from the last run.",
    )
    args = parser.parse_args()

    logger.info("=== LoL Meta Tracker — pipeline start (dry_run=%s) ===", args.dry_run)

    # ------------------------------------------------------------------
    # Stage 1: Ingest
    # ------------------------------------------------------------------
    try:
        raw_files = run_ingestion(dry_run=args.dry_run)
    except RuntimeError as exc:
        logger.error("Ingestion failed: %s", exc)
        sys.exit(1)

    logger.info("Stage 1 complete: %d raw match files available.", len(raw_files))

    # ------------------------------------------------------------------
    # Stage 2: Transform
    # ------------------------------------------------------------------
    df = parse_matches(raw_files)

    if df.empty:
        logger.error(
            "No data to transform. Make sure data/raw/ contains match files."
        )
        sys.exit(1)

    df = filter_valid(df)
    total_matches = int(df["match_id"].nunique())

    stats = aggregate_stats(df)
    patch = current_patch(stats)
    top = top_champions_per_role(stats, patch)

    logger.info(
        "Stage 2 complete: patch=%s, matches=%d, champions=%d.",
        patch,
        total_matches,
        stats["champion_name"].nunique(),
    )

    # ------------------------------------------------------------------
    # Stage 3: Output
    # ------------------------------------------------------------------
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    write_meta_summary(stats, total_matches, patch, config.OUTPUT_DIR)
    write_top_champions(top, config.OUTPUT_DIR)
    write_champions_by_role(stats, patch, config.OUTPUT_DIR)

    logger.info(
        "Stage 3 complete: output written to %s.", config.OUTPUT_DIR
    )
    logger.info("=== Pipeline finished successfully. ===")


if __name__ == "__main__":
    main()
