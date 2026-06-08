#!/usr/bin/env python3
"""Build persistent per-school parquet caches for the outcomes API.

Run this on the Render shell after uploading/extracting platform_parquet. It
does not warm the in-process response cache; it creates files under
OUTCOMES_SCHOOL_CACHE_DIR so first school loads do not have to scan the full
platform bundle.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "api"))

import app as outcomes_app  # noqa: E402


CORE_WORK_DATASETS = [
    "annual_salary",
    "annual_seniority",
    "annual_employers",
    "annual_roles",
    "annual_industries",
    "annual_geography",
    "mobility",
    "employer_tenure",
]


def _school_rows() -> list[dict]:
    con = outcomes_app._connect()
    try:
        return outcomes_app._records_from_query(
            con,
            """
            SELECT CAST(unitid AS VARCHAR) AS unitid, MAX(school_name) AS school_name, COUNT(*) AS rows
            FROM read_parquet(?)
            GROUP BY unitid
            ORDER BY school_name, unitid
            """,
            [outcomes_app._dataset_glob("base_fact")],
        )
    finally:
        con.close()


def _work_dataset_names() -> list[str]:
    work_root = outcomes_app._platform_root() / "work_facts"
    if not work_root.exists():
        return []
    return sorted(path.name for path in work_root.iterdir() if path.is_dir())


def _load_school_filter(path: Path | None) -> set[str] | None:
    if not path:
        return None
    return {
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Warm persistent per-school API parquet caches.")
    parser.add_argument("--schools-file", type=Path, help="Optional file with one unitid per line.")
    parser.add_argument("--limit", type=int, help="Warm only the first N selected schools.")
    parser.add_argument("--skip-base", action="store_true", help="Do not warm base_fact school caches.")
    parser.add_argument(
        "--work",
        choices=["none", "core", "all"],
        default="core",
        help="Which work_facts datasets to warm. Default: core.",
    )
    args = parser.parse_args()

    selected = _load_school_filter(args.schools_file)
    schools = _school_rows()
    if selected is not None:
        schools = [row for row in schools if str(row["unitid"]) in selected]
    if args.limit:
        schools = schools[: args.limit]

    available_work = _work_dataset_names()
    if args.work == "none":
        work_datasets: list[str] = []
    elif args.work == "all":
        work_datasets = available_work
    else:
        work_datasets = [name for name in CORE_WORK_DATASETS if name in available_work]

    print(f"Platform root: {outcomes_app._platform_root()}", flush=True)
    print(f"School cache dir: {outcomes_app.SCHOOL_CACHE_DIR}", flush=True)
    print(f"Schools to warm: {len(schools):,}", flush=True)
    print(f"Work datasets: {', '.join(work_datasets) if work_datasets else 'none'}", flush=True)

    start = time.monotonic()
    warmed_base = 0
    warmed_work = 0
    for idx, row in enumerate(schools, start=1):
        unitid = str(row["unitid"])
        label = row.get("school_name") or unitid
        school_start = time.monotonic()
        if not args.skip_base:
            outcomes_app._school_base_cache(unitid)
            warmed_base += 1
        for dataset in work_datasets:
            outcomes_app._school_work_cache(dataset, unitid)
            warmed_work += 1
        elapsed = time.monotonic() - school_start
        print(f"[{idx:>4}/{len(schools):<4}] {unitid} {label} ({elapsed:.1f}s)", flush=True)

    total_elapsed = time.monotonic() - start
    print(
        f"Done. Warmed base={warmed_base:,}, work_files={warmed_work:,} "
        f"in {total_elapsed / 60:.1f} min.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
