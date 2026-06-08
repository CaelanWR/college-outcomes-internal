#!/usr/bin/env python3
"""Validate platform further-education facts before uploading a Render bundle."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb


ANCHORS = [
    {
        "name": "Columbia History Bachelors",
        "unitid": "190150",
        "cip4": "54.01",
        "min_total": 1000,
        "min_later_pct": 25.0,
        "min_law_pct": 4.0,
    },
]


def parquet_glob(root: Path, dataset: str) -> str:
    return str(root / "aggregate_facts" / dataset / "*.parquet")


def validate_anchor(con: duckdb.DuckDBPyConnection, root: Path, anchor: dict) -> list[str]:
    row = con.execute(
        f"""
        SELECT
          SUM(total_bachelors) AS total_bachelors,
          SUM(has_later_degree) AS has_later_degree,
          SUM(law_count) AS law_count,
          100.0 * SUM(has_later_degree) / NULLIF(SUM(total_bachelors), 0) AS later_pct,
          100.0 * SUM(law_count) / NULLIF(SUM(total_bachelors), 0) AS law_pct
        FROM read_parquet(?)
        WHERE CAST(unitid AS VARCHAR) = ?
          AND undergrad_cip4 = ?
        """,
        [parquet_glob(root, "postgrad"), anchor["unitid"], anchor["cip4"]],
    ).fetchone()
    total, later, law, later_pct, law_pct = row
    failures: list[str] = []
    if total is None:
        return [f"{anchor['name']}: no postgrad rows found"]
    checks = [
        ("total", float(total), anchor["min_total"]),
        ("later_pct", float(later_pct or 0), anchor["min_later_pct"]),
        ("law_pct", float(law_pct or 0), anchor["min_law_pct"]),
    ]
    for label, value, floor in checks:
        if value < floor:
            failures.append(f"{anchor['name']}: {label}={value:.2f} below floor {floor:.2f}")
    print(
        f"{anchor['name']}: total={total:,.0f} later={later:,.0f} "
        f"law={law:,.0f} later_pct={later_pct:.1f}% law_pct={law_pct:.1f}%"
    )
    return failures


def validate_degree_mix(con: duckdb.DuckDBPyConnection, root: Path) -> list[str]:
    rows = con.execute(
        """
        SELECT postgrad_degree, SUM(n_users) AS n
        FROM read_parquet(?)
        GROUP BY postgrad_degree
        ORDER BY n DESC
        """,
        [parquet_glob(root, "postgrad_flows")],
    ).fetchall()
    labels = {row[0] for row in rows}
    print("Postgrad degree labels:", ", ".join(f"{label} ({n:,.0f})" for label, n in rows[:10]))
    required = {"Masters", "LAW", "MBA", "MD"}
    missing = sorted(required - labels)
    return [f"missing expected postgrad degree label: {label}" for label in missing]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("platform_root", type=Path, help="Path to platform_parquet root")
    args = parser.parse_args()

    root = args.platform_root
    if not (root / "aggregate_facts" / "postgrad").exists():
        print(f"Missing aggregate_facts/postgrad under {root}", file=sys.stderr)
        return 2

    con = duckdb.connect()
    failures: list[str] = []
    for anchor in ANCHORS:
        failures.extend(validate_anchor(con, root, anchor))
    failures.extend(validate_degree_mix(con, root))

    if failures:
        print("\nFAILED postgrad validation:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("\nPostgrad validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
