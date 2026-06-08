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
        "law_destination_min_n": 75,
        "law_destination_required_schools": [
            "Columbia University in the City of New York",
            "Harvard University",
            "New York University",
        ],
    },
]


def parquet_glob(root: Path, dataset: str) -> str:
    return str(root / "aggregate_facts" / dataset / "*.parquet")


def root_parquet_glob(root: Path, dataset: str) -> str:
    return str(root / dataset / "*.parquet")


def dataset_exists(root: Path, dataset: str) -> bool:
    return (root / dataset).exists()


def validate_school_coverage(con: duckdb.DuckDBPyConnection, root: Path) -> list[str]:
    if not dataset_exists(root, "base_fact"):
        print("Skipping school coverage check: base_fact missing.")
        return []
    base_schools = con.execute(
        """
        SELECT COUNT(DISTINCT CAST(unitid AS VARCHAR))
        FROM read_parquet(?)
        WHERE degree = 'Bachelors'
        """,
        [root_parquet_glob(root, "base_fact")],
    ).fetchone()[0]
    postgrad_schools = con.execute(
        """
        SELECT COUNT(DISTINCT CAST(unitid AS VARCHAR))
        FROM read_parquet(?)
        """,
        [parquet_glob(root, "postgrad")],
    ).fetchone()[0]
    print(f"Postgrad school coverage: {postgrad_schools:,} of {base_schools:,} bachelor schools")
    if base_schools and postgrad_schools < base_schools:
        return [f"postgrad facts cover {postgrad_schools} schools but base_fact has {base_schools} bachelor schools"]
    return []


def validate_global_mix(con: duckdb.DuckDBPyConnection, root: Path) -> list[str]:
    row = con.execute(
        """
        SELECT
          SUM(total_bachelors) AS total_bachelors,
          SUM(has_later_degree) AS has_later_degree,
          SUM(law_count) AS law_count,
          SUM(md_count) AS md_count,
          SUM(phd_count) AS phd_count,
          100.0 * SUM(has_later_degree) / NULLIF(SUM(total_bachelors), 0) AS later_pct
        FROM read_parquet(?)
        """,
        [parquet_glob(root, "postgrad")],
    ).fetchone()
    total, later, law, md, phd, later_pct = row
    print(
        f"Global postgrad mix: total={total:,.0f} later={later:,.0f} "
        f"later_pct={later_pct:.1f}% LAW={law:,.0f} MD={md:,.0f} PhD={phd:,.0f}"
    )
    failures: list[str] = []
    if float(later_pct or 0) < 12.0:
        failures.append(f"global later_degree_pct={float(later_pct or 0):.2f} below floor 12.00")
    for label, value in [("LAW", law), ("MD", md), ("PhD", phd)]:
        if float(value or 0) <= 0:
            failures.append(f"global {label} count is zero")
    return failures


def validate_flow_summary_consistency(con: duckdb.DuckDBPyConnection, root: Path) -> list[str]:
    rows = con.execute(
        """
        WITH summary_long AS (
          SELECT unitid, cohort_year, undergrad_cip4, 'Masters' AS postgrad_degree, SUM(masters_count) AS summary_n
          FROM read_parquet(?) GROUP BY unitid, cohort_year, undergrad_cip4
          UNION ALL
          SELECT unitid, cohort_year, undergrad_cip4, 'MBA', SUM(mba_count)
          FROM read_parquet(?) GROUP BY unitid, cohort_year, undergrad_cip4
          UNION ALL
          SELECT unitid, cohort_year, undergrad_cip4, 'LAW', SUM(law_count)
          FROM read_parquet(?) GROUP BY unitid, cohort_year, undergrad_cip4
          UNION ALL
          SELECT unitid, cohort_year, undergrad_cip4, 'MD', SUM(md_count)
          FROM read_parquet(?) GROUP BY unitid, cohort_year, undergrad_cip4
          UNION ALL
          SELECT unitid, cohort_year, undergrad_cip4, 'PhD', SUM(phd_count)
          FROM read_parquet(?) GROUP BY unitid, cohort_year, undergrad_cip4
          UNION ALL
          SELECT unitid, cohort_year, undergrad_cip4, 'Professional Doctorate', SUM(professional_doctorate_count)
          FROM read_parquet(?) GROUP BY unitid, cohort_year, undergrad_cip4
          UNION ALL
          SELECT unitid, cohort_year, undergrad_cip4, 'Other Doctorate', SUM(other_doctorate_count)
          FROM read_parquet(?) GROUP BY unitid, cohort_year, undergrad_cip4
        ),
        flow_long AS (
          SELECT unitid, cohort_year, undergrad_cip4, postgrad_degree, SUM(n_users) AS flow_n
          FROM read_parquet(?)
          WHERE postgrad_degree IN ('Masters', 'MBA', 'LAW', 'MD', 'PhD', 'Professional Doctorate', 'Other Doctorate')
          GROUP BY unitid, cohort_year, undergrad_cip4, postgrad_degree
        ),
        joined AS (
          SELECT
            COALESCE(s.unitid, f.unitid) AS unitid,
            COALESCE(s.cohort_year, f.cohort_year) AS cohort_year,
            COALESCE(s.undergrad_cip4, f.undergrad_cip4) AS undergrad_cip4,
            COALESCE(s.postgrad_degree, f.postgrad_degree) AS postgrad_degree,
            COALESCE(s.summary_n, 0) AS summary_n,
            COALESCE(f.flow_n, 0) AS flow_n
          FROM summary_long s
          FULL OUTER JOIN flow_long f
            ON s.unitid = f.unitid
           AND s.cohort_year = f.cohort_year
           AND COALESCE(s.undergrad_cip4, '') = COALESCE(f.undergrad_cip4, '')
           AND s.postgrad_degree = f.postgrad_degree
          WHERE COALESCE(s.summary_n, 0) > 0 OR COALESCE(f.flow_n, 0) > 0
        )
        SELECT
          COUNT(*) AS compared_cells,
          SUM(CASE WHEN ABS(summary_n - flow_n) > GREATEST(2, 0.05 * GREATEST(summary_n, flow_n)) THEN 1 ELSE 0 END) AS bad_cells,
          MAX(ABS(summary_n - flow_n)) AS max_abs_diff
        FROM joined
        """,
        [parquet_glob(root, "postgrad")] * 7 + [parquet_glob(root, "postgrad_flows")],
    ).fetchone()
    compared, bad, max_diff = rows
    doctor_row = con.execute(
        """
        WITH summary_doctor AS (
          SELECT unitid, cohort_year, undergrad_cip4, SUM(doctor_count) AS summary_n
          FROM read_parquet(?)
          GROUP BY unitid, cohort_year, undergrad_cip4
        ),
        flow_doctor AS (
          SELECT unitid, cohort_year, undergrad_cip4, SUM(n_users) AS flow_n
          FROM read_parquet(?)
          WHERE postgrad_degree IN ('PhD', 'LAW', 'MD', 'Professional Doctorate', 'Other Doctorate', 'Doctorate')
          GROUP BY unitid, cohort_year, undergrad_cip4
        ),
        joined AS (
          SELECT
            COALESCE(s.summary_n, 0) AS summary_n,
            COALESCE(f.flow_n, 0) AS flow_n
          FROM summary_doctor s
          FULL OUTER JOIN flow_doctor f
            ON s.unitid = f.unitid
           AND s.cohort_year = f.cohort_year
           AND COALESCE(s.undergrad_cip4, '') = COALESCE(f.undergrad_cip4, '')
          WHERE COALESCE(s.summary_n, 0) > 0 OR COALESCE(f.flow_n, 0) > 0
        )
        SELECT
          COUNT(*) AS compared_cells,
          SUM(CASE WHEN ABS(summary_n - flow_n) > GREATEST(2, 0.05 * GREATEST(summary_n, flow_n)) THEN 1 ELSE 0 END) AS bad_cells,
          MAX(ABS(summary_n - flow_n)) AS max_abs_diff
        FROM joined
        """,
        [parquet_glob(root, "postgrad"), parquet_glob(root, "postgrad_flows")],
    ).fetchone()
    doctor_compared, doctor_bad, doctor_max_diff = doctor_row
    print(
        f"Flow/summary consistency: explicit compared={compared:,} bad={bad or 0:,} max_abs_diff={max_diff or 0:,.1f}; "
        f"doctor compared={doctor_compared:,} bad={doctor_bad or 0:,} max_abs_diff={doctor_max_diff or 0:,.1f}"
    )
    failures: list[str] = []
    if int(bad or 0) > 0:
        failures.append(f"postgrad explicit flow totals disagree with summary counts in {bad} cells")
    if int(doctor_bad or 0) > 0:
        failures.append(f"postgrad doctor flow totals disagree with summary doctor_count in {doctor_bad} cells")
    return failures


def validate_destination_consistency(con: duckdb.DuckDBPyConnection, root: Path) -> list[str]:
    row = con.execute(
        """
        WITH flows AS (
          SELECT unitid, cohort_year, undergrad_cip4, postgrad_degree, SUM(n_users) AS flow_n
          FROM read_parquet(?)
          GROUP BY unitid, cohort_year, undergrad_cip4, postgrad_degree
        ),
        destinations AS (
          SELECT unitid, cohort_year, undergrad_cip4, postgrad_degree, SUM(n) AS destination_n
          FROM read_parquet(?)
          GROUP BY unitid, cohort_year, undergrad_cip4, postgrad_degree
        ),
        joined AS (
          SELECT
            f.flow_n,
            COALESCE(d.destination_n, 0) AS destination_n
          FROM flows f
          LEFT JOIN destinations d
            ON f.unitid = d.unitid
           AND f.cohort_year = d.cohort_year
           AND COALESCE(f.undergrad_cip4, '') = COALESCE(d.undergrad_cip4, '')
           AND f.postgrad_degree = d.postgrad_degree
          WHERE f.flow_n > 0
        )
        SELECT
          COUNT(*) AS compared_cells,
          SUM(CASE WHEN ABS(flow_n - destination_n) > GREATEST(2, 0.05 * flow_n) THEN 1 ELSE 0 END) AS bad_cells,
          MAX(ABS(flow_n - destination_n)) AS max_abs_diff
        FROM joined
        """,
        [parquet_glob(root, "postgrad_flows"), parquet_glob(root, "postgrad_destinations")],
    ).fetchone()
    compared, bad, max_diff = row
    print(f"Flow/destination consistency: compared={compared:,} bad={bad or 0:,} max_abs_diff={max_diff or 0:,.1f}")
    if int(bad or 0) > 0:
        return [f"postgrad destination totals disagree with flow totals in {bad} cells"]
    return []


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


def validate_destination_anchor(con: duckdb.DuckDBPyConnection, root: Path, anchor: dict) -> list[str]:
    failures: list[str] = []
    rows = con.execute(
        f"""
        SELECT postgrad_school, SUM(n) AS n
        FROM read_parquet(?)
        WHERE CAST(unitid AS VARCHAR) = ?
          AND undergrad_cip4 = ?
          AND postgrad_degree = 'LAW'
        GROUP BY postgrad_school
        ORDER BY n DESC
        """,
        [parquet_glob(root, "postgrad_destinations"), anchor["unitid"], anchor["cip4"]],
    ).fetchall()
    total = sum(float(row[1] or 0) for row in rows)
    top = ", ".join(f"{school or 'Unknown'} ({n:,.0f})" for school, n in rows[:5])
    print(f"{anchor['name']} LAW destinations: total={total:,.0f}; top={top or 'none'}")
    floor = anchor.get("law_destination_min_n")
    if floor is not None and total < floor:
        failures.append(f"{anchor['name']}: LAW destination n={total:.2f} below floor {floor:.2f}")
    present = {row[0] for row in rows if row[0]}
    missing = sorted(set(anchor.get("law_destination_required_schools", [])) - present)
    for school in missing:
        failures.append(f"{anchor['name']}: missing expected LAW destination {school}")
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
    failures.extend(validate_school_coverage(con, root))
    failures.extend(validate_global_mix(con, root))
    failures.extend(validate_flow_summary_consistency(con, root))
    failures.extend(validate_destination_consistency(con, root))
    for anchor in ANCHORS:
        failures.extend(validate_anchor(con, root, anchor))
        failures.extend(validate_destination_anchor(con, root, anchor))
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
