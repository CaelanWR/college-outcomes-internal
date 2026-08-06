#!/usr/bin/env python3
"""Validate an exact-school trial release, with additional Scripps safeguards."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Any

import duckdb


SCRIPPS_UNITID = "123165"
CLAREMONT_UNITIDS = {"123165", "121345", "112260", "115409", "121257"}
SCRIPPS_CONTAMINATION_TERMS = (
    "scripps research",
    "scripps institution of oceanography",
    "scripps oceanography",
    "uc san diego",
    "university of california-san diego",
)


def _platform_root(path: Path) -> Path:
    path = path.expanduser().resolve()
    if path.name == "platform_parquet":
        return path
    candidate = path / "platform_parquet"
    if candidate.exists():
        return candidate
    raise SystemExit(f"Could not find platform_parquet under {path}")


def _files(path: Path) -> list[str]:
    return sorted(
        str(candidate)
        for candidate in path.rglob("*.parquet")
        if candidate.is_file() and not candidate.name.startswith("._")
    )


def _load_selection(path: Path) -> list[dict[str, str]]:
    with path.expanduser().open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows or "unitid" not in rows[0]:
        raise SystemExit(f"Selection file has no unitid rows: {path}")
    selected: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        unitid = str(row.get("unitid") or "").strip().removesuffix(".0")
        if not unitid or unitid in seen:
            continue
        seen.add(unitid)
        selected.append({**row, "unitid": unitid})
    return selected


def _sql_values(values: set[str]) -> str:
    return ",".join("'" + value.replace("'", "''") + "'" for value in sorted(values))


def _query_school_set(
    con: duckdb.DuckDBPyConnection,
    files: list[str],
) -> dict[str, str]:
    rows = con.execute(
        """
        SELECT CAST(unitid AS VARCHAR), ANY_VALUE(CAST(school_name AS VARCHAR))
        FROM read_parquet(?)
        GROUP BY CAST(unitid AS VARCHAR)
        """,
        [files],
    ).fetchall()
    return {str(unitid): str(name or "") for unitid, name in rows}


def validate(platform: Path, selection_path: Path) -> dict[str, Any]:
    selected_rows = _load_selection(selection_path)
    requested = {row["unitid"] for row in selected_rows}
    errors: list[str] = []
    warnings: list[str] = []
    metrics: dict[str, Any] = {
        "requested_school_count": len(requested),
        "requested_unitids": sorted(requested),
    }

    if len(requested) != 40:
        errors.append(f"Expected exactly 40 requested schools, found {len(requested)}")
    if SCRIPPS_UNITID not in requested:
        errors.append(f"Scripps College ({SCRIPPS_UNITID}) is absent from the selection")

    base_files = _files(platform / "base_fact")
    current_files = _files(platform / "current_students_fact")
    if not base_files:
        errors.append("base_fact has no readable Parquet files")
        return {"passed": False, "errors": errors, "warnings": warnings, "metrics": metrics}

    con = duckdb.connect(database=":memory:")
    try:
        base_schools = _query_school_set(con, base_files)
        present = set(base_schools)
        missing = sorted(requested - present)
        unexpected = sorted(present - requested)
        metrics["base_school_count"] = len(present)
        metrics["missing_requested_schools"] = missing
        metrics["unexpected_schools"] = unexpected
        if missing:
            errors.append(f"Requested schools missing from base_fact: {', '.join(missing)}")
        if unexpected:
            errors.append(f"Unexpected schools present in base_fact: {', '.join(unexpected)}")

        invalid_cip4 = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM read_parquet(?)
                WHERE cip4 IS NOT NULL
                  AND (
                    NOT regexp_full_match(CAST(cip4 AS VARCHAR), '^[0-9]{2}\\.[0-9]{2}$')
                    OR LEFT(CAST(cip4 AS VARCHAR), 2) = '99'
                  )
                """,
                [base_files],
            ).fetchone()[0]
        )
        metrics["base_invalid_or_unclassified_cip4_rows"] = invalid_cip4
        if invalid_cip4:
            errors.append(f"base_fact contains {invalid_cip4:,} invalid or CIP 99 rows")

        if current_files:
            current_schools = _query_school_set(con, current_files)
            metrics["current_student_school_count"] = len(current_schools)
            current_invalid_cip4 = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM read_parquet(?)
                    WHERE cip4 IS NULL
                       OR NOT regexp_full_match(CAST(cip4 AS VARCHAR), '^[0-9]{2}\\.[0-9]{2}$')
                       OR LEFT(CAST(cip4 AS VARCHAR), 2) = '99'
                    """,
                    [current_files],
                ).fetchone()[0]
            )
            metrics["current_invalid_or_unclassified_cip4_rows"] = current_invalid_cip4
            if current_invalid_cip4:
                errors.append(
                    f"current_students_fact contains {current_invalid_cip4:,} invalid or CIP 99 rows"
                )
        else:
            warnings.append("current_students_fact has no readable Parquet files")

        if SCRIPPS_UNITID in present:
            scripps_names = [
                str(row[0] or "")
                for row in con.execute(
                    """
                    SELECT DISTINCT school_name
                    FROM read_parquet(?)
                    WHERE CAST(unitid AS VARCHAR) = ?
                    ORDER BY school_name
                    """,
                    [base_files, SCRIPPS_UNITID],
                ).fetchall()
            ]
            scripps_stats = con.execute(
                """
                SELECT
                  COUNT(DISTINCT person_key) AS people,
                  COUNT(DISTINCT CASE WHEN degree = 'Bachelors' THEN person_key END) AS bachelor_people,
                  COUNT(DISTINCT CASE
                    WHEN degree = 'Bachelors' AND grad_year BETWEEN 2020 AND 2025
                    THEN person_key END) AS recent_bachelor_people,
                  COUNT(DISTINCT CASE WHEN degree = 'Bachelors' THEN cip4 END) AS bachelor_cip4s,
                  MIN(grad_year) AS min_grad_year,
                  MAX(grad_year) AS max_grad_year
                FROM read_parquet(?)
                WHERE CAST(unitid AS VARCHAR) = ?
                """,
                [base_files, SCRIPPS_UNITID],
            ).fetchone()
            scripps = {
                "school_names": scripps_names,
                "people": int(scripps_stats[0] or 0),
                "bachelor_people": int(scripps_stats[1] or 0),
                "recent_bachelor_people": int(scripps_stats[2] or 0),
                "bachelor_cip4s": int(scripps_stats[3] or 0),
                "min_grad_year": int(scripps_stats[4]) if scripps_stats[4] is not None else None,
                "max_grad_year": int(scripps_stats[5]) if scripps_stats[5] is not None else None,
            }
            if current_files:
                scripps["current_students"] = int(
                    con.execute(
                        """
                        SELECT COUNT(DISTINCT person_key)
                        FROM read_parquet(?)
                        WHERE CAST(unitid AS VARCHAR) = ?
                        """,
                        [current_files, SCRIPPS_UNITID],
                    ).fetchone()[0]
                    or 0
                )
            metrics["scripps"] = scripps

            contaminated_names = [
                name
                for name in scripps_names
                if any(term in name.lower() for term in SCRIPPS_CONTAMINATION_TERMS)
            ]
            if contaminated_names:
                errors.append(
                    "Scripps unitid contains contaminating school names: "
                    + ", ".join(contaminated_names)
                )
            if not any(name.strip().lower() == "scripps college" for name in scripps_names):
                errors.append(f"Scripps unitid has unexpected school labels: {scripps_names}")
            if scripps["bachelor_people"] < 500:
                errors.append(
                    f"Scripps has only {scripps['bachelor_people']:,} bachelor people; expected at least 500"
                )
            if scripps["recent_bachelor_people"] < 100:
                errors.append(
                    "Scripps recent bachelor sample is unexpectedly thin: "
                    f"{scripps['recent_bachelor_people']:,}"
                )
            if scripps["bachelor_cip4s"] < 10:
                errors.append(
                    f"Scripps has only {scripps['bachelor_cip4s']} bachelor CIP4 majors"
                )
            if current_files and int(scripps.get("current_students", 0)) < 25:
                warnings.append(
                    f"Scripps has only {int(scripps.get('current_students', 0))} current students"
                )

        claremont_values = _sql_values(CLAREMONT_UNITIDS & requested)
        if claremont_values:
            overlap = con.execute(
                f"""
                WITH person_school AS (
                  SELECT DISTINCT person_key, grad_year, degree, CAST(unitid AS VARCHAR) AS unitid
                  FROM read_parquet(?)
                  WHERE CAST(unitid AS VARCHAR) IN ({claremont_values})
                ),
                person_year AS (
                  SELECT person_key, grad_year, degree, COUNT(DISTINCT unitid) AS schools
                  FROM person_school
                  GROUP BY person_key, grad_year, degree
                )
                SELECT
                  COUNT(*) AS person_years,
                  SUM(CASE WHEN schools > 1 THEN 1 ELSE 0 END) AS multi_school_person_years
                FROM person_year
                """,
                [base_files],
            ).fetchone()
            total = int(overlap[0] or 0)
            multi = int(overlap[1] or 0)
            share = 100.0 * multi / total if total else 0.0
            metrics["claremont_same_person_school_overlap"] = {
                "person_degree_years": total,
                "multiple_school_person_degree_years": multi,
                "share_pct": round(share, 3),
            }
            if share > 20:
                errors.append(
                    f"Claremont same-person/same-degree/same-year overlap is {share:.1f}%"
                )
            elif share > 5:
                warnings.append(
                    f"Claremont same-person/same-degree/same-year overlap is {share:.1f}%; review cross-registration"
                )
    finally:
        con.close()

    return {
        "passed": not errors,
        "platform_root": str(platform),
        "selection_file": str(selection_path.expanduser().resolve()),
        "errors": errors,
        "warnings": warnings,
        "metrics": metrics,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("platform_root", type=Path)
    parser.add_argument(
        "--selection",
        type=Path,
        default=Path("config/scripps_competitor_40.csv"),
    )
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args()

    platform = _platform_root(args.platform_root)
    report = validate(platform, args.selection)
    payload = json.dumps(report, indent=2)
    print(payload)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(payload + "\n")
    if report["passed"]:
        print("\nTrial release validation passed.")
        return 0
    print("\nTrial release validation failed.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
