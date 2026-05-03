"""
Build the API-ready Parquet export for the internal outcomes platform.

Run this from the Snowflake precompute notebook after the existing fact tables
have been created. The script expects the notebook globals from
school_outcomes_precompute.work.ipynb to exist: sfClient, OUT_DIR, SCRATCH,
EDUCATION_CIP, POSITION_TABLE, helper SQL functions, and school_meta.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


PLATFORM_EXPORT_VERSION = "2026-05-03-platform-v1"
PLATFORM_SUPPRESSION_THRESHOLD = 25


def _normalize_platform_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Snowflake/Pandas dtypes so pyarrow writes predictably."""
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]

    for col in df.columns:
        if df[col].dtype == "object":
            non_null = df[col].dropna()
            if len(non_null) and non_null.map(lambda v: isinstance(v, Decimal)).any():
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif len(non_null) and non_null.map(lambda v: isinstance(v, bytes)).any():
                df[col] = df[col].map(lambda v: v.decode("utf-8", errors="ignore") if isinstance(v, bytes) else v)

    for col in ["unitid", "degree", "horizon"]:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown").astype(str)
    for col in ["grad_year", "horizon_years", "partial_horizon_flag", "no_further_education_flag"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def _clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _write_df_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_platform_df(df)
    table = pa.Table.from_pandas(normalized, preserve_index=False)
    pq.write_table(table, path, compression="snappy")


def _write_query_dataset(sf_client, query: str, root: Path, partition_cols: list[str]) -> dict:
    """Execute a Snowflake query and stream batches into a partitioned dataset."""
    _clean_dir(root)
    conn = sf_client.connect()
    cur = conn.cursor()
    row_count = 0
    batch_count = 0
    try:
        cur.execute(query)
        for batch in cur.fetch_pandas_batches():
            df = _normalize_platform_df(batch)
            if df.empty:
                continue
            batch_count += 1
            row_count += len(df)
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_to_dataset(
                table,
                root_path=str(root),
                partition_cols=partition_cols,
                compression="snappy",
                basename_template=f"part-{batch_count:05d}-{{i}}.parquet",
            )
    finally:
        cur.close()
        conn.close()
    return {"rows": row_count, "batches": batch_count, "partition_cols": partition_cols}


def _copy_aggregate_facts(out_dir: Path, aggregate_dir: Path) -> dict:
    """Copy existing current-demo facts into the platform bundle as Parquet."""
    _clean_dir(aggregate_dir)
    fact_files = {
        "overview": "school_overview_fact.parquet",
        "earnings": "school_earnings_fact.parquet",
        "earnings_curve": "school_earnings_curve_fact.parquet",
        "employers": "school_employer_fact.parquet",
        "employer_roles": "school_employer_role_fact.parquet",
        "geography": "school_geo_fact.parquet",
        "roles": "school_role_fact.parquet",
        "majors": "school_major_fact.parquet",
        "major_mix": "school_major_mix_fact.parquet",
        "postgrad": "school_postgrad_fact.parquet",
        "postgrad_flows": "school_postgrad_flow_fact.parquet",
        "postgrad_destinations": "school_postgrad_destination_fact.parquet",
        "quality": "school_quality_fact.parquet",
        "missing_grad": "school_missing_grad_fact.parquet",
        "demographics": "school_demographics_fact.parquet",
    }

    written = {}
    for fact_name, filename in fact_files.items():
        source = out_dir / filename
        if not source.exists():
            continue
        df = pd.read_parquet(source)
        target = aggregate_dir / filename
        _write_df_parquet(df, target)
        written[fact_name] = {"path": str(target.relative_to(aggregate_dir.parent)), "rows": int(len(df))}
    return written


def _platform_base_sql() -> str:
    demographics_table = globals().get(
        "DEMOGRAPHICS_TABLE",
        "CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_202303_INDIVIDUAL_USER",
    )

    return f"""
CREATE OR REPLACE TABLE {SCRATCH}.SCHOOL_OUTCOMES_PLATFORM_BASE AS
WITH cpi_u AS (
    SELECT column1 AS cpi_year, column2 AS cpi_u_avg
    FROM VALUES
        (2005, 195.300), (2006, 201.600), (2007, 207.342), (2008, 215.303),
        (2009, 214.537), (2010, 218.056), (2011, 224.939), (2012, 229.594),
        (2013, 232.957), (2014, 236.736), (2015, 237.017), (2016, 240.007),
        (2017, 245.120), (2018, 251.107), (2019, 255.657), (2020, 258.811),
        (2021, 270.970), (2022, 292.655), (2023, 304.702), (2024, 313.689),
        (2025, 321.943)
),
standard_outcomes AS (
    SELECT
        b.user_id,
        b.unitid,
        b.ipeds_name AS school_name,
        b.degree,
        b.cip2,
        b.cip4,
        b.cip6,
        b.cip_title AS major_title,
        b.cohort_year AS grad_year,
        b.cohort_band,
        TO_VARCHAR(b.horizon) || 'yr' AS horizon,
        b.horizon AS horizon_years,
        0 AS partial_horizon_flag,
        b.grad_date,
        b.target_date,
        b.salary_nominal,
        b.salary,
        b.company_name,
        b.ultimate_parent_company_name,
        b.metro_area,
        b.msa,
        b.city,
        b.state,
        b.country,
        b.role_k10_v3,
        b.role_k50_v3,
        b.role_k150_v3,
        b.role_k500_v3,
        b.rics_k50,
        b.rics_k200,
        b.rics_k400,
        b.naics_code,
        b.naics_description,
        b.seniority,
        b.title_raw,
        b.position_weight,
        b.education_weight,
        b.analysis_weight,
        b.ipeds_calibration_source,
        b.calibration_observed_completions,
        b.calibration_ipeds_completions
    FROM {SCRATCH}.SCHOOL_OUTCOMES_BASE b
),
early_2025_outcomes AS (
    SELECT *
    FROM (
        SELECT
            g.user_id,
            g.unitid,
            g.ipeds_name AS school_name,
            g.degree,
            g.cip2,
            g.cip4,
            g.cip6,
            g.cip_title AS major_title,
            g.cohort_year AS grad_year,
            g.cohort_band,
            'early_2025' AS horizon,
            CAST(NULL AS INTEGER) AS horizon_years,
            1 AS partial_horizon_flag,
            g.grad_date,
            CURRENT_DATE() AS target_date,
            p.salary AS salary_nominal,
            CASE
                WHEN p.salary IS NOT NULL AND cpi.cpi_u_avg IS NOT NULL
                THEN ROUND(p.salary * {SALARY_REAL_BASE_CPI} / cpi.cpi_u_avg, 0)
                ELSE NULL
            END AS salary,
            p.company_name,
            p.ultimate_parent_company_name,
            p.metro_area,
            p.msa,
            p.city,
            p.state,
            p.country,
            p.role_k10_v3,
            p.role_k50_v3,
            p.role_k150_v3,
            p.role_k500_v3,
            p.rics_k50,
            p.rics_k200,
            p.rics_k400,
            p.naics_code,
            p.naics_description,
            p.seniority,
            p.title_raw,
            GREATEST(0.0, {position_weight_sql('p')}) AS position_weight,
            COALESCE(g.ipeds_calibration_weight, 1.0) AS education_weight,
            GREATEST(0.0, {position_weight_sql('p')}) * COALESCE(g.ipeds_calibration_weight, 1.0) AS analysis_weight,
            g.ipeds_calibration_source,
            g.calibration_observed_completions,
            g.calibration_ipeds_completions,
            ROW_NUMBER() OVER (
                PARTITION BY g.user_id, g.unitid, g.degree, g.cohort_year
                ORDER BY
                    p.is_primary DESC NULLS LAST,
                    p.salary DESC NULLS LAST,
                    p.startdate DESC
            ) AS pos_rank
        FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED g
        JOIN {POSITION_TABLE} p
          ON g.user_id = p.user_id
         AND p.startdate <= CURRENT_DATE()
         AND (p.enddate >= CURRENT_DATE() OR p.enddate IS NULL)
        LEFT JOIN cpi_u cpi
          ON LEAST(YEAR(CURRENT_DATE()), 2025) = cpi.cpi_year
        WHERE g.cohort_year = 2025
    )
    WHERE pos_rank = 1
),
outcomes AS (
    SELECT * FROM standard_outcomes
    UNION ALL
    SELECT * EXCLUDE (pos_rank) FROM early_2025_outcomes
),
demo AS (
    SELECT
        user_id,
        COALESCE(NULLIF(sex_predicted, ''), 'Unknown') AS gender,
        COALESCE(NULLIF(ethnicity_predicted, ''), 'Unknown') AS race_ethnicity,
        prestige
    FROM {demographics_table}
),
later_edu AS (
    SELECT
        o.user_id,
        o.unitid,
        o.degree,
        o.grad_year,
        o.cip4,
        {postgrad_degree_label_sql('e2')} AS later_degree_type,
        e2.ipeds_name AS later_school,
        {assigned_cip4_sql('e2')} AS later_cip4,
        {assigned_cip_title_sql('e2')} AS later_program,
        YEAR(e2.enddate) AS later_grad_year,
        DATEDIFF('day', o.grad_date, e2.enddate) / 365.25 AS years_to_later_degree,
        ROW_NUMBER() OVER (
            PARTITION BY o.user_id, o.unitid, o.degree, o.grad_year, o.cip4
            ORDER BY e2.enddate ASC
        ) AS later_rank
    FROM (
        SELECT DISTINCT user_id, unitid, degree, grad_year, cip4, grad_date
        FROM outcomes
    ) o
    JOIN {EDUCATION_CIP} e2
      ON o.user_id = e2.user_id
     AND (e2.degree IN ('Master', 'MBA') OR e2.degree LIKE 'Doctor%')
     AND e2.enddate > o.grad_date
),
enriched AS (
    SELECT
        SHA2(TO_VARCHAR(o.user_id), 256) AS person_key,
        CAST(o.unitid AS VARCHAR) AS unitid,
        o.school_name,
        o.degree,
        o.cip2,
        o.cip4,
        o.cip6,
        o.major_title,
        o.grad_year,
        o.cohort_band,
        o.horizon,
        o.horizon_years,
        o.partial_horizon_flag,
        o.grad_date,
        o.target_date,
        COALESCE(d.gender, 'Unknown') AS gender,
        COALESCE(d.race_ethnicity, 'Unknown') AS race_ethnicity,
        d.prestige,
        CASE
            WHEN LOWER(COALESCE(o.ultimate_parent_company_name, '')) = 'government of the united states of america'
                 AND NULLIF(o.company_name, '') IS NOT NULL
            THEN o.company_name
            ELSE COALESCE(NULLIF(o.ultimate_parent_company_name, ''), NULLIF(o.company_name, ''), 'Unknown')
        END AS employer,
        CASE WHEN COALESCE(NULLIF(o.ultimate_parent_company_name, ''), NULLIF(o.company_name, '')) IS NULL THEN 1 ELSE 0 END AS unknown_employer_flag,
        CASE WHEN COALESCE(NULLIF(o.ultimate_parent_company_name, ''), NULLIF(o.company_name, '')) IS NULL THEN 0 ELSE 1 END AS named_employer_flag,
        LOWER(REGEXP_REPLACE(o.school_name, '[^a-z0-9]', '')) AS school_norm,
        LOWER(REGEXP_REPLACE(
            CASE
                WHEN LOWER(COALESCE(o.ultimate_parent_company_name, '')) = 'government of the united states of america'
                     AND NULLIF(o.company_name, '') IS NOT NULL
                THEN o.company_name
                ELSE COALESCE(NULLIF(o.ultimate_parent_company_name, ''), NULLIF(o.company_name, ''), 'Unknown')
            END,
            '[^a-z0-9]',
            ''
        )) AS employer_norm,
        COALESCE(NULLIF(o.metro_area, ''), NULLIF(o.state, ''), NULLIF(o.country, ''), 'Unknown') AS location,
        o.metro_area,
        o.city,
        o.state,
        o.country,
        o.role_k10_v3,
        o.role_k50_v3,
        o.role_k150_v3,
        o.role_k500_v3,
        o.rics_k50 AS industry_k50,
        o.rics_k200 AS industry_k200,
        o.rics_k400 AS industry_k400,
        o.naics_code,
        o.naics_description,
        o.seniority,
        o.title_raw,
        o.salary_nominal,
        o.salary,
        o.position_weight,
        o.education_weight AS ipeds_calibration_weight,
        o.analysis_weight AS final_weight,
        o.ipeds_calibration_source,
        o.calibration_observed_completions,
        o.calibration_ipeds_completions,
        le.later_degree_type,
        le.later_school,
        le.later_cip4,
        le.later_program,
        le.later_grad_year,
        le.years_to_later_degree,
        CASE WHEN le.user_id IS NULL THEN 1 ELSE 0 END AS no_further_education_flag
    FROM outcomes o
    LEFT JOIN demo d
      ON o.user_id = d.user_id
    LEFT JOIN later_edu le
      ON o.user_id = le.user_id
     AND o.unitid = le.unitid
     AND o.degree = le.degree
     AND o.grad_year = le.grad_year
     AND COALESCE(o.cip4, '') = COALESCE(le.cip4, '')
     AND le.later_rank = 1
)
SELECT
    *,
    CASE
        WHEN named_employer_flag = 1
         AND employer_norm <> ''
         AND (
            employer_norm = school_norm
            OR employer_norm LIKE '%' || school_norm || '%'
            OR school_norm LIKE '%' || employer_norm || '%'
            OR (unitid = '190150' AND employer_norm = 'thetrusteesofcolumbiauniversityinthecityofnewyork')
         )
        THEN 1 ELSE 0
    END AS same_school_employer_flag,
    CASE
        WHEN named_employer_flag = 1
         AND NOT (
            employer_norm = school_norm
            OR employer_norm LIKE '%' || school_norm || '%'
            OR school_norm LIKE '%' || employer_norm || '%'
            OR (unitid = '190150' AND employer_norm = 'thetrusteesofcolumbiauniversityinthecityofnewyork')
         )
        THEN 1 ELSE 0
    END AS career_employer_flag
FROM enriched
"""


def _current_students_sql() -> str:
    demographics_table = globals().get(
        "DEMOGRAPHICS_TABLE",
        "CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_202303_INDIVIDUAL_USER",
    )
    education_columns = globals().get("EDUCATION_COLUMNS", set())
    has_startdate = "startdate" in {str(c).lower() for c in education_columns}
    projected_year_expr = f"""
        CASE
            WHEN e.enddate IS NOT NULL AND YEAR(e.enddate) BETWEEN 2026 AND 2029 THEN YEAR(e.enddate)
            {"WHEN e.enddate IS NULL AND e.startdate IS NOT NULL AND YEAR(e.startdate) BETWEEN 2022 AND 2025 THEN YEAR(e.startdate) + 4" if has_startdate else ""}
        END
    """
    projected_filter = """
        e.enddate IS NOT NULL AND YEAR(e.enddate) BETWEEN 2026 AND 2029
    """
    if has_startdate:
        projected_filter = f"""
        ({projected_filter})
        OR (e.enddate IS NULL AND e.startdate IS NOT NULL AND YEAR(e.startdate) BETWEEN 2022 AND 2025)
        """

    return f"""
SELECT *
FROM (
    SELECT
        SHA2(TO_VARCHAR(e.user_id), 256) AS person_key,
        CAST(e.unitid AS VARCHAR) AS unitid,
        e.ipeds_name AS school_name,
        'Bachelors' AS degree,
        LEFT({assigned_cip4_sql('e')}, 2) AS cip2,
        {assigned_cip4_sql('e')} AS cip4,
        {candidate_cip6_sql('e')} AS cip6,
        COALESCE(c4.title, {assigned_cip_title_sql('e')}, '') AS major_title,
        {projected_year_expr} AS grad_year,
        '2026-2029' AS cohort_band,
        1 AS current_student_flag,
        COALESCE(NULLIF(d.sex_predicted, ''), 'Unknown') AS gender,
        COALESCE(NULLIF(d.ethnicity_predicted, ''), 'Unknown') AS race_ethnicity,
        d.prestige,
        1.0 AS final_weight,
        e.cip_probability,
        CASE WHEN e.cip_probability >= 0.8 THEN 1 ELSE 0 END AS high_conf_major_flag
    FROM {EDUCATION_CIP} e
    LEFT JOIN {SCRATCH}.CIP4_TITLES c4
      ON {assigned_cip4_sql('e')} = c4.code
    LEFT JOIN {demographics_table} d
      ON e.user_id = d.user_id
    WHERE e.unitid IN ({UNITID_SQL})
      AND e.degree = 'Bachelor'
      AND {assigned_cip4_sql('e')} IS NOT NULL
      AND ({projected_filter})
)
WHERE grad_year BETWEEN 2026 AND 2029
"""


def run_platform_parquet_export(platform_out_dir: Path | None = None) -> dict:
    """Create the platform Parquet bundle and return its manifest."""
    out_dir = Path(OUT_DIR)
    platform_dir = Path(platform_out_dir) if platform_out_dir else out_dir / "platform_parquet"
    platform_dir.mkdir(parents=True, exist_ok=True)

    print(f"Building platform export: {platform_dir}")
    print("Creating enriched platform base table in Snowflake...")
    conn = sfClient.connect()
    cur = conn.cursor()
    try:
        cur.execute(_platform_base_sql())
    finally:
        cur.close()
        conn.close()

    base_info = _write_query_dataset(
        sfClient,
        f"SELECT * FROM {SCRATCH}.SCHOOL_OUTCOMES_PLATFORM_BASE",
        platform_dir / "base_fact",
        ["unitid", "degree", "grad_year", "horizon"],
    )
    print(f"  base_fact: {base_info['rows']:,} rows")

    current_students_info = _write_query_dataset(
        sfClient,
        _current_students_sql(),
        platform_dir / "current_students_fact",
        ["unitid", "grad_year"],
    )
    print(f"  current_students_fact: {current_students_info['rows']:,} rows")

    aggregate_facts = _copy_aggregate_facts(out_dir, platform_dir / "aggregate_facts")
    print(f"  aggregate facts: {len(aggregate_facts):,}")

    reference_facts = {}
    for filename in ["cip2_titles.parquet", "cip4_titles.parquet", "cip6_titles.parquet"]:
        source = out_dir / filename
        if source.exists():
            target = platform_dir / "references" / filename
            _write_df_parquet(pd.read_parquet(source), target)
            reference_facts[filename.replace(".parquet", "")] = str(target.relative_to(platform_dir))

    manifest = {
        "version": PLATFORM_EXPORT_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "suppression_threshold": PLATFORM_SUPPRESSION_THRESHOLD,
        "source_out_dir": str(out_dir),
        "base_fact": {
            "path": "base_fact",
            **base_info,
            "notes": "Person-level API input with hashed person_key, final_weight, demographics, employer flags, postgrad filters, and early_2025 partial horizon.",
        },
        "current_students_fact": {
            "path": "current_students_fact",
            **current_students_info,
            "notes": "Projected classes 2026-2029 for major growth only; no earnings fields.",
        },
        "aggregate_facts": aggregate_facts,
        "references": reference_facts,
        "primary_filters": [
            "unitid",
            "degree",
            "cip2",
            "cip4",
            "cip6",
            "grad_year",
            "cohort_band",
            "horizon",
            "gender",
            "race_ethnicity",
            "later_degree_type",
            "no_further_education_flag",
            "career_employer_flag",
            "location",
            "role_k50_v3",
            "role_k150_v3",
            "industry_k50",
        ],
        "partial_horizons": ["early_2025"],
        "privacy": {
            "person_key": "SHA2 hash of source user_id; raw user_id is not exported.",
            "browser_rule": "Do not expose base_fact directly from public GitHub Pages. Serve aggregate responses through an authenticated API.",
        },
    }

    manifest_path = platform_dir / "platform_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote {manifest_path}")
    return manifest
