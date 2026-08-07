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


PLATFORM_EXPORT_VERSION = "2026-08-07-postgrad-destination-recovery-v2"
PLATFORM_SUPPRESSION_THRESHOLD = 25
PLATFORM_ROWS_PER_PART = 5000


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
    for col in [
        "grad_year",
        "horizon_years",
        "partial_horizon_flag",
        "no_further_education_flag",
        "plus_one_masters_flag",
        "grad_plus_one_masters_flag",
        "feeder_grad_year",
        "missing_grad_start_flag",
        "years_after_degree",
        "pre_year_bucket",
    ]:
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


def _write_df_parquet_parts(df: pd.DataFrame, root: Path, rows_per_part: int = PLATFORM_ROWS_PER_PART) -> dict:
    _clean_dir(root)
    row_count = 0
    part_count = 0
    for start in range(0, len(df), rows_per_part):
        chunk = df.iloc[start:start + rows_per_part]
        if chunk.empty:
            continue
        part_count += 1
        row_count += len(chunk)
        _write_df_parquet(chunk, root / f"part-{part_count:05d}.parquet")
    return {"rows": int(row_count), "parts": int(part_count), "rows_per_part": int(rows_per_part)}


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
                max_partitions=8192,
            )
    finally:
        cur.close()
        conn.close()
    return {"rows": row_count, "batches": batch_count, "partition_cols": partition_cols}


def _write_query_file_parts(sf_client, query: str, root: Path) -> dict:
    """Execute a Snowflake query and write one flat Parquet file per batch.

    This is intentionally not hive-partitioned. Some download tools handle a
    flat folder of part files more reliably than deeply nested partition trees.
    DuckDB can still query the folder with a recursive glob.
    """
    _clean_dir(root)
    conn = sf_client.connect()
    cur = conn.cursor()
    row_count = 0
    batch_count = 0
    try:
        cur.execute(query)
        part_count = 0
        for batch in cur.fetch_pandas_batches():
            df = _normalize_platform_df(batch)
            if df.empty:
                continue
            batch_count += 1
            row_count += len(df)
            for start in range(0, len(df), PLATFORM_ROWS_PER_PART):
                chunk = df.iloc[start:start + PLATFORM_ROWS_PER_PART]
                if chunk.empty:
                    continue
                part_count += 1
                target = root / f"part-{part_count:05d}.parquet"
                _write_df_parquet(chunk, target)
    finally:
        cur.close()
        conn.close()
    return {"rows": row_count, "batches": batch_count, "parts": part_count, "rows_per_part": PLATFORM_ROWS_PER_PART, "partition_cols": []}


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
        target = aggregate_dir / fact_name
        info = _write_df_parquet_parts(df, target)
        written[fact_name] = {"path": str(target.relative_to(aggregate_dir.parent)), **info}
    return written


def education_later_date_sql(alias: str) -> str:
    """Observed later-education date, allowing in-progress programs with no end date."""
    education_columns = globals().get("EDUCATION_COLUMNS", set())
    education_column_names = {str(c).lower() for c in education_columns}
    if "startdate" in education_column_names:
        return f"COALESCE({alias}.enddate, {alias}.startdate)"
    return f"{alias}.enddate"


def education_degree_text_sql(alias: str) -> str:
    return f"UPPER(TRIM(COALESCE({alias}.degree, '')))"


def _education_column_names() -> set[str]:
    return {str(column).lower() for column in globals().get("EDUCATION_COLUMNS", set())}


def _optional_education_column_sql(alias: str, column: str) -> str:
    if column.lower() in _education_column_names():
        return f"{alias}.{column}"
    return "CAST(NULL AS VARCHAR)"


def _clean_education_text_sql(expression: str) -> str:
    return f"""
    CASE
        WHEN {expression} IS NULL THEN NULL
        WHEN LOWER(TRIM(CAST({expression} AS VARCHAR))) IN (
            '', 'empty', 'unknown', 'other', 'n/a', 'na', 'none', 'null',
            'not available', 'unavailable', 'undisclosed'
        ) THEN NULL
        ELSE TRIM(CAST({expression} AS VARCHAR))
    END
    """.strip()


def education_raw_degree_text_sql(alias: str) -> str:
    raw_degree = _optional_education_column_sql(alias, "degree_raw")
    return f"UPPER(TRIM(COALESCE({_clean_education_text_sql(raw_degree)}, {alias}.degree, '')))"


def postgrad_program_title_sql(alias: str) -> str:
    """Best available observed program name when a CIP match is unavailable."""
    terms = [_clean_education_text_sql(assigned_cip_title_sql(alias))]
    for column in ("field_raw", "field"):
        if column in _education_column_names():
            terms.append(_clean_education_text_sql(f"{alias}.{column}"))
    return f"COALESCE({', '.join(terms)})"


def postgrad_school_name_sql(alias: str) -> str:
    """Best available destination name without manufacturing an Unknown school."""
    terms = [_clean_education_text_sql(f"{alias}.ipeds_name")]
    for column in ("ultimate_parent_school_name", "university_name"):
        if column in _education_column_names():
            terms.append(_clean_education_text_sql(f"{alias}.{column}"))
    return f"COALESCE({', '.join(terms)})"


def postgrad_degree_filter_sql(alias: str = "e") -> str:
    """Later-degree filter that accepts normalized and raw graduate labels.

    The source education table has used both raw labels (Master/Doctor) and
    normalized labels (Masters/Research Doctorate/Professional Doctorate). A
    narrow raw-label filter silently drops law/medical/research doctorates.
    """
    alias = alias.strip()
    degree_text = education_degree_text_sql(alias)
    raw_degree_text = education_raw_degree_text_sql(alias)
    degree_cip = degree_cip_sql(alias)
    return f"""
    (
        {degree_text} IN (
            'MASTER',
            'MASTERS',
            'MASTER''S',
            'MBA',
            'DOCTOR',
            'DOCTORATE',
            'PHD',
            'PH.D.',
            'RESEARCH DOCTORATE',
            'PROFESSIONAL DOCTORATE',
            'OTHER DOCTORATE',
            'LAW',
            'JD',
            'J.D.',
            'JURIS DOCTOR',
            'MD',
            'M.D.'
        )
        OR {degree_text} LIKE 'DOCTOR%'
        OR REGEXP_LIKE({raw_degree_text}, '.*(PHD|PH\\.D\\.|DPHIL|DOCTOR OF PHILOSOPHY|JURIS DOCTOR|JD|J\\.D\\.|LLB|LL\\.B\\.|DOCTOR OF MEDICINE|MBBS|MBCHB|PROFESSIONAL DOCTORATE).*')
        OR LEFT(COALESCE({degree_cip}, ''), 2) = '22'
        OR LEFT(COALESCE({degree_cip}, ''), 5) = '51.12'
    )
    """.strip()


def postgrad_degree_label_sql(alias: str = "e") -> str:
    """Postgrad degree labels for platform export, with professional CIPs first."""
    alias = alias.strip()
    doctor_cip = degree_cip_sql(alias)
    degree_text = education_degree_text_sql(alias)
    raw_degree_text = education_raw_degree_text_sql(alias)
    return f"""
    CASE
        WHEN LEFT(COALESCE({doctor_cip}, ''), 2) = '22'
          OR {degree_text} IN ('LAW', 'JD', 'J.D.', 'JURIS DOCTOR')
          OR REGEXP_LIKE({raw_degree_text}, '.*(JURIS DOCTOR|(^|[^A-Z])JD([^A-Z]|$)|J\\.D\\.|LLB|LL\\.B\\.).*') THEN 'LAW'
        WHEN LEFT(COALESCE({doctor_cip}, ''), 5) = '51.12'
          OR {degree_text} IN ('MD', 'M.D.')
          OR REGEXP_LIKE({raw_degree_text}, '.*(DOCTOR OF MEDICINE|DOCTOR OF OSTEOPATHIC MEDICINE|(^|[^A-Z])MD([^A-Z]|$)|M\\.D\\.|MBBS|MBCHB).*') THEN 'MD'
        WHEN {degree_text} IN ('MASTER', 'MASTERS', 'MASTER''S') THEN 'Masters'
        WHEN {degree_text} = 'MBA' THEN 'MBA'
        WHEN {degree_text} IN ('PHD', 'PH.D.', 'RESEARCH DOCTORATE')
          OR REGEXP_LIKE({raw_degree_text}, '.*(PHD|PH\\.D\\.|DPHIL|DOCTOR OF PHILOSOPHY).*') THEN 'PhD'
        WHEN {degree_text} = 'PROFESSIONAL DOCTORATE'
          OR REGEXP_LIKE({raw_degree_text}, '.*(EDD|ED\\.D\\.|PSYD|PSY\\.D\\.|PHARMD|DPT|DNP|DDS|DMD|DVM|DBA|PROFESSIONAL DOCTORATE).*') THEN 'Professional Doctorate'
        WHEN {degree_text} = 'OTHER DOCTORATE' THEN 'Other Doctorate'
        WHEN {degree_text} IN ('DOCTOR', 'DOCTORATE') AND {alias}.ipeds_awlevel IN ('17') THEN 'PhD'
        WHEN {degree_text} IN ('DOCTOR', 'DOCTORATE') AND {alias}.ipeds_awlevel IN ('18', '09', '9', '10', '11') THEN 'Professional Doctorate'
        WHEN {degree_text} IN ('DOCTOR', 'DOCTORATE') AND {alias}.ipeds_awlevel IN ('19') THEN 'Other Doctorate'
        WHEN {degree_text} IN ('DOCTOR', 'DOCTORATE') THEN 'Doctorate'
        WHEN {degree_text} LIKE 'DOCTOR%' THEN 'Doctorate'
        ELSE {alias}.degree
    END
    """.strip()


def plus_one_masters_flag_sql(later_alias: str, origin_alias: str) -> str:
    """Bachelor-to-master continuation at the same school within one academic year."""
    later_date = education_later_date_sql(later_alias)
    return f"""
        CASE
            WHEN {origin_alias}.degree = 'Bachelors'
             AND {education_degree_text_sql(later_alias)} IN ('MASTER', 'MASTERS', 'MASTER''S')
             AND CAST({later_alias}.unitid AS VARCHAR) = CAST({origin_alias}.unitid AS VARCHAR)
             AND DATEDIFF('day', {origin_alias}.grad_date, {later_date}) BETWEEN 1 AND 366
            THEN 1
            ELSE 0
        END
    """


def postgrad_degree_with_plus_one_sql(later_alias: str, origin_alias: str) -> str:
    return f"""
        CASE
            WHEN {plus_one_masters_flag_sql(later_alias, origin_alias)} = 1 THEN 'Plus-One Masters'
            ELSE {postgrad_degree_label_sql(later_alias)}
        END
    """


def postgrad_education_source_sql() -> str:
    """Education table used to find later degrees.

    Bachelor rows are intentionally limited to selected schools, but later-degree
    matching must come from the full education universe. Server notebooks can set
    POSTGRAD_EDUCATION_CIP when the selected-school education table is narrowed.
    """
    return globals().get("POSTGRAD_EDUCATION_CIP") or globals().get("LATER_EDUCATION_CIP") or EDUCATION_CIP


def _postgrad_raw_bachelors_cte_sql() -> str:
    education_columns = globals().get("EDUCATION_COLUMNS", set())
    education_column_names = {str(c).lower() for c in education_columns}
    weight_col = next(
        (
            col
            for col in [
                "education_weight",
                "profile_weight",
                "individual_weight",
                "representation_weight",
                "universe_weight",
                "final_weight",
            ]
            if col in education_column_names
        ),
        None,
    )
    weight_expr = f"GREATEST(0.0, COALESCE(e.{weight_col}, 1.0))" if weight_col else "1.0"
    return f"""
raw_bachelors AS (
    SELECT DISTINCT
        e.user_id,
        CAST(e.unitid AS VARCHAR) AS unitid,
        e.ipeds_name,
        YEAR(e.enddate) AS cohort_year,
        CASE
            WHEN YEAR(e.enddate) BETWEEN 2005 AND 2009 THEN '2005-2009'
            WHEN YEAR(e.enddate) BETWEEN 2010 AND 2014 THEN '2010-2014'
            WHEN YEAR(e.enddate) BETWEEN 2015 AND 2019 THEN '2015-2019'
            WHEN YEAR(e.enddate) BETWEEN 2020 AND 2025 THEN '2020-2025'
        END AS cohort_band,
        LEFT({assigned_cip4_sql('e')}, 2) AS undergrad_cip2,
        {assigned_cip4_sql('e')} AS undergrad_cip4,
        {assigned_cip4_sql('e')} AS undergrad_cip_code,
        {assigned_cip_title_sql('e')} AS undergrad_cip_title,
        e.enddate AS grad_date,
        CAST({weight_expr} AS DOUBLE) AS education_weight
    FROM {EDUCATION_CIP} e
    WHERE e.unitid IN ({UNITID_SQL})
      AND e.degree = 'Bachelor'
      AND e.enddate IS NOT NULL
      AND YEAR(e.enddate) BETWEEN 2005 AND 2025
      AND {assigned_cip4_sql('e')} IS NOT NULL
)
"""


def _postgrad_later_education_cte_sql(base_cte: str = "raw_bachelors") -> str:
    later_date = education_later_date_sql("e2")
    later_source = postgrad_education_source_sql()
    return f"""
later_candidates AS (
    SELECT
        b.*,
        {postgrad_degree_label_sql('e2')} AS postgrad_degree,
        {postgrad_school_name_sql('e2')} AS postgrad_school,
        LEFT({assigned_cip4_sql('e2')}, 2) AS postgrad_cip2,
        {assigned_cip4_sql('e2')} AS postgrad_cip4,
        {assigned_cip4_sql('e2')} AS postgrad_cip_code,
        {postgrad_program_title_sql('e2')} AS postgrad_cip_title,
        YEAR({later_date}) AS postgrad_year,
        DATEDIFF('day', b.grad_date, {later_date}) / 365.25 AS years_to_postgrad
    FROM {base_cte} b
    JOIN {later_source} e2
      ON b.user_id = e2.user_id
     AND {postgrad_degree_filter_sql('e2')}
     AND {later_date} > b.grad_date
),
later_edu AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (
            PARTITION BY c.user_id, c.unitid, c.cohort_year, c.undergrad_cip4
            ORDER BY c.postgrad_year ASC, c.years_to_postgrad ASC, c.postgrad_degree ASC
        ) AS edu_rank,
        ROW_NUMBER() OVER (
            PARTITION BY c.user_id, c.unitid, c.cohort_year, c.undergrad_cip4, c.postgrad_degree
            ORDER BY c.postgrad_year ASC, c.years_to_postgrad ASC, c.postgrad_school ASC
        ) AS degree_rank
    FROM later_candidates c
)
"""


def _postgrad_aggregate_queries() -> dict[str, str]:
    base_cte = _postgrad_raw_bachelors_cte_sql()
    later_cte = _postgrad_later_education_cte_sql()
    common_ctes = f"WITH {base_cte},\n{later_cte}"
    postgrad = f"""
{common_ctes},
later_any AS (
    SELECT DISTINCT user_id, unitid, cohort_year, undergrad_cip4
    FROM later_edu
),
degree_flags AS (
    SELECT
        user_id,
        unitid,
        cohort_year,
        undergrad_cip4,
        MAX(CASE WHEN postgrad_degree = 'Masters' THEN 1 ELSE 0 END) AS has_masters,
        MAX(CASE WHEN postgrad_degree = 'MBA' THEN 1 ELSE 0 END) AS has_mba,
        MAX(CASE WHEN postgrad_degree = 'LAW' THEN 1 ELSE 0 END) AS has_law,
        MAX(CASE WHEN postgrad_degree = 'MD' THEN 1 ELSE 0 END) AS has_md,
        MAX(CASE WHEN postgrad_degree = 'PhD' THEN 1 ELSE 0 END) AS has_phd,
        MAX(CASE WHEN postgrad_degree = 'Professional Doctorate' THEN 1 ELSE 0 END) AS has_professional_doctorate,
        MAX(CASE WHEN postgrad_degree = 'Other Doctorate' THEN 1 ELSE 0 END) AS has_other_doctorate,
        MAX(CASE WHEN postgrad_degree IN ('PhD', 'LAW', 'MD', 'Professional Doctorate', 'Other Doctorate', 'Doctorate') THEN 1 ELSE 0 END) AS has_doctor
    FROM later_edu
    WHERE degree_rank = 1
    GROUP BY user_id, unitid, cohort_year, undergrad_cip4
)
SELECT
    b.unitid,
    MAX(b.ipeds_name) AS ipeds_name,
    b.cohort_year,
    b.cohort_band,
    b.undergrad_cip2,
    b.undergrad_cip4,
    b.undergrad_cip_code,
    MAX(b.undergrad_cip_title) AS undergrad_cip_title,
    ROUND(SUM(b.education_weight), 0) AS total_bachelors,
    COUNT(DISTINCT b.user_id) AS raw_total_bachelors,
    ROUND(SUM(CASE WHEN la.user_id IS NOT NULL THEN b.education_weight ELSE 0 END), 0) AS has_later_degree,
    COUNT(DISTINCT CASE WHEN la.user_id IS NOT NULL THEN b.user_id END) AS raw_has_later_degree,
    ROUND(SUM(CASE WHEN COALESCE(df.has_masters, 0) = 1 THEN b.education_weight ELSE 0 END), 0) AS masters_count,
    ROUND(SUM(CASE WHEN COALESCE(df.has_mba, 0) = 1 THEN b.education_weight ELSE 0 END), 0) AS mba_count,
    ROUND(SUM(CASE WHEN COALESCE(df.has_law, 0) = 1 THEN b.education_weight ELSE 0 END), 0) AS law_count,
    ROUND(SUM(CASE WHEN COALESCE(df.has_md, 0) = 1 THEN b.education_weight ELSE 0 END), 0) AS md_count,
    ROUND(SUM(CASE WHEN COALESCE(df.has_phd, 0) = 1 THEN b.education_weight ELSE 0 END), 0) AS phd_count,
    ROUND(SUM(CASE WHEN COALESCE(df.has_professional_doctorate, 0) = 1 THEN b.education_weight ELSE 0 END), 0) AS professional_doctorate_count,
    ROUND(SUM(CASE WHEN COALESCE(df.has_other_doctorate, 0) = 1 THEN b.education_weight ELSE 0 END), 0) AS other_doctorate_count,
    ROUND(SUM(CASE WHEN COALESCE(df.has_doctor, 0) = 1 THEN b.education_weight ELSE 0 END), 0) AS doctor_count,
    ROUND(100.0 * SUM(CASE WHEN la.user_id IS NOT NULL THEN b.education_weight ELSE 0 END) / NULLIF(SUM(b.education_weight), 0), 1) AS later_degree_pct
FROM raw_bachelors b
LEFT JOIN later_any la
  ON b.user_id = la.user_id
 AND b.unitid = la.unitid
 AND b.cohort_year = la.cohort_year
 AND b.undergrad_cip4 = la.undergrad_cip4
LEFT JOIN degree_flags df
  ON b.user_id = df.user_id
 AND b.unitid = df.unitid
 AND b.cohort_year = df.cohort_year
 AND b.undergrad_cip4 = df.undergrad_cip4
GROUP BY b.unitid, b.cohort_year, b.cohort_band,
         b.undergrad_cip2, b.undergrad_cip4, b.undergrad_cip_code
HAVING total_bachelors >= 1
"""
    flows = f"""
{common_ctes},
degree_flow AS (
    SELECT * FROM later_edu WHERE degree_rank = 1
),
base_totals AS (
    SELECT
        unitid,
        cohort_year,
        undergrad_cip2,
        undergrad_cip4,
        undergrad_cip_code,
        SUM(education_weight) AS weighted_total_bachelors,
        COUNT(DISTINCT user_id) AS raw_total_bachelors
    FROM raw_bachelors
    GROUP BY unitid, cohort_year, undergrad_cip2, undergrad_cip4, undergrad_cip_code
)
SELECT
    f.unitid,
    f.ipeds_name,
    f.cohort_year,
    f.cohort_band,
    f.undergrad_cip2,
    f.undergrad_cip4,
    f.undergrad_cip_code,
    f.undergrad_cip_title,
    f.postgrad_degree,
    ROUND(SUM(f.education_weight), 0) AS n_users,
    COUNT(DISTINCT f.user_id) AS raw_n_users,
    ROUND(SUM(f.years_to_postgrad * f.education_weight) / NULLIF(SUM(f.education_weight), 0), 2) AS avg_years_to_postgrad,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY f.years_to_postgrad), 2) AS median_years_to_postgrad,
    ROUND(t.weighted_total_bachelors, 0) AS total_bachelors,
    t.raw_total_bachelors,
    ROUND(SUM(f.education_weight) / NULLIF(t.weighted_total_bachelors, 0) * 100, 2) AS flow_pct
FROM degree_flow f
JOIN base_totals t
  ON f.unitid = t.unitid
 AND f.cohort_year = t.cohort_year
 AND f.undergrad_cip2 = t.undergrad_cip2
 AND f.undergrad_cip4 = t.undergrad_cip4
 AND f.undergrad_cip_code = t.undergrad_cip_code
GROUP BY f.unitid, f.ipeds_name, f.cohort_year, f.cohort_band,
         f.undergrad_cip2, f.undergrad_cip4, f.undergrad_cip_code, f.undergrad_cip_title,
         f.postgrad_degree, t.weighted_total_bachelors, t.raw_total_bachelors
HAVING n_users >= 1
"""
    destinations = f"""
{common_ctes}
SELECT
    le.unitid,
    le.ipeds_name,
    le.cohort_year,
    le.cohort_band,
    le.undergrad_cip2,
    le.undergrad_cip4,
    le.undergrad_cip_code,
    le.undergrad_cip_title,
    le.postgrad_degree,
    le.postgrad_school,
    le.postgrad_cip2,
    le.postgrad_cip4,
    le.postgrad_cip_code,
    le.postgrad_cip_title,
    ROUND(SUM(le.education_weight), 0) AS n,
    COUNT(DISTINCT le.user_id) AS raw_n,
    ROUND(SUM(le.years_to_postgrad * le.education_weight) / NULLIF(SUM(le.education_weight), 0), 2) AS avg_years_to_postgrad
FROM later_edu le
WHERE le.degree_rank = 1
GROUP BY le.unitid, le.ipeds_name, le.cohort_year, le.cohort_band,
         le.undergrad_cip2, le.undergrad_cip4, le.undergrad_cip_code, le.undergrad_cip_title,
         le.postgrad_degree, le.postgrad_school, le.postgrad_cip2, le.postgrad_cip4, le.postgrad_cip_code, le.postgrad_cip_title
HAVING n >= 1
"""
    return {
        "postgrad": postgrad,
        "postgrad_flows": flows,
        "postgrad_destinations": destinations,
    }


def _write_postgrad_aggregate_facts(aggregate_dir: Path, written: dict) -> None:
    print("Regenerating education-native postgrad aggregate facts...")
    for fact_name, query in _postgrad_aggregate_queries().items():
        info = _write_query_file_parts(sfClient, query, aggregate_dir / fact_name)
        written[fact_name] = {
            "path": str((aggregate_dir / fact_name).relative_to(aggregate_dir.parent)),
            **info,
            "notes": "Education-native further-education fact regenerated during platform export with startdate fallback for in-progress later degrees.",
        }
        print(f"  aggregate_facts/{fact_name}: {info['rows']:,} rows")


def feeder_bachelor_education_cte_sql(base_cte: str) -> str:
    return f"""
feeder_bachelor_edu AS (
    SELECT
        o.user_id,
        o.unitid,
        o.degree,
        o.grad_year,
        o.cip4,
        CAST(e0.unitid AS VARCHAR) AS feeder_unitid,
        e0.ipeds_name AS feeder_school,
        {assigned_cip4_sql('e0')} AS feeder_cip4,
        {assigned_cip_title_sql('e0')} AS feeder_program,
        YEAR(e0.enddate) AS feeder_grad_year,
        DATEDIFF('day', e0.enddate, o.grad_date) / 365.25 AS years_since_feeder_degree,
        ROW_NUMBER() OVER (
            PARTITION BY o.user_id, o.unitid, o.degree, o.grad_year, o.cip4
            ORDER BY e0.enddate DESC
        ) AS feeder_rank
    FROM (
        SELECT DISTINCT user_id, unitid, degree, grad_year, cip4, grad_date
        FROM {base_cte}
        WHERE degree <> 'Bachelors'
    ) o
    JOIN {EDUCATION_CIP} e0
      ON o.user_id = e0.user_id
     AND e0.degree = 'Bachelor'
     AND e0.enddate < o.grad_date
)
"""


def _platform_base_sql() -> str:
    demographics_table = globals().get(
        "DEMOGRAPHICS_TABLE",
        "CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_202303_INDIVIDUAL_USER",
    )

    later_date = education_later_date_sql("e2")
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
recent_school_cip4_calibration AS (
    SELECT *
    FROM (
        SELECT
            CAST(unitid AS VARCHAR) AS unitid,
            degree,
            cip4,
            cohort_year AS calibration_year,
            AVG(ipeds_calibration_weight) AS ipeds_calibration_weight,
            ANY_VALUE(ipeds_calibration_source) AS ipeds_calibration_source,
            AVG(calibration_observed_completions) AS calibration_observed_completions,
            AVG(calibration_ipeds_completions) AS calibration_ipeds_completions,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(unitid AS VARCHAR), degree, cip4
                ORDER BY cohort_year DESC
            ) AS calibration_rank
        FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED
        WHERE cohort_year < 2025
          AND cip4 IS NOT NULL
          AND ipeds_calibration_weight IS NOT NULL
          AND calibration_ipeds_completions IS NOT NULL
          AND COALESCE(ipeds_calibration_source, 'none') <> 'none'
        GROUP BY CAST(unitid AS VARCHAR), degree, cip4, cohort_year
    )
    WHERE calibration_rank = 1
),
recent_global_cip4_calibration AS (
    SELECT *
    FROM (
        SELECT
            degree,
            cip4,
            cohort_year AS calibration_year,
            AVG(ipeds_calibration_weight) AS ipeds_calibration_weight,
            ANY_VALUE(ipeds_calibration_source) AS ipeds_calibration_source,
            AVG(calibration_observed_completions) AS calibration_observed_completions,
            AVG(calibration_ipeds_completions) AS calibration_ipeds_completions,
            ROW_NUMBER() OVER (
                PARTITION BY degree, cip4
                ORDER BY cohort_year DESC
            ) AS calibration_rank
        FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED
        WHERE cohort_year < 2025
          AND cip4 IS NOT NULL
          AND ipeds_calibration_weight IS NOT NULL
          AND calibration_ipeds_completions IS NOT NULL
          AND COALESCE(ipeds_calibration_source, 'none') <> 'none'
        GROUP BY degree, cip4, cohort_year
    )
    WHERE calibration_rank = 1
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
        CASE
            WHEN b.cohort_year >= 2023 AND b.calibration_ipeds_completions IS NULL
            THEN COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, b.education_weight, 1.0)
            ELSE COALESCE(b.education_weight, 1.0)
        END AS education_weight,
        GREATEST(0.0, COALESCE(b.position_weight, 1.0))
          * CASE
                WHEN b.cohort_year >= 2023 AND b.calibration_ipeds_completions IS NULL
                THEN COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, b.education_weight, 1.0)
                ELSE COALESCE(b.education_weight, 1.0)
            END AS analysis_weight,
        CASE
            WHEN b.cohort_year >= 2023 AND b.calibration_ipeds_completions IS NULL AND rsc.ipeds_calibration_weight IS NOT NULL
                THEN 'recent_school_year_cip4'
            WHEN b.cohort_year >= 2023 AND b.calibration_ipeds_completions IS NULL AND rgc.ipeds_calibration_weight IS NOT NULL
                THEN 'recent_global_year_cip4'
            ELSE b.ipeds_calibration_source
        END AS ipeds_calibration_source,
        CASE
            WHEN b.cohort_year >= 2023 AND b.calibration_ipeds_completions IS NULL
            THEN COALESCE(rsc.calibration_observed_completions, rgc.calibration_observed_completions, b.calibration_observed_completions)
            ELSE b.calibration_observed_completions
        END AS calibration_observed_completions,
        CASE
            WHEN b.cohort_year >= 2023 AND b.calibration_ipeds_completions IS NULL
            THEN COALESCE(rsc.calibration_ipeds_completions, rgc.calibration_ipeds_completions, b.calibration_ipeds_completions)
            ELSE b.calibration_ipeds_completions
        END AS calibration_ipeds_completions
    FROM {SCRATCH}.SCHOOL_OUTCOMES_BASE b
    LEFT JOIN recent_school_cip4_calibration rsc
      ON CAST(b.unitid AS VARCHAR) = rsc.unitid
     AND b.degree = rsc.degree
     AND b.cip4 = rsc.cip4
    LEFT JOIN recent_global_cip4_calibration rgc
      ON b.degree = rgc.degree
     AND b.cip4 = rgc.cip4
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
            CASE
                WHEN g.calibration_ipeds_completions IS NULL
                THEN COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, g.ipeds_calibration_weight, 1.0)
                ELSE COALESCE(g.ipeds_calibration_weight, 1.0)
            END AS education_weight,
            GREATEST(0.0, {position_weight_sql('p')})
              * CASE
                    WHEN g.calibration_ipeds_completions IS NULL
                    THEN COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, g.ipeds_calibration_weight, 1.0)
                    ELSE COALESCE(g.ipeds_calibration_weight, 1.0)
                END AS analysis_weight,
            CASE
                WHEN g.calibration_ipeds_completions IS NULL AND rsc.ipeds_calibration_weight IS NOT NULL
                    THEN 'recent_school_year_cip4'
                WHEN g.calibration_ipeds_completions IS NULL AND rgc.ipeds_calibration_weight IS NOT NULL
                    THEN 'recent_global_year_cip4'
                ELSE g.ipeds_calibration_source
            END AS ipeds_calibration_source,
            CASE
                WHEN g.calibration_ipeds_completions IS NULL
                THEN COALESCE(rsc.calibration_observed_completions, rgc.calibration_observed_completions, g.calibration_observed_completions)
                ELSE g.calibration_observed_completions
            END AS calibration_observed_completions,
            CASE
                WHEN g.calibration_ipeds_completions IS NULL
                THEN COALESCE(rsc.calibration_ipeds_completions, rgc.calibration_ipeds_completions, g.calibration_ipeds_completions)
                ELSE g.calibration_ipeds_completions
            END AS calibration_ipeds_completions,
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
        LEFT JOIN recent_school_cip4_calibration rsc
          ON CAST(g.unitid AS VARCHAR) = rsc.unitid
         AND g.degree = rsc.degree
         AND g.cip4 = rsc.cip4
        LEFT JOIN recent_global_cip4_calibration rgc
          ON g.degree = rgc.degree
         AND g.cip4 = rgc.cip4
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
        {postgrad_degree_with_plus_one_sql('e2', 'o')} AS later_degree_type,
        {postgrad_school_name_sql('e2')} AS later_school,
        {assigned_cip4_sql('e2')} AS later_cip4,
        {postgrad_program_title_sql('e2')} AS later_program,
        YEAR({later_date}) AS later_grad_year,
        DATEDIFF('day', o.grad_date, {later_date}) / 365.25 AS years_to_later_degree,
        {plus_one_masters_flag_sql('e2', 'o')} AS plus_one_masters_flag,
        ROW_NUMBER() OVER (
            PARTITION BY o.user_id, o.unitid, o.degree, o.grad_year, o.cip4
            ORDER BY {later_date} ASC
        ) AS later_rank
    FROM (
        SELECT DISTINCT user_id, unitid, degree, grad_year, cip4, grad_date
        FROM outcomes
    ) o
    JOIN {postgrad_education_source_sql()} e2
      ON o.user_id = e2.user_id
     AND {postgrad_degree_filter_sql('e2')}
     AND {later_date} > o.grad_date
),
{feeder_bachelor_education_cte_sql('outcomes')},
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
        COALESCE(le.plus_one_masters_flag, 0) AS plus_one_masters_flag,
        fe.feeder_unitid,
        fe.feeder_school,
        fe.feeder_cip4,
        fe.feeder_program,
        fe.feeder_grad_year,
        fe.years_since_feeder_degree,
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
    LEFT JOIN feeder_bachelor_edu fe
      ON o.user_id = fe.user_id
     AND o.unitid = fe.unitid
     AND o.degree = fe.degree
     AND o.grad_year = fe.grad_year
     AND COALESCE(o.cip4, '') = COALESCE(fe.cip4, '')
     AND fe.feeder_rank = 1
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
    education_column_names = {str(c).lower() for c in education_columns}
    has_startdate = "startdate" in education_column_names
    assigned_cip4_expr = assigned_cip4_sql("e")
    candidate_cip6_expr = candidate_cip6_sql("e")
    assigned_cip_title_expr = assigned_cip_title_sql("e")
    cip_probability_expr = "e.cip_probability" if "cip_probability" in education_column_names else "CAST(NULL AS DOUBLE)"
    cip_probability_rank_expr = (
        "COALESCE(e.cip_probability, 0)"
        if "cip_probability" in education_column_names
        else "0"
    )
    profile_weight_col = next(
        (
            col
            for col in [
                "profile_weight",
                "education_weight",
                "individual_weight",
                "representation_weight",
                "universe_weight",
                "final_weight",
            ]
            if col in education_column_names
        ),
        None,
    )
    current_profile_weight_expr = (
        f"CAST(GREATEST(0.0, COALESCE(e.{profile_weight_col}, 1.0)) AS DOUBLE)"
        if profile_weight_col
        else "CAST(1.0 AS DOUBLE)"
    )
    current_profile_weight_source = profile_weight_col or "unit_weight"
    startdate_rank_sql = "e.startdate DESC NULLS LAST," if has_startdate else ""
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
WITH recent_school_cip4_calibration AS (
    SELECT *
    FROM (
        SELECT
            CAST(unitid AS VARCHAR) AS unitid,
            degree,
            cip4,
            cohort_year AS calibration_year,
            AVG(ipeds_calibration_weight) AS ipeds_calibration_weight,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(unitid AS VARCHAR), degree, cip4
                ORDER BY cohort_year DESC
            ) AS calibration_rank
        FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED
        WHERE cohort_year < 2025
          AND cip4 IS NOT NULL
          AND ipeds_calibration_weight IS NOT NULL
          AND calibration_ipeds_completions IS NOT NULL
          AND COALESCE(ipeds_calibration_source, 'none') <> 'none'
        GROUP BY CAST(unitid AS VARCHAR), degree, cip4, cohort_year
    )
    WHERE calibration_rank = 1
),
recent_global_cip4_calibration AS (
    SELECT *
    FROM (
        SELECT
            degree,
            cip4,
            cohort_year AS calibration_year,
            AVG(ipeds_calibration_weight) AS ipeds_calibration_weight,
            ROW_NUMBER() OVER (
                PARTITION BY degree, cip4
                ORDER BY cohort_year DESC
            ) AS calibration_rank
        FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED
        WHERE cohort_year < 2025
          AND cip4 IS NOT NULL
          AND ipeds_calibration_weight IS NOT NULL
          AND calibration_ipeds_completions IS NOT NULL
          AND COALESCE(ipeds_calibration_source, 'none') <> 'none'
        GROUP BY degree, cip4, cohort_year
    )
    WHERE calibration_rank = 1
)
SELECT * EXCLUDE (current_rank)
FROM (
    SELECT
        SHA2(TO_VARCHAR(e.user_id), 256) AS person_key,
        CAST(e.unitid AS VARCHAR) AS unitid,
        e.ipeds_name AS school_name,
        'Bachelors' AS degree,
        LEFT({assigned_cip4_expr}, 2) AS cip2,
        {assigned_cip4_expr} AS cip4,
        {candidate_cip6_expr} AS cip6,
        COALESCE(c4.title, {assigned_cip_title_expr}, '') AS major_title,
        {projected_year_expr} AS grad_year,
        '2026-2029' AS cohort_band,
        1 AS current_student_flag,
        COALESCE(NULLIF(d.sex_predicted, ''), 'Unknown') AS gender,
        COALESCE(NULLIF(d.ethnicity_predicted, ''), 'Unknown') AS race_ethnicity,
        d.prestige,
        CASE
            WHEN rsc.ipeds_calibration_weight IS NOT NULL THEN '{current_profile_weight_source}+recent_school_year_cip4'
            WHEN rgc.ipeds_calibration_weight IS NOT NULL THEN '{current_profile_weight_source}+recent_global_year_cip4'
            ELSE '{current_profile_weight_source}'
        END AS profile_weight_source,
        COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, 1.0) AS ipeds_calibration_weight,
        CASE
            WHEN rsc.ipeds_calibration_weight IS NOT NULL THEN 'recent_school_year_cip4'
            WHEN rgc.ipeds_calibration_weight IS NOT NULL THEN 'recent_global_year_cip4'
            ELSE 'none'
        END AS ipeds_calibration_source,
        COALESCE(rsc.calibration_year, rgc.calibration_year) AS calibration_year,
        {current_profile_weight_expr} * COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, 1.0) AS profile_weight,
        {current_profile_weight_expr} * COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, 1.0) AS final_weight,
        {cip_probability_expr} AS cip_probability,
        CASE WHEN {cip_probability_rank_expr} >= 0.8 THEN 1 ELSE 0 END AS high_conf_major_flag,
        ROW_NUMBER() OVER (
            PARTITION BY
                e.user_id,
                CAST(e.unitid AS VARCHAR),
                'Bachelors',
                LEFT({assigned_cip4_expr}, 2),
                {assigned_cip4_expr},
                {candidate_cip6_expr},
                {projected_year_expr}
            ORDER BY
                CASE WHEN {cip_probability_rank_expr} >= 0.8 THEN 1 ELSE 0 END DESC,
                {cip_probability_rank_expr} DESC,
                e.enddate DESC NULLS LAST,
                {startdate_rank_sql}
                {current_profile_weight_expr} * COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, 1.0) DESC
        ) AS current_rank
    FROM {EDUCATION_CIP} e
    LEFT JOIN {SCRATCH}.CIP4_TITLES c4
      ON {assigned_cip4_expr} = c4.code
    LEFT JOIN {demographics_table} d
      ON e.user_id = d.user_id
    LEFT JOIN recent_school_cip4_calibration rsc
      ON CAST(e.unitid AS VARCHAR) = rsc.unitid
     AND rsc.degree = 'Bachelors'
     AND {assigned_cip4_expr} = rsc.cip4
    LEFT JOIN recent_global_cip4_calibration rgc
      ON rgc.degree = 'Bachelors'
     AND {assigned_cip4_expr} = rgc.cip4
    WHERE e.unitid IN ({UNITID_SQL})
      AND e.degree = 'Bachelor'
      AND {assigned_cip4_expr} IS NOT NULL
      AND ({projected_filter})
)
WHERE grad_year BETWEEN 2026 AND 2029
  AND current_rank = 1
"""


WORK_MAX_YEARS_OUT = 15
WORK_TOP_N_PER_GROUP = 50
GRAD_TRANSITION_WINDOWS = list(range(1, 11))
GRAD_BENCHMARK_MIN_WEIGHT = 50
GRAD_DEGREE_VALUES = [
    "Masters",
    "MBA",
    "LAW",
    "MD",
    "PhD",
    "Research Doctorate",
    "Professional Doctorate",
    "Other Doctorate",
    "Doctorate",
]


def _sql_literal_list(values: list[str]) -> str:
    return "(" + ",".join("'" + value.replace("'", "''") + "'" for value in values) + ")"


def _seniority_group_sql(expr: str) -> str:
    return f"""
        CASE
            WHEN {expr} IS NULL THEN 'Unknown'
            WHEN {expr} < 2 THEN 'Entry'
            WHEN {expr} < 4 THEN 'Associate'
            WHEN {expr} < 6 THEN 'Management'
            ELSE 'Senior Leadership'
        END
    """


def _work_cpi_sql() -> str:
    return """
    SELECT column1 AS cpi_year, column2 AS cpi_u_avg
    FROM VALUES
        (2005, 195.300), (2006, 201.600), (2007, 207.342), (2008, 215.303),
        (2009, 214.537), (2010, 218.056), (2011, 224.939), (2012, 229.594),
        (2013, 232.957), (2014, 236.736), (2015, 237.017), (2016, 240.007),
        (2017, 245.120), (2018, 251.107), (2019, 255.657), (2020, 258.811),
        (2021, 270.970), (2022, 292.655), (2023, 304.702), (2024, 313.689),
        (2025, 321.943)
    """


def _recent_calibration_ctes_sql() -> str:
    return f"""
recent_school_cip4_calibration AS (
    SELECT *
    FROM (
        SELECT
            CAST(unitid AS VARCHAR) AS unitid,
            degree,
            cip4,
            cohort_year AS calibration_year,
            AVG(ipeds_calibration_weight) AS ipeds_calibration_weight,
            ANY_VALUE(ipeds_calibration_source) AS ipeds_calibration_source,
            AVG(calibration_observed_completions) AS calibration_observed_completions,
            AVG(calibration_ipeds_completions) AS calibration_ipeds_completions,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(unitid AS VARCHAR), degree, cip4
                ORDER BY cohort_year DESC
            ) AS calibration_rank
        FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED
        WHERE cohort_year < 2025
          AND cip4 IS NOT NULL
          AND ipeds_calibration_weight IS NOT NULL
          AND calibration_ipeds_completions IS NOT NULL
          AND COALESCE(ipeds_calibration_source, 'none') <> 'none'
        GROUP BY CAST(unitid AS VARCHAR), degree, cip4, cohort_year
    )
    WHERE calibration_rank = 1
),
recent_global_cip4_calibration AS (
    SELECT *
    FROM (
        SELECT
            degree,
            cip4,
            cohort_year AS calibration_year,
            AVG(ipeds_calibration_weight) AS ipeds_calibration_weight,
            ANY_VALUE(ipeds_calibration_source) AS ipeds_calibration_source,
            AVG(calibration_observed_completions) AS calibration_observed_completions,
            AVG(calibration_ipeds_completions) AS calibration_ipeds_completions,
            ROW_NUMBER() OVER (
                PARTITION BY degree, cip4
                ORDER BY cohort_year DESC
            ) AS calibration_rank
        FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED
        WHERE cohort_year < 2025
          AND cip4 IS NOT NULL
          AND ipeds_calibration_weight IS NOT NULL
          AND calibration_ipeds_completions IS NOT NULL
          AND COALESCE(ipeds_calibration_source, 'none') <> 'none'
        GROUP BY degree, cip4, cohort_year
    )
    WHERE calibration_rank = 1
)
"""


def _work_later_education_cte_sql(base_cte: str = "grad_years") -> str:
    later_date = education_later_date_sql("e2")
    later_source = postgrad_education_source_sql()
    return f"""
later_edu AS (
    SELECT
        o.user_id,
        o.unitid,
        o.degree,
        o.grad_year,
        o.cip4,
        {postgrad_degree_with_plus_one_sql('e2', 'o')} AS later_degree_type,
        {postgrad_school_name_sql('e2')} AS later_school,
        {assigned_cip4_sql('e2')} AS later_cip4,
        {postgrad_program_title_sql('e2')} AS later_program,
        YEAR({later_date}) AS later_grad_year,
        DATEDIFF('day', o.grad_date, {later_date}) / 365.25 AS years_to_later_degree,
        {plus_one_masters_flag_sql('e2', 'o')} AS plus_one_masters_flag,
        ROW_NUMBER() OVER (
            PARTITION BY o.user_id, o.unitid, o.degree, o.grad_year, o.cip4
            ORDER BY {later_date} ASC
        ) AS later_rank
    FROM (
        SELECT DISTINCT user_id, unitid, degree, grad_year, cip4, grad_date
        FROM {base_cte}
    ) o
    JOIN {later_source} e2
     ON o.user_id = e2.user_id
     AND {postgrad_degree_filter_sql('e2')}
     AND {later_date} > o.grad_date
)
"""


def _work_annual_base_sql() -> str:
    demographics_table = globals().get(
        "DEMOGRAPHICS_TABLE",
        "CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_202303_INDIVIDUAL_USER",
    )
    years_values = ", ".join(f"({i})" for i in range(WORK_MAX_YEARS_OUT + 1))
    return f"""
CREATE OR REPLACE TABLE {SCRATCH}.SCHOOL_OUTCOMES_WORK_ANNUAL_BASE AS
WITH years_out AS (
    SELECT column1 AS years_since_grad
    FROM VALUES {years_values}
),
cpi_u AS (
    {_work_cpi_sql()}
),
{_recent_calibration_ctes_sql()},
grad_years AS (
    SELECT
        g.user_id,
        CAST(g.unitid AS VARCHAR) AS unitid,
        g.ipeds_name AS school_name,
        g.degree,
        g.cip2,
        g.cip4,
        g.cip6,
        g.cip_title AS major_title,
        g.cohort_year AS grad_year,
        g.cohort_band,
        g.grad_date,
        y.years_since_grad,
        DATEADD('year', y.years_since_grad, g.grad_date) AS target_date,
        CASE
            WHEN g.cohort_year >= 2023 AND g.calibration_ipeds_completions IS NULL
            THEN COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, g.ipeds_calibration_weight, 1.0)
            ELSE COALESCE(g.ipeds_calibration_weight, 1.0)
        END AS education_weight,
        CASE
            WHEN g.cohort_year >= 2023 AND g.calibration_ipeds_completions IS NULL AND rsc.ipeds_calibration_weight IS NOT NULL
                THEN 'recent_school_year_cip4'
            WHEN g.cohort_year >= 2023 AND g.calibration_ipeds_completions IS NULL AND rgc.ipeds_calibration_weight IS NOT NULL
                THEN 'recent_global_year_cip4'
            ELSE g.ipeds_calibration_source
        END AS ipeds_calibration_source
    FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED g
    CROSS JOIN years_out y
    LEFT JOIN recent_school_cip4_calibration rsc
      ON CAST(g.unitid AS VARCHAR) = rsc.unitid
     AND g.degree = rsc.degree
     AND g.cip4 = rsc.cip4
    LEFT JOIN recent_global_cip4_calibration rgc
      ON g.degree = rgc.degree
     AND g.cip4 = rgc.cip4
    WHERE DATEADD('year', y.years_since_grad, g.grad_date) <= CURRENT_DATE()
),
demo AS (
    SELECT
        user_id,
        COALESCE(NULLIF(sex_predicted, ''), 'Unknown') AS gender,
        COALESCE(NULLIF(ethnicity_predicted, ''), 'Unknown') AS race_ethnicity,
        prestige
    FROM {demographics_table}
),
{_work_later_education_cte_sql('grad_years')},
position_candidates AS (
    SELECT
        g.*,
        p.startdate AS position_start,
        p.enddate AS position_end,
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
        GREATEST(0.0, {position_weight_sql('p')}) * COALESCE(g.education_weight, 1.0) AS final_weight,
        DATEDIFF('month', GREATEST(p.startdate, g.grad_date), g.target_date) AS tenure_months_at_year,
        CASE WHEN p.enddate IS NULL THEN 1 ELSE 0 END AS current_position_flag,
        ROW_NUMBER() OVER (
            PARTITION BY g.user_id, g.unitid, g.degree, g.cip4, g.grad_year, g.years_since_grad
            ORDER BY
                p.salary DESC NULLS LAST,
                p.startdate DESC NULLS LAST,
                p.enddate DESC NULLS LAST
        ) AS pos_rank
    FROM grad_years g
    JOIN {POSITION_TABLE} p
      ON g.user_id = p.user_id
     AND p.startdate <= g.target_date
     AND (p.enddate >= g.target_date OR p.enddate IS NULL)
     AND COALESCE(p.enddate, CURRENT_DATE()) >= g.grad_date
    LEFT JOIN cpi_u cpi
      ON LEAST(YEAR(g.target_date), 2025) = cpi.cpi_year
),
annual AS (
    SELECT * EXCLUDE (pos_rank)
    FROM position_candidates
    WHERE pos_rank = 1
),
enriched AS (
    SELECT
        SHA2(TO_VARCHAR(a.user_id), 256) AS person_key,
        a.user_id,
        a.unitid,
        a.school_name,
        a.degree,
        a.cip2,
        a.cip4,
        a.cip6,
        a.major_title,
        a.grad_year,
        a.cohort_band,
        a.grad_date,
        a.years_since_grad,
        a.target_date,
        COALESCE(d.gender, 'Unknown') AS gender,
        COALESCE(d.race_ethnicity, 'Unknown') AS race_ethnicity,
        d.prestige,
        CASE
            WHEN LOWER(COALESCE(a.ultimate_parent_company_name, '')) = 'government of the united states of america'
                 AND NULLIF(a.company_name, '') IS NOT NULL
            THEN a.company_name
            ELSE COALESCE(NULLIF(a.ultimate_parent_company_name, ''), NULLIF(a.company_name, ''), 'Unknown')
        END AS employer,
        CASE WHEN COALESCE(NULLIF(a.ultimate_parent_company_name, ''), NULLIF(a.company_name, '')) IS NULL THEN 1 ELSE 0 END AS unknown_employer_flag,
        CASE WHEN COALESCE(NULLIF(a.ultimate_parent_company_name, ''), NULLIF(a.company_name, '')) IS NULL THEN 0 ELSE 1 END AS named_employer_flag,
        LOWER(REGEXP_REPLACE(a.school_name, '[^a-z0-9]', '')) AS school_norm,
        LOWER(REGEXP_REPLACE(
            CASE
                WHEN LOWER(COALESCE(a.ultimate_parent_company_name, '')) = 'government of the united states of america'
                     AND NULLIF(a.company_name, '') IS NOT NULL
                THEN a.company_name
                ELSE COALESCE(NULLIF(a.ultimate_parent_company_name, ''), NULLIF(a.company_name, ''), 'Unknown')
            END,
            '[^a-z0-9]',
            ''
        )) AS employer_norm,
        COALESCE(NULLIF(a.metro_area, ''), NULLIF(a.state, ''), NULLIF(a.country, ''), 'Unknown') AS location,
        a.metro_area,
        a.city,
        a.state,
        a.country,
        a.role_k10_v3,
        a.role_k50_v3,
        a.role_k150_v3,
        a.role_k500_v3,
        a.rics_k50 AS industry_k50,
        a.rics_k200 AS industry_k200,
        a.rics_k400 AS industry_k400,
        a.naics_code,
        a.naics_description,
        a.seniority,
        CASE
            WHEN a.seniority IS NULL THEN 'Unknown'
            WHEN a.seniority < 1 THEN 'Entry I'
            WHEN a.seniority < 2 THEN 'Entry II'
            WHEN a.seniority < 3 THEN 'Mid I'
            WHEN a.seniority < 4 THEN 'Mid II'
            WHEN a.seniority < 5 THEN 'Senior I'
            WHEN a.seniority < 6 THEN 'Senior II'
            WHEN a.seniority < 7 THEN 'Manager I'
            WHEN a.seniority < 8 THEN 'Manager II'
            ELSE 'Director+'
        END AS seniority_band,
        a.title_raw,
        a.position_start,
        a.position_end,
        a.tenure_months_at_year,
        a.current_position_flag,
        a.salary_nominal,
        a.salary,
        a.position_weight,
        a.education_weight AS ipeds_calibration_weight,
        a.final_weight,
        a.ipeds_calibration_source,
        le.later_degree_type,
        le.later_school,
        le.later_cip4,
        le.later_program,
        le.later_grad_year,
        le.years_to_later_degree,
        COALESCE(le.plus_one_masters_flag, 0) AS plus_one_masters_flag,
        CASE WHEN le.user_id IS NULL THEN 1 ELSE 0 END AS no_further_education_flag
    FROM annual a
    LEFT JOIN demo d
      ON a.user_id = d.user_id
    LEFT JOIN later_edu le
      ON a.user_id = le.user_id
     AND a.unitid = le.unitid
     AND a.degree = le.degree
     AND a.grad_year = le.grad_year
     AND COALESCE(a.cip4, '') = COALESCE(le.cip4, '')
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


def _work_position_base_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE {SCRATCH}.SCHOOL_OUTCOMES_WORK_POSITION_BASE AS
WITH {_recent_calibration_ctes_sql()},
grads AS (
    SELECT
        g.user_id,
        CAST(g.unitid AS VARCHAR) AS unitid,
        g.ipeds_name AS school_name,
        g.degree,
        g.cip2,
        g.cip4,
        g.cip6,
        g.cip_title AS major_title,
        g.cohort_year AS grad_year,
        g.cohort_band,
        g.grad_date,
        CASE
            WHEN g.cohort_year >= 2023 AND g.calibration_ipeds_completions IS NULL
            THEN COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, g.ipeds_calibration_weight, 1.0)
            ELSE COALESCE(g.ipeds_calibration_weight, 1.0)
        END AS education_weight
    FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED g
    LEFT JOIN recent_school_cip4_calibration rsc
      ON CAST(g.unitid AS VARCHAR) = rsc.unitid
     AND g.degree = rsc.degree
     AND g.cip4 = rsc.cip4
    LEFT JOIN recent_global_cip4_calibration rgc
      ON g.degree = rgc.degree
     AND g.cip4 = rgc.cip4
),
{_work_later_education_cte_sql('grads')},
positions_raw AS (
    SELECT
        g.*,
        le.later_degree_type,
        le.later_school,
        le.later_cip4,
        le.later_program,
        le.later_grad_year,
        le.years_to_later_degree,
        COALESCE(le.plus_one_masters_flag, 0) AS plus_one_masters_flag,
        CASE WHEN le.user_id IS NULL THEN 1 ELSE 0 END AS no_further_education_flag,
        CASE
            WHEN LOWER(COALESCE(p.ultimate_parent_company_name, '')) = 'government of the united states of america'
                 AND NULLIF(p.company_name, '') IS NOT NULL
            THEN p.company_name
            ELSE COALESCE(NULLIF(p.ultimate_parent_company_name, ''), NULLIF(p.company_name, ''), 'Unknown')
        END AS employer,
        p.role_k50_v3,
        p.role_k150_v3,
        p.rics_k50 AS industry_k50,
        p.rics_k200 AS industry_k200,
        p.metro_area,
        p.city,
        p.state,
        p.country,
        COALESCE(NULLIF(p.metro_area, ''), NULLIF(p.state, ''), NULLIF(p.country, ''), 'Unknown') AS location,
        p.seniority,
        p.salary,
        p.startdate AS position_start,
        p.enddate AS position_end,
        GREATEST(p.startdate, g.grad_date) AS postgrad_start_date,
        LEAST(COALESCE(p.enddate, CURRENT_DATE()), DATEADD('year', {WORK_MAX_YEARS_OUT}, g.grad_date), CURRENT_DATE()) AS observed_end_date,
        CASE WHEN p.enddate IS NULL OR p.enddate > CURRENT_DATE() THEN 1 ELSE 0 END AS right_censored_flag,
        GREATEST(0.0, {position_weight_sql('p')}) AS position_weight,
        GREATEST(0.0, {position_weight_sql('p')}) * COALESCE(g.education_weight, 1.0) AS final_weight
    FROM grads g
    JOIN {POSITION_TABLE} p
      ON g.user_id = p.user_id
     AND p.startdate <= LEAST(DATEADD('year', {WORK_MAX_YEARS_OUT}, g.grad_date), CURRENT_DATE())
     AND COALESCE(p.enddate, CURRENT_DATE()) >= g.grad_date
    LEFT JOIN later_edu le
      ON g.user_id = le.user_id
     AND g.unitid = le.unitid
     AND g.degree = le.degree
     AND g.grad_year = le.grad_year
     AND COALESCE(g.cip4, '') = COALESCE(le.cip4, '')
     AND le.later_rank = 1
),
positions AS (
    SELECT
        *,
        DATEDIFF('month', postgrad_start_date, observed_end_date) AS observed_tenure_months,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, unitid, degree, cip4, grad_year
            ORDER BY postgrad_start_date ASC, observed_end_date ASC, employer ASC
        ) AS job_rank
    FROM positions_raw
    WHERE DATEDIFF('day', postgrad_start_date, observed_end_date) >= 30
)
SELECT *
FROM positions
"""


def _graduate_degree_match_sql(left_col: str, right_col: str) -> str:
    research_values = _sql_literal_list(["Research Doctorate", "PhD", "Doctorate", "Other Doctorate"])
    return f"""
        (
            {left_col} = {right_col}
            OR ({left_col} IN {research_values} AND {right_col} IN {research_values})
        )
    """


def _grad_transition_base_sql() -> str:
    grad_windows = ", ".join(f"({window})" for window in GRAD_TRANSITION_WINDOWS)
    grad_degree_values = _sql_literal_list(GRAD_DEGREE_VALUES)
    raw_grad_degree_filter = postgrad_degree_filter_sql("e")
    graduate_degree_match = _graduate_degree_match_sql("g.degree", "ge.degree")
    return f"""
CREATE OR REPLACE TABLE {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BASE AS
WITH cpi_u AS (
    {_work_cpi_sql()}
),
{_recent_calibration_ctes_sql()},
grad_edu_labeled AS (
    SELECT
        e.user_id,
        CAST(e.unitid AS VARCHAR) AS unitid,
        {postgrad_degree_label_sql('e')} AS degree,
        {assigned_cip4_sql('e')} AS cip4,
        {assigned_cip_title_sql('e')} AS major_title,
        YEAR(e.enddate) AS grad_year,
        e.startdate AS grad_start_date,
        e.enddate AS grad_date
    FROM {EDUCATION_CIP} e
    WHERE e.unitid IN ({UNITID_SQL})
      AND e.enddate IS NOT NULL
      AND YEAR(e.enddate) BETWEEN 2005 AND 2025
      AND {raw_grad_degree_filter}
),
grad_edu AS (
    SELECT * EXCLUDE (grad_edu_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY user_id, unitid, degree, COALESCE(cip4, ''), grad_year
                ORDER BY grad_start_date DESC NULLS LAST, grad_date DESC
            ) AS grad_edu_rank
        FROM grad_edu_labeled
    )
    WHERE grad_edu_rank = 1
),
grads AS (
    SELECT
        g.user_id,
        SHA2(TO_VARCHAR(g.user_id), 256) AS person_key,
        CAST(g.unitid AS VARCHAR) AS unitid,
        g.ipeds_name AS school_name,
        g.degree,
        g.cip2,
        g.cip4,
        g.cip6,
        g.cip_title AS major_title,
        g.cohort_year AS grad_year,
        g.cohort_band,
        g.grad_date,
        ge.grad_start_date,
        CASE WHEN ge.grad_start_date IS NULL THEN 1 ELSE 0 END AS missing_grad_start_flag,
        CASE
            WHEN COALESCE(ge.grad_start_date, DATEADD('year', -2, g.grad_date)) >= g.grad_date
            THEN DATEADD('year', -1, g.grad_date)
            ELSE COALESCE(ge.grad_start_date, DATEADD('year', -2, g.grad_date))
        END AS pre_anchor_date,
        CASE
            WHEN g.cohort_year >= 2023 AND g.calibration_ipeds_completions IS NULL
            THEN COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, g.ipeds_calibration_weight, 1.0)
            ELSE COALESCE(g.ipeds_calibration_weight, 1.0)
        END AS education_weight
    FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED g
    LEFT JOIN grad_edu ge
      ON g.user_id = ge.user_id
     AND CAST(g.unitid AS VARCHAR) = ge.unitid
     AND g.cohort_year = ge.grad_year
     AND COALESCE(g.cip4, '') = COALESCE(ge.cip4, '')
     AND {graduate_degree_match}
    LEFT JOIN recent_school_cip4_calibration rsc
      ON CAST(g.unitid AS VARCHAR) = rsc.unitid
     AND g.degree = rsc.degree
     AND g.cip4 = rsc.cip4
    LEFT JOIN recent_global_cip4_calibration rgc
      ON g.degree = rgc.degree
     AND g.cip4 = rgc.cip4
    WHERE g.degree IN {grad_degree_values}
),
feeder_bachelor AS (
    SELECT *
    FROM (
        SELECT
            g.user_id,
            g.unitid,
            g.degree,
            g.grad_year,
            g.cip4,
            CAST(e0.unitid AS VARCHAR) AS feeder_unitid,
            e0.ipeds_name AS feeder_school,
            {assigned_cip4_sql('e0')} AS feeder_cip4,
            {assigned_cip_title_sql('e0')} AS feeder_program,
            e0.enddate AS feeder_grad_date,
            YEAR(e0.enddate) AS feeder_grad_year,
            DATEDIFF('day', e0.enddate, g.grad_date) / 365.25 AS years_since_feeder_degree,
            ROW_NUMBER() OVER (
                PARTITION BY g.user_id, g.unitid, g.degree, g.grad_year, COALESCE(g.cip4, '')
                ORDER BY e0.enddate DESC
            ) AS feeder_rank
        FROM grads g
        JOIN {EDUCATION_CIP} e0
          ON g.user_id = e0.user_id
         AND e0.degree = 'Bachelor'
         AND e0.enddate < g.grad_date
    )
    WHERE feeder_rank = 1
),
grads_enriched AS (
    SELECT
        g.*,
        f.feeder_unitid,
        f.feeder_school,
        f.feeder_cip4,
        f.feeder_program,
        f.feeder_grad_date,
        f.feeder_grad_year,
        f.years_since_feeder_degree,
        CASE
            WHEN g.degree = 'Masters'
             AND f.feeder_unitid = g.unitid
             AND DATEDIFF('day', f.feeder_grad_date, g.grad_date) BETWEEN 1 AND 366
            THEN 1 ELSE 0
        END AS grad_plus_one_masters_flag,
        CASE
            WHEN f.feeder_grad_date IS NOT NULL
            THEN CAST(LEAST(12, GREATEST(0, FLOOR(DATEDIFF('day', f.feeder_grad_date, g.pre_anchor_date) / 365.25))) AS INTEGER)
            ELSE -1
        END AS pre_year_bucket
    FROM grads g
    LEFT JOIN feeder_bachelor f
      ON g.user_id = f.user_id
     AND g.unitid = f.unitid
     AND g.degree = f.degree
     AND g.grad_year = f.grad_year
     AND COALESCE(g.cip4, '') = COALESCE(f.cip4, '')
),
pre_candidates AS (
    SELECT
        g.*,
        CASE
            WHEN LOWER(COALESCE(p.ultimate_parent_company_name, '')) = 'government of the united states of america'
                 AND NULLIF(p.company_name, '') IS NOT NULL
            THEN p.company_name
            ELSE COALESCE(NULLIF(p.ultimate_parent_company_name, ''), NULLIF(p.company_name, ''), 'Unknown')
        END AS pre_employer,
        COALESCE(NULLIF(p.role_k50_v3, ''), 'Unknown') AS pre_role,
        COALESCE(NULLIF(p.role_k150_v3, ''), 'Unknown') AS pre_role_detail,
        COALESCE(NULLIF(p.rics_k50, ''), 'Unknown') AS pre_industry,
        COALESCE(NULLIF(p.rics_k200, ''), 'Unknown') AS pre_industry_detail,
        COALESCE(NULLIF(p.metro_area, ''), NULLIF(p.state, ''), NULLIF(p.country, ''), 'Unknown') AS pre_location,
        p.seniority AS pre_seniority,
        {_seniority_group_sql('p.seniority')} AS pre_seniority_group,
        p.salary AS pre_salary_nominal,
        CASE
            WHEN p.salary IS NOT NULL AND cpi.cpi_u_avg IS NOT NULL
            THEN ROUND(p.salary * {SALARY_REAL_BASE_CPI} / cpi.cpi_u_avg, 0)
            ELSE NULL
        END AS pre_salary,
        p.startdate AS pre_position_start,
        p.enddate AS pre_position_end,
        GREATEST(0.0, {position_weight_sql('p')}) AS pre_position_weight,
        ROW_NUMBER() OVER (
            PARTITION BY g.user_id, g.unitid, g.degree, g.grad_year, COALESCE(g.cip4, '')
            ORDER BY
                COALESCE(p.enddate, g.pre_anchor_date) DESC NULLS LAST,
                p.startdate DESC NULLS LAST,
                p.salary DESC NULLS LAST
        ) AS pre_rank
    FROM grads_enriched g
    JOIN {POSITION_TABLE} p
      ON g.user_id = p.user_id
     AND p.startdate < g.pre_anchor_date
     AND COALESCE(p.enddate, g.pre_anchor_date) >= DATEADD('year', -3, g.pre_anchor_date)
     AND DATEDIFF('day', p.startdate, LEAST(COALESCE(p.enddate, g.pre_anchor_date), g.pre_anchor_date)) >= 60
    LEFT JOIN cpi_u cpi
      ON LEAST(YEAR(g.pre_anchor_date), 2025) = cpi.cpi_year
),
pre_jobs AS (
    SELECT * EXCLUDE (pre_rank)
    FROM pre_candidates
    WHERE pre_rank = 1
),
windows AS (
    SELECT column1 AS years_after_degree
    FROM VALUES {grad_windows}
),
post_candidates AS (
    SELECT
        pr.*,
        w.years_after_degree,
        DATEADD('year', w.years_after_degree, pr.grad_date) AS target_date,
        CASE
            WHEN LOWER(COALESCE(p.ultimate_parent_company_name, '')) = 'government of the united states of america'
                 AND NULLIF(p.company_name, '') IS NOT NULL
            THEN p.company_name
            ELSE COALESCE(NULLIF(p.ultimate_parent_company_name, ''), NULLIF(p.company_name, ''), 'Unknown')
        END AS post_employer,
        COALESCE(NULLIF(p.role_k50_v3, ''), 'Unknown') AS post_role,
        COALESCE(NULLIF(p.role_k150_v3, ''), 'Unknown') AS post_role_detail,
        COALESCE(NULLIF(p.rics_k50, ''), 'Unknown') AS post_industry,
        COALESCE(NULLIF(p.rics_k200, ''), 'Unknown') AS post_industry_detail,
        COALESCE(NULLIF(p.metro_area, ''), NULLIF(p.state, ''), NULLIF(p.country, ''), 'Unknown') AS post_location,
        p.seniority AS post_seniority,
        {_seniority_group_sql('p.seniority')} AS post_seniority_group,
        p.salary AS post_salary_nominal,
        CASE
            WHEN p.salary IS NOT NULL AND cpi.cpi_u_avg IS NOT NULL
            THEN ROUND(p.salary * {SALARY_REAL_BASE_CPI} / cpi.cpi_u_avg, 0)
            ELSE NULL
        END AS post_salary,
        p.startdate AS post_position_start,
        p.enddate AS post_position_end,
        GREATEST(0.0, {position_weight_sql('p')}) AS post_position_weight,
        ROW_NUMBER() OVER (
            PARTITION BY pr.user_id, pr.unitid, pr.degree, pr.grad_year, COALESCE(pr.cip4, ''), w.years_after_degree
            ORDER BY
                p.salary DESC NULLS LAST,
                p.startdate DESC NULLS LAST,
                p.enddate DESC NULLS LAST
        ) AS post_rank
    FROM pre_jobs pr
    JOIN windows w
      ON DATEADD('year', w.years_after_degree, pr.grad_date) <= CURRENT_DATE()
    JOIN {POSITION_TABLE} p
      ON pr.user_id = p.user_id
     AND p.startdate <= DATEADD('year', w.years_after_degree, pr.grad_date)
     AND (p.enddate >= DATEADD('year', w.years_after_degree, pr.grad_date) OR p.enddate IS NULL)
     AND COALESCE(p.enddate, CURRENT_DATE()) >= pr.grad_date
    LEFT JOIN cpi_u cpi
      ON LEAST(YEAR(DATEADD('year', w.years_after_degree, pr.grad_date)), 2025) = cpi.cpi_year
)
SELECT
    * EXCLUDE (post_rank),
    COALESCE(education_weight, 1.0) * SQRT(GREATEST(0.0, COALESCE(pre_position_weight, 1.0)) * GREATEST(0.0, COALESCE(post_position_weight, 1.0))) AS transition_weight,
    CASE WHEN pre_role <> post_role AND pre_role <> 'Unknown' AND post_role <> 'Unknown' THEN 1 ELSE 0 END AS role_changed_flag,
    CASE WHEN pre_industry <> post_industry AND pre_industry <> 'Unknown' AND post_industry <> 'Unknown' THEN 1 ELSE 0 END AS industry_changed_flag,
    CASE WHEN pre_employer <> post_employer AND pre_employer <> 'Unknown' AND post_employer <> 'Unknown' THEN 1 ELSE 0 END AS employer_changed_flag
FROM post_candidates
WHERE post_rank = 1
"""


def _grad_transition_benchmark_sql() -> str:
    grad_windows = ", ".join(f"({window})" for window in GRAD_TRANSITION_WINDOWS)
    return f"""
CREATE OR REPLACE TABLE {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK AS
WITH windows AS (
    SELECT column1 AS years_after_degree
    FROM VALUES {grad_windows}
),
pairs AS (
    SELECT
        COALESCE(NULLIF(a0.role_k50_v3, ''), 'Unknown') AS pre_role,
        COALESCE(NULLIF(a0.industry_k50, ''), 'Unknown') AS pre_industry,
        {_seniority_group_sql('a0.seniority')} AS pre_seniority_group,
        CAST(LEAST(12, GREATEST(0, a0.years_since_grad)) AS INTEGER) AS pre_year_bucket,
        w.years_after_degree,
        a0.salary AS pre_salary,
        a1.salary AS post_salary,
        a0.seniority AS pre_seniority,
        a1.seniority AS post_seniority,
        SQRT(GREATEST(0.0, COALESCE(a0.final_weight, 1.0)) * GREATEST(0.0, COALESCE(a1.final_weight, 1.0))) AS pair_weight
    FROM {SCRATCH}.SCHOOL_OUTCOMES_WORK_ANNUAL_BASE a0
    JOIN windows w
      ON a0.years_since_grad + w.years_after_degree <= {WORK_MAX_YEARS_OUT}
    JOIN {SCRATCH}.SCHOOL_OUTCOMES_WORK_ANNUAL_BASE a1
      ON a0.user_id = a1.user_id
     AND a0.unitid = a1.unitid
     AND a0.degree = a1.degree
     AND COALESCE(a0.cip4, '') = COALESCE(a1.cip4, '')
     AND a0.grad_year = a1.grad_year
     AND a1.years_since_grad = a0.years_since_grad + w.years_after_degree
    WHERE a0.degree = 'Bachelors'
      AND COALESCE(a0.no_further_education_flag, 0) = 1
      AND COALESCE(a1.no_further_education_flag, 0) = 1
      AND a0.salary IS NOT NULL
      AND a1.salary IS NOT NULL
      AND a0.salary > 0
      AND a0.years_since_grad BETWEEN 0 AND 12
),
benchmarks AS (
    SELECT 'role_industry_seniority_year' AS benchmark_tier, pre_role, pre_industry, pre_seniority_group, pre_year_bucket, years_after_degree, pre_salary, post_salary, pre_seniority, post_seniority, pair_weight FROM pairs
    UNION ALL
    SELECT 'role_industry_seniority' AS benchmark_tier, pre_role, pre_industry, pre_seniority_group, -1 AS pre_year_bucket, years_after_degree, pre_salary, post_salary, pre_seniority, post_seniority, pair_weight FROM pairs
    UNION ALL
    SELECT 'role_seniority' AS benchmark_tier, pre_role, 'All' AS pre_industry, pre_seniority_group, -1 AS pre_year_bucket, years_after_degree, pre_salary, post_salary, pre_seniority, post_seniority, pair_weight FROM pairs
    UNION ALL
    SELECT 'industry_seniority' AS benchmark_tier, 'All' AS pre_role, pre_industry, pre_seniority_group, -1 AS pre_year_bucket, years_after_degree, pre_salary, post_salary, pre_seniority, post_seniority, pair_weight FROM pairs
    UNION ALL
    SELECT 'seniority' AS benchmark_tier, 'All' AS pre_role, 'All' AS pre_industry, pre_seniority_group, -1 AS pre_year_bucket, years_after_degree, pre_salary, post_salary, pre_seniority, post_seniority, pair_weight FROM pairs
    UNION ALL
    SELECT 'overall' AS benchmark_tier, 'All' AS pre_role, 'All' AS pre_industry, 'All' AS pre_seniority_group, -1 AS pre_year_bucket, years_after_degree, pre_salary, post_salary, pre_seniority, post_seniority, pair_weight FROM pairs
)
SELECT
    benchmark_tier,
    pre_role,
    pre_industry,
    pre_seniority_group,
    pre_year_bucket,
    years_after_degree,
    ROUND(SUM(pair_weight), 0) AS n_benchmark,
    COUNT(*) AS raw_n,
    ROUND(SUM((post_salary / NULLIF(pre_salary, 0)) * pair_weight) / NULLIF(SUM(pair_weight), 0), 4) AS expected_salary_ratio,
    ROUND(100 * (SUM((post_salary / NULLIF(pre_salary, 0)) * pair_weight) / NULLIF(SUM(pair_weight), 0) - 1), 2) AS expected_salary_growth_pct,
    ROUND(SUM((post_seniority - pre_seniority) * pair_weight) / NULLIF(SUM(CASE WHEN post_seniority IS NOT NULL AND pre_seniority IS NOT NULL THEN pair_weight ELSE 0 END), 0), 2) AS expected_seniority_delta
FROM benchmarks
GROUP BY benchmark_tier, pre_role, pre_industry, pre_seniority_group, pre_year_bucket, years_after_degree
"""


def _grad_transition_stacked_source_sql() -> str:
    columns = """
        person_key, unitid, school_name, degree, cip2, cip4, major_title,
        grad_year, cohort_band, grad_date, grad_start_date, pre_anchor_date,
        years_after_degree, grad_plus_one_masters_flag,
        missing_grad_start_flag, feeder_unitid, feeder_school, feeder_cip4, feeder_program,
        feeder_grad_year, years_since_feeder_degree, pre_year_bucket,
        pre_employer, post_employer, pre_role, post_role, pre_role_detail, post_role_detail,
        pre_industry, post_industry, pre_industry_detail, post_industry_detail,
        pre_location, post_location, pre_seniority_group, post_seniority_group,
        pre_salary, post_salary, pre_seniority, post_seniority,
        transition_weight, role_changed_flag, industry_changed_flag, employer_changed_flag
    """
    return f"""
    SELECT
        'CIP4' AS cip_level,
        {columns}
    FROM {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BASE
    WHERE cip4 IS NOT NULL
    UNION ALL
    SELECT
        'ALL' AS cip_level,
        person_key, unitid, school_name, degree, cip2, 'ALL' AS cip4, 'All majors' AS major_title,
        grad_year, cohort_band, grad_date, grad_start_date, pre_anchor_date,
        years_after_degree, grad_plus_one_masters_flag,
        missing_grad_start_flag, feeder_unitid, feeder_school, feeder_cip4, feeder_program,
        feeder_grad_year, years_since_feeder_degree, pre_year_bucket,
        pre_employer, post_employer, pre_role, post_role, pre_role_detail, post_role_detail,
        pre_industry, post_industry, pre_industry_detail, post_industry_detail,
        pre_location, post_location, pre_seniority_group, post_seniority_group,
        pre_salary, post_salary, pre_seniority, post_seniority,
        transition_weight, role_changed_flag, industry_changed_flag, employer_changed_flag
    FROM {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BASE
    """


def _grad_value_summary_query() -> str:
    min_benchmark = GRAD_BENCHMARK_MIN_WEIGHT
    return f"""
WITH stacked AS (
    {_grad_transition_stacked_source_sql()}
), benchmarked AS (
    SELECT
        s.*,
        COALESCE(b1.expected_salary_ratio, b2.expected_salary_ratio, b3.expected_salary_ratio, b4.expected_salary_ratio, b5.expected_salary_ratio, b6.expected_salary_ratio) AS expected_salary_ratio,
        COALESCE(b1.expected_salary_growth_pct, b2.expected_salary_growth_pct, b3.expected_salary_growth_pct, b4.expected_salary_growth_pct, b5.expected_salary_growth_pct, b6.expected_salary_growth_pct) AS expected_salary_growth_pct,
        COALESCE(b1.expected_seniority_delta, b2.expected_seniority_delta, b3.expected_seniority_delta, b4.expected_seniority_delta, b5.expected_seniority_delta, b6.expected_seniority_delta) AS expected_seniority_delta,
        COALESCE(b1.benchmark_tier, b2.benchmark_tier, b3.benchmark_tier, b4.benchmark_tier, b5.benchmark_tier, b6.benchmark_tier) AS benchmark_tier
    FROM stacked s
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b1
      ON b1.benchmark_tier = 'role_industry_seniority_year'
     AND b1.pre_role = s.pre_role
     AND b1.pre_industry = s.pre_industry
     AND b1.pre_seniority_group = s.pre_seniority_group
     AND b1.pre_year_bucket = s.pre_year_bucket
     AND b1.years_after_degree = s.years_after_degree
     AND b1.n_benchmark >= {min_benchmark}
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b2
      ON b2.benchmark_tier = 'role_industry_seniority'
     AND b2.pre_role = s.pre_role
     AND b2.pre_industry = s.pre_industry
     AND b2.pre_seniority_group = s.pre_seniority_group
     AND b2.pre_year_bucket = -1
     AND b2.years_after_degree = s.years_after_degree
     AND b2.n_benchmark >= {min_benchmark}
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b3
      ON b3.benchmark_tier = 'role_seniority'
     AND b3.pre_role = s.pre_role
     AND b3.pre_industry = 'All'
     AND b3.pre_seniority_group = s.pre_seniority_group
     AND b3.pre_year_bucket = -1
     AND b3.years_after_degree = s.years_after_degree
     AND b3.n_benchmark >= {min_benchmark}
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b4
      ON b4.benchmark_tier = 'industry_seniority'
     AND b4.pre_role = 'All'
     AND b4.pre_industry = s.pre_industry
     AND b4.pre_seniority_group = s.pre_seniority_group
     AND b4.pre_year_bucket = -1
     AND b4.years_after_degree = s.years_after_degree
     AND b4.n_benchmark >= {min_benchmark}
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b5
      ON b5.benchmark_tier = 'seniority'
     AND b5.pre_role = 'All'
     AND b5.pre_industry = 'All'
     AND b5.pre_seniority_group = s.pre_seniority_group
     AND b5.pre_year_bucket = -1
     AND b5.years_after_degree = s.years_after_degree
     AND b5.n_benchmark >= {min_benchmark}
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b6
      ON b6.benchmark_tier = 'overall'
     AND b6.pre_role = 'All'
     AND b6.pre_industry = 'All'
     AND b6.pre_seniority_group = 'All'
     AND b6.pre_year_bucket = -1
     AND b6.years_after_degree = s.years_after_degree
     AND b6.n_benchmark >= {min_benchmark}
), grouped AS (
    SELECT
        unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, years_after_degree, grad_plus_one_masters_flag,
        ROUND(SUM(transition_weight), 0) AS n_alumni,
        COUNT(*) AS raw_n,
        ROUND(SUM(CASE WHEN pre_salary IS NOT NULL THEN pre_salary * transition_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN pre_salary IS NOT NULL THEN transition_weight ELSE 0 END), 0), 0) AS avg_salary_before,
        ROUND(SUM(CASE WHEN post_salary IS NOT NULL THEN post_salary * transition_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN post_salary IS NOT NULL THEN transition_weight ELSE 0 END), 0), 0) AS avg_salary_after,
        ROUND(SUM(CASE WHEN pre_salary IS NOT NULL AND expected_salary_ratio IS NOT NULL THEN pre_salary * expected_salary_ratio * transition_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN pre_salary IS NOT NULL AND expected_salary_ratio IS NOT NULL THEN transition_weight ELSE 0 END), 0), 0) AS expected_salary_after,
        ROUND(SUM(CASE WHEN pre_seniority IS NOT NULL THEN pre_seniority * transition_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN pre_seniority IS NOT NULL THEN transition_weight ELSE 0 END), 0), 2) AS avg_seniority_before,
        ROUND(SUM(CASE WHEN post_seniority IS NOT NULL THEN post_seniority * transition_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN post_seniority IS NOT NULL THEN transition_weight ELSE 0 END), 0), 2) AS avg_seniority_after,
        ROUND(SUM(CASE WHEN expected_seniority_delta IS NOT NULL THEN expected_seniority_delta * transition_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN expected_seniority_delta IS NOT NULL THEN transition_weight ELSE 0 END), 0), 2) AS expected_seniority_delta,
        ROUND(100 * SUM(role_changed_flag * transition_weight) / NULLIF(SUM(transition_weight), 0), 2) AS role_change_pct,
        ROUND(100 * SUM(industry_changed_flag * transition_weight) / NULLIF(SUM(transition_weight), 0), 2) AS industry_change_pct,
        ROUND(100 * SUM(employer_changed_flag * transition_weight) / NULLIF(SUM(transition_weight), 0), 2) AS employer_change_pct,
        ROUND(100 * SUM(missing_grad_start_flag * transition_weight) / NULLIF(SUM(transition_weight), 0), 2) AS missing_grad_start_pct,
        ROUND(SUM(GREATEST(0.25, DATEDIFF('day', pre_anchor_date, grad_date) / 365.25) * transition_weight) / NULLIF(SUM(transition_weight), 0), 2) AS avg_degree_duration_years,
        ANY_VALUE(benchmark_tier) AS primary_benchmark_tier
    FROM benchmarked
    GROUP BY unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, years_after_degree, grad_plus_one_masters_flag
)
SELECT
    *,
    ROUND(100 * (avg_salary_after / NULLIF(avg_salary_before, 0) - 1), 2) AS observed_salary_growth_pct,
    ROUND(100 * (expected_salary_after / NULLIF(avg_salary_before, 0) - 1), 2) AS expected_salary_growth_pct,
    ROUND(avg_salary_after - expected_salary_after, 0) AS salary_lift_dollars,
    ROUND(100 * (avg_salary_after / NULLIF(expected_salary_after, 0) - 1), 2) AS salary_lift_pct,
    ROUND(avg_seniority_after - avg_seniority_before, 2) AS observed_seniority_delta,
    ROUND((avg_seniority_after - avg_seniority_before) - expected_seniority_delta, 2) AS seniority_lift
FROM grouped
"""


def _grad_transition_category_query(category: str) -> str:
    min_benchmark = GRAD_BENCHMARK_MIN_WEIGHT
    if category == "role":
        pre_col = "pre_role"
        post_col = "post_role"
    elif category == "industry":
        pre_col = "pre_industry"
        post_col = "post_industry"
    else:
        raise ValueError("category must be role or industry")
    return f"""
WITH stacked AS (
    {_grad_transition_stacked_source_sql()}
), benchmarked AS (
    SELECT
        s.*,
        COALESCE(b1.expected_salary_ratio, b2.expected_salary_ratio, b3.expected_salary_ratio, b4.expected_salary_ratio, b5.expected_salary_ratio, b6.expected_salary_ratio) AS expected_salary_ratio,
        COALESCE(b1.benchmark_tier, b2.benchmark_tier, b3.benchmark_tier, b4.benchmark_tier, b5.benchmark_tier, b6.benchmark_tier) AS benchmark_tier
    FROM stacked s
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b1
      ON b1.benchmark_tier = 'role_industry_seniority_year'
     AND b1.pre_role = s.pre_role
     AND b1.pre_industry = s.pre_industry
     AND b1.pre_seniority_group = s.pre_seniority_group
     AND b1.pre_year_bucket = s.pre_year_bucket
     AND b1.years_after_degree = s.years_after_degree
     AND b1.n_benchmark >= {min_benchmark}
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b2
      ON b2.benchmark_tier = 'role_industry_seniority'
     AND b2.pre_role = s.pre_role
     AND b2.pre_industry = s.pre_industry
     AND b2.pre_seniority_group = s.pre_seniority_group
     AND b2.pre_year_bucket = -1
     AND b2.years_after_degree = s.years_after_degree
     AND b2.n_benchmark >= {min_benchmark}
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b3
      ON b3.benchmark_tier = 'role_seniority'
     AND b3.pre_role = s.pre_role
     AND b3.pre_industry = 'All'
     AND b3.pre_seniority_group = s.pre_seniority_group
     AND b3.pre_year_bucket = -1
     AND b3.years_after_degree = s.years_after_degree
     AND b3.n_benchmark >= {min_benchmark}
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b4
      ON b4.benchmark_tier = 'industry_seniority'
     AND b4.pre_role = 'All'
     AND b4.pre_industry = s.pre_industry
     AND b4.pre_seniority_group = s.pre_seniority_group
     AND b4.pre_year_bucket = -1
     AND b4.years_after_degree = s.years_after_degree
     AND b4.n_benchmark >= {min_benchmark}
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b5
      ON b5.benchmark_tier = 'seniority'
     AND b5.pre_role = 'All'
     AND b5.pre_industry = 'All'
     AND b5.pre_seniority_group = s.pre_seniority_group
     AND b5.pre_year_bucket = -1
     AND b5.years_after_degree = s.years_after_degree
     AND b5.n_benchmark >= {min_benchmark}
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK b6
      ON b6.benchmark_tier = 'overall'
     AND b6.pre_role = 'All'
     AND b6.pre_industry = 'All'
     AND b6.pre_seniority_group = 'All'
     AND b6.pre_year_bucket = -1
     AND b6.years_after_degree = s.years_after_degree
     AND b6.n_benchmark >= {min_benchmark}
), grouped AS (
    SELECT
        unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, years_after_degree, grad_plus_one_masters_flag,
        {pre_col} AS pre_{category},
        {post_col} AS post_{category},
        ROUND(SUM(transition_weight), 0) AS n_alumni,
        COUNT(*) AS raw_n,
        ROUND(SUM(CASE WHEN pre_salary IS NOT NULL THEN pre_salary * transition_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN pre_salary IS NOT NULL THEN transition_weight ELSE 0 END), 0), 0) AS avg_salary_before,
        ROUND(SUM(CASE WHEN post_salary IS NOT NULL THEN post_salary * transition_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN post_salary IS NOT NULL THEN transition_weight ELSE 0 END), 0), 0) AS avg_salary_after,
        ROUND(SUM(CASE WHEN pre_salary IS NOT NULL AND expected_salary_ratio IS NOT NULL THEN pre_salary * expected_salary_ratio * transition_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN pre_salary IS NOT NULL AND expected_salary_ratio IS NOT NULL THEN transition_weight ELSE 0 END), 0), 0) AS expected_salary_after,
        ROUND(SUM(CASE WHEN pre_seniority IS NOT NULL THEN pre_seniority * transition_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN pre_seniority IS NOT NULL THEN transition_weight ELSE 0 END), 0), 2) AS avg_seniority_before,
        ROUND(SUM(CASE WHEN post_seniority IS NOT NULL THEN post_seniority * transition_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN post_seniority IS NOT NULL THEN transition_weight ELSE 0 END), 0), 2) AS avg_seniority_after
    FROM benchmarked
    WHERE {pre_col} <> 'Unknown'
      AND {post_col} <> 'Unknown'
    GROUP BY unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, years_after_degree, grad_plus_one_masters_flag, {pre_col}, {post_col}
), totals AS (
    SELECT unitid, degree, cip_level, cip4, grad_year, years_after_degree, grad_plus_one_masters_flag, SUM(n_alumni) AS total_n
    FROM grouped
    GROUP BY unitid, degree, cip_level, cip4, grad_year, years_after_degree, grad_plus_one_masters_flag
), ranked AS (
    SELECT
        g.*,
        ROUND(100 * g.n_alumni / NULLIF(t.total_n, 0), 2) AS share_pct,
        ROW_NUMBER() OVER (
            PARTITION BY g.unitid, g.degree, g.cip_level, g.cip4, g.grad_year, g.years_after_degree, g.grad_plus_one_masters_flag
            ORDER BY g.n_alumni DESC, pre_{category}, post_{category}
        ) AS transition_rank
    FROM grouped g
    JOIN totals t
      ON g.unitid = t.unitid
     AND g.degree = t.degree
     AND g.cip_level = t.cip_level
     AND g.cip4 = t.cip4
     AND g.grad_year = t.grad_year
     AND g.years_after_degree = t.years_after_degree
     AND g.grad_plus_one_masters_flag = t.grad_plus_one_masters_flag
)
SELECT *
    EXCLUDE (transition_rank),
    ROUND(avg_salary_after - avg_salary_before, 0) AS salary_delta,
    ROUND(avg_salary_after - expected_salary_after, 0) AS salary_lift_dollars
FROM ranked
WHERE transition_rank <= {WORK_TOP_N_PER_GROUP}
"""


def _work_stacked_source_sql(source_table: str, extra_cols: str = "") -> str:
    extra = f", {extra_cols}" if extra_cols else ""
    return f"""
    SELECT
        person_key, unitid, school_name, degree,
        'CIP4' AS cip_level, cip4, major_title,
        grad_year, cohort_band, years_since_grad,
        COALESCE(later_degree_type, '') AS later_degree_type,
        COALESCE(no_further_education_flag, 0) AS no_further_education_flag,
        salary, seniority, seniority_band, final_weight AS analysis_weight{extra}
    FROM {source_table}
    WHERE cip4 IS NOT NULL
    UNION ALL
    SELECT
        person_key, unitid, school_name, degree,
        'ALL' AS cip_level, 'ALL' AS cip4, 'All majors' AS major_title,
        grad_year, cohort_band, years_since_grad,
        COALESCE(later_degree_type, '') AS later_degree_type,
        COALESCE(no_further_education_flag, 0) AS no_further_education_flag,
        salary, seniority, seniority_band, final_weight AS analysis_weight{extra}
    FROM {source_table}
    """


def _work_annual_salary_query() -> str:
    source = _work_stacked_source_sql(f"{SCRATCH}.SCHOOL_OUTCOMES_WORK_ANNUAL_BASE")
    group_cols = [
        'unitid', 'school_name', 'degree', 'cip_level', 'cip4', 'major_title',
        'grad_year', 'cohort_band', 'years_since_grad',
        'later_degree_type', 'no_further_education_flag'
    ]
    return weighted_aggregate_sql(
        source,
        group_cols,
        count_alias='n_alumni',
        salary_count_alias='salary_obs',
        quantile_aliases={0.25: 'p25_salary', 0.50: 'median_salary', 0.75: 'p75_salary', 0.90: 'p90_salary'},
        mean_alias='mean_salary',
    )


def _work_seniority_query() -> str:
    return f"""
WITH stacked AS (
    {_work_stacked_source_sql(f'{SCRATCH}.SCHOOL_OUTCOMES_WORK_ANNUAL_BASE')}
), grouped AS (
    SELECT
        unitid, school_name, degree, cip_level, cip4, major_title, years_since_grad,
        later_degree_type, no_further_education_flag,
        seniority_band,
        ROUND(SUM(COALESCE(analysis_weight, 1.0)), 0) AS n_alumni,
        COUNT(*) AS raw_n,
        ROUND(SUM(CASE WHEN seniority IS NOT NULL THEN seniority * COALESCE(analysis_weight, 1.0) ELSE 0 END) /
              NULLIF(SUM(CASE WHEN seniority IS NOT NULL THEN COALESCE(analysis_weight, 1.0) ELSE 0 END), 0), 2) AS avg_seniority,
        ROUND(SUM(CASE WHEN salary IS NOT NULL THEN salary * COALESCE(analysis_weight, 1.0) ELSE 0 END) /
              NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN COALESCE(analysis_weight, 1.0) ELSE 0 END), 0), 0) AS mean_salary,
        ROUND(SUM(CASE WHEN salary IS NOT NULL THEN COALESCE(analysis_weight, 1.0) ELSE 0 END), 0) AS salary_obs
    FROM stacked
    WHERE seniority_band <> 'Unknown'
    GROUP BY unitid, school_name, degree, cip_level, cip4, major_title, years_since_grad,
        later_degree_type, no_further_education_flag, seniority_band
), totals AS (
    SELECT unitid, degree, cip_level, cip4, years_since_grad, later_degree_type, no_further_education_flag, SUM(n_alumni) AS total_n
    FROM grouped
    GROUP BY unitid, degree, cip_level, cip4, years_since_grad, later_degree_type, no_further_education_flag
)
SELECT
    g.*,
    ROUND(100 * g.n_alumni / NULLIF(t.total_n, 0), 2) AS share_pct
FROM grouped g
JOIN totals t
  ON g.unitid = t.unitid
 AND g.degree = t.degree
 AND g.cip_level = t.cip_level
 AND g.cip4 = t.cip4
 AND g.years_since_grad = t.years_since_grad
 AND g.later_degree_type = t.later_degree_type
 AND g.no_further_education_flag = t.no_further_education_flag
"""


def _work_category_query(fact_name: str, category_expr: str, category_alias: str, extra_select: str = "") -> str:
    extra = f", {extra_select}" if extra_select else ""
    flag_select = ""
    flag_group = ""
    flag_output = ""
    if fact_name == 'annual_employers':
        flag_select = ", same_school_employer_flag, career_employer_flag"
        flag_group = ", same_school_employer_flag, career_employer_flag"
        flag_output = ", MAX(same_school_employer_flag) AS same_school_employer_flag, MAX(career_employer_flag) AS career_employer_flag"
    return f"""
WITH stacked AS (
    SELECT
        unitid, school_name, degree, 'CIP4' AS cip_level, cip4, major_title, years_since_grad,
        COALESCE(later_degree_type, '') AS later_degree_type,
        COALESCE(no_further_education_flag, 0) AS no_further_education_flag,
        COALESCE(NULLIF({category_expr}, ''), 'Unknown') AS {category_alias},
        salary, seniority, final_weight AS analysis_weight{flag_select}{extra}
    FROM {SCRATCH}.SCHOOL_OUTCOMES_WORK_ANNUAL_BASE
    WHERE cip4 IS NOT NULL
    UNION ALL
    SELECT
        unitid, school_name, degree, 'ALL' AS cip_level, 'ALL' AS cip4, 'All majors' AS major_title, years_since_grad,
        COALESCE(later_degree_type, '') AS later_degree_type,
        COALESCE(no_further_education_flag, 0) AS no_further_education_flag,
        COALESCE(NULLIF({category_expr}, ''), 'Unknown') AS {category_alias},
        salary, seniority, final_weight AS analysis_weight{flag_select}{extra}
    FROM {SCRATCH}.SCHOOL_OUTCOMES_WORK_ANNUAL_BASE
), grouped AS (
    SELECT
        unitid, school_name, degree, cip_level, cip4, major_title, years_since_grad,
        later_degree_type, no_further_education_flag, {category_alias},
        ROUND(SUM(COALESCE(analysis_weight, 1.0)), 0) AS n_alumni,
        COUNT(*) AS raw_n,
        ROUND(SUM(CASE WHEN salary IS NOT NULL THEN salary * COALESCE(analysis_weight, 1.0) ELSE 0 END) /
              NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN COALESCE(analysis_weight, 1.0) ELSE 0 END), 0), 0) AS mean_salary,
        ROUND(SUM(CASE WHEN salary IS NOT NULL THEN COALESCE(analysis_weight, 1.0) ELSE 0 END), 0) AS salary_obs,
        ROUND(SUM(CASE WHEN seniority IS NOT NULL THEN seniority * COALESCE(analysis_weight, 1.0) ELSE 0 END) /
              NULLIF(SUM(CASE WHEN seniority IS NOT NULL THEN COALESCE(analysis_weight, 1.0) ELSE 0 END), 0), 2) AS avg_seniority
        {flag_output}
    FROM stacked
    WHERE {category_alias} <> 'Unknown'
    GROUP BY unitid, school_name, degree, cip_level, cip4, major_title, years_since_grad,
        later_degree_type, no_further_education_flag, {category_alias}{flag_group}
), totals AS (
    SELECT unitid, degree, cip_level, cip4, years_since_grad, later_degree_type, no_further_education_flag, SUM(n_alumni) AS total_n
    FROM grouped
    GROUP BY unitid, degree, cip_level, cip4, years_since_grad, later_degree_type, no_further_education_flag
), ranked AS (
    SELECT
        g.*,
        ROUND(100 * g.n_alumni / NULLIF(t.total_n, 0), 2) AS share_pct,
        ROW_NUMBER() OVER (
            PARTITION BY g.unitid, g.degree, g.cip_level, g.cip4, g.years_since_grad,
                g.later_degree_type, g.no_further_education_flag
            ORDER BY g.n_alumni DESC, {category_alias}
        ) AS category_rank
    FROM grouped g
    JOIN totals t
      ON g.unitid = t.unitid
     AND g.degree = t.degree
     AND g.cip_level = t.cip_level
     AND g.cip4 = t.cip4
     AND g.years_since_grad = t.years_since_grad
     AND g.later_degree_type = t.later_degree_type
     AND g.no_further_education_flag = t.no_further_education_flag
)
SELECT *
FROM ranked
WHERE category_rank <= {WORK_TOP_N_PER_GROUP}
"""


def _work_mobility_query() -> str:
    return f"""
WITH first_jobs AS (
    SELECT *
    FROM {SCRATCH}.SCHOOL_OUTCOMES_WORK_POSITION_BASE
    WHERE job_rank = 1
), person_mobility AS (
    SELECT
        f.user_id,
        f.unitid,
        f.school_name,
        f.degree,
        f.cip2,
        f.cip4,
        f.major_title,
        f.grad_year,
        f.cohort_band,
        f.later_degree_type,
        f.no_further_education_flag,
        f.grad_date,
        f.final_weight AS person_weight,
        f.employer AS first_employer,
        COUNT(DISTINCT CASE WHEN p.postgrad_start_date <= LEAST(DATEADD('year', 5, f.grad_date), CURRENT_DATE()) THEN p.employer END) AS employers_5yr,
        COUNT(DISTINCT CASE WHEN p.postgrad_start_date <= LEAST(DATEADD('year', 10, f.grad_date), CURRENT_DATE()) THEN p.employer END) AS employers_10yr,
        COUNT(DISTINCT p.employer) AS employers_15yr,
        MAX(CASE WHEN DATEADD('year', 1, f.grad_date) <= CURRENT_DATE() THEN 1 ELSE 0 END) AS eligible_1yr,
        MAX(CASE WHEN DATEADD('year', 3, f.grad_date) <= CURRENT_DATE() THEN 1 ELSE 0 END) AS eligible_3yr,
        MAX(CASE WHEN DATEADD('year', 5, f.grad_date) <= CURRENT_DATE() THEN 1 ELSE 0 END) AS eligible_5yr,
        MAX(CASE WHEN p.employer = f.employer AND p.postgrad_start_date <= DATEADD('year', 1, f.grad_date) AND p.observed_end_date >= DATEADD('year', 1, f.grad_date) THEN 1 ELSE 0 END) AS retained_first_employer_1yr,
        MAX(CASE WHEN p.employer = f.employer AND p.postgrad_start_date <= DATEADD('year', 3, f.grad_date) AND p.observed_end_date >= DATEADD('year', 3, f.grad_date) THEN 1 ELSE 0 END) AS retained_first_employer_3yr,
        MAX(CASE WHEN p.employer = f.employer AND p.postgrad_start_date <= DATEADD('year', 5, f.grad_date) AND p.observed_end_date >= DATEADD('year', 5, f.grad_date) THEN 1 ELSE 0 END) AS retained_first_employer_5yr
    FROM first_jobs f
    JOIN {SCRATCH}.SCHOOL_OUTCOMES_WORK_POSITION_BASE p
      ON f.user_id = p.user_id
     AND f.unitid = p.unitid
     AND f.degree = p.degree
     AND COALESCE(f.cip4, '') = COALESCE(p.cip4, '')
     AND f.grad_year = p.grad_year
    GROUP BY f.user_id, f.unitid, f.school_name, f.degree, f.cip2, f.cip4, f.major_title, f.grad_year, f.cohort_band,
        f.later_degree_type, f.no_further_education_flag, f.grad_date, f.final_weight, f.employer
), stacked AS (
    SELECT unitid, school_name, degree, 'CIP4' AS cip_level, cip4, major_title, grad_year, cohort_band,
           later_degree_type, no_further_education_flag, person_weight,
           employers_5yr, employers_10yr, employers_15yr, eligible_1yr, eligible_3yr, eligible_5yr,
           retained_first_employer_1yr, retained_first_employer_3yr, retained_first_employer_5yr
    FROM person_mobility
    WHERE cip4 IS NOT NULL
    UNION ALL
    SELECT unitid, school_name, degree, 'ALL' AS cip_level, 'ALL' AS cip4, 'All majors' AS major_title, grad_year, cohort_band,
           later_degree_type, no_further_education_flag, person_weight,
           employers_5yr, employers_10yr, employers_15yr, eligible_1yr, eligible_3yr, eligible_5yr,
           retained_first_employer_1yr, retained_first_employer_3yr, retained_first_employer_5yr
    FROM person_mobility
)
SELECT
    unitid, school_name, degree, cip_level, cip4, major_title, grad_year, cohort_band,
    later_degree_type, no_further_education_flag,
    ROUND(SUM(person_weight), 0) AS n_alumni,
    COUNT(*) AS raw_n,
    ROUND(SUM(employers_5yr * person_weight) / NULLIF(SUM(person_weight), 0), 2) AS avg_employers_5yr,
    ROUND(SUM(employers_10yr * person_weight) / NULLIF(SUM(person_weight), 0), 2) AS avg_employers_10yr,
    ROUND(SUM(employers_15yr * person_weight) / NULLIF(SUM(person_weight), 0), 2) AS avg_employers_15yr,
    ROUND(100 * SUM(CASE WHEN eligible_1yr = 1 THEN retained_first_employer_1yr * person_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN eligible_1yr = 1 THEN person_weight ELSE 0 END), 0), 2) AS first_employer_retention_1yr_pct,
    ROUND(100 * SUM(CASE WHEN eligible_3yr = 1 THEN retained_first_employer_3yr * person_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN eligible_3yr = 1 THEN person_weight ELSE 0 END), 0), 2) AS first_employer_retention_3yr_pct,
    ROUND(100 * SUM(CASE WHEN eligible_5yr = 1 THEN retained_first_employer_5yr * person_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN eligible_5yr = 1 THEN person_weight ELSE 0 END), 0), 2) AS first_employer_retention_5yr_pct
FROM stacked
GROUP BY unitid, school_name, degree, cip_level, cip4, major_title, grad_year, cohort_band,
    later_degree_type, no_further_education_flag
"""


def _work_employer_tenure_query() -> str:
    return f"""
WITH first_jobs AS (
    SELECT *
    FROM {SCRATCH}.SCHOOL_OUTCOMES_WORK_POSITION_BASE
    WHERE job_rank = 1
      AND employer <> 'Unknown'
), first_job_outcomes AS (
    SELECT
        f.*,
        a5.salary AS salary_5yr,
        a5.seniority AS seniority_5yr,
        a10.salary AS salary_10yr,
        a10.seniority AS seniority_10yr
    FROM first_jobs f
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_WORK_ANNUAL_BASE a5
      ON f.user_id = a5.user_id
     AND f.unitid = a5.unitid
     AND f.degree = a5.degree
     AND COALESCE(f.cip4, '') = COALESCE(a5.cip4, '')
     AND f.grad_year = a5.grad_year
     AND a5.years_since_grad = 5
    LEFT JOIN {SCRATCH}.SCHOOL_OUTCOMES_WORK_ANNUAL_BASE a10
      ON f.user_id = a10.user_id
     AND f.unitid = a10.unitid
     AND f.degree = a10.degree
     AND COALESCE(f.cip4, '') = COALESCE(a10.cip4, '')
     AND f.grad_year = a10.grad_year
     AND a10.years_since_grad = 10
), stacked AS (
    SELECT unitid, school_name, degree, 'CIP4' AS cip_level, cip4, major_title,
           later_degree_type, no_further_education_flag, employer,
           final_weight, observed_tenure_months, right_censored_flag, grad_date, postgrad_start_date, observed_end_date,
           salary_5yr, salary_10yr, seniority_5yr, seniority_10yr
    FROM first_job_outcomes
    WHERE cip4 IS NOT NULL
    UNION ALL
    SELECT unitid, school_name, degree, 'ALL' AS cip_level, 'ALL' AS cip4, 'All majors' AS major_title,
           later_degree_type, no_further_education_flag, employer,
           final_weight, observed_tenure_months, right_censored_flag, grad_date, postgrad_start_date, observed_end_date,
           salary_5yr, salary_10yr, seniority_5yr, seniority_10yr
    FROM first_job_outcomes
), grouped AS (
    SELECT
        unitid, school_name, degree, cip_level, cip4, major_title,
        later_degree_type, no_further_education_flag, employer,
        ROUND(SUM(final_weight), 0) AS n_starters,
        COUNT(*) AS raw_n,
        ROUND(SUM(observed_tenure_months * final_weight) / NULLIF(SUM(final_weight), 0), 1) AS avg_observed_tenure_months,
        ROUND(SUM(CASE WHEN right_censored_flag = 0 THEN observed_tenure_months * final_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN right_censored_flag = 0 THEN final_weight ELSE 0 END), 0), 1) AS avg_completed_tenure_months,
        ROUND(100 * SUM(right_censored_flag * final_weight) / NULLIF(SUM(final_weight), 0), 2) AS still_current_pct,
        ROUND(100 * SUM(CASE WHEN DATEADD('year', 1, grad_date) <= CURRENT_DATE() AND observed_end_date >= DATEADD('year', 1, grad_date) THEN final_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN DATEADD('year', 1, grad_date) <= CURRENT_DATE() THEN final_weight ELSE 0 END), 0), 2) AS retained_1yr_pct,
        ROUND(100 * SUM(CASE WHEN DATEADD('year', 3, grad_date) <= CURRENT_DATE() AND observed_end_date >= DATEADD('year', 3, grad_date) THEN final_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN DATEADD('year', 3, grad_date) <= CURRENT_DATE() THEN final_weight ELSE 0 END), 0), 2) AS retained_3yr_pct,
        ROUND(100 * SUM(CASE WHEN DATEADD('year', 5, grad_date) <= CURRENT_DATE() AND observed_end_date >= DATEADD('year', 5, grad_date) THEN final_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN DATEADD('year', 5, grad_date) <= CURRENT_DATE() THEN final_weight ELSE 0 END), 0), 2) AS retained_5yr_pct,
        ROUND(SUM(CASE WHEN salary_5yr IS NOT NULL OR seniority_5yr IS NOT NULL THEN final_weight ELSE 0 END), 0) AS outcome_obs_5yr,
        ROUND(SUM(CASE WHEN salary_10yr IS NOT NULL OR seniority_10yr IS NOT NULL THEN final_weight ELSE 0 END), 0) AS outcome_obs_10yr,
        ROUND(SUM(CASE WHEN salary_5yr IS NOT NULL THEN salary_5yr * final_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN salary_5yr IS NOT NULL THEN final_weight ELSE 0 END), 0), 0) AS avg_salary_5yr,
        ROUND(SUM(CASE WHEN salary_10yr IS NOT NULL THEN salary_10yr * final_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN salary_10yr IS NOT NULL THEN final_weight ELSE 0 END), 0), 0) AS avg_salary_10yr,
        ROUND(SUM(CASE WHEN seniority_5yr IS NOT NULL THEN seniority_5yr * final_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN seniority_5yr IS NOT NULL THEN final_weight ELSE 0 END), 0), 2) AS avg_seniority_5yr,
        ROUND(SUM(CASE WHEN seniority_10yr IS NOT NULL THEN seniority_10yr * final_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN seniority_10yr IS NOT NULL THEN final_weight ELSE 0 END), 0), 2) AS avg_seniority_10yr,
        ROUND(100 * SUM(CASE WHEN seniority_5yr >= 4 THEN final_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN seniority_5yr IS NOT NULL THEN final_weight ELSE 0 END), 0), 2) AS senior_plus_5yr_pct,
        ROUND(100 * SUM(CASE WHEN seniority_10yr >= 6 THEN final_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN seniority_10yr IS NOT NULL THEN final_weight ELSE 0 END), 0), 2) AS manager_plus_10yr_pct
    FROM stacked
    GROUP BY unitid, school_name, degree, cip_level, cip4, major_title,
        later_degree_type, no_further_education_flag, employer
), ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY unitid, degree, cip_level, cip4, later_degree_type, no_further_education_flag
            ORDER BY n_starters DESC, employer
        ) AS employer_rank
    FROM grouped
)
SELECT *
FROM ranked
WHERE employer_rank <= {WORK_TOP_N_PER_GROUP}
"""


INTERNSHIP_START_YEAR = 2020
INTERNSHIP_END_YEAR = 2025
INTERNSHIP_TOP_N_PER_GROUP = 50


def _position_column_map() -> dict[str, str]:
    columns = globals().get("POSITION_COLUMNS", {})
    if isinstance(columns, dict):
        return {str(key).lower(): str(value) for key, value in columns.items()}
    return {str(column).lower(): str(column) for column in columns}


def _position_column(candidates: list[str]) -> str | None:
    column_map = _position_column_map()
    for candidate in candidates:
        match = column_map.get(candidate.lower())
        if match:
            return match
    return None


def _internship_signal_sql(alias: str = "p") -> str:
    flag_col = _position_column([
        "internship_flag",
        "is_internship",
        "is_intern",
        "intern_flag",
        "pregrad_internship_flag",
        "student_internship_flag",
    ])
    if flag_col:
        value = f"LOWER(TRIM(TO_VARCHAR({alias}.{flag_col})))"
        return f"({value} IN ('1', 'true', 't', 'yes', 'y'))"

    text_parts = [f"COALESCE({alias}.title_raw, '')"]
    for column in [
        _position_column(["role_k50_v3"]),
        _position_column(["role_k150_v3"]),
        _position_column(["role_k500_v3"]),
    ]:
        if column:
            text_parts.append(f"COALESCE({alias}.{column}, '')")
    text_expr = "LOWER(" + " || ' ' || ".join(text_parts) + ")"
    return f"""
        (
            REGEXP_LIKE({text_expr}, '(^|[^a-z])(intern|internship|summer analyst|summer associate|co[- ]?op)([^a-z]|$)')
            AND NOT REGEXP_LIKE({text_expr}, 'international|internal medicine|internist')
        )
    """


def _internship_base_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE {SCRATCH}.SCHOOL_OUTCOMES_INTERNSHIP_BASE AS
WITH cpi_u AS (
    {_work_cpi_sql()}
),
{_recent_calibration_ctes_sql()},
grads AS (
    SELECT
        g.user_id,
        SHA2(TO_VARCHAR(g.user_id), 256) AS person_key,
        CAST(g.unitid AS VARCHAR) AS unitid,
        g.ipeds_name AS school_name,
        g.degree,
        g.cip2,
        g.cip4,
        g.cip6,
        g.cip_title AS major_title,
        g.cohort_year AS grad_year,
        g.cohort_band,
        g.grad_date,
        CASE
            WHEN g.cohort_year >= 2023 AND g.calibration_ipeds_completions IS NULL
            THEN COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, g.ipeds_calibration_weight, 1.0)
            ELSE COALESCE(g.ipeds_calibration_weight, 1.0)
        END AS education_weight
    FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED g
    LEFT JOIN recent_school_cip4_calibration rsc
      ON CAST(g.unitid AS VARCHAR) = rsc.unitid
     AND g.degree = rsc.degree
     AND g.cip4 = rsc.cip4
    LEFT JOIN recent_global_cip4_calibration rgc
      ON g.degree = rgc.degree
     AND g.cip4 = rgc.cip4
    WHERE g.cohort_year BETWEEN {INTERNSHIP_START_YEAR} AND {INTERNSHIP_END_YEAR}
),
{_work_later_education_cte_sql('grads')},
internship_positions AS (
    SELECT
        g.*,
        le.later_degree_type,
        le.later_school,
        le.later_cip4,
        le.later_program,
        le.later_grad_year,
        le.years_to_later_degree,
        COALESCE(le.plus_one_masters_flag, 0) AS plus_one_masters_flag,
        CASE WHEN le.user_id IS NULL THEN 1 ELSE 0 END AS no_further_education_flag,
        CASE
            WHEN LOWER(COALESCE(p.ultimate_parent_company_name, '')) = 'government of the united states of america'
                 AND NULLIF(p.company_name, '') IS NOT NULL
            THEN p.company_name
            ELSE COALESCE(NULLIF(p.ultimate_parent_company_name, ''), NULLIF(p.company_name, ''), 'Unknown')
        END AS employer,
        COALESCE(NULLIF(p.role_k50_v3, ''), 'Unknown') AS role,
        COALESCE(NULLIF(p.role_k150_v3, ''), 'Unknown') AS role_detail,
        COALESCE(NULLIF(p.rics_k50, ''), 'Unknown') AS industry,
        COALESCE(NULLIF(p.rics_k200, ''), 'Unknown') AS industry_detail,
        COALESCE(NULLIF(p.metro_area, ''), NULLIF(p.state, ''), NULLIF(p.country, ''), 'Unknown') AS location,
        p.metro_area,
        p.city,
        p.state,
        p.country,
        p.salary AS salary_nominal,
        CASE
            WHEN p.salary IS NOT NULL AND cpi.cpi_u_avg IS NOT NULL
            THEN ROUND(p.salary * {SALARY_REAL_BASE_CPI} / cpi.cpi_u_avg, 0)
            ELSE NULL
        END AS salary,
        p.startdate AS internship_start,
        p.enddate AS internship_end,
        GREATEST(0.0, {position_weight_sql('p')}) AS position_weight,
        GREATEST(0.0, {position_weight_sql('p')}) * COALESCE(g.education_weight, 1.0) AS final_weight
    FROM grads g
    JOIN {POSITION_TABLE} p
      ON g.user_id = p.user_id
     AND p.startdate BETWEEN DATEADD('year', -3, g.grad_date) AND DATEADD('month', 6, g.grad_date)
     AND p.startdate <= CURRENT_DATE()
     AND {_internship_signal_sql('p')}
     AND DATEDIFF('day', p.startdate, COALESCE(p.enddate, DATEADD('month', 3, p.startdate))) >= 14
    LEFT JOIN cpi_u cpi
      ON LEAST(YEAR(p.startdate), 2025) = cpi.cpi_year
    LEFT JOIN later_edu le
      ON g.user_id = le.user_id
     AND g.unitid = le.unitid
     AND g.degree = le.degree
     AND g.grad_year = le.grad_year
     AND COALESCE(g.cip4, '') = COALESCE(le.cip4, '')
     AND le.later_rank = 1
)
SELECT *
FROM internship_positions
"""


def _internship_grads_stacked_sql() -> str:
    return f"""
    SELECT
        SHA2(TO_VARCHAR(g.user_id), 256) AS person_key,
        CAST(g.unitid AS VARCHAR) AS unitid,
        g.ipeds_name AS school_name,
        g.degree,
        'CIP4' AS cip_level,
        g.cip4,
        g.cip_title AS major_title,
        g.cohort_year AS grad_year,
        g.cohort_band,
        COALESCE(le.later_degree_type, '') AS later_degree_type,
        CASE WHEN le.user_id IS NULL THEN 1 ELSE 0 END AS no_further_education_flag,
        CASE
            WHEN g.cohort_year >= 2023 AND g.calibration_ipeds_completions IS NULL
            THEN COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, g.ipeds_calibration_weight, 1.0)
            ELSE COALESCE(g.ipeds_calibration_weight, 1.0)
        END AS education_weight
    FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED g
    LEFT JOIN recent_school_cip4_calibration rsc
      ON CAST(g.unitid AS VARCHAR) = rsc.unitid
     AND g.degree = rsc.degree
     AND g.cip4 = rsc.cip4
    LEFT JOIN recent_global_cip4_calibration rgc
      ON g.degree = rgc.degree
     AND g.cip4 = rgc.cip4
    LEFT JOIN later_edu le
      ON g.user_id = le.user_id
     AND CAST(g.unitid AS VARCHAR) = le.unitid
     AND g.degree = le.degree
     AND g.cohort_year = le.grad_year
     AND COALESCE(g.cip4, '') = COALESCE(le.cip4, '')
     AND le.later_rank = 1
    WHERE g.cohort_year BETWEEN {INTERNSHIP_START_YEAR} AND {INTERNSHIP_END_YEAR}
      AND g.cip4 IS NOT NULL
    UNION ALL
    SELECT
        SHA2(TO_VARCHAR(g.user_id), 256) AS person_key,
        CAST(g.unitid AS VARCHAR) AS unitid,
        g.ipeds_name AS school_name,
        g.degree,
        'ALL' AS cip_level,
        'ALL' AS cip4,
        'All majors' AS major_title,
        g.cohort_year AS grad_year,
        g.cohort_band,
        COALESCE(le.later_degree_type, '') AS later_degree_type,
        CASE WHEN le.user_id IS NULL THEN 1 ELSE 0 END AS no_further_education_flag,
        CASE
            WHEN g.cohort_year >= 2023 AND g.calibration_ipeds_completions IS NULL
            THEN COALESCE(rsc.ipeds_calibration_weight, rgc.ipeds_calibration_weight, g.ipeds_calibration_weight, 1.0)
            ELSE COALESCE(g.ipeds_calibration_weight, 1.0)
        END AS education_weight
    FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED g
    LEFT JOIN recent_school_cip4_calibration rsc
      ON CAST(g.unitid AS VARCHAR) = rsc.unitid
     AND g.degree = rsc.degree
     AND g.cip4 = rsc.cip4
    LEFT JOIN recent_global_cip4_calibration rgc
      ON g.degree = rgc.degree
     AND g.cip4 = rgc.cip4
    LEFT JOIN later_edu le
      ON g.user_id = le.user_id
     AND CAST(g.unitid AS VARCHAR) = le.unitid
     AND g.degree = le.degree
     AND g.cohort_year = le.grad_year
     AND COALESCE(g.cip4, '') = COALESCE(le.cip4, '')
     AND le.later_rank = 1
    WHERE g.cohort_year BETWEEN {INTERNSHIP_START_YEAR} AND {INTERNSHIP_END_YEAR}
"""


def _internship_positions_stacked_sql(extra_cols: str = "") -> str:
    extra = f", {extra_cols}" if extra_cols else ""
    return f"""
    SELECT
        person_key, unitid, school_name, degree,
        'CIP4' AS cip_level, cip4, major_title,
        grad_year, cohort_band,
        COALESCE(later_degree_type, '') AS later_degree_type,
        COALESCE(no_further_education_flag, 0) AS no_further_education_flag,
        salary, education_weight, final_weight{extra}
    FROM {SCRATCH}.SCHOOL_OUTCOMES_INTERNSHIP_BASE
    WHERE cip4 IS NOT NULL
    UNION ALL
    SELECT
        person_key, unitid, school_name, degree,
        'ALL' AS cip_level, 'ALL' AS cip4, 'All majors' AS major_title,
        grad_year, cohort_band,
        COALESCE(later_degree_type, '') AS later_degree_type,
        COALESCE(no_further_education_flag, 0) AS no_further_education_flag,
        salary, education_weight, final_weight{extra}
    FROM {SCRATCH}.SCHOOL_OUTCOMES_INTERNSHIP_BASE
"""


def _internship_summary_query() -> str:
    return f"""
WITH {_recent_calibration_ctes_sql()},
grad_years AS (
    SELECT DISTINCT user_id, unitid, degree, cohort_year AS grad_year, cip4, grad_date
    FROM {SCRATCH}.SCHOOL_GRADS_CALIBRATED
    WHERE cohort_year BETWEEN {INTERNSHIP_START_YEAR} AND {INTERNSHIP_END_YEAR}
),
{_work_later_education_cte_sql('grad_years')},
grads AS (
    {_internship_grads_stacked_sql()}
),
intern_positions AS (
    {_internship_positions_stacked_sql()}
),
intern_people AS (
    SELECT
        unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, later_degree_type, no_further_education_flag,
        person_key,
        MAX(education_weight) AS intern_weight,
        SUM(final_weight) AS internship_position_weight,
        COUNT(*) AS raw_internship_positions
    FROM intern_positions
    GROUP BY unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, later_degree_type, no_further_education_flag, person_key
),
grad_grouped AS (
    SELECT
        unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, later_degree_type, no_further_education_flag,
        SUM(education_weight) AS n_alumni,
        COUNT(DISTINCT person_key) AS raw_alumni
    FROM grads
    GROUP BY unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, later_degree_type, no_further_education_flag
),
intern_grouped AS (
    SELECT
        unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, later_degree_type, no_further_education_flag,
        SUM(intern_weight) AS n_interns,
        COUNT(DISTINCT person_key) AS raw_interns,
        SUM(internship_position_weight) AS internship_positions,
        SUM(raw_internship_positions) AS raw_internship_positions
    FROM intern_people
    GROUP BY unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, later_degree_type, no_further_education_flag
)
SELECT
    g.unitid,
    g.school_name,
    g.degree,
    g.cip_level,
    g.cip4,
    g.major_title,
    g.grad_year,
    g.cohort_band,
    g.later_degree_type,
    g.no_further_education_flag,
    ROUND(g.n_alumni, 0) AS n_alumni,
    g.raw_alumni,
    ROUND(COALESCE(i.n_interns, 0), 0) AS n_interns,
    COALESCE(i.raw_interns, 0) AS raw_interns,
    ROUND(COALESCE(i.internship_positions, 0), 0) AS internship_positions,
    COALESCE(i.raw_internship_positions, 0) AS raw_internship_positions
FROM grad_grouped g
LEFT JOIN intern_grouped i
  ON g.unitid = i.unitid
 AND g.degree = i.degree
 AND g.cip_level = i.cip_level
 AND g.cip4 = i.cip4
 AND g.grad_year = i.grad_year
 AND g.later_degree_type = i.later_degree_type
 AND g.no_further_education_flag = i.no_further_education_flag
WHERE g.n_alumni >= 1
"""


def _internship_category_query(category_expr: str, category_alias: str) -> str:
    return f"""
WITH stacked AS (
    {_internship_positions_stacked_sql(f"COALESCE(NULLIF({category_expr}, ''), 'Unknown') AS {category_alias}")}
),
grouped AS (
    SELECT
        unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, later_degree_type, no_further_education_flag,
        {category_alias},
        ROUND(SUM(final_weight), 0) AS n_internship_positions,
        COUNT(*) AS raw_n,
        ROUND(SUM(CASE WHEN salary IS NOT NULL THEN salary * final_weight ELSE 0 END) /
              NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0), 0) AS mean_salary
    FROM stacked
    WHERE {category_alias} <> 'Unknown'
    GROUP BY unitid, school_name, degree, cip_level, cip4, major_title,
        grad_year, cohort_band, later_degree_type, no_further_education_flag, {category_alias}
),
totals AS (
    SELECT unitid, degree, cip_level, cip4, grad_year, later_degree_type, no_further_education_flag,
           SUM(n_internship_positions) AS total_n
    FROM grouped
    GROUP BY unitid, degree, cip_level, cip4, grad_year, later_degree_type, no_further_education_flag
),
ranked AS (
    SELECT
        g.*,
        ROUND(100 * g.n_internship_positions / NULLIF(t.total_n, 0), 2) AS share_pct,
        ROW_NUMBER() OVER (
            PARTITION BY g.unitid, g.degree, g.cip_level, g.cip4, g.grad_year,
                g.later_degree_type, g.no_further_education_flag
            ORDER BY g.n_internship_positions DESC, {category_alias}
        ) AS category_rank
    FROM grouped g
    JOIN totals t
      ON g.unitid = t.unitid
     AND g.degree = t.degree
     AND g.cip_level = t.cip_level
     AND g.cip4 = t.cip4
     AND g.grad_year = t.grad_year
     AND g.later_degree_type = t.later_degree_type
     AND g.no_further_education_flag = t.no_further_education_flag
)
SELECT *
FROM ranked
WHERE category_rank <= {INTERNSHIP_TOP_N_PER_GROUP}
"""


def _write_work_history_facts(platform_dir: Path) -> dict:
    """Build medium-weight career trajectory facts without exporting raw position history."""
    work_dir = platform_dir / "work_facts"
    _clean_dir(work_dir)

    conn = sfClient.connect()
    cur = conn.cursor()
    try:
        print("Creating annual work snapshot base in Snowflake...")
        cur.execute(_work_annual_base_sql())
        print("Creating post-graduation position base in Snowflake...")
        cur.execute(_work_position_base_sql())
        print("Creating internship position base in Snowflake...")
        cur.execute(_internship_base_sql())
        print("Creating graduate transition base in Snowflake...")
        cur.execute(_grad_transition_base_sql())
        print("Creating graduate transition benchmark in Snowflake...")
        cur.execute(_grad_transition_benchmark_sql())
    finally:
        cur.close()
        conn.close()

    queries = {
        "annual_salary": _work_annual_salary_query(),
        "annual_seniority": _work_seniority_query(),
        "annual_employers": _work_category_query("annual_employers", "employer", "employer"),
        "annual_roles": _work_category_query("annual_roles", "role_k50_v3", "role"),
        "annual_industries": _work_category_query("annual_industries", "industry_k50", "industry"),
        "annual_geography": _work_category_query("annual_geography", "location", "location"),
        "mobility": _work_mobility_query(),
        "employer_tenure": _work_employer_tenure_query(),
        "internship_summary": _internship_summary_query(),
        "internship_employers": _internship_category_query("employer", "employer"),
        "internship_roles": _internship_category_query("role", "role"),
        "internship_industries": _internship_category_query("industry", "industry"),
        "internship_geography": _internship_category_query("location", "location"),
        "grad_value_summary": _grad_value_summary_query(),
        "grad_role_transitions": _grad_transition_category_query("role"),
        "grad_industry_transitions": _grad_transition_category_query("industry"),
        "grad_value_benchmarks": f"SELECT * FROM {SCRATCH}.SCHOOL_OUTCOMES_GRAD_TRANSITION_BENCHMARK",
    }

    written = {}
    for fact_name, query in queries.items():
        print(f"  work_facts/{fact_name}...")
        info = _write_query_file_parts(sfClient, query, work_dir / fact_name)
        written[fact_name] = {
            "path": str((work_dir / fact_name).relative_to(platform_dir)),
            **info,
        }
        print(f"    {info['rows']:,} rows")
    return written


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

    base_query = f"SELECT * FROM {SCRATCH}.SCHOOL_OUTCOMES_PLATFORM_BASE"
    base_info = _write_query_file_parts(
        sfClient,
        base_query,
        platform_dir / "base_fact",
    )
    print(f"  base_fact: {base_info['rows']:,} rows")

    current_students_query = _current_students_sql()
    current_students_info = _write_query_file_parts(
        sfClient,
        current_students_query,
        platform_dir / "current_students_fact",
    )
    print(f"  current_students_fact: {current_students_info['rows']:,} rows")

    aggregate_facts = _copy_aggregate_facts(out_dir, platform_dir / "aggregate_facts")
    _write_postgrad_aggregate_facts(platform_dir / "aggregate_facts", aggregate_facts)
    print(f"  aggregate facts: {len(aggregate_facts):,}")

    work_facts = _write_work_history_facts(platform_dir)
    print(f"  work facts: {len(work_facts):,}")

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
            "notes": "Person-level API input with hashed person_key, IPEDS/profile calibration weights, final outcome weights, demographics, employer flags, postgrad filters, and early_2025 partial horizon.",
        },
        "current_students_fact": {
            "path": "current_students_fact",
            **current_students_info,
            "notes": "Projected classes 2026-2029 for major growth only; uses profile weights when available and has no earnings fields.",
        },
        "aggregate_facts": aggregate_facts,
        "work_facts": work_facts,
        "work_history": {
            "max_years_out": WORK_MAX_YEARS_OUT,
            "top_n_per_group": WORK_TOP_N_PER_GROUP,
            "grad_transition_windows": GRAD_TRANSITION_WINDOWS,
            "grad_benchmark_min_weight": GRAD_BENCHMARK_MIN_WEIGHT,
            "notes": "Annual career snapshots, tenure/mobility aggregates, and graduate before/after transition facts. Graduate lift is benchmarked against comparable bachelor-only career growth. No raw user_id or raw position-level timeline is exported.",
        },
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
            "years_after_degree",
            "grad_plus_one_masters_flag",
            "feeder_unitid",
            "pre_role",
            "post_role",
            "pre_industry",
            "post_industry",
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


platform_manifest = run_platform_parquet_export()
print(json.dumps({
    'version': platform_manifest['version'],
    'base_rows': platform_manifest['base_fact']['rows'],
    'current_student_rows': platform_manifest['current_students_fact']['rows'],
    'aggregate_fact_count': len(platform_manifest['aggregate_facts']),
    'work_fact_count': len(platform_manifest.get('work_facts', {})),
}, indent=2))
