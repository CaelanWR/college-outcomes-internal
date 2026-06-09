"""
Build optional labor-demand Parquet facts for the college outcomes platform.

Run this from the Snowflake precompute notebook after the platform export has
created SCRATCH.SCHOOL_OUTCOMES_PLATFORM_BASE. It does not require rerunning the
full school outcomes precompute.

Expected notebook globals:
  sfClient, OUT_DIR, SCRATCH

Optional globals:
  POSTINGS_DYNAMICS_TABLE, SKILL_DYNAMICS_TABLE, POSTINGS_DETAIL_TABLE
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


DEMAND_EXPORT_VERSION = "2026-06-09-demand-v1"
DEMAND_ROWS_PER_PART = 5000
DEMAND_RECENT_MONTHS = 6
DEMAND_PREVIOUS_MONTHS = 6
DEMAND_OUTCOME_START_YEAR = 2021
DEMAND_TOP_ROLES_PER_GROUP = 40
DEMAND_TOP_SKILLS_PER_GROUP = 40
DEMAND_TOP_SKILLS_PER_ROLE = 80
DEMAND_MIN_ROLE_ALUMNI_WEIGHT = 5
DEMAND_MIN_POSTING_ACTIVITY = 10
DEMAND_MIN_SKILL_ACTIVITY = 5

POSTINGS_DYNAMICS_TABLE = globals().get(
    "POSTINGS_DYNAMICS_TABLE",
    "CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_202605_POSTINGS_UNIFIED_DYNAMICS",
)
SKILL_DYNAMICS_TABLE = globals().get(
    "SKILL_DYNAMICS_TABLE",
    "CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_202605_SKILL_DYNAM",
)
POSTINGS_DETAIL_TABLE = globals().get("POSTINGS_DETAIL_TABLE")


def _normalize_demand_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    for col in df.columns:
        if df[col].dtype == "object":
            non_null = df[col].dropna()
            if len(non_null) and non_null.map(lambda v: isinstance(v, Decimal)).any():
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif len(non_null) and non_null.map(lambda v: isinstance(v, bytes)).any():
                df[col] = df[col].map(lambda v: v.decode("utf-8", errors="ignore") if isinstance(v, bytes) else v)
    for col in ["unitid", "degree", "horizon", "month", "country", "state"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    return df


def _clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _write_df_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(_normalize_demand_df(df), preserve_index=False)
    pq.write_table(table, path, compression="snappy")


def _write_query_file_parts(sf_client, query: str, root: Path) -> dict:
    _clean_dir(root)
    conn = sf_client.connect()
    cur = conn.cursor()
    row_count = 0
    batch_count = 0
    part_count = 0
    try:
        cur.execute(query)
        for batch in cur.fetch_pandas_batches():
            df = _normalize_demand_df(batch)
            if df.empty:
                continue
            batch_count += 1
            row_count += len(df)
            for start in range(0, len(df), DEMAND_ROWS_PER_PART):
                chunk = df.iloc[start:start + DEMAND_ROWS_PER_PART]
                if chunk.empty:
                    continue
                part_count += 1
                _write_df_parquet(chunk, root / f"part-{part_count:05d}.parquet")
    finally:
        cur.close()
        conn.close()
    return {
        "rows": int(row_count),
        "batches": int(batch_count),
        "parts": int(part_count),
        "rows_per_part": int(DEMAND_ROWS_PER_PART),
        "partition_cols": [],
    }


def _run_sql(sql: str) -> None:
    conn = sfClient.connect()
    cur = conn.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()
        conn.close()


def _demand_base_sql() -> str:
    recent = DEMAND_RECENT_MONTHS
    previous = DEMAND_PREVIOUS_MONTHS
    return f"""
CREATE OR REPLACE TABLE {SCRATCH}.SCHOOL_OUTCOMES_DEMAND_ROLE_BASE AS
WITH month_bounds AS (
    SELECT
        MAX(TO_DATE(month || '-01')) AS latest_month
    FROM {POSTINGS_DYNAMICS_TABLE}
    WHERE country = 'United States'
),
normalized AS (
    SELECT
        COALESCE(NULLIF(TRIM(country), ''), 'Unknown') AS country,
        COALESCE(NULLIF(TRIM(state), ''), 'Unknown') AS state,
        COALESCE(NULLIF(TRIM(role_k10), ''), 'Unknown') AS role_k10,
        COALESCE(NULLIF(TRIM(role_k50), ''), 'Unknown') AS role_k50,
        COALESCE(NULLIF(TRIM(role_k150), ''), 'Unknown') AS role_k150,
        TO_DATE(month || '-01') AS month_date,
        month,
        GREATEST(0, COALESCE(active_posting, 0)) AS active_posting,
        GREATEST(0, COALESCE(new_posting, 0)) AS new_posting,
        GREATEST(0, COALESCE(removed_posting, 0)) AS removed_posting,
        GREATEST(0, COALESCE(expected_hires, 0)) AS expected_hires,
        NULLIF(active_salary_avg, 0) AS active_salary_avg,
        NULLIF(new_salary_avg, 0) AS new_salary_avg,
        NULLIF(removed_salary_avg, 0) AS removed_salary_avg,
        NULLIF(filling_time_avg, 0) AS filling_time_avg
    FROM {POSTINGS_DYNAMICS_TABLE}
    WHERE country = 'United States'
      AND month IS NOT NULL
      AND COALESCE(NULLIF(TRIM(role_k50), ''), 'Unknown') NOT IN ('Unknown', 'unknown', 'empty')
),
role_month AS (
    SELECT
        n.country,
        n.state,
        n.role_k10,
        n.role_k50,
        n.role_k150,
        n.month,
        n.month_date,
        SUM(n.active_posting) AS active_postings,
        SUM(n.new_posting) AS new_postings,
        SUM(n.removed_posting) AS removed_postings,
        SUM(n.expected_hires) AS expected_hires,
        SUM(n.active_salary_avg * n.active_posting) / NULLIF(SUM(CASE WHEN n.active_salary_avg IS NOT NULL THEN n.active_posting ELSE 0 END), 0) AS active_salary_avg,
        SUM(n.new_salary_avg * n.new_posting) / NULLIF(SUM(CASE WHEN n.new_salary_avg IS NOT NULL THEN n.new_posting ELSE 0 END), 0) AS new_salary_avg,
        SUM(n.filling_time_avg * n.active_posting) / NULLIF(SUM(CASE WHEN n.filling_time_avg IS NOT NULL THEN n.active_posting ELSE 0 END), 0) AS filling_time_avg,
        MAX(b.latest_month) AS latest_month
    FROM normalized n
    CROSS JOIN month_bounds b
    GROUP BY n.country, n.state, n.role_k10, n.role_k50, n.role_k150, n.month, n.month_date
)
SELECT
    *,
    CASE
        WHEN month_date > DATEADD('month', -{recent}, latest_month) THEN 1
        ELSE 0
    END AS recent_window_flag,
    CASE
        WHEN month_date <= DATEADD('month', -{recent}, latest_month)
         AND month_date > DATEADD('month', -{recent + previous}, latest_month) THEN 1
        ELSE 0
    END AS previous_window_flag
FROM role_month
WHERE active_postings + new_postings + expected_hires >= {DEMAND_MIN_POSTING_ACTIVITY}
"""


def _school_role_fit_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE {SCRATCH}.SCHOOL_OUTCOMES_DEMAND_SCHOOL_ROLE_FIT AS
WITH role_rows AS (
    SELECT
        CAST(unitid AS VARCHAR) AS unitid,
        ANY_VALUE(school_name) AS school_name,
        degree,
        cip2,
        cip4,
        cip6,
        ANY_VALUE(major_title) AS major_title,
        horizon,
        role_k10_v3 AS role_k10,
        role_k50_v3 AS role_k50,
        role_k150_v3 AS role_k150,
        SUM(final_weight) AS alumni_weight,
        COUNT(DISTINCT person_key) AS observed_profiles
    FROM {SCRATCH}.SCHOOL_OUTCOMES_PLATFORM_BASE
    WHERE grad_year >= {DEMAND_OUTCOME_START_YEAR}
      AND role_k50_v3 IS NOT NULL
      AND TRIM(role_k50_v3) <> ''
      AND LOWER(TRIM(role_k50_v3)) NOT IN ('unknown', 'empty', 'other')
      AND final_weight > 0
    GROUP BY
        CAST(unitid AS VARCHAR), degree, cip2, cip4, cip6, horizon,
        role_k10_v3, role_k50_v3, role_k150_v3
),
totals AS (
    SELECT
        unitid,
        degree,
        cip4,
        horizon,
        SUM(alumni_weight) AS total_alumni_weight
    FROM role_rows
    GROUP BY unitid, degree, cip4, horizon
),
ranked AS (
    SELECT
        r.*,
        t.total_alumni_weight,
        100.0 * r.alumni_weight / NULLIF(t.total_alumni_weight, 0) AS alumni_role_share_pct,
        ROW_NUMBER() OVER (
            PARTITION BY r.unitid, r.degree, r.cip4, r.horizon
            ORDER BY r.alumni_weight DESC, r.role_k50, r.role_k150
        ) AS role_rank
    FROM role_rows r
    JOIN totals t
      ON r.unitid = t.unitid
     AND r.degree = t.degree
     AND r.cip4 = t.cip4
     AND r.horizon = t.horizon
    WHERE r.alumni_weight >= {DEMAND_MIN_ROLE_ALUMNI_WEIGHT}
)
SELECT *
FROM ranked
WHERE role_rank <= {DEMAND_TOP_ROLES_PER_GROUP}
"""


def _role_demand_summary_query() -> str:
    return f"""
WITH summary AS (
    SELECT
        role_k10,
        role_k50,
        role_k150,
        SUM(CASE WHEN recent_window_flag = 1 THEN active_postings ELSE 0 END) AS active_postings_recent,
        SUM(CASE WHEN recent_window_flag = 1 THEN new_postings ELSE 0 END) AS new_postings_recent,
        SUM(CASE WHEN recent_window_flag = 1 THEN expected_hires ELSE 0 END) AS expected_hires_recent,
        SUM(CASE WHEN previous_window_flag = 1 THEN active_postings ELSE 0 END) AS active_postings_previous,
        SUM(CASE WHEN previous_window_flag = 1 THEN new_postings ELSE 0 END) AS new_postings_previous,
        SUM(CASE WHEN previous_window_flag = 1 THEN expected_hires ELSE 0 END) AS expected_hires_previous,
        SUM(CASE WHEN recent_window_flag = 1 THEN active_salary_avg * active_postings ELSE 0 END)
          / NULLIF(SUM(CASE WHEN recent_window_flag = 1 AND active_salary_avg IS NOT NULL THEN active_postings ELSE 0 END), 0) AS active_salary_avg_recent,
        SUM(CASE WHEN recent_window_flag = 1 THEN filling_time_avg * active_postings ELSE 0 END)
          / NULLIF(SUM(CASE WHEN recent_window_flag = 1 AND filling_time_avg IS NOT NULL THEN active_postings ELSE 0 END), 0) AS filling_time_avg_recent,
        MAX(latest_month) AS latest_month
    FROM {SCRATCH}.SCHOOL_OUTCOMES_DEMAND_ROLE_BASE
    GROUP BY role_k10, role_k50, role_k150
)
SELECT
    *,
    active_postings_recent - active_postings_previous AS active_postings_change,
    100.0 * (active_postings_recent - active_postings_previous) / NULLIF(active_postings_previous, 0) AS active_postings_growth_pct,
    new_postings_recent - new_postings_previous AS new_postings_change,
    expected_hires_recent - expected_hires_previous AS expected_hires_change,
    LN(1 + GREATEST(0, expected_hires_recent))
      + LN(1 + GREATEST(0, new_postings_recent))
      + LEAST(2.0, GREATEST(-1.0, COALESCE((active_postings_recent - active_postings_previous) / NULLIF(active_postings_previous, 0), 0))) AS role_demand_score
FROM summary
WHERE active_postings_recent + new_postings_recent + expected_hires_recent >= {DEMAND_MIN_POSTING_ACTIVITY}
"""


def _role_month_query() -> str:
    return f"""
SELECT
    country,
    state,
    role_k10,
    role_k50,
    role_k150,
    month,
    active_postings,
    new_postings,
    removed_postings,
    expected_hires,
    active_salary_avg,
    new_salary_avg,
    filling_time_avg,
    recent_window_flag,
    previous_window_flag
FROM {SCRATCH}.SCHOOL_OUTCOMES_DEMAND_ROLE_BASE
"""


def _school_major_role_demand_query() -> str:
    role_demand = _role_demand_summary_query()
    return f"""
WITH demand AS ({role_demand}),
joined AS (
    SELECT
        f.unitid,
        f.school_name,
        f.degree,
        f.cip2,
        f.cip4,
        f.cip6,
        f.major_title,
        f.horizon,
        f.role_k10,
        f.role_k50,
        f.role_k150,
        f.role_rank,
        f.alumni_weight,
        f.observed_profiles,
        f.total_alumni_weight,
        f.alumni_role_share_pct,
        d.active_postings_recent,
        d.new_postings_recent,
        d.expected_hires_recent,
        d.active_postings_previous,
        d.active_postings_change,
        d.active_postings_growth_pct,
        d.active_salary_avg_recent,
        d.filling_time_avg_recent,
        d.role_demand_score,
        f.alumni_role_share_pct * d.role_demand_score AS school_major_role_opportunity_score
    FROM {SCRATCH}.SCHOOL_OUTCOMES_DEMAND_SCHOOL_ROLE_FIT f
    JOIN demand d
      ON f.role_k50 = d.role_k50
     AND COALESCE(f.role_k150, f.role_k50) = COALESCE(d.role_k150, d.role_k50)
)
SELECT *
FROM joined
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY unitid, degree, cip4, horizon
    ORDER BY school_major_role_opportunity_score DESC, alumni_weight DESC
) <= {DEMAND_TOP_ROLES_PER_GROUP}
"""


def _skill_base_sql() -> str:
    recent = DEMAND_RECENT_MONTHS
    previous = DEMAND_PREVIOUS_MONTHS
    return f"""
CREATE OR REPLACE TABLE {SCRATCH}.SCHOOL_OUTCOMES_DEMAND_SKILL_BASE AS
WITH month_bounds AS (
    SELECT MAX(TO_DATE(month || '-01')) AS latest_month
    FROM {SKILL_DYNAMICS_TABLE}
),
normalized AS (
    SELECT
        COALESCE(NULLIF(TRIM(role_k10), ''), 'Unknown') AS role_k10,
        COALESCE(NULLIF(TRIM(skill), ''), 'Unknown') AS skill,
        COALESCE(NULLIF(TRIM(region), ''), 'Unknown') AS region,
        seniority,
        TO_DATE(month || '-01') AS month_date,
        month,
        GREATEST(0, COALESCE(count, 0)) AS skill_count,
        GREATEST(0, COALESCE(inflow, 0)) AS skill_inflow,
        GREATEST(0, COALESCE(outflow, 0)) AS skill_outflow,
        GREATEST(0, COALESCE(external_inflow, 0)) AS external_skill_inflow,
        GREATEST(0, COALESCE(external_outflow, 0)) AS external_skill_outflow,
        GREATEST(0, COALESCE(scaled_count, count, 0)) AS scaled_skill_count,
        GREATEST(0, COALESCE(scaled_inflow, inflow, 0)) AS scaled_skill_inflow,
        GREATEST(0, COALESCE(scaled_outflow, outflow, 0)) AS scaled_skill_outflow,
        GREATEST(0, COALESCE(scaled_external_inflow, external_inflow, 0)) AS scaled_external_skill_inflow,
        GREATEST(0, COALESCE(scaled_external_outflow, external_outflow, 0)) AS scaled_external_skill_outflow
    FROM {SKILL_DYNAMICS_TABLE}
    WHERE month IS NOT NULL
      AND COALESCE(NULLIF(TRIM(role_k10), ''), 'Unknown') NOT IN ('Unknown', 'unknown', 'empty')
      AND COALESCE(NULLIF(TRIM(skill), ''), 'Unknown') NOT IN ('Unknown', 'unknown', 'empty')
),
skill_month AS (
    SELECT
        n.role_k10,
        n.skill,
        n.month,
        n.month_date,
        SUM(n.skill_count) AS skill_count,
        SUM(n.skill_inflow) AS skill_inflow,
        SUM(n.skill_outflow) AS skill_outflow,
        SUM(n.external_skill_inflow) AS external_skill_inflow,
        SUM(n.external_skill_outflow) AS external_skill_outflow,
        SUM(n.scaled_skill_count) AS scaled_skill_count,
        SUM(n.scaled_skill_inflow) AS scaled_skill_inflow,
        SUM(n.scaled_skill_outflow) AS scaled_skill_outflow,
        SUM(n.scaled_external_skill_inflow) AS scaled_external_skill_inflow,
        SUM(n.scaled_external_skill_outflow) AS scaled_external_skill_outflow,
        MAX(b.latest_month) AS latest_month
    FROM normalized n
    CROSS JOIN month_bounds b
    GROUP BY n.role_k10, n.skill, n.month, n.month_date
)
SELECT
    *,
    CASE
        WHEN month_date > DATEADD('month', -{recent}, latest_month) THEN 1
        ELSE 0
    END AS recent_window_flag,
    CASE
        WHEN month_date <= DATEADD('month', -{recent}, latest_month)
         AND month_date > DATEADD('month', -{recent + previous}, latest_month) THEN 1
        ELSE 0
    END AS previous_window_flag
FROM skill_month
WHERE scaled_skill_count + scaled_skill_inflow >= {DEMAND_MIN_SKILL_ACTIVITY}
"""


def _role_skill_demand_query() -> str:
    return f"""
WITH summary AS (
    SELECT
        role_k10,
        skill,
        SUM(CASE WHEN recent_window_flag = 1 THEN scaled_skill_count ELSE 0 END) AS scaled_skill_count_recent,
        SUM(CASE WHEN recent_window_flag = 1 THEN scaled_skill_inflow ELSE 0 END) AS scaled_skill_inflow_recent,
        SUM(CASE WHEN recent_window_flag = 1 THEN scaled_external_skill_inflow ELSE 0 END) AS scaled_external_skill_inflow_recent,
        SUM(CASE WHEN previous_window_flag = 1 THEN scaled_skill_count ELSE 0 END) AS scaled_skill_count_previous,
        SUM(CASE WHEN previous_window_flag = 1 THEN scaled_skill_inflow ELSE 0 END) AS scaled_skill_inflow_previous
    FROM {SCRATCH}.SCHOOL_OUTCOMES_DEMAND_SKILL_BASE
    GROUP BY role_k10, skill
),
scored AS (
    SELECT
        *,
        scaled_skill_count_recent - scaled_skill_count_previous AS scaled_skill_count_change,
        100.0 * (scaled_skill_count_recent - scaled_skill_count_previous) / NULLIF(scaled_skill_count_previous, 0) AS scaled_skill_count_growth_pct,
        LN(1 + GREATEST(0, scaled_skill_count_recent))
          + LN(1 + GREATEST(0, scaled_skill_inflow_recent))
          + LEAST(2.0, GREATEST(-1.0, COALESCE((scaled_skill_count_recent - scaled_skill_count_previous) / NULLIF(scaled_skill_count_previous, 0), 0))) AS skill_demand_score
    FROM summary
    WHERE scaled_skill_count_recent + scaled_skill_inflow_recent >= {DEMAND_MIN_SKILL_ACTIVITY}
)
SELECT *
FROM scored
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY role_k10
    ORDER BY skill_demand_score DESC, scaled_skill_count_recent DESC
) <= {DEMAND_TOP_SKILLS_PER_ROLE}
"""


def _school_major_skill_demand_query() -> str:
    role_skill = _role_skill_demand_query()
    return f"""
WITH role_skill AS ({role_skill}),
joined AS (
    SELECT
        f.unitid,
        f.school_name,
        f.degree,
        f.cip2,
        f.cip4,
        f.cip6,
        f.major_title,
        f.horizon,
        rs.skill,
        SUM(f.alumni_role_share_pct * rs.skill_demand_score) AS school_major_skill_opportunity_score,
        SUM(f.alumni_weight) AS supporting_role_alumni_weight,
        SUM(f.observed_profiles) AS supporting_role_observed_profiles,
        MAX(rs.scaled_skill_count_recent) AS scaled_skill_count_recent,
        MAX(rs.scaled_skill_inflow_recent) AS scaled_skill_inflow_recent,
        MAX(rs.scaled_external_skill_inflow_recent) AS scaled_external_skill_inflow_recent,
        MAX(rs.scaled_skill_count_growth_pct) AS scaled_skill_count_growth_pct
    FROM {SCRATCH}.SCHOOL_OUTCOMES_DEMAND_SCHOOL_ROLE_FIT f
    JOIN role_skill rs
      ON f.role_k10 = rs.role_k10
    WHERE f.role_k10 IS NOT NULL
      AND TRIM(f.role_k10) <> ''
    GROUP BY
        f.unitid, f.school_name, f.degree, f.cip2, f.cip4, f.cip6,
        f.major_title, f.horizon, rs.skill
)
SELECT *
FROM joined
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY unitid, degree, cip4, horizon
    ORDER BY school_major_skill_opportunity_score DESC, scaled_skill_count_recent DESC
) <= {DEMAND_TOP_SKILLS_PER_GROUP}
"""


def _posting_degree_demand_query() -> str:
    if not POSTINGS_DETAIL_TABLE:
        raise ValueError("POSTINGS_DETAIL_TABLE is required for degree-demand export")
    recent = DEMAND_RECENT_MONTHS
    previous = DEMAND_PREVIOUS_MONTHS
    return f"""
WITH month_bounds AS (
    SELECT MAX(DATE_TRUNC('month', TO_DATE(post_date))) AS latest_month
    FROM {POSTINGS_DETAIL_TABLE}
    WHERE country = 'United States'
      AND post_date IS NOT NULL
),
normalized AS (
    SELECT
        COALESCE(NULLIF(TRIM(role_k10), ''), 'Unknown') AS role_k10,
        COALESCE(NULLIF(TRIM(role_k50), ''), 'Unknown') AS role_k50,
        COALESCE(NULLIF(TRIM(role_k150), ''), 'Unknown') AS role_k150,
        COALESCE(NULLIF(TRIM(required_degree), ''), 'No degree specified') AS required_degree,
        DATE_TRUNC('month', TO_DATE(post_date)) AS month_date,
        GREATEST(0, COALESCE(expected_hires, 1)) AS expected_hires
    FROM {POSTINGS_DETAIL_TABLE}
    WHERE country = 'United States'
      AND post_date IS NOT NULL
      AND COALESCE(NULLIF(TRIM(role_k50), ''), 'Unknown') NOT IN ('Unknown', 'unknown', 'empty')
),
windowed AS (
    SELECT
        n.*,
        b.latest_month,
        CASE
            WHEN n.month_date > DATEADD('month', -{recent}, b.latest_month) THEN 1
            ELSE 0
        END AS recent_window_flag,
        CASE
            WHEN n.month_date <= DATEADD('month', -{recent}, b.latest_month)
             AND n.month_date > DATEADD('month', -{recent + previous}, b.latest_month) THEN 1
            ELSE 0
        END AS previous_window_flag
    FROM normalized n
    CROSS JOIN month_bounds b
),
summary AS (
    SELECT
        role_k10,
        role_k50,
        role_k150,
        required_degree,
        COUNT_IF(recent_window_flag = 1) AS postings_recent,
        COUNT_IF(previous_window_flag = 1) AS postings_previous,
        SUM(CASE WHEN recent_window_flag = 1 THEN expected_hires ELSE 0 END) AS expected_hires_recent,
        SUM(CASE WHEN previous_window_flag = 1 THEN expected_hires ELSE 0 END) AS expected_hires_previous,
        MAX(latest_month) AS latest_month
    FROM windowed
    GROUP BY role_k10, role_k50, role_k150, required_degree
)
SELECT
    *,
    postings_recent - postings_previous AS postings_change,
    100.0 * (postings_recent - postings_previous) / NULLIF(postings_previous, 0) AS postings_growth_pct,
    LN(1 + GREATEST(0, expected_hires_recent))
      + LN(1 + GREATEST(0, postings_recent))
      + LEAST(2.0, GREATEST(-1.0, COALESCE((postings_recent - postings_previous) / NULLIF(postings_previous, 0), 0))) AS degree_demand_score
FROM summary
WHERE postings_recent + expected_hires_recent >= {DEMAND_MIN_POSTING_ACTIVITY}
"""


def _school_major_degree_demand_query() -> str:
    degree_demand = _posting_degree_demand_query()
    return f"""
WITH degree_demand AS ({degree_demand}),
joined AS (
    SELECT
        f.unitid,
        f.school_name,
        f.degree,
        f.cip2,
        f.cip4,
        f.cip6,
        f.major_title,
        f.horizon,
        dd.required_degree,
        SUM(f.alumni_role_share_pct * dd.degree_demand_score) AS school_major_degree_demand_score,
        SUM(f.alumni_weight) AS supporting_role_alumni_weight,
        SUM(f.observed_profiles) AS supporting_role_observed_profiles,
        SUM(dd.postings_recent) AS postings_recent,
        SUM(dd.expected_hires_recent) AS expected_hires_recent,
        MAX(dd.postings_growth_pct) AS max_postings_growth_pct
    FROM {SCRATCH}.SCHOOL_OUTCOMES_DEMAND_SCHOOL_ROLE_FIT f
    JOIN degree_demand dd
      ON f.role_k50 = dd.role_k50
     AND COALESCE(f.role_k150, f.role_k50) = COALESCE(dd.role_k150, dd.role_k50)
    GROUP BY
        f.unitid, f.school_name, f.degree, f.cip2, f.cip4, f.cip6,
        f.major_title, f.horizon, dd.required_degree
)
SELECT *
FROM joined
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY unitid, degree, cip4, horizon
    ORDER BY school_major_degree_demand_score DESC, postings_recent DESC
) <= 12
"""


def run_demand_parquet_export(platform_out_dir: Optional[Path] = None) -> dict:
    out_dir = Path(OUT_DIR)
    platform_dir = Path(platform_out_dir) if platform_out_dir else out_dir / "platform_parquet"
    demand_dir = platform_dir / "demand_facts"
    demand_dir.mkdir(parents=True, exist_ok=True)

    print(f"Building demand export: {demand_dir}")
    print(f"  postings dynamics table: {POSTINGS_DYNAMICS_TABLE}")
    print(f"  skill dynamics table: {SKILL_DYNAMICS_TABLE}")
    if POSTINGS_DETAIL_TABLE:
        print(f"  postings detail table: {POSTINGS_DETAIL_TABLE}")
    else:
        print("  postings detail table: not set; skipping degree-demand facts")

    print("Creating role demand base in Snowflake...")
    _run_sql(_demand_base_sql())
    print("Creating school-major role fit in Snowflake...")
    _run_sql(_school_role_fit_sql())
    print("Creating skill demand base in Snowflake...")
    _run_sql(_skill_base_sql())

    queries = {
        "posting_role_month": _role_month_query(),
        "posting_role_summary": _role_demand_summary_query(),
        "school_major_role_demand": _school_major_role_demand_query(),
        "role_skill_demand": _role_skill_demand_query(),
        "school_major_skill_demand": _school_major_skill_demand_query(),
    }
    if POSTINGS_DETAIL_TABLE:
        queries["posting_degree_demand"] = _posting_degree_demand_query()
        queries["school_major_degree_demand"] = _school_major_degree_demand_query()

    written = {}
    for fact_name, query in queries.items():
        print(f"  demand_facts/{fact_name}...")
        info = _write_query_file_parts(sfClient, query, demand_dir / fact_name)
        written[fact_name] = {
            "path": str((demand_dir / fact_name).relative_to(platform_dir)),
            **info,
        }
        print(f"    {info['rows']:,} rows")

    manifest_path = platform_dir / "platform_manifest.json"
    manifest = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception:
            manifest = {}
    manifest["demand_facts"] = written
    manifest["labor_demand"] = {
        "version": DEMAND_EXPORT_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "postings_dynamics_table": POSTINGS_DYNAMICS_TABLE,
        "skill_dynamics_table": SKILL_DYNAMICS_TABLE,
        "postings_detail_table": POSTINGS_DETAIL_TABLE,
        "recent_months": DEMAND_RECENT_MONTHS,
        "previous_months": DEMAND_PREVIOUS_MONTHS,
        "outcome_start_year": DEMAND_OUTCOME_START_YEAR,
        "top_roles_per_group": DEMAND_TOP_ROLES_PER_GROUP,
        "top_skills_per_group": DEMAND_TOP_SKILLS_PER_GROUP,
        "notes": "Demand facts are aggregated from postings dynamics and skill dynamics. School/major opportunity uses alumni role fit from SCHOOL_OUTCOMES_PLATFORM_BASE joined to demand by Revelio role taxonomy. No raw posting descriptions, URLs, or person-level records are exported.",
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
    print(f"Wrote {manifest_path}")
    return manifest


demand_manifest = run_demand_parquet_export()
print(json.dumps({
    "demand_fact_count": len(demand_manifest.get("demand_facts", {})),
    "demand_version": demand_manifest.get("labor_demand", {}).get("version"),
}, indent=2))
