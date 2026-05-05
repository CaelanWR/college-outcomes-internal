from __future__ import annotations

import hmac
import json
import math
import os
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


DATA_ROOT = Path(os.environ.get("OUTCOMES_PARQUET_ROOT", "./data/parquet")).expanduser()
PLATFORM_ROOT = Path(os.environ.get("OUTCOMES_PLATFORM_ROOT", "")).expanduser() if os.environ.get("OUTCOMES_PLATFORM_ROOT") else None
SUPPRESSION_THRESHOLD = int(os.environ.get("SUPPRESSION_THRESHOLD", "25"))
TREND_SUPPRESSION_THRESHOLD = int(os.environ.get("TREND_SUPPRESSION_THRESHOLD", "5"))
APP_PASSWORD = os.environ.get("OUTCOMES_APP_PASSWORD")
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.environ.get("OUTCOMES_ALLOWED_ORIGINS", "").split(",")
    if origin.strip()
]

DOCTORATE_DEGREES = ["Research Doctorate", "Professional Doctorate", "Other Doctorate"]
DEGREE_ALIASES = {
    "All Doctorates": DOCTORATE_DEGREES,
    "Doctorate": DOCTORATE_DEGREES,
    "PhD": ["Research Doctorate"],
    "Research Doctorate": ["Research Doctorate"],
    "Professional Doctorate": ["Professional Doctorate"],
    "Other Doctorate": ["Other Doctorate"],
}
CIP_COLUMNS = {"cip2", "cip4", "cip6"}
HORIZON_ORDER = {"1yr": 1, "5yr": 5, "10yr": 10, "early_2025": 0}
SAME_SCHOOL_EMPLOYER_FILTER = """
AND NOT (
  same_school_employer_flag = 1
  OR (unitid IN ('190150', '196468') AND LOWER(employer) LIKE '%columbia university%')
  OR (unitid = '189097' AND LOWER(employer) LIKE '%barnard%')
  OR (unitid = '110635' AND (LOWER(employer) LIKE '%berkeley%' OR LOWER(employer) LIKE '%university of california%'))
  OR (unitid = '110662' AND (LOWER(employer) LIKE '%ucla%' OR LOWER(employer) LIKE '%university of california%'))
  OR (unitid = '170976' AND LOWER(employer) LIKE '%university of michigan%')
  OR (unitid = '217156' AND LOWER(employer) LIKE '%brown university%')
  OR (unitid = '243744' AND LOWER(employer) LIKE '%stanford university%')
  OR (unitid = '166683' AND (LOWER(employer) LIKE '%massachusetts institute of technology%' OR LOWER(employer) LIKE '%mit%'))
  OR (unitid = '166027' AND LOWER(employer) LIKE '%harvard%')
  OR (unitid = '130794' AND LOWER(employer) LIKE '%yale%')
  OR (unitid = '186131' AND LOWER(employer) LIKE '%princeton%')
  OR (unitid = '215062' AND LOWER(employer) LIKE '%university of pennsylvania%')
  OR (unitid = '190415' AND LOWER(employer) LIKE '%cornell%')
  OR (unitid = '198419' AND LOWER(employer) LIKE '%duke university%')
  OR (unitid = '147767' AND LOWER(employer) LIKE '%northwestern university%')
  OR (unitid = '144050' AND LOWER(employer) LIKE '%university of chicago%')
  OR (unitid = '193900' AND (LOWER(employer) LIKE '%new york university%' OR LOWER(employer) LIKE '%nyu%'))
  OR (unitid = '123961' AND (LOWER(employer) LIKE '%university of southern california%' OR LOWER(employer) LIKE '%usc%'))
  OR (unitid = '236948' AND LOWER(employer) LIKE '%university of washington%')
  OR (unitid = '234076' AND LOWER(employer) LIKE '%university of virginia%')
  OR (unitid = '199120' AND (LOWER(employer) LIKE '%university of north carolina%' OR LOWER(employer) LIKE '%unc%'))
  OR (unitid = '139755' AND (LOWER(employer) LIKE '%georgia institute of technology%' OR LOWER(employer) LIKE '%georgia tech%'))
  OR (unitid = '211440' AND LOWER(employer) LIKE '%carnegie mellon%')
)
"""

app = FastAPI(title="College Outcomes API")

if ALLOWED_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )


class DemographicFilters(BaseModel):
    gender: Optional[str] = None
    race_ethnicity: Optional[str] = None


class PostgradFilters(BaseModel):
    later_degree_type: Optional[str] = None
    no_further_education: Optional[bool] = None


class QueryRequest(BaseModel):
    schools: list[str] = Field(default_factory=list)
    degree: str = "Bachelors"
    cip_level: str = "cip4"
    majors: list[str] = Field(default_factory=list)
    grad_years: list[int] = Field(default_factory=list)
    horizon: str = "1yr"
    demographics: DemographicFilters = Field(default_factory=DemographicFilters)
    postgrad: PostgradFilters = Field(default_factory=PostgradFilters)
    include_current_students: bool = False
    compare_mode: bool = False
    selected_employer: Optional[str] = None
    selected_postgrad_degree: Optional[str] = None
    selected_postgrad_school: Optional[str] = None
    selected_postgrad_program: Optional[str] = None
    top_n: int = 12


def require_internal_password(x_outcomes_password: Optional[str] = Header(default=None)) -> None:
    """Fallback API password guard.

    Prefer SSO or network-level access control in production. This protects the
    data API when a stronger gate is not available.
    """
    if not APP_PASSWORD:
        return
    if not x_outcomes_password or not hmac.compare_digest(x_outcomes_password, APP_PASSWORD):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _platform_root() -> Path:
    if PLATFORM_ROOT:
        return PLATFORM_ROOT
    if DATA_ROOT.name == "base_fact":
        return DATA_ROOT.parent
    return DATA_ROOT


def _dataset_glob(dataset: str) -> str:
    root = _platform_root()
    if DATA_ROOT.name == dataset:
        path = DATA_ROOT
    else:
        path = root / dataset
    return str(path / "**" / "*.parquet")


@lru_cache(maxsize=8)
def _dataset_columns(dataset: str) -> frozenset[str]:
    con = _connect()
    try:
        rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [_dataset_glob(dataset)]).fetchall()
        return frozenset(str(row[0]).lower() for row in rows)
    finally:
        con.close()


def _profile_weight_sql(columns: frozenset[str]) -> str:
    candidates = [
        "profile_weight",
        "ipeds_calibration_weight",
        "education_weight",
        "individual_weight",
        "representation_weight",
        "universe_weight",
        "final_weight",
    ]
    available = [column for column in candidates if column in columns]
    if not available:
        return "1.0"
    return f"GREATEST(0.0, COALESCE({', '.join(available)}, 1.0))"


def _position_profile_weight_sql(columns: frozenset[str]) -> str:
    candidates = [
        "position_weight",
        "profile_weight",
        "education_weight",
        "individual_weight",
        "representation_weight",
        "universe_weight",
        "final_weight",
    ]
    available = [column for column in candidates if column in columns]
    if not available:
        return "1.0"
    return f"GREATEST(0.0, COALESCE({', '.join(available)}, 1.0))"


def _cohort_profile_weight_sql(columns: frozenset[str]) -> str:
    calibrated_weight = _profile_weight_sql(columns)
    position_weight = _position_profile_weight_sql(columns)
    has_recent_calibration_fields = {"degree", "grad_year", "calibration_ipeds_completions"}.issubset(columns)
    if not has_recent_calibration_fields:
        return calibrated_weight
    return f"""
      CASE
        WHEN degree = 'Bachelors'
          AND grad_year >= 2023
          AND calibration_ipeds_completions IS NULL
        THEN {position_weight}
        ELSE {calibrated_weight}
      END
    """


def _source_star_without_profile(columns: frozenset[str]) -> str:
    return "* EXCLUDE (profile_weight)" if "profile_weight" in columns else "*"


def _manifest() -> dict[str, Any]:
    path = _platform_root() / "platform_manifest.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute("SET enable_progress_bar=false")
    con.execute("SET threads=4")
    return con


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    try:
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
    except TypeError:
        pass
    return value


def _records_from_query(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    cols = [desc[0] for desc in result.description]
    return [{col: _json_safe(value) for col, value in zip(cols, row)} for row in result.fetchall()]


def _single_record(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> dict[str, Any]:
    rows = _records_from_query(con, sql, params)
    return rows[0] if rows else {}


def _cip_col(filters: QueryRequest) -> str:
    if filters.cip_level not in CIP_COLUMNS:
        raise HTTPException(status_code=400, detail="cip_level must be cip2, cip4, or cip6")
    return filters.cip_level


def _degree_values(degree: str) -> list[str]:
    if not degree or degree == "All":
        return []
    return DEGREE_ALIASES.get(degree, [degree])


def _append_in_clause(clauses: list[str], params: list[Any], column: str, values: list[Any]) -> None:
    if not values:
        return
    clauses.append(f"{column} IN (" + ",".join(["?"] * len(values)) + ")")
    params.extend(values)


def _where(filters: QueryRequest, *, include_horizon: bool = True, include_postgrad: bool = True) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    _append_in_clause(clauses, params, "unitid", filters.schools)
    _append_in_clause(clauses, params, "degree", _degree_values(filters.degree))
    _append_in_clause(clauses, params, _cip_col(filters), filters.majors)
    _append_in_clause(clauses, params, "grad_year", filters.grad_years)

    if include_horizon and filters.horizon:
        clauses.append("horizon = ?")
        params.append(filters.horizon)

    if filters.demographics.gender:
        clauses.append("gender = ?")
        params.append(filters.demographics.gender)
    if filters.demographics.race_ethnicity:
        clauses.append("race_ethnicity = ?")
        params.append(filters.demographics.race_ethnicity)

    if include_postgrad:
        if filters.postgrad.later_degree_type:
            clauses.append("later_degree_type = ?")
            params.append(filters.postgrad.later_degree_type)
        elif filters.postgrad.no_further_education is not None:
            clauses.append("no_further_education_flag = ?")
            params.append(1 if filters.postgrad.no_further_education else 0)

    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params


def _safe_limit(value: int) -> int:
    return min(max(value, 5), 30)


def _create_slice(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> None:
    base_columns = _dataset_columns("base_fact")
    profile_weight_sql = _cohort_profile_weight_sql(base_columns)
    source_star = _source_star_without_profile(base_columns)
    where_sql, params = _where(filters)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE slice AS
        SELECT *
        FROM read_parquet(?)
        {where_sql}
        """,
        [_dataset_glob("base_fact"), *params],
    )
    cohort_where_sql, cohort_params = _where(filters, include_horizon=False)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE cohort_slice AS
        SELECT *
        FROM (
          SELECT
            {source_star},
            {profile_weight_sql} AS profile_weight,
            ROW_NUMBER() OVER (
              PARTITION BY person_key, unitid, degree, cip2, cip4, cip6, grad_year
              ORDER BY CASE horizon
                WHEN '1yr' THEN 1
                WHEN '5yr' THEN 2
                WHEN '10yr' THEN 3
                WHEN 'early_2025' THEN 4
                ELSE 5
              END
            ) AS cohort_rank
          FROM read_parquet(?)
          {cohort_where_sql}
        )
        WHERE cohort_rank = 1
        """,
        [_dataset_glob("base_fact"), *cohort_params],
    )


def _create_current_slice(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> bool:
    current_path = _platform_root() / "current_students_fact"
    if not current_path.exists():
        return False
    current_columns = _dataset_columns("current_students_fact")
    profile_weight_sql = _profile_weight_sql(current_columns)
    source_star = _source_star_without_profile(current_columns)
    current_filters = filters.model_copy(deep=True)
    current_filters.grad_years = []
    where_sql, params = _where(current_filters, include_horizon=False, include_postgrad=False)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE current_slice AS
        SELECT {source_star}, {profile_weight_sql} AS profile_weight
        FROM read_parquet(?)
        {where_sql}
        """,
        [_dataset_glob("current_students_fact"), *params],
    )
    return True


@lru_cache(maxsize=1)
def _static_options() -> dict[str, Any]:
    con = _connect()
    try:
        base_columns = _dataset_columns("base_fact")
        profile_weight_sql = _cohort_profile_weight_sql(base_columns)
        source_star = _source_star_without_profile(base_columns)
        con.execute(
            f"""
            CREATE TEMP TABLE static_cohort AS
            SELECT *
            FROM (
              SELECT
                {source_star},
                {profile_weight_sql} AS profile_weight,
                ROW_NUMBER() OVER (
                  PARTITION BY person_key, unitid, degree, cip2, cip4, cip6, grad_year
                  ORDER BY CASE horizon
                    WHEN '1yr' THEN 1
                    WHEN '5yr' THEN 2
                    WHEN '10yr' THEN 3
                    WHEN 'early_2025' THEN 4
                    ELSE 5
                  END
                ) AS cohort_rank
              FROM read_parquet(?)
            )
            WHERE cohort_rank = 1
            """,
            [_dataset_glob("base_fact")],
        )
        schools = _records_from_query(
            con,
            """
            SELECT unitid, MAX(school_name) AS name, ROUND(SUM(profile_weight)) AS alumni
            FROM static_cohort
            GROUP BY unitid
            ORDER BY name
            """,
        )
        degree_rows = _records_from_query(
            con,
            """
            SELECT degree, ROUND(SUM(profile_weight)) AS alumni
            FROM static_cohort
            GROUP BY degree
            ORDER BY alumni DESC
            """,
        )
        grad_years = [
            int(row["grad_year"])
            for row in _records_from_query(
                con,
                """
                SELECT DISTINCT grad_year
                FROM read_parquet(?)
                WHERE grad_year IS NOT NULL
                ORDER BY grad_year
                """,
                [_dataset_glob("base_fact")],
            )
        ]
        current_years = []
        if (_platform_root() / "current_students_fact").exists():
            current_years = [
                int(row["grad_year"])
                for row in _records_from_query(
                    con,
                    """
                    SELECT DISTINCT grad_year
                    FROM read_parquet(?)
                    WHERE grad_year IS NOT NULL
                    ORDER BY grad_year
                    """,
                    [_dataset_glob("current_students_fact")],
                )
            ]
        return {
            "schools": schools,
            "degrees": [{"degree": "All", "label": "All"}]
            + [{"degree": row["degree"], "label": row["degree"]} for row in degree_rows]
            + [{"degree": "All Doctorates", "label": "All Doctorates"}],
            "horizons": [
                {"value": "1yr", "label": "1 year out"},
                {"value": "5yr", "label": "5 years out"},
                {"value": "10yr", "label": "10 years out"},
                {"value": "early_2025", "label": "2025 early earnings"},
            ],
            "grad_years": grad_years,
            "current_student_years": current_years,
        }
    finally:
        con.close()


@app.get("/api/health")
def health() -> dict[str, Any]:
    manifest = _manifest()
    return {
        "status": "ok",
        "data_version": manifest.get("version"),
        "base_fact_exists": (_platform_root() / "base_fact").exists() or DATA_ROOT.exists(),
    }


@app.post("/api/options")
def options(filters: QueryRequest, _: None = Depends(require_internal_password)) -> dict[str, Any]:
    cip_col = _cip_col(filters)
    limit = 500
    con = _connect()
    try:
        base_columns = _dataset_columns("base_fact")
        profile_weight_sql = _cohort_profile_weight_sql(base_columns)
        source_star = _source_star_without_profile(base_columns)
        where_sql, params = _where(filters, include_horizon=False, include_postgrad=False)
        con.execute(
            f"""
            CREATE TEMP TABLE option_cohort AS
            SELECT *
            FROM (
              SELECT
                {source_star},
                {profile_weight_sql} AS profile_weight,
                ROW_NUMBER() OVER (
                  PARTITION BY person_key, unitid, degree, cip2, cip4, cip6, grad_year
                  ORDER BY CASE horizon
                    WHEN '1yr' THEN 1
                    WHEN '5yr' THEN 2
                    WHEN '10yr' THEN 3
                    WHEN 'early_2025' THEN 4
                    ELSE 5
                  END
                ) AS cohort_rank
              FROM read_parquet(?)
              {where_sql}
            )
            WHERE cohort_rank = 1
            """,
            [_dataset_glob("base_fact"), *params],
        )
        majors = _records_from_query(
            con,
            f"""
            WITH major_counts AS (
              SELECT
                {cip_col} AS code,
                MAX(major_title) AS title,
                ROUND(SUM(profile_weight)) AS alumni
              FROM option_cohort
              WHERE {cip_col} IS NOT NULL
              GROUP BY {cip_col}
            )
            SELECT code, COALESCE(title, code) AS title, alumni
            FROM major_counts
            WHERE alumni >= ?
            ORDER BY alumni DESC, title
            LIMIT {limit}
            """,
            [SUPPRESSION_THRESHOLD],
        )
        demographics = {
            "gender": [
                row["value"]
                for row in _records_from_query(
                    con,
                    """
                    SELECT gender AS value, SUM(profile_weight) AS n
                    FROM option_cohort
                    WHERE gender IS NOT NULL
                      AND gender <> ''
                      AND LOWER(TRIM(gender)) NOT IN ('empty', 'unknown')
                    GROUP BY gender
                    ORDER BY n DESC
                    """,
                )
            ],
            "race_ethnicity": [
                row["value"]
                for row in _records_from_query(
                    con,
                    """
                    SELECT race_ethnicity AS value, SUM(profile_weight) AS n
                    FROM option_cohort
                    WHERE race_ethnicity IS NOT NULL
                      AND race_ethnicity <> ''
                      AND LOWER(TRIM(race_ethnicity)) NOT IN ('empty', 'unknown')
                    GROUP BY race_ethnicity
                    ORDER BY n DESC
                    """,
                )
            ],
        }
        later_degrees = [
            row["later_degree_type"]
            for row in _records_from_query(
                con,
                """
                SELECT later_degree_type, SUM(profile_weight) AS n
                FROM option_cohort
                WHERE later_degree_type IS NOT NULL AND later_degree_type <> ''
                GROUP BY later_degree_type
                ORDER BY n DESC
                """,
            )
        ]
        static = _static_options()
        return {
            **static,
            "major_options": majors,
            "demographics": demographics,
            "later_degrees": later_degrees,
            "meta": {
                "data_version": _manifest().get("version"),
                "suppression_threshold": SUPPRESSION_THRESHOLD,
            },
        }
    finally:
        con.close()


def _overview(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    return _single_record(
        con,
        """
        WITH cohort AS (
          SELECT
            SUM(profile_weight) AS alumni,
            COUNT(*) AS raw_rows,
            SUM(CASE WHEN later_degree_type IS NOT NULL THEN profile_weight ELSE 0 END) AS later_degree_n,
            SUM(CASE WHEN no_further_education_flag = 1 THEN profile_weight ELSE 0 END) AS no_further_n
          FROM cohort_slice
        ),
        outcomes AS (
          SELECT
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_mean_salary,
            quantile_cont(salary, 0.5) AS median_salary,
            COUNT(DISTINCT CASE WHEN employer IS NOT NULL AND employer <> '' AND unknown_employer_flag = 0 THEN employer END) AS unique_employers,
            COUNT(DISTINCT CASE WHEN location IS NOT NULL AND location <> '' THEN location END) AS unique_locations
          FROM slice
        )
        SELECT
          ROUND(c.alumni) AS alumni,
          c.raw_rows,
          ROUND(o.salary_weight) AS salary_weight,
          CASE WHEN o.salary_weight >= ? THEN ROUND(o.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN o.salary_weight >= ? THEN ROUND(o.median_salary) ELSE NULL END AS median_salary,
          o.unique_employers,
          o.unique_locations,
          ROUND(c.later_degree_n) AS later_degree_n,
          ROUND(c.no_further_n) AS no_further_n,
          ROUND(100.0 * c.later_degree_n / NULLIF(c.alumni, 0), 1) AS later_degree_pct
        FROM cohort c
        CROSS JOIN outcomes o
        """,
        [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD],
    )


def _salary_trend(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    return _records_from_query(
        con,
        """
        WITH by_year AS (
          SELECT
            grad_year,
            SUM(final_weight) AS alumni,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_mean_salary,
            quantile_cont(salary, 0.5) AS median_salary
          FROM slice
          WHERE grad_year IS NOT NULL
          GROUP BY grad_year
        )
        SELECT
          grad_year,
          ROUND(alumni) AS alumni,
          ROUND(salary_weight) AS salary_weight,
          CASE WHEN salary_weight >= ? THEN ROUND(weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN salary_weight >= ? THEN ROUND(median_salary) ELSE NULL END AS median_salary
        FROM by_year
        WHERE alumni >= ?
        ORDER BY grad_year
        """,
        [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD, TREND_SUPPRESSION_THRESHOLD],
    )


def _alumni_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    return _records_from_query(
        con,
        """
        WITH by_year AS (
          SELECT grad_year, SUM(profile_weight) AS alumni
          FROM cohort_slice
          WHERE grad_year IS NOT NULL
          GROUP BY grad_year
        )
        SELECT grad_year, ROUND(alumni) AS alumni
        FROM by_year
        WHERE alumni >= ?
        ORDER BY grad_year
        """,
        [TREND_SUPPRESSION_THRESHOLD],
    )


def _salary_trend_by_school(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    return _records_from_query(
        con,
        """
        WITH by_year AS (
          SELECT
            unitid,
            MAX(school_name) AS school_name,
            grad_year,
            SUM(final_weight) AS alumni,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_mean_salary,
            quantile_cont(salary, 0.5) AS median_salary
          FROM slice
          WHERE grad_year IS NOT NULL
          GROUP BY unitid, grad_year
        )
        SELECT
          unitid,
          school_name,
          grad_year,
          ROUND(alumni) AS alumni,
          ROUND(salary_weight) AS salary_weight,
          CASE WHEN salary_weight >= ? THEN ROUND(weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN salary_weight >= ? THEN ROUND(median_salary) ELSE NULL END AS median_salary
        FROM by_year
        WHERE alumni >= ?
        ORDER BY school_name, grad_year
        """,
        [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD, TREND_SUPPRESSION_THRESHOLD],
    )


def _alumni_trend_by_school(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    return _records_from_query(
        con,
        """
        WITH by_year AS (
          SELECT
            unitid,
            MAX(school_name) AS school_name,
            grad_year,
            SUM(profile_weight) AS alumni
          FROM cohort_slice
          WHERE grad_year IS NOT NULL
          GROUP BY unitid, grad_year
        )
        SELECT unitid, school_name, grad_year, ROUND(alumni) AS alumni
        FROM by_year
        WHERE alumni >= ?
        ORDER BY school_name, grad_year
        """,
        [TREND_SUPPRESSION_THRESHOLD],
    )


def _current_student_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    if not filters.include_current_students or not (_platform_root() / "current_students_fact").exists():
        return []
    if not _create_current_slice(con, filters):
        return []
    return _records_from_query(
        con,
        """
        SELECT
          grad_year,
          ROUND(SUM(profile_weight)) AS current_students
        FROM current_slice
        WHERE grad_year IS NOT NULL
        GROUP BY grad_year
        HAVING SUM(profile_weight) >= ?
        ORDER BY grad_year
        """,
        [TREND_SUPPRESSION_THRESHOLD],
    )


def _school_comparison(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    return _records_from_query(
        con,
        """
        WITH cohort AS (
          SELECT
            unitid,
            MAX(school_name) AS school_name,
            SUM(profile_weight) AS alumni,
            SUM(CASE WHEN later_degree_type IS NOT NULL THEN profile_weight ELSE 0 END) AS later_degree_n
          FROM cohort_slice
          GROUP BY unitid
        ),
        outcomes AS (
          SELECT
            unitid,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_mean_salary,
            quantile_cont(salary, 0.5) AS median_salary,
            COUNT(DISTINCT CASE WHEN employer IS NOT NULL AND employer <> '' AND unknown_employer_flag = 0 THEN employer END) AS unique_employers
          FROM slice
          GROUP BY unitid
        )
        SELECT
          c.unitid,
          c.school_name,
          ROUND(c.alumni) AS alumni,
          ROUND(o.salary_weight) AS salary_weight,
          CASE WHEN o.salary_weight >= ? THEN ROUND(o.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN o.salary_weight >= ? THEN ROUND(o.median_salary) ELSE NULL END AS median_salary,
          o.unique_employers,
          ROUND(100.0 * c.later_degree_n / NULLIF(c.alumni, 0), 1) AS later_degree_pct
        FROM cohort c
        LEFT JOIN outcomes o USING (unitid)
        WHERE c.alumni >= ?
        ORDER BY o.weighted_mean_salary DESC NULLS LAST, c.alumni DESC
        """,
        [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD],
    )


def _top_majors(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    cip_col = _cip_col(filters)
    limit = _safe_limit(filters.top_n)
    return _records_from_query(
        con,
        f"""
        WITH cohort AS (
          SELECT
            {cip_col} AS code,
            MAX(major_title) AS title,
            SUM(profile_weight) AS alumni
          FROM cohort_slice
          WHERE {cip_col} IS NOT NULL
          GROUP BY {cip_col}
        ),
        outcomes AS (
          SELECT
            {cip_col} AS code,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_mean_salary,
            quantile_cont(salary, 0.5) AS median_salary
          FROM slice
          WHERE {cip_col} IS NOT NULL
          GROUP BY {cip_col}
        )
        SELECT
          c.code,
          COALESCE(c.title, c.code) AS title,
          ROUND(c.alumni) AS alumni,
          ROUND(o.salary_weight) AS salary_weight,
          CASE WHEN o.salary_weight >= ? THEN ROUND(o.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN o.salary_weight >= ? THEN ROUND(o.median_salary) ELSE NULL END AS median_salary
        FROM cohort c
        LEFT JOIN outcomes o USING (code)
        WHERE c.alumni >= ?
        ORDER BY c.alumni DESC
        LIMIT {limit}
        """,
        [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD],
    )


def _major_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest, include_current: bool) -> dict[str, Any]:
    cip_col = _cip_col(filters)
    limit = min(_safe_limit(filters.top_n), 5)
    current_exists = include_current and _create_current_slice(con, filters)

    source_for_top = "current_slice" if current_exists else "cohort_slice"
    weight_expr = "SUM(profile_weight)"
    top_codes = _records_from_query(
        con,
        f"""
        SELECT {cip_col} AS code, MAX(major_title) AS title, {weight_expr} AS n
        FROM {source_for_top}
        WHERE {cip_col} IS NOT NULL
        GROUP BY {cip_col}
        HAVING {weight_expr} >= ?
        ORDER BY n DESC
        LIMIT {limit}
        """,
        [SUPPRESSION_THRESHOLD],
    )
    codes = [row["code"] for row in top_codes if row["code"]]
    if not codes:
        return {"series": [], "current_series": [], "top": []}

    placeholders = ",".join(["?"] * len(codes))
    base_series = _records_from_query(
        con,
        f"""
        WITH by_major AS (
          SELECT grad_year, {cip_col} AS code, MAX(major_title) AS title, SUM(profile_weight) AS n
          FROM cohort_slice
          WHERE {cip_col} IN ({placeholders}) AND grad_year IS NOT NULL
          GROUP BY grad_year, {cip_col}
        ),
        totals AS (
          SELECT grad_year, SUM(profile_weight) AS total_n
          FROM cohort_slice
          WHERE grad_year IS NOT NULL
          GROUP BY grad_year
        )
        SELECT
          b.grad_year,
          b.code,
          COALESCE(b.title, b.code) AS title,
          ROUND(b.n) AS n,
          ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct
        FROM by_major b
        JOIN totals t USING (grad_year)
        WHERE b.n >= ?
        ORDER BY b.grad_year, b.code
        """,
        [*codes, TREND_SUPPRESSION_THRESHOLD],
    )
    current_series: list[dict[str, Any]] = []
    if current_exists:
        current_series = _records_from_query(
            con,
            f"""
            WITH by_major AS (
              SELECT grad_year, {cip_col} AS code, MAX(major_title) AS title, SUM(profile_weight) AS n
              FROM current_slice
              WHERE {cip_col} IN ({placeholders}) AND grad_year IS NOT NULL
              GROUP BY grad_year, {cip_col}
            ),
            totals AS (
              SELECT grad_year, SUM(profile_weight) AS total_n
              FROM current_slice
              WHERE grad_year IS NOT NULL
              GROUP BY grad_year
            )
            SELECT
              b.grad_year,
              b.code,
              COALESCE(b.title, b.code) AS title,
              ROUND(b.n) AS n,
              ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct
            FROM by_major b
            JOIN totals t USING (grad_year)
            WHERE b.n >= ?
            ORDER BY b.grad_year, b.code
            """,
            [*codes, TREND_SUPPRESSION_THRESHOLD],
        )
    return {"top": top_codes, "series": base_series, "current_series": current_series}


def _employer_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    limit = min(_safe_limit(filters.top_n), 8)
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT grad_year, employer, final_weight
          FROM slice
          WHERE grad_year IS NOT NULL
            AND employer IS NOT NULL
            AND employer <> ''
            AND unknown_employer_flag = 0
            AND named_employer_flag = 1
            AND career_employer_flag = 1
            {SAME_SCHOOL_EMPLOYER_FILTER}
        ),
        top_employers AS (
          SELECT employer, SUM(final_weight) AS total_n
          FROM eligible
          GROUP BY employer
          HAVING SUM(final_weight) >= ?
          ORDER BY total_n DESC
          LIMIT {limit}
        ),
        by_year AS (
          SELECT e.grad_year, e.employer, SUM(e.final_weight) AS n
          FROM eligible e
          JOIN top_employers t USING (employer)
          GROUP BY e.grad_year, e.employer
        ),
        totals AS (
          SELECT grad_year, SUM(final_weight) AS total_n
          FROM eligible
          GROUP BY grad_year
        )
        SELECT
          b.grad_year,
          b.employer,
          ROUND(b.n) AS n,
          ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct
        FROM by_year b
        JOIN totals t USING (grad_year)
        WHERE b.n >= ?
        ORDER BY b.employer, b.grad_year
        """,
        [SUPPRESSION_THRESHOLD, TREND_SUPPRESSION_THRESHOLD],
    )


def _employers(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, Any]:
    limit = _safe_limit(filters.top_n)
    employers = _records_from_query(
        con,
        f"""
        WITH denom AS (
          SELECT SUM(final_weight) AS total_n
          FROM slice
          WHERE career_employer_flag = 1
            {SAME_SCHOOL_EMPLOYER_FILTER}
        ),
        by_employer AS (
          SELECT
            employer,
            SUM(final_weight) AS n,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_mean_salary,
            quantile_cont(salary, 0.5) AS median_salary
          FROM slice
          WHERE employer IS NOT NULL
            AND employer <> ''
            AND unknown_employer_flag = 0
            AND named_employer_flag = 1
            AND career_employer_flag = 1
            {SAME_SCHOOL_EMPLOYER_FILTER}
          GROUP BY employer
        )
        SELECT
          employer,
          ROUND(n) AS n,
          ROUND(100.0 * n / NULLIF((SELECT total_n FROM denom), 0), 2) AS share_pct,
          CASE WHEN salary_weight >= ? THEN ROUND(weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN salary_weight >= ? THEN ROUND(median_salary) ELSE NULL END AS median_salary
        FROM by_employer
        WHERE n >= ?
        ORDER BY n DESC
        LIMIT {limit}
        """,
        [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD],
    )
    employer = filters.selected_employer or (employers[0]["employer"] if employers else None)
    roles: list[dict[str, Any]] = []
    if employer:
        roles = _records_from_query(
            con,
            f"""
            SELECT
              COALESCE(role_k50_v3, role_k150_v3, role_k10_v3, 'Unknown role') AS role,
              ROUND(SUM(final_weight)) AS n,
              ROUND(100.0 * SUM(final_weight) / NULLIF((SELECT SUM(final_weight) FROM slice WHERE employer = ?), 0), 2) AS share_pct,
              ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
                / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0)) AS weighted_mean_salary
            FROM slice
            WHERE employer = ?
              {SAME_SCHOOL_EMPLOYER_FILTER}
              AND role_k50_v3 IS NOT NULL
              AND role_k50_v3 <> ''
            GROUP BY 1
            HAVING SUM(final_weight) >= ?
            ORDER BY SUM(final_weight) DESC
            LIMIT {limit}
            """,
            [employer, employer, SUPPRESSION_THRESHOLD],
        )
    return {"top": employers, "selected_employer": employer, "roles": roles}


def _geography_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    limit = min(_safe_limit(filters.top_n), 5)
    location_expr = "REGEXP_REPLACE(COALESCE(location, city, 'Unknown'), '(?i)\\s+(non)?metropolitan area$', '')"
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            grad_year,
            {location_expr} AS location,
            final_weight,
            salary
          FROM slice
          WHERE grad_year IS NOT NULL
            AND COALESCE(location, city) IS NOT NULL
            AND LOWER(COALESCE(location, city)) NOT IN ('empty', 'unknown')
        ),
        top_locations AS (
          SELECT
            location,
            SUM(final_weight) AS total_n,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight
          FROM eligible
          GROUP BY location
          HAVING SUM(final_weight) >= ?
          ORDER BY salary_weight DESC, total_n DESC
          LIMIT {limit}
        ),
        by_year AS (
          SELECT
            e.grad_year,
            e.location,
            SUM(e.final_weight) AS n,
            SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight * e.salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight ELSE 0 END), 0) AS weighted_mean_salary,
            quantile_cont(e.salary, 0.5) AS median_salary
          FROM eligible e
          JOIN top_locations t USING (location)
          GROUP BY e.grad_year, e.location
        ),
        totals AS (
          SELECT grad_year, SUM(final_weight) AS total_n
          FROM eligible
          GROUP BY grad_year
        )
        SELECT
          b.grad_year,
          b.location,
          ROUND(b.n) AS n,
          ROUND(b.salary_weight) AS salary_weight,
          ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct,
          CASE WHEN b.salary_weight >= ? THEN ROUND(b.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN b.salary_weight >= ? THEN ROUND(b.median_salary) ELSE NULL END AS median_salary
        FROM by_year b
        JOIN totals t USING (grad_year)
        WHERE b.n >= ?
        ORDER BY b.location, b.grad_year
        """,
        [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD, TREND_SUPPRESSION_THRESHOLD],
    )


def _geography(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    limit = min(_safe_limit(filters.top_n), 8)
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            REGEXP_REPLACE(COALESCE(location, city, 'Unknown'), '(?i)\\s+(non)?metropolitan area$', '') AS location,
            final_weight,
            salary
          FROM slice
          WHERE COALESCE(location, city) IS NOT NULL
            AND LOWER(COALESCE(location, city)) NOT IN ('empty', 'unknown')
        ),
        totals AS (
          SELECT SUM(final_weight) AS total_n
          FROM eligible
        )
        SELECT
          location,
          ROUND(SUM(final_weight)) AS n,
          ROUND(100.0 * SUM(final_weight) / NULLIF((SELECT total_n FROM totals), 0), 2) AS share_pct,
          ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END)) AS salary_weight,
          ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
            / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0)) AS weighted_mean_salary,
          quantile_cont(salary, 0.5) AS median_salary
        FROM eligible
        GROUP BY 1
        HAVING SUM(final_weight) >= ?
        ORDER BY SUM(final_weight) DESC
        LIMIT {limit}
        """,
        [SUPPRESSION_THRESHOLD],
    )


def _role_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, list[dict[str, Any]]]:
    limit = min(_safe_limit(filters.top_n), 8)

    def group(column: str, exclude_corporate_attorney: bool = False) -> list[dict[str, Any]]:
        exclusion = "AND NOT (degree = 'Bachelors' AND horizon = '1yr' AND role_k50_v3 = 'Corporate Attorney')" if exclude_corporate_attorney else ""
        return _records_from_query(
            con,
            f"""
            WITH eligible AS (
              SELECT grad_year, {column} AS label, final_weight
              FROM slice
              WHERE grad_year IS NOT NULL
                AND {column} IS NOT NULL
                AND {column} <> ''
                {exclusion}
            ),
            top_labels AS (
              SELECT label, SUM(final_weight) AS total_n
              FROM eligible
              GROUP BY label
              HAVING SUM(final_weight) >= ?
              ORDER BY total_n DESC
              LIMIT {limit}
            ),
            by_year AS (
              SELECT e.grad_year, e.label, SUM(e.final_weight) AS n
              FROM eligible e
              JOIN top_labels t USING (label)
              GROUP BY e.grad_year, e.label
            ),
            totals AS (
              SELECT grad_year, SUM(final_weight) AS total_n
              FROM slice
              WHERE grad_year IS NOT NULL
              GROUP BY grad_year
            )
            SELECT
              b.grad_year,
              b.label,
              ROUND(b.n) AS n,
              ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct
            FROM by_year b
            JOIN totals t USING (grad_year)
            WHERE b.n >= ?
            ORDER BY b.label, b.grad_year
            """,
            [SUPPRESSION_THRESHOLD, TREND_SUPPRESSION_THRESHOLD],
        )

    return {
        "roles": group("role_k50_v3", exclude_corporate_attorney=True),
        "industries": group("industry_k50"),
    }


def _roles(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, Any]:
    limit = _safe_limit(filters.top_n)
    role_rows = _records_from_query(
        con,
        f"""
        SELECT
          role_k50_v3 AS label,
          ROUND(SUM(final_weight)) AS n,
          ROUND(100.0 * SUM(final_weight) / NULLIF((SELECT SUM(final_weight) FROM slice), 0), 2) AS share_pct,
          ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
            / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0)) AS weighted_mean_salary
        FROM slice
        WHERE role_k50_v3 IS NOT NULL AND role_k50_v3 <> ''
          AND NOT (degree = 'Bachelors' AND horizon = '1yr' AND role_k50_v3 = 'Corporate Attorney')
        GROUP BY role_k50_v3
        HAVING SUM(final_weight) >= ?
        ORDER BY SUM(final_weight) DESC
        LIMIT {limit}
        """,
        [SUPPRESSION_THRESHOLD],
    )
    industry_rows = _records_from_query(
        con,
        f"""
        SELECT
          industry_k50 AS label,
          ROUND(SUM(final_weight)) AS n,
          ROUND(100.0 * SUM(final_weight) / NULLIF((SELECT SUM(final_weight) FROM slice), 0), 2) AS share_pct,
          ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
            / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0)) AS weighted_mean_salary
        FROM slice
        WHERE industry_k50 IS NOT NULL AND industry_k50 <> ''
        GROUP BY industry_k50
        HAVING SUM(final_weight) >= ?
        ORDER BY SUM(final_weight) DESC
        LIMIT {limit}
        """,
        [SUPPRESSION_THRESHOLD],
    )
    hierarchy_rows = _role_industry_hierarchy(con, filters)
    return {
        "roles": role_rows,
        "industries": industry_rows,
        "hierarchy": hierarchy_rows,
        "role_tree": _hierarchy_nodes(hierarchy_rows, ["role_k50_v3", "role_k150_v3"], "All roles", "Role"),
        "industry_tree": _hierarchy_nodes(hierarchy_rows, ["industry_k200", "industry_k400"], "All industries", "Industry"),
    }


def _role_industry_hierarchy(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    """Flat role/industry hierarchy used by the zoomable blue treemaps."""
    base_columns = _dataset_columns("base_fact")
    role_detail_expr = "NULLIF(TRIM(role_k150_v3), '')" if "role_k150_v3" in base_columns else "NULL"
    industry_expr = "TRIM(industry_k200)" if "industry_k200" in base_columns else "TRIM(industry_k50)"
    industry_detail_expr = "NULLIF(TRIM(industry_k400), '')" if "industry_k400" in base_columns else "NULL"
    max_rows = max(600, _safe_limit(filters.top_n) * 90)
    return _records_from_query(
        con,
        f"""
        SELECT
          TRIM(role_k50_v3) AS role_k50_v3,
          COALESCE({role_detail_expr}, TRIM(role_k50_v3)) AS role_k150_v3,
          {industry_expr} AS industry_k200,
          COALESCE({industry_detail_expr}, {industry_expr}) AS industry_k400,
          ROUND(SUM(final_weight)) AS n,
          SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
          SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END) AS salary_sum,
          ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
            / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0)) AS weighted_mean_salary
        FROM slice
        WHERE role_k50_v3 IS NOT NULL
          AND TRIM(role_k50_v3) <> ''
          AND LOWER(TRIM(role_k50_v3)) NOT IN ('empty', 'unknown', 'other')
          AND {industry_expr} IS NOT NULL
          AND {industry_expr} <> ''
          AND LOWER({industry_expr}) NOT IN ('empty', 'unknown', 'other')
          AND NOT (degree = 'Bachelors' AND horizon = '1yr' AND role_k50_v3 = 'Corporate Attorney')
        GROUP BY role_k50_v3, role_k150_v3, industry_k200, industry_k400
        HAVING SUM(final_weight) >= ?
        ORDER BY SUM(final_weight) DESC
        LIMIT {max_rows}
        """,
        [TREND_SUPPRESSION_THRESHOLD],
    )


def _hierarchy_nodes(rows: list[dict[str, Any]], levels: list[str], root_label: str, level_label: str) -> list[dict[str, Any]]:
    """Compatibility node payload for consumers that prefer native Plotly treemaps."""
    if not rows or not levels:
        return []

    def clean(value: Any) -> str:
        return str(value or "").strip()

    def add_stats(target: dict[str, Any], row: dict[str, Any]) -> None:
        target["n"] = float(target.get("n") or 0) + float(row.get("n") or 0)
        target["salary_weight"] = float(target.get("salary_weight") or 0) + float(row.get("salary_weight") or 0)
        target["salary_sum"] = float(target.get("salary_sum") or 0) + float(row.get("salary_sum") or 0)

    parent_nodes: dict[str, dict[str, Any]] = {}
    child_nodes: dict[tuple[str, str], dict[str, Any]] = {}
    parent_field = levels[0]
    child_field = levels[1] if len(levels) > 1 else None
    for row in rows:
        parent = clean(row.get(parent_field))
        if not parent:
            continue
        parent_node = parent_nodes.setdefault(
            parent,
            {"id": f"{level_label.lower()}::{parent}", "parent": "root", "label": parent, "level": level_label},
        )
        add_stats(parent_node, row)
        if child_field:
            child = clean(row.get(child_field))
            if child and child != parent:
                child_node = child_nodes.setdefault(
                    (parent, child),
                    {
                        "id": f"{level_label.lower()}::{parent}::{child}",
                        "parent": f"{level_label.lower()}::{parent}",
                        "label": child,
                        "level": f"Detailed {level_label.lower()}",
                    },
                )
                add_stats(child_node, row)

    root_total = sum(float(node.get("n") or 0) for node in parent_nodes.values())
    nodes: list[dict[str, Any]] = [
        {"id": "root", "parent": "", "label": root_label, "level": "All", "n": root_total, "salary_weight": 0, "salary_sum": 0}
    ]
    nodes.extend(sorted(parent_nodes.values(), key=lambda row: row["n"], reverse=True))
    nodes.extend(sorted(child_nodes.values(), key=lambda row: row["n"], reverse=True))
    for node in nodes:
        salary_weight = float(node.pop("salary_weight", 0) or 0)
        salary_sum = float(node.pop("salary_sum", 0) or 0)
        node["n"] = round(float(node.get("n") or 0))
        node["share_pct"] = round(100.0 * float(node["n"]) / root_total, 2) if root_total else None
        node["weighted_mean_salary"] = round(salary_sum / salary_weight) if salary_weight >= SUPPRESSION_THRESHOLD else None
        node["drillable"] = any(child.get("parent") == node.get("id") for child in child_nodes.values())
    return nodes


def _role_tree(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    """Hierarchy for the zoomable role/industry treemap."""
    industry_limit = min(_safe_limit(filters.top_n), 8)
    role_limit = min(_safe_limit(filters.top_n), 12)
    base_columns = _dataset_columns("base_fact")
    detail_expr = "NULLIF(TRIM(role_k150_v3), '')" if "role_k150_v3" in base_columns else "NULL"
    leaf_rows = _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            TRIM(industry_k50) AS industry,
            TRIM(role_k50_v3) AS role,
            COALESCE({detail_expr}, TRIM(role_k50_v3)) AS detail_role,
            final_weight,
            salary
          FROM slice
          WHERE industry_k50 IS NOT NULL
            AND role_k50_v3 IS NOT NULL
            AND TRIM(industry_k50) <> ''
            AND TRIM(role_k50_v3) <> ''
            AND LOWER(TRIM(industry_k50)) NOT IN ('empty', 'unknown')
            AND LOWER(TRIM(role_k50_v3)) NOT IN ('empty', 'unknown')
            AND NOT (degree = 'Bachelors' AND horizon = '1yr' AND role_k50_v3 = 'Corporate Attorney')
        ),
        top_industries AS (
          SELECT industry, SUM(final_weight) AS n
          FROM eligible
          GROUP BY industry
          HAVING SUM(final_weight) >= ?
          ORDER BY n DESC
          LIMIT {industry_limit}
        ),
        role_totals_raw AS (
          SELECT e.industry, e.role, SUM(e.final_weight) AS n
          FROM eligible e
          JOIN top_industries i USING (industry)
          GROUP BY e.industry, e.role
          HAVING SUM(e.final_weight) >= ?
        ),
        top_roles AS (
          SELECT industry, role, n
          FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY industry ORDER BY n DESC) AS role_rank
            FROM role_totals_raw
          )
          WHERE role_rank <= {role_limit}
        )
        SELECT
          e.industry,
          e.role,
          e.detail_role,
          SUM(e.final_weight) AS n,
          SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight ELSE 0 END) AS salary_weight,
          SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight * e.salary ELSE 0 END) AS salary_sum
        FROM eligible e
        JOIN top_roles r
          ON e.industry = r.industry
         AND e.role = r.role
        GROUP BY e.industry, e.role, e.detail_role
        HAVING SUM(e.final_weight) >= ?
        ORDER BY e.industry, e.role, n DESC
        """,
        [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD, TREND_SUPPRESSION_THRESHOLD],
    )
    if not leaf_rows:
        return []

    industry_nodes: dict[str, dict[str, Any]] = {}
    role_nodes: dict[tuple[str, str], dict[str, Any]] = {}
    detail_nodes: dict[tuple[str, str, str], dict[str, Any]] = {}

    def add_stats(target: dict[str, Any], row: dict[str, Any]) -> None:
        target["n"] = float(target.get("n") or 0) + float(row.get("n") or 0)
        target["salary_weight"] = float(target.get("salary_weight") or 0) + float(row.get("salary_weight") or 0)
        target["salary_sum"] = float(target.get("salary_sum") or 0) + float(row.get("salary_sum") or 0)

    for row in leaf_rows:
        industry = row["industry"]
        role = row["role"]
        detail = row["detail_role"] or role
        industry_node = industry_nodes.setdefault(
            industry,
            {"id": f"industry::{industry}", "parent": "root", "label": industry, "level": "Industry"},
        )
        role_node = role_nodes.setdefault(
            (industry, role),
            {"id": f"role::{industry}::{role}", "parent": f"industry::{industry}", "label": role, "level": "Role"},
        )
        add_stats(industry_node, row)
        add_stats(role_node, row)
        if detail != role:
            detail_node = detail_nodes.setdefault(
                (industry, role, detail),
                {
                    "id": f"detail::{industry}::{role}::{detail}",
                    "parent": f"role::{industry}::{role}",
                    "label": detail,
                    "level": "Detailed role",
                },
            )
            add_stats(detail_node, row)

    root_total = sum(float(node.get("n") or 0) for node in industry_nodes.values())
    nodes = [
        {
            "id": "root",
            "parent": "",
            "label": "All role outcomes",
            "level": "All",
            "n": root_total,
            "salary_weight": 0,
            "salary_sum": 0,
        }
    ]
    nodes.extend(sorted(industry_nodes.values(), key=lambda row: row["n"], reverse=True))
    nodes.extend(sorted(role_nodes.values(), key=lambda row: row["n"], reverse=True))
    nodes.extend(sorted(detail_nodes.values(), key=lambda row: row["n"], reverse=True))

    for node in nodes:
        salary_weight = float(node.pop("salary_weight", 0) or 0)
        salary_sum = float(node.pop("salary_sum", 0) or 0)
        node["n"] = round(float(node.get("n") or 0))
        node["share_pct"] = round(100.0 * float(node["n"]) / root_total, 2) if root_total else None
        node["weighted_mean_salary"] = round(salary_sum / salary_weight) if salary_weight >= SUPPRESSION_THRESHOLD else None
    return nodes


def _coverage_filters(
    filters: QueryRequest,
    *,
    ignore_degree: bool = False,
    ignore_major: bool = False,
    force_degree: str | None = None,
) -> tuple[str, list[Any]]:
    coverage_filters = filters.model_copy(deep=True)
    coverage_filters.demographics = DemographicFilters()
    coverage_filters.postgrad = PostgradFilters()
    if ignore_degree:
        coverage_filters.degree = "All"
    if force_degree:
        coverage_filters.degree = force_degree
    if ignore_major:
        coverage_filters.majors = []
    return _where(coverage_filters, include_horizon=False, include_postgrad=False)


def _create_coverage_groups(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    filters: QueryRequest,
    *,
    ignore_degree: bool = False,
    ignore_major: bool = False,
    force_degree: str | None = None,
) -> None:
    where_sql, params = _coverage_filters(
        filters,
        ignore_degree=ignore_degree,
        ignore_major=ignore_major,
        force_degree=force_degree,
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
        SELECT
          unitid,
          MAX(school_name) AS school_name,
          degree,
          grad_year,
          cip4 AS code,
          MAX(major_title) AS title,
          MAX(calibration_observed_completions) AS revelio_completions,
          MAX(calibration_ipeds_completions) AS ipeds_completions
        FROM read_parquet(?)
        {where_sql}
        GROUP BY unitid, degree, grad_year, cip4
        HAVING grad_year IS NOT NULL
          AND cip4 IS NOT NULL
          AND MAX(calibration_observed_completions) IS NOT NULL
        """,
        [_dataset_glob("base_fact"), *params],
    )


def _coverage(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, Any]:
    _create_coverage_groups(con, "coverage_selected", filters)
    _create_coverage_groups(con, "coverage_scope", filters, ignore_degree=True, ignore_major=True)
    _create_coverage_groups(con, "coverage_bachelors", filters, force_degree="Bachelors", ignore_major=True)

    coverage_expr = """
      CASE
        WHEN SUM(COALESCE(ipeds_completions, 0)) > 0
        THEN ROUND(100.0 * SUM(COALESCE(revelio_completions, 0)) / SUM(COALESCE(ipeds_completions, 0)), 1)
        ELSE NULL
      END
    """
    degree_rows = _records_from_query(
        con,
        f"""
        SELECT
          degree,
          ROUND(SUM(COALESCE(revelio_completions, 0))) AS revelio_completions,
          ROUND(SUM(COALESCE(ipeds_completions, 0))) AS ipeds_completions,
          {coverage_expr} AS coverage_pct
        FROM coverage_scope
        GROUP BY degree
        HAVING SUM(COALESCE(revelio_completions, 0)) >= ?
            OR SUM(COALESCE(ipeds_completions, 0)) >= ?
        ORDER BY revelio_completions DESC
        """,
        [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD],
    )
    bachelor_major_rows = _records_from_query(
        con,
        f"""
        SELECT
          code,
          COALESCE(MAX(title), code) AS title,
          ROUND(SUM(COALESCE(revelio_completions, 0))) AS revelio_completions,
          ROUND(SUM(COALESCE(ipeds_completions, 0))) AS ipeds_completions,
          {coverage_expr} AS coverage_pct
        FROM coverage_bachelors
        GROUP BY code
        HAVING SUM(COALESCE(revelio_completions, 0)) >= ?
            OR SUM(COALESCE(ipeds_completions, 0)) >= ?
        ORDER BY revelio_completions DESC
        LIMIT 30
        """,
        [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD],
    )
    school_rows = _records_from_query(
        con,
        f"""
        SELECT
          unitid,
          MAX(school_name) AS school_name,
          ROUND(SUM(COALESCE(revelio_completions, 0))) AS revelio_completions,
          ROUND(SUM(COALESCE(ipeds_completions, 0))) AS ipeds_completions,
          {coverage_expr} AS coverage_pct
        FROM coverage_selected
        GROUP BY unitid
        HAVING SUM(COALESCE(revelio_completions, 0)) >= ?
            OR SUM(COALESCE(ipeds_completions, 0)) >= ?
        ORDER BY revelio_completions DESC
        """,
        [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD],
    )
    trend_rows = _records_from_query(
        con,
        f"""
        SELECT
          grad_year,
          ROUND(SUM(COALESCE(revelio_completions, 0))) AS revelio_completions,
          ROUND(SUM(COALESCE(ipeds_completions, 0))) AS ipeds_completions,
          {coverage_expr} AS coverage_pct
        FROM coverage_selected
        WHERE grad_year IS NOT NULL
        GROUP BY grad_year
        HAVING SUM(COALESCE(revelio_completions, 0)) >= ?
            OR SUM(COALESCE(ipeds_completions, 0)) >= ?
        ORDER BY grad_year
        """,
        [TREND_SUPPRESSION_THRESHOLD, TREND_SUPPRESSION_THRESHOLD],
    )
    return {
        "degree": degree_rows,
        "bachelor_majors": bachelor_major_rows,
        "schools": school_rows,
        "trend": trend_rows,
    }


def _demographic_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, list[dict[str, Any]]]:
    limit = min(_safe_limit(filters.top_n), 8)

    def group(column: str) -> list[dict[str, Any]]:
        return _records_from_query(
            con,
            f"""
            WITH eligible AS (
              SELECT grad_year, {column} AS label, profile_weight
              FROM cohort_slice
              WHERE grad_year IS NOT NULL
                AND {column} IS NOT NULL
                AND {column} <> ''
                AND LOWER(TRIM({column})) NOT IN ('empty', 'unknown')
            ),
            top_labels AS (
              SELECT label, SUM(profile_weight) AS total_n
              FROM eligible
              GROUP BY label
              HAVING SUM(profile_weight) >= ?
              ORDER BY total_n DESC
              LIMIT {limit}
            ),
            by_year AS (
              SELECT
                e.grad_year,
                e.label,
                SUM(e.profile_weight) AS n
              FROM eligible e
              JOIN top_labels t USING (label)
              GROUP BY e.grad_year, e.label
            ),
            totals AS (
              SELECT grad_year, SUM(profile_weight) AS total_n
              FROM eligible
              GROUP BY grad_year
            )
            SELECT
              b.grad_year,
              b.label,
              ROUND(b.n) AS n,
              ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct,
              NULL AS weighted_mean_salary
            FROM by_year b
            JOIN totals t USING (grad_year)
            WHERE b.n >= ?
            ORDER BY b.label, b.grad_year
            """,
            [SUPPRESSION_THRESHOLD, TREND_SUPPRESSION_THRESHOLD],
        )

    return {"gender": group("gender"), "race_ethnicity": group("race_ethnicity")}


def _demographics(con: duckdb.DuckDBPyConnection) -> dict[str, list[dict[str, Any]]]:
    def group(column: str) -> list[dict[str, Any]]:
        return _records_from_query(
            con,
            f"""
            WITH cohort AS (
              SELECT
                {column} AS label,
                SUM(profile_weight) AS n
              FROM cohort_slice
              WHERE {column} IS NOT NULL
                AND {column} <> ''
                AND LOWER(TRIM({column})) NOT IN ('empty', 'unknown')
              GROUP BY {column}
            ),
            salary AS (
              SELECT
                {column} AS label,
                SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
                SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_mean_salary
              FROM slice
              WHERE {column} IS NOT NULL
                AND {column} <> ''
                AND LOWER(TRIM({column})) NOT IN ('empty', 'unknown')
              GROUP BY {column}
            ),
            totals AS (
              SELECT SUM(n) AS total_n FROM cohort
            )
            SELECT
              c.label,
              ROUND(c.n) AS n,
              ROUND(100.0 * c.n / NULLIF((SELECT total_n FROM totals), 0), 2) AS share_pct,
              CASE WHEN s.salary_weight >= ? THEN ROUND(s.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary
            FROM cohort c
            LEFT JOIN salary s USING (label)
            WHERE c.n >= ?
            ORDER BY c.n DESC
            """,
            [SUPPRESSION_THRESHOLD, SUPPRESSION_THRESHOLD],
        )

    return {"gender": group("gender"), "race_ethnicity": group("race_ethnicity")}


def _postgrad_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    limit = min(_safe_limit(filters.top_n), 8)
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            grad_year,
            COALESCE(later_degree_type, CASE WHEN no_further_education_flag = 1 THEN 'No further education' ELSE 'Unknown' END) AS degree_type,
            profile_weight
          FROM cohort_slice
          WHERE grad_year IS NOT NULL
        ),
        top_paths AS (
          SELECT degree_type, SUM(profile_weight) AS total_n
          FROM eligible
          WHERE degree_type <> 'Unknown'
          GROUP BY degree_type
          HAVING SUM(profile_weight) >= ?
          ORDER BY CASE WHEN degree_type = 'No further education' THEN 1 ELSE 0 END, total_n DESC
          LIMIT {limit}
        ),
        by_year AS (
          SELECT e.grad_year, e.degree_type, SUM(e.profile_weight) AS n
          FROM eligible e
          JOIN top_paths t USING (degree_type)
          GROUP BY e.grad_year, e.degree_type
        ),
        totals AS (
          SELECT grad_year, SUM(profile_weight) AS total_n
          FROM eligible
          GROUP BY grad_year
        )
        SELECT
          b.grad_year,
          b.degree_type,
          ROUND(b.n) AS n,
          ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct
        FROM by_year b
        JOIN totals t USING (grad_year)
        WHERE b.n >= ?
        ORDER BY b.degree_type, b.grad_year
        """,
        [SUPPRESSION_THRESHOLD, TREND_SUPPRESSION_THRESHOLD],
    )


def _postgrad(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, Any]:
    limit = _safe_limit(filters.top_n)

    def detail_degree_values(degree_type: str) -> list[str]:
        if degree_type in {"PhD", "Doctorate", "Research Doctorate"}:
            return ["PhD", "Doctorate", "Research Doctorate"]
        return [degree_type]

    def detail_degree_label(degree_type: str | None) -> str | None:
        if degree_type in {"PhD", "Doctorate", "Research Doctorate"}:
            return "PhD / Doctorate"
        return degree_type

    def show_program_detail(degree_type: str | None) -> bool:
        return degree_type in {"Masters", "PhD", "Doctorate", "Research Doctorate"}

    flows = _records_from_query(
        con,
        f"""
        WITH denom AS (
          SELECT SUM(profile_weight) AS total_n FROM cohort_slice
        ),
        flows AS (
          SELECT
            COALESCE(later_degree_type, CASE WHEN no_further_education_flag = 1 THEN 'No further education' ELSE 'Unknown' END) AS degree_type,
            SUM(profile_weight) AS n
          FROM cohort_slice
          GROUP BY 1
        )
        SELECT
          degree_type,
          ROUND(n) AS n,
          ROUND(100.0 * n / NULLIF((SELECT total_n FROM denom), 0), 2) AS share_pct
        FROM flows
        WHERE n >= ? AND degree_type <> 'Unknown'
        ORDER BY CASE WHEN degree_type = 'No further education' THEN 1 ELSE 0 END, n DESC
        LIMIT {limit}
        """,
        [SUPPRESSION_THRESHOLD],
    )
    selected = filters.selected_postgrad_degree
    if not selected:
        selected = next((row["degree_type"] for row in flows if row["degree_type"] != "No further education"), None)
    schools: list[dict[str, Any]] = []
    programs: list[dict[str, Any]] = []
    if selected and selected != "No further education":
        selected_values = detail_degree_values(selected)
        placeholders = ",".join(["?"] * len(selected_values))
        school_filter_sql = ""
        school_filter_params: list[Any] = []
        if filters.selected_postgrad_program:
            school_filter_sql = "AND later_program = ?"
            school_filter_params.append(filters.selected_postgrad_program)
        schools = _records_from_query(
            con,
            f"""
            SELECT
              later_school AS label,
              ROUND(SUM(profile_weight)) AS n
            FROM cohort_slice
            WHERE later_degree_type IN ({placeholders})
              AND later_school IS NOT NULL
              AND later_school <> ''
              {school_filter_sql}
            GROUP BY later_school
            HAVING SUM(profile_weight) >= ?
            ORDER BY SUM(profile_weight) DESC
            LIMIT {limit}
            """,
            [*selected_values, *school_filter_params, SUPPRESSION_THRESHOLD],
        )
        program_filter_sql = ""
        program_filter_params: list[Any] = []
        if filters.selected_postgrad_school:
            program_filter_sql = "AND later_school = ?"
            program_filter_params.append(filters.selected_postgrad_school)
        programs = _records_from_query(
            con,
            f"""
            SELECT
              later_program AS label,
              ROUND(SUM(profile_weight)) AS n
            FROM cohort_slice
            WHERE later_degree_type IN ({placeholders})
              AND later_program IS NOT NULL
              AND later_program <> ''
              {program_filter_sql}
            GROUP BY later_program
            HAVING SUM(profile_weight) >= ?
            ORDER BY SUM(profile_weight) DESC
            LIMIT {limit}
            """,
            [*selected_values, *program_filter_params, SUPPRESSION_THRESHOLD],
        )
    return {
        "flows": flows,
        "selected_degree": selected,
        "detail_degree": detail_degree_label(selected),
        "selected_school": filters.selected_postgrad_school,
        "selected_program": filters.selected_postgrad_program,
        "show_program_detail": show_program_detail(selected),
        "schools": schools,
        "programs": programs,
    }


@app.post("/api/dashboard")
def dashboard(filters: QueryRequest, _: None = Depends(require_internal_password)) -> dict[str, Any]:
    con = _connect()
    try:
        _create_slice(con, filters)
        return {
            "meta": {
                "data_version": _manifest().get("version"),
                "suppression_threshold": SUPPRESSION_THRESHOLD,
                "partial_horizon": filters.horizon == "early_2025",
                "filters": filters.model_dump(),
            },
            "overview": _overview(con),
            "salary_trend": _salary_trend(con),
            "alumni_trend": _alumni_trend(con, filters),
            "salary_trend_by_school": _salary_trend_by_school(con),
            "alumni_trend_by_school": _alumni_trend_by_school(con, filters) if filters.compare_mode else [],
            "current_student_trend": _current_student_trend(con, filters),
            "school_comparison": _school_comparison(con),
            "top_majors": _top_majors(con, filters),
            "major_trend": _major_trend(con, filters, filters.include_current_students),
            "employers": _employers(con, filters),
            "employer_trend": _employer_trend(con, filters),
            "geography": _geography(con, filters),
            "geography_trend": _geography_trend(con, filters),
            "roles": _roles(con, filters),
            "role_trend": _role_trend(con, filters),
            "coverage": _coverage(con, filters),
            "demographics": _demographics(con),
            "demographic_trend": _demographic_trend(con, filters),
            "postgrad": _postgrad(con, filters),
            "postgrad_trend": _postgrad_trend(con, filters),
        }
    finally:
        con.close()
