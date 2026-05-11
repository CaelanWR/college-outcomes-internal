from __future__ import annotations

import hmac
import hashlib
import json
import math
import os
import time
import threading
from collections import OrderedDict
from copy import deepcopy
from decimal import Decimal
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field


DATA_ROOT = Path(os.environ.get("OUTCOMES_PARQUET_ROOT", "./data/parquet")).expanduser()
PLATFORM_ROOT = Path(os.environ.get("OUTCOMES_PLATFORM_ROOT", "")).expanduser() if os.environ.get("OUTCOMES_PLATFORM_ROOT") else None
STATIC_ROOT = Path(__file__).resolve().parent / "static"
MIN_CELL_WEIGHT = float(os.environ.get("MIN_CELL_WEIGHT", "0"))
SALARY_MIN_WEIGHT = float(os.environ.get("SALARY_MIN_WEIGHT", "0"))
EMPLOYER_ROW_MIN_WEIGHT = float(os.environ.get("EMPLOYER_ROW_MIN_WEIGHT", str(MIN_CELL_WEIGHT)))
GEOGRAPHY_ROW_MIN_WEIGHT = float(os.environ.get("GEOGRAPHY_ROW_MIN_WEIGHT", str(MIN_CELL_WEIGHT)))
SALARY_DISTRIBUTION_BUCKETS = int(os.environ.get("SALARY_DISTRIBUTION_BUCKETS", "32"))
DUCKDB_THREADS = int(os.environ.get("OUTCOMES_DUCKDB_THREADS", "1"))
DUCKDB_MEMORY_LIMIT = os.environ.get("OUTCOMES_DUCKDB_MEMORY_LIMIT", "1200MB")
DUCKDB_TEMP_DIR = Path(os.environ.get("OUTCOMES_DUCKDB_TEMP_DIR", "/tmp/duckdb")).expanduser()
DEFAULT_SCHOOL_CACHE_DIR = "/var/data/outcomes_school_cache" if Path("/var/data").exists() else "/tmp/outcomes_school_cache"
SCHOOL_CACHE_DIR = Path(os.environ.get("OUTCOMES_SCHOOL_CACHE_DIR", DEFAULT_SCHOOL_CACHE_DIR)).expanduser()
MAX_COMPARE_SCHOOLS = int(os.environ.get("OUTCOMES_MAX_COMPARE_SCHOOLS", "3"))
QUERY_SEMAPHORE = threading.BoundedSemaphore(int(os.environ.get("OUTCOMES_QUERY_CONCURRENCY", "1")))
SCHOOL_CACHE_LOCK = threading.Lock()
RESPONSE_CACHE_LOCK = threading.Lock()
RESPONSE_CACHE: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()
RESPONSE_CACHE_TTL_SECONDS = int(os.environ.get("OUTCOMES_RESPONSE_CACHE_TTL_SECONDS", "600"))
RESPONSE_CACHE_MAX_ENTRIES = int(os.environ.get("OUTCOMES_RESPONSE_CACHE_MAX_ENTRIES", "96"))
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
    include_school_employers: bool = False
    compare_mode: bool = False
    compare_dimension: str = "school"
    active_tab: str = "overview"
    view_mode: str = "overtime"
    selected_employer: Optional[str] = None
    selected_employer_role: Optional[str] = None
    selected_postgrad_degree: Optional[str] = None
    selected_postgrad_school: Optional[str] = None
    selected_postgrad_program: Optional[str] = None
    top_n: int = 8


def _same_school_employer_filter(filters: QueryRequest) -> str:
    return "" if filters.include_school_employers else SAME_SCHOOL_EMPLOYER_FILTER


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


def _dataset_root(dataset: str) -> Path:
    if DATA_ROOT.name == dataset:
        return DATA_ROOT
    return _platform_root() / dataset


@lru_cache(maxsize=8)
def _dataset_files(dataset: str) -> tuple[str, ...]:
    root = _dataset_root(dataset)
    if not root.exists():
        return tuple()
    return tuple(
        sorted(
            str(path)
            for path in root.rglob("*.parquet")
            if path.is_file() and not path.name.startswith(".")
        )
    )


def _dataset_glob(dataset: str) -> list[str]:
    return list(_dataset_files(dataset))


def _base_source_for_filters(filters: QueryRequest) -> list[str]:
    schools = list(dict.fromkeys(str(school) for school in filters.schools if str(school)))
    if len(schools) == 1:
        return _school_base_cache(schools[0])
    if filters.compare_mode and filters.compare_dimension == "school" and schools:
        if len(schools) > MAX_COMPARE_SCHOOLS:
            raise HTTPException(
                status_code=400,
                detail=f"School compare is capped at {MAX_COMPARE_SCHOOLS} schools for performance.",
            )
        return [path for school in schools for path in _school_base_cache(school)]
    return _dataset_glob("base_fact")


def _school_cache_key(unitid: str) -> str:
    manifest_path = _platform_root() / "platform_manifest.json"
    manifest_mtime = int(manifest_path.stat().st_mtime) if manifest_path.exists() else 0
    payload = {
        "dataset": "base_fact",
        "unitid": str(unitid),
        "version": _manifest().get("version"),
        "manifest_mtime": manifest_mtime,
        "file_count": len(_dataset_files("base_fact")),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:20]


def _school_base_cache(unitid: str) -> list[str]:
    key = _school_cache_key(unitid)
    SCHOOL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = SCHOOL_CACHE_DIR / f"base_fact_unitid_{unitid}_{key}.parquet"
    if path.exists() and path.stat().st_size > 0:
        return [str(path)]

    with SCHOOL_CACHE_LOCK:
        if path.exists() and path.stat().st_size > 0:
            return [str(path)]
        tmp_path = SCHOOL_CACHE_DIR / f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        con = _connect()
        try:
            target = str(tmp_path).replace("'", "''")
            con.execute(
                f"""
                COPY (
                  SELECT *
                  FROM read_parquet(?)
                  WHERE unitid = ?
                ) TO '{target}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
                """,
                [_dataset_glob("base_fact"), str(unitid)],
            )
        finally:
            con.close()
        os.replace(tmp_path, path)
    return [str(path)]


def _dataset_exists(dataset: str) -> bool:
    return bool(_dataset_files(dataset))


def _work_dataset_exists(dataset: str) -> bool:
    return _dataset_exists(f"work_facts/{dataset}")


@lru_cache(maxsize=8)
def _dataset_columns(dataset: str) -> frozenset[str]:
    if not _dataset_exists(dataset):
        return frozenset()
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


def _qualified_weight_sql(columns: frozenset[str], candidates: list[str], alias: str) -> str:
    available = [f"{alias}.{column}" for column in candidates if column in columns]
    if not available:
        return "1.0"
    return f"GREATEST(0.0, COALESCE({', '.join(available)}, 1.0))"


def _cohort_profile_weight_sql(columns: frozenset[str]) -> str:
    calibrated_weight = _profile_weight_sql(columns)
    position_weight = _position_profile_weight_sql(columns)
    has_recent_calibration_fields = {"degree", "grad_year", "calibration_ipeds_completions"}.issubset(columns)
    if not has_recent_calibration_fields:
        return calibrated_weight
    recent_source_guard = ""
    if "ipeds_calibration_source" in columns:
        recent_source_guard = "\n          AND COALESCE(ipeds_calibration_source, '') NOT LIKE 'recent_%'"
    return f"""
      CASE
        WHEN degree = 'Bachelors'
          AND grad_year >= 2023
          AND calibration_ipeds_completions IS NULL
          {recent_source_guard}
        THEN {position_weight}
        ELSE {calibrated_weight}
      END
    """


def _source_star_without_profile(columns: frozenset[str]) -> str:
    return "* EXCLUDE (profile_weight)" if "profile_weight" in columns else "*"


def _source_star_excluding(columns: frozenset[str], names: list[str], alias: str = "b") -> str:
    excluded = [name for name in names if name in columns]
    if excluded:
        return f"{alias}.* EXCLUDE ({', '.join(excluded)})"
    return f"{alias}.*"


def _runtime_adjusted_select(columns: frozenset[str], alias: str = "b") -> str:
    star = _source_star_excluding(columns, ["profile_weight", "final_weight", "ipeds_calibration_weight"], alias=alias)
    calibrated_weight = _qualified_weight_sql(
        columns,
        [
            "profile_weight",
            "ipeds_calibration_weight",
            "education_weight",
            "individual_weight",
            "representation_weight",
            "universe_weight",
            "final_weight",
        ],
        alias,
    )
    position_weight = _qualified_weight_sql(
        columns,
        [
            "position_weight",
            "profile_weight",
            "education_weight",
            "individual_weight",
            "representation_weight",
            "universe_weight",
            "final_weight",
        ],
        alias,
    )
    if "final_weight" in columns:
        existing_combined_weight = f"GREATEST(0.0, COALESCE({alias}.final_weight, {position_weight} * {calibrated_weight}, {calibrated_weight}))"
    elif {"position_weight", "ipeds_calibration_weight"}.issubset(columns):
        existing_combined_weight = f"{position_weight} * {calibrated_weight}"
    else:
        existing_combined_weight = calibrated_weight
    existing_ipeds_weight = f"COALESCE({alias}.ipeds_calibration_weight, 1.0)" if "ipeds_calibration_weight" in columns else "1.0"
    has_recent_fields = {
        "degree",
        "grad_year",
        "calibration_ipeds_completions",
        "ipeds_calibration_weight",
        "position_weight",
    }.issubset(columns)
    if not has_recent_fields:
        return f"""
            {star},
            {existing_ipeds_weight} AS ipeds_calibration_weight,
            {existing_combined_weight} AS final_weight,
            {existing_combined_weight} AS profile_weight
        """
    carried_weight = f"COALESCE(rsc.carried_ipeds_weight, rgc.carried_ipeds_weight, {alias}.ipeds_calibration_weight, 1.0)"
    recent_condition = f"""
        {alias}.degree IS NOT NULL
        AND {alias}.grad_year >= 2023
        AND {alias}.calibration_ipeds_completions IS NULL
    """
    return f"""
        {star},
        CASE
          WHEN {recent_condition}
          THEN {carried_weight}
          ELSE {existing_ipeds_weight}
        END AS ipeds_calibration_weight,
        CASE
          WHEN {recent_condition}
          THEN {position_weight} * {carried_weight}
          ELSE {existing_combined_weight}
        END AS final_weight,
        CASE
          WHEN {recent_condition}
          THEN {position_weight} * {carried_weight}
          ELSE {existing_combined_weight}
        END AS profile_weight
    """


def _project_columns(columns: frozenset[str], names: list[str]) -> str:
    return ",\n                ".join(
        name if name in columns else f"NULL AS {name}"
        for name in dict.fromkeys(names)
    )


@contextmanager
def _query_slot():
    QUERY_SEMAPHORE.acquire()
    try:
        yield
    finally:
        QUERY_SEMAPHORE.release()


def _manifest() -> dict[str, Any]:
    path = _platform_root() / "platform_manifest.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _connect() -> duckdb.DuckDBPyConnection:
    DUCKDB_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=":memory:")
    con.execute("SET enable_progress_bar=false")
    con.execute(f"SET threads={max(1, DUCKDB_THREADS)}")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT.replace(chr(39), '')}'")
    con.execute(f"SET temp_directory='{str(DUCKDB_TEMP_DIR).replace(chr(39), '')}'")
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


def _cache_payload(filters: QueryRequest, *, options: bool = False) -> dict[str, Any]:
    payload = filters.model_dump()
    if options:
        # Options only depend on the slice definition used to populate selects.
        # Excluding active tab/view/drilldown fields keeps option refreshes fast
        # when users move around the app or change chart-specific controls.
        keep = {
            "schools",
            "degree",
            "cip_level",
            "majors",
            "grad_years",
            "demographics",
            "compare_mode",
            "compare_dimension",
        }
        payload = {key: value for key, value in payload.items() if key in keep}
    return payload


def _response_cache_key(namespace: str, filters: QueryRequest, *, options: bool = False) -> str:
    manifest_path = _platform_root() / "platform_manifest.json"
    payload = {
        "namespace": namespace,
        "version": _manifest().get("version"),
        "manifest_mtime": int(manifest_path.stat().st_mtime) if manifest_path.exists() else 0,
        "root": str(_platform_root()),
        "filters": _cache_payload(filters, options=options),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _response_cache_get(key: str) -> dict[str, Any] | None:
    if RESPONSE_CACHE_TTL_SECONDS <= 0 or RESPONSE_CACHE_MAX_ENTRIES <= 0:
        return None
    now = time.monotonic()
    with RESPONSE_CACHE_LOCK:
        cached = RESPONSE_CACHE.get(key)
        if not cached:
            return None
        created_at, value = cached
        if now - created_at > RESPONSE_CACHE_TTL_SECONDS:
            RESPONSE_CACHE.pop(key, None)
            return None
        RESPONSE_CACHE.move_to_end(key)
        return deepcopy(value)


def _response_cache_set(key: str, value: dict[str, Any]) -> None:
    if RESPONSE_CACHE_TTL_SECONDS <= 0 or RESPONSE_CACHE_MAX_ENTRIES <= 0:
        return
    with RESPONSE_CACHE_LOCK:
        RESPONSE_CACHE[key] = (time.monotonic(), deepcopy(value))
        RESPONSE_CACHE.move_to_end(key)
        while len(RESPONSE_CACHE) > RESPONSE_CACHE_MAX_ENTRIES:
            RESPONSE_CACHE.popitem(last=False)


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


def _current_students_applicable(filters: QueryRequest) -> bool:
    """Current-student rows only support profile/student filters.

    Outcome filters such as "later degree = MBA" describe observed alumni
    outcomes, so current students cannot honestly satisfy them.
    """
    if not filters.include_current_students:
        return False
    if filters.postgrad.later_degree_type:
        return False
    if filters.postgrad.no_further_education is not None:
        return False
    return True


def _safe_limit(value: int) -> int:
    return min(max(value, 5), 50)


def _horizon_years(filters: QueryRequest) -> int | None:
    if not filters.horizon:
        return None
    if filters.horizon == "early_2025":
        return 0
    try:
        return int(str(filters.horizon).replace("yr", ""))
    except ValueError:
        return None


def _work_where(
    filters: QueryRequest,
    *,
    years_since_grad: int | None = None,
    include_grad_years: bool = True,
    postgrad_dataset: str | None = None,
) -> tuple[str, list[Any]]:
    """Filters for pre-aggregated work facts.

    Work facts are aggregated at school x degree x CIP4. They intentionally do
    not carry demographic or further-education filters; those still apply to
    the person-level core dashboard.
    """
    clauses: list[str] = []
    params: list[Any] = []
    _append_in_clause(clauses, params, "unitid", filters.schools)
    _append_in_clause(clauses, params, "degree", _degree_values(filters.degree))
    clauses.append("UPPER(cip_level) = 'CIP4'")
    if filters.majors and filters.cip_level == "cip4":
        _append_in_clause(clauses, params, "cip4", filters.majors)
    if include_grad_years:
        _append_in_clause(clauses, params, "grad_year", filters.grad_years)
    if years_since_grad is not None:
        clauses.append("years_since_grad = ?")
        params.append(years_since_grad)
    if postgrad_dataset and _work_dataset_supports_postgrad(postgrad_dataset):
        if filters.postgrad.later_degree_type:
            clauses.append("later_degree_type = ?")
            params.append(filters.postgrad.later_degree_type)
        elif filters.postgrad.no_further_education is not None:
            clauses.append("no_further_education_flag = ?")
            params.append(1 if filters.postgrad.no_further_education else 0)
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params


def _work_series_expr(filters: QueryRequest) -> str:
    if filters.compare_mode and filters.compare_dimension == "major":
        return "COALESCE(major_title, cip4)"
    if filters.compare_mode and filters.compare_dimension == "school":
        return "school_name"
    return "COALESCE(major_title, school_name)"


def _work_postgrad_filter_active(filters: QueryRequest) -> bool:
    return bool(
        filters.postgrad.later_degree_type
        or filters.postgrad.no_further_education is not None
    )


def _work_dataset_supports_postgrad(dataset: str) -> bool:
    if not _work_dataset_exists(dataset):
        return False
    columns = _dataset_columns(f"work_facts/{dataset}")
    return "later_degree_type" in columns and "no_further_education_flag" in columns


def _work_postgrad_unsupported(filters: QueryRequest, dataset: str) -> bool:
    return _work_postgrad_filter_active(filters) and not _work_dataset_supports_postgrad(dataset)


def _career_requires_person_level(filters: QueryRequest) -> bool:
    postgrad_requires_fallback = _work_postgrad_filter_active(filters) and not _work_dataset_supports_postgrad("annual_salary")
    return bool(
        postgrad_requires_fallback
        or filters.demographics.gender
        or filters.demographics.race_ethnicity
    )


def _create_slice(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> None:
    base_columns = _dataset_columns("base_fact")
    adjusted_select = _runtime_adjusted_select(base_columns, "b")
    base_source = _base_source_for_filters(filters)
    where_sql, params = _where(filters)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE filtered_base AS
        SELECT *
        FROM read_parquet(?)
        {where_sql}
        """,
        [base_source, *params],
    )
    cohort_where_sql, cohort_params = _where(filters, include_horizon=False)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE filtered_cohort_base AS
        SELECT *
        FROM read_parquet(?)
        {cohort_where_sql}
        """,
        [base_source, *cohort_params],
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE runtime_recent_calibration_source AS
        SELECT *
        FROM read_parquet(?)
        WHERE degree IS NOT NULL
          AND grad_year < 2023
          AND cip4 IS NOT NULL
          AND ipeds_calibration_weight IS NOT NULL
          AND calibration_ipeds_completions IS NOT NULL
          AND COALESCE(ipeds_calibration_source, 'none') <> 'none'
        """,
        [base_source],
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE recent_school_calibration AS
        SELECT unitid, degree, cip4, calibration_year, carried_ipeds_weight
        FROM (
          SELECT
            unitid,
            degree,
            cip4,
            grad_year AS calibration_year,
            AVG(ipeds_calibration_weight) AS carried_ipeds_weight,
            ROW_NUMBER() OVER (
              PARTITION BY unitid, degree, cip4
              ORDER BY grad_year DESC
            ) AS calibration_rank
          FROM runtime_recent_calibration_source
          GROUP BY unitid, degree, cip4, grad_year
        )
        WHERE calibration_rank = 1
        """,
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE recent_global_calibration AS
        SELECT degree, cip4, calibration_year, carried_ipeds_weight
        FROM (
          SELECT
            degree,
            cip4,
            grad_year AS calibration_year,
            AVG(ipeds_calibration_weight) AS carried_ipeds_weight,
            ROW_NUMBER() OVER (
              PARTITION BY degree, cip4
              ORDER BY grad_year DESC
            ) AS calibration_rank
          FROM runtime_recent_calibration_source
          GROUP BY degree, cip4, grad_year
        )
        WHERE calibration_rank = 1
        """,
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE slice AS
        SELECT
          {adjusted_select}
        FROM filtered_base b
        LEFT JOIN recent_school_calibration rsc
          ON b.unitid = rsc.unitid
         AND b.degree = rsc.degree
         AND b.cip4 = rsc.cip4
        LEFT JOIN recent_global_calibration rgc
          ON b.degree = rgc.degree
         AND b.cip4 = rgc.cip4
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE cohort_slice AS
        SELECT *
        FROM (
          SELECT
            {adjusted_select},
            ROW_NUMBER() OVER (
              PARTITION BY b.person_key, b.unitid, b.degree, b.cip2, b.cip4, b.cip6, b.grad_year
              ORDER BY CASE b.horizon
                WHEN '1yr' THEN 1
                WHEN '5yr' THEN 2
                WHEN '10yr' THEN 3
                WHEN 'early_2025' THEN 4
                ELSE 5
              END
            ) AS cohort_rank
          FROM filtered_cohort_base b
          LEFT JOIN recent_school_calibration rsc
            ON b.unitid = rsc.unitid
           AND b.degree = rsc.degree
           AND b.cip4 = rsc.cip4
          LEFT JOIN recent_global_calibration rgc
            ON b.degree = rgc.degree
           AND b.cip4 = rgc.cip4
        )
        WHERE cohort_rank = 1
        """
    )


def _create_current_slice(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> bool:
    return _create_current_slice_table(con, filters, "current_slice")


def _create_current_slice_table(con: duckdb.DuckDBPyConnection, filters: QueryRequest, table_name: str) -> bool:
    if not _current_students_applicable(filters):
        return False
    if not _dataset_exists("current_students_fact"):
        return False
    current_columns = _dataset_columns("current_students_fact")
    profile_weight_sql = _cohort_profile_weight_sql(current_columns)
    source_star = _source_star_without_profile(current_columns)
    high_conf_expr = "COALESCE(high_conf_major_flag, 0)" if "high_conf_major_flag" in current_columns else "0"
    cip_prob_expr = "COALESCE(cip_probability, 0)" if "cip_probability" in current_columns else "0"
    current_filters = filters.model_copy(deep=True)
    current_filters.grad_years = []
    where_sql, params = _where(current_filters, include_horizon=False, include_postgrad=False)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
        SELECT *
        FROM (
          SELECT
            {source_star},
            {profile_weight_sql} AS profile_weight,
            ROW_NUMBER() OVER (
              PARTITION BY person_key, unitid, degree, cip2, cip4, cip6, grad_year
              ORDER BY {high_conf_expr} DESC, {cip_prob_expr} DESC, {profile_weight_sql} DESC
            ) AS current_rank
          FROM read_parquet(?)
          {where_sql}
        )
        WHERE current_rank = 1
        """,
        [_dataset_glob("current_students_fact"), *params],
    )
    return True


def _create_cohort_slice_table(con: duckdb.DuckDBPyConnection, filters: QueryRequest, table_name: str) -> None:
    base_columns = _dataset_columns("base_fact")
    adjusted_select = _runtime_adjusted_select(base_columns, "b")
    base_source = _base_source_for_filters(filters)
    cohort_where_sql, cohort_params = _where(filters, include_horizon=False)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
        SELECT *
        FROM (
          SELECT
            {adjusted_select},
            ROW_NUMBER() OVER (
              PARTITION BY b.person_key, b.unitid, b.degree, b.cip2, b.cip4, b.cip6, b.grad_year
              ORDER BY CASE b.horizon
                WHEN '1yr' THEN 1
                WHEN '5yr' THEN 2
                WHEN '10yr' THEN 3
                WHEN 'early_2025' THEN 4
                ELSE 5
              END
            ) AS cohort_rank
          FROM (
            SELECT *
            FROM read_parquet(?)
            {cohort_where_sql}
          ) b
          LEFT JOIN recent_school_calibration rsc
            ON b.unitid = rsc.unitid
           AND b.degree = rsc.degree
           AND b.cip4 = rsc.cip4
          LEFT JOIN recent_global_calibration rgc
            ON b.degree = rgc.degree
           AND b.cip4 = rgc.cip4
        )
        WHERE cohort_rank = 1
        """,
        [base_source, *cohort_params],
    )


@lru_cache(maxsize=1)
def _static_options() -> dict[str, Any]:
    con = _connect()
    try:
        schools = _records_from_query(
            con,
            """
            SELECT unitid, MAX(school_name) AS name, COUNT(*) AS alumni
            FROM read_parquet(?)
            GROUP BY unitid
            ORDER BY name
            """,
            [_dataset_glob("base_fact")],
        )
        degree_rows = _records_from_query(
            con,
            """
            SELECT degree, COUNT(*) AS alumni
            FROM read_parquet(?)
            GROUP BY degree
            ORDER BY alumni DESC
            """,
            [_dataset_glob("base_fact")],
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
        if _dataset_exists("current_students_fact"):
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
        "base_fact_exists": _dataset_exists("base_fact"),
        "current_students_fact_exists": _dataset_exists("current_students_fact"),
        "work_facts_exists": bool((_platform_root() / "work_facts").exists()),
        "base_fact_file_count": len(_dataset_files("base_fact")),
        "current_students_fact_file_count": len(_dataset_files("current_students_fact")),
        "work_fact_file_count": sum(
            len(_dataset_files(f"work_facts/{name}"))
            for name in [
                "annual_employers",
                "annual_geography",
                "annual_industries",
                "annual_roles",
                "annual_salary",
                "annual_seniority",
                "career_archetypes",
                "employer_tenure",
                "internship_employers",
                "internship_geography",
                "internship_industries",
                "internship_roles",
                "internship_summary",
                "mobility",
            ]
        ),
    }


@app.post("/api/options")
def options(filters: QueryRequest, _: None = Depends(require_internal_password)) -> dict[str, Any]:
    cache_key = _response_cache_key("options", filters, options=True)
    cached = _response_cache_get(cache_key)
    if cached is not None:
        return cached
    with _query_slot():
        cip_col = _cip_col(filters)
        limit = 500
        static = _static_options()
        if not filters.schools and static.get("schools"):
            default_school = next(
                (
                    row
                    for row in static["schools"]
                    if "columbia university in the city of new york" in str(row.get("name", "")).lower()
                ),
                static["schools"][0],
            )
            filters = filters.model_copy(deep=True)
            filters.schools = [str(default_school["unitid"])]
        con = _connect()
        try:
            base_columns = _dataset_columns("base_fact")
            profile_weight_sql = _cohort_profile_weight_sql(base_columns)
            base_source = _base_source_for_filters(filters)
            option_projection = _project_columns(
                base_columns,
                [
                    "person_key",
                    "unitid",
                    "degree",
                    "cip2",
                    "cip4",
                    "cip6",
                    "grad_year",
                    "horizon",
                    "major_title",
                    "gender",
                    "race_ethnicity",
                    "later_degree_type",
                ],
            )
            where_sql, params = _where(filters, include_horizon=False, include_postgrad=False)
            con.execute(
                f"""
                CREATE TEMP TABLE option_cohort AS
                SELECT *
                FROM (
                  SELECT
                    {option_projection},
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
                [base_source, *params],
            )
            majors = _records_from_query(
                con,
                f"""
                WITH major_counts AS (
                  SELECT
                    {cip_col} AS code,
                    MAX(major_title) AS title,
                    ROUND(SUM(profile_weight), 2) AS alumni
                  FROM option_cohort
                  WHERE {cip_col} IS NOT NULL
                  GROUP BY {cip_col}
                )
                SELECT code, COALESCE(title, code) AS title, alumni
                FROM major_counts
                WHERE alumni > ?
                ORDER BY alumni DESC, title
                LIMIT {limit}
                """,
                [MIN_CELL_WEIGHT],
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
            result = {
                **static,
                "major_options": majors,
                "demographics": demographics,
                "later_degrees": later_degrees,
                "meta": {
                    "data_version": _manifest().get("version"),
                    "min_cell_weight": MIN_CELL_WEIGHT,
                    "salary_min_weight": SALARY_MIN_WEIGHT,
                },
            }
            _response_cache_set(cache_key, result)
            return result
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
          ROUND(c.alumni, 2) AS alumni,
          c.raw_rows,
          ROUND(o.salary_weight) AS salary_weight,
          CASE WHEN o.salary_weight > ? THEN ROUND(o.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN o.salary_weight > ? THEN ROUND(o.median_salary) ELSE NULL END AS median_salary,
          o.unique_employers,
          o.unique_locations,
          ROUND(c.later_degree_n) AS later_degree_n,
          ROUND(c.no_further_n) AS no_further_n,
          ROUND(100.0 * c.later_degree_n / NULLIF(c.alumni, 0), 1) AS later_degree_pct
        FROM cohort c
        CROSS JOIN outcomes o
        """,
        [SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT],
    )


def _salary_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    rows = _records_from_query(
        con,
        """
        WITH by_year AS (
          SELECT
            grad_year,
            MAX(COALESCE(partial_horizon_flag, 0)) AS partial_horizon,
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
          CAST(partial_horizon AS INTEGER) AS partial_horizon,
          ROUND(alumni, 2) AS alumni,
          ROUND(salary_weight) AS salary_weight,
          CASE WHEN salary_weight > ? THEN ROUND(weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN salary_weight > ? THEN ROUND(median_salary) ELSE NULL END AS median_salary
        FROM by_year
        WHERE salary_weight > ?
        ORDER BY grad_year
        """,
        [SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT],
    )
    if filters.horizon == "1yr":
        rows.extend(_early_2025_salary_trend(con, filters, by_school=False))
    return sorted(rows, key=lambda row: (row.get("grad_year") or 0, row.get("partial_horizon") or 0, row.get("school_name") or ""))


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
        SELECT grad_year, ROUND(alumni, 2) AS alumni
        FROM by_year
        WHERE alumni > ?
        ORDER BY grad_year
        """,
        [MIN_CELL_WEIGHT],
    )


def _salary_trend_by_school(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    rows = _records_from_query(
        con,
        """
        WITH by_year AS (
          SELECT
            unitid,
            MAX(school_name) AS school_name,
            grad_year,
            MAX(COALESCE(partial_horizon_flag, 0)) AS partial_horizon,
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
          CAST(partial_horizon AS INTEGER) AS partial_horizon,
          ROUND(alumni, 2) AS alumni,
          ROUND(salary_weight) AS salary_weight,
          CASE WHEN salary_weight > ? THEN ROUND(weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN salary_weight > ? THEN ROUND(median_salary) ELSE NULL END AS median_salary
        FROM by_year
        WHERE salary_weight > ?
        ORDER BY school_name, grad_year
        """,
        [SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT],
    )
    if filters.horizon == "1yr":
        rows.extend(_early_2025_salary_trend(con, filters, by_school=True))
    return sorted(rows, key=lambda row: (row.get("school_name") or "", row.get("grad_year") or 0, row.get("partial_horizon") or 0))


def _early_2025_salary_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest, *, by_school: bool) -> list[dict[str, Any]]:
    early_filters = filters.model_copy(deep=True)
    early_filters.horizon = "early_2025"
    base_columns = _dataset_columns("base_fact")
    adjusted_select = _runtime_adjusted_select(base_columns, "b")
    base_source = _base_source_for_filters(filters)
    where_sql, params = _where(early_filters)
    year_clause = "grad_year = 2025"
    if where_sql:
        where_sql = f"{where_sql} AND {year_clause}"
    else:
        where_sql = f" WHERE {year_clause}"
    if by_school:
        return _records_from_query(
            con,
            f"""
            WITH early_slice AS (
              SELECT
                {adjusted_select}
              FROM (
                SELECT *
                FROM read_parquet(?)
                {where_sql}
              ) b
              LEFT JOIN recent_school_calibration rsc
                ON b.unitid = rsc.unitid
               AND b.degree = rsc.degree
               AND b.cip4 = rsc.cip4
              LEFT JOIN recent_global_calibration rgc
                ON b.degree = rgc.degree
               AND b.cip4 = rgc.cip4
            ),
            by_year AS (
              SELECT
                unitid,
                MAX(school_name) AS school_name,
                grad_year,
                SUM(final_weight) AS alumni,
                SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
                SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_mean_salary,
                quantile_cont(salary, 0.5) AS median_salary
              FROM early_slice
              GROUP BY unitid, grad_year
            )
            SELECT
              unitid,
              school_name,
              grad_year,
              1 AS partial_horizon,
              ROUND(alumni, 2) AS alumni,
              ROUND(salary_weight) AS salary_weight,
              CASE WHEN salary_weight > ? THEN ROUND(weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
              CASE WHEN salary_weight > ? THEN ROUND(median_salary) ELSE NULL END AS median_salary
            FROM by_year
            WHERE salary_weight > ?
            ORDER BY school_name, grad_year
            """,
            [base_source, *params, SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT],
        )
    return _records_from_query(
        con,
        f"""
        WITH early_slice AS (
          SELECT
            {adjusted_select}
          FROM (
            SELECT *
            FROM read_parquet(?)
            {where_sql}
          ) b
          LEFT JOIN recent_school_calibration rsc
            ON b.unitid = rsc.unitid
           AND b.degree = rsc.degree
           AND b.cip4 = rsc.cip4
          LEFT JOIN recent_global_calibration rgc
            ON b.degree = rgc.degree
           AND b.cip4 = rgc.cip4
        ),
        by_year AS (
          SELECT
            grad_year,
            SUM(final_weight) AS alumni,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_mean_salary,
            quantile_cont(salary, 0.5) AS median_salary
          FROM early_slice
          GROUP BY grad_year
        )
        SELECT
          grad_year,
          1 AS partial_horizon,
          ROUND(alumni, 2) AS alumni,
          ROUND(salary_weight) AS salary_weight,
          CASE WHEN salary_weight > ? THEN ROUND(weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN salary_weight > ? THEN ROUND(median_salary) ELSE NULL END AS median_salary
        FROM by_year
        WHERE salary_weight > ?
        ORDER BY grad_year
        """,
        [base_source, *params, SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT],
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
        SELECT unitid, school_name, grad_year, ROUND(alumni, 2) AS alumni
        FROM by_year
        WHERE alumni > ?
        ORDER BY school_name, grad_year
        """,
        [MIN_CELL_WEIGHT],
    )


def _salary_trend_by_major(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    cip_col = _cip_col(filters)
    rows = _records_from_query(
        con,
        f"""
        WITH by_year AS (
          SELECT
            {cip_col} AS code,
            MAX(major_title) AS title,
            grad_year,
            MAX(COALESCE(partial_horizon_flag, 0)) AS partial_horizon,
            SUM(final_weight) AS alumni,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_mean_salary,
            quantile_cont(salary, 0.5) AS median_salary
          FROM slice
          WHERE grad_year IS NOT NULL
            AND {cip_col} IS NOT NULL
          GROUP BY {cip_col}, grad_year
        )
        SELECT
          code,
          COALESCE(title, code) AS title,
          grad_year,
          CAST(partial_horizon AS INTEGER) AS partial_horizon,
          ROUND(alumni, 2) AS alumni,
          ROUND(salary_weight) AS salary_weight,
          CASE WHEN salary_weight > ? THEN ROUND(weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN salary_weight > ? THEN ROUND(median_salary) ELSE NULL END AS median_salary
        FROM by_year
        WHERE salary_weight > ?
        ORDER BY title, grad_year
        """,
        [SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT],
    )
    if filters.horizon == "1yr":
        rows.extend(_early_2025_salary_trend_by_major(con, filters))
    return sorted(rows, key=lambda row: (row.get("title") or "", row.get("grad_year") or 0, row.get("partial_horizon") or 0))


def _early_2025_salary_trend_by_major(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    cip_col = _cip_col(filters)
    early_filters = filters.model_copy(deep=True)
    early_filters.horizon = "early_2025"
    base_source = _base_source_for_filters(filters)
    where_sql, params = _where(early_filters)
    year_clause = "grad_year = 2025"
    if where_sql:
        where_sql = f"{where_sql} AND {year_clause}"
    else:
        where_sql = f" WHERE {year_clause}"
    return _records_from_query(
        con,
        f"""
        WITH by_year AS (
          SELECT
            {cip_col} AS code,
            MAX(major_title) AS title,
            grad_year,
            SUM(final_weight) AS alumni,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_mean_salary,
            quantile_cont(salary, 0.5) AS median_salary
          FROM read_parquet(?)
          {where_sql}
            AND {cip_col} IS NOT NULL
          GROUP BY {cip_col}, grad_year
        )
        SELECT
          code,
          COALESCE(title, code) AS title,
          grad_year,
          1 AS partial_horizon,
          ROUND(alumni, 2) AS alumni,
          ROUND(salary_weight) AS salary_weight,
          CASE WHEN salary_weight > ? THEN ROUND(weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN salary_weight > ? THEN ROUND(median_salary) ELSE NULL END AS median_salary
        FROM by_year
        WHERE salary_weight > ?
        ORDER BY title, grad_year
        """,
        [base_source, *params, SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT],
    )


def _alumni_trend_by_major(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    cip_col = _cip_col(filters)
    denominator_filters = filters.model_copy(deep=True)
    denominator_filters.majors = []
    _create_cohort_slice_table(con, denominator_filters, "alumni_trend_major_total")
    return _records_from_query(
        con,
        f"""
        WITH by_year AS (
          SELECT
            {cip_col} AS code,
            MAX(major_title) AS title,
            grad_year,
            SUM(profile_weight) AS alumni
          FROM cohort_slice
          WHERE grad_year IS NOT NULL
            AND {cip_col} IS NOT NULL
          GROUP BY {cip_col}, grad_year
        ),
        totals AS (
          SELECT
            grad_year,
            SUM(profile_weight) AS total_alumni
          FROM alumni_trend_major_total
          WHERE grad_year IS NOT NULL
            AND {cip_col} IS NOT NULL
          GROUP BY grad_year
        )
        SELECT
          b.code,
          COALESCE(b.title, b.code) AS title,
          b.grad_year,
          ROUND(b.alumni, 2) AS alumni,
          ROUND(t.total_alumni, 2) AS total_alumni,
          ROUND(100.0 * b.alumni / NULLIF(t.total_alumni, 0), 2) AS share_pct
        FROM by_year b
        JOIN totals t USING (grad_year)
        WHERE b.alumni > ?
        ORDER BY title, b.grad_year
        """,
        [MIN_CELL_WEIGHT],
    )


def _salary_distribution(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    bucket_count = max(12, min(80, SALARY_DISTRIBUTION_BUCKETS))
    max_bucket = bucket_count - 1
    summary = _single_record(
        con,
        """
        WITH salaries AS (
          SELECT salary, final_weight
          FROM slice
          WHERE salary IS NOT NULL
            AND salary > 0
            AND final_weight > 0
        )
        SELECT
          ROUND(SUM(final_weight), 2) AS salary_weight,
          ROUND(quantile_cont(salary, 0.10)) AS p10,
          ROUND(quantile_cont(salary, 0.25)) AS p25,
          ROUND(quantile_cont(salary, 0.50)) AS median,
          ROUND(quantile_cont(salary, 0.75)) AS p75,
          ROUND(quantile_cont(salary, 0.90)) AS p90
        FROM salaries
        """,
    )
    rows = _records_from_query(
        con,
        f"""
        WITH salaries AS (
          SELECT salary, final_weight
          FROM slice
          WHERE salary IS NOT NULL
            AND salary > 0
            AND final_weight > 0
        ),
        bounds AS (
          SELECT
            quantile_cont(salary, 0.02) AS lo,
            quantile_cont(salary, 0.98) AS hi
          FROM salaries
        ),
        binned AS (
          SELECT
            CASE
              WHEN (SELECT hi - lo FROM bounds) <= 0 THEN 0
              ELSE LEAST({max_bucket}, GREATEST(0, FLOOR((LEAST(GREATEST(s.salary, b.lo), b.hi) - b.lo) / NULLIF((b.hi - b.lo) / {float(bucket_count)}, 0))))
            END AS bucket,
            s.final_weight,
            b.lo,
            b.hi
          FROM salaries s
          CROSS JOIN bounds b
        )
        SELECT
          ROUND(MIN(lo + bucket * ((hi - lo) / {float(bucket_count)}))) AS bin_start,
          ROUND(MIN(lo + (bucket + 1) * ((hi - lo) / {float(bucket_count)}))) AS bin_end,
          ROUND(SUM(final_weight), 2) AS n
        FROM binned
        GROUP BY bucket
        HAVING SUM(final_weight) > 0
        ORDER BY bucket
        """,
    )
    return {"summary": summary, "bins": rows}


def _salary_distribution_by_entity(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, Any]:
    bucket_count = max(12, min(80, SALARY_DISTRIBUTION_BUCKETS))
    max_bucket = bucket_count - 1
    if filters.compare_dimension == "major":
        code_expr = _cip_col(filters)
        label_expr = "major_title"
    else:
        code_expr = "unitid"
        label_expr = "school_name"
    summaries = _records_from_query(
        con,
        f"""
        WITH salaries AS (
          SELECT
            CAST({code_expr} AS VARCHAR) AS code,
            COALESCE({label_expr}, CAST({code_expr} AS VARCHAR)) AS label,
            salary,
            final_weight
          FROM slice
          WHERE salary IS NOT NULL
            AND salary > 0
            AND final_weight > 0
            AND {code_expr} IS NOT NULL
        )
        SELECT
          code,
          MAX(label) AS label,
          ROUND(SUM(final_weight), 2) AS salary_weight,
          ROUND(quantile_cont(salary, 0.10)) AS p10,
          ROUND(quantile_cont(salary, 0.25)) AS p25,
          ROUND(quantile_cont(salary, 0.50)) AS median,
          ROUND(quantile_cont(salary, 0.75)) AS p75,
          ROUND(quantile_cont(salary, 0.90)) AS p90
        FROM salaries
        GROUP BY code
        HAVING SUM(final_weight) > ?
        ORDER BY salary_weight DESC
        """,
        [SALARY_MIN_WEIGHT],
    )
    rows = _records_from_query(
        con,
        f"""
        WITH salaries AS (
          SELECT
            CAST({code_expr} AS VARCHAR) AS code,
            COALESCE({label_expr}, CAST({code_expr} AS VARCHAR)) AS label,
            salary,
            final_weight
          FROM slice
          WHERE salary IS NOT NULL
            AND salary > 0
            AND final_weight > 0
            AND {code_expr} IS NOT NULL
        ),
        bounds AS (
          SELECT
            quantile_cont(salary, 0.02) AS lo,
            quantile_cont(salary, 0.98) AS hi
          FROM salaries
        ),
        group_totals AS (
          SELECT code, MAX(label) AS label, SUM(final_weight) AS salary_weight
          FROM salaries
          GROUP BY code
          HAVING SUM(final_weight) > ?
        ),
        binned AS (
          SELECT
            s.code,
            CASE
              WHEN (SELECT hi - lo FROM bounds) <= 0 THEN 0
              ELSE LEAST({max_bucket}, GREATEST(0, FLOOR((LEAST(GREATEST(s.salary, b.lo), b.hi) - b.lo) / NULLIF((b.hi - b.lo) / {float(bucket_count)}, 0))))
            END AS bucket,
            s.final_weight,
            b.lo,
            b.hi
          FROM salaries s
          JOIN group_totals gt USING (code)
          CROSS JOIN bounds b
        )
        SELECT
          b.code,
          MAX(gt.label) AS label,
          ROUND(MIN(lo + bucket * ((hi - lo) / {float(bucket_count)}))) AS bin_start,
          ROUND(MIN(lo + (bucket + 1) * ((hi - lo) / {float(bucket_count)}))) AS bin_end,
          ROUND(SUM(b.final_weight), 2) AS n,
          ROUND(100.0 * SUM(b.final_weight) / NULLIF(MAX(gt.salary_weight), 0), 2) AS share_pct
        FROM binned b
        JOIN group_totals gt USING (code)
        GROUP BY b.code, bucket
        HAVING SUM(b.final_weight) > 0
        ORDER BY label, bucket
        """,
        [SALARY_MIN_WEIGHT],
    )
    return {"summary": summaries, "bins": rows}


def _current_student_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    if not _current_students_applicable(filters) or not _dataset_exists("current_students_fact"):
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
        HAVING SUM(profile_weight) > ?
        ORDER BY grad_year
        """,
        [MIN_CELL_WEIGHT],
    )


def _current_student_trend_by_major(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    if not _current_students_applicable(filters) or not _dataset_exists("current_students_fact"):
        return []
    if not _create_current_slice(con, filters):
        return []
    cip_col = _cip_col(filters)
    denominator_filters = filters.model_copy(deep=True)
    denominator_filters.majors = []
    if not _create_current_slice_table(con, denominator_filters, "current_major_total_slice"):
        return []
    return _records_from_query(
        con,
        f"""
        WITH by_year AS (
          SELECT
            {cip_col} AS code,
            COALESCE(MAX(major_title), {cip_col}) AS title,
            grad_year,
            SUM(profile_weight) AS current_students
          FROM current_slice
          WHERE grad_year IS NOT NULL
            AND {cip_col} IS NOT NULL
          GROUP BY {cip_col}, grad_year
        ),
        totals AS (
          SELECT
            grad_year,
            SUM(profile_weight) AS total_current_students
          FROM current_major_total_slice
          WHERE grad_year IS NOT NULL
            AND {cip_col} IS NOT NULL
          GROUP BY grad_year
        )
        SELECT
          b.code,
          b.title,
          b.grad_year,
          ROUND(b.current_students, 2) AS current_students,
          ROUND(t.total_current_students, 2) AS total_current_students,
          ROUND(100.0 * b.current_students / NULLIF(t.total_current_students, 0), 2) AS share_pct
        FROM by_year b
        JOIN totals t USING (grad_year)
        WHERE b.current_students > ?
        ORDER BY b.title, b.grad_year
        """,
        [MIN_CELL_WEIGHT],
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
          ROUND(c.alumni, 2) AS alumni,
          ROUND(o.salary_weight) AS salary_weight,
          CASE WHEN o.salary_weight > ? THEN ROUND(o.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN o.salary_weight > ? THEN ROUND(o.median_salary) ELSE NULL END AS median_salary,
          o.unique_employers,
          ROUND(100.0 * c.later_degree_n / NULLIF(c.alumni, 0), 1) AS later_degree_pct
        FROM cohort c
        LEFT JOIN outcomes o USING (unitid)
        WHERE c.alumni > ?
        ORDER BY o.weighted_mean_salary DESC NULLS LAST, c.alumni DESC
        """,
        [SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, MIN_CELL_WEIGHT],
    )


def _school_outcome_comparison(
    con: duckdb.DuckDBPyConnection,
    label_sql: str,
    where_extra: str,
    min_weight: float,
    filters: QueryRequest,
) -> list[dict[str, Any]]:
    limit = _safe_limit(filters.top_n)
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            unitid AS code,
            school_name AS title,
            {label_sql} AS label,
            final_weight,
            salary
          FROM slice
          WHERE {label_sql} IS NOT NULL
            AND TRIM(CAST({label_sql} AS VARCHAR)) <> ''
            AND LOWER(TRIM(CAST({label_sql} AS VARCHAR))) NOT IN ('empty', 'unknown', 'other')
            {where_extra}
        ),
        top_labels AS (
          SELECT label, SUM(final_weight) AS total_n
          FROM eligible
          GROUP BY label
          HAVING SUM(final_weight) > ?
          ORDER BY total_n DESC
          LIMIT {limit}
        ),
        denom AS (
          SELECT code, SUM(final_weight) AS total_n
          FROM eligible
          GROUP BY code
        ),
        grouped AS (
          SELECT
            e.code,
            MAX(e.title) AS title,
            e.label,
            SUM(e.final_weight) AS n,
            SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight * e.salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight ELSE 0 END), 0) AS weighted_mean_salary,
            quantile_cont(e.salary, 0.5) AS median_salary
          FROM eligible e
          JOIN top_labels t USING (label)
          GROUP BY e.code, e.label
        )
        SELECT
          g.code,
          g.title,
          g.label,
          ROUND(g.n, 2) AS n,
          ROUND(100.0 * g.n / NULLIF(d.total_n, 0), 2) AS share_pct,
          ROUND(g.salary_weight) AS salary_weight,
          CASE WHEN g.salary_weight > ? THEN ROUND(g.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN g.salary_weight > ? THEN ROUND(g.median_salary) ELSE NULL END AS median_salary
        FROM grouped g
        JOIN denom d USING (code)
        WHERE g.n > ?
        ORDER BY g.label, g.title
        """,
        [min_weight, SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, min_weight],
    )


def _school_cohort_label_comparison(
    con: duckdb.DuckDBPyConnection,
    label_sql: str,
    where_extra: str,
    min_weight: float,
    filters: QueryRequest,
) -> list[dict[str, Any]]:
    limit = _safe_limit(filters.top_n)
    return _records_from_query(
        con,
        f"""
        WITH raw AS (
          SELECT
            unitid AS code,
            school_name AS title,
            {label_sql} AS label,
            profile_weight
          FROM cohort_slice
          WHERE 1 = 1
            {where_extra}
        ),
        eligible AS (
          SELECT *
          FROM raw
          WHERE label IS NOT NULL
            AND TRIM(CAST(label AS VARCHAR)) <> ''
            AND LOWER(TRIM(CAST(label AS VARCHAR))) NOT IN ('empty', 'unknown', 'other')
        ),
        top_labels AS (
          SELECT label, SUM(profile_weight) AS total_n
          FROM eligible
          GROUP BY label
          HAVING SUM(profile_weight) > ?
          ORDER BY total_n DESC
          LIMIT {limit}
        ),
        denom AS (
          SELECT code, SUM(profile_weight) AS total_n
          FROM eligible
          GROUP BY code
        ),
        grouped AS (
          SELECT
            e.code,
            MAX(e.title) AS title,
            e.label,
            SUM(e.profile_weight) AS n
          FROM eligible e
          JOIN top_labels t USING (label)
          GROUP BY e.code, e.label
        )
        SELECT
          g.code,
          g.title,
          g.label,
          ROUND(g.n, 2) AS n,
          ROUND(100.0 * g.n / NULLIF(d.total_n, 0), 2) AS share_pct
        FROM grouped g
        JOIN denom d USING (code)
        WHERE g.n > ?
        ORDER BY g.label, g.title
        """,
        [min_weight, min_weight],
    )


def _school_employer_comparison(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    same_school_filter = _same_school_employer_filter(filters)
    return _school_outcome_comparison(
        con,
        "employer",
        f"""
            AND employer IS NOT NULL
            AND employer <> ''
            AND unknown_employer_flag = 0
            AND named_employer_flag = 1
            AND career_employer_flag = 1
            {same_school_filter}
        """,
        EMPLOYER_ROW_MIN_WEIGHT,
        filters,
    )


def _school_geography_comparison(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    location_expr = _location_label_expr()
    return _school_outcome_comparison(
        con,
        location_expr,
        """
            AND COALESCE(location, city) IS NOT NULL
            AND LOWER(COALESCE(location, city)) NOT IN ('empty', 'unknown')
        """,
        GEOGRAPHY_ROW_MIN_WEIGHT,
        filters,
    )


def _school_role_comparison(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, list[dict[str, Any]]]:
    return {
        "roles": _school_outcome_comparison(
            con,
            "role_k50_v3",
            """
                AND role_k50_v3 IS NOT NULL
                AND role_k50_v3 <> ''
                AND NOT (degree = 'Bachelors' AND horizon = '1yr' AND role_k50_v3 = 'Corporate Attorney')
            """,
            MIN_CELL_WEIGHT,
            filters,
        ),
        "industries": _school_outcome_comparison(
            con,
            "industry_k50",
            """
                AND industry_k50 IS NOT NULL
                AND industry_k50 <> ''
            """,
            MIN_CELL_WEIGHT,
            filters,
        ),
    }


def _school_demographic_comparison(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, list[dict[str, Any]]]:
    return {
        "gender": _school_cohort_label_comparison(con, "gender", "AND gender IS NOT NULL AND gender <> ''", MIN_CELL_WEIGHT, filters),
        "race_ethnicity": _school_cohort_label_comparison(
            con,
            "race_ethnicity",
            "AND race_ethnicity IS NOT NULL AND race_ethnicity <> ''",
            MIN_CELL_WEIGHT,
            filters,
        ),
    }


def _school_postgrad_comparison(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    return _school_cohort_label_comparison(
        con,
        "CASE WHEN later_degree_type IS NOT NULL AND later_degree_type <> '' THEN later_degree_type WHEN no_further_education_flag = 1 THEN 'No further education' ELSE NULL END",
        "",
        MIN_CELL_WEIGHT,
        filters,
    )


def _major_comparison(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    cip_col = _cip_col(filters)
    denominator_filters = filters.model_copy(deep=True)
    denominator_filters.majors = []
    _create_cohort_slice_table(con, denominator_filters, "major_compare_total_cohort")
    return _records_from_query(
        con,
        f"""
        WITH cohort AS (
          SELECT
            {cip_col} AS code,
            MAX(major_title) AS title,
            SUM(profile_weight) AS alumni,
            SUM(CASE WHEN later_degree_type IS NOT NULL THEN profile_weight ELSE 0 END) AS later_degree_n
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
            quantile_cont(salary, 0.5) AS median_salary,
            COUNT(DISTINCT CASE WHEN employer IS NOT NULL AND employer <> '' AND unknown_employer_flag = 0 THEN employer END) AS unique_employers
          FROM slice
          WHERE {cip_col} IS NOT NULL
          GROUP BY {cip_col}
        ),
        total_cohort AS (
          SELECT SUM(profile_weight) AS total_alumni
          FROM major_compare_total_cohort
          WHERE {cip_col} IS NOT NULL
        )
        SELECT
          c.code,
          COALESCE(c.title, c.code) AS title,
          ROUND(c.alumni, 2) AS alumni,
          ROUND(t.total_alumni, 2) AS total_alumni,
          ROUND(100.0 * c.alumni / NULLIF(t.total_alumni, 0), 2) AS alumni_share_pct,
          ROUND(o.salary_weight) AS salary_weight,
          CASE WHEN o.salary_weight > ? THEN ROUND(o.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN o.salary_weight > ? THEN ROUND(o.median_salary) ELSE NULL END AS median_salary,
          o.unique_employers,
          ROUND(100.0 * c.later_degree_n / NULLIF(c.alumni, 0), 1) AS later_degree_pct
        FROM cohort c
        CROSS JOIN total_cohort t
        LEFT JOIN outcomes o USING (code)
        WHERE c.alumni > ?
        ORDER BY o.weighted_mean_salary DESC NULLS LAST, c.alumni DESC
        """,
        [SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, MIN_CELL_WEIGHT],
    )


def _major_employer_comparison(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    cip_col = _cip_col(filters)
    limit = _safe_limit(filters.top_n)
    same_school_filter = _same_school_employer_filter(filters)
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            {cip_col} AS code,
            COALESCE(major_title, {cip_col}) AS title,
            employer AS label,
            final_weight,
            salary
          FROM slice
          WHERE {cip_col} IS NOT NULL
            AND employer IS NOT NULL
            AND employer <> ''
            AND unknown_employer_flag = 0
            AND named_employer_flag = 1
            AND career_employer_flag = 1
            {same_school_filter}
        ),
        top_labels AS (
          SELECT label, SUM(final_weight) AS total_n
          FROM eligible
          GROUP BY label
          HAVING SUM(final_weight) > ?
          ORDER BY total_n DESC
          LIMIT {limit}
        ),
        denom AS (
          SELECT code, SUM(final_weight) AS total_n
          FROM eligible
          GROUP BY code
        ),
        grouped AS (
          SELECT
            e.code,
            MAX(e.title) AS title,
            e.label,
            SUM(e.final_weight) AS n,
            SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight * e.salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight ELSE 0 END), 0) AS weighted_mean_salary
          FROM eligible e
          JOIN top_labels t USING (label)
          GROUP BY e.code, e.label
        )
        SELECT
          g.code,
          g.title,
          g.label,
          ROUND(g.n) AS n,
          ROUND(100.0 * g.n / NULLIF(d.total_n, 0), 2) AS share_pct,
          CASE WHEN g.salary_weight > ? THEN ROUND(g.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary
        FROM grouped g
        JOIN denom d USING (code)
        WHERE g.n > ?
        ORDER BY g.label, g.title
        """,
        [EMPLOYER_ROW_MIN_WEIGHT, SALARY_MIN_WEIGHT, EMPLOYER_ROW_MIN_WEIGHT],
    )


def _major_geography_comparison(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    cip_col = _cip_col(filters)
    limit = min(_safe_limit(filters.top_n), 12)
    location_expr = _location_label_expr()
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            {cip_col} AS code,
            COALESCE(major_title, {cip_col}) AS title,
            {location_expr} AS label,
            final_weight,
            salary
          FROM slice
          WHERE {cip_col} IS NOT NULL
            AND COALESCE(location, city) IS NOT NULL
            AND LOWER(COALESCE(location, city)) NOT IN ('empty', 'unknown')
        ),
        top_labels AS (
          SELECT label, SUM(final_weight) AS total_n
          FROM eligible
          GROUP BY label
          HAVING SUM(final_weight) > ?
          ORDER BY total_n DESC
          LIMIT {limit}
        ),
        denom AS (
          SELECT code, SUM(final_weight) AS total_n
          FROM eligible
          GROUP BY code
        ),
        grouped AS (
          SELECT
            e.code,
            MAX(e.title) AS title,
            e.label,
            SUM(e.final_weight) AS n,
            SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight ELSE 0 END) AS salary_weight,
            SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight * e.salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight ELSE 0 END), 0) AS weighted_mean_salary
          FROM eligible e
          JOIN top_labels t USING (label)
          GROUP BY e.code, e.label
        )
        SELECT
          g.code,
          g.title,
          g.label,
          ROUND(g.n, 2) AS n,
          ROUND(100.0 * g.n / NULLIF(d.total_n, 0), 2) AS share_pct,
          CASE WHEN g.salary_weight > ? THEN ROUND(g.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary
        FROM grouped g
        JOIN denom d USING (code)
        WHERE g.n > ?
        ORDER BY g.label, g.title
        """,
        [GEOGRAPHY_ROW_MIN_WEIGHT, SALARY_MIN_WEIGHT, GEOGRAPHY_ROW_MIN_WEIGHT],
    )


def _major_role_comparison(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, list[dict[str, Any]]]:
    cip_col = _cip_col(filters)
    limit = _safe_limit(filters.top_n)

    def group(column: str, *, exclude_corporate_attorney: bool = False) -> list[dict[str, Any]]:
        exclusion = "AND NOT (degree = 'Bachelors' AND horizon = '1yr' AND role_k50_v3 = 'Corporate Attorney')" if exclude_corporate_attorney else ""
        return _records_from_query(
            con,
            f"""
            WITH eligible AS (
              SELECT
                {cip_col} AS code,
                COALESCE(major_title, {cip_col}) AS title,
                {column} AS label,
                final_weight,
                salary
              FROM slice
              WHERE {cip_col} IS NOT NULL
                AND {column} IS NOT NULL
                AND {column} <> ''
                AND LOWER(TRIM({column})) NOT IN ('empty', 'unknown', 'other')
                {exclusion}
            ),
            top_labels AS (
              SELECT label, SUM(final_weight) AS total_n
              FROM eligible
              GROUP BY label
              HAVING SUM(final_weight) > ?
              ORDER BY total_n DESC
              LIMIT {limit}
            ),
            denom AS (
              SELECT {cip_col} AS code, SUM(final_weight) AS total_n
              FROM slice
              WHERE {cip_col} IS NOT NULL
              GROUP BY {cip_col}
            ),
            grouped AS (
              SELECT
                e.code,
                MAX(e.title) AS title,
                e.label,
                SUM(e.final_weight) AS n,
                SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight ELSE 0 END) AS salary_weight,
                SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight * e.salary ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN e.salary IS NOT NULL THEN e.final_weight ELSE 0 END), 0) AS weighted_mean_salary
              FROM eligible e
              JOIN top_labels t USING (label)
              GROUP BY e.code, e.label
            )
            SELECT
              g.code,
              g.title,
              g.label,
              ROUND(g.n, 2) AS n,
              ROUND(100.0 * g.n / NULLIF(d.total_n, 0), 2) AS share_pct,
              CASE WHEN g.salary_weight > ? THEN ROUND(g.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary
            FROM grouped g
            JOIN denom d USING (code)
            WHERE g.n > ?
            ORDER BY g.label, g.title
            """,
            [MIN_CELL_WEIGHT, SALARY_MIN_WEIGHT, MIN_CELL_WEIGHT],
        )

    return {
        "roles": group("role_k50_v3", exclude_corporate_attorney=True),
        "industries": group("industry_k50"),
    }


def _major_concentration(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, list[dict[str, Any]]]:
    cip_col = _cip_col(filters)
    same_school_filter = _same_school_employer_filter(filters)
    location_expr = _location_label_expr()

    def group(label_sql: str, where_extra: str, min_weight: float) -> list[dict[str, Any]]:
        return _records_from_query(
            con,
            f"""
            WITH eligible AS (
              SELECT
                {cip_col} AS code,
                COALESCE(major_title, {cip_col}) AS title,
                {label_sql} AS label,
                final_weight
              FROM slice
              WHERE {cip_col} IS NOT NULL
                AND {label_sql} IS NOT NULL
                AND TRIM({label_sql}) <> ''
                AND LOWER(TRIM({label_sql})) NOT IN ('empty', 'unknown', 'other')
                {where_extra}
            ),
            grouped AS (
              SELECT
                code,
                MAX(title) AS title,
                label,
                SUM(final_weight) AS n
              FROM eligible
              GROUP BY code, label
            ),
            ranked AS (
              SELECT
                *,
                SUM(n) OVER (PARTITION BY code) AS total_n,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY n DESC, label) AS rn
              FROM grouped
            )
            SELECT
              code,
              MAX(title) AS title,
              MAX(CASE WHEN rn = 1 THEN label ELSE NULL END) AS top_label,
              ROUND(MAX(CASE WHEN rn = 1 THEN n ELSE NULL END)) AS top_n,
              ROUND(100.0 * SUM(CASE WHEN rn = 1 THEN n ELSE 0 END) / NULLIF(MAX(total_n), 0), 2) AS top1_share_pct,
              ROUND(100.0 * SUM(CASE WHEN rn <= 3 THEN n ELSE 0 END) / NULLIF(MAX(total_n), 0), 2) AS top3_share_pct,
              ROUND(SUM(POWER(100.0 * n / NULLIF(total_n, 0), 2)), 1) AS hhi,
              COUNT(*) AS unique_labels,
              ROUND(MAX(total_n), 2) AS n
            FROM ranked
            GROUP BY code
            HAVING MAX(total_n) > ?
            ORDER BY top1_share_pct DESC
            """,
            [min_weight],
        )

    return {
        "employers": group(
            "employer",
            f"""
                AND employer IS NOT NULL
                AND employer <> ''
                AND unknown_employer_flag = 0
                AND named_employer_flag = 1
                AND career_employer_flag = 1
                {same_school_filter}
            """,
            EMPLOYER_ROW_MIN_WEIGHT,
        ),
        "industries": group(
            "industry_k50",
            """
                AND industry_k50 IS NOT NULL
                AND industry_k50 <> ''
            """,
            MIN_CELL_WEIGHT,
        ),
        "roles": group(
            "role_k50_v3",
            """
                AND role_k50_v3 IS NOT NULL
                AND role_k50_v3 <> ''
                AND NOT (degree = 'Bachelors' AND horizon = '1yr' AND role_k50_v3 = 'Corporate Attorney')
            """,
            MIN_CELL_WEIGHT,
        ),
        "geography": group(
            location_expr,
            """
                AND COALESCE(location, city) IS NOT NULL
                AND LOWER(COALESCE(location, city)) NOT IN ('empty', 'unknown')
            """,
            GEOGRAPHY_ROW_MIN_WEIGHT,
        ),
    }


def _major_demographic_comparison(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, list[dict[str, Any]]]:
    cip_col = _cip_col(filters)

    def group(column: str) -> list[dict[str, Any]]:
        return _records_from_query(
            con,
            f"""
            WITH eligible AS (
              SELECT
                {cip_col} AS code,
                COALESCE(major_title, {cip_col}) AS title,
                {column} AS label,
                profile_weight
              FROM cohort_slice
              WHERE {cip_col} IS NOT NULL
                AND {column} IS NOT NULL
                AND {column} <> ''
                AND LOWER(TRIM({column})) NOT IN ('empty', 'unknown')
            ),
            denom AS (
              SELECT code, SUM(profile_weight) AS total_n
              FROM eligible
              GROUP BY code
            ),
            grouped AS (
              SELECT
                code,
                MAX(title) AS title,
                label,
                SUM(profile_weight) AS n
              FROM eligible
              GROUP BY code, label
            )
            SELECT
              g.code,
              g.title,
              g.label,
              ROUND(g.n, 2) AS n,
              ROUND(100.0 * g.n / NULLIF(d.total_n, 0), 2) AS share_pct
            FROM grouped g
            JOIN denom d USING (code)
            WHERE g.n > ?
            ORDER BY g.label, g.title
            """,
            [MIN_CELL_WEIGHT],
        )

    return {"gender": group("gender"), "race_ethnicity": group("race_ethnicity")}


def _major_postgrad_comparison(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    cip_col = _cip_col(filters)
    limit = min(_safe_limit(filters.top_n), 10)
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            {cip_col} AS code,
            COALESCE(major_title, {cip_col}) AS title,
            COALESCE(
              later_degree_type,
              CASE WHEN no_further_education_flag = 1 THEN 'No further education' ELSE 'Unknown' END
            ) AS label,
            profile_weight
          FROM cohort_slice
          WHERE {cip_col} IS NOT NULL
        ),
        top_labels AS (
          SELECT label, SUM(profile_weight) AS total_n
          FROM eligible
          WHERE label <> 'Unknown'
          GROUP BY label
          HAVING SUM(profile_weight) > ?
          ORDER BY CASE WHEN label = 'No further education' THEN 1 ELSE 0 END, total_n DESC
          LIMIT {limit}
        ),
        denom AS (
          SELECT code, SUM(profile_weight) AS total_n
          FROM eligible
          GROUP BY code
        ),
        grouped AS (
          SELECT
            e.code,
            MAX(e.title) AS title,
            e.label,
            SUM(e.profile_weight) AS n
          FROM eligible e
          JOIN top_labels t USING (label)
          GROUP BY e.code, e.label
        )
        SELECT
          g.code,
          g.title,
          g.label,
          ROUND(g.n, 2) AS n,
          ROUND(100.0 * g.n / NULLIF(d.total_n, 0), 2) AS share_pct
        FROM grouped g
        JOIN denom d USING (code)
        WHERE g.n > ?
        ORDER BY g.label, g.title
        """,
        [MIN_CELL_WEIGHT, MIN_CELL_WEIGHT],
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
          ROUND(c.alumni, 2) AS alumni,
          ROUND(o.salary_weight) AS salary_weight,
          CASE WHEN o.salary_weight > ? THEN ROUND(o.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN o.salary_weight > ? THEN ROUND(o.median_salary) ELSE NULL END AS median_salary
        FROM cohort c
        LEFT JOIN outcomes o USING (code)
        WHERE c.alumni > ?
        ORDER BY c.alumni DESC
        LIMIT {limit}
        """,
        [SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, MIN_CELL_WEIGHT],
    )


def _major_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest, include_current: bool) -> dict[str, Any]:
    cip_col = _cip_col(filters)
    limit = _safe_limit(filters.top_n)
    current_exists = include_current and _create_current_slice(con, filters)
    has_major_filter = bool(filters.majors)

    cohort_denominator_source = "cohort_slice"
    current_denominator_source = "current_slice"
    if has_major_filter:
        denominator_filters = filters.model_copy(deep=True)
        denominator_filters.majors = []
        _create_cohort_slice_table(con, denominator_filters, "major_denominator_cohort_slice")
        cohort_denominator_source = "major_denominator_cohort_slice"
        if current_exists:
            _create_current_slice_table(con, denominator_filters, "major_denominator_current_slice")
            current_denominator_source = "major_denominator_current_slice"

    weight_expr = "SUM(profile_weight)"
    if has_major_filter:
        top_sources = [
            f"SELECT {cip_col} AS code, major_title AS title, profile_weight AS n FROM cohort_slice WHERE {cip_col} IS NOT NULL"
        ]
        if current_exists:
            top_sources.append(
                f"SELECT {cip_col} AS code, major_title AS title, profile_weight AS n FROM current_slice WHERE {cip_col} IS NOT NULL"
            )
        top_codes = _records_from_query(
            con,
            f"""
            SELECT code, MAX(title) AS title, SUM(n) AS n
            FROM ({' UNION ALL '.join(top_sources)})
            GROUP BY code
            HAVING SUM(n) > ?
            ORDER BY n DESC
            LIMIT {limit}
            """,
            [MIN_CELL_WEIGHT],
        )
    else:
        source_for_top = "current_slice" if current_exists else "cohort_slice"
        top_codes = _records_from_query(
            con,
            f"""
            SELECT {cip_col} AS code, MAX(major_title) AS title, {weight_expr} AS n
            FROM {source_for_top}
            WHERE {cip_col} IS NOT NULL
            GROUP BY {cip_col}
            HAVING {weight_expr} > ?
            ORDER BY n DESC
            LIMIT {limit}
            """,
            [MIN_CELL_WEIGHT],
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
          FROM {cohort_denominator_source}
          WHERE grad_year IS NOT NULL
          GROUP BY grad_year
        )
        SELECT
          b.grad_year,
          b.code,
          COALESCE(b.title, b.code) AS title,
          ROUND(b.n, 2) AS n,
          ROUND(t.total_n, 2) AS total_n,
          ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct
        FROM by_major b
        JOIN totals t USING (grad_year)
        WHERE b.n > ?
        ORDER BY b.grad_year, b.code
        """,
        [*codes, MIN_CELL_WEIGHT],
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
              FROM {current_denominator_source}
              WHERE grad_year IS NOT NULL
              GROUP BY grad_year
            )
            SELECT
              b.grad_year,
              b.code,
              COALESCE(b.title, b.code) AS title,
              ROUND(b.n, 2) AS n,
              ROUND(t.total_n, 2) AS total_n,
              ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct
            FROM by_major b
            JOIN totals t USING (grad_year)
            WHERE b.n > ?
            ORDER BY b.grad_year, b.code
            """,
            [*codes, MIN_CELL_WEIGHT],
        )
    return {"top": top_codes, "series": base_series, "current_series": current_series}


def _employer_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    limit = min(_safe_limit(filters.top_n), 8)
    same_school_filter = _same_school_employer_filter(filters)
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
            {same_school_filter}
        ),
        top_employers AS (
          SELECT employer, SUM(final_weight) AS total_n
          FROM eligible
          GROUP BY employer
          HAVING SUM(final_weight) > ?
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
          ROUND(b.n, 2) AS n,
          ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct
        FROM by_year b
        JOIN totals t USING (grad_year)
        WHERE b.n > ?
        ORDER BY b.employer, b.grad_year
        """,
        [EMPLOYER_ROW_MIN_WEIGHT, EMPLOYER_ROW_MIN_WEIGHT],
    )


def _employers(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, Any]:
    limit = _safe_limit(filters.top_n)
    same_school_filter = _same_school_employer_filter(filters)
    employers = _records_from_query(
        con,
        f"""
        WITH denom AS (
          SELECT SUM(final_weight) AS total_n
          FROM slice
          WHERE career_employer_flag = 1
            {same_school_filter}
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
            {same_school_filter}
          GROUP BY employer
        )
        SELECT
          employer,
          ROUND(n, 2) AS n,
          ROUND(100.0 * n / NULLIF((SELECT total_n FROM denom), 0), 2) AS share_pct,
          CASE WHEN salary_weight > ? THEN ROUND(weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN salary_weight > ? THEN ROUND(median_salary) ELSE NULL END AS median_salary
        FROM by_employer
        WHERE n > ?
        ORDER BY n DESC
        LIMIT {limit}
        """,
        [SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, EMPLOYER_ROW_MIN_WEIGHT],
    )
    employer = filters.selected_employer or (employers[0]["employer"] if employers else None)
    roles: list[dict[str, Any]] = []
    subroles: list[dict[str, Any]] = []
    selected_role: str | None = None
    if employer:
        roles = _records_from_query(
            con,
            f"""
            SELECT
              COALESCE(role_k50_v3, role_k10_v3, 'Unknown role') AS role,
              ROUND(SUM(final_weight), 2) AS n,
              ROUND(100.0 * SUM(final_weight) / NULLIF((SELECT SUM(final_weight) FROM slice WHERE employer = ? {same_school_filter}), 0), 2) AS share_pct,
              ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
                / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0)) AS weighted_mean_salary
            FROM slice
            WHERE employer = ?
              {same_school_filter}
              AND role_k50_v3 IS NOT NULL
              AND role_k50_v3 <> ''
            GROUP BY 1
            HAVING SUM(final_weight) > ?
            ORDER BY SUM(final_weight) DESC
            LIMIT {limit}
            """,
            [employer, employer, EMPLOYER_ROW_MIN_WEIGHT],
        )
        selected_role = filters.selected_employer_role
        if selected_role:
            subroles = _records_from_query(
                con,
                f"""
                SELECT
                  COALESCE(role_k150_v3, title_raw, 'Unknown detailed role') AS role,
                  ROUND(SUM(final_weight), 2) AS n,
                  ROUND(100.0 * SUM(final_weight) / NULLIF((
                    SELECT SUM(final_weight)
                    FROM slice
                    WHERE employer = ?
                      AND COALESCE(role_k50_v3, role_k10_v3, 'Unknown role') = ?
                      {same_school_filter}
                  ), 0), 2) AS share_pct,
                  ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0)) AS weighted_mean_salary
                FROM slice
                WHERE employer = ?
                  AND COALESCE(role_k50_v3, role_k10_v3, 'Unknown role') = ?
                  {same_school_filter}
                  AND COALESCE(role_k150_v3, title_raw, '') <> ''
                GROUP BY 1
                HAVING SUM(final_weight) > ?
                ORDER BY SUM(final_weight) DESC
                LIMIT {limit}
                """,
                [employer, selected_role, employer, selected_role, EMPLOYER_ROW_MIN_WEIGHT],
            )
    return {
        "top": employers,
        "selected_employer": employer,
        "roles": roles,
        "selected_role": selected_role,
        "subroles": subroles,
    }


def _location_label_expr() -> str:
    raw = "COALESCE(location, city, 'Unknown')"
    return (
        f"REGEXP_REPLACE("
        f"REGEXP_REPLACE({raw}, '(?i)\\s+non\\s*metropolitan area$', ' non-metro'), "
        f"'(?i)\\s+metropolitan area$', '')"
    )


def _geography_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    limit = min(_safe_limit(filters.top_n), 5)
    location_expr = _location_label_expr()
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
          HAVING SUM(final_weight) > ?
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
          ROUND(b.n, 2) AS n,
          ROUND(b.salary_weight) AS salary_weight,
          ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct,
          CASE WHEN b.salary_weight > ? THEN ROUND(b.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary,
          CASE WHEN b.salary_weight > ? THEN ROUND(b.median_salary) ELSE NULL END AS median_salary
        FROM by_year b
        JOIN totals t USING (grad_year)
        WHERE b.n > ?
        ORDER BY b.location, b.grad_year
        """,
        [GEOGRAPHY_ROW_MIN_WEIGHT, SALARY_MIN_WEIGHT, SALARY_MIN_WEIGHT, GEOGRAPHY_ROW_MIN_WEIGHT],
    )


def _geography(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    limit = min(_safe_limit(filters.top_n), 8)
    location_expr = _location_label_expr()
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            {location_expr} AS location,
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
          ROUND(SUM(final_weight), 2) AS n,
          ROUND(100.0 * SUM(final_weight) / NULLIF((SELECT total_n FROM totals), 0), 2) AS share_pct,
          ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END)) AS salary_weight,
          ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
            / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0)) AS weighted_mean_salary,
          quantile_cont(salary, 0.5) AS median_salary
        FROM eligible
        GROUP BY 1
        HAVING SUM(final_weight) > ?
        ORDER BY SUM(final_weight) DESC
        LIMIT {limit}
        """,
        [GEOGRAPHY_ROW_MIN_WEIGHT],
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
              HAVING SUM(final_weight) > ?
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
              ROUND(b.n, 2) AS n,
              ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct
            FROM by_year b
            JOIN totals t USING (grad_year)
            WHERE b.n > ?
            ORDER BY b.label, b.grad_year
            """,
            [MIN_CELL_WEIGHT, MIN_CELL_WEIGHT],
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
          ROUND(SUM(final_weight), 2) AS n,
          ROUND(100.0 * SUM(final_weight) / NULLIF((SELECT SUM(final_weight) FROM slice), 0), 2) AS share_pct,
          ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
            / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0)) AS weighted_mean_salary
        FROM slice
        WHERE role_k50_v3 IS NOT NULL AND role_k50_v3 <> ''
          AND NOT (degree = 'Bachelors' AND horizon = '1yr' AND role_k50_v3 = 'Corporate Attorney')
        GROUP BY role_k50_v3
        HAVING SUM(final_weight) > ?
        ORDER BY SUM(final_weight) DESC
        LIMIT {limit}
        """,
        [MIN_CELL_WEIGHT],
    )
    industry_rows = _records_from_query(
        con,
        f"""
        SELECT
          industry_k50 AS label,
          ROUND(SUM(final_weight), 2) AS n,
          ROUND(100.0 * SUM(final_weight) / NULLIF((SELECT SUM(final_weight) FROM slice), 0), 2) AS share_pct,
          ROUND(SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
            / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0)) AS weighted_mean_salary
        FROM slice
        WHERE industry_k50 IS NOT NULL AND industry_k50 <> ''
        GROUP BY industry_k50
        HAVING SUM(final_weight) > ?
        ORDER BY SUM(final_weight) DESC
        LIMIT {limit}
        """,
        [MIN_CELL_WEIGHT],
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
    max_rows = max(5000, _safe_limit(filters.top_n) * 250)
    return _records_from_query(
        con,
        f"""
        SELECT
          TRIM(role_k50_v3) AS role_k50_v3,
          COALESCE({role_detail_expr}, TRIM(role_k50_v3)) AS role_k150_v3,
          {industry_expr} AS industry_k200,
          COALESCE({industry_detail_expr}, {industry_expr}) AS industry_k400,
          ROUND(SUM(final_weight), 2) AS n,
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
        HAVING SUM(final_weight) > 0
        ORDER BY SUM(final_weight) DESC
        LIMIT {max_rows}
        """,
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
        node["weighted_mean_salary"] = round(salary_sum / salary_weight) if salary_weight > SALARY_MIN_WEIGHT else None
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
          HAVING SUM(final_weight) > ?
          ORDER BY n DESC
          LIMIT {industry_limit}
        ),
        role_totals_raw AS (
          SELECT e.industry, e.role, SUM(e.final_weight) AS n
          FROM eligible e
          JOIN top_industries i USING (industry)
          GROUP BY e.industry, e.role
          HAVING SUM(e.final_weight) > ?
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
        HAVING SUM(e.final_weight) > ?
        ORDER BY e.industry, e.role, n DESC
        """,
        [MIN_CELL_WEIGHT, MIN_CELL_WEIGHT, MIN_CELL_WEIGHT],
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
        node["weighted_mean_salary"] = round(salary_sum / salary_weight) if salary_weight > SALARY_MIN_WEIGHT else None
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
    base_source = _base_source_for_filters(filters)
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
        [base_source, *params],
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
        HAVING SUM(COALESCE(revelio_completions, 0)) > ?
            OR SUM(COALESCE(ipeds_completions, 0)) > ?
        ORDER BY revelio_completions DESC
        """,
        [MIN_CELL_WEIGHT, MIN_CELL_WEIGHT],
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
        HAVING SUM(COALESCE(revelio_completions, 0)) > ?
            OR SUM(COALESCE(ipeds_completions, 0)) > ?
        ORDER BY revelio_completions DESC
        LIMIT 30
        """,
        [MIN_CELL_WEIGHT, MIN_CELL_WEIGHT],
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
        HAVING SUM(COALESCE(revelio_completions, 0)) > ?
            OR SUM(COALESCE(ipeds_completions, 0)) > ?
        ORDER BY revelio_completions DESC
        """,
        [MIN_CELL_WEIGHT, MIN_CELL_WEIGHT],
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
        HAVING SUM(COALESCE(revelio_completions, 0)) > ?
            OR SUM(COALESCE(ipeds_completions, 0)) > ?
        ORDER BY grad_year
        """,
        [MIN_CELL_WEIGHT, MIN_CELL_WEIGHT],
    )
    return {
        "degree": degree_rows,
        "bachelor_majors": bachelor_major_rows,
        "schools": school_rows,
        "trend": trend_rows,
    }


def _demographic_trend(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, list[dict[str, Any]]]:
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
            by_year AS (
              SELECT
                grad_year,
                label,
                SUM(profile_weight) AS n
              FROM eligible
              GROUP BY grad_year, label
            ),
            totals AS (
              SELECT grad_year, SUM(profile_weight) AS total_n
              FROM eligible
              GROUP BY grad_year
            )
            SELECT
              b.grad_year,
              b.label,
              ROUND(b.n, 2) AS n,
              100.0 * b.n / NULLIF(t.total_n, 0) AS share_pct,
              NULL AS weighted_mean_salary
            FROM by_year b
            JOIN totals t USING (grad_year)
            WHERE b.n > ?
            ORDER BY b.label, b.grad_year
            """,
            [MIN_CELL_WEIGHT],
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
              ROUND(c.n, 2) AS n,
              100.0 * c.n / NULLIF((SELECT total_n FROM totals), 0) AS share_pct,
              CASE WHEN s.salary_weight > ? THEN ROUND(s.weighted_mean_salary) ELSE NULL END AS weighted_mean_salary
            FROM cohort c
            LEFT JOIN salary s USING (label)
            WHERE c.n > ?
            ORDER BY c.n DESC
            """,
            [SALARY_MIN_WEIGHT, MIN_CELL_WEIGHT],
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
          HAVING SUM(profile_weight) > ?
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
          ROUND(b.n, 2) AS n,
          ROUND(100.0 * b.n / NULLIF(t.total_n, 0), 2) AS share_pct
        FROM by_year b
        JOIN totals t USING (grad_year)
        WHERE b.n > ?
        ORDER BY b.degree_type, b.grad_year
        """,
        [MIN_CELL_WEIGHT, MIN_CELL_WEIGHT],
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
          ROUND(n, 2) AS n,
          ROUND(100.0 * n / NULLIF((SELECT total_n FROM denom), 0), 2) AS share_pct
        FROM flows
        WHERE n > ? AND degree_type <> 'Unknown'
        ORDER BY CASE WHEN degree_type = 'No further education' THEN 1 ELSE 0 END, n DESC
        LIMIT {limit}
        """,
        [MIN_CELL_WEIGHT],
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
              ROUND(SUM(profile_weight), 2) AS n
            FROM cohort_slice
            WHERE later_degree_type IN ({placeholders})
              AND later_school IS NOT NULL
              AND later_school <> ''
              {school_filter_sql}
            GROUP BY later_school
            HAVING SUM(profile_weight) > ?
            ORDER BY SUM(profile_weight) DESC
            LIMIT {limit}
            """,
            [*selected_values, *school_filter_params, MIN_CELL_WEIGHT],
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
              ROUND(SUM(profile_weight), 2) AS n
            FROM cohort_slice
            WHERE later_degree_type IN ({placeholders})
              AND later_program IS NOT NULL
              AND later_program <> ''
              {program_filter_sql}
            GROUP BY later_program
            HAVING SUM(profile_weight) > ?
            ORDER BY SUM(profile_weight) DESC
            LIMIT {limit}
            """,
            [*selected_values, *program_filter_params, MIN_CELL_WEIGHT],
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


def _career_archetypes(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    if not _work_dataset_exists("career_archetypes"):
        return []
    if _work_postgrad_unsupported(filters, "career_archetypes"):
        return []
    limit = _safe_limit(filters.top_n)
    where_sql, params = _work_where(filters, include_grad_years=False, postgrad_dataset="career_archetypes")
    series_expr = _work_series_expr(filters)
    return _records_from_query(
        con,
        f"""
        SELECT
          {series_expr} AS series,
          archetype_signal,
          first_employer,
          first_role,
          first_industry,
          employer_5yr,
          role_5yr,
          industry_5yr,
          employer_10yr,
          role_10yr,
          industry_10yr,
          ROUND(SUM(n_alumni)) AS n_alumni,
          ROUND(SUM(raw_n)) AS raw_n,
          MIN(min_grad_year) AS min_grad_year,
          MAX(max_grad_year) AS max_grad_year,
          ROUND(SUM(COALESCE(avg_salary_1yr, 0) * n_alumni) / NULLIF(SUM(CASE WHEN avg_salary_1yr IS NOT NULL THEN n_alumni ELSE 0 END), 0)) AS avg_salary_1yr,
          ROUND(SUM(COALESCE(avg_salary_5yr, 0) * n_alumni) / NULLIF(SUM(CASE WHEN avg_salary_5yr IS NOT NULL THEN n_alumni ELSE 0 END), 0)) AS avg_salary_5yr,
          ROUND(SUM(COALESCE(avg_salary_10yr, 0) * n_alumni) / NULLIF(SUM(CASE WHEN avg_salary_10yr IS NOT NULL THEN n_alumni ELSE 0 END), 0)) AS avg_salary_10yr,
          ROUND(SUM(COALESCE(avg_seniority_5yr, 0) * n_alumni) / NULLIF(SUM(CASE WHEN avg_seniority_5yr IS NOT NULL THEN n_alumni ELSE 0 END), 0), 2) AS avg_seniority_5yr,
          ROUND(SUM(COALESCE(avg_seniority_10yr, 0) * n_alumni) / NULLIF(SUM(CASE WHEN avg_seniority_10yr IS NOT NULL THEN n_alumni ELSE 0 END), 0), 2) AS avg_seniority_10yr,
          ROUND(SUM(COALESCE(archetype_score, 0) * n_alumni) / NULLIF(SUM(CASE WHEN archetype_score IS NOT NULL THEN n_alumni ELSE 0 END), 0), 2) AS archetype_score
        FROM read_parquet(?)
        {where_sql}
        GROUP BY series, archetype_signal, first_employer, first_role, first_industry,
          employer_5yr, role_5yr, industry_5yr, employer_10yr, role_10yr, industry_10yr
        HAVING SUM(n_alumni) > ?
        ORDER BY archetype_score DESC NULLS LAST, n_alumni DESC
        LIMIT {limit}
        """,
        [_dataset_glob("work_facts/career_archetypes"), *params, MIN_CELL_WEIGHT],
    )


def _career_earnings(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    if _career_requires_person_level(filters):
        return _career_earnings_from_base(con, filters)
    if not _work_dataset_exists("annual_salary"):
        return []
    where_sql, params = _work_where(filters, postgrad_dataset="annual_salary")
    if filters.compare_mode and filters.compare_dimension in {"major", "school"}:
        series_expr = _work_series_expr(filters)
    else:
        series_expr = "'Selected slice'"
    return _records_from_query(
        con,
        f"""
        SELECT
          years_since_grad,
          {series_expr} AS series,
          ROUND(SUM(n_alumni)) AS n_alumni,
          ROUND(SUM(weighted_salary_n)) AS salary_obs,
          ROUND(SUM(COALESCE(mean_salary, 0) * weighted_salary_n) / NULLIF(SUM(CASE WHEN mean_salary IS NOT NULL THEN weighted_salary_n ELSE 0 END), 0)) AS mean_salary,
          ROUND(SUM(COALESCE(p25_salary, 0) * weighted_salary_n) / NULLIF(SUM(CASE WHEN p25_salary IS NOT NULL THEN weighted_salary_n ELSE 0 END), 0)) AS p25_salary,
          ROUND(SUM(COALESCE(median_salary, 0) * weighted_salary_n) / NULLIF(SUM(CASE WHEN median_salary IS NOT NULL THEN weighted_salary_n ELSE 0 END), 0)) AS median_salary,
          ROUND(SUM(COALESCE(p75_salary, 0) * weighted_salary_n) / NULLIF(SUM(CASE WHEN p75_salary IS NOT NULL THEN weighted_salary_n ELSE 0 END), 0)) AS p75_salary,
          ROUND(SUM(COALESCE(p90_salary, 0) * weighted_salary_n) / NULLIF(SUM(CASE WHEN p90_salary IS NOT NULL THEN weighted_salary_n ELSE 0 END), 0)) AS p90_salary
        FROM read_parquet(?)
        {where_sql}
          {"AND" if where_sql else "WHERE"} years_since_grad BETWEEN 0 AND 15
        GROUP BY years_since_grad, series
        HAVING SUM(weighted_salary_n) > ?
        ORDER BY series, years_since_grad
        """,
        [_dataset_glob("work_facts/annual_salary"), *params, SALARY_MIN_WEIGHT],
    )


def _career_earnings_from_base(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    base_columns = _dataset_columns("base_fact")
    if "horizon_years" in base_columns:
        years_expr = "COALESCE(horizon_years, CASE WHEN horizon = 'early_2025' THEN 0 ELSE NULL END)"
    else:
        years_expr = "CASE WHEN horizon = 'early_2025' THEN 0 WHEN horizon = '1yr' THEN 1 WHEN horizon = '5yr' THEN 5 WHEN horizon = '10yr' THEN 10 ELSE NULL END"
    series_expr = "'Selected slice'"
    group_expr = "series"
    if filters.compare_mode and filters.compare_dimension == "major":
        cip_col = _cip_col(filters)
        series_expr = f"COALESCE(major_title, {cip_col})"
    elif filters.compare_mode and filters.compare_dimension == "school":
        series_expr = "school_name"
    source = _base_source_for_filters(filters)
    where_sql, params = _where(filters, include_horizon=False, include_postgrad=True)
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            {years_expr} AS years_since_grad,
            {series_expr} AS series,
            final_weight,
            salary
          FROM read_parquet(?)
          {where_sql}
        ),
        grouped AS (
          SELECT
            years_since_grad,
            {group_expr},
            SUM(final_weight) AS n_alumni,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_obs,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS mean_salary,
            quantile_cont(salary, 0.25) AS p25_salary,
            quantile_cont(salary, 0.5) AS median_salary,
            quantile_cont(salary, 0.75) AS p75_salary,
            quantile_cont(salary, 0.9) AS p90_salary
          FROM eligible
          WHERE years_since_grad BETWEEN 0 AND 15
          GROUP BY years_since_grad, {group_expr}
        )
        SELECT
          years_since_grad,
          series,
          ROUND(n_alumni) AS n_alumni,
          ROUND(salary_obs) AS salary_obs,
          CASE WHEN salary_obs > ? THEN ROUND(mean_salary) ELSE NULL END AS mean_salary,
          CASE WHEN salary_obs > ? THEN ROUND(p25_salary) ELSE NULL END AS p25_salary,
          CASE WHEN salary_obs > ? THEN ROUND(median_salary) ELSE NULL END AS median_salary,
          CASE WHEN salary_obs > ? THEN ROUND(p75_salary) ELSE NULL END AS p75_salary,
          CASE WHEN salary_obs > ? THEN ROUND(p90_salary) ELSE NULL END AS p90_salary
        FROM grouped
        WHERE salary_obs > ?
        ORDER BY series, years_since_grad
        """,
        [
            source,
            *params,
            SALARY_MIN_WEIGHT,
            SALARY_MIN_WEIGHT,
            SALARY_MIN_WEIGHT,
            SALARY_MIN_WEIGHT,
            SALARY_MIN_WEIGHT,
            SALARY_MIN_WEIGHT,
        ],
    )


def _career_internships(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, Any]:
    if not _work_dataset_exists("internship_summary"):
        return {
            "summary": {},
            "rates": [],
            "employer_comparison": [],
            "employers": [],
            "roles": [],
            "industries": [],
            "geography": [],
            "conversions": [],
        }
    if _work_postgrad_unsupported(filters, "internship_summary"):
        return {
            "summary": {},
            "rates": [],
            "employer_comparison": [],
            "employers": [],
            "roles": [],
            "industries": [],
            "geography": [],
            "conversions": _career_internship_conversions(con, filters),
        }
    where_sql, params = _work_where(filters, postgrad_dataset="internship_summary")
    summary = _single_record(
        con,
        f"""
        SELECT
          ROUND(SUM(n_alumni)) AS n_alumni,
          ROUND(SUM(n_interns)) AS n_interns,
          ROUND(SUM(raw_interns)) AS raw_interns,
          ROUND(SUM(internship_positions)) AS internship_positions,
          ROUND(100.0 * SUM(n_interns) / NULLIF(SUM(n_alumni), 0), 1) AS internship_rate_pct,
          ROUND(SUM(internship_positions) / NULLIF(SUM(n_interns), 0), 2) AS avg_internships_per_intern
        FROM read_parquet(?)
        {where_sql}
        """,
        [_dataset_glob("work_facts/internship_summary"), *params],
    )

    rates: list[dict[str, Any]] = []
    employer_comparison: list[dict[str, Any]] = []
    if filters.compare_mode and filters.compare_dimension in {"major", "school"}:
        series_expr = _work_series_expr(filters)
        rates = _records_from_query(
            con,
            f"""
            SELECT
              {series_expr} AS series,
              ROUND(SUM(n_alumni)) AS n_alumni,
              ROUND(SUM(n_interns)) AS n_interns,
              ROUND(SUM(raw_interns)) AS raw_interns,
              ROUND(SUM(internship_positions)) AS internship_positions,
              ROUND(100.0 * SUM(n_interns) / NULLIF(SUM(n_alumni), 0), 1) AS internship_rate_pct,
              ROUND(SUM(internship_positions) / NULLIF(SUM(n_interns), 0), 2) AS avg_internships_per_intern
            FROM read_parquet(?)
            {where_sql}
            GROUP BY series
            HAVING SUM(n_alumni) > ?
            ORDER BY internship_rate_pct DESC NULLS LAST, n_interns DESC
            """,
            [_dataset_glob("work_facts/internship_summary"), *params, MIN_CELL_WEIGHT],
        )
        if _work_dataset_exists("internship_employers"):
            limit = _safe_limit(filters.top_n)
            employer_comparison = _records_from_query(
                con,
                f"""
                WITH eligible AS (
                  SELECT
                    {series_expr} AS series,
                    employer AS label,
                    n_internship_positions,
                    raw_n,
                    mean_salary
                  FROM read_parquet(?)
                  {where_sql}
                ),
                grouped AS (
                  SELECT
                    series,
                    label,
                    SUM(n_internship_positions) AS n,
                    SUM(raw_n) AS raw_n,
                    SUM(COALESCE(mean_salary, 0) * n_internship_positions)
                      / NULLIF(SUM(CASE WHEN mean_salary IS NOT NULL THEN n_internship_positions ELSE 0 END), 0) AS mean_salary
                  FROM eligible
                  WHERE series IS NOT NULL
                    AND label IS NOT NULL
                    AND TRIM(label) <> ''
                  GROUP BY series, label
                ),
                totals AS (
                  SELECT series, SUM(n) AS series_total
                  FROM grouped
                  GROUP BY series
                ),
                ranked AS (
                  SELECT
                    g.*,
                    ROUND(100.0 * g.n / NULLIF(t.series_total, 0), 1) AS share_pct,
                    ROW_NUMBER() OVER (
                      PARTITION BY g.series
                      ORDER BY g.n DESC, g.label
                    ) AS employer_rank
                  FROM grouped g
                  JOIN totals t
                    ON g.series = t.series
                )
                SELECT
                  series,
                  label,
                  ROUND(n) AS n,
                  ROUND(raw_n) AS raw_n,
                  share_pct,
                  ROUND(mean_salary) AS mean_salary,
                  employer_rank
                FROM ranked
                WHERE employer_rank <= {limit}
                  AND n > ?
                ORDER BY series, employer_rank, label
                """,
                [_dataset_glob("work_facts/internship_employers"), *params, MIN_CELL_WEIGHT],
            )

    def top_rows(dataset: str, column: str) -> list[dict[str, Any]]:
        if not _work_dataset_exists(dataset):
            return []
        limit = _safe_limit(filters.top_n)
        return _records_from_query(
            con,
            f"""
            WITH eligible AS (
              SELECT
                {column} AS label,
                n_internship_positions,
                raw_n,
                mean_salary
              FROM read_parquet(?)
              {where_sql}
            ),
            totals AS (
              SELECT SUM(n_internship_positions) AS total_n FROM eligible
            )
            SELECT
              label,
              ROUND(SUM(n_internship_positions)) AS n,
              ROUND(SUM(raw_n)) AS raw_n,
              ROUND(100.0 * SUM(n_internship_positions) / NULLIF((SELECT total_n FROM totals), 0), 1) AS share_pct,
              ROUND(SUM(COALESCE(mean_salary, 0) * n_internship_positions) / NULLIF(SUM(CASE WHEN mean_salary IS NOT NULL THEN n_internship_positions ELSE 0 END), 0)) AS mean_salary
            FROM eligible
            WHERE label IS NOT NULL AND TRIM(label) <> ''
            GROUP BY label
            HAVING SUM(n_internship_positions) > ?
            ORDER BY SUM(n_internship_positions) DESC
            LIMIT {limit}
            """,
            [_dataset_glob(f"work_facts/{dataset}"), *params, MIN_CELL_WEIGHT],
        )

    return {
        "summary": summary,
        "rates": rates,
        "employer_comparison": employer_comparison,
        "employers": top_rows("internship_employers", "employer"),
        "roles": top_rows("internship_roles", "role"),
        "industries": top_rows("internship_industries", "industry"),
        "geography": top_rows("internship_geography", "location"),
        "conversions": _career_internship_conversions(con, filters),
    }


def _career_internship_conversions(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    """Internship employers that also appear as later career employers.

    This is inferred at person x normalized-employer level. It is intentionally
    only shown for a single selected slice; compare mode gets the cleaner rate
    comparison instead.
    """
    if filters.compare_mode:
        return []
    source = _base_source_for_filters(filters)
    if not source:
        return []
    where_sql, params = _where(filters, include_horizon=False, include_postgrad=True)
    same_school_filter = _same_school_employer_filter(filters)
    limit = _safe_limit(filters.top_n)
    return _records_from_query(
        con,
        f"""
        WITH filtered AS (
          SELECT
            person_key,
            unitid,
            employer,
            COALESCE(NULLIF(TRIM(employer_norm), ''), LOWER(TRIM(employer))) AS employer_key,
            internship_flag,
            career_employer_flag,
            same_school_employer_flag,
            horizon_years,
            final_weight,
            salary
          FROM read_parquet(?)
          {where_sql}
        ),
        intern_person_employer AS (
          SELECT
            person_key,
            employer_key,
            ANY_VALUE(employer) AS employer,
            MAX(final_weight) AS intern_weight
          FROM filtered
          WHERE internship_flag = 1
            AND employer_key IS NOT NULL
            AND employer_key <> ''
            AND LOWER(COALESCE(employer, '')) <> 'unknown'
            {same_school_filter}
          GROUP BY person_key, employer_key
        ),
        career_person_employer AS (
          SELECT
            person_key,
            employer_key,
            MIN(horizon_years) AS first_career_year,
            SUM(CASE WHEN salary IS NOT NULL THEN final_weight * salary ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS mean_salary
          FROM filtered
          WHERE COALESCE(internship_flag, 0) = 0
            AND career_employer_flag = 1
            AND horizon_years >= 1
            AND employer_key IS NOT NULL
            AND employer_key <> ''
            AND LOWER(COALESCE(employer, '')) <> 'unknown'
            {same_school_filter}
          GROUP BY person_key, employer_key
        )
        SELECT
          i.employer,
          ROUND(SUM(i.intern_weight)) AS n_interns,
          ROUND(SUM(CASE WHEN c.person_key IS NOT NULL THEN i.intern_weight ELSE 0 END)) AS converted_to_job,
          ROUND(
            100.0 * SUM(CASE WHEN c.person_key IS NOT NULL THEN i.intern_weight ELSE 0 END)
            / NULLIF(SUM(i.intern_weight), 0),
            1
          ) AS conversion_rate_pct,
          ROUND(
            SUM(CASE WHEN c.person_key IS NOT NULL THEN i.intern_weight * c.first_career_year ELSE 0 END)
            / NULLIF(SUM(CASE WHEN c.person_key IS NOT NULL THEN i.intern_weight ELSE 0 END), 0),
            1
          ) AS avg_first_career_year,
          ROUND(
            SUM(CASE WHEN c.mean_salary IS NOT NULL THEN i.intern_weight * c.mean_salary ELSE 0 END)
            / NULLIF(SUM(CASE WHEN c.mean_salary IS NOT NULL THEN i.intern_weight ELSE 0 END), 0)
          ) AS mean_salary
        FROM intern_person_employer i
        LEFT JOIN career_person_employer c
          ON i.person_key = c.person_key
         AND i.employer_key = c.employer_key
        GROUP BY i.employer
        HAVING SUM(i.intern_weight) > ?
           AND SUM(CASE WHEN c.person_key IS NOT NULL THEN i.intern_weight ELSE 0 END) > 0
        ORDER BY converted_to_job DESC NULLS LAST, conversion_rate_pct DESC NULLS LAST, n_interns DESC
        LIMIT {limit}
        """,
        [source, *params, MIN_CELL_WEIGHT],
    )


def _base_career_years_expr(base_columns: set[str]) -> str:
    if "horizon_years" in base_columns:
        return "COALESCE(horizon_years, CASE WHEN horizon = 'early_2025' THEN 0 ELSE NULL END)"
    return "CASE WHEN horizon = 'early_2025' THEN 0 WHEN horizon = '1yr' THEN 1 WHEN horizon = '5yr' THEN 5 WHEN horizon = '10yr' THEN 10 ELSE NULL END"


def _base_career_stage_expr(base_columns: set[str]) -> str:
    if "seniority_band" in base_columns:
        return """
            CASE
              WHEN seniority_band IN ('Entry I', 'Entry II', 'Mid I') THEN 'Entry level'
              WHEN seniority_band IN ('Mid II', 'Senior I') THEN 'Mid level'
              WHEN seniority_band IN ('Senior II', 'Manager I', 'Manager II', 'Manager III', 'Director', 'Director+', 'Executive') THEN 'Senior level'
              ELSE 'Other'
            END
        """
    return """
            CASE
              WHEN seniority IS NULL THEN 'Other'
              WHEN seniority < 3 THEN 'Entry level'
              WHEN seniority < 5 THEN 'Mid level'
              ELSE 'Senior level'
            END
        """


def _career_seniority_from_base(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    base_columns = _dataset_columns("base_fact")
    years_expr = _base_career_years_expr(base_columns)
    stage_expr = _base_career_stage_expr(base_columns)
    source = _base_source_for_filters(filters)
    where_sql, params = _where(filters, include_horizon=False, include_postgrad=True)
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            {years_expr} AS years_since_grad,
            {stage_expr} AS career_stage,
            final_weight,
            salary
          FROM read_parquet(?)
          {where_sql}
        ),
        grouped AS (
          SELECT
            years_since_grad,
            career_stage,
            SUM(final_weight) AS n_alumni,
            SUM(CASE WHEN salary IS NOT NULL THEN salary * final_weight ELSE 0 END)
              / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS mean_salary
          FROM eligible
          WHERE years_since_grad BETWEEN 0 AND 15
            AND career_stage <> 'Other'
          GROUP BY years_since_grad, career_stage
        )
        SELECT
          years_since_grad,
          career_stage AS seniority_band,
          ROUND(n_alumni) AS n_alumni,
          ROUND(100.0 * n_alumni / NULLIF(SUM(n_alumni) OVER (PARTITION BY years_since_grad), 0), 1) AS share_pct,
          ROUND(mean_salary) AS mean_salary
        FROM grouped
        WHERE n_alumni > ?
        ORDER BY years_since_grad,
          CASE career_stage WHEN 'Entry level' THEN 1 WHEN 'Mid level' THEN 2 WHEN 'Senior level' THEN 3 ELSE 4 END
        """,
        [source, *params, MIN_CELL_WEIGHT],
    )


def _career_average_seniority_from_base(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    base_columns = _dataset_columns("base_fact")
    years_expr = _base_career_years_expr(base_columns)
    stage_expr = _base_career_stage_expr(base_columns)
    series_expr = "'Selected slice'"
    if filters.compare_mode and filters.compare_dimension == "major":
        cip_col = _cip_col(filters)
        series_expr = f"COALESCE(major_title, {cip_col})"
    elif filters.compare_mode and filters.compare_dimension == "school":
        series_expr = "school_name"
    source = _base_source_for_filters(filters)
    where_sql, params = _where(filters, include_horizon=False, include_postgrad=True)
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            {years_expr} AS years_since_grad,
            {series_expr} AS series,
            {stage_expr} AS career_stage,
            final_weight,
            salary
          FROM read_parquet(?)
          {where_sql}
        ),
        scored AS (
          SELECT
            years_since_grad,
            series,
            CASE
              WHEN career_stage = 'Entry level' THEN 1
              WHEN career_stage = 'Mid level' THEN 2
              WHEN career_stage = 'Senior level' THEN 3
              ELSE NULL
            END AS stage_score,
            final_weight,
            salary
          FROM eligible
          WHERE years_since_grad BETWEEN 0 AND 15
        )
        SELECT
          years_since_grad,
          series,
          ROUND(SUM(final_weight)) AS n_alumni,
          ROUND(SUM(stage_score * final_weight) / NULLIF(SUM(CASE WHEN stage_score IS NOT NULL THEN final_weight ELSE 0 END), 0), 2) AS avg_seniority,
          ROUND(SUM(stage_score * final_weight) / NULLIF(SUM(CASE WHEN stage_score IS NOT NULL THEN final_weight ELSE 0 END), 0), 2) AS raw_avg_seniority,
          ROUND(SUM(CASE WHEN salary IS NOT NULL THEN salary * final_weight ELSE 0 END) / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0)) AS mean_salary
        FROM scored
        GROUP BY years_since_grad, series
        HAVING SUM(final_weight) > ?
        ORDER BY series, years_since_grad
        """,
        [source, *params, MIN_CELL_WEIGHT],
    )


def _career_seniority(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    if not _work_dataset_exists("annual_seniority"):
        return []
    if _work_postgrad_unsupported(filters, "annual_seniority"):
        return _career_seniority_from_base(con, filters)
    where_sql, params = _work_where(filters, include_grad_years=False, postgrad_dataset="annual_seniority")
    return _records_from_query(
        con,
        f"""
        WITH grouped AS (
          SELECT
            years_since_grad,
            CASE
              WHEN seniority_band IN ('Entry I', 'Entry II', 'Mid I') THEN 'Entry level'
              WHEN seniority_band IN ('Mid II', 'Senior I') THEN 'Mid level'
              WHEN seniority_band IN ('Senior II', 'Manager I', 'Manager II', 'Manager III', 'Director', 'Executive') THEN 'Senior level'
              ELSE 'Other'
            END AS career_stage,
            CASE
              WHEN seniority_band IN ('Entry I', 'Entry II', 'Mid I') THEN 1
              WHEN seniority_band IN ('Mid II', 'Senior I') THEN 2
              WHEN seniority_band IN ('Senior II', 'Manager I', 'Manager II', 'Manager III', 'Director', 'Executive') THEN 3
              ELSE 4
            END AS stage_order,
            n_alumni,
            mean_salary
          FROM read_parquet(?)
          {where_sql}
        )
        SELECT
          years_since_grad,
          career_stage AS seniority_band,
          ROUND(SUM(n_alumni)) AS n_alumni,
          ROUND(100.0 * SUM(n_alumni) / NULLIF(SUM(SUM(n_alumni)) OVER (PARTITION BY years_since_grad), 0), 1) AS share_pct,
          ROUND(SUM(COALESCE(mean_salary, 0) * n_alumni) / NULLIF(SUM(CASE WHEN mean_salary IS NOT NULL THEN n_alumni ELSE 0 END), 0)) AS mean_salary
        FROM grouped
        GROUP BY years_since_grad, career_stage, stage_order
        HAVING SUM(n_alumni) > ?
        ORDER BY years_since_grad, stage_order
        """,
        [_dataset_glob("work_facts/annual_seniority"), *params, MIN_CELL_WEIGHT],
    )


def _career_average_seniority(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    if not _work_dataset_exists("annual_seniority"):
        return []
    if _work_postgrad_unsupported(filters, "annual_seniority"):
        return _career_average_seniority_from_base(con, filters)
    where_sql, params = _work_where(filters, include_grad_years=False, postgrad_dataset="annual_seniority")
    series_expr = _work_series_expr(filters) if filters.compare_mode and filters.compare_dimension in {"major", "school"} else "'Selected slice'"
    return _records_from_query(
        con,
        f"""
        SELECT
          years_since_grad,
          {series_expr} AS series,
          ROUND(SUM(n_alumni)) AS n_alumni,
          ROUND(
            SUM(
              CASE
                WHEN seniority_band IN ('Entry I', 'Entry II', 'Mid I') THEN 1
                WHEN seniority_band IN ('Mid II', 'Senior I') THEN 2
                WHEN seniority_band IN ('Senior II', 'Manager I', 'Manager II', 'Manager III', 'Director', 'Executive') THEN 3
                ELSE NULL
              END * n_alumni
            )
            / NULLIF(
              SUM(
                CASE
                  WHEN seniority_band IN ('Entry I', 'Entry II', 'Mid I', 'Mid II', 'Senior I', 'Senior II', 'Manager I', 'Manager II', 'Manager III', 'Director', 'Executive')
                  THEN n_alumni
                  ELSE 0
                END
              ),
              0
            ),
            2
          ) AS avg_seniority,
          ROUND(SUM(COALESCE(avg_seniority, 0) * n_alumni) / NULLIF(SUM(CASE WHEN avg_seniority IS NOT NULL THEN n_alumni ELSE 0 END), 0), 2) AS raw_avg_seniority,
          ROUND(SUM(COALESCE(mean_salary, 0) * n_alumni) / NULLIF(SUM(CASE WHEN mean_salary IS NOT NULL THEN n_alumni ELSE 0 END), 0)) AS mean_salary
        FROM read_parquet(?)
        {where_sql}
          {"AND" if where_sql else "WHERE"} years_since_grad BETWEEN 0 AND 15
        GROUP BY years_since_grad, series
        HAVING SUM(n_alumni) > ?
        ORDER BY series, years_since_grad
        """,
        [_dataset_glob("work_facts/annual_seniority"), *params, MIN_CELL_WEIGHT],
    )


def _career_employer_tenure(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    if not _work_dataset_exists("employer_tenure"):
        return []
    if _work_postgrad_unsupported(filters, "employer_tenure"):
        return []
    limit = _safe_limit(filters.top_n)
    where_sql, params = _work_where(filters, include_grad_years=False, postgrad_dataset="employer_tenure")
    return _records_from_query(
        con,
        f"""
        SELECT
          employer,
          ROUND(SUM(n_starters)) AS n_starters,
          ROUND(SUM(raw_n)) AS raw_n,
          ROUND(SUM(COALESCE(avg_observed_tenure_months, 0) * n_starters) / NULLIF(SUM(CASE WHEN avg_observed_tenure_months IS NOT NULL THEN n_starters ELSE 0 END), 0), 1) AS avg_observed_tenure_months,
          ROUND(SUM(COALESCE(retained_1yr_pct, 0) * n_starters) / NULLIF(SUM(CASE WHEN retained_1yr_pct IS NOT NULL THEN n_starters ELSE 0 END), 0), 1) AS retained_1yr_pct,
          ROUND(SUM(COALESCE(retained_3yr_pct, 0) * n_starters) / NULLIF(SUM(CASE WHEN retained_3yr_pct IS NOT NULL THEN n_starters ELSE 0 END), 0), 1) AS retained_3yr_pct,
          ROUND(SUM(COALESCE(retained_5yr_pct, 0) * n_starters) / NULLIF(SUM(CASE WHEN retained_5yr_pct IS NOT NULL THEN n_starters ELSE 0 END), 0), 1) AS retained_5yr_pct,
          ROUND(SUM(COALESCE(avg_salary_5yr, 0) * n_starters) / NULLIF(SUM(CASE WHEN avg_salary_5yr IS NOT NULL THEN n_starters ELSE 0 END), 0)) AS avg_salary_5yr,
          ROUND(SUM(COALESCE(avg_seniority_5yr, 0) * n_starters) / NULLIF(SUM(CASE WHEN avg_seniority_5yr IS NOT NULL THEN n_starters ELSE 0 END), 0), 2) AS avg_seniority_5yr,
          ROUND(SUM(COALESCE(senior_plus_5yr_pct, 0) * n_starters) / NULLIF(SUM(CASE WHEN senior_plus_5yr_pct IS NOT NULL THEN n_starters ELSE 0 END), 0), 1) AS senior_plus_5yr_pct
        FROM read_parquet(?)
        {where_sql}
        GROUP BY employer
        HAVING SUM(n_starters) > ?
        ORDER BY n_starters DESC
        LIMIT {limit}
        """,
        [_dataset_glob("work_facts/employer_tenure"), *params, EMPLOYER_ROW_MIN_WEIGHT],
    )


def _career_employer_share(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> list[dict[str, Any]]:
    if not _work_dataset_exists("annual_employers"):
        return []
    if _work_postgrad_unsupported(filters, "annual_employers"):
        return []
    limit = _safe_limit(filters.top_n)
    where_sql, params = _work_where(filters, include_grad_years=False, postgrad_dataset="annual_employers")
    same_school_filter = _same_school_employer_filter(filters)
    return _records_from_query(
        con,
        f"""
        WITH eligible AS (
          SELECT
            unitid,
            degree,
            cip4,
            years_since_grad,
            employer,
            n_alumni,
            raw_n,
            mean_salary,
            salary_obs,
            share_pct,
            CASE
              WHEN share_pct > 0 THEN n_alumni * 100.0 / share_pct
              ELSE NULL
            END AS inferred_denominator
          FROM read_parquet(?)
          {where_sql}
            {"AND" if where_sql else "WHERE"} years_since_grad BETWEEN 0 AND 15
            AND employer IS NOT NULL
            AND employer <> ''
            AND COALESCE(career_employer_flag, 1) = 1
            {same_school_filter}
        ),
        top_employers AS (
          SELECT employer, SUM(n_alumni) AS total_n
          FROM eligible
          GROUP BY employer
          HAVING SUM(n_alumni) > ?
          ORDER BY total_n DESC
          LIMIT {limit}
        ),
        group_denominators AS (
          SELECT
            unitid,
            degree,
            cip4,
            years_since_grad,
            MAX(inferred_denominator) AS denominator
          FROM eligible
          WHERE inferred_denominator IS NOT NULL
          GROUP BY unitid, degree, cip4, years_since_grad
        ),
        denominators AS (
          SELECT years_since_grad, SUM(denominator) AS denominator
          FROM group_denominators
          GROUP BY years_since_grad
        ),
        by_employer AS (
          SELECT
            e.years_since_grad,
            e.employer,
            SUM(e.n_alumni) AS n_alumni,
            SUM(e.raw_n) AS raw_n,
            SUM(COALESCE(e.mean_salary, 0) * e.salary_obs)
              / NULLIF(SUM(CASE WHEN e.mean_salary IS NOT NULL THEN e.salary_obs ELSE 0 END), 0) AS mean_salary
          FROM eligible e
          JOIN top_employers t USING (employer)
          GROUP BY e.years_since_grad, e.employer
        )
        SELECT
          b.years_since_grad,
          b.employer,
          ROUND(b.n_alumni) AS n_alumni,
          ROUND(b.raw_n) AS raw_n,
          ROUND(100.0 * b.n_alumni / NULLIF(d.denominator, 0), 2) AS share_pct,
          ROUND(b.mean_salary) AS mean_salary
        FROM by_employer b
        LEFT JOIN denominators d USING (years_since_grad)
        ORDER BY b.employer, b.years_since_grad
        """,
        [_dataset_glob("work_facts/annual_employers"), *params, MIN_CELL_WEIGHT],
    )


def _career_mobility(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, Any]:
    if not _work_dataset_exists("mobility"):
        return {}
    if _work_postgrad_unsupported(filters, "mobility"):
        return {}
    where_sql, params = _work_where(filters, postgrad_dataset="mobility")
    return _single_record(
        con,
        f"""
        SELECT
          ROUND(SUM(n_alumni)) AS n_alumni,
          ROUND(SUM(COALESCE(avg_employers_5yr, 0) * n_alumni) / NULLIF(SUM(CASE WHEN avg_employers_5yr IS NOT NULL THEN n_alumni ELSE 0 END), 0), 2) AS avg_employers_5yr,
          ROUND(SUM(COALESCE(avg_employers_10yr, 0) * n_alumni) / NULLIF(SUM(CASE WHEN avg_employers_10yr IS NOT NULL THEN n_alumni ELSE 0 END), 0), 2) AS avg_employers_10yr,
          ROUND(SUM(COALESCE(avg_employers_15yr, 0) * n_alumni) / NULLIF(SUM(CASE WHEN avg_employers_15yr IS NOT NULL THEN n_alumni ELSE 0 END), 0), 2) AS avg_employers_15yr,
          ROUND(SUM(COALESCE(first_employer_retention_1yr_pct, 0) * n_alumni) / NULLIF(SUM(CASE WHEN first_employer_retention_1yr_pct IS NOT NULL THEN n_alumni ELSE 0 END), 0), 1) AS first_employer_retention_1yr_pct,
          ROUND(SUM(COALESCE(first_employer_retention_3yr_pct, 0) * n_alumni) / NULLIF(SUM(CASE WHEN first_employer_retention_3yr_pct IS NOT NULL THEN n_alumni ELSE 0 END), 0), 1) AS first_employer_retention_3yr_pct,
          ROUND(SUM(COALESCE(first_employer_retention_5yr_pct, 0) * n_alumni) / NULLIF(SUM(CASE WHEN first_employer_retention_5yr_pct IS NOT NULL THEN n_alumni ELSE 0 END), 0), 1) AS first_employer_retention_5yr_pct
        FROM read_parquet(?)
        {where_sql}
        """,
        [_dataset_glob("work_facts/mobility"), *params],
    )


def _career(con: duckdb.DuckDBPyConnection, filters: QueryRequest) -> dict[str, Any]:
    include_employer_share = not filters.compare_mode and filters.view_mode in {"overtime", "all"}
    return {
        "earnings": _career_earnings(con, filters),
        "archetypes": _career_archetypes(con, filters),
        "internships": _career_internships(con, filters),
        "seniority": _career_seniority(con, filters),
        "average_seniority": _career_average_seniority(con, filters),
        "employer_tenure": _career_employer_tenure(con, filters),
        "employer_share": _career_employer_share(con, filters) if include_employer_share else [],
        "mobility": _career_mobility(con, filters),
    }


def _dashboard_uncached(filters: QueryRequest) -> dict[str, Any]:
    with _query_slot():
        con = _connect()
        try:
            _create_slice(con, filters)
            tab = filters.active_tab or ("overview" if filters.compare_mode else "overview")
            if tab == "compare":
                tab = "overview"
            view_mode = filters.view_mode or "snapshot"
            result: dict[str, Any] = {
                "meta": {
                    "data_version": _manifest().get("version"),
                    "min_cell_weight": MIN_CELL_WEIGHT,
                    "salary_min_weight": SALARY_MIN_WEIGHT,
                    "partial_horizon": filters.horizon == "early_2025",
                    "filters": filters.model_dump(),
                    "active_tab": tab,
                    "view_mode": view_mode,
                },
            }

            if tab in {"all", "full"} and not filters.compare_mode:
                result.update(
                    {
                        "overview": _overview(con),
                        "salary_trend": _salary_trend(con, filters),
                        "salary_distribution": _salary_distribution(con),
                        "alumni_trend": _alumni_trend(con, filters),
                        "salary_trend_by_school": _salary_trend_by_school(con, filters),
                        "alumni_trend_by_school": [],
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
                        "career": _career(con, filters),
                        "coverage": _coverage(con, filters),
                        "demographics": _demographics(con),
                        "demographic_trend": _demographic_trend(con, filters),
                        "postgrad": _postgrad(con, filters),
                        "postgrad_trend": _postgrad_trend(con, filters),
                    }
                )
                return result

            if filters.compare_mode:
                result["overview"] = _overview(con)
                if filters.compare_dimension == "major":
                    result["major_comparison"] = _major_comparison(con, filters)
                    if tab in {"all", "full"}:
                        result["current_student_trend_by_major"] = _current_student_trend_by_major(con, filters)
                        result["alumni_trend_by_major"] = _alumni_trend_by_major(con, filters)
                        result["salary_trend_by_major"] = _salary_trend_by_major(con, filters)
                        result["salary_distribution"] = _salary_distribution(con)
                        result["salary_distributions_by_entity"] = _salary_distribution_by_entity(con, filters)
                        result["major_employer_comparison"] = _major_employer_comparison(con, filters)
                        result["major_geography_comparison"] = _major_geography_comparison(con, filters)
                        result["major_role_comparison"] = _major_role_comparison(con, filters)
                        result["major_demographic_comparison"] = _major_demographic_comparison(con, filters)
                        result["major_postgrad_comparison"] = _major_postgrad_comparison(con, filters)
                        result["major_concentration"] = _major_concentration(con, filters)
                        result["career"] = _career(con, filters)
                        result["coverage"] = _coverage(con, filters)
                        return result
                    if tab == "overview":
                        result["current_student_trend_by_major"] = _current_student_trend_by_major(con, filters)
                    if view_mode == "overtime" and tab == "overview":
                        result["alumni_trend_by_major"] = _alumni_trend_by_major(con, filters)
                    if view_mode == "overtime" and tab == "earnings":
                        result["salary_trend_by_major"] = _salary_trend_by_major(con, filters)
                    if tab == "employers":
                        result["major_employer_comparison"] = _major_employer_comparison(con, filters)
                        result["major_concentration"] = _major_concentration(con, filters)
                    if tab == "geography":
                        result["major_geography_comparison"] = _major_geography_comparison(con, filters)
                        result["major_concentration"] = _major_concentration(con, filters)
                    if tab == "roles":
                        result["major_role_comparison"] = _major_role_comparison(con, filters)
                        result["major_concentration"] = _major_concentration(con, filters)
                    if tab == "demographics":
                        result["major_demographic_comparison"] = _major_demographic_comparison(con, filters)
                    if tab == "postgrad":
                        result["major_postgrad_comparison"] = _major_postgrad_comparison(con, filters)
                else:
                    result["school_comparison"] = _school_comparison(con)
                    if tab in {"all", "full"}:
                        result["alumni_trend_by_school"] = _alumni_trend_by_school(con, filters)
                        result["salary_trend_by_school"] = _salary_trend_by_school(con, filters)
                        result["salary_distribution"] = _salary_distribution(con)
                        result["salary_distributions_by_entity"] = _salary_distribution_by_entity(con, filters)
                        result["school_employer_comparison"] = _school_employer_comparison(con, filters)
                        result["school_geography_comparison"] = _school_geography_comparison(con, filters)
                        result["school_role_comparison"] = _school_role_comparison(con, filters)
                        result["school_demographic_comparison"] = _school_demographic_comparison(con, filters)
                        result["school_postgrad_comparison"] = _school_postgrad_comparison(con, filters)
                        result["career"] = _career(con, filters)
                        result["coverage"] = _coverage(con, filters)
                        return result
                    if view_mode == "overtime" and tab == "overview":
                        result["alumni_trend_by_school"] = _alumni_trend_by_school(con, filters)
                    if view_mode == "overtime" and tab == "earnings":
                        result["salary_trend_by_school"] = _salary_trend_by_school(con, filters)
                    if tab == "employers":
                        result["school_employer_comparison"] = _school_employer_comparison(con, filters)
                    if tab == "geography":
                        result["school_geography_comparison"] = _school_geography_comparison(con, filters)
                    if tab == "roles":
                        result["school_role_comparison"] = _school_role_comparison(con, filters)
                    if tab == "demographics":
                        result["school_demographic_comparison"] = _school_demographic_comparison(con, filters)
                    if tab == "postgrad":
                        result["school_postgrad_comparison"] = _school_postgrad_comparison(con, filters)
                if tab == "earnings":
                    result["salary_distribution"] = _salary_distribution(con)
                    result["salary_distributions_by_entity"] = _salary_distribution_by_entity(con, filters)
                if tab == "career":
                    result["career"] = _career(con, filters)
                if tab == "coverage":
                    result["coverage"] = _coverage(con, filters)
                return result

            if tab == "overview":
                result["overview"] = _overview(con)
                if view_mode == "snapshot":
                    result["top_majors"] = _top_majors(con, filters)
                    result["employers"] = _employers(con, filters)
                    result["geography"] = _geography(con, filters)
                else:
                    result["salary_trend"] = _salary_trend(con, filters)
                    result["alumni_trend"] = _alumni_trend(con, filters)
                    result["current_student_trend"] = _current_student_trend(con, filters)
                    result["major_trend"] = _major_trend(con, filters, filters.include_current_students)
                return result

            if tab == "earnings":
                result["top_majors"] = _top_majors(con, filters)
                result["salary_distribution"] = _salary_distribution(con)
                if view_mode == "overtime":
                    result["salary_trend"] = _salary_trend(con, filters)
                    result["salary_trend_by_school"] = _salary_trend_by_school(con, filters)
                return result

            if tab == "employers":
                result["employers"] = _employers(con, filters)
                if view_mode == "overtime":
                    result["employer_trend"] = _employer_trend(con, filters)
                return result

            if tab == "geography":
                result["geography"] = _geography(con, filters)
                if view_mode == "overtime":
                    result["geography_trend"] = _geography_trend(con, filters)
                return result

            if tab == "roles":
                if view_mode == "snapshot":
                    result["roles"] = _roles(con, filters)
                else:
                    result["role_trend"] = _role_trend(con, filters)
                return result

            if tab == "career":
                result["career"] = _career(con, filters)
                return result

            if tab == "demographics":
                if view_mode == "snapshot":
                    result["demographics"] = _demographics(con)
                else:
                    result["demographic_trend"] = _demographic_trend(con, filters)
                return result

            if tab == "postgrad":
                result["postgrad"] = _postgrad(con, filters)
                if view_mode == "overtime":
                    result["postgrad_trend"] = _postgrad_trend(con, filters)
                return result

            if tab == "coverage":
                result["coverage"] = _coverage(con, filters)
                return result

            result["overview"] = _overview(con)
            return result
        finally:
            con.close()


@app.post("/api/dashboard")
def dashboard(filters: QueryRequest, _: None = Depends(require_internal_password)) -> dict[str, Any]:
    cache_key = _response_cache_key("dashboard", filters)
    cached = _response_cache_get(cache_key)
    if cached is not None:
        return cached
    result = _dashboard_uncached(filters)
    _response_cache_set(cache_key, result)
    return result


@app.post("/api/warm-cache")
def warm_cache(filters: QueryRequest, _: None = Depends(require_internal_password)) -> dict[str, Any]:
    """Build per-school cache files before compare-mode drilldowns."""
    schools = list(dict.fromkeys(str(school) for school in filters.schools if str(school)))
    if not schools:
        return {"status": "skipped", "reason": "no schools selected", "schools": []}
    if len(schools) > MAX_COMPARE_SCHOOLS:
        raise HTTPException(
            status_code=400,
            detail=f"School compare is capped at {MAX_COMPARE_SCHOOLS} schools for performance.",
        )
    with _query_slot():
        paths = [path for school in schools for path in _school_base_cache(school)]
    return {
        "status": "ok",
        "schools": schools,
        "files": len(paths),
        "bytes": sum(Path(path).stat().st_size for path in paths if Path(path).exists()),
        "max_compare_schools": MAX_COMPARE_SCHOOLS,
    }


@app.get("/", include_in_schema=False)
def frontend_index() -> FileResponse:
    index_path = STATIC_ROOT / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Dashboard static files not installed")
    return FileResponse(index_path)


@app.get("/config.js", include_in_schema=False)
def frontend_config() -> Response:
    # Same-origin hosting avoids CORS and keeps data access behind the API password.
    return Response(
        'window.OUTCOMES_API_URL = window.location.origin;\n',
        media_type="application/javascript",
    )
