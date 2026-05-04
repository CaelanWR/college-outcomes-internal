from __future__ import annotations

import hmac
import math
import os
from typing import Any, Optional

import duckdb
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


DATA_ROOT = os.environ.get("OUTCOMES_PARQUET_ROOT", "./data/parquet")
SUPPRESSION_THRESHOLD = int(os.environ.get("SUPPRESSION_THRESHOLD", "25"))
APP_PASSWORD = os.environ.get("OUTCOMES_APP_PASSWORD")
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.environ.get("OUTCOMES_ALLOWED_ORIGINS", "").split(",")
    if origin.strip()
]

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
    tabs: list[str] = Field(default_factory=lambda: ["overview"])


def require_internal_password(x_outcomes_password: Optional[str] = Header(default=None)) -> None:
    """Fallback API password guard.

    Prefer real SSO or network-level access control in production. This protects
    the data API when a stronger gate is not available.
    """
    if not APP_PASSWORD:
        return
    if not x_outcomes_password or not hmac.compare_digest(x_outcomes_password, APP_PASSWORD):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute("SET enable_progress_bar=false")
    return con


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    try:
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
    except TypeError:
        pass
    return value


def _records_from_query(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cols = None
    out: list[dict[str, Any]] = []
    result = con.execute(sql, params)
    cols = [desc[0] for desc in result.description]
    for row in result.fetchall():
        out.append({col: _json_safe(value) for col, value in zip(cols, row)})
    return out


def _where(filters: QueryRequest) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if filters.schools:
        clauses.append("unitid IN (" + ",".join(["?"] * len(filters.schools)) + ")")
        params.extend(filters.schools)
    if filters.degree and filters.degree != "All":
        clauses.append("degree = ?")
        params.append(filters.degree)
    if filters.majors:
        if filters.cip_level not in {"cip2", "cip4", "cip6"}:
            raise HTTPException(status_code=400, detail="cip_level must be cip2, cip4, or cip6")
        clauses.append(f"{filters.cip_level} IN (" + ",".join(["?"] * len(filters.majors)) + ")")
        params.extend(filters.majors)
    if filters.grad_years:
        clauses.append("grad_year IN (" + ",".join(["?"] * len(filters.grad_years)) + ")")
        params.extend(filters.grad_years)
    if filters.horizon:
        clauses.append("horizon = ?")
        params.append(filters.horizon)
    if filters.demographics.gender:
        clauses.append("gender = ?")
        params.append(filters.demographics.gender)
    if filters.demographics.race_ethnicity:
        clauses.append("race_ethnicity = ?")
        params.append(filters.demographics.race_ethnicity)
    if filters.postgrad.later_degree_type:
        clauses.append("later_degree_type = ?")
        params.append(filters.postgrad.later_degree_type)
    if filters.postgrad.no_further_education is not None:
        clauses.append("no_further_education_flag = ?")
        params.append(filters.postgrad.no_further_education)

    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/query")
def query(filters: QueryRequest, _: None = Depends(require_internal_password)) -> dict[str, Any]:
    where_sql, params = _where(filters)
    path = os.path.join(DATA_ROOT, "**", "*.parquet")

    sql = f"""
        SELECT
          unitid,
          school_name,
          grad_year,
          SUM(final_weight) AS alumni,
          SUM(final_weight * salary) / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS avg_salary,
          SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight
        FROM read_parquet(?)
        {where_sql}
        GROUP BY unitid, school_name, grad_year
        HAVING SUM(final_weight) >= ?
        ORDER BY school_name, grad_year
    """

    con = _connect()
    try:
      rows = _records_from_query(con, sql, [path, *params, SUPPRESSION_THRESHOLD])
    finally:
      con.close()

    return {
        "meta": {
            "suppression_threshold": SUPPRESSION_THRESHOLD,
            "partial_horizon": filters.horizon.lower() in {"early", "partial", "early_2025"},
        },
        "overview": rows,
    }
