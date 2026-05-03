from __future__ import annotations

import os
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


DATA_ROOT = os.environ.get("OUTCOMES_PARQUET_ROOT", "./data/parquet")
SUPPRESSION_THRESHOLD = int(os.environ.get("SUPPRESSION_THRESHOLD", "25"))

app = FastAPI(title="College Outcomes API")


class DemographicFilters(BaseModel):
    gender: str | None = None
    race_ethnicity: str | None = None


class PostgradFilters(BaseModel):
    later_degree_type: str | None = None
    no_further_education: bool | None = None


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


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute("SET enable_progress_bar=false")
    return con


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
def query(filters: QueryRequest) -> dict[str, Any]:
    where_sql, params = _where(filters)
    path = os.path.join(DATA_ROOT, "**", "*.parquet")

    sql = f"""
        SELECT
          unitid,
          school_name,
          grad_year,
          SUM(final_weight) AS alumni,
          SUM(final_weight * salary) / NULLIF(SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END), 0) AS weighted_median_proxy,
          SUM(CASE WHEN salary IS NOT NULL THEN final_weight ELSE 0 END) AS salary_weight
        FROM read_parquet(?)
        {where_sql}
        GROUP BY unitid, school_name, grad_year
        HAVING SUM(final_weight) >= ?
        ORDER BY school_name, grad_year
    """

    con = _connect()
    try:
      rows = con.execute(sql, [path, *params, SUPPRESSION_THRESHOLD]).fetchdf().to_dict(orient="records")
    finally:
      con.close()

    return {
        "meta": {
            "suppression_threshold": SUPPRESSION_THRESHOLD,
            "partial_horizon": filters.horizon.lower() in {"early", "partial", "early_2025"},
        },
        "overview": rows,
    }

