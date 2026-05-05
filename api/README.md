# API Layer

GitHub Pages cannot run this API. Host this separately on Render, Fly.io, Cloud Run, AWS, or an internal server.

The API exposes protected aggregate endpoints:

```text
GET /api/health
POST /api/options
POST /api/dashboard
```

The service:

- Uses FastAPI and DuckDB over `platform_parquet`.
- Reads `OUTCOMES_PARQUET_ROOT=/path/to/platform_parquet/base_fact`.
- Can bootstrap a mounted data disk from `OUTCOMES_DATA_ARCHIVE_URL` when run through `api/start.py`.
- Uses `OUTCOMES_APP_PASSWORD` as a fallback API password when configured.
- Returns aggregate chart data only.
- Shows positive-weight aggregate cells by default. Raise `MIN_CELL_WEIGHT`, `EMPLOYER_ROW_MIN_WEIGHT`, `GEOGRAPHY_ROW_MIN_WEIGHT`, or `SALARY_MIN_WEIGHT` if you need stricter display thresholds.
