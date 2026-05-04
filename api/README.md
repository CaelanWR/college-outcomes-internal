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
- Uses `OUTCOMES_APP_PASSWORD` as a fallback API password when configured.
- Returns aggregate chart data only.
- Suppresses small cells with `SUPPRESSION_THRESHOLD`.
