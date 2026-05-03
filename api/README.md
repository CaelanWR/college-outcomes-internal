# API Layer

GitHub Pages cannot run this API. Host this separately on Render, Fly.io, Cloud Run, AWS, or an internal server.

The intended API is small:

```text
POST /api/query
GET /api/health
GET /api/metadata
```

Recommended implementation:

- FastAPI or Node.
- DuckDB querying partitioned Parquet.
- Auth in front of the service.
- Return aggregate chart data only.

