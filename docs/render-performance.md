# Render Performance Plan

Recommended starting point for the expanded school bundle is a single Render
Pro Max web service.

## Pro Max Env Vars

For a 16 GB RAM / 4 CPU instance:

```text
OUTCOMES_DUCKDB_THREADS=4
OUTCOMES_DUCKDB_MEMORY_LIMIT=12GB
OUTCOMES_QUERY_CONCURRENCY=2
OUTCOMES_RESPONSE_CACHE_MAX_ENTRIES=600
OUTCOMES_RESPONSE_CACHE_TTL_SECONDS=3600
OUTCOMES_DUCKDB_TEMP_DIR=/var/data/duckdb_tmp
OUTCOMES_SCHOOL_CACHE_DIR=/var/data/outcomes_school_cache
```

Keep `OUTCOMES_QUERY_CONCURRENCY=1` if the service becomes less responsive
under load. Two concurrent queries is useful on Pro Max only when the cache is
warm and DuckDB has enough memory.

## After Uploading New Data

Clear stale school caches when replacing the data bundle:

```bash
rm -rf /var/data/outcomes_school_cache
mkdir -p /var/data/outcomes_school_cache /var/data/duckdb_tmp
```

Then redeploy or restart the service so it picks up the new manifest.

## Warm Caches

Run this from the repo root on the Render shell after the API has the new
`platform_parquet` available:

```bash
OUTCOMES_PARQUET_ROOT=/var/data/outcomes/platform_parquet/base_fact \
OUTCOMES_DUCKDB_THREADS=4 \
OUTCOMES_DUCKDB_MEMORY_LIMIT=12GB \
OUTCOMES_DUCKDB_TEMP_DIR=/var/data/duckdb_tmp \
OUTCOMES_SCHOOL_CACHE_DIR=/var/data/outcomes_school_cache \
python scripts/warm_school_cache.py --work core
```

For a quicker first pass, warm only `base_fact`:

```bash
OUTCOMES_PARQUET_ROOT=/var/data/outcomes/platform_parquet/base_fact \
OUTCOMES_DUCKDB_THREADS=4 \
OUTCOMES_DUCKDB_MEMORY_LIMIT=12GB \
OUTCOMES_DUCKDB_TEMP_DIR=/var/data/duckdb_tmp \
OUTCOMES_SCHOOL_CACHE_DIR=/var/data/outcomes_school_cache \
python scripts/warm_school_cache.py --work none
```

The full `--work all` mode is useful for maximum first-click speed but can take
much longer because it builds every school x work-fact cache.

## Expected Impact

Pro Max helps raw query speed, especially with `OUTCOMES_DUCKDB_THREADS=4` and
a higher memory limit. Cache warming helps first-load latency by preventing the
first user for each school from paying the cost of slicing the full Parquet
bundle into persistent per-school files.

The next larger optimization is changing the export to write partitioned
per-school Parquet directly. That removes most of the cache-warming scan cost
and should be considered if the 829-school bundle still feels slow after Pro
Max plus cache warming.
