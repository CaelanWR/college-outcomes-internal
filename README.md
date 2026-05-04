# College Outcomes Internal Explorer

This is the deployment scaffold for the outcomes explorer.

The current `site/index.html` is an API-backed static frontend. It can run on GitHub Pages, but the data must stay behind the protected API. The architecture is:

1. Snowflake produces clean, weighted, privacy-safe analytical outputs.
2. Data is exported as platform Parquet.
3. A query layer returns chart-ready aggregates for selected filters.
4. The frontend is deployed as a polished internal web app.

## Why GitHub Pages Is Only The Frontend

GitHub Pages hosts static HTML, CSS, and JavaScript. It cannot run a Python/Node API, query DuckDB server-side, or protect sensitive data unless your GitHub setup supports private Pages through Enterprise access controls.

Use GitHub Pages for the frontend shell. Use one of these for data:

- A small API service backed by DuckDB over Parquet.
- Snowflake/Streamlit for a less polished but simpler internal deployment.

## Local API

From this folder:

```bash
cd /path/to/college_outcomes_platform
python3 -m venv .venv
.venv/bin/pip install -r api/requirements.txt
OUTCOMES_PARQUET_ROOT=/path/to/platform_parquet/base_fact \
OUTCOMES_ALLOWED_ORIGINS=http://localhost:8124 \
  .venv/bin/uvicorn api.app:app --reload --port 8000
```

To require a password locally or in production, add:

```text
OUTCOMES_APP_PASSWORD=<long random password>
```

## Local Frontend

In a second terminal:

```bash
python3 -m http.server 8124 --directory site
```

Open `http://localhost:8124/`, enter `http://localhost:8000` as the API URL, and enter the API password if one is configured.

The validated local bundle currently has `6,086,227` base rows and `171,390` current-student rows.

## GitHub Pages Deployment

1. Create a new private GitHub repository, for example `college-outcomes-internal`.
2. Push this folder to the repository.
3. In GitHub, go to `Settings > Pages`.
4. Set the source to `GitHub Actions`.
5. Push to `main`; `.github/workflows/pages.yml` will deploy the contents of `site/`.

## Data Safety

Do not commit raw source data, row-level alumni data, or large generated data folders. The API should only return aggregate chart data. The `.gitignore` intentionally excludes data folders and Parquet/database files.

## New Platform Precompute

The Snowflake notebook now has a final cell, `CELL 13: API-ready platform Parquet export`, that writes:

```text
OUT_DIR/platform_parquet/base_fact/
OUT_DIR/platform_parquet/current_students_fact/
OUT_DIR/platform_parquet/aggregate_facts/
OUT_DIR/platform_parquet/platform_manifest.json
```

The base fact is partitioned by `unitid`, `degree`, `grad_year`, and `horizon`, and includes hashed `person_key`, demographics, postgrad filters, employer flags, position weights, calibration weights, and `final_weight`.

The `early_2025` horizon is included as a partial earnings view. Do not label it as a complete 1-year outcome.
