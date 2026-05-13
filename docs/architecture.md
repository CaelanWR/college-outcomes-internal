# Target Architecture

## Goals

- Filter by school, degree, major, graduation year, cohort range, horizon, demographics, employer status, geography, and post-secondary outcomes.
- Support more schools without making the browser download multi-GB JSON.
- Include 2025 earnings as an explicitly labeled early/partial horizon.
- Publish a polished internal app that non-technical users can explore safely.

## Recommended Shape

### 1. Canonical Weighted Base Fact

Create one precomputed table with one record per alumni outcome observation at the useful analysis grain:

```text
unitid
school_name
degree
ipeds_degree_level
cip2
cip4
cip6
major_title
grad_year
cohort_band
horizon
horizon_label
gender
race_ethnicity
employer
career_employer_flag
role_k50
role_k150
industry
location
salary
later_degree_type
later_school
later_program
no_further_education_flag
plus_one_masters_flag
position_weight
ipeds_calibration_weight
final_weight
```

`Plus-One Masters` is assigned when a bachelor alum has a later `Master` education row at the same `unitid` with an end date within 366 days of bachelor graduation.

### 2. Pre-Aggregated Facts

For speed and privacy, generate aggregate tables from that base fact:

- `overview_fact`
- `earnings_fact`
- `employer_fact`
- `role_industry_fact`
- `geography_fact`
- `demographics_fact`
- `postgrad_fact`
- `major_mix_fact`

Every aggregate should include the filter dimensions needed by the app:

```text
unitid, degree, cip2, cip4, cip6, grad_year, cohort_band, horizon, gender, race_ethnicity, postgrad_filter
```

### 3. Query API

The frontend sends filter state to a single endpoint and receives chart-ready data.

```http
POST /api/query
```

The API can be implemented as:

- DuckDB over partitioned Parquet for low-cost internal hosting.
- Snowflake-backed queries for highest fidelity and simplest data governance.
- Static aggregate JSON only for demo/fallback mode.

### 4. Frontend

Use a React/Next frontend once the API exists. Until then, the current static HTML can be deployed from `site/index.html`.

## 2025 Earnings

Do not label 2025 as `1yr` until the full observation window exists. Use:

```text
Early 2025
```

or:

```text
Partial-year earnings
```

Charts should visually distinguish early/partial horizons with a dotted line, lighter opacity, or explicit badge.
