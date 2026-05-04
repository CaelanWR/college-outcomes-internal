# API Contract

## Endpoints

```text
GET /api/health
POST /api/options
POST /api/dashboard
```

`/api/options` and `/api/dashboard` require `X-Outcomes-Password` when `OUTCOMES_APP_PASSWORD` is set.

## Query Request

```json
{
  "schools": ["110635", "190150"],
  "degree": "Bachelors",
  "cip_level": "cip4",
  "majors": ["11.07"],
  "grad_years": [2020, 2021, 2022, 2023, 2024, 2025],
  "horizon": "1yr",
  "demographics": {
    "gender": "Female",
    "race_ethnicity": null
  },
  "postgrad": {
    "later_degree_type": "LAW",
    "no_further_education": null
  },
  "include_current_students": false,
  "selected_employer": null,
  "selected_postgrad_degree": null,
  "top_n": 12
}
```

## Dashboard Response

```json
{
  "meta": {
    "data_version": "2026-05-03-platform-v1",
    "partial_horizon": false,
    "suppression_threshold": 25
  },
  "overview": {},
  "salary_trend": [],
  "salary_trend_by_school": [],
  "school_comparison": [],
  "top_majors": [],
  "major_trend": {
    "series": [],
    "current_series": []
  },
  "employers": {
    "top": [],
    "selected_employer": null,
    "roles": []
  },
  "geography": [],
  "roles": {
    "roles": [],
    "industries": []
  },
  "demographics": {
    "gender": [],
    "race_ethnicity": []
  },
  "postgrad": {
    "flows": [],
    "selected_degree": null,
    "schools": [],
    "programs": []
  }
}
```

## Rules

- Suppress cells below the minimum threshold before returning data.
- Never return raw person-level records.
- Every salary metric includes its denominator where relevant.
- 2025 early earnings are labeled as a partial horizon.
- The frontend must not fetch Parquet, CSV, or raw static data directly.
