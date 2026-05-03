# API Contract

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
  "tabs": ["overview", "earnings", "employers", "geography", "roles", "demographics", "postgrad"]
}
```

## Query Response

```json
{
  "meta": {
    "data_version": "2026-05-03-v1",
    "partial_horizon": false,
    "suppression_threshold": 25
  },
  "overview": [],
  "earnings": [],
  "employers": [],
  "geography": [],
  "roles": [],
  "demographics": [],
  "postgrad": []
}
```

## Rules

- Suppress cells below the minimum threshold before returning data.
- Never return raw person-level records.
- Every weighted metric should include the denominator used.
- 2025 early earnings must be labeled as partial unless the full horizon has elapsed.

