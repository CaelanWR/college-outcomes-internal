# Strong/Excellent School Precompute

The detached server notebook at:

`/Users/caelan/Downloads/nace_june5_503_precompute_server/school_outcomes_precompute.ipynb`

has been updated so the U.S. data-capacity audit is no longer diagnostic only. By default, after the audit cell runs it promotes every school with `data_capacity_tier` in `excellent` or `strong` into `UNITID_LIST` and rebuilds `UNITID_SQL` before the downstream fact/export cells run.

Expected audit counts from the June 8 run:

- `excellent`: 546
- `strong`: 283
- selected for full run: 829

The notebook writes the promoted run list to:

`<OUT_DIR>/us_strong_excellent_school_run_list.csv`

To opt out and keep the older NACE/elite list for a test run:

```bash
OUTCOMES_USE_US_STRONG_EXCELLENT_SCHOOLS=0
```

After the platform export finishes, validate before uploading to Render:

```bash
python validate_postgrad_facts.py <OUT_DIR>/platform_parquet
```
