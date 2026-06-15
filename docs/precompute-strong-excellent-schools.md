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

For the broader rebuild, use the expanded-list builder after the U.S. capacity
audit exists:

```bash
python scripts/build_expanded_school_run_list.py \
  --source <OUT_DIR>/us_school_data_capacity_audit.csv \
  --current <OUT_DIR>/us_strong_excellent_school_run_list.csv \
  --out-dir <OUT_DIR> \
  --target-max 1350
```

This writes:

```text
<OUT_DIR>/expanded_school_run_list.csv
<OUT_DIR>/expanded_school_must_include_review.csv
<OUT_DIR>/school_quality_bin_counts.csv
```

Use `expanded_school_run_list.csv` to set `UNITID_LIST` and `UNITID_SQL` before
rerunning the platform export. The must-include review file is where missing or
too-thin priority schools should be checked, including UCLA and other flagship
systems. The quality-bin counts file is the lightweight check to rerun when you
just want to see how many schools are excellent, strong, usable, watchlist, or
otherwise too thin before committing to the full rebuild.

Further-education matching must use the full education universe for the later
degree lookup, even when bachelor rows are limited to the selected run list. If
the notebook narrows `EDUCATION_CIP` to selected schools, set
`POSTGRAD_EDUCATION_CIP` or `LATER_EDUCATION_CIP` to the full education CIP
table before running the platform export cell. Otherwise law, medical, and
other graduate destinations outside the selected school list will be
undercounted.

To opt out and keep the older NACE/elite list for a test run:

```bash
OUTCOMES_USE_US_STRONG_EXCELLENT_SCHOOLS=0
```

After the platform export finishes, validate before uploading to Render. Do not
upload a bundle if this validator fails; it includes a Columbia History anchor
specifically to catch the too-small law/further-education issue.

```bash
python scripts/validate_postgrad_facts.py <OUT_DIR>/platform_parquet
```
