# Demand Precompute Server Bundle

Drop this folder onto the server after the main platform export has finished.
It is an add-on export: it does not rerun the school/alumni precompute.

## Required State

The notebook/server session must already have:

```text
sfClient
SCRATCH
OUT_DIR
{SCRATCH}.SCHOOL_OUTCOMES_PLATFORM_BASE
```

That means run this after the platform export has reached at least:

```text
base_fact: ...
```

It is safest to run it after the full platform export is done.

## What It Writes

```text
{OUT_DIR}/platform_parquet/demand_facts/
  posting_role_month/
  posting_role_summary/
  school_major_role_demand/
  role_skill_demand/
  school_major_skill_demand/

{OUT_DIR}/platform_parquet/platform_manifest.json
```

If you supply a raw/detail postings table with `REQUIRED_DEGREE`, it can also
write:

```text
posting_degree_demand/
school_major_degree_demand/
```

## Notebook Cell

Paste the contents of `run_demand_export_cell.py` into a notebook cell, or run:

```python
exec(open("demand_precompute_server/run_demand_export_cell.py").read())
```

If the folder is next to `nace_june5_503_precompute_server`, use:

```python
exec(open("/data0/data0_caelan/demand_precompute_server/run_demand_export_cell.py").read())
```

## Package For Render

After the export finishes:

```bash
cd /data0/data0_caelan/nace_june5_503_precompute_server/school_outcomes_data_nace_june5_503_plus_elite
tar -czf outcomes-demand-facts-render.tar.gz \
  platform_parquet/demand_facts \
  platform_parquet/platform_manifest.json
```

Upload/extract that tarball into Render at `/var/data/outcomes`.

This only adds demand facts and the updated manifest; it does not replace the
main platform parquet bundle.
