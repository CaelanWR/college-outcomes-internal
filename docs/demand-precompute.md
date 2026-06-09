# Labor Demand Precompute

This is an optional add-on export. It does not rerun the full school outcomes
precompute. Run it after the platform export has created:

```text
{SCRATCH}.SCHOOL_OUTCOMES_PLATFORM_BASE
```

It writes:

```text
school_outcomes_data_.../platform_parquet/demand_facts/
  posting_role_month/
  posting_role_summary/
  school_major_role_demand/
  role_skill_demand/
  school_major_skill_demand/
  posting_degree_demand/          # only when POSTINGS_DETAIL_TABLE is set
  school_major_degree_demand/     # only when POSTINGS_DETAIL_TABLE is set
```

and updates `platform_parquet/platform_manifest.json`.

## Notebook Cell

Put `demand_parquet_export.py` in the same server folder as
`platform_parquet_export.py`, then run:

```python
from pathlib import Path
import os

OUT_DIR = Path(os.environ.get(
    "OUTCOMES_PRECOMPUTE_OUT_DIR",
    "school_outcomes_data_nace_june5_503_plus_elite",
)).expanduser()

POSTINGS_DYNAMICS_TABLE = "CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_202605_POSTINGS_UNIFIED_DYNAMICS"
SKILL_DYNAMICS_TABLE = "CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_202605_SKILL_DYNAM"

# Optional. Only set this if you have a raw/detail postings table with:
# COUNTRY, POST_DATE, EXPECTED_HIRES, REQUIRED_DEGREE, ROLE_K10, ROLE_K50, ROLE_K150.
# POSTINGS_DETAIL_TABLE = "CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_202605_POSTINGS_UNIFIED"

DEMAND_EXPORT_SCRIPT_PATHS = []
if os.environ.get("DEMAND_EXPORT_SCRIPT"):
    DEMAND_EXPORT_SCRIPT_PATHS.append(Path(os.environ["DEMAND_EXPORT_SCRIPT"]).expanduser())
DEMAND_EXPORT_SCRIPT_PATHS.extend([
    Path("demand_parquet_export.py"),
    Path("college_outcomes_platform/scripts/demand_parquet_export.py"),
    Path("scripts/demand_parquet_export.py"),
])

demand_export_script = next((path for path in DEMAND_EXPORT_SCRIPT_PATHS if path.exists()), None)
if demand_export_script is None:
    raise FileNotFoundError("Could not find demand_parquet_export.py")

print(f"Running demand export script: {demand_export_script}")
exec(compile(demand_export_script.read_text(), str(demand_export_script), "exec"))
```

## What The Facts Mean

`posting_role_summary` is role-level labor demand from posting dynamics:
recent active postings, new postings, expected hires, salary, filling time, and
growth versus the previous window.

`school_major_role_demand` joins school/major alumni role fit to role demand.
It answers: “Which in-demand roles do graduates from this school/major already
enter?”

`role_skill_demand` uses skill dynamics by broad role.

`school_major_skill_demand` joins school/major role fit to role-skill demand.
It answers: “Which skills are demanded in the roles this school/major already
feeds into?”

The two dynamics tables listed above do not contain `REQUIRED_DEGREE`, so degree
demand is skipped by default. If a detail postings table with `REQUIRED_DEGREE`
is supplied as `POSTINGS_DETAIL_TABLE`, the export also writes role-level and
school/major-level degree-demand facts.

No raw posting descriptions, URLs, or person-level records are exported.
