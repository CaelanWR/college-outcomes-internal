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
    Path("demand_precompute_server/demand_parquet_export.py"),
    Path("/data0/data0_caelan/demand_precompute_server/demand_parquet_export.py"),
    Path("college_outcomes_platform/scripts/demand_parquet_export.py"),
    Path("scripts/demand_parquet_export.py"),
])

demand_export_script = next((path for path in DEMAND_EXPORT_SCRIPT_PATHS if path.exists()), None)
if demand_export_script is None:
    raise FileNotFoundError("Could not find demand_parquet_export.py")

print(f"Using OUT_DIR: {OUT_DIR}")
print(f"Running demand export script: {demand_export_script}")
exec(compile(demand_export_script.read_text(), str(demand_export_script), "exec"))
