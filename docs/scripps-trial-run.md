# Scripps Competitor Trial Run

This run builds a fresh, physically smaller release for the 40 schools in
`config/scripps_competitor_40.csv`. It does not modify the current Render
release.

## Start the detached run

On the Snowflake precompute server:

```bash
cd /path/to/college_outcomes_platform
conda activate fast-pipelines
python scripts/run_scripps_trial_precompute.py
```

The command prints the run folder, PID, log path, and status path, then returns
to the shell. The process runs in a new session, so it is safe to disconnect
from SSH.

The runner automatically:

1. Confirms the selection contains exactly 40 unique schools and Scripps
   College (`123165`).
2. Selects the newest common monthly `STANDARD_*_INDIVIDUAL_POSITION` and
   `STANDARD_*_INDIVIDUAL_USER` tables available in Snowflake.
3. Audits the Scripps source rows before starting the expensive work.
4. Executes `school_outcomes_precompute.ipynb` for only the explicit school
   list.
5. Prevents the broad strong/excellent list from replacing the trial list.
6. Runs the platform validator and the Scripps/UC/Claremont trial validator.
7. Creates `scripps-competitor-40-platform.tar.gz` only after validation passes.

## Monitor it

Use the paths printed by the start command:

```bash
tail -f /path/printed/by/the/runner/run.log
cat /path/printed/by/the/runner/status.json
```

When complete, the run folder contains:

```text
READY.json
source-audit.json
platform-validation.json
trial-validation.json
scripps-competitor-40-platform.tar.gz
school_outcomes_data/platform_parquet/
```

If the process fails, `status.json` records the failing phase and error. The
current live release remains unchanged.

## Explicit source overrides

Automatic source discovery is preferred. To pin a tested source month:

```bash
python scripts/run_scripps_trial_precompute.py \
  --position-table CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_YYYYMM_INDIVIDUAL_POSITION \
  --user-table CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_YYYYMM_INDIVIDUAL_USER
```

To use a different matched education/CIP table:

```bash
python scripts/run_scripps_trial_precompute.py \
  --education-cip USER_CAELAN.TMP_MONTHLY.EDUCATION_WITH_CIP
```

Use `--foreground` only for debugging.
