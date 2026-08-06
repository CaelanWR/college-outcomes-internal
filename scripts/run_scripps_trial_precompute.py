#!/usr/bin/env python3
"""Run, validate, and package the fresh 40-school Scripps trial release.

The command detaches by default, so it is safe to disconnect from SSH:

    python scripts/run_scripps_trial_precompute.py

Use --foreground while testing or debugging.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_NOTEBOOK = ROOT / "school_outcomes_precompute.ipynb"
DEFAULT_SELECTION = ROOT / "config" / "scripps_competitor_40.csv"
DEFAULT_RUNS_ROOT = Path("~/data0_caelan/scripps_trial_runs").expanduser()
DEFAULT_EDUCATION_CIP = "USER_CAELAN.TMP_MONTHLY.EDUCATION_WITH_CIP"
STANDARD_TABLE_PATTERN = re.compile(
    r"^STANDARD_(\d{6})_INDIVIDUAL_(POSITION|USER)$",
    re.IGNORECASE,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_status(path: Path, **values: Any) -> None:
    payload: dict[str, Any] = {}
    if path.exists():
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            payload = {}
    payload.update(values)
    payload["updated_at"] = _utc_now()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    tmp.replace(path)


def _run(
    command: list[str],
    *,
    env: dict[str, str],
    cwd: Path = ROOT,
) -> None:
    print("$", " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def _selection_summary(path: Path) -> dict[str, Any]:
    import csv

    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    unitids = [str(row.get("unitid") or "").strip().removesuffix(".0") for row in rows]
    unitids = [unitid for unitid in unitids if unitid]
    if len(unitids) != 40 or len(set(unitids)) != 40:
        raise RuntimeError(
            f"Selection must contain exactly 40 unique unitids; found {len(unitids)} rows and {len(set(unitids))} unique"
        )
    if "123165" not in unitids:
        raise RuntimeError("Selection does not include Scripps College unitid 123165")
    return {"rows": len(unitids), "unitids": unitids}


def _snowflake_client():
    import revelio
    import revelio.base

    user = os.environ.get("OUTCOMES_SNOWFLAKE_USER", "caelan@reveliolabs.com")
    return revelio.base.client("snowflake", user)


def _discover_latest_standard_tables() -> tuple[str, str, str]:
    client = _snowflake_client()
    tables = client.load_df(
        """
        SELECT table_name
        FROM CLIENT_STANDARD.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'REVELIO_INTERNAL'
          AND table_name LIKE 'STANDARD_%_INDIVIDUAL_%'
        """
    )
    names = [str(value).upper() for value in tables.iloc[:, 0].dropna().tolist()]
    by_month: dict[str, set[str]] = {}
    for name in names:
        match = STANDARD_TABLE_PATTERN.fullmatch(name)
        if not match:
            continue
        month, kind = match.groups()
        by_month.setdefault(month, set()).add(kind.upper())
    common = sorted(month for month, kinds in by_month.items() if {"POSITION", "USER"} <= kinds)
    if not common:
        raise RuntimeError(
            "Could not discover a common monthly STANDARD position/user source. "
            "Pass --position-table and --user-table explicitly."
        )
    month = common[-1]
    prefix = f"CLIENT_STANDARD.REVELIO_INTERNAL.STANDARD_{month}_INDIVIDUAL"
    return f"{prefix}_POSITION", f"{prefix}_USER", month


def _audit_source(
    *,
    education_cip: str,
    position_table: str,
    user_table: str,
) -> dict[str, Any]:
    client = _snowflake_client()
    education = client.load_df(
        f"""
        SELECT
          COUNT(*) AS education_rows,
          COUNT(DISTINCT user_id) AS people,
          COUNT(DISTINCT CASE WHEN degree = 'Bachelor' THEN user_id END) AS bachelor_people,
          COUNT(DISTINCT CASE
            WHEN degree = 'Bachelor' AND enddate IS NOT NULL
             AND YEAR(enddate) BETWEEN 2020 AND 2025 THEN user_id END
          ) AS recent_bachelor_people,
          MIN(enddate) AS min_enddate,
          MAX(enddate) AS max_enddate
        FROM {education_cip}
        WHERE CAST(unitid AS VARCHAR) = '123165'
        """
    )
    if education.empty:
        raise RuntimeError(f"Scripps audit returned no result from {education_cip}")
    row = {str(key).lower(): value for key, value in education.iloc[0].to_dict().items()}
    people = int(row.get("people") or 0)
    if people < 500:
        raise RuntimeError(
            f"Scripps has only {people:,} people in {education_cip}; refusing to start the full run"
        )

    labels = client.load_df(
        f"""
        SELECT
          CAST(unitid AS VARCHAR) AS unitid,
          ipeds_name,
          COUNT(DISTINCT user_id) AS people
        FROM {education_cip}
        WHERE LOWER(COALESCE(ipeds_name, '')) LIKE '%scripps%'
           OR CAST(unitid AS VARCHAR) = '123165'
        GROUP BY CAST(unitid AS VARCHAR), ipeds_name
        ORDER BY people DESC
        """
    )
    return {
        "education_cip": education_cip,
        "position_table": position_table,
        "user_table": user_table,
        "scripps": {key: str(value) if hasattr(value, "isoformat") else value for key, value in row.items()},
        "scripps_name_matches": labels.to_dict(orient="records"),
    }


def _resolve_sources(args: argparse.Namespace) -> tuple[str, str, str | None]:
    if bool(args.position_table) != bool(args.user_table):
        raise RuntimeError("--position-table and --user-table must be supplied together")
    if args.position_table and args.user_table:
        return args.position_table, args.user_table, None
    return _discover_latest_standard_tables()


def _worker(args: argparse.Namespace, run_root: Path) -> int:
    status_path = run_root / "status.json"
    output_dir = run_root / "school_outcomes_data"
    platform_dir = output_dir / "platform_parquet"
    archive_path = run_root / "scripps-competitor-40-platform.tar.gz"
    validation_path = run_root / "platform-validation.json"
    trial_validation_path = run_root / "trial-validation.json"
    executed_notebook = run_root / "executed-precompute.ipynb"

    _write_status(
        status_path,
        state="starting",
        started_at=_utc_now(),
        run_root=str(run_root),
        output_dir=str(output_dir),
        archive=str(archive_path),
    )

    try:
        selection = _selection_summary(args.selection)
        position_table, user_table, source_month = _resolve_sources(args)
        education_cip = args.education_cip or os.environ.get(
            "OUTCOMES_EDUCATION_CIP",
            DEFAULT_EDUCATION_CIP,
        )
        print(f"Selected schools: {selection['rows']}", flush=True)
        print(f"Education source: {education_cip}", flush=True)
        print(f"Position source: {position_table}", flush=True)
        print(f"User source: {user_table}", flush=True)
        if source_month:
            print(f"Auto-selected latest common STANDARD source month: {source_month}", flush=True)

        _write_status(status_path, state="auditing_sources", source_month=source_month)
        source_audit = _audit_source(
            education_cip=education_cip,
            position_table=position_table,
            user_table=user_table,
        )
        (run_root / "source-audit.json").write_text(
            json.dumps(source_audit, indent=2, default=str) + "\n"
        )

        jupyter = shutil.which("jupyter")
        if not jupyter:
            raise RuntimeError("jupyter is not available in this environment")

        env = os.environ.copy()
        env.update(
            {
                "PYTHONUNBUFFERED": "1",
                "OUTCOMES_SCHOOL_SELECTION_FILE": str(args.selection),
                "OUTCOMES_PRECOMPUTE_OUT_DIR": str(output_dir),
                "OUTCOMES_USE_US_STRONG_EXCELLENT_SCHOOLS": "0",
                "OUTCOMES_RUN_CAPACITY_AUDIT": "0",
                "OUTCOMES_EDUCATION_CIP": education_cip,
                "OUTCOMES_POSITION_TABLE": position_table,
                "OUTCOMES_USER_TABLE": user_table,
                "PLATFORM_EXPORT_SCRIPT": str(ROOT / "scripts" / "platform_parquet_export.py"),
            }
        )
        if args.data_dir:
            env["OUTCOMES_PRECOMPUTE_DATA_DIR"] = str(args.data_dir)

        _write_status(status_path, state="running_precompute")
        _run(
            [
                jupyter,
                "nbconvert",
                "--to",
                "notebook",
                "--execute",
                str(args.notebook),
                "--output",
                executed_notebook.name,
                "--output-dir",
                str(run_root),
                "--ExecutePreprocessor.timeout=-1",
                "--ExecutePreprocessor.kernel_name=fast-pipelines",
            ],
            env=env,
        )

        if not (platform_dir / "platform_manifest.json").exists():
            raise RuntimeError(f"Precompute finished without a platform manifest at {platform_dir}")

        _write_status(status_path, state="validating")
        validation_command = [
            sys.executable,
            str(ROOT / "scripts" / "validate_platform_release.py"),
            str(platform_dir),
            "--json-out",
            str(validation_path),
        ]
        if args.quick_validation:
            validation_command.append("--quick")
        _run(validation_command, env=env)
        _run(
            [
                sys.executable,
                str(ROOT / "scripts" / "validate_trial_release.py"),
                str(platform_dir),
                "--selection",
                str(args.selection),
                "--json-out",
                str(trial_validation_path),
            ],
            env=env,
        )

        manifest_path = platform_dir / "platform_manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["trial_release"] = {
            "name": "Scripps competitor 40",
            "school_count": 40,
            "selection_file": args.selection.name,
            "selection_unitids": selection["unitids"],
            "source_month": source_month,
            "source_audit": "source-audit.json",
        }
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

        _write_status(status_path, state="packaging")
        _run(
            [
                "bash",
                str(ROOT / "scripts" / "package_platform_data.sh"),
                str(output_dir),
                str(archive_path),
            ],
            env=env,
        )

        ready = {
            "state": "complete",
            "completed_at": _utc_now(),
            "archive": str(archive_path),
            "archive_bytes": archive_path.stat().st_size,
            "platform_dir": str(platform_dir),
            "validation": str(validation_path),
            "trial_validation": str(trial_validation_path),
        }
        (run_root / "READY.json").write_text(json.dumps(ready, indent=2) + "\n")
        _write_status(status_path, **ready)
        print(json.dumps(ready, indent=2), flush=True)
        return 0
    except Exception as exc:
        _write_status(
            status_path,
            state="failed",
            failed_at=_utc_now(),
            error=f"{type(exc).__name__}: {exc}",
        )
        print(f"FAILED: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise


def _resolved_run_root(value: Path | None) -> Path:
    if value:
        return value.expanduser().resolve()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (DEFAULT_RUNS_ROOT / f"scripps_competitor_40_{timestamp}").resolve()


def _launch_detached(args: argparse.Namespace, run_root: Path) -> int:
    run_root.mkdir(parents=True, exist_ok=False)
    log_path = run_root / "run.log"
    status_path = run_root / "status.json"
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--run-root",
        str(run_root),
        "--notebook",
        str(args.notebook),
        "--selection",
        str(args.selection),
    ]
    if args.data_dir:
        command.extend(["--data-dir", str(args.data_dir)])
    if args.education_cip:
        command.extend(["--education-cip", args.education_cip])
    if args.position_table:
        command.extend(["--position-table", args.position_table])
    if args.user_table:
        command.extend(["--user-table", args.user_table])
    if args.quick_validation:
        command.append("--quick-validation")

    with log_path.open("ab", buffering=0) as log:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    _write_status(
        status_path,
        state="launched",
        launched_at=_utc_now(),
        pid=process.pid,
        log=str(log_path),
        run_root=str(run_root),
    )
    print(f"Detached precompute started with PID {process.pid}")
    print(f"Run folder: {run_root}")
    print(f"Follow progress: tail -f {log_path}")
    print(f"Check status: cat {status_path}")
    print("It is safe to disconnect from SSH.")
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--notebook", type=Path, default=DEFAULT_NOTEBOOK)
    parser.add_argument("--selection", type=Path, default=DEFAULT_SELECTION)
    parser.add_argument("--run-root", type=Path)
    parser.add_argument("--data-dir", type=Path)
    parser.add_argument("--education-cip")
    parser.add_argument("--position-table")
    parser.add_argument("--user-table")
    parser.add_argument("--quick-validation", action="store_true")
    parser.add_argument(
        "--foreground",
        action="store_true",
        help="Run in this terminal instead of detaching.",
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    return parser


def main() -> int:
    args = _parser().parse_args()
    args.notebook = args.notebook.expanduser().resolve()
    args.selection = args.selection.expanduser().resolve()
    args.data_dir = args.data_dir.expanduser().resolve() if args.data_dir else None
    if not args.notebook.exists():
        raise SystemExit(f"Notebook not found: {args.notebook}")
    if not args.selection.exists():
        raise SystemExit(f"Selection file not found: {args.selection}")
    _selection_summary(args.selection)

    run_root = _resolved_run_root(args.run_root)
    if args.worker or args.foreground:
        run_root.mkdir(parents=True, exist_ok=True)
        return _worker(args, run_root)
    return _launch_detached(args, run_root)


if __name__ == "__main__":
    raise SystemExit(main())
