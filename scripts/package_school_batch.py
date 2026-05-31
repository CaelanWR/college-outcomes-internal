#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Any

import duckdb


def _platform_root(path: Path) -> Path:
    path = path.expanduser().resolve()
    if path.name == "platform_parquet":
        return path
    candidate = path / "platform_parquet"
    if candidate.exists():
        return candidate
    raise SystemExit(f"Could not find platform_parquet under {path}")


def _read_schools(values: list[str], schools_file: str | None) -> list[str]:
    schools: list[str] = []
    for value in values:
        schools.extend(part.strip() for part in value.split(",") if part.strip())
    if schools_file:
        schools.extend(
            line.strip()
            for line in Path(schools_file).expanduser().read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        )
    schools = list(dict.fromkeys(str(school) for school in schools))
    if not schools:
        raise SystemExit("Provide at least one school unitid with --school or --schools-file")
    return schools


def _manifest_dataset_paths(manifest: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    for key in ("base_fact", "current_students_fact"):
        value = manifest.get(key)
        if isinstance(value, dict) and value.get("path"):
            paths.append(str(value["path"]))
    for group_key in ("aggregate_facts", "work_facts", "references"):
        group = manifest.get(group_key) or {}
        if not isinstance(group, dict):
            continue
        for value in group.values():
            if isinstance(value, dict) and value.get("path"):
                paths.append(str(value["path"]))
            elif isinstance(value, str):
                paths.append(value)
    return list(dict.fromkeys(paths))


def _scan_dataset_paths(root: Path) -> list[str]:
    paths = set()
    for path in root.rglob("*.parquet"):
        rel = path.parent.relative_to(root)
        parts = rel.parts
        if not parts:
            continue
        if parts[0] in {"base_fact", "current_students_fact", "references"}:
            paths.add(parts[0] if parts[0] != "references" else str(rel))
        elif parts[0] in {"aggregate_facts", "work_facts"} and len(parts) >= 2:
            paths.add(str(Path(parts[0]) / parts[1]))
    return sorted(paths)


def _dataset_paths(root: Path) -> list[str]:
    manifest_path = root / "platform_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        paths = [path for path in _manifest_dataset_paths(manifest) if (root / path).exists()]
        if paths:
            return paths
    return _scan_dataset_paths(root)


def _parquet_files(path: Path) -> list[str]:
    if path.is_file() and path.suffix == ".parquet":
        return [str(path)]
    return sorted(str(candidate) for candidate in path.rglob("*.parquet") if candidate.is_file())


def _columns(con: duckdb.DuckDBPyConnection, files: list[str]) -> set[str]:
    rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [files]).fetchall()
    return {str(row[0]).lower() for row in rows}


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _copy_static_dataset(source: Path, dest: Path) -> int:
    if source.is_file():
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, dest)
        return 1
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(
        source,
        dest,
        ignore=shutil.ignore_patterns("._*", ".DS_Store"),
    )
    return len(_parquet_files(dest))


def _write_filtered_dataset(
    con: duckdb.DuckDBPyConnection,
    files: list[str],
    dest: Path,
    batch_name: str,
    schools: list[str],
) -> int:
    placeholders = ",".join(["?"] * len(schools))
    count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE CAST(unitid AS VARCHAR) IN ({placeholders})
        """,
        [files, *schools],
    ).fetchone()[0]
    if not count:
        return 0

    batch_dir = dest / batch_name
    batch_dir.mkdir(parents=True, exist_ok=True)
    output = batch_dir / "part-00000.parquet"
    target = str(output).replace("'", "''")
    school_list = ",".join(_sql_literal(school) for school in schools)
    con.execute(
        f"""
        COPY (
          SELECT *
          FROM read_parquet(?)
          WHERE CAST(unitid AS VARCHAR) IN ({school_list})
        ) TO '{target}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """,
        [files],
    )
    return int(count)


def _write_archive(staging_parent: Path, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output, "w:gz") as tar:
        tar.add(staging_parent / "platform_parquet", arcname="platform_parquet")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create an additive platform_parquet archive for a batch of school unitids."
    )
    parser.add_argument("--source", required=True, help="Path to platform_parquet or its parent directory.")
    parser.add_argument("--output", required=True, help="Output .tar.gz path.")
    parser.add_argument("--batch-name", required=True, help="Stable name like schools-001.")
    parser.add_argument("--school", action="append", default=[], help="Unitid or comma-separated unitids.")
    parser.add_argument("--schools-file", help="Text file with one unitid per line.")
    parser.add_argument(
        "--include-static",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include non-unitid reference datasets in the archive.",
    )
    args = parser.parse_args()

    root = _platform_root(Path(args.source))
    schools = _read_schools(args.school, args.schools_file)
    batch_name = args.batch_name.strip().replace("/", "_")
    if not batch_name:
        raise SystemExit("--batch-name cannot be empty")

    with tempfile.TemporaryDirectory(prefix="outcomes-school-batch-") as tmp:
        staging_root = Path(tmp) / "platform_parquet"
        staging_root.mkdir(parents=True)
        con = duckdb.connect(database=":memory:")
        summary: dict[str, Any] = {
            "batch_name": batch_name,
            "source": str(root),
            "schools": schools,
            "datasets": {},
        }
        try:
            for dataset in _dataset_paths(root):
                source_dir = root / dataset
                files = _parquet_files(source_dir)
                if not files:
                    continue
                dest_dir = staging_root / dataset
                columns = _columns(con, files)
                if "unitid" in columns:
                    rows = _write_filtered_dataset(con, files, dest_dir, batch_name, schools)
                    if rows:
                        summary["datasets"][dataset] = {"rows": rows, "mode": "filtered"}
                elif args.include_static:
                    parts = _copy_static_dataset(source_dir, dest_dir)
                    summary["datasets"][dataset] = {"parts": parts, "mode": "copied"}
        finally:
            con.close()

        if not summary["datasets"]:
            raise SystemExit("No matching rows were written. Check the school unitids and source path.")

        manifest_dir = staging_root / "batch_manifests"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        (manifest_dir / f"{batch_name}.json").write_text(json.dumps(summary, indent=2) + "\n")
        _write_archive(Path(tmp), Path(args.output).expanduser().resolve())

    print(f"Wrote {args.output}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
