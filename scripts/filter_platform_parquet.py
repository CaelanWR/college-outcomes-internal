#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import tarfile
from datetime import datetime, timezone
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


def _read_unitids(path: Path) -> list[str]:
    unitids = [
        line.strip()
        for line in path.expanduser().read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    unitids = list(dict.fromkeys(unitids))
    if not unitids:
        raise SystemExit(f"No unitids found in {path}")
    return unitids


def _parquet_files(path: Path) -> list[str]:
    return sorted(str(candidate) for candidate in path.rglob("*.parquet") if candidate.is_file())


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _manifest_dataset_paths(manifest: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    for key in ("base_fact", "current_students_fact"):
        value = manifest.get(key)
        if isinstance(value, dict) and value.get("path"):
            paths.append(str(value["path"]))
    for group_key in ("aggregate_facts", "work_facts"):
        group = manifest.get(group_key) or {}
        if isinstance(group, dict):
            for value in group.values():
                if isinstance(value, dict) and value.get("path"):
                    paths.append(str(value["path"]))
    return list(dict.fromkeys(paths))


def _copy_references(source_root: Path, dest_root: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    references = manifest.get("references") or {}
    copied: dict[str, Any] = {}
    if not isinstance(references, dict):
        return copied
    for name, rel in references.items():
        source = source_root / str(rel)
        dest = dest_root / str(rel)
        if not source.exists():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, dest)
        copied[name] = str(rel)
    return copied


def _write_filtered_dataset(
    con: duckdb.DuckDBPyConnection,
    source_root: Path,
    dest_root: Path,
    rel: str,
    unitids: list[str],
) -> dict[str, Any] | None:
    source = source_root / rel
    files = _parquet_files(source)
    if not files:
        return None

    columns = {
        str(row[0]).lower()
        for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [files]).fetchall()
    }
    if "unitid" not in columns:
        dest = dest_root / rel
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(source, dest, ignore=shutil.ignore_patterns("._*", ".DS_Store"))
        return {
            "path": rel,
            "rows": int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [files]).fetchone()[0]),
            "parts": len(_parquet_files(dest)),
            "rows_per_part": None,
            "mode": "copied_unfiltered_no_unitid",
        }

    unitid_list = ",".join(_sql_literal(unitid) for unitid in unitids)
    row_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE CAST(unitid AS VARCHAR) IN ({unitid_list})
        """,
        [files],
    ).fetchone()[0]
    if not row_count:
        return None

    dest = dest_root / rel
    dest.mkdir(parents=True, exist_ok=True)
    target = str(dest / "part-00000.parquet").replace("'", "''")
    con.execute(
        f"""
        COPY (
          SELECT *
          FROM read_parquet(?)
          WHERE CAST(unitid AS VARCHAR) IN ({unitid_list})
        ) TO '{target}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """,
        [files],
    )
    return {
        "path": rel,
        "rows": int(row_count),
        "parts": 1,
        "rows_per_part": int(row_count),
        "filtered_unitid_count": len(unitids),
    }


def _write_archive(platform_root: Path, archive: Path) -> None:
    archive.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(platform_root, arcname="platform_parquet")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a filtered platform_parquet bundle for Render.")
    parser.add_argument("--source", required=True, help="Source platform_parquet path or parent.")
    parser.add_argument("--unitids-file", required=True, help="Text file with one enabled unitid per line.")
    parser.add_argument("--dest", required=True, help="Destination directory for filtered platform_parquet.")
    parser.add_argument("--archive", help="Optional output .tar.gz archive.")
    args = parser.parse_args()

    source_root = _platform_root(Path(args.source))
    dest_root = Path(args.dest).expanduser().resolve()
    if dest_root.exists():
        shutil.rmtree(dest_root)
    dest_root.mkdir(parents=True)

    manifest_path = source_root / "platform_manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"Missing manifest: {manifest_path}")
    source_manifest = json.loads(manifest_path.read_text())
    unitids = _read_unitids(Path(args.unitids_file))

    con = duckdb.connect(database=":memory:")
    new_manifest = {
        **source_manifest,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_manifest_created_at": source_manifest.get("created_at"),
        "filtered_unitid_count": len(unitids),
        "filter_unitids_file": str(Path(args.unitids_file).expanduser().resolve()),
        "aggregate_facts": {},
        "work_facts": {},
    }
    try:
        for rel in _manifest_dataset_paths(source_manifest):
            info = _write_filtered_dataset(con, source_root, dest_root, rel, unitids)
            if not info:
                continue
            if rel == source_manifest.get("base_fact", {}).get("path"):
                new_manifest["base_fact"] = {**source_manifest.get("base_fact", {}), **info}
            elif rel == source_manifest.get("current_students_fact", {}).get("path"):
                new_manifest["current_students_fact"] = {**source_manifest.get("current_students_fact", {}), **info}
            elif rel.startswith("aggregate_facts/"):
                name = rel.split("/", 1)[1]
                old = (source_manifest.get("aggregate_facts") or {}).get(name, {})
                new_manifest["aggregate_facts"][name] = {**old, **info}
            elif rel.startswith("work_facts/"):
                name = rel.split("/", 1)[1]
                old = (source_manifest.get("work_facts") or {}).get(name, {})
                new_manifest["work_facts"][name] = {**old, **info}
    finally:
        con.close()

    new_manifest["references"] = _copy_references(source_root, dest_root, source_manifest)
    (dest_root / "platform_manifest.json").write_text(json.dumps(new_manifest, indent=2) + "\n")

    if args.archive:
        _write_archive(dest_root, Path(args.archive).expanduser().resolve())

    print(json.dumps({
        "dest": str(dest_root),
        "archive": args.archive,
        "filtered_unitid_count": len(unitids),
        "base_rows": new_manifest.get("base_fact", {}).get("rows"),
        "aggregate_fact_count": len(new_manifest.get("aggregate_facts", {})),
        "work_fact_count": len(new_manifest.get("work_facts", {})),
    }, indent=2))


if __name__ == "__main__":
    main()
