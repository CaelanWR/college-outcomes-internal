from __future__ import annotations

import os
import shutil
import sys
import tarfile
import urllib.request
import zipfile
from pathlib import Path


DATA_DIR = Path(os.environ.get("OUTCOMES_DATA_DIR", "/var/data/outcomes")).expanduser()
ALLOW_EMPTY_STARTUP = os.environ.get("OUTCOMES_ALLOW_EMPTY_STARTUP", "0").lower() in {"1", "true", "yes"}


def _has_parquet(path: Path) -> bool:
    return path.exists() and any(
        candidate.is_file() and not candidate.name.startswith(".")
        for candidate in path.rglob("*.parquet")
    )


def _safe_extract_tar(archive: Path, dest: Path) -> None:
    dest_root = dest.resolve()
    with tarfile.open(archive) as tar:
        for member in tar.getmembers():
            target = (dest / member.name).resolve()
            if not str(target).startswith(str(dest_root)):
                raise RuntimeError(f"Unsafe archive member: {member.name}")
        tar.extractall(dest)


def _safe_extract_zip(archive: Path, dest: Path) -> None:
    dest_root = dest.resolve()
    with zipfile.ZipFile(archive) as zf:
        for member in zf.namelist():
            target = (dest / member).resolve()
            if not str(target).startswith(str(dest_root)):
                raise RuntimeError(f"Unsafe archive member: {member}")
        zf.extractall(dest)


def _download(url: str, dest: Path) -> None:
    print("Downloading outcomes data archive...", flush=True)
    with urllib.request.urlopen(url, timeout=120) as response, dest.open("wb") as out:
        shutil.copyfileobj(response, out, length=1024 * 1024)


def _extract(archive: Path, dest: Path) -> None:
    print("Extracting outcomes data archive...", flush=True)
    name = archive.name.lower()
    if name.endswith(".zip"):
        _safe_extract_zip(archive, dest)
    elif name.endswith((".tar.gz", ".tgz", ".tar")):
        _safe_extract_tar(archive, dest)
    else:
        raise RuntimeError("OUTCOMES_DATA_ARCHIVE_URL must point to .tar, .tar.gz, .tgz, or .zip")


def _find_base_fact() -> Path | None:
    configured = os.environ.get("OUTCOMES_PARQUET_ROOT")
    if configured and _has_parquet(Path(configured).expanduser()):
        return Path(configured).expanduser()

    preferred = DATA_DIR / "platform_parquet" / "base_fact"
    if _has_parquet(preferred):
        return preferred

    for candidate in DATA_DIR.rglob("platform_parquet/base_fact"):
        if _has_parquet(candidate):
            return candidate
    return None


def _ensure_data() -> Path:
    base_fact = _find_base_fact()
    if base_fact:
        return base_fact

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    archive_url = os.environ.get("OUTCOMES_DATA_ARCHIVE_URL")
    if not archive_url:
        if ALLOW_EMPTY_STARTUP:
            empty_root = DATA_DIR / "platform_parquet" / "base_fact"
            empty_root.mkdir(parents=True, exist_ok=True)
            print(
                "No parquet data found. Starting anyway because OUTCOMES_ALLOW_EMPTY_STARTUP is enabled. "
                f"Upload/extract platform_parquet under {DATA_DIR}, then redeploy.",
                flush=True,
            )
            return empty_root
        raise RuntimeError(
            "No parquet data found. Set OUTCOMES_PARQUET_ROOT to a mounted base_fact path "
            "or set OUTCOMES_DATA_ARCHIVE_URL so the service can bootstrap its data disk."
        )

    archive_path = Path("/tmp/outcomes-data-archive")
    if archive_url.lower().endswith(".zip"):
        archive_path = archive_path.with_suffix(".zip")
    elif archive_url.lower().endswith(".tgz"):
        archive_path = archive_path.with_suffix(".tgz")
    elif archive_url.lower().endswith(".tar"):
        archive_path = archive_path.with_suffix(".tar")
    else:
        archive_path = archive_path.with_suffix(".tar.gz")

    _download(archive_url, archive_path)
    _extract(archive_path, DATA_DIR)
    archive_path.unlink(missing_ok=True)

    base_fact = _find_base_fact()
    if not base_fact:
        raise RuntimeError("Data archive extracted, but no platform_parquet/base_fact parquet files were found.")
    return base_fact


def main() -> None:
    try:
        base_fact = _ensure_data()
    except Exception as exc:
        print(f"Startup failed: {exc}", file=sys.stderr, flush=True)
        raise

    os.environ.setdefault("OUTCOMES_PARQUET_ROOT", str(base_fact))
    os.environ.setdefault("OUTCOMES_UVICORN_APP", "app:app")

    host = os.environ.get("HOST", "0.0.0.0")
    port = os.environ.get("PORT", "8000")
    app = os.environ["OUTCOMES_UVICORN_APP"]
    print(f"Starting API with OUTCOMES_PARQUET_ROOT={os.environ['OUTCOMES_PARQUET_ROOT']}", flush=True)
    os.execvp("uvicorn", ["uvicorn", app, "--host", host, "--port", port])


if __name__ == "__main__":
    main()
