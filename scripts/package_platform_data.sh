#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: bash scripts/package_platform_data.sh /path/to/school_outcomes_data_v4_3 /path/to/outcomes-platform.tar.gz" >&2
  exit 2
fi

source_dir="$1"
archive_path="$2"

if [[ ! -d "$source_dir/platform_parquet/base_fact" ]]; then
  echo "Missing platform_parquet/base_fact under: $source_dir" >&2
  exit 1
fi

mkdir -p "$(dirname "$archive_path")"
tar -czf "$archive_path" -C "$source_dir" platform_parquet
echo "Wrote $archive_path"
