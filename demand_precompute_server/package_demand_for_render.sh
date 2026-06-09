#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${OUTCOMES_PRECOMPUTE_OUT_DIR:-/data0/data0_caelan/nace_june5_503_precompute_server/school_outcomes_data_nace_june5_503_plus_elite}"
ARCHIVE_NAME="${1:-outcomes-demand-facts-render.tar.gz}"

cd "$OUT_DIR"

if [ ! -d "platform_parquet/demand_facts" ]; then
  echo "Missing platform_parquet/demand_facts. Run the demand export first." >&2
  exit 1
fi

if [ ! -f "platform_parquet/platform_manifest.json" ]; then
  echo "Missing platform_parquet/platform_manifest.json." >&2
  exit 1
fi

tar -czf "$ARCHIVE_NAME" \
  platform_parquet/demand_facts \
  platform_parquet/platform_manifest.json

du -sh "$ARCHIVE_NAME"
echo "Wrote $OUT_DIR/$ARCHIVE_NAME"
