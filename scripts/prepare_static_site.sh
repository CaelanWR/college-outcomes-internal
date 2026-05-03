#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
SOURCE_HTML="${1:-/Users/caelan/Downloads/Untitled/legacy-index.work.html}"

cp "$SOURCE_HTML" "$ROOT_DIR/site/index.html"
printf '%s\n' "Copied $SOURCE_HTML to $ROOT_DIR/site/index.html"

