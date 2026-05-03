#!/usr/bin/env sh
set -eu

if [ "$#" -ne 1 ]; then
  printf '%s\n' "Usage: $0 git@github.com:OWNER/REPO.git"
  printf '%s\n' "   or: $0 https://github.com/OWNER/REPO.git"
  exit 2
fi

REMOTE_URL="$1"

if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "$REMOTE_URL"
else
  git remote add origin "$REMOTE_URL"
fi

git branch -M main
git push -u origin main

