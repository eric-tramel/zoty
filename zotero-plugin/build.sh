#!/usr/bin/env bash
# Build zoty-bridge.xpi and its Zotero update manifest.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

python3 "$REPO_ROOT/scripts/build_zotero_plugin.py" \
  --check-version-sync \
  "$@"
