#!/usr/bin/env bash
# Build zoty-bridge.xpi from plugin sources.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIST_DIR="$SCRIPT_DIR/dist"

mkdir -p "$DIST_DIR"

cd "$SCRIPT_DIR"
rm -f "$DIST_DIR/zoty-bridge.xpi"
zip -j "$DIST_DIR/zoty-bridge.xpi" manifest.json bootstrap.js

echo "Built: $DIST_DIR/zoty-bridge.xpi"
