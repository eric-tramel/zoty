#!/usr/bin/env python3
"""Verify that a built zoty wheel contains the bundled Zotero bridge XPI."""

from __future__ import annotations

import argparse
import io
import hashlib
import zipfile
from pathlib import Path


WHEEL_XPI_PATH = "zoty/assets/zoty-bridge.xpi"
SOURCE_XPI_PATH = Path("zotero-plugin/dist/zoty-bridge.xpi")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path, help="Path to a built zoty wheel.")
    args = parser.parse_args()

    if not SOURCE_XPI_PATH.is_file():
        raise SystemExit(f"Missing source XPI: {SOURCE_XPI_PATH}")

    with zipfile.ZipFile(args.wheel) as wheel:
        try:
            xpi_bytes = wheel.read(WHEEL_XPI_PATH)
        except KeyError as exc:
            raise SystemExit(f"Wheel is missing {WHEEL_XPI_PATH}") from exc

    if hashlib.sha256(xpi_bytes).hexdigest() != _sha256(SOURCE_XPI_PATH):
        raise SystemExit("Wheel XPI hash does not match zotero-plugin/dist/zoty-bridge.xpi")

    with zipfile.ZipFile(io.BytesIO(xpi_bytes)) as xpi:
        names = set(xpi.namelist())
    missing = {"manifest.json", "bootstrap.js"} - names
    if missing:
        raise SystemExit(f"Bundled XPI is missing: {', '.join(sorted(missing))}")

    print(f"Verified bundled XPI in {args.wheel}")
    return 0


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
