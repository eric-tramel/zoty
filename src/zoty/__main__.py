"""Allow `python -m zoty` to use the zoty CLI."""

from zoty.cli import main

raise SystemExit(main())
