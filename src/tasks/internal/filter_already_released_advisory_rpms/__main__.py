"""Entry point for filter_already_released_advisory_rpms task."""

from __future__ import annotations

from .filter_already_released_advisory_rpms import main

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
