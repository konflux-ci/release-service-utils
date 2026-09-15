"""Entry point for create_advisory task."""

from __future__ import annotations

from release_service_utils.tasks.internal.create_advisory.create_advisory import main

if __name__ == "__main__":
    raise SystemExit(main())
