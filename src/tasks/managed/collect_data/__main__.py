"""Entry point for collect_data task."""

from __future__ import annotations

from release_service_utils.tasks.managed.collect_data.collect_data import main

if __name__ == "__main__":
    raise SystemExit(main())
