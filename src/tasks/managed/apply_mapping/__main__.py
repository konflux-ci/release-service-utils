"""Entry point for apply_mapping task."""

from __future__ import annotations

from release_service_utils.tasks.managed.apply_mapping.apply_mapping import main

if __name__ == "__main__":
    raise SystemExit(main())
