"""Entry point for reduce_snapshot task."""

from __future__ import annotations

from release_service_utils.tasks.managed.reduce_snapshot.reduce_snapshot import main

if __name__ == "__main__":
    raise SystemExit(main())
