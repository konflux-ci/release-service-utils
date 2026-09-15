"""Entry point for push_snapshot task."""

from __future__ import annotations

from release_service_utils.tasks.managed.push_snapshot.push_snapshot import main

if __name__ == "__main__":
    raise SystemExit(main())
