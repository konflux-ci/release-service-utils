"""Entry point for cleanup_workspace task."""

from __future__ import annotations

from release_service_utils.tasks.managed.cleanup_workspace.cleanup_workspace import main

if __name__ == "__main__":
    raise SystemExit(main())
