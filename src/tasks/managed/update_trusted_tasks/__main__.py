"""Entry point for update_trusted_tasks task."""

from __future__ import annotations

from release_service_utils.tasks.managed.update_trusted_tasks.update_trusted_tasks import main

if __name__ == "__main__":
    raise SystemExit(main())
