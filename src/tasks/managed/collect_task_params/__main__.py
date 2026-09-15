"""Entry point for collect_task_params task."""

from __future__ import annotations

from release_service_utils.tasks.managed.collect_task_params.collect_task_params import main

if __name__ == "__main__":
    raise SystemExit(main())
