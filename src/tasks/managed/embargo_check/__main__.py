"""Entry point for embargo_check task."""

from __future__ import annotations

from release_service_utils.tasks.managed.embargo_check.embargo_check import main

if __name__ == "__main__":
    raise SystemExit(main())
