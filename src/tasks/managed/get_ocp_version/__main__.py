"""Entry point for get_ocp_version task."""

from __future__ import annotations

from release_service_utils.tasks.managed.get_ocp_version.get_ocp_version import main

if __name__ == "__main__":
    raise SystemExit(main())
