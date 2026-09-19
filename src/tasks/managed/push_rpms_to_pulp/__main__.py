"""Entry point for push_rpms_to_pulp task."""

from __future__ import annotations

from release_service_utils.tasks.managed.push_rpms_to_pulp.push_rpms_to_pulp import main

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
