"""Entry point for push_rpm_to_koji task."""

from __future__ import annotations

from release_service_utils.tasks.managed.push_rpm_to_koji.push_rpm_to_koji import main

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
