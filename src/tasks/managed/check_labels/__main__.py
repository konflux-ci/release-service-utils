"""Entry point for check_labels task."""

from __future__ import annotations

from release_service_utils.tasks.managed.check_labels.check_labels import main

if __name__ == "__main__":
    raise SystemExit(main())
