"""Entry point for check_data_keys task."""

from __future__ import annotations

from release_service_utils.tasks.managed.check_data_keys.check_data_keys import main

if __name__ == "__main__":
    raise SystemExit(main())
