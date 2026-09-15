"""Entry point for the ``push_rpm_data_to_pyxis`` task."""

from __future__ import annotations

from release_service_utils.tasks.managed.push_rpm_data_to_pyxis.push_rpm_data_to_pyxis import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
