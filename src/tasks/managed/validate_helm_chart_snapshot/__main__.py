"""Run the validate_helm_chart_snapshot task."""

from __future__ import annotations

from release_service_utils.tasks.managed.validate_helm_chart_snapshot.validate_helm_chart_snapshot import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
