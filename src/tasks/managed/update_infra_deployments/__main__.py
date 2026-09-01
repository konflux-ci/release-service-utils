"""Entry point for update_infra_deployments task."""

from __future__ import annotations

from release_service_utils.tasks.managed.update_infra_deployments.update_infra_deployments import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
