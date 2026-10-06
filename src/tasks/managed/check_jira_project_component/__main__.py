"""Entry point for check_jira_project_component task."""

from __future__ import annotations

from release_service_utils.tasks.managed.check_jira_project_component import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
