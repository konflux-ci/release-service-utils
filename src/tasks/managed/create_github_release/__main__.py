"""Entry point for create_github_release task."""

from __future__ import annotations

from release_service_utils.tasks.managed.create_github_release.create_github_release import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
