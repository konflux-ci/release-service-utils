"""Entry point for push_oot_kmods_to_git task."""

from __future__ import annotations

from release_service_utils.tasks.managed.push_oot_kmods_to_git.push_oot_kmods_to_git import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
