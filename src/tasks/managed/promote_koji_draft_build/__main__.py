"""Entry point for promote_koji_draft_build task."""

from __future__ import annotations

from release_service_utils.tasks.managed.promote_koji_draft_build.promote_koji_draft_build import (  # noqa: E501
    main,
)

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
