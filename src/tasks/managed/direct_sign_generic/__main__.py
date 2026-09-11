"""Entry point for direct_sign_generic task."""

from __future__ import annotations

from release_service_utils.tasks.managed.direct_sign_generic.direct_sign_generic import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
