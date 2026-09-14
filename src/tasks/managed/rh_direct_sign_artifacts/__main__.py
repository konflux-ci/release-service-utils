"""Entry point for rh_direct_sign_artifacts task."""

from __future__ import annotations

from release_service_utils.tasks.managed.rh_direct_sign_artifacts import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
