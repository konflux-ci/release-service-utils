"""Entry point for collect_signing_params task."""

from __future__ import annotations

from release_service_utils.tasks.managed.collect_signing_params.collect_signing_params import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
