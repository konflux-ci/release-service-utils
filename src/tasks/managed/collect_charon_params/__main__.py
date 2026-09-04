"""Entry point for collect_charon_params task."""

from __future__ import annotations

from release_service_utils.tasks.managed.collect_charon_params.collect_charon_params import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
