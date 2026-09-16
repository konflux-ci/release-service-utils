"""Run the prepare_fbc_snapshot task."""

from __future__ import annotations

from release_service_utils.tasks.managed.prepare_fbc_snapshot.prepare_fbc_snapshot import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
