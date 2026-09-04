"""Entry point for update_fbc_catalog task."""

from __future__ import annotations

from release_service_utils.tasks.internal.update_fbc_catalog.update_fbc_catalog import main

if __name__ == "__main__":
    raise SystemExit(main())
