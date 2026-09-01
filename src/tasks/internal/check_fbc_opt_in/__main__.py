"""Entry point for check_fbc_opt_in task."""

from __future__ import annotations

from release_service_utils.tasks.internal.check_fbc_opt_in.check_fbc_opt_in import main

if __name__ == "__main__":
    raise SystemExit(main())
