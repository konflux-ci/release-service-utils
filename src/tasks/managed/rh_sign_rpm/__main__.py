"""Run the rh_sign_rpm task as a module entry point."""

from __future__ import annotations

from release_service_utils.tasks.managed.rh_sign_rpm import main

if __name__ == "__main__":
    raise SystemExit(main())
