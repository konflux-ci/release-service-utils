"""Run rh_sign_python_wheels as a module entry point."""

from __future__ import annotations

from release_service_utils.tasks.managed.rh_sign_python_wheels.rh_sign_python_wheels import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
