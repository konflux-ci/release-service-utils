"""Run the sign_and_push_to_internal_oci task as a module entry point."""

from __future__ import annotations

from release_service_utils.tasks.managed.sign_and_push_to_internal_oci import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
