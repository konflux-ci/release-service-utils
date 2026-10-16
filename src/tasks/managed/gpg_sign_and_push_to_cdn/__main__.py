"""Run the gpg_sign_and_push_to_cdn task."""

from __future__ import annotations

from release_service_utils.tasks.managed.gpg_sign_and_push_to_cdn import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
