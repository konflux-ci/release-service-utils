"""Entry point for rh_sign_image_cosign task."""

from __future__ import annotations

from release_service_utils.tasks.managed.rh_sign_image_cosign.rh_sign_image_cosign import main

if __name__ == "__main__":
    raise SystemExit(main())
