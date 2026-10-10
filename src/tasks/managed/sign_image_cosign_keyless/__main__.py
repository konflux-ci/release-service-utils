"""Entry point for sign_image_cosign_keyless task."""

from __future__ import annotations

from release_service_utils.tasks.managed.sign_image_cosign_keyless.sign_image_cosign_keyless import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
