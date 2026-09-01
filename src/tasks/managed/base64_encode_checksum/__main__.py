"""Entry point for base64_encode_checksum task."""

from __future__ import annotations

from release_service_utils.tasks.managed.base64_encode_checksum.base64_encode_checksum import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
