"""Entry point for sign_checksum_blob task."""

from __future__ import annotations

from release_service_utils.tasks.managed.sign_checksum_blob.sign_checksum_blob import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
