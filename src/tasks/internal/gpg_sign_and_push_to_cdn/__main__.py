"""Run the gpg_sign_and_push_to_cdn internal task."""

from __future__ import annotations

import sys

from release_service_utils.tasks.internal.gpg_sign_and_push_to_cdn import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
