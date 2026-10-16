"""Entry point for the sign_oot_kmods task."""

from __future__ import annotations

from release_service_utils.tasks.managed.sign_oot_kmods.sign_oot_kmods import main

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
