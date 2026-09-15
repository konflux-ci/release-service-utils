"""Entry point for extract_oot_kmods task."""

from __future__ import annotations

from release_service_utils.tasks.managed.extract_oot_kmods.extract_oot_kmods import main

if __name__ == "__main__":
    raise SystemExit(main())
