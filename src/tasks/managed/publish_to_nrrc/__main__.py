"""Entry point for publish_to_nrrc task."""

from __future__ import annotations

from release_service_utils.tasks.managed.publish_to_nrrc.publish_to_nrrc import main

if __name__ == "__main__":
    raise SystemExit(main())
