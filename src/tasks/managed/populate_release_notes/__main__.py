"""Entry point for populate_release_notes task."""

from __future__ import annotations

from release_service_utils.tasks.managed.populate_release_notes.populate_release_notes import (
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
