"""Entry point for process_file_updates task."""

from __future__ import annotations

from release_service_utils.tasks.internal.process_file_updates.process_file_updates import main

if __name__ == "__main__":
    raise SystemExit(main())
