"""Entry point for make_repo_public task."""

from __future__ import annotations

from release_service_utils.tasks.managed.make_repo_public.make_repo_public import main

if __name__ == "__main__":
    raise SystemExit(main())
