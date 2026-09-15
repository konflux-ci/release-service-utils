"""Entry point for collect_gh_params task."""

from __future__ import annotations

from release_service_utils.tasks.managed.collect_gh_params.collect_gh_params import main

if __name__ == "__main__":
    raise SystemExit(main())
