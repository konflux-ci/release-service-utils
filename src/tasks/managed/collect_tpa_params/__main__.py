"""Entry point for collect_tpa_params task."""

from __future__ import annotations

from release_service_utils.tasks.managed.collect_tpa_params.collect_tpa_params import main

if __name__ == "__main__":
    raise SystemExit(main())
