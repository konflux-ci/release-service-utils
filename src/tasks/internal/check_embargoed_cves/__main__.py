"""Entry point for check_embargoed_cves task."""

from __future__ import annotations

from release_service_utils.tasks.internal.check_embargoed_cves.check_embargoed_cves import main

if __name__ == "__main__":
    raise SystemExit(main())
