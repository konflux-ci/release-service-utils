"""Fail a signed RPM push after its first completed Pulp repository update."""

from __future__ import annotations

import os

from release_service_utils.helpers.pulp_client import PulpClient
from release_service_utils.tasks.managed.push_rpms_to_pulp import push_rpms_to_pulp


def main() -> int:
    """Run the real RPM push, injecting one failure only on the signed path."""
    if not os.environ.get("SIGNED_RPMS_OCI_ARTIFACT", "").strip():
        return push_rpms_to_pulp.main()

    original_add_content = PulpClient.add_content
    completed = 0

    def fail_after_first_add(
        client: PulpClient,
        repo_name: str,
        hrefs: list[str],
        timeout_seconds: int,
        *,
        remove_hrefs: list[str] | None = None,
    ) -> None:
        """Raise after the first real, completed repository modification."""
        nonlocal completed
        original_add_content(
            client, repo_name, hrefs, timeout_seconds, remove_hrefs=remove_hrefs
        )
        completed += 1
        if completed == 1:
            raise RuntimeError(
                f"Injected signed Pulp failure after completed repository update: {repo_name}"
            )

    PulpClient.add_content = fail_after_first_add
    return push_rpms_to_pulp.main()


if __name__ == "__main__":
    raise SystemExit(main())
