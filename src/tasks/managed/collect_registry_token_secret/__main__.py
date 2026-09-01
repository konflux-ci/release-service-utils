"""Entry point for collect_registry_token_secret task."""

from __future__ import annotations

from release_service_utils.tasks.managed.collect_registry_token_secret.collect_registry_token_secret import (  # noqa: E501
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
