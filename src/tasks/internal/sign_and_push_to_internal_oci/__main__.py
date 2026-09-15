"""Provide the module entry point for the sign-and-push-to-internal-oci task."""

from __future__ import annotations

from .sign_and_push_to_internal_oci import main

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
