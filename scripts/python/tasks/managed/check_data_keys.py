"""Check data keys for a release pipeline.

Reads DATA_FILE and PROFILE from environment variables, loads the JSON,
and validates it against the Pydantic model registered for that pipeline.
Exits 0 on success, 1 on any validation or input error.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import ValidationError

import file
import tekton
from logger import logger
from validate_data_keys import format_errors, validate

PROG = "check_data_keys.py"


def run_check_data_keys(*, data_path: Path, profile: str) -> None:
    """Load and validate data_path against the profile model."""
    if not data_path.is_file():
        raise FileNotFoundError(f"Data file not found: {data_path}")

    data = file.load_json_dict(data_path)
    validate(data, profile)
    logger.info("Validation passed for profile %s", profile)


def main() -> int:
    """Entry point called by the Tekton task step."""
    data_path = Path(tekton.require_env("DATA_FILE"))
    profile = tekton.require_env("PROFILE")

    try:
        run_check_data_keys(data_path=data_path, profile=profile)
    except FileNotFoundError as exc:
        logger.error("%s: %s", PROG, exc)
        return 1
    # ValidationError is a subclass of ValueError so it must be
    # caught first, otherwise the ValueError handler swallows it
    # and prints the raw Pydantic output instead of our format.
    except ValidationError as exc:
        logger.error(
            "%s: Validation failed for profile %r with %d error(s):\n%s",
            PROG,
            profile,
            exc.error_count(),
            format_errors(exc),
        )
        return 1
    except ValueError as exc:
        logger.error("%s: %s", PROG, exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
