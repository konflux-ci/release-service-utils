"""Fake SkopeoClient for testing."""

from __future__ import annotations

import os
import re
import yaml
from pathlib import Path
from typing import Any, Optional
from logging import Logger

from rsmodels.secret import Secret
from skopeo import SkopeoClientError


class FakeSkopeoClient:
    """Fake implementation of SkopeoClient for testing.

    Loads mock responses from a YAML configuration file specified by the
    RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP environment variable.

    YAML structure:
        inspect:
          - match:
              image: "docker://quay.io/image:tag"
              format: "{{.Digest}}"  # optional
            return: "sha256:abc123"  # string when format specified

          - match:
              image:
                regex: "docker://quay.io/.*@sha256:.*"
            return:  # dict when format not specified
              Digest: "sha256:abc123"
              Name: "quay.io/image"

        copy:
          - match:
              source: "docker://quay.io/src:tag"
              destination: "docker://quay.io/dest:tag"
            # omit return for success

          - match:
              source: "docker://quay.io/bad:tag"
              destination: "docker://quay.io/dest:tag"
            return:
              success: false
              stderr: "Error: manifest unknown"
              returncode: 1  # optional, defaults to 1
    """

    def __init__(
        self,
        *,
        debug: bool = False,
        insecure_policy: bool = False,
        tmpdir: Optional[str] = None,
        command_timeout: Optional[str] = None,
        override_arch: Optional[str] = None,
        override_os: Optional[str] = None,
        override_variant: Optional[str] = None,
        logger: Optional[Logger] = None,
    ):
        """Initialize fake client and load configuration from environment variable.

        Constructor parameters match SkopeoClient for drop-in compatibility.
        They are stored but not used in matching logic.
        """
        self.debug = debug
        self.insecure_policy = insecure_policy
        self.tmpdir = tmpdir
        self.command_timeout = command_timeout
        self.override_arch = override_arch
        self.override_os = override_os
        self.override_variant = override_variant
        self.logger = logger

        # Load configuration
        config_path = os.getenv("RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP")
        if not config_path:
            raise ValueError(
                "RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP environment variable not set"
            )

        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        self.config_path = config_path
        self.config = self._load_and_validate_config()

    def _load_and_validate_config(self) -> dict[str, Any]:
        """Load YAML config and validate structure."""
        try:
            with open(self.config_path, encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse YAML config: {e}") from e

        # Validate top-level structure
        if not isinstance(config, dict):
            raise ValueError(f"Config must be a dict, got {type(config).__name__}")

        # Validate each operation section
        for operation in ["inspect", "copy"]:
            if operation in config:
                if not isinstance(config[operation], list):
                    raise ValueError(f"Config section '{operation}' must be a list of rules")

                for idx, rule in enumerate(config[operation]):
                    self._validate_rule(operation, idx, rule)

        return config

    def _validate_rule(self, operation: str, idx: int, rule: dict) -> None:
        """Validate a single rule structure."""
        if not isinstance(rule, dict):
            raise ValueError(
                f"Rule #{idx} in '{operation}' must be a dict, got {type(rule).__name__}"
            )

        # Check required fields
        if "match" not in rule:
            raise ValueError(f"Rule #{idx} in '{operation}' missing required 'match' field")

        if not isinstance(rule["match"], dict):
            raise ValueError(f"Rule #{idx} in '{operation}': 'match' must be a dict")

        if operation == "inspect" and "return" not in rule:
            raise ValueError(f"Rule #{idx} in 'inspect' missing required 'return' field")

        # Validate return structure based on operation
        if "return" in rule:
            self._validate_return(operation, idx, rule)

    def _validate_return(self, operation: str, idx: int, rule: dict) -> None:
        """Validate return value structure for a rule."""
        return_value = rule["return"]

        if operation == "inspect":
            # Check if format is specified in match
            match = rule["match"]
            has_format = "format" in match

            if has_format:
                # Return must be string when format specified
                if not isinstance(return_value, str):
                    raise ValueError(
                        f"Rule #{idx} in 'inspect': when format is specified, "
                        f"return must be a string, got {type(return_value).__name__}"
                    )
            else:
                # Return must be dict when format not specified
                if not isinstance(return_value, dict):
                    raise ValueError(
                        f"Rule #{idx} in 'inspect': when format is not specified, "
                        f"return must be a dict, got {type(return_value).__name__}"
                    )

        elif operation == "copy":
            # Return must be dict for copy operations
            if not isinstance(return_value, dict):
                raise ValueError(
                    f"Rule #{idx} in 'copy': return must be a dict, "
                    f"got {type(return_value).__name__}"
                )

            # Check success field if present
            if "success" in return_value and not isinstance(return_value["success"], bool):
                raise ValueError(f"Rule #{idx} in 'copy': 'success' field must be boolean")

    def _match_value(self, pattern: Any, actual: Any) -> bool:
        """Check if actual value matches pattern.

        Pattern can be:
        - dict with 'regex' key: regex match (fullmatch)
        - any other value: exact equality match

        Secret objects are ignored (always match).
        None in actual doesn't match a specified pattern.
        """
        # Ignore Secret objects
        if isinstance(actual, Secret):
            return True

        # If pattern is a regex dict
        if isinstance(pattern, dict) and "regex" in pattern:
            if not isinstance(actual, str):
                return False
            regex_pattern = pattern["regex"]
            try:
                return re.fullmatch(regex_pattern, actual) is not None
            except re.error as e:
                raise ValueError(f"Invalid regex pattern '{regex_pattern}': {e}") from e

        # Exact match (None doesn't match specified value)
        if actual is None:
            return False

        return pattern == actual

    def _match_rule(self, rule_match: dict, actual_params: dict) -> bool:
        """Check if actual parameters match the rule.

        Only fields specified in rule_match need to match.
        Extra fields in actual_params are ignored.
        """
        for key, pattern in rule_match.items():
            actual_value = actual_params.get(key)
            if not self._match_value(pattern, actual_value):
                return False
        return True

    def _find_matching_rule(self, operation: str, actual_params: dict) -> Optional[dict]:
        """Find first matching rule for the operation.

        Returns the rule dict if found, None otherwise.
        """
        rules = self.config.get(operation, [])

        for idx, rule in enumerate(rules):
            if self._match_rule(rule["match"], actual_params):
                if self.logger:
                    self.logger.debug(
                        f"FAKE: Matched rule #{idx} for {operation}({actual_params})"
                    )
                return rule
        return None

    def _build_error_message(self, operation: str, actual_params: dict) -> str:
        """Build detailed error message when no rule matches."""
        params_str = "\n".join(f"  {k}: {repr(v)}" for k, v in actual_params.items())
        return (
            f"MOCK ERROR: No matching rule found for {operation}()\n"
            f"Attempted with:\n{params_str}\n"
            f"Config file: {self.config_path}"
        )

    def _build_command(self, operation: str, **kwargs) -> list[str]:
        """Reconstruct the skopeo command for error reporting."""
        cmd = ["skopeo", operation]

        # Add common parameters
        for key, value in kwargs.items():
            if value is None or isinstance(value, Secret):
                continue

            # Convert pythonic parameter names to CLI flags
            flag_name = key.replace("_", "-")

            if isinstance(value, bool):
                if value:
                    cmd.append(f"--{flag_name}")
            else:
                cmd.extend([f"--{flag_name}", str(value)])

        return cmd

    def inspect(
        self,
        image: str,
        *,
        format: Optional[str] = None,
        retry_times: Optional[str] = None,
        creds: Optional[Secret] = None,
        tls_verify: Optional[bool] = None,
        config: bool = False,
        raw: bool = False,
        no_tags: bool = False,
        authfile: Optional[str] = None,
        cert_dir: Optional[str] = None,
        registry_token: Optional[Secret] = None,
    ) -> dict[str, Any] | str:
        """Fake implementation of inspect operation.

        Matches against rules in config and returns the configured response.
        """
        # Build actual parameters for matching (only non-Secret, non-constructor params)
        actual_params = {"image": image}
        if format is not None:
            actual_params["format"] = format

        # Find matching rule
        rule = self._find_matching_rule("inspect", actual_params)

        if rule is None:
            # No match - raise error
            error_msg = self._build_error_message("inspect", actual_params)
            if self.logger:
                self.logger.error(error_msg)

            raise SkopeoClientError(
                "No mock match found",
                command=self._build_command(
                    "inspect",
                    image=image,
                    format=format,
                    retry_times=retry_times,
                    creds=creds,
                    tls_verify=tls_verify,
                    config=config,
                    raw=raw,
                    no_tags=no_tags,
                    authfile=authfile,
                    cert_dir=cert_dir,
                    registry_token=registry_token,
                ),
                returncode=1,
                stdout="",
                stderr=error_msg,
            )
        # Return the configured value
        return rule["return"]

    def copy(
        self,
        source: str,
        destination: str,
        *,
        all: Optional[bool] = None,
        preserve_digests: Optional[bool] = None,
        format: Optional[str] = None,
        retry_times: Optional[int] = None,
        quiet: bool = False,
        src_creds: Optional[Secret] = None,
        src_tls_verify: Optional[bool] = None,
        src_no_creds: bool = False,
        src_cert_dir: Optional[str] = None,
        src_authfile: Optional[str] = None,
        dest_creds: Optional[Secret] = None,
        dest_tls_verify: Optional[bool] = None,
        dest_no_creds: bool = False,
        dest_cert_dir: Optional[str] = None,
        dest_authfile: Optional[str] = None,
        remove_signatures: bool = False,
    ) -> None:
        """Fake implementation of copy operation.

        Matches against rules in config and either returns None (success)
        or raises SkopeoClientError (failure).
        """
        # Build actual parameters for matching
        actual_params = {
            "source": source,
            "destination": destination,
        }

        # Find matching rule
        rule = self._find_matching_rule("copy", actual_params)

        if rule is None:
            # No match - raise error
            error_msg = self._build_error_message("copy", actual_params)
            if self.logger:
                self.logger.error(error_msg)

            raise SkopeoClientError(
                "No mock match found",
                command=self._build_command(
                    "copy",
                    source=source,
                    destination=destination,
                    all=all,
                    preserve_digests=preserve_digests,
                    format=format,
                    retry_times=retry_times,
                    quiet=quiet,
                    src_creds=src_creds,
                    src_tls_verify=src_tls_verify,
                    src_no_creds=src_no_creds,
                    src_cert_dir=src_cert_dir,
                    src_authfile=src_authfile,
                    dest_creds=dest_creds,
                    dest_tls_verify=dest_tls_verify,
                    dest_no_creds=dest_no_creds,
                    dest_cert_dir=dest_cert_dir,
                    dest_authfile=dest_authfile,
                    remove_signatures=remove_signatures,
                ),
                returncode=1,
                stdout="",
                stderr=error_msg,
            )
        # Check return value (missing return means success)
        return_value = rule.get("return", {})

        # Default to success if not specified
        success = return_value.get("success", True)

        if success:
            # Success - return None
            return None
        else:
            # Failure - raise SkopeoClientError
            returncode = return_value.get("returncode", 1)
            stdout = return_value.get("stdout", "")
            stderr = return_value.get("stderr", "")
            message = return_value.get("message", "Skopeo command failed")

            raise SkopeoClientError(
                message,
                command=self._build_command("copy", source=source, destination=destination),
                returncode=returncode,
                stdout=stdout,
                stderr=stderr,
            )
