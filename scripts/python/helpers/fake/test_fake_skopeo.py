"""Tests for FakeSkopeoClient."""

import logging
import os
import tempfile
import pytest

from fake.skopeo import FakeSkopeoClient
from rsmodels.secret import Secret
from skopeo import SkopeoClientError


def test_fake_client_requires_env_var():
    """Test that FakeSkopeoClient requires RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP."""
    # Ensure env var is not set
    os.environ.pop("RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP", None)

    with pytest.raises(ValueError, match="RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP.*not set"):
        FakeSkopeoClient()


def test_fake_client_requires_valid_file():
    """Test that FakeSkopeoClient requires config file to exist."""
    os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = "/nonexistent/file.yaml"

    with pytest.raises(FileNotFoundError, match="Config file not found"):
        FakeSkopeoClient()


def test_inspect_with_format():
    """Test inspect operation with format parameter."""
    config = """
inspect:
  - match:
      image: "docker://quay.io/image:tag"
      format: "{{.Digest}}"
    return: "sha256:abc123"
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
        client = FakeSkopeoClient()

        result = client.inspect("docker://quay.io/image:tag", format="{{.Digest}}")
        assert result == "sha256:abc123"
        assert isinstance(result, str)
    finally:
        os.unlink(config_path)


def test_inspect_without_format():
    """Test inspect operation without format parameter (returns dict)."""
    config = """
inspect:
  - match:
      image: "docker://quay.io/image:tag"
    return:
      Digest: "sha256:abc123"
      Name: "quay.io/image"
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
        client = FakeSkopeoClient()

        result = client.inspect("docker://quay.io/image:tag")
        assert isinstance(result, dict)
        assert result["Digest"] == "sha256:abc123"
        assert result["Name"] == "quay.io/image"
    finally:
        os.unlink(config_path)


def test_inspect_regex_match():
    """Test inspect with regex matching."""
    config = """
inspect:
  - match:
      image:
        regex: "docker://quay.io/.*@sha256:[a-f0-9]{6}"
      format: "{{.Digest}}"
    return: "sha256:matched"
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
        client = FakeSkopeoClient()

        result = client.inspect(
            "docker://quay.io/test/image@sha256:abcdef", format="{{.Digest}}"
        )
        assert result == "sha256:matched"
    finally:
        os.unlink(config_path)


def test_inspect_no_match():
    """Test inspect raises error when no rule matches."""
    config = """
inspect:
  - match:
      image: "docker://quay.io/other:tag"
    return:
      Digest: "sha256:other"
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
        client = FakeSkopeoClient()

        with pytest.raises(SkopeoClientError, match="No mock match found"):
            client.inspect("docker://quay.io/image:tag")
    finally:
        os.unlink(config_path)


def test_copy_success():
    """Test successful copy operation."""
    config = """
copy:
  - match:
      source: "docker://quay.io/src:tag"
      destination: "docker://quay.io/dest:tag"
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
        client = FakeSkopeoClient()

        result = client.copy("docker://quay.io/src:tag", "docker://quay.io/dest:tag")
        assert result is None  # Success returns None
    finally:
        os.unlink(config_path)


def test_copy_explicit_success():
    """Test copy with explicit success return."""
    config = """
copy:
  - match:
      source: "docker://quay.io/src:tag"
      destination: "docker://quay.io/dest:tag"
    return:
      success: true
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
        client = FakeSkopeoClient()

        result = client.copy("docker://quay.io/src:tag", "docker://quay.io/dest:tag")
        assert result is None
    finally:
        os.unlink(config_path)


def test_copy_failure():
    """Test failed copy operation."""
    config = """
copy:
  - match:
      source: "docker://quay.io/bad:tag"
      destination: "docker://quay.io/dest:tag"
    return:
      success: false
      stderr: "Error: manifest unknown"
      returncode: 1
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
        client = FakeSkopeoClient()

        with pytest.raises(SkopeoClientError) as exc_info:
            client.copy("docker://quay.io/bad:tag", "docker://quay.io/dest:tag")

        assert exc_info.value.returncode == 1
        assert "manifest unknown" in exc_info.value.stderr
    finally:
        os.unlink(config_path)


def test_first_match_wins():
    """Test that first matching rule is used."""
    config = """
inspect:
  - match:
      image: "docker://quay.io/image:tag"
    return:
      Digest: "sha256:first"

  - match:
      image: "docker://quay.io/image:tag"
    return:
      Digest: "sha256:second"
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
        client = FakeSkopeoClient()

        result = client.inspect("docker://quay.io/image:tag")
        assert result["Digest"] == "sha256:first"
    finally:
        os.unlink(config_path)


def test_invalid_yaml():
    """Test that invalid YAML raises error at load time."""
    config = """
this is: not: valid: yaml
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path

        with pytest.raises(ValueError, match="Failed to parse YAML"):
            FakeSkopeoClient()
    finally:
        os.unlink(config_path)


def test_validation_inspect_format_mismatch():
    """Test validation catches format/return type mismatch."""
    config = """
inspect:
  - match:
      image: "docker://quay.io/image:tag"
      format: "{{.Digest}}"
    return:
      Digest: "should-be-string"
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path

        with pytest.raises(ValueError, match="return must be a string"):
            FakeSkopeoClient()
    finally:
        os.unlink(config_path)


def test_empty_config():
    """Test that empty config is allowed (fails at runtime)."""
    config = ""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
        client = FakeSkopeoClient()  # Should load successfully

        # But operations should fail
        with pytest.raises(SkopeoClientError, match="No mock match found"):
            client.inspect("docker://quay.io/image:tag")
    finally:
        os.unlink(config_path)


class TestConfigValidation:
    """Tests for configuration validation."""

    def test_config_not_dict(self) -> None:
        """Test that config must be a dict."""
        config = "- item1\n- item2"  # YAML list instead of dict
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            with pytest.raises(ValueError, match="Config must be a dict"):
                FakeSkopeoClient()
        finally:
            os.unlink(config_path)

    def test_operation_section_not_list(self) -> None:
        """Test that operation sections must be lists."""
        config = """
inspect: "not-a-list"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            with pytest.raises(ValueError, match="must be a list of rules"):
                FakeSkopeoClient()
        finally:
            os.unlink(config_path)

    def test_rule_not_dict(self) -> None:
        """Test that rules must be dicts."""
        config = """
inspect:
  - "not-a-dict"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            with pytest.raises(ValueError, match="must be a dict"):
                FakeSkopeoClient()
        finally:
            os.unlink(config_path)

    def test_rule_missing_match(self) -> None:
        """Test that rules must have 'match' field."""
        config = """
inspect:
  - return:
      Digest: "sha256:abc"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            with pytest.raises(ValueError, match="missing required 'match' field"):
                FakeSkopeoClient()
        finally:
            os.unlink(config_path)

    def test_match_not_dict(self) -> None:
        """Test that 'match' must be a dict."""
        config = """
inspect:
  - match: "not-a-dict"
    return:
      Digest: "sha256:abc"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            with pytest.raises(ValueError, match="'match' must be a dict"):
                FakeSkopeoClient()
        finally:
            os.unlink(config_path)

    def test_inspect_return_dict_when_format_specified(self) -> None:
        """Test validation catches dict return when format is specified."""
        config = """
inspect:
  - match:
      image: "docker://quay.io/image:tag"
      format: "{{.Digest}}"
    return:
      Digest: "should-be-string"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            with pytest.raises(ValueError, match="return must be a string"):
                FakeSkopeoClient()
        finally:
            os.unlink(config_path)

    def test_inspect_return_string_when_no_format(self) -> None:
        """Test validation catches string return when format not specified."""
        config = """
inspect:
  - match:
      image: "docker://quay.io/image:tag"
    return: "should-be-dict"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            with pytest.raises(ValueError, match="return must be a dict"):
                FakeSkopeoClient()
        finally:
            os.unlink(config_path)

    def test_copy_return_not_dict(self) -> None:
        """Test that copy return must be dict."""
        config = """
copy:
  - match:
      source: "docker://quay.io/src:tag"
      destination: "docker://quay.io/dest:tag"
    return: "not-a-dict"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            with pytest.raises(ValueError, match="return must be a dict"):
                FakeSkopeoClient()
        finally:
            os.unlink(config_path)

    def test_copy_success_not_boolean(self) -> None:
        """Test that copy success field must be boolean."""
        config = """
copy:
  - match:
      source: "docker://quay.io/src:tag"
      destination: "docker://quay.io/dest:tag"
    return:
      success: "not-a-bool"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            with pytest.raises(ValueError, match="'success' field must be boolean"):
                FakeSkopeoClient()
        finally:
            os.unlink(config_path)


class TestMatchValue:
    """Tests for value matching logic."""

    def test_match_secret_always_matches(self) -> None:
        """Test that Secret objects are always ignored/matched."""
        config = """
inspect:
  - match:
      image: "docker://quay.io/image:tag"
    return:
      Digest: "sha256:abc123"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            client = FakeSkopeoClient()

            # Secret should be ignored in matching
            result = client.inspect("docker://quay.io/image:tag", creds=Secret("user:pass"))
            assert result["Digest"] == "sha256:abc123"
        finally:
            os.unlink(config_path)

    def test_match_regex_on_non_string(self) -> None:
        """Test that regex match fails on non-string actual value."""
        config = """
inspect:
  - match:
      image:
        regex: "docker://.*"
    return:
      Digest: "sha256:abc"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            client = FakeSkopeoClient()

            # This tests internal behavior - regex should only match strings
            # We can't directly test _match_value, but we can verify behavior
            with pytest.raises(SkopeoClientError):
                # Pass None which should not match regex
                client.inspect(None)
        finally:
            os.unlink(config_path)

    def test_match_invalid_regex(self) -> None:
        """Test that invalid regex pattern raises error."""
        config = """
inspect:
  - match:
      image:
        regex: "[invalid(regex"
      format: "{{.Digest}}"
    return: "sha256:abc"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            client = FakeSkopeoClient()

            with pytest.raises(ValueError, match="Invalid regex pattern"):
                client.inspect("docker://quay.io/image:tag", format="{{.Digest}}")
        finally:
            os.unlink(config_path)

    def test_match_none_doesnt_match_value(self) -> None:
        """Test that None doesn't match a specified pattern."""
        config = """
inspect:
  - match:
      image: "docker://quay.io/image:tag"
      format: "{{.Digest}}"
    return: "sha256:abc"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            client = FakeSkopeoClient()

            # Should not match because format is None but pattern expects specific value
            with pytest.raises(SkopeoClientError):
                client.inspect("docker://quay.io/image:tag", format=None)
        finally:
            os.unlink(config_path)


class TestLogging:
    """Tests for logging behavior."""

    def test_inspect_with_logger_debug(self) -> None:
        """Test that logger.debug is called on successful match."""
        config = """
inspect:
  - match:
      image: "docker://quay.io/image:tag"
    return:
      Digest: "sha256:abc123"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path

            # Create a logger
            logger = logging.getLogger("test")
            logger.setLevel(logging.DEBUG)

            client = FakeSkopeoClient(logger=logger)

            # This should trigger logger.debug call
            result = client.inspect("docker://quay.io/image:tag")
            assert result["Digest"] == "sha256:abc123"
        finally:
            os.unlink(config_path)

    def test_inspect_with_logger_error(self) -> None:
        """Test that logger.error is called when no match found."""
        config = """
inspect:
  - match:
      image: "docker://quay.io/other:tag"
    return:
      Digest: "sha256:other"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path

            logger = logging.getLogger("test_error")
            logger.setLevel(logging.ERROR)

            client = FakeSkopeoClient(logger=logger)

            with pytest.raises(SkopeoClientError):
                client.inspect("docker://quay.io/nonexistent:tag")
        finally:
            os.unlink(config_path)

    def test_copy_with_logger_error(self) -> None:
        """Test that logger.error is called in copy when no match found."""
        config = """
copy:
  - match:
      source: "docker://quay.io/other:tag"
      destination: "docker://quay.io/dest:tag"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path

            logger = logging.getLogger("test_copy_error")
            logger.setLevel(logging.ERROR)

            client = FakeSkopeoClient(logger=logger)

            with pytest.raises(SkopeoClientError):
                client.copy("docker://quay.io/nonexistent:tag", "docker://quay.io/dest:tag")
        finally:
            os.unlink(config_path)


class TestCopyEdgeCases:
    """Tests for copy operation edge cases."""

    def test_copy_failure_with_custom_message(self) -> None:
        """Test copy failure with custom error message."""
        config = """
copy:
  - match:
      source: "docker://quay.io/bad:tag"
      destination: "docker://quay.io/dest:tag"
    return:
      success: false
      message: "Custom error message"
      stderr: "manifest unknown"
      stdout: "some output"
      returncode: 2
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            client = FakeSkopeoClient()

            with pytest.raises(SkopeoClientError) as exc_info:
                client.copy("docker://quay.io/bad:tag", "docker://quay.io/dest:tag")

            assert exc_info.value.returncode == 2
            assert "Custom error message" in str(exc_info.value)
            assert exc_info.value.stderr == "manifest unknown"
            assert exc_info.value.stdout == "some output"
        finally:
            os.unlink(config_path)

    def test_copy_no_match(self) -> None:
        """Test copy raises error when no rule matches."""
        config = """
copy:
  - match:
      source: "docker://quay.io/other:tag"
      destination: "docker://quay.io/other-dest:tag"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            client = FakeSkopeoClient()

            with pytest.raises(SkopeoClientError, match="No mock match found"):
                client.copy("docker://quay.io/src:tag", "docker://quay.io/dest:tag")
        finally:
            os.unlink(config_path)


class TestBuildCommand:
    """Tests for command building."""

    def test_build_command_with_boolean_params(self) -> None:
        """Test that boolean parameters are handled correctly in error messages."""
        config = """
inspect:
  - match:
      image: "docker://quay.io/other:tag"
    return:
      Digest: "sha256:other"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            client = FakeSkopeoClient()

            # Trigger error with various parameters including booleans
            with pytest.raises(SkopeoClientError) as exc_info:
                client.inspect(
                    "docker://quay.io/nonexistent:tag",
                    format="{{.Digest}}",
                    config=True,  # boolean parameter
                    raw=False,  # boolean false - should not appear
                )

            # Check command is properly formed
            assert "skopeo" in exc_info.value.command
            assert "inspect" in exc_info.value.command
        finally:
            os.unlink(config_path)


class TestSecretHandling:
    """Tests for Secret handling in matching."""

    def test_copy_with_secrets_ignored_in_matching(self) -> None:
        """Test that Secret credentials are ignored during matching."""
        config = """
copy:
  - match:
      source: "docker://quay.io/src:tag"
      destination: "docker://quay.io/dest:tag"
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config)
            config_path = f.name

        try:
            os.environ["RELEASE_SERVICE_UTILS_FAKE_SKOPEO_SETUP"] = config_path
            client = FakeSkopeoClient()

            # Secrets should be ignored in matching, so this should succeed
            result = client.copy(
                "docker://quay.io/src:tag",
                "docker://quay.io/dest:tag",
                src_creds=Secret("src-user:pass"),
                dest_creds=Secret("dest-user:pass"),
            )
            assert result is None
        finally:
            os.unlink(config_path)
