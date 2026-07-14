"""Unit tests for the SkopeoClient."""

from __future__ import annotations

import subprocess
from unittest import mock

from rsmodels.secret import Secret
from rsmodels import ContainerImage, ContainerImageRaw

import skopeo
from skopeo import SkopeoClient, SkopeoClientError
import pytest


def _completed(
    stdout: str = "",
    returncode: int = 0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=""
    )


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------


def test_inspect_basic_command() -> None:
    """Default inspect builds the correct command."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.inspect("registry.example.com/repo:tag")

    cmd = run_mock.call_args[0][0]
    assert cmd == [
        "skopeo",
        "inspect",
        "--retry-times",
        "3",
        "docker://registry.example.com/repo:tag",
    ]
    assert run_mock.call_args[1]["capture_output"] is True
    assert run_mock.call_args[1]["text"] is True
    assert run_mock.call_args[1]["check"] is False


def test_inspect_config_flag() -> None:
    """``config=True`` adds ``--config`` to the command."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.inspect("img:v1", config=True)

    cmd = run_mock.call_args[0][0]
    assert "--config" in cmd
    assert "--raw" not in cmd


def test_inspect_raw_flag() -> None:
    """``raw=True`` adds ``--raw`` to the command."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.inspect("img:v1", raw=True)

    cmd = run_mock.call_args[0][0]
    assert "--raw" in cmd
    assert "--config" not in cmd


def test_inspect_custom_retry_times() -> None:
    """``retry_times`` overrides the default retry count."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.inspect("img:v1", retry_times=5)

    cmd = run_mock.call_args[0][0]
    assert "--retry-times" in cmd
    idx = cmd.index("--retry-times")
    assert cmd[idx + 1] == "5"


def test_inspect_returns_completed_process() -> None:
    """The raw ``CompletedProcess`` is returned to the caller."""
    expected = _completed(stdout='{"created": "2024-01-01T00:00:00Z"}')
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=expected,
    ):
        result = skopeo.inspect("img:v1", config=True)

    assert result is expected
    assert result.stdout == '{"created": "2024-01-01T00:00:00Z"}'


def test_inspect_nonzero_exit_code() -> None:
    """A non-zero exit code is returned, not raised."""
    expected = _completed(returncode=1)
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=expected,
    ):
        result = skopeo.inspect("img:v1")

    assert result.returncode == 1


def test_inspect_config_and_raw_together() -> None:
    """Both flags can be set simultaneously."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.inspect("img:v1", config=True, raw=True)

    cmd = run_mock.call_args[0][0]
    assert "--config" in cmd
    assert "--raw" in cmd


def test_inspect_image_ref_with_digest() -> None:
    """Digest references are passed through correctly."""
    digest = "sha256:abc123"
    ref = f"registry.example.com/repo@{digest}"
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.inspect(ref)

    cmd = run_mock.call_args[0][0]
    assert cmd[-1] == f"docker://{ref}"


# ---------------------------------------------------------------------------
# copy
# ---------------------------------------------------------------------------


def test_copy_basic_command() -> None:
    """Default copy builds the correct command."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.copy("docker://registry.example.com/repo:tag", "dir:/tmp/out")

    cmd = run_mock.call_args[0][0]
    assert cmd == [
        "skopeo",
        "copy",
        "--retry-times",
        "3",
        "docker://registry.example.com/repo:tag",
        "dir:/tmp/out",
    ]
    assert run_mock.call_args[1]["capture_output"] is True
    assert run_mock.call_args[1]["text"] is True
    assert run_mock.call_args[1]["check"] is False


def test_copy_custom_retry_times() -> None:
    """``retry_times`` overrides the default retry count."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.copy("docker://img:v1", "dir:/tmp/out", retry_times=5)

    cmd = run_mock.call_args[0][0]
    idx = cmd.index("--retry-times")
    assert cmd[idx + 1] == "5"


def test_copy_returns_completed_process() -> None:
    """The raw ``CompletedProcess`` is returned to the caller."""
    expected = _completed(stdout="copied successfully")
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=expected,
    ):
        result = skopeo.copy("docker://img:v1", "dir:/tmp/out")

    assert result is expected


def test_copy_nonzero_exit_code() -> None:
    """A non-zero exit code is returned, not raised."""
    expected = _completed(returncode=1)
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=expected,
    ):
        result = skopeo.copy("docker://img:v1", "dir:/tmp/out")

    assert result.returncode == 1


class TestSkopeoClientInit:
    """Tests for SkopeoClient initialization."""

    def test_init_with_defaults(self):
        """Test creating client with default parameters."""
        client = SkopeoClient()
        assert client.debug is False
        assert client.insecure_policy is False
        assert client.tmpdir is None
        assert client.command_timeout is None

    def test_init_with_all_params(self):
        """Test creating client with all parameters."""
        client = SkopeoClient(
            debug=True,
            insecure_policy=True,
            tmpdir="/tmp/test",
            command_timeout="5m",
            override_arch="amd64",
            override_os="linux",
            override_variant="v8",
        )
        assert client.debug is True
        assert client.insecure_policy is True
        assert client.tmpdir == "/tmp/test"
        assert client.command_timeout == "5m"
        assert client.override_arch == "amd64"
        assert client.override_os == "linux"
        assert client.override_variant == "v8"


class TestSkopeoClientGlobalFlags:
    """Tests for global flag building."""

    def test_build_global_flags_empty(self):
        """Test building global flags with defaults."""
        client = SkopeoClient()
        flags = client._build_global_flags()
        assert flags == []

    def test_build_global_flags_all_set(self):
        """Test building global flags with all options."""
        client = SkopeoClient(
            debug=True,
            insecure_policy=True,
            tmpdir="/tmp/test",
            command_timeout="5m",
            override_arch="amd64",
            override_os="linux",
            override_variant="v8",
        )
        flags = client._build_global_flags()
        assert "--debug" in flags
        assert "--insecure-policy" in flags
        assert "--tmpdir" in flags
        assert "/tmp/test" in flags
        assert "--command-timeout" in flags
        assert "5m" in flags
        assert "--override-arch" in flags
        assert "amd64" in flags
        assert "--override-os" in flags
        assert "linux" in flags
        assert "--override-variant" in flags
        assert "v8" in flags


class TestSkopeoClientInspect:
    """Tests for the inspect method."""

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_returns_dict_by_default(self, mock_run):
        """Test that inspect returns parsed JSON dict when format is None."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"Digest": "sha256:abc123", "RepoTags": ["latest"]}',
            stderr="",
        )

        client = SkopeoClient()
        result = client.inspect("docker://quay.io/image:tag")

        assert isinstance(result, ContainerImage)
        assert result["Digest"] == "sha256:abc123"
        assert result["RepoTags"] == ["latest"]

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_returns_string_with_format(self, mock_run):
        """Test that inspect returns formatted string when format is specified."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="sha256:abc123",
            stderr="",
        )

        client = SkopeoClient()
        result = client.inspect(
            "docker://quay.io/image:tag",
            format="{{.Digest}}",
        )

        assert isinstance(result, str)
        assert result == "sha256:abc123"

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_with_credentials(self, mock_run):
        """Test inspect with credentials."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"Digest": "sha256:abc123"}',
            stderr="",
        )

        client = SkopeoClient()
        creds = Secret("user:password")
        client.inspect(
            "docker://registry.io/image:tag",
            creds=creds,
        )

        # Verify the command includes credentials
        called_cmd = mock_run.call_args[0][0]
        assert "--creds" in called_cmd
        # The actual password should be in the command (unveiled)
        creds_idx = called_cmd.index("--creds")
        assert called_cmd[creds_idx + 1] == "user:password"

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_with_retry_times(self, mock_run):
        """Test inspect with retry times."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"Digest": "sha256:abc123"}',
            stderr="",
        )

        client = SkopeoClient()
        client.inspect(
            "docker://quay.io/image:tag",
            retry_times=5,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--retry-times" in called_cmd
        assert "5" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_with_tls_verify_false(self, mock_run):
        """Test inspect with tls_verify=False."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"Digest": "sha256:abc123"}',
            stderr="",
        )

        client = SkopeoClient()
        client.inspect(
            "docker://quay.io/image:tag",
            tls_verify=False,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--tls-verify" in called_cmd
        assert "false" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_with_tls_verify_true(self, mock_run):
        """Test inspect with tls_verify=True."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"Digest": "sha256:abc123"}',
            stderr="",
        )

        client = SkopeoClient()
        client.inspect(
            "docker://quay.io/image:tag",
            tls_verify=True,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--tls-verify" in called_cmd
        assert "true" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_with_tls_verify_none(self, mock_run):
        """Test inspect with tls_verify=None (omit flag)."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"Digest": "sha256:abc123"}',
            stderr="",
        )

        client = SkopeoClient()
        client.inspect(
            "docker://quay.io/image:tag",
            tls_verify=None,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--tls-verify" not in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_with_boolean_flags(self, mock_run):
        """Test inspect with boolean flags."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"Digest": "sha256:abc123"}',
            stderr="",
        )

        client = SkopeoClient()
        result = client.inspect(
            "docker://quay.io/image:tag",
            config=True,
            raw=True,
            no_tags=True,
        )

        assert isinstance(result, ContainerImageRaw)
        called_cmd = mock_run.call_args[0][0]
        assert "--config" in called_cmd
        assert "--raw" in called_cmd
        assert "--no-tags" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_raw_returns_container_image_raw(self, mock_run):
        """Test that inspect with raw=True returns a ContainerImageRaw instance."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"schemaVersion": 2, "mediaType": "application/json"}',
            stderr="",
        )

        client = SkopeoClient()
        result = client.inspect(
            "docker://quay.io/image:tag",
            raw=True,
        )

        assert isinstance(result, ContainerImageRaw)
        assert result.schema_version == 2
        assert result.media_type == "application/json"

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_raises_on_command_failure(self, mock_run):
        """Test that inspect raises SkopeoClientError on failure."""
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=["skopeo", "inspect", "docker://image"],
            output="",
            stderr="Error: manifest unknown",
        )

        client = SkopeoClient()
        with pytest.raises(SkopeoClientError) as exc_info:
            client.inspect("docker://quay.io/nonexistent:tag")

        assert exc_info.value.returncode == 1
        assert "manifest unknown" in exc_info.value.stderr
        assert exc_info.value.command[0] == "skopeo"

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_raises_on_invalid_json(self, mock_run):
        """Test that inspect raises SkopeoClientError on invalid JSON."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="not valid json",
            stderr="",
        )

        client = SkopeoClient()
        with pytest.raises(SkopeoClientError) as exc_info:
            client.inspect("docker://quay.io/image:tag")

        assert "Failed to parse skopeo output as JSON" in str(exc_info.value)

    @mock.patch("skopeo.subprocess.run")
    def test_inspect_with_registry_token(self, mock_run):
        """Test inspect with registry bearer token."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"Digest": "sha256:abc123"}',
            stderr="",
        )

        client = SkopeoClient()
        token = Secret("bearer-token-12345")
        client.inspect(
            "docker://registry.io/image:tag",
            registry_token=token,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--registry-token" in called_cmd
        token_idx = called_cmd.index("--registry-token")
        assert called_cmd[token_idx + 1] == "bearer-token-12345"


class TestSkopeoClientCopy:
    """Tests for the copy method."""

    @mock.patch("skopeo.subprocess.run")
    def test_copy_minimal(self, mock_run):
        """Test copy with minimal parameters."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
        )

        called_cmd = mock_run.call_args[0][0]
        assert called_cmd[0] == "skopeo"
        assert "copy" in called_cmd
        assert "docker://quay.io/src:tag" in called_cmd
        assert "docker://quay.io/dest:tag" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_all_flag_true(self, mock_run):
        """Test copy with all=True."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            all=True,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--all" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_all_flag_false(self, mock_run):
        """Test copy with all=False (flag not added)."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            all=False,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--all" not in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_all_flag_none(self, mock_run):
        """Test copy with all=None (flag not added)."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            all=None,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--all" not in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_preserve_digests(self, mock_run):
        """Test copy with preserve_digests=True."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            preserve_digests=True,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--preserve-digests" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_credentials(self, mock_run):
        """Test copy with source and destination credentials."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        src_creds = Secret("src-user:src-pass")
        dest_creds = Secret("dest-user:dest-pass")

        client.copy(
            "docker://registry.io/src:tag",
            "docker://registry.io/dest:tag",
            src_creds=src_creds,
            dest_creds=dest_creds,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--src-creds" in called_cmd
        src_idx = called_cmd.index("--src-creds")
        assert called_cmd[src_idx + 1] == "src-user:src-pass"

        assert "--dest-creds" in called_cmd
        dest_idx = called_cmd.index("--dest-creds")
        assert called_cmd[dest_idx + 1] == "dest-user:dest-pass"

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_tls_verify(self, mock_run):
        """Test copy with TLS verification options."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            src_tls_verify=False,
            dest_tls_verify=True,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--src-tls-verify=false" in called_cmd
        assert "--dest-tls-verify=true" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_no_creds_flags(self, mock_run):
        """Test copy with no-creds flags."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            src_no_creds=True,
            dest_no_creds=True,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--src-no-creds" in called_cmd
        assert "--dest-no-creds" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_retry_times(self, mock_run):
        """Test copy with retry times."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            retry_times=10,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--retry-times" in called_cmd
        assert "10" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_format(self, mock_run):
        """Test copy with manifest format."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            format="oci",
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--format" in called_cmd
        assert "oci" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_quiet(self, mock_run):
        """Test copy with quiet flag."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            quiet=True,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--quiet" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_cert_dirs(self, mock_run):
        """Test copy with certificate directories."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            src_cert_dir="/etc/certs/src",
            dest_cert_dir="/etc/certs/dest",
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--src-cert-dir" in called_cmd
        assert "/etc/certs/src" in called_cmd
        assert "--dest-cert-dir" in called_cmd
        assert "/etc/certs/dest" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_authfiles(self, mock_run):
        """Test copy with authfile paths."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            src_authfile="/auth/src.json",
            dest_authfile="/auth/dest.json",
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--src-authfile" in called_cmd
        assert "/auth/src.json" in called_cmd
        assert "--dest-authfile" in called_cmd
        assert "/auth/dest.json" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_remove_signatures(self, mock_run):
        """Test copy with remove-signatures flag."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient()
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
            remove_signatures=True,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--remove-signatures" in called_cmd

    @mock.patch("skopeo.subprocess.run")
    def test_copy_raises_on_failure(self, mock_run):
        """Test that copy raises SkopeoClientError on failure."""
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=["skopeo", "copy", "docker://src", "docker://dest"],
            output="",
            stderr="Error: copying image failed",
        )

        client = SkopeoClient()
        with pytest.raises(SkopeoClientError) as exc_info:
            client.copy(
                "docker://quay.io/src:tag",
                "docker://quay.io/dest:tag",
            )

        assert exc_info.value.returncode == 1
        assert "copying image failed" in exc_info.value.stderr

    @mock.patch("skopeo.subprocess.run")
    def test_copy_with_global_flags(self, mock_run):
        """Test that copy includes global flags from client init."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="",
            stderr="",
        )

        client = SkopeoClient(debug=True, tmpdir="/tmp/skopeo")
        client.copy(
            "docker://quay.io/src:tag",
            "docker://quay.io/dest:tag",
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--debug" in called_cmd
        assert "--tmpdir" in called_cmd
        assert "/tmp/skopeo" in called_cmd


class TestSkopeoClientError:
    """Tests for SkopeoClientError exception."""

    def test_error_attributes(self):
        """Test that error stores all required attributes."""
        err = SkopeoClientError(
            message="Command failed",
            command=["skopeo", "inspect", "docker://image"],
            returncode=1,
            stdout="",
            stderr="Error: manifest unknown",
        )

        assert str(err).startswith("Command failed")
        assert err.command == ["skopeo", "inspect", "docker://image"]
        assert err.returncode == 1
        assert err.stdout == ""
        assert err.stderr == "Error: manifest unknown"

    def test_error_string_representation(self):
        """Test error string representation includes details."""
        err = SkopeoClientError(
            message="Command failed",
            command=["skopeo", "inspect", "docker://image"],
            returncode=1,
            stdout="",
            stderr="Error: manifest unknown",
        )

        error_str = str(err)
        assert "Command failed" in error_str
        assert "skopeo inspect docker://image" in error_str
        assert "Return code: 1" in error_str
        assert "Error: manifest unknown" in error_str
