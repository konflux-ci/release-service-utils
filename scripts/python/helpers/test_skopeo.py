"""Unit tests for the SkopeoClient."""

import subprocess
from unittest import mock

import pytest

from rsmodels.secret import Secret
from skopeo import SkopeoClient, SkopeoClientError


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

        assert isinstance(result, dict)
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
        client.inspect(
            "docker://quay.io/image:tag",
            config=True,
            raw=True,
            no_tags=True,
        )

        called_cmd = mock_run.call_args[0][0]
        assert "--config" in called_cmd
        assert "--raw" in called_cmd
        assert "--no-tags" in called_cmd

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
