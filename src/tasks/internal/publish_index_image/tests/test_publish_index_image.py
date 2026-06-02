"""Unit tests for publish_index_image."""

from __future__ import annotations

import json
import logging
import subprocess
from unittest import mock

import pytest

from release_service_utils.tasks.internal.publish_index_image.publish_index_image import (
    copy_image,
    extract_source_digest,
    inspect_image,
    load_credential,
    main,
    needs_source_auth,
    parse_arguments,
    write_result,
)

TASK = "release_service_utils.tasks.internal.publish_index_image.publish_index_image"
SOURCE_CONFIG_412 = {"config": {"Labels": {"com.redhat.component.ocp-version": "4.12"}}}


def _completed(
    stdout: str = "",
    stderr: str = "",
    returncode: int = 0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=stderr
    )


def _inspect_ok(data: dict) -> subprocess.CompletedProcess[str]:
    return _completed(stdout=json.dumps(data))


def _inspect_missing() -> subprocess.CompletedProcess[str]:
    return _completed(returncode=1)


class TestLoadCredential:
    """Tests for load_credential function."""

    def test_load_credential_from_file(self, tmp_path) -> None:
        """Test loading credential from a file."""
        cred_file = tmp_path / "credential.txt"
        cred_file.write_text("user:password\n")

        result = load_credential(str(cred_file), logger=mock.Mock())

        assert result == "user:password"

    def test_load_credential_strips_whitespace(self, tmp_path) -> None:
        """Test that load_credential strips whitespace."""
        cred_file = tmp_path / "credential.txt"
        cred_file.write_text("  user:password  \n  ")

        result = load_credential(str(cred_file), logger=mock.Mock())

        assert result == "user:password"

    def test_load_credential_file_not_found(self) -> None:
        """Test error when credential file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            load_credential("/nonexistent/file.txt", logger=mock.Mock())


class TestExtractSourceDigest:
    """Tests for extract_source_digest function."""

    def test_extract_valid_digest(self) -> None:
        """Test extracting digest from valid source."""
        source = "quay.io/repo/image@sha256:abc123"
        result = extract_source_digest(source, logger=mock.Mock())
        assert result == "sha256:abc123"

    def test_extract_digest_with_registry(self) -> None:
        """Test extracting digest with full registry path."""
        source = "registry.io/namespace/repo@sha256:def456"
        result = extract_source_digest(source, logger=mock.Mock())
        assert result == "sha256:def456"

    def test_extract_digest_but_tag(self) -> None:
        """Test that if source has a tag instead of digest, return the whole string."""
        source = "quay.io/repo/image:tag"
        assert extract_source_digest(source, logger=mock.Mock()) == source


class TestNeedsSourceAuth:
    """Tests for needs_source_auth function."""

    def test_proxy_registry_no_auth(self) -> None:
        """Test that proxy registries don't need auth."""
        assert needs_source_auth("registry-proxy.engineering.redhat.com/image") is False

    def test_proxy_stage_registry_no_auth(self) -> None:
        """Test that proxy stage registries don't need auth."""
        assert needs_source_auth("registry-proxy-stage.engineering.redhat.com/image") is False

    def test_other_registry_needs_auth(self) -> None:
        """Test that other registries need auth."""
        assert needs_source_auth("quay.io/repo/image") is True
        assert needs_source_auth("registry.io/image") is True


class TestInspectImage:
    """Tests for inspect_image function."""

    @mock.patch(f"{TASK}.inspect")
    def test_inspect_existing_image(self, mock_inspect) -> None:
        """Test inspecting an image that exists."""
        inspect_data = {"Digest": "sha256:abc123", "Labels": {}}
        mock_inspect.return_value = _inspect_ok(inspect_data)

        result = inspect_image("quay.io/image:tag", "user:pass", logger=mock.Mock())

        assert result == inspect_data
        mock_inspect.assert_called_once_with(
            "quay.io/image:tag",
            config=False,
            creds="user:pass",
        )

    @mock.patch(f"{TASK}.inspect")
    def test_inspect_existing_image_with_config(self, mock_inspect) -> None:
        """Test inspecting an image config."""
        config_data = {"config": {"Labels": {"key": "value"}}}
        mock_inspect.return_value = _inspect_ok(config_data)

        result = inspect_image(
            "quay.io/image:tag",
            "user:pass",
            logger=mock.Mock(),
            config=True,
        )

        assert result == config_data
        mock_inspect.assert_called_once_with(
            "quay.io/image:tag",
            config=True,
            creds="user:pass",
        )

    @mock.patch(f"{TASK}.inspect")
    def test_inspect_nonexistent_image(self, mock_inspect) -> None:
        """Test inspecting an image that doesn't exist."""
        mock_inspect.return_value = _inspect_missing()

        result = inspect_image("quay.io/image:tag", "user:pass", logger=mock.Mock())

        assert result is None

    @mock.patch(f"{TASK}.inspect")
    def test_inspect_without_credentials(self, mock_inspect) -> None:
        """Test inspecting without credentials."""
        inspect_data = {"Digest": "sha256:abc123"}
        mock_inspect.return_value = _inspect_ok(inspect_data)

        result = inspect_image("quay.io/image:tag", None, logger=mock.Mock())

        assert result == inspect_data
        mock_inspect.assert_called_once_with("quay.io/image:tag", config=False)

    @mock.patch(f"{TASK}.inspect")
    def test_inspect_with_retry_times(self, mock_inspect) -> None:
        """Test inspecting with custom retry count."""
        mock_inspect.return_value = _inspect_ok({})

        inspect_image(
            "quay.io/image:tag",
            "user:pass",
            logger=mock.Mock(),
            retry_times=5,
        )

        mock_inspect.assert_called_once_with(
            "quay.io/image:tag",
            config=False,
            creds="user:pass",
            retry_times=5,
        )


class TestCopyImage:
    """Tests for copy_image function."""

    @mock.patch(f"{TASK}.copy")
    def test_copy_successful(self, mock_copy) -> None:
        """Test successful image copy."""
        mock_copy.return_value = _completed()

        success, message = copy_image(
            "quay.io/src/image@sha256:abc",
            "quay.io/dest/image:tag",
            "src-user:pass",
            "dest-user:pass",
            logger=mock.Mock(),
        )

        assert success is True
        assert message == "Index Image Published successfully"
        mock_copy.assert_called_once_with(
            "docker://quay.io/src/image@sha256:abc",
            "docker://quay.io/dest/image:tag",
            all=True,
            preserve_digests=True,
            src_tls_verify=False,
            src_creds="src-user:pass",
            dest_creds="dest-user:pass",
        )

    @mock.patch(f"{TASK}.copy")
    def test_copy_without_source_creds(self, mock_copy) -> None:
        """Test copy without source credentials (proxy registry)."""
        mock_copy.return_value = _completed()

        success, _ = copy_image(
            "registry-proxy.engineering.redhat.com/image@sha256:abc",
            "quay.io/dest/image:tag",
            None,
            "dest-user:pass",
            logger=mock.Mock(),
        )

        assert success is True
        call_kwargs = mock_copy.call_args[1]
        assert "src_creds" not in call_kwargs

    @mock.patch(f"{TASK}.copy")
    def test_copy_failure(self, mock_copy) -> None:
        """Test image copy failure."""
        mock_copy.return_value = _completed(returncode=1, stderr="connection timeout")

        success, message = copy_image(
            "quay.io/src/image@sha256:abc",
            "quay.io/dest/image:tag",
            "src-user:pass",
            "dest-user:pass",
            logger=mock.Mock(),
        )

        assert success is False
        assert message == "Error: Failed publishing Index Image"

    @mock.patch(f"{TASK}.copy")
    def test_copy_with_retry_times(self, mock_copy) -> None:
        """Test copy with custom retry count."""
        mock_copy.return_value = _completed()

        copy_image(
            "quay.io/src@sha256:abc",
            "quay.io/dest:tag",
            "user:pass",
            "user:pass",
            logger=mock.Mock(),
            retry_times=5,
        )

        call_kwargs = mock_copy.call_args[1]
        assert call_kwargs["retry_times"] == 5


class TestWriteResult:
    """Tests for write_result function."""

    def test_write_result_prints_message(self, capsys) -> None:
        """Test that write_result prints to stdout."""
        write_result("Test message")
        captured = capsys.readouterr()
        assert captured.out == "Test message\n"


class TestParseArguments:
    """Tests for parse_arguments function."""

    def test_parse_minimal_required_args(self) -> None:
        """Test parsing with only required arguments."""
        test_args = [
            "--source-index",
            "quay.io/src/image@sha256:abc123",
            "--target-index",
            "quay.io/dest/image:v1.0",
        ]
        with mock.patch("sys.argv", ["publish_index_image.py"] + test_args):
            args = parse_arguments()
            assert args.source_index == "quay.io/src/image@sha256:abc123"
            assert args.target_index == "quay.io/dest/image:v1.0"
            assert args.retries == 3
            assert (
                args.source_credential_path
                == "/mnt/publishingCredentials/sourceIndexCredential"
            )
            assert (
                args.target_credential_path
                == "/mnt/publishingCredentials/targetIndexCredential"
            )
            assert args.verbose is False

    def test_parse_all_args(self) -> None:
        """Test parsing with all arguments specified."""
        test_args = [
            "--source-index",
            "quay.io/src/image@sha256:abc123",
            "--target-index",
            "quay.io/dest/image:v1.0",
            "--retries",
            "5",
            "--source-credential-path",
            "/custom/src/cred",
            "--target-credential-path",
            "/custom/target/cred",
            "--verbose",
        ]
        with mock.patch("sys.argv", ["publish_index_image.py"] + test_args):
            args = parse_arguments()
            assert args.source_index == "quay.io/src/image@sha256:abc123"
            assert args.target_index == "quay.io/dest/image:v1.0"
            assert args.retries == 5
            assert args.source_credential_path == "/custom/src/cred"
            assert args.target_credential_path == "/custom/target/cred"
            assert args.verbose is True

    def test_parse_verbose_short_flag(self) -> None:
        """Test parsing with -v short flag for verbose."""
        test_args = [
            "--source-index",
            "quay.io/src@sha256:abc",
            "--target-index",
            "quay.io/dest:tag",
            "-v",
        ]
        with mock.patch("sys.argv", ["publish_index_image.py"] + test_args):
            args = parse_arguments()
            assert args.verbose is True

    def test_parse_missing_required_args(self) -> None:
        """Test that missing required args raises SystemExit."""
        with mock.patch("sys.argv", ["publish_index_image.py"]):
            with pytest.raises(SystemExit):
                parse_arguments()


class TestMain:
    """Tests for main function integration."""

    @staticmethod
    def _mock_args(**overrides) -> mock.Mock:
        defaults = {
            "source_index": "quay.io/src/image@sha256:abc123",
            "target_index": "quay.io/dest/image:v1.0",
            "target_ocp_version": "4.12",
            "verbose": False,
            "retries": 3,
            "source_credential_path": "/src/cred",
            "target_credential_path": "/dest/cred",
        }
        defaults.update(overrides)
        return mock.Mock(**defaults)

    @pytest.fixture(autouse=True)
    def _patch_main_deps(self) -> None:
        TASK = "release_service_utils.tasks.internal.publish_index_image.publish_index_image"
        with (
            mock.patch(f"{TASK}.parse_arguments") as self.parse_args,
            mock.patch(
                f"{TASK}.setup_logger",
                return_value=mock.Mock(),
            ) as self.setup_logger,
            mock.patch(f"{TASK}.load_credential") as self.load_cred,
            mock.patch(f"{TASK}.inspect") as self.inspect,
            mock.patch(f"{TASK}.copy") as self.copy,
        ):
            self.parse_args.return_value = self._mock_args()
            self.load_cred.side_effect = ["src:pass", "dest:pass"]
            self.copy.return_value = _completed()
            yield

    def test_main_successful_copy(self, capsys) -> None:
        """Test main workflow with successful image copy."""
        target_info = {"Digest": "sha256:different", "Labels": {}}
        self.inspect.side_effect = [
            _inspect_ok(SOURCE_CONFIG_412),
            _inspect_ok(target_info),
        ]

        result = main()

        assert result == 0
        assert "Index Image Published successfully" in capsys.readouterr().out
        self.copy.assert_called_once()

    def test_main_digest_match_skip_copy(self, capsys) -> None:
        """Test main workflow when target exists with same digest."""
        target_info = {"Digest": "sha256:abc123", "Labels": {}}
        self.inspect.side_effect = [
            _inspect_ok(SOURCE_CONFIG_412),
            _inspect_ok(target_info),
        ]

        result = main()

        assert result == 0
        assert "already exists with the same digest" in capsys.readouterr().out
        self.copy.assert_not_called()

    def test_main_copy_failure(self, capsys) -> None:
        """Test main workflow when copy fails."""
        target_info = {"Digest": "sha256:different", "Labels": {}}
        self.inspect.side_effect = [
            _inspect_ok(SOURCE_CONFIG_412),
            _inspect_ok(target_info),
        ]
        self.copy.return_value = _completed(returncode=1, stderr="connection timeout")

        result = main()

        assert result == 0
        assert "Error: Failed publishing Index Image" in capsys.readouterr().out

    def test_main_credential_load_failure(self, capsys) -> None:
        """Test main workflow when credential loading fails."""
        self.load_cred.side_effect = FileNotFoundError("Credential file not found")

        result = main()

        assert result == 0
        assert "Credential file not found" in capsys.readouterr().out

    def test_main_verbose_logging(self) -> None:
        """Test that verbose flag enables debug logging."""
        self.parse_args.return_value = self._mock_args(verbose=True)
        self.inspect.side_effect = [
            _inspect_ok(SOURCE_CONFIG_412),
            _inspect_missing(),
        ]

        main()

        self.setup_logger.assert_called_once_with(
            level=logging.DEBUG, name="publish_index_image"
        )

    def test_main_proxy_registry_no_auth(self) -> None:
        """Test main workflow with proxy registry (no source auth needed)."""
        self.parse_args.return_value = self._mock_args(
            source_index=("registry-proxy.engineering.redhat.com/image@sha256:abc123"),
        )
        self.inspect.side_effect = [
            _inspect_ok(SOURCE_CONFIG_412),
            _inspect_missing(),
        ]

        assert main() == 0
        assert "src_creds" not in self.copy.call_args[1]

    def test_main_no_source_digest(self) -> None:
        """Test main workflow when source index has no digest."""
        self.parse_args.return_value = self._mock_args(
            source_index="quay.io/src/image:latest",
        )
        self.inspect.side_effect = [
            _inspect_ok(SOURCE_CONFIG_412),
            _inspect_missing(),
        ]

        assert main() == 0

    def test_main_ocp_version_mismatch(self, capsys) -> None:
        """Test main returns 0 with error in result when OCP version mismatches."""
        self.parse_args.return_value = self._mock_args(
            target_ocp_version="4.14",
        )
        self.inspect.return_value = _inspect_ok(SOURCE_CONFIG_412)

        result = main()

        assert result == 0
        assert "does not match" in capsys.readouterr().out

    def test_main_target_not_found_copies(self, capsys) -> None:
        """Test main copies when target image does not exist."""
        self.inspect.side_effect = [
            _inspect_ok(SOURCE_CONFIG_412),
            _inspect_missing(),
        ]

        result = main()

        assert result == 0
        assert "Index Image Published successfully" in capsys.readouterr().out
        self.copy.assert_called_once()
