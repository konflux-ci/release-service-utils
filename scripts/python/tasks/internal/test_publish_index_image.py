"""Unit tests for publish_index_image using SkopeoClient."""

import logging
from unittest import mock

import pytest

from publish_index_image import (
    extract_source_digest,
    load_credential,
    needs_source_auth,
    inspect_image,
    copy_image,
    write_result,
    parse_arguments,
    main,
)
from rsmodels.secret import Secret
from skopeo import SkopeoClientError


class TestLoadCredential:
    """Tests for load_credential function."""

    def test_load_credential_from_file(self, tmp_path):
        """Test loading credential from a file."""
        cred_file = tmp_path / "credential.txt"
        cred_file.write_text("user:password\n")

        result = load_credential(str(cred_file), logger=mock.Mock())

        assert isinstance(result, Secret)
        assert result.unveil() == "user:password"

    def test_load_credential_strips_whitespace(self, tmp_path):
        """Test that load_credential strips whitespace."""
        cred_file = tmp_path / "credential.txt"
        cred_file.write_text("  user:password  \n  ")

        result = load_credential(str(cred_file), logger=mock.Mock())

        assert result.unveil() == "user:password"

    def test_load_credential_with_name(self, tmp_path):
        """Test that Secret has the filename as name."""
        cred_file = tmp_path / "sourceIndexCredential"
        cred_file.write_text("user:pass")

        result = load_credential(str(cred_file), logger=mock.Mock())

        assert isinstance(result, Secret)
        # The name should be the filename
        assert "sourceIndexCredential" in repr(result)

    def test_load_credential_file_not_found(self):
        """Test error when credential file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            load_credential("/nonexistent/file.txt", logger=mock.Mock())


class TestExtractSourceDigest:
    """Tests for extract_source_digest function."""

    def test_extract_valid_digest(self):
        """Test extracting digest from valid source."""
        source = "quay.io/repo/image@sha256:abc123"
        result = extract_source_digest(source)
        assert result == "sha256:abc123"

    def test_extract_digest_with_registry(self):
        """Test extracting digest with full registry path."""
        source = "registry.io/namespace/repo@sha256:def456"
        result = extract_source_digest(source)
        assert result == "sha256:def456"

    def test_extract_digest_but_tag(self):
        """Test that if source has a tag instead of digest, it returns the whole string."""
        source = "quay.io/repo/image:tag"
        assert extract_source_digest(source) == "quay.io/repo/image:tag"


class TestNeedsSourceAuth:
    """Tests for needs_source_auth function."""

    def test_proxy_registry_no_auth(self):
        """Test that proxy registries don't need auth."""
        assert needs_source_auth("registry-proxy.engineering.redhat.com/image") is False

    def test_proxy_stage_registry_no_auth(self):
        """Test that proxy stage registries don't need auth."""
        assert needs_source_auth("registry-proxy-stage.engineering.redhat.com/image") is False

    def test_other_registry_needs_auth(self):
        """Test that other registries need auth."""
        assert needs_source_auth("quay.io/repo/image") is True
        assert needs_source_auth("registry.io/image") is True


class TestInspectTargetImage:
    """Tests for inspect_image function."""

    @mock.patch("publish_index_image.SkopeoClient")
    def test_inspect_existing_image(self, mock_client_class):
        """Test inspecting an image that exists."""
        mock_client = mock.Mock()
        mock_client.inspect.return_value = "sha256:abc123"

        target_cred = Secret("user:pass")
        result = inspect_image(
            mock_client, "quay.io/image:tag", target_cred, logger=mock.Mock()
        )

        assert result == "sha256:abc123"
        mock_client.inspect.assert_called_once_with(
            "docker://quay.io/image:tag",
            config=False,
            creds=target_cred,
            retry_times=None,
        )

    @mock.patch("publish_index_image.SkopeoClient")
    def test_inspect_nonexistent_image(self, mock_client_class):
        """Test inspecting an image that doesn't exist."""
        mock_client = mock.Mock()
        mock_client.inspect.side_effect = SkopeoClientError(
            "Image not found",
            command=["skopeo", "inspect"],
            returncode=1,
            stdout="",
            stderr="manifest unknown",
        )

        target_cred = Secret("user:pass")
        result = inspect_image(
            mock_client, "quay.io/image:tag", target_cred, logger=mock.Mock()
        )

        assert result is None


class TestCopyImage:
    """Tests for copy_image function."""

    @mock.patch("publish_index_image.SkopeoClient")
    def test_copy_successful(self, mock_client_class):
        """Test successful image copy."""
        mock_client = mock.Mock()
        mock_client.copy.return_value = None

        src_cred = Secret("src-user:pass")
        dest_cred = Secret("dest-user:pass")

        success, message = copy_image(
            mock_client,
            "quay.io/src/image@sha256:abc",
            "quay.io/dest/image:tag",
            src_cred,
            dest_cred,
            logger=mock.Mock(),
        )

        assert success is True
        assert message == "Index Image Published successfully"
        mock_client.copy.assert_called_once_with(
            "docker://quay.io/src/image@sha256:abc",
            "docker://quay.io/dest/image:tag",
            all=True,
            preserve_digests=True,
            src_tls_verify=False,
            src_creds=src_cred,
            dest_creds=dest_cred,
            retry_times=None,
        )

    @mock.patch("publish_index_image.SkopeoClient")
    def test_copy_without_source_creds(self, mock_client_class):
        """Test copy without source credentials (proxy registry)."""
        mock_client = mock.Mock()
        mock_client.copy.return_value = None

        dest_cred = Secret("dest-user:pass")

        success, message = copy_image(
            mock_client,
            "registry-proxy.engineering.redhat.com/image@sha256:abc",
            "quay.io/dest/image:tag",
            None,  # No source creds
            dest_cred,
            logger=mock.Mock(),
        )

        assert success is True
        mock_client.copy.assert_called_once()
        # Verify src_creds is None
        call_kwargs = mock_client.copy.call_args[1]
        assert call_kwargs["src_creds"] is None

    @mock.patch("publish_index_image.SkopeoClient")
    def test_copy_failure(self, mock_client_class):
        """Test image copy failure."""
        mock_client = mock.Mock()
        mock_client.copy.side_effect = SkopeoClientError(
            "Copy failed",
            command=["skopeo", "copy"],
            returncode=1,
            stdout="",
            stderr="network error",
        )

        src_cred = Secret("src-user:pass")
        dest_cred = Secret("dest-user:pass")

        success, message = copy_image(
            mock_client,
            "quay.io/src/image@sha256:abc",
            "quay.io/dest/image:tag",
            src_cred,
            dest_cred,
            logger=mock.Mock(),
        )

        assert success is False
        assert message == "Error: Failed publishing Index Image"


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

    @mock.patch("publish_index_image.parse_arguments")
    @mock.patch("publish_index_image.setup_logger")
    @mock.patch("publish_index_image.SkopeoClient")
    @mock.patch("publish_index_image.load_credential")
    def test_main_successful_copy(
        self, mock_load_cred, mock_skopeo_class, mock_logger, mock_parse_args, capsys
    ) -> None:
        """Test main workflow with successful image copy."""
        mock_args = mock.Mock()
        mock_args.source_index = "quay.io/src/image@sha256:abc123"
        mock_args.target_index = "quay.io/dest/image:v1.0"
        mock_args.target_ocp_version = "4.12"
        mock_args.verbose = False
        mock_args.source_credential_path = "/src/cred"
        mock_args.target_credential_path = "/dest/cred"
        mock_parse_args.return_value = mock_args

        mock_logger_instance = mock.Mock()
        mock_logger.return_value = mock_logger_instance

        mock_client = mock.Mock()
        mock_skopeo_class.return_value = mock_client
        mock_client.inspect.return_value.config.Labels.get.return_value = "4.12"
        mock_client.inspect.return_value.digest = "sha256:abc456"
        mock_client.copy.return_value = None

        src_cred = Secret("src:pass")
        dest_cred = Secret("dest:pass")
        mock_load_cred.side_effect = [src_cred, dest_cred]

        result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "Index Image Published successfully" in captured.out
        mock_client.copy.assert_called_once()

    @mock.patch("publish_index_image.parse_arguments")
    @mock.patch("publish_index_image.setup_logger")
    @mock.patch("publish_index_image.SkopeoClient")
    @mock.patch("publish_index_image.load_credential")
    def test_main_digest_match_skip_copy(
        self, mock_load_cred, mock_skopeo_class, mock_logger, mock_parse_args, capsys
    ) -> None:
        """Test main workflow when target exists with same digest."""
        mock_args = mock.Mock()
        mock_args.source_index = "quay.io/src/image@sha256:abc123"
        mock_args.target_index = "quay.io/dest/image:v1.0"
        mock_args.target_ocp_version = "4.12"
        mock_args.verbose = False
        mock_args.source_credential_path = "/src/cred"
        mock_args.target_credential_path = "/dest/cred"
        mock_parse_args.return_value = mock_args

        mock_logger_instance = mock.Mock()
        mock_logger.return_value = mock_logger_instance

        mock_client = mock.Mock()
        mock_client.inspect.return_value.config.Labels.get.return_value = "4.12"
        mock_client.inspect.return_value.digest = "sha256:abc123"
        mock_skopeo_class.return_value = mock_client

        src_cred = Secret("src:pass")
        dest_cred = Secret("dest:pass")
        mock_load_cred.side_effect = [src_cred, dest_cred]

        result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "already exists with the same digest" in captured.out
        mock_client.copy.assert_not_called()

    @mock.patch("publish_index_image.parse_arguments")
    @mock.patch("publish_index_image.setup_logger")
    @mock.patch("publish_index_image.SkopeoClient")
    @mock.patch("publish_index_image.load_credential")
    def test_main_digest_mismatch_copy(
        self, mock_load_cred, mock_skopeo_class, mock_logger, mock_parse_args, capsys
    ) -> None:
        """Test main workflow when target exists with different digest."""
        mock_args = mock.Mock()
        mock_args.source_index = "quay.io/src/image@sha256:abc123"
        mock_args.target_index = "quay.io/dest/image:v1.0"
        mock_args.verbose = False
        mock_args.source_credential_path = "/src/cred"
        mock_args.target_credential_path = "/dest/cred"
        mock_args.target_ocp_version = "4.12"
        mock_parse_args.return_value = mock_args

        mock_logger_instance = mock.Mock()
        mock_logger.return_value = mock_logger_instance

        mock_client = mock.Mock()
        mock_client.inspect.return_value.config.Labels.get.return_value = "4.12"
        mock_client.inspect.return_value.digest = "sha256:different"
        mock_skopeo_class.return_value = mock_client
        mock_client.copy.return_value = None

        src_cred = Secret("src:pass")
        dest_cred = Secret("dest:pass")
        mock_load_cred.side_effect = [src_cred, dest_cred]

        result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "Index Image Published successfully" in captured.out
        mock_client.copy.assert_called_once()

    @mock.patch("publish_index_image.parse_arguments")
    @mock.patch("publish_index_image.setup_logger")
    @mock.patch("publish_index_image.SkopeoClient")
    @mock.patch("publish_index_image.load_credential")
    def test_main_copy_failure(
        self, mock_load_cred, mock_skopeo_class, mock_logger, mock_parse_args, capsys
    ) -> None:
        """Test main workflow when copy fails."""
        mock_args = mock.Mock()
        mock_args.source_index = "quay.io/src/image@sha256:abc123"
        mock_args.target_index = "quay.io/dest/image:v1.0"
        mock_args.verbose = False
        mock_args.source_credential_path = "/src/cred"
        mock_args.target_credential_path = "/dest/cred"
        mock_args.target_ocp_version = "4.12"
        mock_parse_args.return_value = mock_args

        mock_logger_instance = mock.Mock()
        mock_logger.return_value = mock_logger_instance


        mock_client = mock.Mock()
        mock_client.inspect.return_value.config.Labels.get.return_value = "4.12"
        mock_client.inspect.return_value.digest = "sha256:abc456"
        mock_skopeo_class.return_value = mock_client
        mock_client.copy.side_effect = SkopeoClientError(
            "Network error",
            command=["skopeo", "copy"],
            returncode=1,
            stdout="",
            stderr="connection timeout",
        )

        src_cred = Secret("src:pass")
        dest_cred = Secret("dest:pass")
        mock_load_cred.side_effect = [src_cred, dest_cred]

        result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Error: Failed publishing Index Image" in captured.out

    @mock.patch("publish_index_image.parse_arguments")
    @mock.patch("publish_index_image.setup_logger")
    @mock.patch("publish_index_image.SkopeoClient")
    @mock.patch("publish_index_image.load_credential")
    def test_main_credential_load_failure(
        self, mock_load_cred, mock_skopeo_class, mock_logger, mock_parse_args, capsys
    ) -> None:
        """Test main workflow when credential loading fails."""
        mock_args = mock.Mock()
        mock_args.source_index = "quay.io/src/image@sha256:abc123"
        mock_args.target_index = "quay.io/dest/image:v1.0"
        mock_args.verbose = False
        mock_args.source_credential_path = "/src/cred"
        mock_args.target_credential_path = "/dest/cred"
        mock_parse_args.return_value = mock_args

        mock_logger_instance = mock.Mock()
        mock_logger.return_value = mock_logger_instance

        mock_client = mock.Mock()
        mock_skopeo_class.return_value = mock_client

        mock_load_cred.side_effect = FileNotFoundError("Credential file not found")

        result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Credential file not found" in captured.out

    @mock.patch("publish_index_image.parse_arguments")
    @mock.patch("publish_index_image.setup_logger")
    @mock.patch("publish_index_image.SkopeoClient")
    @mock.patch("publish_index_image.load_credential")
    def test_main_verbose_logging(
        self, mock_load_cred, mock_skopeo_class, mock_logger, mock_parse_args
    ) -> None:
        """Test that verbose flag enables debug logging."""
        mock_args = mock.Mock()
        mock_args.source_index = "quay.io/src/image@sha256:abc123"
        mock_args.target_index = "quay.io/dest/image:v1.0"
        mock_args.verbose = True
        mock_args.source_credential_path = "/src/cred"
        mock_args.target_credential_path = "/dest/cred"
        mock_parse_args.return_value = mock_args

        mock_logger_instance = mock.Mock()
        mock_logger.return_value = mock_logger_instance

        mock_client = mock.Mock()
        mock_skopeo_class.return_value = mock_client
        mock_client.inspect.return_value = "sha256:abc123"

        src_cred = Secret("src:pass")
        dest_cred = Secret("dest:pass")
        mock_load_cred.side_effect = [src_cred, dest_cred]

        main()

        mock_logger.assert_called_once_with(level=logging.DEBUG, name="publish_index_image")

    @mock.patch("publish_index_image.parse_arguments")
    @mock.patch("publish_index_image.setup_logger")
    @mock.patch("publish_index_image.SkopeoClient")
    @mock.patch("publish_index_image.load_credential")
    def test_main_proxy_registry_no_auth(
        self, mock_load_cred, mock_skopeo_class, mock_logger, mock_parse_args
    ) -> None:
        """Test main workflow with proxy registry (no source auth needed)."""
        mock_args = mock.Mock()
        mock_args.source_index = "registry-proxy.engineering.redhat.com/image@sha256:abc123"
        mock_args.target_index = "quay.io/dest/image:v1.0"
        mock_args.verbose = False
        mock_args.source_credential_path = "/src/cred"
        mock_args.target_credential_path = "/dest/cred"
        mock_args.target_ocp_version = None
        mock_parse_args.return_value = mock_args

        mock_logger_instance = mock.Mock()
        mock_logger.return_value = mock_logger_instance

        mock_client = mock.Mock()
        mock_skopeo_class.return_value = mock_client
        mock_client.inspect.return_value = None
        mock_client.copy.return_value = None

        src_cred = Secret("src:pass")
        dest_cred = Secret("dest:pass")
        mock_load_cred.side_effect = [src_cred, dest_cred]

        result = main()

        assert result == 0
        call_kwargs = mock_client.copy.call_args[1]
        assert call_kwargs["src_creds"] is None

    @mock.patch("publish_index_image.parse_arguments")
    @mock.patch("publish_index_image.setup_logger")
    @mock.patch("publish_index_image.SkopeoClient")
    @mock.patch("publish_index_image.load_credential")
    def test_main_no_source_digest(
        self, mock_load_cred, mock_skopeo_class, mock_logger, mock_parse_args
    ) -> None:
        """Test main workflow when source index has no digest."""
        mock_args = mock.Mock()
        mock_args.source_index = "quay.io/src/image:latest"
        mock_args.target_index = "quay.io/dest/image:v1.0"
        mock_args.verbose = False
        mock_args.source_credential_path = "/src/cred"
        mock_args.target_credential_path = "/dest/cred"
        mock_args.target_ocp_version = None
        mock_parse_args.return_value = mock_args

        mock_logger_instance = mock.Mock()
        mock_logger.return_value = mock_logger_instance

        mock_client = mock.Mock()
        mock_skopeo_class.return_value = mock_client
        mock_client.inspect.return_value = None
        mock_client.copy.return_value = None

        src_cred = Secret("src:pass")
        dest_cred = Secret("dest:pass")
        mock_load_cred.side_effect = [src_cred, dest_cred]

        result = main()

        assert result == 0
