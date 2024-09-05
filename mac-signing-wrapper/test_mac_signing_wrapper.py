import os
import tempfile
from unittest.mock import Mock, mock_open, patch

import pytest
import yaml

import mac_signing_wrapper


@pytest.fixture
def mock_config():
    return {
        "mac_host": "example_mac_host",
        "mac_user": "mac_user",
        "mac_password": "mac_password",
        "oci_registry_repo": "example_registry.com",
        "quay_username": "quay_user",
        "quay_password": "quay_pass",
        "keychain_password": "keychain_pass",
        "signing_identity": "Test Signing Identity",
        "apple_id": "apple@example.com",
        "app_specific_password": "app_pass",
        "team_id": "KONFLUX",
    }


@pytest.fixture
def mock_args():
    args = Mock()
    args.config_file = "config.yaml"
    args.digest = "unsigned_digest"
    return args


def test_load_config():
    yaml_date = {"OS": "mac"}
    mock_config_data = yaml.dump(yaml_date)
    with patch("builtins.open", mock_open(read_data=mock_config_data)):
        config = mac_signing_wrapper.load_config("some/path")
    assert config == yaml_date


@patch("argparse.ArgumentParser.parse_args")
def test_parse_arguments(mock_parse_args):
    mock_parse_args.return_value = Mock(config_file="config.yaml", digest="unsigned_digest")
    args = mac_signing_wrapper.parse_arguments()
    assert args.config_file == "config.yaml"
    assert args.digest == "unsigned_digest"


@patch("mac_signing_wrapper.SSHConnection")
@patch("mac_signing_wrapper.OrasCommands")
@patch("mac_signing_wrapper.MacOSCommands")
@patch("mac_signing_wrapper.zip_files")
@patch("mac_signing_wrapper.tempfile.TemporaryDirectory")
@patch("mac_signing_wrapper.parse_arguments")
@patch("mac_signing_wrapper.load_config")
def test_main_success(
    mock_load_config,
    mock_parse_args,
    mock_temp_dir,
    mock_zip_files,
    MockMacOSCommands,
    MockOrasCommands,
    MockSSHConnection,
    mock_config,
    mock_args,
):
    mock_parse_args.return_value = mock_args
    mock_load_config.return_value = mock_config
    mock_temp_dir.return_value.__enter__.return_value = "/tmp/test_dir"

    mock_ssh = MockSSHConnection.return_value.__enter__.return_value
    mock_oras = MockOrasCommands.return_value
    mock_macos = MockMacOSCommands.return_value
    mock_oras.push_zip.return_value = "new_digest"

    result = mac_signing_wrapper.main()

    assert result == "new_digest"
    mock_oras.pull_content.assert_called_once_with("unsigned_digest", "/tmp/test_dir")
    mock_macos.unlock_keychain.assert_called_once_with("keychain_pass")
    mock_macos.sign_binaries.assert_called_once_with("Test Signing Identity", "/tmp/test_dir")
    mock_zip_files.assert_called_once()
    mock_macos.notarize_binaries.assert_called_once_with(
        "apple@example.com", "app_pass", "KONFLUX", "/tmp/test_dir/signed_binaries.zip"
    )
    mock_oras.push_zip.assert_called_once()
