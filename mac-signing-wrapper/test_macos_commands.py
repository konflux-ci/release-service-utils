import json
from unittest.mock import Mock

import pytest

from macos_commands import MacOSCommands


@pytest.fixture
def mock_ssh():
    return Mock()


@pytest.fixture
def macos_commands(mock_ssh):
    return MacOSCommands(mock_ssh)


@pytest.fixture
def notarize_params():
    return {
        "username": "mac_user",
        "password": "mac_password",
        "team_id": "KONFLUX",
        "zip_path": "/test/signed_binaries.zip",
    }


def test_unlock_keychain(macos_commands, mock_ssh):
    keychain_password = "mac_password"
    macos_commands.unlock_keychain(keychain_password)
    mock_ssh.run_command.assert_called_once_with(
        f"security unlock-keychain -p {keychain_password} login.keychain", sensitive=True
    )


def test_sign_binaries(macos_commands, mock_ssh):
    mock_ssh.run_command.side_effect = [
        ("test_binary_1\ntest_binary_2", None),  # mock output of find command
        None,
        None,  # mock output of codesign command
    ]

    signing_identity = "Test Signing Identity"
    target_dir = "/test/target_dir"
    signed_files = macos_commands.sign_binaries(signing_identity, target_dir)

    assert signed_files == ["test_binary_1", "test_binary_2"]
    assert mock_ssh.run_command.call_count == 3
    mock_ssh.run_command.assert_any_call(f"find {target_dir} -type f")
    for file in signed_files:
        mock_ssh.run_command.assert_any_call(
            f"xcrun codesign --sign 'Developer ID Application: {signing_identity}' "
            f"--options runtime --timestamp --force {file}",
            sensitive=True,
        )


def test_notarize_binaries_success(macos_commands, mock_ssh, notarize_params):
    mock_ssh.run_command.return_value = ('{"status": "Accepted", "id": "test_id"}', None)
    result = macos_commands.notarize_binaries(**notarize_params)

    assert result == {"status": "Accepted", "id": "test_id"}
    mock_ssh.run_command.assert_called_once_with(
        f"xcrun notarytool submit {notarize_params['zip_path']} "
        f"--output-format json "
        f"--wait "
        f"--apple-id {notarize_params['username']} "
        f"--team-id {notarize_params['team_id']} "
        f"--password {notarize_params['password']}",
        sensitive=True,
    )


def test_notarize_binaries_failure(macos_commands, mock_ssh, notarize_params):
    mock_ssh.run_command.return_value = (
        json.dumps({"status": "Invalid", "issues": [{"message": "Test error message"}]}),
        None,
    )

    with pytest.raises(Exception) as failure_exception:
        macos_commands.notarize_binaries(**notarize_params)

    assert "Notarization failed" in str(failure_exception.value)
    assert "Test error message" in str(failure_exception.value)


def test_notarize_binaries_error(macos_commands, mock_ssh, notarize_params):
    mock_ssh.run_command.return_value = (None, "Command failed")

    with pytest.raises(Exception) as failure_exception:
        macos_commands.notarize_binaries(**notarize_params)

    assert "Notarization error: Command failed" in str(failure_exception.value)
