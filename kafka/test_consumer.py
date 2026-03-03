"""Unit tests for kafka.consumer."""

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure kafka dir is on path when running pytest from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent))
import consumer as consumer_module  # noqa: E402


@pytest.fixture
def temp_files():
    """Create temp dir with bootstrap, username, and password files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        p = Path(tmpdir)
        (p / "bootstrap.txt").write_text("broker1:9096,broker2:9096")
        (p / "username.txt").write_text("test-user")
        (p / "password.txt").write_text("test-password")
        yield p


@pytest.fixture
def consumer_argv(temp_files):
    """argv for consumer with all required file options."""
    return [
        "consumer.py",
        "--bootstrap-servers-file",
        str(temp_files / "bootstrap.txt"),
        "--username-file",
        str(temp_files / "username.txt"),
        "--password-file",
        str(temp_files / "password.txt"),
    ]


@patch.dict(os.environ, {"KAFKA_TOPIC": "test.topic"})
@patch("consumer.Consumer")
def test_consumer_main_builds_config_from_files(MockConsumer, consumer_argv, temp_files):
    """Consumer main() builds config from file contents and subscribes to KAFKA_TOPIC."""
    mock_consumer = MagicMock()
    mock_consumer.poll.side_effect = KeyboardInterrupt()  # exit loop on first poll
    MockConsumer.return_value = mock_consumer

    with patch.object(sys, "argv", consumer_argv):
        consumer_module.main()

    MockConsumer.assert_called_once()
    config = MockConsumer.call_args[0][0]
    assert config["bootstrap.servers"] == "broker1:9096,broker2:9096"
    assert config["sasl.username"] == "test-user"
    assert config["sasl.password"] == "test-password"
    assert config["security.protocol"] == "SASL_SSL"
    assert config["group.id"] == "kafka-python-getting-started"
    mock_consumer.subscribe.assert_called_once_with(["test.topic"])
    mock_consumer.close.assert_called_once()


def test_consumer_main_requires_kafka_topic(consumer_argv):
    """Consumer main() exits when KAFKA_TOPIC is not set."""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("KAFKA_TOPIC", None)
        with patch.object(sys, "argv", consumer_argv):
            with pytest.raises(SystemExit):
                consumer_module.main()


def test_consumer_main_missing_required_arg():
    """Consumer main() exits when required args are missing."""
    with patch.dict(os.environ, {"KAFKA_TOPIC": "t"}):
        with patch.object(sys, "argv", ["consumer.py"]):
            with pytest.raises(SystemExit):
                consumer_module.main()
