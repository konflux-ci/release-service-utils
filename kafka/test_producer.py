"""Unit tests for kafka.producer."""

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure kafka dir is on path when running pytest from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent))
import producer as producer_module  # noqa: E402


@pytest.fixture
def temp_files():
    """Create temp dir with bootstrap, username, password, and message JSON files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        p = Path(tmpdir)
        (p / "bootstrap.txt").write_text("broker1:9096,broker2:9096")
        (p / "username.txt").write_text("test-user")
        (p / "password.txt").write_text("test-password")
        (p / "message.json").write_text('{"kind": "Advisory", "metadata": {"name": "2026:1"}}')
        yield p


@pytest.fixture
def producer_argv(temp_files):
    """argv for producer with all required file options."""
    return [
        "producer.py",
        "--json-file",
        str(temp_files / "message.json"),
        "--bootstrap-servers-file",
        str(temp_files / "bootstrap.txt"),
        "--username-file",
        str(temp_files / "username.txt"),
        "--password-file",
        str(temp_files / "password.txt"),
    ]


@patch.dict(os.environ, {"KAFKA_TOPIC": "test.topic"})
@patch("producer.Producer")
def test_producer_main_calls_produce_with_value_and_topic(
    MockProducer, producer_argv, temp_files
):
    """Producer main() reads JSON file and calls produce() with correct topic and value."""
    mock_producer = MagicMock()
    MockProducer.return_value = mock_producer

    with patch.object(sys, "argv", producer_argv):
        producer_module.main()

    mock_producer.produce.assert_called_once()
    call_kw = mock_producer.produce.call_args[1]
    assert call_kw["topic"] == "test.topic"
    assert json.loads(call_kw["value"]) == {
        "kind": "Advisory",
        "metadata": {"name": "2026:1"},
    }
    assert call_kw["headers"] is None
    mock_producer.poll.assert_called()
    mock_producer.flush.assert_called_once()


@patch.dict(os.environ, {"KAFKA_TOPIC": "test.topic"})
@patch("producer.Producer")
def test_producer_main_passes_headers(MockProducer, producer_argv, temp_files):
    """Producer main() passes parsed headers to produce() when --header is given."""
    mock_producer = MagicMock()
    MockProducer.return_value = mock_producer
    producer_argv.extend(["--header", "advisory_state=updated", "--header", "a=b"])

    with patch.object(sys, "argv", producer_argv):
        producer_module.main()

    call_kw = mock_producer.produce.call_args[1]
    assert call_kw["headers"] == [
        ("advisory_state", b"updated"),
        ("a", b"b"),
    ]


@patch.dict(os.environ, {"KAFKA_TOPIC": "test.topic"})
@patch("producer.Producer")
def test_producer_main_builds_config_from_files(MockProducer, producer_argv, temp_files):
    """Producer main() builds config from file contents."""
    with patch.object(sys, "argv", producer_argv):
        producer_module.main()

    MockProducer.assert_called_once()
    config = MockProducer.call_args[0][0]
    assert config["bootstrap.servers"] == "broker1:9096,broker2:9096"
    assert config["sasl.username"] == "test-user"
    assert config["sasl.password"] == "test-password"
    assert config["security.protocol"] == "SASL_SSL"
    assert config["retries"] == 5
    assert config["message.timeout.ms"] == 60000


def test_producer_main_requires_kafka_topic(producer_argv):
    """Producer main() exits when KAFKA_TOPIC is not set."""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("KAFKA_TOPIC", None)
        with patch.object(sys, "argv", producer_argv):
            with pytest.raises(SystemExit):
                producer_module.main()


@patch.dict(os.environ, {"KAFKA_TOPIC": "t"})
@patch("producer.Producer")
def test_producer_main_invalid_header_exits(MockProducer, producer_argv):
    """Producer main() exits when --header has no '='."""
    producer_argv.extend(["--header", "invalid"])
    with patch.object(sys, "argv", producer_argv):
        with pytest.raises(SystemExit):
            producer_module.main()


def test_producer_main_missing_required_arg():
    """Producer main() exits when required args are missing."""
    with patch.dict(os.environ, {"KAFKA_TOPIC": "t"}):
        with patch.object(sys, "argv", ["producer.py"]):
            with pytest.raises(SystemExit):
                producer_module.main()
