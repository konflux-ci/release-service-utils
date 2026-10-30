"""Tests for the ``cosign`` helper module."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from release_service_utils.helpers import cosign

_RUN_CMD = "release_service_utils.helpers.cosign.cosign.subprocess_cmd.run_cmd"
_SLEEP = "release_service_utils.helpers.retry.retry.time.sleep"
_IMAGE = "quay.io/test/img@sha256:abc"


def _completed(
    cmd: list[str],
    stdout: str = "",
    returncode: int = 0,
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    """Build a CompletedProcess for command mocks."""
    return subprocess.CompletedProcess(
        args=cmd, returncode=returncode, stdout=stdout, stderr=stderr
    )


def _called_process_error(
    cmd: list[str],
    *,
    stderr: str = "",
    stdout: str = "",
) -> subprocess.CalledProcessError:
    """Build a CalledProcessError as raised by subprocess_cmd.run_cmd."""
    return subprocess.CalledProcessError(1, cmd, output=stdout, stderr=stderr)


def test_download_sbom_success(tmp_path: Path) -> None:
    """Authenticate, invoke cosign, and write the output file."""
    output = tmp_path / "nested" / "image.sbom"
    captured: dict[str, object] = {}

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout='{"auths": {"quay.io": {}}}\n')
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env")
        output.write_text("sbom-data", encoding="utf-8")
        return _completed(cmd, stderr="downloaded\n")

    with patch(_RUN_CMD, side_effect=fake_run) as mock_run:
        cosign.download("sbom", _IMAGE, output)

    assert output.read_text(encoding="utf-8") == "sbom-data"
    assert captured["cmd"] == [
        "cosign",
        "download",
        "sbom",
        "--output-file",
        str(output),
        _IMAGE,
    ]
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["COSIGN_MAX_ATTACHMENT_SIZE"] == "200000000"
    assert mock_run.call_args_list[0].args[0] == ["select-oci-auth", _IMAGE]


def test_download_sbom_with_platform(tmp_path: Path) -> None:
    """Pass --platform so a multi-arch image yields one architecture."""
    output = tmp_path / "image.sbom"
    captured: dict[str, object] = {}

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        captured["cmd"] = cmd
        output.write_text("sbom-data", encoding="utf-8")
        return _completed(cmd)

    with patch(_RUN_CMD, side_effect=fake_run):
        cosign.download("sbom", _IMAGE, output, platform="linux/amd64")

    assert captured["cmd"] == [
        "cosign",
        "download",
        "sbom",
        "--output-file",
        str(output),
        "--platform",
        "linux/amd64",
        _IMAGE,
    ]


def test_download_attestation_empty_auth(tmp_path: Path) -> None:
    """Write an empty auths object when select-oci-auth prints nothing."""
    output = tmp_path / "image.att"
    auth_written: list[str] = []

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="  \n")
        env = kwargs.get("env")
        assert isinstance(env, dict)
        config_path = Path(str(env["DOCKER_CONFIG"])) / "config.json"
        auth_written.append(config_path.read_text(encoding="utf-8"))
        output.write_text("att", encoding="utf-8")
        return _completed(cmd)

    with patch(_RUN_CMD, side_effect=fake_run):
        cosign.download("attestation", _IMAGE, output)

    assert auth_written == ["{}"]
    assert output.is_file()


def test_download_select_oci_auth_failure(tmp_path: Path) -> None:
    """Raise when select-oci-auth fails and log stderr only."""
    output = tmp_path / "image.sbom"

    with patch(
        _RUN_CMD,
        side_effect=_called_process_error(
            ["select-oci-auth", _IMAGE], stderr="no credentials\n"
        ),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            cosign.download("sbom", _IMAGE, output)


def test_download_select_oci_auth_failure_empty_stderr(tmp_path: Path) -> None:
    """Raise when select-oci-auth fails with no stderr to log."""
    output = tmp_path / "image.sbom"

    with patch(
        _RUN_CMD,
        side_effect=_called_process_error(["select-oci-auth", _IMAGE], stderr=""),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            cosign.download("sbom", _IMAGE, output)


@patch(_SLEEP)
def test_download_retries_then_succeeds(mock_sleep: MagicMock, tmp_path: Path) -> None:
    """Retry a failed cosign download then succeed."""
    output = tmp_path / "image.sbom"
    attempts = {"cosign": 0}

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        attempts["cosign"] += 1
        if attempts["cosign"] == 1:
            raise _called_process_error(cmd, stderr="timeout\n")
        output.write_text("ok", encoding="utf-8")
        return _completed(cmd)

    with patch(_RUN_CMD, side_effect=fake_run):
        cosign.download("sbom", _IMAGE, output, max_attempts=3)

    assert output.read_text(encoding="utf-8") == "ok"
    mock_sleep.assert_called_once()


@patch(_SLEEP)
def test_download_retries_exhausted(mock_sleep: MagicMock, tmp_path: Path) -> None:
    """Raise RuntimeError after cosign retries are exhausted."""
    output = tmp_path / "image.sbom"

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        raise _called_process_error(cmd, stderr="unavailable\n")

    with patch(_RUN_CMD, side_effect=fake_run):
        with pytest.raises(RuntimeError, match="after 3 attempts"):
            cosign.download("sbom", _IMAGE, output)

    assert mock_sleep.call_count == 2


def test_download_missing_output_file(tmp_path: Path) -> None:
    """Raise when cosign exits 0 but does not create the output file."""
    output = tmp_path / "missing.sbom"

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        return _completed(cmd)

    with patch(_RUN_CMD, side_effect=fake_run):
        with pytest.raises(RuntimeError, match="was not created"):
            cosign.download("sbom", _IMAGE, output)


def test_download_custom_attachment_size(tmp_path: Path) -> None:
    """Honor a caller-supplied COSIGN_MAX_ATTACHMENT_SIZE."""
    output = tmp_path / "image.sbom"
    sizes: list[str] = []

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        env = kwargs.get("env")
        assert isinstance(env, dict)
        sizes.append(str(env["COSIGN_MAX_ATTACHMENT_SIZE"]))
        output.write_text("ok", encoding="utf-8")
        return _completed(cmd)

    with patch(_RUN_CMD, side_effect=fake_run):
        cosign.download("sbom", _IMAGE, output, max_attachment_size=10)

    assert sizes == ["10"]


def test_download_logs_empty_stderr(tmp_path: Path) -> None:
    """Skip logging when commands produce no stderr."""
    output = tmp_path / "image.sbom"

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}", stderr="")
        output.write_text("ok", encoding="utf-8")
        return _completed(cmd, stderr="")

    with patch(_RUN_CMD, side_effect=fake_run):
        cosign.download("sbom", _IMAGE, output)


def test_docker_config_writes_auth_and_removes_dir() -> None:
    """Write select-oci-auth JSON into a temp dir and delete it on exit."""

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        return _completed(cmd, stdout='{"auths": {}}\n')

    with patch(_RUN_CMD, side_effect=fake_run):
        with cosign.docker_config(_IMAGE) as config_dir:
            assert (config_dir / "config.json").read_text(encoding="utf-8") == '{"auths": {}}'
            kept = config_dir
        assert not kept.exists()


def test_download_uses_provided_docker_config_dir(tmp_path: Path) -> None:
    """Skip select-oci-auth when a docker config directory is supplied."""
    output = tmp_path / "image.sbom"
    config_dir = tmp_path / "docker"
    config_dir.mkdir()
    (config_dir / "config.json").write_text("{}", encoding="utf-8")
    captured: dict[str, object] = {}

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env")
        output.write_text("sbom-data", encoding="utf-8")
        return _completed(cmd)

    with patch(_RUN_CMD, side_effect=fake_run) as mock_run:
        cosign.download("sbom", _IMAGE, output, docker_config_dir=config_dir)

    assert mock_run.call_count == 1
    assert captured["cmd"] == [
        "cosign",
        "download",
        "sbom",
        "--output-file",
        str(output),
        _IMAGE,
    ]
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["DOCKER_CONFIG"] == str(config_dir)


def test_docker_config_shared_across_downloads(tmp_path: Path) -> None:
    """Authenticate once when two downloads reuse the same docker config."""
    sbom = tmp_path / "image.sbom"
    att = tmp_path / "image.att"
    commands: list[str] = []

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(cmd[0])
        if cmd[0] == "cosign":
            Path(cmd[cmd.index("--output-file") + 1]).write_text("x", encoding="utf-8")
        stdout = "{}" if cmd[0] == "select-oci-auth" else ""
        return _completed(cmd, stdout=stdout)

    with patch(_RUN_CMD, side_effect=fake_run):
        with cosign.docker_config(_IMAGE) as config_dir:
            cosign.download("sbom", _IMAGE, sbom, docker_config_dir=config_dir)
            cosign.download("attestation", _IMAGE, att, docker_config_dir=config_dir)

    assert commands == ["select-oci-auth", "cosign", "cosign"]


def test_verify_attestation_uses_key_and_returns_stdout() -> None:
    """Run verify-attestation with the key and return its output."""
    with patch(_RUN_CMD) as run:
        run.side_effect = [
            _completed([], stdout='{"auths": {}}'),
            _completed([], stdout="envelope\n"),
        ]
        out = cosign.verify_attestation(_IMAGE, key="k8s://ns/key", max_attempts=1)

    assert out == "envelope\n"
    assert run.call_args.args[0] == [
        "cosign",
        "verify-attestation",
        "--type",
        "spdxjson",
        "--key",
        "k8s://ns/key",
        "--insecure-ignore-tlog=true",
        _IMAGE,
    ]
    assert "DOCKER_CONFIG" in run.call_args.kwargs["env"]


@patch(_SLEEP)
def test_verify_attestation_retries_exhausted(mock_sleep: MagicMock) -> None:
    """Raise RuntimeError after the last failed verification attempt."""

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        raise _called_process_error(cmd, stderr="no matching signatures")

    with patch(_RUN_CMD, side_effect=fake_run):
        with pytest.raises(RuntimeError, match="after 2 attempts"):
            cosign.verify_attestation(_IMAGE, key="k", max_attempts=2)
