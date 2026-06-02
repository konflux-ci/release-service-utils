"""Tests for the ``skopeo`` helper module."""

from __future__ import annotations

import subprocess
import unittest.mock as mock

import pytest
import skopeo


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


def test_inspect_check_true_passes_check() -> None:
    """``check=True`` is forwarded to subprocess.run."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.inspect("img:v1", check=True)

    assert run_mock.call_args[1]["check"] is True


def test_inspect_check_true_raises_on_failure() -> None:
    """``check=True`` raises CalledProcessError on non-zero exit."""
    with mock.patch(
        "skopeo.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "skopeo"),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            skopeo.inspect("img:v1", check=True)


def test_inspect_creds_flag() -> None:
    """``creds`` adds ``--creds`` to the command."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.inspect("img:v1", creds="user:pass")

    cmd = run_mock.call_args[0][0]
    assert "--creds" in cmd
    idx = cmd.index("--creds")
    assert cmd[idx + 1] == "user:pass"


def test_inspect_creds_none_omitted() -> None:
    """``creds=None`` does not add ``--creds`` to the command."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.inspect("img:v1", creds=None)

    cmd = run_mock.call_args[0][0]
    assert "--creds" not in cmd


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


def test_copy_check_true_passes_check() -> None:
    """``check=True`` is forwarded to subprocess.run."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.copy("docker://img:v1", "dir:/tmp/out", check=True)

    assert run_mock.call_args[1]["check"] is True


def test_copy_check_true_raises_on_failure() -> None:
    """``check=True`` raises CalledProcessError on non-zero exit."""
    with mock.patch(
        "skopeo.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "skopeo"),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            skopeo.copy("docker://img:v1", "dir:/tmp/out", check=True)


def test_copy_all_flag() -> None:
    """``all=True`` adds ``--all`` to the command."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.copy("docker://img:v1", "docker://img:v2", all=True)

    cmd = run_mock.call_args[0][0]
    assert "--all" in cmd


def test_copy_preserve_digests_flag() -> None:
    """``preserve_digests=True`` adds ``--preserve-digests``."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.copy("docker://img:v1", "docker://img:v2", preserve_digests=True)

    cmd = run_mock.call_args[0][0]
    assert "--preserve-digests" in cmd


def test_copy_src_tls_verify_false() -> None:
    """``src_tls_verify=False`` adds ``--src-tls-verify=false``."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.copy("docker://img:v1", "docker://img:v2", src_tls_verify=False)

    cmd = run_mock.call_args[0][0]
    assert "--src-tls-verify=false" in cmd


def test_copy_src_tls_verify_none_omitted() -> None:
    """``src_tls_verify=None`` does not add the flag."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.copy("docker://img:v1", "docker://img:v2", src_tls_verify=None)

    cmd = run_mock.call_args[0][0]
    assert not any("--src-tls-verify" in c for c in cmd)


def test_copy_src_creds() -> None:
    """``src_creds`` adds ``--src-creds`` to the command."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.copy("docker://img:v1", "docker://img:v2", src_creds="user:pass")

    cmd = run_mock.call_args[0][0]
    assert "--src-creds" in cmd
    idx = cmd.index("--src-creds")
    assert cmd[idx + 1] == "user:pass"


def test_copy_dest_creds() -> None:
    """``dest_creds`` adds ``--dest-creds`` to the command."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.copy("docker://img:v1", "docker://img:v2", dest_creds="user:pass")

    cmd = run_mock.call_args[0][0]
    assert "--dest-creds" in cmd
    idx = cmd.index("--dest-creds")
    assert cmd[idx + 1] == "user:pass"


def test_copy_all_options_combined() -> None:
    """All copy options are included when specified."""
    with mock.patch(
        "skopeo.subprocess.run",
        return_value=_completed(),
    ) as run_mock:
        skopeo.copy(
            "docker://src:v1",
            "docker://dest:v1",
            all=True,
            preserve_digests=True,
            src_tls_verify=False,
            src_creds="src-user:pass",
            dest_creds="dest-user:pass",
            retry_times=5,
        )

    cmd = run_mock.call_args[0][0]
    assert "--all" in cmd
    assert "--preserve-digests" in cmd
    assert "--src-tls-verify=false" in cmd
    assert "--src-creds" in cmd
    assert "--dest-creds" in cmd
    assert cmd[-2] == "docker://src:v1"
    assert cmd[-1] == "docker://dest:v1"
