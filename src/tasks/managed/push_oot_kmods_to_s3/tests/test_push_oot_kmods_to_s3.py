"""Tests for push_oot_kmods_to_s3."""

from __future__ import annotations

import runpy
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.tasks.managed.push_oot_kmods_to_s3 import main, run
from release_service_utils.tasks.managed.push_oot_kmods_to_s3.push_oot_kmods_to_s3 import (
    DEFAULT_S3_CREDENTIALS_MOUNT,
    _s3_client,
)

TASK = "release_service_utils.tasks.managed" ".push_oot_kmods_to_s3.push_oot_kmods_to_s3"


def _write_envfile(
    arch_dir: Path,
    *,
    vendor: str = "mocked-vendor-s3",
    version: str = "1.2.3-s3",
    kernel: str = "6.5.0-s3.x86_64",
    arch: str | None = None,
) -> None:
    """Write a kmod envfile into *arch_dir*."""
    lines = [
        f"DRIVER_VENDOR={vendor}",
        f"DRIVER_VERSION={version}",
        f"KERNEL_VERSION={kernel}",
    ]
    if arch is not None:
        lines.append(f"ARCH={arch}")
    (arch_dir / "envfile").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _single_arch_tree(
    data_dir: Path,
    *,
    with_checksum: bool = True,
    with_ko: bool = True,
    arch: str | None = None,
    kernel: str = "6.5.0-s3.x86_64",
) -> Path:
    """Create a single-arch signed-kmods tree and return the arch directory."""
    arch_dir = data_dir / "signed-kmods" / "x86_64"
    arch_dir.mkdir(parents=True)
    if with_ko:
        (arch_dir / "mod1.ko").write_text("mod1", encoding="utf-8")
        nested = arch_dir / "driver_001" / "subdir"
        nested.mkdir(parents=True)
        (nested / "nested.ko").write_text("nested", encoding="utf-8")
    _write_envfile(arch_dir, arch=arch, kernel=kernel)
    if with_checksum:
        (arch_dir / "signed_kmods_checksums_x86_64.txt").write_text(
            "deadbeef  mod1.ko\n",
            encoding="utf-8",
        )
    return arch_dir


def _creds(tmp_path: Path) -> Path:
    """Create a mock S3 credentials mount and return it."""
    mount = tmp_path / "secrets"
    mount.mkdir()
    (mount / "aws_access_key_id").write_text("AKIA_TEST\n", encoding="utf-8")
    (mount / "aws_secret_access_key").write_text("secret_test\n", encoding="utf-8")
    return mount


def _uploaded_keys(client: MagicMock) -> list[str]:
    """Return S3 keys passed to upload_file, in call order."""
    return [call.args[2] for call in client.upload_file.call_args_list]


def test_run_single_arch_uploads(tmp_path: Path) -> None:
    """Single-arch upload strips the kernel suffix and preserves .ko paths."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir)
    creds = _creds(tmp_path)
    client = MagicMock()

    with patch(f"{TASK}.boto3.client", return_value=client) as mock_client:
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            s3_endpoint="https://s3.mock.endpoint.com",
            s3_bucket="mock-bucket",
            creds_mount=creds,
        )

    mock_client.assert_called_once()
    kwargs = mock_client.call_args.kwargs
    assert kwargs["endpoint_url"] == "https://s3.mock.endpoint.com"
    assert kwargs["aws_access_key_id"] == "AKIA_TEST"
    assert kwargs["aws_secret_access_key"] == "secret_test"
    assert kwargs["region_name"] == "us-east-1"
    assert kwargs["config"].s3["addressing_style"] == "path"
    assert kwargs["config"].request_checksum_calculation == "when_required"
    assert kwargs["config"].response_checksum_validation == "when_required"

    keys = _uploaded_keys(client)
    prefix = "mocked-vendor-s3/1.2.3-s3/6.5.0-s3/x86_64/"
    assert prefix + "mod1.ko" in keys
    assert prefix + "driver_001/subdir/nested.ko" in keys
    assert prefix + "signed_kmods_checksums_x86_64.txt" in keys
    assert prefix + "envfile" in keys
    assert all("6.5.0-s3.x86_64" not in key for key in keys)
    for call in client.upload_file.call_args_list:
        assert call.args[1] == "mock-bucket"


def test_run_extracts_tarball_before_upload(tmp_path: Path) -> None:
    """signed-kmods.tar.gz is extracted when present."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir)
    signed = data_dir / "signed-kmods"
    archive = data_dir / "signed-kmods.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(signed, arcname="signed-kmods")
    for child in sorted(signed.rglob("*"), reverse=True):
        if child.is_file():
            child.unlink()
        elif child.is_dir():
            child.rmdir()
    signed.rmdir()

    client = MagicMock()
    with patch(f"{TASK}.boto3.client", return_value=client):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            s3_endpoint="https://s3.example",
            s3_bucket="bucket",
            creds_mount=_creds(tmp_path),
        )

    keys = _uploaded_keys(client)
    assert any(key.endswith("mod1.ko") for key in keys)


def test_run_extracts_hardcoded_tarball_for_custom_dest(tmp_path: Path) -> None:
    """A non-default signedKmodsPath still extracts signed-kmods.tar.gz."""
    data_dir = tmp_path / "data"
    dest_name = "custom-kmods"
    arch_dir = data_dir / dest_name / "x86_64"
    arch_dir.mkdir(parents=True)
    (arch_dir / "mod1.ko").write_text("mod1", encoding="utf-8")
    _write_envfile(arch_dir)
    signed = data_dir / dest_name
    archive = data_dir / "signed-kmods.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(signed, arcname=dest_name)
    for child in sorted(signed.rglob("*"), reverse=True):
        if child.is_file():
            child.unlink()
        elif child.is_dir():
            child.rmdir()
    signed.rmdir()

    client = MagicMock()
    with patch(f"{TASK}.boto3.client", return_value=client):
        run(
            data_dir=data_dir,
            signed_kmods_path=dest_name,
            s3_endpoint="https://s3.example",
            s3_bucket="bucket",
            creds_mount=_creds(tmp_path),
        )

    keys = _uploaded_keys(client)
    assert any(key.endswith("mod1.ko") for key in keys)


def test_run_extracts_tarball_for_nested_signed_kmods_path(tmp_path: Path) -> None:
    """A nested signedKmodsPath still extracts signed-kmods.tar.gz from data_dir."""
    data_dir = tmp_path / "data"
    dest_name = "artifacts/signed-kmods"
    arch_dir = data_dir / dest_name / "x86_64"
    arch_dir.mkdir(parents=True)
    (arch_dir / "mod1.ko").write_text("mod1", encoding="utf-8")
    _write_envfile(arch_dir)
    signed = data_dir / dest_name
    archive = data_dir / "signed-kmods.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(signed, arcname=dest_name)
    for child in sorted(signed.rglob("*"), reverse=True):
        if child.is_file():
            child.unlink()
        elif child.is_dir():
            child.rmdir()
    signed.rmdir()

    client = MagicMock()
    with patch(f"{TASK}.boto3.client", return_value=client):
        run(
            data_dir=data_dir,
            signed_kmods_path=dest_name,
            s3_endpoint="https://s3.example",
            s3_bucket="bucket",
            creds_mount=_creds(tmp_path),
        )

    keys = _uploaded_keys(client)
    assert any(key.endswith("mod1.ko") for key in keys)


def test_run_uses_envfile_arch(tmp_path: Path) -> None:
    """ARCH from envfile is used in the destination prefix."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir, arch="amd64")
    client = MagicMock()

    with patch(f"{TASK}.boto3.client", return_value=client):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            s3_endpoint="https://s3.example",
            s3_bucket="bucket",
            creds_mount=_creds(tmp_path),
        )

    keys = _uploaded_keys(client)
    assert any("/amd64/mod1.ko" in key for key in keys)
    assert all("/x86_64/" not in key for key in keys)


def test_run_multi_platform_uses_directory_name(tmp_path: Path) -> None:
    """ARCH=MULTI_PLATFORM keeps the architecture directory name."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir, arch="MULTI_PLATFORM")
    client = MagicMock()

    with patch(f"{TASK}.boto3.client", return_value=client):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            s3_endpoint="https://s3.example",
            s3_bucket="bucket",
            creds_mount=_creds(tmp_path),
        )

    keys = _uploaded_keys(client)
    assert any("/x86_64/mod1.ko" in key for key in keys)


def test_run_skips_missing_checksum(tmp_path: Path) -> None:
    """Checksum upload is skipped when the file is absent."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir, with_checksum=False)
    client = MagicMock()

    with patch(f"{TASK}.boto3.client", return_value=client):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            s3_endpoint="https://s3.example",
            s3_bucket="bucket",
            creds_mount=_creds(tmp_path),
        )

    keys = _uploaded_keys(client)
    assert not any("checksums" in key for key in keys)
    assert any(key.endswith("/envfile") for key in keys)


def test_run_multi_arch_uploads_summaries(tmp_path: Path) -> None:
    """Multi-arch uploads summaries then each architecture's artifacts."""
    data_dir = tmp_path / "data"
    signed = data_dir / "signed-kmods"
    x86 = signed / "x86_64"
    arm = signed / "aarch64"
    x86.mkdir(parents=True)
    arm.mkdir(parents=True)
    (x86 / "a.ko").write_text("a", encoding="utf-8")
    (arm / "b.ko").write_text("b", encoding="utf-8")
    _write_envfile(x86, kernel="6.5.0-s3.x86_64")
    _write_envfile(arm, kernel="6.5.0-s3.aarch64", vendor="mocked-vendor-s3")
    (signed / "signing_summary.txt").write_text("sign", encoding="utf-8")
    (signed / "extraction_summary.txt").write_text("extract", encoding="utf-8")
    client = MagicMock()

    with patch(f"{TASK}.boto3.client", return_value=client):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            s3_endpoint="https://s3.example",
            s3_bucket="bucket",
            creds_mount=_creds(tmp_path),
        )

    keys = _uploaded_keys(client)
    summary_prefix = "mocked-vendor-s3/1.2.3-s3/6.5.0-s3/multi-arch-summary/"
    assert summary_prefix + "signing_summary.txt" in keys
    assert summary_prefix + "extraction_summary.txt" in keys
    assert any(key.endswith("/x86_64/a.ko") for key in keys)
    assert any(key.endswith("/aarch64/b.ko") for key in keys)


def test_run_multi_arch_skips_missing_extraction_summary(tmp_path: Path) -> None:
    """Extraction summary is optional for multi-arch uploads."""
    data_dir = tmp_path / "data"
    signed = data_dir / "signed-kmods"
    x86 = signed / "x86_64"
    arm = signed / "aarch64"
    x86.mkdir(parents=True)
    arm.mkdir(parents=True)
    (x86 / "a.ko").write_text("a", encoding="utf-8")
    (arm / "b.ko").write_text("b", encoding="utf-8")
    _write_envfile(x86)
    _write_envfile(arm)
    (signed / "signing_summary.txt").write_text("sign", encoding="utf-8")
    client = MagicMock()

    with patch(f"{TASK}.boto3.client", return_value=client):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            s3_endpoint="https://s3.example",
            s3_bucket="bucket",
            creds_mount=_creds(tmp_path),
        )

    keys = _uploaded_keys(client)
    assert any(key.endswith("signing_summary.txt") for key in keys)
    assert not any(key.endswith("extraction_summary.txt") for key in keys)


def test_run_multi_arch_skips_summaries_without_signing(tmp_path: Path) -> None:
    """Multi-arch summaries are skipped when signing_summary.txt is absent."""
    data_dir = tmp_path / "data"
    signed = data_dir / "signed-kmods"
    x86 = signed / "x86_64"
    arm = signed / "aarch64"
    x86.mkdir(parents=True)
    arm.mkdir(parents=True)
    (x86 / "a.ko").write_text("a", encoding="utf-8")
    (arm / "b.ko").write_text("b", encoding="utf-8")
    _write_envfile(x86)
    _write_envfile(arm)
    (signed / "extraction_summary.txt").write_text("extract", encoding="utf-8")
    client = MagicMock()

    with patch(f"{TASK}.boto3.client", return_value=client):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            s3_endpoint="https://s3.example",
            s3_bucket="bucket",
            creds_mount=_creds(tmp_path),
        )

    keys = _uploaded_keys(client)
    assert not any("multi-arch-summary" in key for key in keys)


def test_run_no_ko_files_raises(tmp_path: Path) -> None:
    """An architecture directory with no .ko files fails."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir, with_ko=False)
    with patch(f"{TASK}.boto3.client", return_value=MagicMock()):
        with pytest.raises(FileNotFoundError, match="No .ko files"):
            run(
                data_dir=data_dir,
                signed_kmods_path="signed-kmods",
                s3_endpoint="https://s3.example",
                s3_bucket="bucket",
                creds_mount=_creds(tmp_path),
            )


def test_run_missing_envfile_raises(tmp_path: Path) -> None:
    """A missing envfile fails the upload."""
    data_dir = tmp_path / "data"
    arch_dir = data_dir / "signed-kmods" / "x86_64"
    arch_dir.mkdir(parents=True)
    (arch_dir / "mod1.ko").write_text("mod1", encoding="utf-8")
    with patch(f"{TASK}.boto3.client", return_value=MagicMock()):
        with pytest.raises(FileNotFoundError, match="envfile not found"):
            run(
                data_dir=data_dir,
                signed_kmods_path="signed-kmods",
                s3_endpoint="https://s3.example",
                s3_bucket="bucket",
                creds_mount=_creds(tmp_path),
            )


def test_run_no_arch_directories_raises(tmp_path: Path) -> None:
    """An empty signed-kmods path fails."""
    data_dir = tmp_path / "data"
    (data_dir / "signed-kmods").mkdir(parents=True)
    with pytest.raises(RuntimeError, match="No architecture directories"):
        run(
            data_dir=data_dir,
            signed_kmods_path="signed-kmods",
            s3_endpoint="https://s3.example",
            s3_bucket="bucket",
            creds_mount=_creds(tmp_path),
        )


def test_run_upload_failure_propagates(tmp_path: Path) -> None:
    """S3 upload errors are not swallowed."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir)
    client = MagicMock()
    client.upload_file.side_effect = RuntimeError("boom")
    with patch(f"{TASK}.boto3.client", return_value=client):
        with pytest.raises(RuntimeError, match="boom"):
            run(
                data_dir=data_dir,
                signed_kmods_path="signed-kmods",
                s3_endpoint="https://s3.example",
                s3_bucket="bucket",
                creds_mount=_creds(tmp_path),
            )


def test_s3_client_builds_path_style_client() -> None:
    """_s3_client passes endpoint, keys, and path-style config to boto3."""
    with patch(f"{TASK}.boto3.client") as mock_client:
        mock_client.return_value = MagicMock()
        _s3_client("https://s3.example", "id", "secret")
    kwargs = mock_client.call_args.kwargs
    assert kwargs["endpoint_url"] == "https://s3.example"
    assert kwargs["aws_access_key_id"] == "id"
    assert kwargs["aws_secret_access_key"] == "secret"
    assert kwargs["config"].s3["addressing_style"] == "path"
    assert kwargs["config"].request_checksum_calculation == "when_required"
    assert kwargs["config"].response_checksum_validation == "when_required"


def test_main_wires_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() reads PARAM_* env vars and calls run()."""
    creds = _creds(tmp_path)
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("PARAM_SIGNED_KMODS_PATH", "signed-kmods")
    monkeypatch.setenv("PARAM_S3_ENDPOINT", "https://s3.example")
    monkeypatch.setenv("PARAM_S3_BUCKET", "bucket")
    monkeypatch.setenv("PARAM_S3_CREDENTIALS_MOUNT", str(creds))
    with patch(f"{TASK}.run") as mock_run:
        assert main() == 0
    mock_run.assert_called_once_with(
        data_dir=tmp_path / "data",
        signed_kmods_path="signed-kmods",
        s3_endpoint="https://s3.example",
        s3_bucket="bucket",
        creds_mount=creds,
    )


def test_main_default_creds_mount(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() defaults the credentials mount to /var/secrets/s3."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SIGNED_KMODS_PATH", "signed-kmods")
    monkeypatch.setenv("PARAM_S3_ENDPOINT", "https://s3.example")
    monkeypatch.setenv("PARAM_S3_BUCKET", "bucket")
    monkeypatch.delenv("PARAM_S3_CREDENTIALS_MOUNT", raising=False)
    with patch(f"{TASK}.run") as mock_run:
        assert main() == 0
    assert mock_run.call_args.kwargs["creds_mount"] == DEFAULT_S3_CREDENTIALS_MOUNT


def test_main_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() exits when a required env var is missing."""
    monkeypatch.delenv("PARAM_DATA_DIR", raising=False)
    monkeypatch.delenv("PARAM_SIGNED_KMODS_PATH", raising=False)
    monkeypatch.delenv("PARAM_S3_ENDPOINT", raising=False)
    monkeypatch.delenv("PARAM_S3_BUCKET", raising=False)
    with pytest.raises(SystemExit):
        main()


def test_dunder_main_invokes_main() -> None:
    """Running the package as a module calls main()."""
    module = "release_service_utils.tasks.managed.push_oot_kmods_to_s3"
    with (
        patch(f"{TASK}.main", return_value=0) as mock_main,
        pytest.raises(SystemExit) as exc,
    ):
        runpy.run_module(module, run_name="__main__")
    assert exc.value.code == 0
    mock_main.assert_called_once_with()
