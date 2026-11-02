"""Tests for push_oot_kmods_to_azure."""

from __future__ import annotations

import runpy
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import ResourceExistsError

from release_service_utils.tasks.managed.push_oot_kmods_to_azure import (
    main,
    run,
    push_oot_kmods_to_azure as task_mod,
)

DEFAULT_AZURE_SP_MOUNT = task_mod.DEFAULT_AZURE_SP_MOUNT
TASK = task_mod.__name__


def _write_envfile(
    arch_dir: Path,
    *,
    vendor: str = "mocked-vendor-azure",
    version: str = "1.2.3-az",
    kernel: str = "6.5.0-az.x86_64",
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
    kernel: str = "6.5.0-az.x86_64",
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
    """Create a mock Azure service-principal mount and return it."""
    mount = tmp_path / "secrets"
    mount.mkdir()
    (mount / "AZURE_TENANT_ID").write_text("mock-tenant-id\n", encoding="utf-8")
    (mount / "AZURE_CLIENT_ID").write_text("mock-client-id\n", encoding="utf-8")
    (mount / "AZURE_CLIENT_SECRET").write_text("mock-client-secret\n", encoding="utf-8")
    return mount


def _uploaded_blobs(service: MagicMock) -> list[str]:
    """Return blob names passed to get_blob_client, in call order."""
    return [call.kwargs["blob"] for call in service.get_blob_client.call_args_list]


def _run_with_service(
    tmp_path: Path,
    data_dir: Path,
    service: MagicMock,
    *,
    signed_kmods_path: str = "signed-kmods",
    account: str = "mockstorageaccount",
    container: str = "mock-container",
) -> MagicMock:
    """Call run() with patched Azure clients and return the BlobServiceClient mock."""
    with (
        patch(f"{TASK}.ClientSecretCredential") as mock_cred,
        patch(f"{TASK}.BlobServiceClient", return_value=service) as mock_svc,
    ):
        run(
            data_dir=data_dir,
            signed_kmods_path=signed_kmods_path,
            azure_storage_account=account,
            azure_container=container,
            creds_mount=_creds(tmp_path),
        )
    mock_cred.assert_called_once_with(
        "mock-tenant-id",
        "mock-client-id",
        "mock-client-secret",
    )
    mock_svc.assert_called_once()
    assert (
        mock_svc.call_args.kwargs["account_url"] == f"https://{account}.blob.core.windows.net"
    )
    return mock_svc


def test_run_single_arch_uploads(tmp_path: Path) -> None:
    """Single-arch upload strips the kernel suffix and preserves .ko paths."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir)
    service = MagicMock()

    _run_with_service(tmp_path, data_dir, service)

    service.create_container.assert_called_once_with("mock-container")
    blobs = _uploaded_blobs(service)
    prefix = "mocked-vendor-azure/1.2.3-az/6.5.0-az/x86_64/"
    assert prefix + "mod1.ko" in blobs
    assert prefix + "driver_001/subdir/nested.ko" in blobs
    assert prefix + "signed_kmods_checksums_x86_64.txt" in blobs
    assert prefix + "envfile" in blobs
    assert all("6.5.0-az.x86_64" not in blob for blob in blobs)
    for call in service.get_blob_client.call_args_list:
        assert call.kwargs["container"] == "mock-container"
    blob_client = service.get_blob_client.return_value
    assert blob_client.upload_blob.call_count == len(blobs)
    for call in blob_client.upload_blob.call_args_list:
        assert call.kwargs["overwrite"] is True


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

    service = MagicMock()
    _run_with_service(tmp_path, data_dir, service)

    blobs = _uploaded_blobs(service)
    assert any(blob.endswith("mod1.ko") for blob in blobs)


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

    service = MagicMock()
    _run_with_service(tmp_path, data_dir, service, signed_kmods_path=dest_name)

    blobs = _uploaded_blobs(service)
    assert any(blob.endswith("mod1.ko") for blob in blobs)


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

    service = MagicMock()
    _run_with_service(tmp_path, data_dir, service, signed_kmods_path=dest_name)

    blobs = _uploaded_blobs(service)
    assert any(blob.endswith("mod1.ko") for blob in blobs)


def test_run_uses_envfile_arch(tmp_path: Path) -> None:
    """ARCH from envfile is used in the destination prefix."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir, arch="amd64")
    service = MagicMock()

    _run_with_service(tmp_path, data_dir, service)

    blobs = _uploaded_blobs(service)
    assert any("/amd64/mod1.ko" in blob for blob in blobs)
    assert all("/x86_64/" not in blob for blob in blobs)


def test_run_multi_platform_uses_directory_name(tmp_path: Path) -> None:
    """ARCH=MULTI_PLATFORM keeps the architecture directory name."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir, arch="MULTI_PLATFORM")
    service = MagicMock()

    _run_with_service(tmp_path, data_dir, service)

    blobs = _uploaded_blobs(service)
    assert any("/x86_64/mod1.ko" in blob for blob in blobs)


def test_run_skips_missing_checksum(tmp_path: Path) -> None:
    """Checksum upload is skipped when the file is absent."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir, with_checksum=False)
    service = MagicMock()

    _run_with_service(tmp_path, data_dir, service)

    blobs = _uploaded_blobs(service)
    assert not any("checksums" in blob for blob in blobs)
    assert any(blob.endswith("/envfile") for blob in blobs)


def test_run_container_already_exists(tmp_path: Path) -> None:
    """An existing container does not fail the upload."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir)
    service = MagicMock()
    service.create_container.side_effect = ResourceExistsError("exists")

    _run_with_service(tmp_path, data_dir, service)

    blobs = _uploaded_blobs(service)
    assert any(blob.endswith("mod1.ko") for blob in blobs)


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
    _write_envfile(x86, kernel="6.5.0-az.x86_64")
    _write_envfile(arm, kernel="6.5.0-az.aarch64", vendor="mocked-vendor-azure")
    (signed / "signing_summary.txt").write_text("sign", encoding="utf-8")
    (signed / "extraction_summary.txt").write_text("extract", encoding="utf-8")
    service = MagicMock()

    _run_with_service(tmp_path, data_dir, service)

    blobs = _uploaded_blobs(service)
    summary_prefix = "mocked-vendor-azure/1.2.3-az/6.5.0-az/multi-arch-summary/"
    assert summary_prefix + "signing_summary.txt" in blobs
    assert summary_prefix + "extraction_summary.txt" in blobs
    assert any(blob.endswith("/x86_64/a.ko") for blob in blobs)
    assert any(blob.endswith("/aarch64/b.ko") for blob in blobs)


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
    service = MagicMock()

    _run_with_service(tmp_path, data_dir, service)

    blobs = _uploaded_blobs(service)
    assert any(blob.endswith("signing_summary.txt") for blob in blobs)
    assert not any(blob.endswith("extraction_summary.txt") for blob in blobs)


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
    service = MagicMock()

    _run_with_service(tmp_path, data_dir, service)

    blobs = _uploaded_blobs(service)
    assert not any("multi-arch-summary" in blob for blob in blobs)


def test_run_no_ko_files_raises(tmp_path: Path) -> None:
    """An architecture directory with no .ko files fails."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir, with_ko=False)
    with (
        patch(f"{TASK}.ClientSecretCredential"),
        patch(f"{TASK}.BlobServiceClient", return_value=MagicMock()),
    ):
        with pytest.raises(FileNotFoundError, match="No .ko files"):
            run(
                data_dir=data_dir,
                signed_kmods_path="signed-kmods",
                azure_storage_account="mockstorageaccount",
                azure_container="mock-container",
                creds_mount=_creds(tmp_path),
            )


def test_run_missing_envfile_raises(tmp_path: Path) -> None:
    """A missing envfile fails the upload."""
    data_dir = tmp_path / "data"
    arch_dir = data_dir / "signed-kmods" / "x86_64"
    arch_dir.mkdir(parents=True)
    (arch_dir / "mod1.ko").write_text("mod1", encoding="utf-8")
    with (
        patch(f"{TASK}.ClientSecretCredential"),
        patch(f"{TASK}.BlobServiceClient", return_value=MagicMock()),
    ):
        with pytest.raises(FileNotFoundError, match="envfile not found"):
            run(
                data_dir=data_dir,
                signed_kmods_path="signed-kmods",
                azure_storage_account="mockstorageaccount",
                azure_container="mock-container",
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
            azure_storage_account="mockstorageaccount",
            azure_container="mock-container",
            creds_mount=_creds(tmp_path),
        )


def test_run_upload_failure_propagates(tmp_path: Path) -> None:
    """Azure upload errors are not swallowed."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _single_arch_tree(data_dir)
    service = MagicMock()
    service.get_blob_client.return_value.upload_blob.side_effect = RuntimeError("boom")
    with (
        patch(f"{TASK}.ClientSecretCredential"),
        patch(f"{TASK}.BlobServiceClient", return_value=service),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            run(
                data_dir=data_dir,
                signed_kmods_path="signed-kmods",
                azure_storage_account="mockstorageaccount",
                azure_container="mock-container",
                creds_mount=_creds(tmp_path),
            )


def test_blob_service_builds_client() -> None:
    """_blob_service passes account URL and service-principal credentials."""
    credential = MagicMock()
    with (
        patch(f"{TASK}.ClientSecretCredential", return_value=credential) as mock_cred,
        patch(f"{TASK}.BlobServiceClient") as mock_svc,
    ):
        mock_svc.return_value = MagicMock()
        task_mod._blob_service("acct", "tenant", "client", "secret")
    mock_cred.assert_called_once_with("tenant", "client", "secret")
    kwargs = mock_svc.call_args.kwargs
    assert kwargs["account_url"] == "https://acct.blob.core.windows.net"
    assert kwargs["credential"] is credential


def test_main_wires_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() reads PARAM_* env vars and calls run()."""
    creds = _creds(tmp_path)
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("PARAM_SIGNED_KMODS_PATH", "signed-kmods")
    monkeypatch.setenv("PARAM_AZURE_STORAGE_ACCOUNT", "mockstorageaccount")
    monkeypatch.setenv("PARAM_AZURE_CONTAINER", "mock-container")
    monkeypatch.setenv("PARAM_AZURE_SP_MOUNT", str(creds))
    with patch(f"{TASK}.run") as mock_run:
        assert main() == 0
    mock_run.assert_called_once_with(
        data_dir=tmp_path / "data",
        signed_kmods_path="signed-kmods",
        azure_storage_account="mockstorageaccount",
        azure_container="mock-container",
        creds_mount=creds,
    )


def test_main_default_creds_mount(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() defaults the credentials mount to /var/secrets/azure-sp."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SIGNED_KMODS_PATH", "signed-kmods")
    monkeypatch.setenv("PARAM_AZURE_STORAGE_ACCOUNT", "mockstorageaccount")
    monkeypatch.setenv("PARAM_AZURE_CONTAINER", "mock-container")
    monkeypatch.delenv("PARAM_AZURE_SP_MOUNT", raising=False)
    with patch(f"{TASK}.run") as mock_run:
        assert main() == 0
    assert mock_run.call_args.kwargs["creds_mount"] == DEFAULT_AZURE_SP_MOUNT


def test_main_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() exits when a required env var is missing."""
    monkeypatch.delenv("PARAM_DATA_DIR", raising=False)
    monkeypatch.delenv("PARAM_SIGNED_KMODS_PATH", raising=False)
    monkeypatch.delenv("PARAM_AZURE_STORAGE_ACCOUNT", raising=False)
    monkeypatch.delenv("PARAM_AZURE_CONTAINER", raising=False)
    with pytest.raises(SystemExit):
        main()


def test_dunder_main_invokes_main() -> None:
    """Running the package as a module calls main()."""
    module = "release_service_utils.tasks.managed.push_oot_kmods_to_azure"
    with (
        patch(f"{TASK}.main", return_value=0) as mock_main,
        pytest.raises(SystemExit) as exc,
    ):
        runpy.run_module(module, run_name="__main__")
    assert exc.value.code == 0
    mock_main.assert_called_once_with()
