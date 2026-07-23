"""Tests for marketplacesvm_push_disk_images."""

from __future__ import annotations

import gzip
import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Any
from unittest import mock

import marketplacesvm_push_disk_images as m
import pytest
import yaml


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _no_wait(*_args: Any, **_kwargs: Any) -> None:
    """No-op stand-in for memory_throttle.wait_for_memory in tests."""
    return None


def _valid_credential() -> dict[str, Any]:
    return {"marketplace_account": "aws-na", "auth": {"token": "secret"}}


def _valid_component(
    *,
    name: str = "amd-bootc-1-3-raw-disk-image",
    filename: str = "test-product-amd-1.3-1732045201-x86_64.raw.gz",
    source: str = "disk.raw.gz",
    file_prefix: str = "test-product-amd-1.3",
) -> dict[str, Any]:
    return {
        "containerImage": "quay.io/org/image@sha256:abc",
        "name": name,
        "productInfo": {
            "filePrefix": file_prefix,
            "productCode": "TEST",
            "productName": "Test Product",
            "productVersionName": "1.3",
        },
        "staged": {
            "destination": "dest",
            "files": [{"filename": filename, "source": source}],
            "version": "1.3",
        },
        "starmap": [{"cloud": "aws", "name": "test-product-amd"}],
    }


def _valid_snapshot(components: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "application": "app",
        "components": components if components is not None else [_valid_component()],
    }


# --- parse_args ---


def test_parse_args_defaults() -> None:
    """Required flags parse; optional flags use defaults."""
    args = m.parse_args(["--data-dir", "/data", "--snapshot-path", "snap.json"])
    assert args.data_dir == "/data"
    assert args.snapshot_path == "snap.json"
    assert args.pre_push == "false"
    assert args.concurrent_limit == 3
    assert args.secrets_dir == str(m.DEFAULT_SECRETS_DIR)
    assert args.workdir == str(m.DEFAULT_WORKDIR)


def test_parse_args_custom_values() -> None:
    """Custom optional flags are accepted."""
    args = m.parse_args(
        [
            "--data-dir",
            "/data",
            "--snapshot-path",
            "snap.json",
            "--pre-push",
            "true",
            "--concurrent-limit",
            "5",
            "--secrets-dir",
            "/secrets",
            "--workdir",
            "/work",
        ]
    )
    assert args.pre_push == "true"
    assert args.concurrent_limit == 5
    assert args.secrets_dir == "/secrets"
    assert args.workdir == "/work"


# --- require_field ---


def test_require_field_nested_ok() -> None:
    """Nested keys resolve to the leaf value."""
    assert m.require_field({"a": {"b": "x"}}, "a", "b") == "x"


def test_require_field_missing() -> None:
    """Missing nested keys raise ValueError with a path."""
    with pytest.raises(ValueError, match="Missing productInfo.filePrefix"):
        m.require_field({"productInfo": {}}, "productInfo", "filePrefix")


def test_require_field_empty() -> None:
    """Empty string values raise ValueError."""
    with pytest.raises(ValueError, match="Missing name"):
        m.require_field({"name": ""}, "name")


# --- log_command_failure ---


def test_log_command_failure_logs_stdout_and_stderr(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Captured stdout and stderr are both logged for a CalledProcessError."""
    exc = subprocess.CalledProcessError(
        1, ["cmd"], output="captured stdout", stderr="captured stderr"
    )
    with caplog.at_level(logging.ERROR, logger="release"):
        m.log_command_failure(exc)
    assert "captured stdout" in caplog.text
    assert "captured stderr" in caplog.text


def test_log_command_failure_non_called_process_error_is_noop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Non-CalledProcessError exceptions produce no log output."""
    with caplog.at_level(logging.ERROR, logger="release"):
        m.log_command_failure(ValueError("boom"))
    assert caplog.text == ""


# --- validate_credentials / set_cloud_credentials ---


def test_validate_credentials_happy_path(tmp_path: Path) -> None:
    """Valid credential JSON files are returned sorted."""
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    _write_json(secrets / "b.json", _valid_credential())
    _write_json(secrets / "a.json", _valid_credential())

    files = m.validate_credentials(secrets)
    assert [p.name for p in files] == ["a.json", "b.json"]


def test_validate_credentials_none_found(tmp_path: Path) -> None:
    """RuntimeError when the secrets directory has no JSON files."""
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    with pytest.raises(RuntimeError, match="No credential files found"):
        m.validate_credentials(secrets)


def test_validate_credentials_missing_keys(tmp_path: Path) -> None:
    """RuntimeError when marketplace_account or auth is missing."""
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    _write_json(secrets / "bad.json", {"marketplace_account": "x"})
    with pytest.raises(RuntimeError, match="Validation failed"):
        m.validate_credentials(secrets)


def test_validate_credentials_invalid_json(tmp_path: Path) -> None:
    """RuntimeError when a credential file is not valid JSON."""
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    (secrets / "bad.json").write_text("{not-json", encoding="utf-8")
    with pytest.raises(RuntimeError, match="Validation failed"):
        m.validate_credentials(secrets)


def test_validate_credentials_non_object_json(tmp_path: Path) -> None:
    """RuntimeError when credential JSON root is not an object."""
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    (secrets / "bad.json").write_text('["not", "an", "object"]', encoding="utf-8")
    with pytest.raises(RuntimeError, match="Validation failed"):
        m.validate_credentials(secrets)


def test_set_cloud_credentials(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """CLOUD_CREDENTIALS is set to a comma-separated path list."""
    monkeypatch.delenv("CLOUD_CREDENTIALS", raising=False)
    paths = [tmp_path / "a.json", tmp_path / "b.json"]
    value = m.set_cloud_credentials(paths)
    assert value == f"{paths[0]},{paths[1]}"
    assert os.environ["CLOUD_CREDENTIALS"] == value


# --- write_starmap_file ---


def test_write_starmap_file_flattens_components(tmp_path: Path) -> None:
    """Starmap entries from all components are flattened into one YAML list."""
    snapshot_file = tmp_path / "mapped" / "snapshot.json"
    snapshot = _valid_snapshot(
        [
            _valid_component(name="c1"),
            {
                **_valid_component(name="c2"),
                "starmap": [{"cloud": "azure", "name": "other"}],
            },
        ]
    )
    _write_json(snapshot_file, snapshot)

    out = m.write_starmap_file(snapshot, snapshot_file)
    assert out == snapshot_file.parent / "starmap.yaml"
    loaded = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert loaded == [
        {"cloud": "aws", "name": "test-product-amd"},
        {"cloud": "azure", "name": "other"},
    ]


def test_write_starmap_file_skips_non_mapping_components(tmp_path: Path) -> None:
    """Non-object entries in components are skipped rather than erroring."""
    snapshot_file = tmp_path / "mapped" / "snapshot.json"
    snapshot = _valid_snapshot([_valid_component(name="c1"), "not-a-mapping"])
    _write_json(snapshot_file, snapshot)

    out = m.write_starmap_file(snapshot, snapshot_file)
    loaded = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert loaded == [{"cloud": "aws", "name": "test-product-amd"}]


# --- filename / metadata parsers ---


def test_strip_extensions() -> None:
    """Compound extensions are stripped in one pass."""
    assert (
        m.strip_extensions("test-product-amd-1.3-1732045201-x86_64.raw.gz")
        == "test-product-amd-1.3-1732045201-x86_64"
    )


def test_parse_build_respin() -> None:
    """Respin is the timestamp between version and architecture."""
    assert (
        m.parse_build_respin("test-product-amd-1.3-1732045201-x86_64.raw.gz") == "1732045201"
    )


def test_parse_build_name() -> None:
    """Build name drops the trailing version segment from filePrefix."""
    assert m.parse_build_name("test-product-amd-1.3") == "test-product-amd"


def test_parse_architecture() -> None:
    """Architecture is the final hyphen segment before the extension."""
    assert m.parse_architecture("test-product-amd-1.3-1732045201-x86_64.raw") == "x86_64"


def test_image_type_for_filename() -> None:
    """VHD and AMI types are detected; others return None."""
    assert m.image_type_for_filename("disk.vhd") == "VHD"
    assert m.image_type_for_filename("disk.raw") == "AMI"
    assert m.image_type_for_filename("disk.qcow2") is None


def test_build_date_from_respin() -> None:
    """Unix timestamp respin converts to YYYYMMDD UTC."""
    assert m.build_date_from_respin("1732045201") == "20241119"


# --- decompress_gzip_source ---


def test_decompress_gzip_source_decompresses(tmp_path: Path) -> None:
    """Gzipped sources are decompressed and the .gz file is removed."""
    gz_path = tmp_path / "disk.raw.gz"
    with gzip.open(gz_path, "wb") as handle:
        handle.write(b"disk-bytes")

    out = m.decompress_gzip_source(gz_path)
    assert out == tmp_path / "disk.raw"
    assert out.read_bytes() == b"disk-bytes"
    assert not gz_path.exists()


def test_decompress_gzip_source_noop(tmp_path: Path) -> None:
    """Non-gzip paths are returned unchanged."""
    path = tmp_path / "disk.raw"
    path.write_bytes(b"x")
    assert m.decompress_gzip_source(path) == path


# --- write_resources_yaml ---


def test_write_resources_yaml(tmp_path: Path) -> None:
    """resources.yaml is written under the destination directory."""
    resources = {"api": "v1", "type": "AMI", "images": []}
    m.write_resources_yaml(tmp_path, resources)
    loaded = yaml.safe_load((tmp_path / "resources.yaml").read_text(encoding="utf-8"))
    assert loaded["type"] == "AMI"


# --- prepare_component ---


def test_prepare_component_raw_image(tmp_path: Path) -> None:
    """Raw gzipped disk images are staged with AMI resources metadata."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    disk_imgs = tmp_path / "starmap" / "CLOUD_IMAGES"
    component = _valid_component()

    def fake_oras_pull(pullspec: str, download_dir: Path, **_kwargs: Any) -> None:
        assert pullspec == component["containerImage"]
        gz_path = download_dir / "disk.raw.gz"
        with gzip.open(gz_path, "wb") as handle:
            handle.write(b"raw-content")

    m.prepare_component(
        component, disk_imgs, workdir, oras_pull=fake_oras_pull, wait_for_memory=_no_wait
    )

    dest = disk_imgs / component["name"]
    staged = dest / "test-product-amd-1.3-1732045201-x86_64.raw"
    assert staged.read_bytes() == b"raw-content"
    resources = yaml.safe_load((dest / "resources.yaml").read_text(encoding="utf-8"))
    assert resources["type"] == "AMI"
    assert resources["build"]["name"] == "test-product-amd"
    assert resources["build"]["respin"] == "1732045201"
    assert resources["build"]["version"] == "1.3"
    assert resources["release"]["date"] == "20241119"
    assert resources["images"] == [
        {
            "path": "test-product-amd-1.3-1732045201-x86_64.raw",
            "architecture": "x86_64",
        }
    ]


def test_prepare_component_vhd_image(tmp_path: Path) -> None:
    """VHD gzipped disk images are staged with VHD resources metadata."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    disk_imgs = tmp_path / "starmap" / "CLOUD_IMAGES"
    component = _valid_component(
        name="azure-disk",
        filename="test-product-amd-1.3-1732045201-x86_64.vhd.gz",
        source="disk.vhd.gz",
    )

    def fake_oras_pull(_pullspec: str, download_dir: Path, **_kwargs: Any) -> None:
        gz_path = download_dir / "disk.vhd.gz"
        with gzip.open(gz_path, "wb") as handle:
            handle.write(b"vhd-content")

    m.prepare_component(
        component, disk_imgs, workdir, oras_pull=fake_oras_pull, wait_for_memory=_no_wait
    )

    dest = disk_imgs / "azure-disk"
    assert (dest / "test-product-amd-1.3-1732045201-x86_64.vhd").is_file()
    resources = yaml.safe_load((dest / "resources.yaml").read_text(encoding="utf-8"))
    assert resources["type"] == "VHD"


def test_prepare_component_missing_source(tmp_path: Path) -> None:
    """RuntimeError when the mapped source file is absent after oras pull."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    disk_imgs = tmp_path / "starmap" / "CLOUD_IMAGES"

    def fake_oras_pull(_pullspec: str, _download_dir: Path, **_kwargs: Any) -> None:
        return None

    with pytest.raises(RuntimeError, match="was not found after oras pull"):
        m.prepare_component(
            _valid_component(),
            disk_imgs,
            workdir,
            oras_pull=fake_oras_pull,
            wait_for_memory=_no_wait,
        )


def test_prepare_component_duplicate_destination(tmp_path: Path) -> None:
    """RuntimeError when two staged files share the same destination name."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    disk_imgs = tmp_path / "starmap" / "CLOUD_IMAGES"
    component = _valid_component()
    component["staged"]["files"] = [
        {
            "filename": "test-product-amd-1.3-1732045201-x86_64.raw.gz",
            "source": "disk1.raw.gz",
        },
        {
            "filename": "test-product-amd-1.3-1732045201-x86_64.raw.gz",
            "source": "disk2.raw.gz",
        },
    ]

    def fake_oras_pull(_pullspec: str, download_dir: Path, **_kwargs: Any) -> None:
        for name in ("disk1.raw.gz", "disk2.raw.gz"):
            with gzip.open(download_dir / name, "wb") as handle:
                handle.write(b"x")

    with pytest.raises(RuntimeError, match="Multiple files use the same destination"):
        m.prepare_component(
            component, disk_imgs, workdir, oras_pull=fake_oras_pull, wait_for_memory=_no_wait
        )


def test_prepare_component_empty_staged_files(tmp_path: Path) -> None:
    """ValueError when staged.files is an empty list."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    disk_imgs = tmp_path / "starmap" / "CLOUD_IMAGES"
    component = _valid_component()
    component["staged"]["files"] = []

    with pytest.raises(ValueError, match="staged.files must be a non-empty list"):
        m.prepare_component(
            component,
            disk_imgs,
            workdir,
            oras_pull=lambda *_a, **_k: None,
            wait_for_memory=_no_wait,
        )


def test_prepare_component_non_mapping_staged_file_entry(tmp_path: Path) -> None:
    """ValueError when a staged.files entry is not an object."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    disk_imgs = tmp_path / "starmap" / "CLOUD_IMAGES"
    component = _valid_component()
    component["staged"]["files"] = [
        {"filename": "test-product-amd-1.3-1732045201-x86_64.raw.gz", "source": "disk.raw.gz"},
        "not-a-mapping",
    ]

    def fake_oras_pull(_pullspec: str, download_dir: Path, **_kwargs: Any) -> None:
        with gzip.open(download_dir / "disk.raw.gz", "wb") as handle:
            handle.write(b"raw-content")

    with pytest.raises(ValueError, match="staged.files entries must be objects"):
        m.prepare_component(
            component, disk_imgs, workdir, oras_pull=fake_oras_pull, wait_for_memory=_no_wait
        )


def test_prepare_component_skips_unsupported(tmp_path: Path) -> None:
    """Unsupported extensions are skipped and omitted from resources.yaml."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    disk_imgs = tmp_path / "starmap" / "CLOUD_IMAGES"
    component = _valid_component(
        filename="test-product-amd-1.3-1732045201-x86_64.qcow2",
        source="disk.qcow2",
    )

    def fake_oras_pull(_pullspec: str, download_dir: Path, **_kwargs: Any) -> None:
        (download_dir / "disk.qcow2").write_bytes(b"qcow")

    m.prepare_component(
        component, disk_imgs, workdir, oras_pull=fake_oras_pull, wait_for_memory=_no_wait
    )
    resources = yaml.safe_load(
        (disk_imgs / component["name"] / "resources.yaml").read_text(encoding="utf-8")
    )
    assert resources["images"] == []
    assert "type" not in resources


# --- prepare_components ---


def test_prepare_components_aggregates_failures(tmp_path: Path) -> None:
    """One failing component fails the whole prepare_components call."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    disk_imgs = tmp_path / "imgs"

    def boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("pull failed")

    with pytest.raises(RuntimeError, match="prepare_component failed for at least"):
        m.prepare_components(
            [_valid_component()],
            disk_imgs,
            workdir,
            concurrent_limit=1,
            oras_pull=boom,
            wait_for_memory=_no_wait,
        )


def test_prepare_components_success(tmp_path: Path) -> None:
    """All components succeed when oras_pull stages expected files."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    disk_imgs = tmp_path / "imgs"

    def fake_oras_pull(_pullspec: str, download_dir: Path, **_kwargs: Any) -> None:
        with gzip.open(download_dir / "disk.raw.gz", "wb") as handle:
            handle.write(b"ok")

    m.prepare_components(
        [_valid_component(name="one"), _valid_component(name="two")],
        disk_imgs,
        workdir,
        concurrent_limit=2,
        oras_pull=fake_oras_pull,
        wait_for_memory=_no_wait,
    )
    assert (disk_imgs / "one" / "resources.yaml").is_file()
    assert (disk_imgs / "two" / "resources.yaml").is_file()


def test_prepare_components_throttles_memory_per_component(tmp_path: Path) -> None:
    """wait_for_memory is called once per component before its heavy work."""
    workdir = tmp_path / "work"
    workdir.mkdir()
    disk_imgs = tmp_path / "imgs"
    calls: list[int] = []

    def fake_oras_pull(_pullspec: str, download_dir: Path, **_kwargs: Any) -> None:
        with gzip.open(download_dir / "disk.raw.gz", "wb") as handle:
            handle.write(b"ok")

    def counting_wait(threshold: int) -> None:
        calls.append(threshold)

    m.prepare_components(
        [_valid_component(name="one"), _valid_component(name="two")],
        disk_imgs,
        workdir,
        concurrent_limit=2,
        oras_pull=fake_oras_pull,
        wait_for_memory=counting_wait,
    )
    assert calls == [m.MEMORY_THRESHOLD, m.MEMORY_THRESHOLD]


# --- validate_staged_structure / run_marketplacesvm_push / copy_artifacts ---


def test_validate_staged_structure_calls_pushsource_ls(tmp_path: Path) -> None:
    """pushsource-ls is invoked with the staged: URL."""
    calls: list[list[str]] = []

    def fake_run_cmd(cmd: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append([str(x) for x in cmd])
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    m.validate_staged_structure(tmp_path, run_cmd=fake_run_cmd)
    assert calls == [["pushsource-ls", f"staged:{tmp_path}"]]


def test_validate_staged_structure_logs_and_reraises_on_failure(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A pushsource-ls failure is logged (with stdout/stderr) and re-raised."""

    def failing_run_cmd(cmd: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(
            1, cmd, output="pushsource-ls stdout", stderr="pushsource-ls stderr"
        )

    with caplog.at_level(logging.ERROR, logger="release"):
        with pytest.raises(subprocess.CalledProcessError):
            m.validate_staged_structure(tmp_path, run_cmd=failing_run_cmd)
    assert "pushsource-ls stdout" in caplog.text
    assert "pushsource-ls stderr" in caplog.text


def test_run_marketplacesvm_push_with_pre_push(tmp_path: Path) -> None:
    """pre_push adds --nochannel to the wrapper invocation."""
    starmap = tmp_path / "starmap.yaml"
    starmap.write_text("[]\n", encoding="utf-8")
    calls: list[list[str]] = []

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append([str(x) for x in cmd])
        assert kwargs["cwd"] == tmp_path
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    m.run_marketplacesvm_push(
        tmp_path,
        starmap,
        pre_push=True,
        run_cmd=fake_run_cmd,
    )
    assert calls[0][:3] == ["marketplacesvm_push_wrapper", "--debug", "--nochannel"]
    assert "--source" in calls[0]
    assert "--starmap-file" in calls[0]


def test_run_marketplacesvm_push_without_pre_push(tmp_path: Path) -> None:
    """Without pre_push, --nochannel is omitted."""
    starmap = tmp_path / "starmap.yaml"
    starmap.write_text("[]\n", encoding="utf-8")
    calls: list[list[str]] = []

    def fake_run_cmd(cmd: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append([str(x) for x in cmd])
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    m.run_marketplacesvm_push(
        tmp_path,
        starmap,
        pre_push=False,
        run_cmd=fake_run_cmd,
    )
    assert "--nochannel" not in calls[0]


def test_run_marketplacesvm_push_logs_and_reraises_on_failure(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A wrapper failure is logged (with stdout/stderr) and re-raised."""
    starmap = tmp_path / "starmap.yaml"
    starmap.write_text("[]\n", encoding="utf-8")

    def failing_run_cmd(cmd: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(
            1, cmd, output="wrapper stdout", stderr="wrapper stderr"
        )

    with caplog.at_level(logging.ERROR, logger="release"):
        with pytest.raises(subprocess.CalledProcessError):
            m.run_marketplacesvm_push(
                tmp_path, starmap, pre_push=False, run_cmd=failing_run_cmd
            )
    assert "wrapper stdout" in caplog.text
    assert "wrapper stderr" in caplog.text


def test_copy_artifacts(tmp_path: Path) -> None:
    """Artifacts directory is copied into data_dir when present."""
    base = tmp_path / "base"
    data = tmp_path / "data"
    data.mkdir()
    artifacts = base / "artifacts" / "run1"
    artifacts.mkdir(parents=True)
    (artifacts / "clouds.json").write_text("{}", encoding="utf-8")

    m.copy_artifacts(base, data)
    assert (data / "artifacts" / "run1" / "clouds.json").read_text(encoding="utf-8") == "{}"


def test_copy_artifacts_noop_when_missing(tmp_path: Path) -> None:
    """Missing artifacts directory is a no-op."""
    data = tmp_path / "data"
    data.mkdir()
    m.copy_artifacts(tmp_path / "base", data)
    assert not (data / "artifacts").exists()


# --- run / main ---


def test_run_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end run stages content, validates, pushes, and copies artifacts."""
    data_dir = tmp_path / "data"
    secrets = tmp_path / "secrets"
    workdir = tmp_path / "work"
    secrets.mkdir()
    workdir.mkdir()
    _write_json(secrets / "creds.json", _valid_credential())
    _write_json(data_dir / "snapshot.json", _valid_snapshot())

    def fake_oras_pull(_pullspec: str, download_dir: Path, **_kwargs: Any) -> None:
        with gzip.open(download_dir / "disk.raw.gz", "wb") as handle:
            handle.write(b"raw")

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "marketplacesvm_push_wrapper":
            cwd = Path(kwargs["cwd"])
            artifact_dir = cwd / "artifacts" / "20260430181240"
            artifact_dir.mkdir(parents=True)
            (artifact_dir / "clouds.json").write_text("{}", encoding="utf-8")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.delenv("CLOUD_CREDENTIALS", raising=False)
    rc = m.run(
        data_dir=data_dir,
        snapshot_path="snapshot.json",
        pre_push=False,
        concurrent_limit=1,
        secrets_dir=secrets,
        workdir=workdir,
        oras_pull=fake_oras_pull,
        run_cmd=fake_run_cmd,
        wait_for_memory=_no_wait,
    )
    assert rc == 0
    assert (data_dir / "starmap.yaml").is_file()
    assert (data_dir / "artifacts" / "20260430181240" / "clouds.json").is_file()
    assert "creds.json" in os.environ["CLOUD_CREDENTIALS"]


def test_run_pre_push_true(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """pre_push=True forwards --nochannel to the wrapper."""
    data_dir = tmp_path / "data"
    secrets = tmp_path / "secrets"
    workdir = tmp_path / "work"
    secrets.mkdir()
    workdir.mkdir()
    _write_json(secrets / "creds.json", _valid_credential())
    _write_json(data_dir / "snapshot.json", _valid_snapshot())
    seen: list[list[str]] = []

    def fake_oras_pull(_pullspec: str, download_dir: Path, **_kwargs: Any) -> None:
        with gzip.open(download_dir / "disk.raw.gz", "wb") as handle:
            handle.write(b"raw")

    def fake_run_cmd(cmd: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        seen.append([str(x) for x in cmd])
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.delenv("CLOUD_CREDENTIALS", raising=False)
    m.run(
        data_dir=data_dir,
        snapshot_path="snapshot.json",
        pre_push=True,
        concurrent_limit=1,
        secrets_dir=secrets,
        workdir=workdir,
        oras_pull=fake_oras_pull,
        run_cmd=fake_run_cmd,
        wait_for_memory=_no_wait,
    )
    wrapper_calls = [c for c in seen if c and c[0] == "marketplacesvm_push_wrapper"]
    assert any("--nochannel" in c for c in wrapper_calls)


def test_run_empty_components(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """ValueError when snapshot has no components."""
    data_dir = tmp_path / "data"
    secrets = tmp_path / "secrets"
    workdir = tmp_path / "work"
    secrets.mkdir()
    workdir.mkdir()
    _write_json(secrets / "creds.json", _valid_credential())
    _write_json(data_dir / "snapshot.json", {"components": []})
    monkeypatch.delenv("CLOUD_CREDENTIALS", raising=False)

    with pytest.raises(ValueError, match="non-empty components"):
        m.run(
            data_dir=data_dir,
            snapshot_path="snapshot.json",
            pre_push=False,
            concurrent_limit=1,
            secrets_dir=secrets,
            workdir=workdir,
            oras_pull=lambda *_a, **_k: None,
            run_cmd=lambda *_a, **_k: subprocess.CompletedProcess([], 0, "", ""),
            wait_for_memory=_no_wait,
        )


def test_run_non_object_component(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """ValueError when a components entry is not a JSON object."""
    data_dir = tmp_path / "data"
    secrets = tmp_path / "secrets"
    workdir = tmp_path / "work"
    secrets.mkdir()
    workdir.mkdir()
    _write_json(secrets / "creds.json", _valid_credential())
    _write_json(
        data_dir / "snapshot.json", {"components": [_valid_component(), "not-an-object"]}
    )
    monkeypatch.delenv("CLOUD_CREDENTIALS", raising=False)

    with pytest.raises(ValueError, match="must all be JSON objects"):
        m.run(
            data_dir=data_dir,
            snapshot_path="snapshot.json",
            pre_push=False,
            concurrent_limit=1,
            secrets_dir=secrets,
            workdir=workdir,
            oras_pull=lambda *_a, **_k: None,
            run_cmd=lambda *_a, **_k: subprocess.CompletedProcess([], 0, "", ""),
            wait_for_memory=_no_wait,
        )


def test_main_wires_args(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() parses argv and delegates to run()."""
    monkeypatch.setattr(
        m,
        "run",
        mock.Mock(return_value=0),
    )
    rc = m.main(
        [
            "--data-dir",
            str(tmp_path),
            "--snapshot-path",
            "snap.json",
            "--pre-push",
            "true",
            "--concurrent-limit",
            "2",
            "--secrets-dir",
            "/secrets",
            "--workdir",
            "/work",
        ]
    )
    assert rc == 0
    m.run.assert_called_once_with(
        data_dir=tmp_path,
        snapshot_path="snap.json",
        pre_push=True,
        concurrent_limit=2,
        secrets_dir=Path("/secrets"),
        workdir=Path("/work"),
    )
