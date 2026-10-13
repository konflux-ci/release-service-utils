"""Tests for upload_src_rpm_sbom_attestation."""

from __future__ import annotations

import json
import runpy
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from release_service_utils.helpers.pulp_client import PulpDigestStatus
from release_service_utils.tasks.managed.upload_src_rpm_sbom_attestation import (
    upload_src_rpm_sbom_attestation as task_mod,
    main,
    run,
)

TASK = task_mod.__name__
_IMAGE = "quay.io/test/happypath@sha256:12345"


def _snapshot(
    components: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return a snapshot document with the given components."""
    return {"application": "rpms", "components": components if components is not None else []}


def _src_rpm() -> dict[str, Any]:
    """Return a source RPM entry."""
    return {"rpm": "hello-2.12.1-6.fc44.src.rpm", "arch": "src"}


def _binary_rpm() -> dict[str, Any]:
    """Return a binary RPM entry."""
    return {"rpm": "hello-2.12.1-6.fc44.x86_64.rpm", "arch": "x86_64"}


def _component(
    *,
    name: str = "test",
    image: str | None = _IMAGE,
    rpms: list[Any] | None = None,
) -> dict[str, Any]:
    """Return a snapshot component."""
    component: dict[str, Any] = {"name": name}
    if image is not None:
        component["containerImage"] = image
    component["rpmsToPublish"] = [_src_rpm(), _binary_rpm()] if rpms is None else rpms
    return component


def _write_inputs(
    tmp_path: Path,
    snapshot: dict[str, Any],
    *,
    secret: str = 'base_url = "https://pulp.example.com"\n',
) -> tuple[Path, Path]:
    """Write snapshot and pulp secret files; return (snapshot_file, secret_file)."""
    snapshot_file = tmp_path / "snapshot_spec.json"
    snapshot_file.write_text(json.dumps(snapshot), encoding="utf-8")
    secret_file = tmp_path / "cli.toml"
    secret_file.write_text(secret, encoding="utf-8")
    return snapshot_file, secret_file


def _run(
    tmp_path: Path,
    snapshot: dict[str, Any],
    *,
    secret: str = 'base_url = "https://pulp.example.com"\n',
) -> Path:
    """Call run() against files under *tmp_path* and return the snapshot path."""
    snapshot_file, secret_file = _write_inputs(tmp_path, snapshot, secret=secret)
    run(
        data_dir=tmp_path,
        snapshot_path=snapshot_file.name,
        pulp_domain="mock",
        pulp_config_file=secret_file,
    )
    return snapshot_file


@pytest.fixture
def sbom_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect SBOM_DIR to a temp directory."""
    path = tmp_path / "sboms"
    monkeypatch.setattr(f"{TASK}.SBOM_DIR", path)
    return path


@pytest.fixture
def mock_cosign() -> Iterator[MagicMock]:
    """Stub docker-config setup and attachment downloads."""

    @contextmanager
    def fake_docker_config(_image: str) -> Iterator[Path]:
        yield Path("/unused-docker-config")

    with (
        patch(f"{TASK}.cosign.docker_config", fake_docker_config),
        patch(f"{TASK}.cosign.download") as download,
    ):
        yield download


def test_run_uploads_and_updates_src_rpm_only(
    tmp_path: Path, sbom_dir: Path, mock_cosign: MagicMock
) -> None:
    """Upload missing artifacts and set paths only on source RPM entries."""
    with (
        patch(f"{TASK}.pulp_client.ensure_file_repo_exists") as ensure,
        patch(
            f"{TASK}.pulp_client.check_file_content",
            return_value=PulpDigestStatus.NOT_FOUND,
        ) as check,
        patch(f"{TASK}.pulp_client.upload_file_content") as upload,
    ):
        snapshot_file = _run(tmp_path, _snapshot([_component()]))

    ensure.assert_called_once()
    assert mock_cosign.call_count == 2
    assert mock_cosign.call_args_list[0].args[0] == "sbom"
    assert mock_cosign.call_args_list[1].args[0] == "attestation"
    shared_config = Path("/unused-docker-config")
    assert mock_cosign.call_args_list[0].kwargs["docker_config_dir"] == shared_config
    assert mock_cosign.call_args_list[1].kwargs["docker_config_dir"] == shared_config
    assert check.call_count == 2
    assert upload.call_count == 2
    assert upload.call_args_list[0].args[3] == "sboms/test/sha256-12345.sbom"
    assert upload.call_args_list[1].args[3] == "attestations/test/sha256-12345.att"
    assert upload.call_args_list[0].args[4] == "metadata"

    updated = json.loads(snapshot_file.read_text(encoding="utf-8"))
    src = updated["components"][0]["rpmsToPublish"][0]
    binary = updated["components"][0]["rpmsToPublish"][1]
    assert src["sbomPath"] == "metadata/sboms/test/sha256-12345.sbom"
    assert src["attestationPath"] == "metadata/attestations/test/sha256-12345.att"
    assert "sbomPath" not in binary
    assert "attestationPath" not in binary
    assert sbom_dir.is_dir()


def test_run_skips_upload_when_checksum_matches(
    tmp_path: Path, sbom_dir: Path, mock_cosign: MagicMock
) -> None:
    """Skip Pulp upload when remote checksums already match."""
    with (
        patch(f"{TASK}.pulp_client.ensure_file_repo_exists"),
        patch(
            f"{TASK}.pulp_client.check_file_content",
            return_value=PulpDigestStatus.MATCH,
        ),
        patch(f"{TASK}.pulp_client.upload_file_content") as upload,
    ):
        snapshot_file = _run(tmp_path, _snapshot([_component()]))

    upload.assert_not_called()
    updated = json.loads(snapshot_file.read_text(encoding="utf-8"))
    src = updated["components"][0]["rpmsToPublish"][0]
    assert src["sbomPath"] == "metadata/sboms/test/sha256-12345.sbom"
    assert src["attestationPath"] == "metadata/attestations/test/sha256-12345.att"


def test_run_checksum_mismatch_raises(
    tmp_path: Path, sbom_dir: Path, mock_cosign: MagicMock
) -> None:
    """Fail when Pulp already has a different checksum."""
    with (
        patch(f"{TASK}.pulp_client.ensure_file_repo_exists"),
        patch(
            f"{TASK}.pulp_client.check_file_content",
            return_value=PulpDigestStatus.MISMATCH,
        ),
        patch(f"{TASK}.pulp_client.upload_file_content") as upload,
    ):
        with pytest.raises(RuntimeError, match="SBOM checksum mismatch for test"):
            _run(tmp_path, _snapshot([_component()]))
    upload.assert_not_called()


def test_run_attestation_checksum_mismatch(
    tmp_path: Path, sbom_dir: Path, mock_cosign: MagicMock
) -> None:
    """Fail on attestation mismatch after a successful SBOM check."""
    statuses = iter([PulpDigestStatus.NOT_FOUND, PulpDigestStatus.MISMATCH])

    with (
        patch(f"{TASK}.pulp_client.ensure_file_repo_exists"),
        patch(
            f"{TASK}.pulp_client.check_file_content",
            side_effect=lambda *args, **kwargs: next(statuses),
        ),
        patch(f"{TASK}.pulp_client.upload_file_content") as upload,
    ):
        with pytest.raises(RuntimeError, match="Attestation checksum mismatch for test"):
            _run(tmp_path, _snapshot([_component()]))
    upload.assert_called_once()


def test_run_skips_component_without_source_rpms(tmp_path: Path, sbom_dir: Path) -> None:
    """Skip components that have no source RPMs."""
    with (
        patch(f"{TASK}.pulp_client.ensure_file_repo_exists"),
        patch(f"{TASK}.cosign.download") as download,
        patch(f"{TASK}.pulp_client.upload_file_content") as upload,
    ):
        snapshot_file = _run(
            tmp_path,
            _snapshot([_component(rpms=[_binary_rpm()])]),
        )

    download.assert_not_called()
    upload.assert_not_called()
    updated = json.loads(snapshot_file.read_text(encoding="utf-8"))
    assert "sbomPath" not in updated["components"][0]["rpmsToPublish"][0]


def test_run_skips_missing_and_non_dict_rpm_entries(tmp_path: Path, sbom_dir: Path) -> None:
    """Treat missing, non-list, and non-dict rpmsToPublish as no source RPMs."""
    with (
        patch(f"{TASK}.pulp_client.ensure_file_repo_exists"),
        patch(f"{TASK}.cosign.download") as download,
    ):
        _run(
            tmp_path,
            _snapshot(
                [
                    {"name": "no-rpms", "containerImage": _IMAGE},
                    _component(name="bad-list", rpms="not-a-list"),  # type: ignore[arg-type]
                    _component(name="mixed", rpms=["skip", {"rpm": "foo.rpm"}]),
                ]
            ),
        )
    download.assert_not_called()


def test_run_processes_second_component_after_skip(
    tmp_path: Path, sbom_dir: Path, mock_cosign: MagicMock
) -> None:
    """Continue after skipping a component with no source RPMs."""
    with (
        patch(f"{TASK}.pulp_client.ensure_file_repo_exists"),
        patch(
            f"{TASK}.pulp_client.check_file_content",
            return_value=PulpDigestStatus.NOT_FOUND,
        ),
        patch(f"{TASK}.pulp_client.upload_file_content"),
    ):
        snapshot_file = _run(
            tmp_path,
            _snapshot(
                [
                    _component(name="skip-me", rpms=[_binary_rpm()]),
                    _component(name="keep"),
                ]
            ),
        )

    assert mock_cosign.call_count == 2
    updated = json.loads(snapshot_file.read_text(encoding="utf-8"))
    assert "sbomPath" not in updated["components"][0]["rpmsToPublish"][0]
    assert updated["components"][1]["rpmsToPublish"][0]["sbomPath"].endswith(".sbom")


def test_run_missing_container_image(tmp_path: Path, sbom_dir: Path) -> None:
    """Fail when a source-RPM component has no containerImage."""
    with patch(f"{TASK}.pulp_client.ensure_file_repo_exists"):
        with pytest.raises(RuntimeError, match="No containerImage for component test"):
            _run(tmp_path, _snapshot([_component(image=None)]))


def test_run_empty_container_image(tmp_path: Path, sbom_dir: Path) -> None:
    """Fail when containerImage is empty."""
    with patch(f"{TASK}.pulp_client.ensure_file_repo_exists"):
        with pytest.raises(RuntimeError, match="No containerImage"):
            _run(tmp_path, _snapshot([_component(image="")]))


def test_run_digest_not_sha256(tmp_path: Path, sbom_dir: Path) -> None:
    """Fail when the image reference digest is not sha256."""
    with patch(f"{TASK}.pulp_client.ensure_file_repo_exists"):
        with pytest.raises(RuntimeError, match="Could not extract sha256 digest"):
            _run(
                tmp_path,
                _snapshot([_component(image="quay.io/test/img@sha512:abcd")]),
            )


def test_run_empty_secret(tmp_path: Path, sbom_dir: Path) -> None:
    """Fail when the mounted cli.toml is empty."""
    with pytest.raises(RuntimeError, match="Missing cli.toml"):
        _run(tmp_path, _snapshot(), secret="")


def test_run_whitespace_secret(tmp_path: Path, sbom_dir: Path) -> None:
    """Fail when the mounted cli.toml is only whitespace."""
    with pytest.raises(RuntimeError, match="Missing cli.toml"):
        _run(tmp_path, _snapshot(), secret="  \n")


def test_run_writes_snapshot_once(
    tmp_path: Path, sbom_dir: Path, mock_cosign: MagicMock
) -> None:
    """Write the snapshot once after every source-RPM component is processed."""
    task_mod = import_module(TASK)
    with (
        patch(f"{TASK}.pulp_client.ensure_file_repo_exists"),
        patch(
            f"{TASK}.pulp_client.check_file_content",
            return_value=PulpDigestStatus.NOT_FOUND,
        ),
        patch(f"{TASK}.pulp_client.upload_file_content"),
        patch.object(task_mod, "_write_snapshot", wraps=task_mod._write_snapshot) as write,
    ):
        snapshot_file = _run(
            tmp_path,
            _snapshot([_component(name="one"), _component(name="two")]),
        )

    write.assert_called_once()
    updated = json.loads(snapshot_file.read_text(encoding="utf-8"))
    assert updated["components"][0]["rpmsToPublish"][0]["sbomPath"].endswith(".sbom")
    assert updated["components"][1]["rpmsToPublish"][0]["sbomPath"].endswith(".sbom")
    assert mock_cosign.call_count == 4


def test_run_components_not_list(tmp_path: Path, sbom_dir: Path) -> None:
    """Fail when snapshot components is not an array."""
    with patch(f"{TASK}.pulp_client.ensure_file_repo_exists"):
        with pytest.raises(TypeError, match="must be an array"):
            _run(tmp_path, {"components": {"name": "x"}})


def test_run_component_not_object(tmp_path: Path, sbom_dir: Path) -> None:
    """Fail when a snapshot component is not an object."""
    with patch(f"{TASK}.pulp_client.ensure_file_repo_exists"):
        with pytest.raises(TypeError, match="must be an object"):
            _run(tmp_path, _snapshot(["nope"]))  # type: ignore[list-item]


def test_run_missing_components_key(tmp_path: Path, sbom_dir: Path) -> None:
    """Treat a missing components field as an empty list."""
    with (
        patch(f"{TASK}.pulp_client.ensure_file_repo_exists"),
        patch(f"{TASK}.cosign.download") as download,
    ):
        _run(tmp_path, {"application": "rpms"})
    download.assert_not_called()


def test_run_deletes_temp_config_on_failure(tmp_path: Path, sbom_dir: Path) -> None:
    """Delete the writable cli.toml copy when a later step fails."""
    created: list[Path] = []
    real_make = __import__(
        "release_service_utils.helpers.file.file", fromlist=["make_tempfile_path"]
    ).make_tempfile_path

    def tracking_make(prefix: str, data: bytes | None = None) -> Path:
        path = real_make(prefix, data)
        created.append(path)
        return path

    with (
        patch(f"{TASK}.file.make_tempfile_path", side_effect=tracking_make),
        patch(
            f"{TASK}.pulp_client.ensure_file_repo_exists",
            side_effect=RuntimeError("repo missing"),
        ),
    ):
        with pytest.raises(RuntimeError, match="repo missing"):
            _run(tmp_path, _snapshot())

    assert created
    assert not created[0].exists()


def test_run_empty_source_rpm_name(tmp_path: Path, sbom_dir: Path) -> None:
    """Ignore source-looking entries whose rpm field is empty."""
    with (
        patch(f"{TASK}.pulp_client.ensure_file_repo_exists"),
        patch(f"{TASK}.cosign.download") as download,
    ):
        _run(tmp_path, _snapshot([_component(rpms=[{"rpm": ""}])]))
    download.assert_not_called()


def test_main_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() reads env and calls run()."""
    secret = tmp_path / "cli.toml"
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snapshot_spec.json")
    monkeypatch.setenv("PULP_DOMAIN", "mock")
    monkeypatch.setenv("PULP_CONFIG_FILE", str(secret))

    with patch(f"{TASK}.run") as mock_run:
        assert main() == 0

    mock_run.assert_called_once_with(
        data_dir=tmp_path,
        snapshot_path="snapshot_spec.json",
        pulp_domain="mock",
        pulp_config_file=secret,
    )


def test_main_default_pulp_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """main() uses the default secret path when PULP_CONFIG_FILE is unset."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PULP_DOMAIN", "mock")
    monkeypatch.delenv("PULP_CONFIG_FILE", raising=False)

    with patch(f"{TASK}.run") as mock_run:
        assert main() == 0

    mock_run.assert_called_once()
    assert mock_run.call_args.kwargs["pulp_config_file"] == Path("/etc/secrets/cli.toml")


def test_main_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() exits when a required env var is missing."""
    monkeypatch.delenv("PARAM_DATA_DIR", raising=False)
    monkeypatch.delenv("PARAM_SNAPSHOT_PATH", raising=False)
    monkeypatch.delenv("PULP_DOMAIN", raising=False)

    with pytest.raises(SystemExit):
        main()


def test_dunder_main_invokes_main() -> None:
    """Running the package as a module calls main()."""
    module = "release_service_utils.tasks.managed.upload_src_rpm_sbom_attestation"
    with (
        patch(f"{TASK}.main", return_value=0) as mock_main,
        pytest.raises(SystemExit) as exc,
    ):
        runpy.run_module(module, run_name="__main__")
    assert exc.value.code == 0
    mock_main.assert_called_once_with()


def test_package_main_not_run_on_import() -> None:
    """Importing the package __main__ module does not call main()."""
    import importlib
    import sys

    module_name = (
        "release_service_utils.tasks.managed.upload_src_rpm_sbom_attestation.__main__"
    )
    sys.modules.pop(module_name, None)
    with patch(f"{TASK}.main") as mock_main:
        importlib.import_module(module_name)
    mock_main.assert_not_called()
