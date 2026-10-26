"""Test extract_py_artifacts."""

from __future__ import annotations

import base64
import json
import runpy
import subprocess
import zipfile
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import patch

import pytest

from release_service_utils.tasks.managed.extract_py_artifacts import (
    extract_py_artifacts as epa,
)

TASK = "release_service_utils.tasks.managed.extract_py_artifacts.extract_py_artifacts"

_SBOM = {
    "packages": [
        {
            "externalRefs": [
                {
                    "referenceType": "purl",
                    "referenceLocator": "pkg:pypi/test_package@1.0.0",
                }
            ]
        }
    ]
}

_STATEMENT = {
    "predicate": {"buildDefinition": {"buildType": "https://tekton.dev/chains/v2/slsa"}},
}


def _write_json(path: Path, payload: dict) -> None:
    """Write *payload* as JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _wheel(
    directory: Path,
    filename: str = "test_package-1.0.0-py3-none-any.whl",
    sbom: dict | None = None,
    member: str | None = None,
) -> Path:
    """Write a wheel whose SBOM member matches the task's dist-info path."""
    stem = filename.removesuffix(".whl")
    parts = stem.split("-")
    sbom_name = member or f"{parts[0]}-{parts[1]}.dist-info/sboms/redhat.spdx.json"
    path = directory / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(sbom_name, json.dumps(sbom or _SBOM))
    return path


def _envelope(statement: dict | None = None) -> str:
    """Return a one-line cosign DSSE envelope."""
    payload = base64.b64encode(json.dumps(statement or _STATEMENT).encode()).decode()
    return json.dumps({"payload": payload})


def _completed(
    cmd: Sequence[str | Path],
    stdout: str = "",
    stderr: str = "",
    returncode: int = 0,
) -> subprocess.CompletedProcess[str]:
    """Return a CompletedProcess for a mocked command."""
    return subprocess.CompletedProcess(
        [str(part) for part in cmd],
        returncode,
        stdout=stdout,
        stderr=stderr,
    )


def test_container_images_keeps_order_duplicates_and_nulls() -> None:
    """Match ``jq -r '.components[].containerImage'``."""
    images = epa.container_images_from_snapshot(
        {
            "components": [
                {"containerImage": "repo/a:tag"},
                {},
                {"containerImage": "repo/a:tag"},
                {"containerImage": ""},
            ]
        }
    )
    assert images == ["repo/a:tag", "null", "repo/a:tag", ""]


def test_container_images_rejects_missing_components() -> None:
    """Fail when ``components`` is missing or null, as jq iteration does."""
    with pytest.raises(ValueError, match="Cannot iterate over null"):
        epa.container_images_from_snapshot({})
    with pytest.raises(ValueError, match="Cannot iterate over null"):
        epa.container_images_from_snapshot({"components": None})


def test_container_images_rejects_non_objects() -> None:
    """Fail when a component cannot be indexed for ``containerImage``."""
    with pytest.raises(ValueError, match="containerImage"):
        epa.container_images_from_snapshot({"components": ["nope"]})
    with pytest.raises(ValueError, match="must be a string"):
        epa.container_images_from_snapshot({"components": [{"containerImage": 1}]})


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        (
            "test_package-1.0.0-py3-none-any.whl",
            ("test_package", "1.0.0", "any"),
        ),
        (
            "pkg-1.0.0-1-py3-none-manylinux2014_x86_64.whl",
            ("pkg", "1.0.0", "manylinux2014_x86_64"),
        ),
    ],
)
def test_parse_wheel_filename(filename: str, expected: tuple[str, str, str]) -> None:
    """Split a 5- or 6-field wheel stem on hyphens."""
    assert epa.parse_wheel_filename(filename) == expected


@pytest.mark.parametrize("filename", ["short-1.0.whl", "a-b-c-d-e-f-g.whl"])
def test_parse_wheel_filename_invalid(filename: str) -> None:
    """Reject stems that are not 5 or 6 hyphen-separated fields."""
    with pytest.raises(SystemExit):
        epa.parse_wheel_filename(filename)


@pytest.mark.parametrize(
    ("platform", "expected"),
    [
        ("any", ("noarch", "any")),
        ("none", ("noarch", "any")),
        ("manylinux2014_x86_64", ("64", "linux")),
        ("manylinux_2_17_ppc64le", ("ppc64le", "linux")),
        ("musllinux_1_1_aarch64", ("aarch64", "linux")),
        ("linux_x86_64", ("64", "linux")),
        ("macosx_11_0_arm64", ("arm64", "macos")),
        ("win_amd64", ("amd64", "windows")),
        ("win32", ("win32", "windows")),
        ("other_s390x", ("s390x", "any")),
    ],
)
def test_parse_wheel_platform(platform: str, expected: tuple[str, str]) -> None:
    """Map a platform tag with the task's prefix and last-underscore rules."""
    assert epa.parse_wheel_platform(platform) == expected


def test_extract_sbom_from_wheel_uses_literal_dist_info(tmp_path: Path) -> None:
    """Read ``{name}-{version}.dist-info``, ignoring other SBOM members."""
    path = tmp_path / "test_package-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "Test_Package-1.0.0.dist-info/sboms/redhat.spdx.json",
            json.dumps(_SBOM),
        )
        archive.writestr(
            "test_package-1.0.0.dist-info/sboms/redhat.spdx.json",
            json.dumps({"packages": []}),
        )
    assert epa.extract_sbom_from_wheel(path, "test_package", "1.0.0") == {"packages": []}


def test_extract_sbom_from_wheel_missing(tmp_path: Path) -> None:
    """Fail with the task's SBOM-not-found message."""
    path = tmp_path / "test_package-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("empty", "x")
    with pytest.raises(SystemExit):
        epa.extract_sbom_from_wheel(path, "test_package", "1.0.0")


def test_extract_pypi_purl_returns_first_pypi_locator() -> None:
    """Keep the first ``pkg:pypi/`` locator and ignore the wheel name."""
    sbom = {
        "packages": [
            None,
            {
                "externalRefs": [
                    {"referenceType": "url", "referenceLocator": "https://example"},
                    {"referenceType": "purl", "referenceLocator": "pkg:rpm/foo"},
                    {"referenceType": "purl", "referenceLocator": "pkg:pypi/other@9"},
                    {"referenceType": "purl", "referenceLocator": "pkg:pypi/test_package@1"},
                ]
            },
        ]
    }
    assert epa.extract_pypi_purl(sbom, "wheel.whl") == "pkg:pypi/other@9"


def test_extract_pypi_purl_missing() -> None:
    """Fail when the SBOM has no ``pkg:pypi/`` purl."""
    with pytest.raises(SystemExit):
        epa.extract_pypi_purl({"packages": []}, "wheel.whl")
    with pytest.raises(SystemExit):
        epa.extract_pypi_purl({}, "wheel.whl")


def test_extract_pypi_purl_rejects_non_string_locator() -> None:
    """Fail the way jq ``startswith`` does when the locator is not a string."""
    sbom = {
        "packages": [{"externalRefs": [{"referenceType": "purl", "referenceLocator": None}]}]
    }
    with pytest.raises(ValueError, match="startswith"):
        epa.extract_pypi_purl(sbom, "wheel.whl")


def test_collect_wheel_artifacts_sorts_and_dedupes(tmp_path: Path) -> None:
    """Emit one row per wheel, sorted and deduped by the jq group key."""
    _wheel(tmp_path, "pkg-1.0.0-py3-none-macosx_11_0_arm64.whl")
    _wheel(tmp_path, "pkg-1.0.0-py3-none-manylinux2014_aarch64.whl")
    _wheel(tmp_path, "pkg-1.0.0-py3-abi3-any.whl")
    _wheel(tmp_path, "pkg-1.0.0-py3-none-any.whl")
    nested = tmp_path / "nested"
    _wheel(nested, "nested_pkg-1.0.0-py3-none-any.whl")

    artifacts = epa.collect_wheel_artifacts(tmp_path)

    assert artifacts == [
        {
            "component": "pkg",
            "purl": "pkg:pypi/test_package@1.0.0",
            "architecture": "aarch64",
            "os": "linux",
        },
        {
            "component": "pkg",
            "purl": "pkg:pypi/test_package@1.0.0",
            "architecture": "arm64",
            "os": "macos",
        },
        {
            "component": "pkg",
            "purl": "pkg:pypi/test_package@1.0.0",
            "architecture": "noarch",
            "os": "any",
        },
    ]
    assert nested.joinpath("nested_pkg-1.0.0-py3-none-any.whl").is_file()


def test_collect_wheel_artifacts_no_wheels(tmp_path: Path) -> None:
    """Fail when the files directory has no top-level wheels."""
    tmp_path.mkdir(exist_ok=True)
    with pytest.raises(SystemExit):
        epa.collect_wheel_artifacts(tmp_path)


def test_update_release_notes_artifacts_appends_and_creates() -> None:
    """Create missing release-note objects and append to an existing list."""
    created: dict = {}
    epa.update_release_notes_artifacts(created, [{"component": "a"}])
    assert created["releaseNotes"]["content"]["artifacts"] == [{"component": "a"}]

    nulled = {"releaseNotes": None}
    epa.update_release_notes_artifacts(nulled, [{"component": "a"}])
    assert nulled["releaseNotes"]["content"]["artifacts"] == [{"component": "a"}]

    existing = {"releaseNotes": {"content": {"artifacts": [{"component": "old"}]}}}
    epa.update_release_notes_artifacts(existing, [{"component": "new"}])
    assert existing["releaseNotes"]["content"]["artifacts"] == [
        {"component": "old"},
        {"component": "new"},
    ]

    false_list = {"releaseNotes": {"content": {"artifacts": False}}}
    epa.update_release_notes_artifacts(false_list, [{"component": "a"}])
    assert false_list["releaseNotes"]["content"]["artifacts"] == [{"component": "a"}]


def test_update_release_notes_artifacts_rejects_non_objects() -> None:
    """Fail when jq cannot index a string or boolean along the path."""
    with pytest.raises(ValueError, match="releaseNotes"):
        epa.update_release_notes_artifacts({"releaseNotes": "x"}, [])
    with pytest.raises(ValueError, match="releaseNotes"):
        epa.update_release_notes_artifacts({"releaseNotes": False}, [])
    with pytest.raises(ValueError, match="non-array"):
        epa.update_release_notes_artifacts(
            {"releaseNotes": {"content": {"artifacts": {}}}},
            [],
        )


def test_update_mapping_components_patches_blank_type_only() -> None:
    """Set top-level contentType when the jq expression is empty."""
    data = {
        "mapping": {
            "components": [
                {"name": "keep", "contentType": "rpm"},
                {"name": "blank"},
                {"name": "empty", "contentType": ""},
                {
                    "name": "nested-empty",
                    "contentType": "rpm",
                    "contentGateway": {"contentType": ""},
                },
                {
                    "name": "nested-rpm",
                    "contentType": "",
                    "contentGateway": {"contentType": "rpm"},
                },
                None,
            ]
        }
    }
    epa.update_mapping_components(
        data, ["blank", "empty", "nested-empty", "nested-rpm", "added"]
    )
    assert data["mapping"]["components"] == [
        {"name": "keep", "contentType": "rpm"},
        {"name": "blank", "contentType": "generic"},
        {"name": "empty", "contentType": "generic"},
        {
            "name": "nested-empty",
            "contentType": "generic",
            "contentGateway": {"contentType": ""},
        },
        {
            "name": "nested-rpm",
            "contentType": "",
            "contentGateway": {"contentType": "rpm"},
        },
        None,
        {"name": "added", "contentType": "generic"},
    ]


def test_update_mapping_components_appends_sorted_and_skips_duplicates() -> None:
    """Append missing names in sorted order and do not duplicate existing ones."""
    data: dict = {"mapping": None}
    epa.update_mapping_components(data, ["b", "a", "b"])
    assert data["mapping"]["components"] == [
        {"name": "a", "contentType": "generic"},
        {"name": "b", "contentType": "generic"},
    ]


def test_update_mapping_components_rejects_bad_mapping() -> None:
    """Fail when ``mapping`` is not an object."""
    with pytest.raises(ValueError, match="components"):
        epa.update_mapping_components({"mapping": "bad"}, ["a"])


def test_pull_oci_artifacts_retries_then_pulls_into_files_dir(tmp_path: Path) -> None:
    """Pull with select-oci-auth and ``oras pull -o``, retrying a failure."""
    calls: list[list[str]] = []
    oras_attempts = {"count": 0}

    def fake_run_cmd(
        cmd: Sequence[str | Path], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        argv = [str(part) for part in cmd]
        calls.append(argv)
        if argv[0] == "select-oci-auth":
            return _completed(cmd, stdout='{"auths":{}}\n')
        if argv[0] == "oras":
            oras_attempts["count"] += 1
            if oras_attempts["count"] == 1:
                return _completed(cmd, stderr="temporary\n", returncode=1)
            dest = Path(argv[argv.index("-o") + 1])
            (dest / "pkg.whl").write_text("wheel", encoding="utf-8")
            return _completed(cmd)
        if argv[0] == "ls":
            return _completed(cmd, stdout="total 0\n")
        raise AssertionError(argv)

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        patch(f"{TASK}.time.sleep") as sleep,
    ):
        epa.pull_oci_artifacts(["quay.io/org/pkg:1"], tmp_path)

    assert sleep.called
    assert calls[0] == ["select-oci-auth", "quay.io/org/pkg:1"]
    assert calls[1][:4] == ["oras", "pull", "--registry-config", "/tmp/auth.json"]
    assert calls[1][-2:] == ["-o", str(tmp_path)]
    assert (tmp_path / "pkg.whl").is_file()
    assert Path("/tmp/auth.json").read_text(encoding="utf-8") == '{"auths":{}}\n'


def test_pull_oci_artifacts_stops_on_unauthorized(tmp_path: Path) -> None:
    """Do not retry an oras failure whose stderr contains unauthorized."""
    oras_calls = {"count": 0}

    def fake_run_cmd(
        cmd: Sequence[str | Path], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="")
        if cmd[0] == "oras":
            oras_calls["count"] += 1
            return _completed(cmd, stderr="Unauthorized\n", returncode=1)
        raise AssertionError(cmd)

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        patch(f"{TASK}.time.sleep") as sleep,
        pytest.raises(subprocess.CalledProcessError),
    ):
        epa.pull_oci_artifacts(["quay.io/org/pkg:1"], tmp_path)

    assert oras_calls["count"] == 1
    sleep.assert_not_called()


def test_write_chains_provenance_saves_decoded_payload(tmp_path: Path) -> None:
    """Save ``${image##*@}.json`` without checking the attestation subject."""
    image = "quay.io/org/pkg:1@sha256:abc"
    seen_env: dict[str, str] = {}

    def fake_run_cmd(
        cmd: Sequence[str | Path], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout='{"auths":{}}')
        if cmd[0] == "cosign":
            env = kwargs.get("env")
            assert isinstance(env, dict)
            seen_env.update(env)
            assert cmd[-1] == image
            assert "k8s://openshift-pipelines/public-key" in [str(part) for part in cmd]
            return _completed(cmd, stdout=_envelope() + "\nsecond line\n")
        if cmd[0] == "ls":
            return _completed(cmd, stdout="prov\n")
        raise AssertionError(cmd)

    with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd):
        epa._write_chains_provenance([image], tmp_path)

    dest = tmp_path / "chains-provenance" / "sha256:abc.json"
    assert json.loads(dest.read_text(encoding="utf-8")) == _STATEMENT
    assert "DOCKER_CONFIG" in seen_env


def test_write_chains_provenance_same_digest_overwrites(tmp_path: Path) -> None:
    """Let a later image replace provenance that uses the same digest."""
    first = "quay.io/org/a:1@sha256:abc"
    second = "quay.io/org/b@sha256:abc"
    statements = {
        first: {"predicate": {"buildDefinition": {"buildType": "first"}}},
        second: {"predicate": {"buildDefinition": {"buildType": "second"}}},
    }

    def fake_run_cmd(
        cmd: Sequence[str | Path], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope(statements[str(cmd[-1])]))
        if cmd[0] == "ls":
            return _completed(cmd, stdout="")
        raise AssertionError(cmd)

    with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd):
        epa._write_chains_provenance([first, second], tmp_path)

    written = list((tmp_path / "chains-provenance").iterdir())
    assert [path.name for path in written] == ["sha256:abc.json"]
    saved = json.loads(written[0].read_text(encoding="utf-8"))
    assert saved["predicate"]["buildDefinition"]["buildType"] == "second"


def test_write_chains_provenance_overwrites_existing_file(tmp_path: Path) -> None:
    """Overwrite an existing provenance file for the same digest name."""
    image = "pkg:latest@sha256:abc"
    dest = tmp_path / "chains-provenance" / "sha256:abc.json"
    dest.parent.mkdir(parents=True)
    dest.write_text("old", encoding="utf-8")

    def fake_run_cmd(
        cmd: Sequence[str | Path], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=_envelope())
        if cmd[0] == "ls":
            return _completed(cmd, stdout="")
        raise AssertionError(cmd)

    with patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd):
        epa._write_chains_provenance([image], tmp_path)

    assert json.loads(dest.read_text(encoding="utf-8"))["predicate"]


def test_write_chains_provenance_failure(tmp_path: Path) -> None:
    """Log the task's provenance error and stop when cosign fails."""

    def fake_run_cmd(
        cmd: Sequence[str | Path], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "cosign":
            return _completed(cmd, stderr="cosign failed\n", returncode=1)
        raise AssertionError(cmd)

    with (
        patch(f"{TASK}.subprocess_cmd.run_cmd", side_effect=fake_run_cmd),
        pytest.raises(SystemExit),
    ):
        epa._write_chains_provenance(["quay.io/org/pkg@sha256:abc"], tmp_path)


def _fake_commands(
    files_dir: Path,
    *,
    oras_writer,
    cosign_stdout: str | None = None,
    cosign_code: int = 0,
) -> object:
    """Return a run_cmd fake for pull, ls, and cosign."""

    def fake_run_cmd(
        cmd: Sequence[str | Path], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if cmd[0] == "select-oci-auth":
            return _completed(cmd, stdout="{}")
        if cmd[0] == "oras":
            dest = Path(str(cmd[list(cmd).index("-o") + 1]))
            oras_writer(dest)
            return _completed(cmd)
        if cmd[0] == "ls":
            return _completed(cmd, stdout="listed\n")
        if cmd[0] == "cosign":
            return _completed(cmd, stdout=cosign_stdout or _envelope(), returncode=cosign_code)
        raise AssertionError(cmd)

    return fake_run_cmd


def test_run_happy_path(tmp_path: Path) -> None:
    """Pull, update data.json in place, and write digest-named provenance."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    files_dir = tmp_path / "files"
    image = "quay.io/test/test-package@sha256:abc"
    _write_json(
        snapshot_path,
        {"components": [{"containerImage": image}, {"containerImage": image}]},
    )
    _write_json(
        data_path,
        {
            "mapping": {"components": [{"name": "test_package"}]},
            "releaseNotes": {"content": {"artifacts": []}, "type": "RHBA"},
        },
    )
    files_dir.mkdir()
    _wheel(
        files_dir,
        "stale_package-9.9.9-py3-none-any.whl",
        sbom={
            "packages": [
                {
                    "externalRefs": [
                        {"referenceType": "purl", "referenceLocator": "pkg:pypi/stale@9.9.9"}
                    ]
                }
            ]
        },
    )

    def write_pull(dest: Path) -> None:
        _wheel(dest)
        (dest / "test_package-1.0.0.tar.gz").write_text("sdist", encoding="utf-8")
        _wheel(dest / "nested", "nested_only-1.0.0-py3-none-any.whl")

    with patch(
        f"{TASK}.subprocess_cmd.run_cmd",
        side_effect=_fake_commands(files_dir, oras_writer=write_pull),
    ):
        epa.run(snapshot_path=snapshot_path, data_path=data_path, files_dir=files_dir)

    assert (files_dir / "test_package-1.0.0-py3-none-any.whl").is_file()
    assert (files_dir / "test_package-1.0.0.tar.gz").is_file()
    assert (files_dir / "stale_package-9.9.9-py3-none-any.whl").is_file()
    assert (files_dir / "nested" / "nested_only-1.0.0-py3-none-any.whl").is_file()
    data = json.loads(data_path.read_text(encoding="utf-8"))
    artifacts = data["releaseNotes"]["content"]["artifacts"]
    assert [row["component"] for row in artifacts] == ["stale_package", "test_package"]
    assert artifacts[1]["architecture"] == "noarch"
    assert artifacts[1]["os"] == "any"
    assert data["releaseNotes"]["type"] == "RHBA"
    assert data["mapping"]["components"] == [
        {"name": "test_package", "contentType": "generic"},
        {"name": "stale_package", "contentType": "generic"},
    ]
    provenance = files_dir / "chains-provenance" / "sha256:abc.json"
    assert json.loads(provenance.read_text(encoding="utf-8"))["predicate"]["buildDefinition"]


def test_run_stops_after_notes_when_provenance_fails(tmp_path: Path) -> None:
    """Leave the updated data.json in place when provenance fetching fails."""
    snapshot_path = tmp_path / "snapshot.json"
    data_path = tmp_path / "data.json"
    files_dir = tmp_path / "files"
    _write_json(
        snapshot_path, {"components": [{"containerImage": "quay.io/org/pkg@sha256:abc"}]}
    )
    _write_json(data_path, {"releaseNotes": {"content": {"artifacts": []}}})

    def write_pull(dest: Path) -> None:
        _wheel(dest)

    with (
        patch(
            f"{TASK}.subprocess_cmd.run_cmd",
            side_effect=_fake_commands(files_dir, oras_writer=write_pull, cosign_code=1),
        ),
        pytest.raises(SystemExit),
    ):
        epa.run(snapshot_path=snapshot_path, data_path=data_path, files_dir=files_dir)

    data = json.loads(data_path.read_text(encoding="utf-8"))
    assert data["releaseNotes"]["content"]["artifacts"][0]["component"] == "test_package"
    assert data["mapping"]["components"][0]["contentType"] == "generic"


def test_main_joins_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Concatenate PARAM_* paths the way the task does, including ``..``."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.setenv("PARAM_FILES_DIR", "nested/../files")

    with patch(f"{TASK}.run") as mock_run:
        assert epa.main() == 0

    mock_run.assert_called_once_with(
        snapshot_path=Path(f"{tmp_path}/snap.json"),
        data_path=Path(f"{tmp_path}/data.json"),
        files_dir=Path(f"{tmp_path}/nested/../files"),
    )


def test_main_defaults_files_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """PARAM_FILES_DIR defaults to ``files`` when unset."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")
    monkeypatch.delenv("PARAM_FILES_DIR", raising=False)

    with patch(f"{TASK}.run") as mock_run:
        assert epa.main() == 0

    assert mock_run.call_args.kwargs["files_dir"] == tmp_path / "files"


def test_main_missing_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() exits when a required PARAM_* env var is missing."""
    monkeypatch.delenv("PARAM_DATA_DIR", raising=False)
    with pytest.raises(SystemExit):
        epa.main()


def test_main_entrypoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Execute the package as ``python -m ...extract_py_artifacts``."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snap.json")
    monkeypatch.setenv("PARAM_DATA_PATH", "data.json")

    with patch(f"{TASK}.run"), pytest.raises(SystemExit) as exc:
        runpy.run_module(
            "release_service_utils.tasks.managed.extract_py_artifacts",
            run_name="__main__",
        )
    assert exc.value.code == 0
