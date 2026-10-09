"""Test Helm manifest validation against mapped snapshot repositories."""

from __future__ import annotations

import copy
import json
import re
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call

import pytest

from release_service_utils.tasks.managed.validate_helm_chart_snapshot import (
    validate_helm_chart_snapshot as task,
)


@pytest.fixture
def component() -> dict[str, Any]:
    """Build a component with a mapped Helm chart repository."""
    return {
        "name": "mychart",
        "containerImage": "quay.io/tenant/mychart@sha256:abc123",
        "repositories": [
            {
                "url": "quay.io/redhat-prod/acme----mychart",
                "tags": ["1.0.0_buildmeta"],
            }
        ],
    }


@pytest.fixture
def manifest() -> dict[str, Any]:
    """Build a Helm OCI manifest with chart metadata annotations."""
    return {
        "schemaVersion": 2,
        "config": {"mediaType": "application/vnd.cncf.helm.config.v1+json"},
        "annotations": {
            "org.opencontainers.image.title": "mychart",
            "org.opencontainers.image.version": "1.0.0+buildmeta",
        },
    }


@pytest.fixture
def inspect_mock(manifest: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Return the test manifest when skopeo inspects an image."""
    mock = MagicMock(
        side_effect=lambda *args, **kwargs: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(manifest), stderr=""
        )
    )
    monkeypatch.setattr(task.skopeo, "inspect", mock)
    return mock


def _write_snapshot(tmp_path: Path, components: list[dict[str, Any]]) -> Path:
    """Write a mapped snapshot under a nested workspace directory."""
    snapshot_path = Path("release/mapped.json")
    snapshot_file = tmp_path / snapshot_path
    snapshot_file.parent.mkdir(parents=True, exist_ok=True)
    snapshot_file.write_text(json.dumps({"components": components}), encoding="utf-8")
    return snapshot_path


def test_validates_all_components_and_repositories(
    tmp_path: Path, component: dict[str, Any], inspect_mock: MagicMock
) -> None:
    """Inspect every component and leave the snapshot unchanged."""
    component["repositories"].append(
        {"url": "registry.redhat.io/acme/mychart", "tags": ["latest", "1.0.0_buildmeta"]}
    )
    second = copy.deepcopy(component)
    second.update(name="another-component", containerImage="quay.io/other/mychart:1.0.0")
    snapshot_path = _write_snapshot(tmp_path, [component, second])
    original = (tmp_path / snapshot_path).read_bytes()

    task.run(data_dir=tmp_path, snapshot_path=snapshot_path)

    assert inspect_mock.call_args_list == [
        call(component["containerImage"], raw=True, check=True),
        call(second["containerImage"], raw=True, check=True),
    ]
    assert (tmp_path / snapshot_path).read_bytes() == original


@pytest.mark.parametrize(
    "url",
    [
        "quay.io/redhat-prod/acme----mychart",
        "quay.io/redhat-prod/acme----nested----mychart:old-tag",
        "registry.redhat.io/acme/mychart",
        "registry.example.com:5000/acme/mychart",
        "registry.example.com:5000/acme/mychart:old-tag",
    ],
)
def test_matches_delivery_repository_basename(
    tmp_path: Path, component: dict[str, Any], inspect_mock: MagicMock, url: str
) -> None:
    """Compare the title to the final decoded repository path segment."""
    component["repositories"][0]["url"] = url
    task.run(data_dir=tmp_path, snapshot_path=_write_snapshot(tmp_path, [component]))


@pytest.mark.parametrize(
    ("version", "tags"),
    [
        ("1.0.0", ["1.0.0"]),
        ("1.0.0-rc.1+build.2", ["1.0.0-rc.1_build.2"]),
        ("1.0.0+buildmeta", ["latest", "1.0.0_buildmeta", "v1.0"]),
    ],
)
def test_accepts_matching_version_tag(
    tmp_path: Path,
    component: dict[str, Any],
    manifest: dict[str, Any],
    inspect_mock: MagicMock,
    version: str,
    tags: list[str],
) -> None:
    """Accept a repository when at least one tag matches the chart version."""
    manifest["annotations"]["org.opencontainers.image.version"] = version
    component["repositories"][0]["tags"] = tags
    task.run(data_dir=tmp_path, snapshot_path=_write_snapshot(tmp_path, [component]))


@pytest.mark.parametrize("tags", [["0.2.0"], ["latest", "v1.0"], ["1.0.0_build_meta"]])
def test_rejects_tags_without_matching_version(
    tmp_path: Path, component: dict[str, Any], inspect_mock: MagicMock, tags: list[str]
) -> None:
    """Report the component, repository, tags, and expected chart version."""
    component["repositories"][0]["tags"] = tags
    message = (
        "component (mychart) repository (quay.io/redhat-prod/acme----mychart) — "
        f"none of the tags [{', '.join(tags)}] match chart version (1.0.0+buildmeta)"
    )
    with pytest.raises(ValueError, match=re.escape(message)):
        task.run(data_dir=tmp_path, snapshot_path=_write_snapshot(tmp_path, [component]))


def test_converts_only_first_underscore(
    tmp_path: Path,
    component: dict[str, Any],
    manifest: dict[str, Any],
    inspect_mock: MagicMock,
) -> None:
    """Reject a tag that would match only if every underscore were replaced."""
    manifest["annotations"]["org.opencontainers.image.version"] = "1.0.0+build+meta"
    component["repositories"][0]["tags"] = ["1.0.0_build_meta"]
    with pytest.raises(ValueError, match="none of the tags"):
        task.run(data_dir=tmp_path, snapshot_path=_write_snapshot(tmp_path, [component]))


@pytest.mark.parametrize("field", ["url", "tags"])
def test_rejects_invalid_second_repository(
    tmp_path: Path, component: dict[str, Any], inspect_mock: MagicMock, field: str
) -> None:
    """Require every repository to match the chart title and version."""
    second_repo = copy.deepcopy(component["repositories"][0])
    second_repo[field] = (
        "quay.io/redhat-prod/acme----wrong-name" if field == "url" else ["0.2.0"]
    )
    component["repositories"].append(second_repo)
    message = "repository basename \\(wrong-name\\)" if field == "url" else "none of the tags"
    with pytest.raises(ValueError, match=message):
        task.run(data_dir=tmp_path, snapshot_path=_write_snapshot(tmp_path, [component]))


@pytest.mark.parametrize(
    "config", [None, {}, {"mediaType": "application/vnd.oci.image.config.v1+json"}]
)
def test_rejects_non_helm_manifest(
    tmp_path: Path,
    component: dict[str, Any],
    manifest: dict[str, Any],
    inspect_mock: MagicMock,
    config: Any,
) -> None:
    """Reject manifests without the Helm config media type."""
    manifest["config"] = config
    with pytest.raises(ValueError, match="not a Helm OCI artifact"):
        task.run(data_dir=tmp_path, snapshot_path=_write_snapshot(tmp_path, [component]))


@pytest.mark.parametrize("annotation", ["title", "version"])
@pytest.mark.parametrize("value", [None, ""])
def test_rejects_empty_annotation(
    tmp_path: Path,
    component: dict[str, Any],
    manifest: dict[str, Any],
    inspect_mock: MagicMock,
    annotation: str,
    value: str | None,
) -> None:
    """Require nonempty chart title and version annotations."""
    manifest["annotations"][f"org.opencontainers.image.{annotation}"] = value
    with pytest.raises(ValueError, match="Helm manifest missing"):
        task.run(data_dir=tmp_path, snapshot_path=_write_snapshot(tmp_path, [component]))


@pytest.mark.parametrize("field", ["config", "annotations"])
def test_rejects_missing_manifest_metadata(
    tmp_path: Path,
    component: dict[str, Any],
    manifest: dict[str, Any],
    inspect_mock: MagicMock,
    field: str,
) -> None:
    """Reject a manifest lacking config or chart annotations."""
    del manifest[field]
    with pytest.raises(ValueError, match="not a Helm OCI artifact|Helm manifest missing"):
        task.run(data_dir=tmp_path, snapshot_path=_write_snapshot(tmp_path, [component]))


@pytest.mark.parametrize("field", ["repositories", "tags"])
@pytest.mark.parametrize("value", [None, [], "missing"])
def test_rejects_absent_repositories_or_tags(
    tmp_path: Path,
    component: dict[str, Any],
    inspect_mock: MagicMock,
    field: str,
    value: Any,
) -> None:
    """Reject missing, null, and empty repository or tag lists."""
    target = component if field == "repositories" else component["repositories"][0]
    if value == "missing":
        del target[field]
    else:
        target[field] = value
    with pytest.raises(ValueError, match=f"has no {field}"):
        task.run(data_dir=tmp_path, snapshot_path=_write_snapshot(tmp_path, [component]))


@pytest.mark.parametrize("snapshot", [{}, {"components": None}, {"components": []}])
def test_allows_snapshot_without_components(
    tmp_path: Path, inspect_mock: MagicMock, snapshot: dict[str, Any]
) -> None:
    """Skip registry calls when the snapshot contains no components."""
    (tmp_path / "mapped.json").write_text(json.dumps(snapshot), encoding="utf-8")
    task.run(data_dir=tmp_path, snapshot_path=Path("mapped.json"))
    inspect_mock.assert_not_called()


@pytest.mark.parametrize("content", [None, "invalid json", "[]"])
def test_rejects_unreadable_snapshot(
    tmp_path: Path, inspect_mock: MagicMock, content: str | None
) -> None:
    """Propagate file loading errors before attempting registry access."""
    if content is not None:
        (tmp_path / "mapped.json").write_text(content, encoding="utf-8")
    error = (
        FileNotFoundError if content is None else TypeError if content == "[]" else ValueError
    )
    with pytest.raises(error):
        task.run(data_dir=tmp_path, snapshot_path=Path("mapped.json"))
    inspect_mock.assert_not_called()


def test_propagates_skopeo_failure(
    tmp_path: Path, component: dict[str, Any], inspect_mock: MagicMock
) -> None:
    """Stop validation when the registry inspection fails."""
    inspect_mock.side_effect = subprocess.CalledProcessError(
        1, "skopeo", stderr="unauthorized"
    )
    with pytest.raises(subprocess.CalledProcessError):
        task.run(data_dir=tmp_path, snapshot_path=_write_snapshot(tmp_path, [component]))


def test_rejects_invalid_manifest_json(
    tmp_path: Path, component: dict[str, Any], inspect_mock: MagicMock
) -> None:
    """Fail when skopeo returns an invalid manifest document."""
    inspect_mock.side_effect = None
    inspect_mock.return_value.stdout = "invalid json"
    with pytest.raises(json.JSONDecodeError):
        task.run(data_dir=tmp_path, snapshot_path=_write_snapshot(tmp_path, [component]))


def test_main_reads_env_and_propagates_validation_failure(
    tmp_path: Path,
    component: dict[str, Any],
    inspect_mock: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read the Tekton workspace paths and let validation failures escape main."""
    snapshot_path = _write_snapshot(tmp_path, [component])
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", str(snapshot_path))
    assert task.main() == 0

    component["repositories"][0]["tags"] = ["0.2.0"]
    _write_snapshot(tmp_path, [component])
    with pytest.raises(ValueError, match="none of the tags"):
        task.main()


@pytest.mark.parametrize("env_var", ["PARAM_DATA_DIR", "PARAM_SNAPSHOT_PATH"])
def test_main_requires_workspace_paths(monkeypatch: pytest.MonkeyPatch, env_var: str) -> None:
    """Fail early when a required Tekton parameter is missing."""
    monkeypatch.setenv("PARAM_DATA_DIR", "/workspace")
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "mapped.json")
    monkeypatch.delenv(env_var)
    with pytest.raises(SystemExit) as error:
        task.main()
    assert error.value.code == 1
