"""Tests for reduce_snapshot."""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest

from release_service_utils.tasks.managed.reduce_snapshot import (
    reduce_snapshot as reduce_snapshot_module,
)

TASK = "release_service_utils.tasks.managed.reduce_snapshot.reduce_snapshot"


def _write_snapshot(path: Path, components: list[dict[str, str]]) -> None:
    path.write_text(
        json.dumps({"application": "myapp", "components": components}),
        encoding="utf-8",
    )


def test_resolve_namespace_provided() -> None:
    """Return the provided namespace directly."""
    assert reduce_snapshot_module.resolve_namespace("my-ns") == "my-ns"


def test_resolve_namespace_from_service_account(tmp_path: Path) -> None:
    """Fall back to the service-account namespace file."""
    ns_file = tmp_path / "namespace"
    ns_file.write_text("sa-namespace\n", encoding="utf-8")
    with mock.patch.object(reduce_snapshot_module, "_SA_NAMESPACE_PATH", ns_file):
        assert reduce_snapshot_module.resolve_namespace("") == "sa-namespace"


def test_resolve_namespace_missing_file() -> None:
    """Raise ValueError when namespace is empty and file is unreadable."""
    with mock.patch.object(
        reduce_snapshot_module, "_SA_NAMESPACE_PATH", Path("/nonexistent/path")
    ):
        with pytest.raises(ValueError, match="cannot read"):
            reduce_snapshot_module.resolve_namespace("")


def test_get_cr_labels_parses_json() -> None:
    """Parse JSON output from get_resource."""
    raw = (
        '{"test.appstudio.openshift.io/type":"component",'
        '"appstudio.openshift.io/component":"foo"}'
    )
    with mock.patch(f"{TASK}.get_resource", return_value=raw):
        labels = reduce_snapshot_module.get_cr_labels("snapshot", "ns", "name")
    assert labels == {
        "test.appstudio.openshift.io/type": "component",
        "appstudio.openshift.io/component": "foo",
    }


def test_get_cr_labels_empty_response() -> None:
    """Return empty dict when get_resource returns '{}'."""
    with mock.patch(f"{TASK}.get_resource", return_value="{}"):
        assert reduce_snapshot_module.get_cr_labels("snapshot", "ns", "name") == {}


def test_get_cr_labels_empty_string() -> None:
    """Return empty dict when get_resource returns empty string."""
    with mock.patch(f"{TASK}.get_resource", return_value=""):
        assert reduce_snapshot_module.get_cr_labels("snapshot", "ns", "name") == {}


def test_validate_labels_valid() -> None:
    """Return component name when labels are valid."""
    labels = {
        "test.appstudio.openshift.io/type": "component",
        "appstudio.openshift.io/component": "my-comp",
    }
    assert reduce_snapshot_module.validate_labels(labels) == "my-comp"


def test_validate_labels_wrong_type() -> None:
    """Raise ValueError when type label is not 'component'."""
    labels = {
        "test.appstudio.openshift.io/type": "group",
        "appstudio.openshift.io/component": "my-comp",
    }
    with pytest.raises(ValueError, match="missing the required labels"):
        reduce_snapshot_module.validate_labels(labels)


def test_validate_labels_missing_component() -> None:
    """Raise ValueError when component label is empty."""
    labels = {"test.appstudio.openshift.io/type": "component"}
    with pytest.raises(ValueError, match="missing the required labels"):
        reduce_snapshot_module.validate_labels(labels)


def test_validate_labels_empty() -> None:
    """Raise ValueError when labels dict is empty."""
    with pytest.raises(ValueError, match="missing the required labels"):
        reduce_snapshot_module.validate_labels({})


def test_reduce_snapshot_success() -> None:
    """Filter to the matching component when it exists."""
    snapshot = {
        "application": "app",
        "components": [
            {"name": "scott", "containerImage": "img1"},
            {"name": "tom", "containerImage": "img2"},
        ],
    }
    result = reduce_snapshot_module.reduce_snapshot(snapshot, "tom")
    assert len(result["components"]) == 1
    assert result["components"][0]["name"] == "tom"
    assert result["application"] == "app"


def test_reduce_snapshot_component_not_found() -> None:
    """Return original snapshot when the target component is not found."""
    snapshot = {
        "components": [
            {"name": "scott", "containerImage": "img1"},
            {"name": "tom", "containerImage": "img2"},
        ]
    }
    result = reduce_snapshot_module.reduce_snapshot(snapshot, "scoobydoo")
    assert result is snapshot


def test_reduce_snapshot_no_components_key() -> None:
    """Return original snapshot when it has no components list."""
    snapshot = {"application": "app"}
    assert reduce_snapshot_module.reduce_snapshot(snapshot, "foo") is snapshot


def test_reduce_snapshot_components_not_list() -> None:
    """Return original snapshot when components is not a list."""
    snapshot = {"components": "bad"}
    assert reduce_snapshot_module.reduce_snapshot(snapshot, "foo") is snapshot


def test_run_single_component_disabled(tmp_path: Path) -> None:
    """Copy snapshot through when single component mode is disabled."""
    src = tmp_path / "snapshot.json"
    dst = tmp_path / "output.json"
    _write_snapshot(src, [{"name": "a", "containerImage": "i1"}])

    reduce_snapshot_module.run(
        single_component="false",
        custom_resource="snapshot/snap1",
        custom_resource_namespace="ns",
        snapshot_path=src,
        snapshot_output_path=dst,
    )
    assert json.loads(dst.read_text(encoding="utf-8")) == json.loads(
        src.read_text(encoding="utf-8")
    )


def test_run_single_component_disabled_same_path(tmp_path: Path) -> None:
    """Write snapshot back when input and output paths are the same."""
    src = tmp_path / "snapshot.json"
    _write_snapshot(src, [{"name": "a", "containerImage": "i1"}])
    original = json.loads(src.read_text(encoding="utf-8"))

    reduce_snapshot_module.run(
        single_component="false",
        custom_resource="snapshot/snap1",
        custom_resource_namespace="ns",
        snapshot_path=src,
        snapshot_output_path=src,
    )
    assert json.loads(src.read_text(encoding="utf-8")) == original


def test_run_reduces_snapshot(tmp_path: Path) -> None:
    """Reduce snapshot to the matching component."""
    src = tmp_path / "snapshot.json"
    dst = tmp_path / "output.json"
    _write_snapshot(
        src,
        [
            {"name": "scott", "containerImage": "img1"},
            {"name": "tom", "containerImage": "img2"},
        ],
    )

    labels_json = json.dumps(
        {
            "test.appstudio.openshift.io/type": "component",
            "appstudio.openshift.io/component": "tom",
        }
    )
    with mock.patch(f"{TASK}.get_resource", return_value=labels_json):
        reduce_snapshot_module.run(
            single_component="true",
            custom_resource="snapshot/snap-sample",
            custom_resource_namespace="default",
            snapshot_path=src,
            snapshot_output_path=dst,
        )

    result = json.loads(dst.read_text(encoding="utf-8"))
    assert len(result["components"]) == 1
    assert result["components"][0]["name"] == "tom"


def test_run_invalid_labels_raises(tmp_path: Path) -> None:
    """Raise ValueError when CR labels are invalid."""
    src = tmp_path / "snapshot.json"
    _write_snapshot(src, [{"name": "a", "containerImage": "i1"}])

    with mock.patch(f"{TASK}.get_resource", return_value="{}"):
        with pytest.raises(ValueError, match="missing the required labels"):
            reduce_snapshot_module.run(
                single_component="true",
                custom_resource="snapshot/snap1",
                custom_resource_namespace="ns",
                snapshot_path=src,
                snapshot_output_path=tmp_path / "out.json",
            )


def test_run_component_not_found_writes_original(tmp_path: Path) -> None:
    """Write original snapshot when target component is not found."""
    src = tmp_path / "snapshot.json"
    dst = tmp_path / "output.json"
    _write_snapshot(
        src,
        [
            {"name": "scott", "containerImage": "img1"},
            {"name": "tom", "containerImage": "img2"},
        ],
    )

    labels_json = json.dumps(
        {
            "test.appstudio.openshift.io/type": "component",
            "appstudio.openshift.io/component": "scoobydoo",
        }
    )
    with mock.patch(f"{TASK}.get_resource", return_value=labels_json):
        reduce_snapshot_module.run(
            single_component="true",
            custom_resource="snapshot/snap1",
            custom_resource_namespace="default",
            snapshot_path=src,
            snapshot_output_path=dst,
        )

    result = json.loads(dst.read_text(encoding="utf-8"))
    assert len(result["components"]) == 2


def test_run_empty_namespace_reads_sa(tmp_path: Path) -> None:
    """Read namespace from service-account file when env var is empty."""
    src = tmp_path / "snapshot.json"
    dst = tmp_path / "output.json"
    _write_snapshot(src, [{"name": "tom", "containerImage": "img"}])

    ns_file = tmp_path / "namespace"
    ns_file.write_text("inferred-ns", encoding="utf-8")

    labels_json = json.dumps(
        {
            "test.appstudio.openshift.io/type": "component",
            "appstudio.openshift.io/component": "tom",
        }
    )
    with (
        mock.patch.object(reduce_snapshot_module, "_SA_NAMESPACE_PATH", ns_file),
        mock.patch(f"{TASK}.get_resource", return_value=labels_json) as mock_gr,
    ):
        reduce_snapshot_module.run(
            single_component="true",
            custom_resource="snapshot/snap1",
            custom_resource_namespace="",
            snapshot_path=src,
            snapshot_output_path=dst,
        )

    mock_gr.assert_called_once_with("snapshot", "inferred-ns", "snap1", "{.metadata.labels}")
    result = json.loads(dst.read_text(encoding="utf-8"))
    assert result["components"][0]["name"] == "tom"


def test_run_invalid_custom_resource_format(tmp_path: Path) -> None:
    """Raise ValueError when CUSTOM_RESOURCE has no slash."""
    src = tmp_path / "snapshot.json"
    _write_snapshot(src, [{"name": "a", "containerImage": "i1"}])

    with pytest.raises(ValueError, match="type/name"):
        reduce_snapshot_module.run(
            single_component="true",
            custom_resource="badformat",
            custom_resource_namespace="ns",
            snapshot_path=src,
            snapshot_output_path=tmp_path / "out.json",
        )


def test_main_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """main() reads env vars and calls run() successfully."""
    src = tmp_path / "snapshot.json"
    dst = tmp_path / "output.json"
    _write_snapshot(src, [{"name": "comp1", "containerImage": "img"}])

    monkeypatch.setenv("SINGLE_COMPONENT", "false")
    monkeypatch.setenv("CUSTOM_RESOURCE", "snapshot/s1")
    monkeypatch.setenv("CUSTOM_RESOURCE_NAMESPACE", "ns")
    monkeypatch.setenv("SNAPSHOT", str(src))
    monkeypatch.setenv("SNAPSHOT_PATH", str(dst))

    assert reduce_snapshot_module.main() == 0
    assert dst.exists()


def test_main_missing_snapshot_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() exits when SNAPSHOT is not set."""
    monkeypatch.delenv("SNAPSHOT", raising=False)
    monkeypatch.delenv("SNAPSHOT_PATH", raising=False)
    with pytest.raises(SystemExit):
        reduce_snapshot_module.main()


def test_main_missing_snapshot_path_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """main() exits when SNAPSHOT_PATH is not set."""
    src = tmp_path / "snapshot.json"
    _write_snapshot(src, [])
    monkeypatch.setenv("SNAPSHOT", str(src))
    monkeypatch.delenv("SNAPSHOT_PATH", raising=False)
    with pytest.raises(SystemExit):
        reduce_snapshot_module.main()
