"""Test the ``check_labels`` module."""

from __future__ import annotations

import json
import logging
import runpy
from pathlib import Path

import sys as _sys
import check_labels
import check_labels.check_labels  # ensure submodule is loaded

import pytest

_check_labels_mod = _sys.modules["check_labels.check_labels"]


@pytest.fixture(autouse=True)
def _propagate_release_logger() -> None:
    """Allow caplog to capture records from the 'release' logger."""
    release_logger = logging.getLogger("release")
    release_logger.propagate = True
    yield
    release_logger.propagate = False


def _write_snapshot(path: Path, components: list[dict]) -> None:
    """Write a snapshot JSON file with the given components list."""
    path.write_text(
        json.dumps({"application": "myapp", "components": components}),
        encoding="utf-8",
    )


def _write_data(path: Path, cpe: str = "cpe:/a:example:openstack:el8") -> None:
    """Write a data JSON file with the given CPE value."""
    path.write_text(
        json.dumps({"releaseNotes": {"cpe": cpe}}),
        encoding="utf-8",
    )


def _make_component(
    name: str = "comp1",
    media_type: str = "application/vnd.oci.image.config.v1+json",
    labels: list[dict] | None = None,
    repositories: list[dict] | None = None,
    canonical_name: str | None = None,
) -> dict:
    """Build a component dict for snapshot JSON."""
    comp: dict = {"name": name, "metadata": {"media_type": media_type}}
    if labels is not None:
        comp["metadata"]["labels"] = labels
    if repositories is not None:
        comp["repositories"] = repositories
    if canonical_name is not None:
        comp["canonicalName"] = canonical_name
    return comp


def _name_label(value: str) -> dict:
    return {"name": "name", "value": value}


def _cpe_label(value: str) -> dict:
    return {"name": "cpe", "value": value}


# --- derive_name_from_url ---


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "registry.redhat.io/openshift-gitops-1/gitops-rhel8-operator",
            "openshift-gitops-1/gitops-rhel8-operator",
        ),
        ("docker://registry.redhat.io/ubi9/ubi", "ubi9/ubi"),
        ("registry.redhat.io/openshift-gitops-1/foo:tag", "openshift-gitops-1/foo"),
        ("registry.redhat.io/repo@sha256:abc", "repo"),
        (
            " registry.redhat.io/openshift-gitops-1/gitops-rhel8-operator",
            "openshift-gitops-1/gitops-rhel8-operator",
        ),
    ],
)
def test_derive_name_from_url(url: str, expected: str) -> None:
    """Derive namespace/repo from a container image URL."""
    assert check_labels.derive_name_from_url(url) == expected


# --- get_label_value ---


def test_get_label_value_found() -> None:
    """Return the value when the label exists."""
    comp = _make_component(labels=[_name_label("myname")])
    assert check_labels.get_label_value(comp, "name") == "myname"


def test_get_label_value_missing() -> None:
    """Return None when the label is absent."""
    comp = _make_component(labels=[])
    assert check_labels.get_label_value(comp, "name") is None


def test_get_label_value_no_labels_key() -> None:
    """Return None when metadata has no labels at all."""
    comp = _make_component()
    assert check_labels.get_label_value(comp, "name") is None


# --- is_image_media_type ---


def test_is_image_media_type_oci() -> None:
    """OCI image config is recognized as an image."""
    comp = _make_component(media_type="application/vnd.oci.image.config.v1+json")
    assert check_labels.is_image_media_type(comp) is True


def test_is_image_media_type_docker() -> None:
    """Docker image config is recognized as an image."""
    comp = _make_component(media_type="application/vnd.docker.container.image.v1+json")
    assert check_labels.is_image_media_type(comp) is True


def test_is_image_media_type_other() -> None:
    """Non-image media types are not images."""
    comp = _make_component(media_type="something weird, not an image")
    assert check_labels.is_image_media_type(comp) is False


# --- check_labels (happy path) ---


def test_single_repo_matching_labels(tmp_path: Path) -> None:
    """Pass when both name and CPE labels match expected values."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[
                    _name_label("openshift-gitops-1/gitops-rhel8-operator"),
                    _cpe_label("cpe:/a:example:openstack:el8"),
                ],
                repositories=[
                    {
                        "rh-registry-repo": "registry.redhat.io/openshift-gitops-1"
                        "/gitops-rhel8-operator"
                    }
                ],
            )
        ],
    )
    _write_data(data)
    check_labels.check_labels(snap, data, enforce=True)


def test_non_image_artifact_skipped(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Non-image artifacts are skipped with an info log."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [_make_component(media_type="something weird, not an image")],
    )
    _write_data(data)
    with caplog.at_level(logging.INFO, logger="release"):
        check_labels.check_labels(snap, data, enforce=True)
    assert "Skipping check for artifact" in caplog.text


def test_no_cpe_label_skips(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Missing CPE label logs info and passes (no enforcement)."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[_name_label("openshift-gitops-1/gitops-rhel8-operator")],
                repositories=[
                    {
                        "rh-registry-repo": "registry.redhat.io/openshift-gitops-1"
                        "/gitops-rhel8-operator"
                    }
                ],
            )
        ],
    )
    _write_data(data)
    with caplog.at_level(logging.INFO, logger="release"):
        check_labels.check_labels(snap, data, enforce=True)
    assert "missing the 'cpe' label" in caplog.text


# --- check_labels (name label failures) ---


def test_name_label_mismatch_enforce(tmp_path: Path) -> None:
    """Fail when name label mismatches and enforce is True."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[
                    _name_label("foo"),
                    _cpe_label("cpe:/a:example:openstack:el8"),
                ],
                repositories=[
                    {
                        "rh-registry-repo": "registry.redhat.io/openshift-gitops-1"
                        "/gitops-rhel8-operator"
                    }
                ],
            )
        ],
    )
    _write_data(data)
    with pytest.raises(check_labels.LabelValidationError, match="does not match"):
        check_labels.check_labels(snap, data, enforce=True)


def test_name_label_mismatch_warn(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Warn but succeed when name label mismatches and enforce is False."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[
                    _name_label("foo"),
                    _cpe_label("cpe:/a:example:openstack:el8"),
                ],
                repositories=[
                    {
                        "rh-registry-repo": "registry.redhat.io/openshift-gitops-1"
                        "/gitops-rhel8-operator"
                    }
                ],
            )
        ],
    )
    _write_data(data)
    with caplog.at_level(logging.WARNING, logger="release"):
        check_labels.check_labels(snap, data, enforce=False)
    assert "name label" in caplog.text
    assert "does not match" in caplog.text


def test_missing_name_label_enforce(tmp_path: Path) -> None:
    """Fail when the name label is entirely missing and enforce is True."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[_cpe_label("cpe:/a:example:openstack:el8")],
                repositories=[
                    {
                        "rh-registry-repo": "registry.redhat.io/openshift-gitops-1"
                        "/gitops-rhel8-operator"
                    }
                ],
            )
        ],
    )
    _write_data(data)
    with pytest.raises(
        check_labels.LabelValidationError,
        match="missing the required container label 'name'",
    ):
        check_labels.check_labels(snap, data, enforce=True)


def test_missing_name_label_warn(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Warn but succeed when the name label is missing and enforce is False."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[_cpe_label("cpe:/a:example:openstack:el8")],
                repositories=[
                    {
                        "rh-registry-repo": "registry.redhat.io/openshift-gitops-1"
                        "/gitops-rhel8-operator"
                    }
                ],
            )
        ],
    )
    _write_data(data)
    with caplog.at_level(logging.WARNING, logger="release"):
        check_labels.check_labels(snap, data, enforce=False)
    assert "missing the required container label 'name'" in caplog.text


def test_no_canonical_with_multiple_repos_warn(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Warn when multiple repos lack canonicalName and enforce is False."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[_name_label("openshift-gitops-1/gitops-rhel8-operator")],
                repositories=[
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/foo"},
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/bar"},
                ],
            )
        ],
    )
    _write_data(data)
    with caplog.at_level(logging.WARNING, logger="release"):
        check_labels.check_labels(snap, data, enforce=False)
    assert "canonicalName" in caplog.text


# --- check_labels (CPE label failures) ---


def test_cpe_label_mismatch_enforce(tmp_path: Path) -> None:
    """Fail when CPE label mismatches and enforce is True."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[
                    _name_label("openshift-gitops-1/gitops-rhel8-operator"),
                    _cpe_label("wrong-cpe"),
                ],
                repositories=[
                    {
                        "rh-registry-repo": "registry.redhat.io/openshift-gitops-1"
                        "/gitops-rhel8-operator"
                    }
                ],
            )
        ],
    )
    _write_data(data)
    with pytest.raises(check_labels.LabelValidationError, match="does not match"):
        check_labels.check_labels(snap, data, enforce=True)


def test_cpe_label_mismatch_warn(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Warn but succeed when CPE label mismatches and enforce is False."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[
                    _name_label("openshift-gitops-1/gitops-rhel8-operator"),
                    _cpe_label("wrong-cpe"),
                ],
                repositories=[
                    {
                        "rh-registry-repo": "registry.redhat.io/openshift-gitops-1"
                        "/gitops-rhel8-operator"
                    }
                ],
            )
        ],
    )
    _write_data(data)
    with caplog.at_level(logging.WARNING, logger="release"):
        check_labels.check_labels(snap, data, enforce=False)
    assert "'cpe' label" in caplog.text
    assert "does not match" in caplog.text


# --- check_labels (canonical name) ---


def test_canonical_name_with_multiple_repos(tmp_path: Path) -> None:
    """Pass when canonicalName is provided with multiple repositories."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                canonical_name="openshift-gitops-1/gitops-rhel8-operator",
                labels=[_name_label("openshift-gitops-1/gitops-rhel8-operator")],
                repositories=[
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/foo"},
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/bar"},
                ],
            )
        ],
    )
    _write_data(data)
    check_labels.check_labels(snap, data, enforce=True)


def test_no_canonical_with_multiple_repos_enforce(
    tmp_path: Path,
) -> None:
    """Fail when multiple repos exist but canonicalName is missing."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[_name_label("openshift-gitops-1/gitops-rhel8-operator")],
                repositories=[
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/foo"},
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/bar"},
                ],
            )
        ],
    )
    _write_data(data)
    with pytest.raises(check_labels.LabelValidationError, match="canonicalName"):
        check_labels.check_labels(snap, data, enforce=True)


def test_canonical_overrides_url(tmp_path: Path) -> None:
    """Use canonicalName even with a single repository."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                canonical_name="ubi9",
                labels=[
                    _name_label("ubi9"),
                    _cpe_label("cpe:/a:example:openstack:el8"),
                ],
                repositories=[{"rh-registry-repo": "registry.redhat.io/ubi9/ubi"}],
            )
        ],
    )
    _write_data(data)
    check_labels.check_labels(snap, data, enforce=True)


def test_no_match_canonical_enforce(tmp_path: Path) -> None:
    """Fail when name label doesn't match canonicalName with enforce."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                canonical_name="openshift-gitops-1/gitops-rhel8-operator",
                labels=[_name_label("foo")],
                repositories=[
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/foo"},
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/bar"},
                ],
            )
        ],
    )
    _write_data(data)
    with pytest.raises(check_labels.LabelValidationError, match="does not match"):
        check_labels.check_labels(snap, data, enforce=True)


# --- check_labels (multiple components) ---


def test_multiple_components(tmp_path: Path) -> None:
    """Pass when multiple components all have valid labels."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                name="comp1",
                canonical_name="openshift-gitops-1/gitops-rhel8-operator",
                labels=[_name_label("openshift-gitops-1/gitops-rhel8-operator")],
                repositories=[
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/foo"},
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/bar"},
                ],
            ),
            _make_component(
                name="comp2",
                canonical_name="openshift-gitops-2/gitops-rhel8-operator",
                labels=[_name_label("openshift-gitops-2/gitops-rhel8-operator")],
                repositories=[
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/foo"},
                    {"rh-registry-repo": "registry.redhat.io/openshift-gitops-1/bar"},
                ],
            ),
        ],
    )
    _write_data(data)
    check_labels.check_labels(snap, data, enforce=True)


# --- check_labels (error cases) ---


def test_missing_snapshot_file(tmp_path: Path) -> None:
    """Raise when the snapshot file does not exist."""
    data = tmp_path / "data.json"
    _write_data(data)
    with pytest.raises(check_labels.LabelValidationError, match="missing.json"):
        check_labels.check_labels(tmp_path / "missing.json", data, enforce=True)


def test_missing_data_file(tmp_path: Path) -> None:
    """Raise when the data file does not exist."""
    snap = tmp_path / "snapshot.json"
    _write_snapshot(snap, [])
    with pytest.raises(check_labels.LabelValidationError, match="missing.json"):
        check_labels.check_labels(snap, tmp_path / "missing.json", enforce=True)


def test_missing_cpe_in_data(tmp_path: Path) -> None:
    """Raise when releaseNotes.cpe is missing from the data file."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(snap, [_make_component(labels=[_name_label("foo")])])
    data.write_text(json.dumps({"releaseNotes": {}}), encoding="utf-8")
    with pytest.raises(check_labels.LabelValidationError, match="releaseNotes.cpe"):
        check_labels.check_labels(snap, data, enforce=True)


def test_missing_component_name(tmp_path: Path) -> None:
    """Raise when a component has no 'name' field."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            {
                "metadata": {
                    "media_type": "application/vnd.oci.image.config.v1+json",
                    "labels": [_name_label("foo")],
                },
                "repositories": [{"rh-registry-repo": "registry.redhat.io/ns/repo"}],
            }
        ],
    )
    _write_data(data)
    with pytest.raises(check_labels.LabelValidationError, match="missing.*name"):
        check_labels.check_labels(snap, data, enforce=True)


def test_missing_rh_registry_repo(tmp_path: Path) -> None:
    """Raise when rh-registry-repo is missing from single-repo component."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [_make_component(labels=[_name_label("foo")], repositories=[{}])],
    )
    _write_data(data)
    with pytest.raises(check_labels.LabelValidationError, match="rh-registry-repo"):
        check_labels.check_labels(snap, data, enforce=True)


def test_unexpected_error(tmp_path: Path) -> None:
    """Propagate unexpected exceptions as tracebacks."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    snap.write_text(
        json.dumps(
            {
                "application": "myapp",
                "components": [{"name": "comp1", "metadata": 42}],
            }
        ),
        encoding="utf-8",
    )
    _write_data(data)
    with pytest.raises(AttributeError):
        check_labels.check_labels(snap, data, enforce=True)


# --- parse_args ---


def test_parse_args_valid() -> None:
    """Parse valid arguments successfully."""
    ns = check_labels.parse_args(
        ["--snapshot-file", "/a/snap.json", "--data-file", "/b/data.json"]
    )
    assert ns.snapshot_file == "/a/snap.json"
    assert ns.data_file == "/b/data.json"
    assert ns.enforce is False


def test_parse_args_with_enforce_true() -> None:
    """Parse --enforce true as a boolean True."""
    ns = check_labels.parse_args(
        ["--snapshot-file", "s.json", "--data-file", "d.json", "--enforce", "true"]
    )
    assert ns.enforce is True


def test_parse_args_with_enforce_false() -> None:
    """Parse --enforce false as a boolean False."""
    ns = check_labels.parse_args(
        ["--snapshot-file", "s.json", "--data-file", "d.json", "--enforce", "false"]
    )
    assert ns.enforce is False


def test_parse_args_missing_required() -> None:
    """Exit when required arguments are missing."""
    with pytest.raises(SystemExit) as exc:
        check_labels.parse_args([])
    assert exc.value.code == 2


# --- main ---


def test_main_success(tmp_path: Path) -> None:
    """Return 0 when labels match."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[
                    _name_label("openshift-gitops-1/gitops-rhel8-operator"),
                    _cpe_label("cpe:/a:example:openstack:el8"),
                ],
                repositories=[
                    {
                        "rh-registry-repo": "registry.redhat.io/openshift-gitops-1"
                        "/gitops-rhel8-operator"
                    }
                ],
            )
        ],
    )
    _write_data(data)
    rc = check_labels.main(
        [
            "check_labels.py",
            "--snapshot-file",
            str(snap),
            "--data-file",
            str(data),
            "--enforce",
            "true",
        ]
    )
    assert rc == 0


def test_main_failure(tmp_path: Path) -> None:
    """Propagate LabelValidationError when label check fails with enforce."""
    snap = tmp_path / "snapshot.json"
    data = tmp_path / "data.json"
    _write_snapshot(
        snap,
        [
            _make_component(
                labels=[_name_label("wrong")],
                repositories=[
                    {
                        "rh-registry-repo": "registry.redhat.io/openshift-gitops-1"
                        "/gitops-rhel8-operator"
                    }
                ],
            )
        ],
    )
    _write_data(data)
    with pytest.raises(check_labels.LabelValidationError):
        check_labels.main(
            [
                "check_labels.py",
                "--snapshot-file",
                str(snap),
                "--data-file",
                str(data),
                "--enforce",
                "true",
            ]
        )


def test_main_bad_args() -> None:
    """Exit with SystemExit on invalid arguments."""
    with pytest.raises(SystemExit):
        check_labels.main(["check_labels.py"])


def test_module_main_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Execute the file as __main__ triggers SystemExit."""
    monkeypatch.setattr("sys.argv", ["check_labels.py"])
    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(Path(_check_labels_mod.__file__)), run_name="__main__")
    assert exc.value.code != 0
