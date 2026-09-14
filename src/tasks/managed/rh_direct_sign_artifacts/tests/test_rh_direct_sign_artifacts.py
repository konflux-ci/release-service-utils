"""Unit tests for rh_direct_sign_artifacts."""

from __future__ import annotations

import io
import json
import logging
import tarfile
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

from release_service_utils.tasks.managed.rh_direct_sign_artifacts import (
    SubmitConfig,
    main,
    prepare_all_components,
    prepare_component,
    pull_and_extract,
    setup_argparser,
    submit_all_signing_requests,
    submit_signing_request,
)
from release_service_utils.tasks.managed.rh_direct_sign_artifacts.rh_direct_sign_artifacts import (  # noqa: E501
    _extract_oras_blobs,
    _extract_tar_layers,
)

TASK = (
    "release_service_utils.tasks.managed" ".rh_direct_sign_artifacts.rh_direct_sign_artifacts"
)

FLAT_MEDIA_TYPE = "application/vnd.oci.empty.v1+json"

FULL_CONFIGMAP = {
    "data": {
        "SIG_KEY_NAME": "key-a",
        "KERBEROS_PRINCIPAL": "svc@REALM",
        "KERBEROS_KEYTAB": "/etc/keytab",
        "KERBEROS_KEYTAB_SECRET": "keytab-secret",
    }
}


@pytest.fixture(autouse=True)
def _propagate_release_logger() -> None:
    """Allow caplog to capture records from the 'release' logger."""
    release_logger = logging.getLogger("release")
    release_logger.propagate = True
    yield
    release_logger.propagate = False


def _make_config(**overrides: object) -> SubmitConfig:
    """Return a SubmitConfig with safe defaults."""
    defaults: dict[str, object] = dict(
        pipeline="middleware-signing",
        requester="tester",
        kerberos_keytab_secret="keytab-secret",
        kerberos_keytab="/etc/keytab",
        kerberos_principal="svc@REALM",
        signing_repo="https://gitlab.cee.redhat.com/signing/signing.git",
        signing_revision="main",
        service_account="signing-pipeline-sa",
        request_timeout="1800",
        task_id="task-uid-123",
        pipelinerun_uid="pr-uid-456",
        concurrent_limit=4,
        intention="release",
        oci_storage="quay.io/org/ta",
        oras_options="",
        exclude=[],
    )
    defaults.update(overrides)
    return SubmitConfig(**defaults)


def _write_flat_oras_image(image_dir: Path, files: dict[str, bytes]) -> None:
    """Write a fake flat ORAS artifact (manifest + blobs) into *image_dir*."""
    layers = []
    for name, content in files.items():
        digest = f"sha256:{name.replace('.', '')}"
        blob_path = image_dir / digest.removeprefix("sha256:")
        blob_path.write_bytes(content)
        layers.append(
            {
                "digest": digest,
                "annotations": {"org.opencontainers.image.title": name},
            }
        )
    manifest = {
        "config": {"mediaType": FLAT_MEDIA_TYPE},
        "layers": layers,
    }
    (image_dir / "manifest.json").write_text(json.dumps(manifest))


def _write_layered_image(image_dir: Path, files: dict[str, bytes]) -> None:
    """Write a fake layered container image (manifest + tar layers)."""
    tar_path = image_dir / "layerdata"
    with tarfile.open(str(tar_path), "w") as tf:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
    digest = "sha256:layerdata"
    tar_path.rename(image_dir / "layerdata")
    manifest = {
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json"},
        "layers": [{"digest": digest}],
    }
    (image_dir / "manifest.json").write_text(json.dumps(manifest))


# --- setup_argparser ---


def test_setup_argparser_defaults(tmp_path: Path) -> None:
    """Verify default values for optional arguments."""
    snap = tmp_path / "snap.json"
    snap.write_text("{}")
    data = tmp_path / "data.json"
    data.write_text("{}")

    parser = setup_argparser()
    args = parser.parse_args(
        [
            "--snapshot",
            str(snap),
            "--data-file",
            str(data),
            "--requester",
            "tester",
        ]
    )

    assert args.pipeline == "middleware-signing"
    assert args.service_account == "signing-pipeline-sa"
    assert args.request_timeout == "1800"
    assert args.concurrent_limit == 4
    assert args.oci_storage == "empty"
    assert args.oras_options == ""
    assert args.verbose is False


def test_setup_argparser_rejects_missing_file(tmp_path: Path) -> None:
    """validate_file raises FileNotFoundError for nonexistent paths."""
    parser = setup_argparser()
    with pytest.raises(FileNotFoundError):
        parser.parse_args(
            [
                "--snapshot",
                str(tmp_path / "nope.json"),
                "--data-file",
                str(tmp_path / "also-nope.json"),
                "--requester",
                "tester",
            ]
        )


def test_setup_argparser_requires_requester(tmp_path: Path) -> None:
    """--requester is mandatory."""
    snap = tmp_path / "snap.json"
    snap.write_text("{}")
    data = tmp_path / "data.json"
    data.write_text("{}")

    parser = setup_argparser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--snapshot",
                str(snap),
                "--data-file",
                str(data),
            ]
        )


# --- _extract_oras_blobs ---


def test_extract_oras_blobs(tmp_path: Path) -> None:
    """Flat ORAS blobs are extracted by title annotation."""
    image_dir = tmp_path / "image"
    image_dir.mkdir()
    _write_flat_oras_image(image_dir, {"binary.tar.gz": b"data"})
    manifest = json.loads((image_dir / "manifest.json").read_text())

    dest = tmp_path / "out"
    dest.mkdir()
    _extract_oras_blobs(manifest, image_dir, dest)

    assert (dest / "binary.tar.gz").read_bytes() == b"data"


def test_extract_oras_blobs_rejects_path_traversal(tmp_path: Path) -> None:
    """Titles with '..' segments are rejected."""
    image_dir = tmp_path / "image"
    image_dir.mkdir()
    manifest = {
        "config": {"mediaType": FLAT_MEDIA_TYPE},
        "layers": [
            {
                "digest": "sha256:abc",
                "annotations": {"org.opencontainers.image.title": "../../../etc/passwd"},
            }
        ],
    }
    (image_dir / "abc").write_bytes(b"evil")
    (image_dir / "manifest.json").write_text(json.dumps(manifest))

    dest = tmp_path / "out"
    dest.mkdir()
    with pytest.raises(RuntimeError, match="unsafe title"):
        _extract_oras_blobs(manifest, image_dir, dest)


def test_extract_oras_blobs_skips_missing_title(tmp_path: Path) -> None:
    """Layers without a title annotation are silently skipped."""
    image_dir = tmp_path / "image"
    image_dir.mkdir()
    manifest = {
        "config": {"mediaType": FLAT_MEDIA_TYPE},
        "layers": [{"digest": "sha256:abc", "annotations": {}}],
    }
    (image_dir / "abc").write_bytes(b"data")
    (image_dir / "manifest.json").write_text(json.dumps(manifest))

    dest = tmp_path / "out"
    dest.mkdir()
    _extract_oras_blobs(manifest, image_dir, dest)
    assert list(dest.iterdir()) == []


# --- _extract_tar_layers ---


def test_extract_tar_layers(tmp_path: Path) -> None:
    """Tar layers are extracted via safe_extract_archive."""
    image_dir = tmp_path / "image"
    image_dir.mkdir()
    _write_layered_image(image_dir, {"hello.txt": b"world"})
    manifest = json.loads((image_dir / "manifest.json").read_text())

    dest = tmp_path / "out"
    dest.mkdir()
    _extract_tar_layers(manifest, image_dir, dest)

    assert (dest / "hello.txt").read_bytes() == b"world"


# --- pull_and_extract ---


def test_pull_and_extract_flat_oras(tmp_path: Path) -> None:
    """pull_and_extract handles flat ORAS artifacts."""

    def fake_copy(src, dest, **_kwargs):
        _write_flat_oras_image(dest, {"tool.bin": b"binary"})

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    with patch(f"{TASK}.skopeo.copy", side_effect=fake_copy):
        result = pull_and_extract("quay.io/org/img@sha256:abc", work_dir)

    assert (result / "tool.bin").read_bytes() == b"binary"


def test_pull_and_extract_layered_image(tmp_path: Path) -> None:
    """pull_and_extract handles standard layered images."""

    def fake_copy(src, dest, **_kwargs):
        _write_layered_image(dest, {"app.bin": b"content"})

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    with patch(f"{TASK}.skopeo.copy", side_effect=fake_copy):
        result = pull_and_extract("quay.io/org/img@sha256:abc", work_dir)

    assert (result / "app.bin").read_bytes() == b"content"


# --- prepare_component ---


def test_prepare_component_creates_zip_and_pushes(tmp_path: Path) -> None:
    """prepare_component zips extracted files and pushes via oras."""

    def fake_copy(src, dest, **_kwargs):
        _write_flat_oras_image(dest, {"file.bin": b"data"})

    pushed_args: dict[str, object] = {}

    def fake_oras_push(tag, directory, subdir, name, *, extra_args=None):
        pushed_args["tag"] = tag
        pushed_args["subdir"] = subdir
        pushed_args["extra_args"] = extra_args
        zp = directory / subdir
        assert zipfile.is_zipfile(zp)
        return "sha256:deadbeef"

    with (
        patch(f"{TASK}.skopeo.copy", side_effect=fake_copy),
        patch(f"{TASK}.oras_push", side_effect=fake_oras_push),
    ):
        uri = prepare_component("my-comp", "quay.io/img@sha256:abc", "quay.io/org/ta", None)

    assert uri == "oci://quay.io/org/ta@sha256:deadbeef"
    assert pushed_args["tag"] == "quay.io/org/ta:my-comp"
    assert pushed_args["extra_args"] is None


def test_prepare_component_passes_extra_args(tmp_path: Path) -> None:
    """prepare_component forwards oras_extra_args to oras_push."""

    def fake_copy(src, dest, **_kwargs):
        _write_flat_oras_image(dest, {"f.bin": b"x"})

    captured_extra: list[list[str] | None] = []

    def fake_oras_push(tag, directory, subdir, name, *, extra_args=None):
        captured_extra.append(extra_args)
        return "sha256:aaa"

    with (
        patch(f"{TASK}.skopeo.copy", side_effect=fake_copy),
        patch(f"{TASK}.oras_push", side_effect=fake_oras_push),
    ):
        prepare_component(
            "c",
            "img@sha256:a",
            "quay.io/ta",
            ["--registry-config", "/auth.json"],
        )

    assert captured_extra[0] == ["--registry-config", "/auth.json"]


def test_prepare_component_raises_on_empty_extract(tmp_path: Path) -> None:
    """prepare_component raises if no files are extracted."""

    def fake_copy(src, dest, **_kwargs):
        manifest = {
            "config": {"mediaType": FLAT_MEDIA_TYPE},
            "layers": [],
        }
        (dest / "manifest.json").write_text(json.dumps(manifest))

    with patch(f"{TASK}.skopeo.copy", side_effect=fake_copy):
        with pytest.raises(RuntimeError, match="No files extracted"):
            prepare_component("c", "img@sha256:a", "quay.io/ta", None)


# --- prepare_all_components ---


def test_prepare_all_components_succeeds() -> None:
    """prepare_all_components returns a mapping of name to TA URI."""
    components = [
        {"name": "a", "containerImage": "img-a@sha256:aaa"},
        {"name": "b", "containerImage": "img-b@sha256:bbb"},
    ]

    def fake_prepare(name, image, storage, args):
        return f"oci://{storage}@sha256:{name}"

    with patch(f"{TASK}.prepare_component", side_effect=fake_prepare):
        result = prepare_all_components(components, "quay.io/ta", None, 4)

    assert result == {
        "a": "oci://quay.io/ta@sha256:a",
        "b": "oci://quay.io/ta@sha256:b",
    }


def test_prepare_all_components_raises_on_failure() -> None:
    """prepare_all_components raises when any component fails."""
    components = [
        {"name": "a", "containerImage": "img-a@sha256:aaa"},
        {"name": "b", "containerImage": "img-b@sha256:bbb"},
    ]

    def fake_prepare(name, image, storage, args):
        if name == "b":
            raise RuntimeError("pull failed")
        return f"oci://{storage}@sha256:{name}"

    with patch(f"{TASK}.prepare_component", side_effect=fake_prepare):
        with pytest.raises(RuntimeError, match="1 component"):
            prepare_all_components(components, "quay.io/ta", None, 4)


# --- submit_signing_request ---


def test_submit_signing_request_calls_create_with_expected_params() -> None:
    """submit_signing_request passes correct params and labels."""
    config = _make_config()

    with patch(f"{TASK}.create_internal_request") as mock_create:
        mock_create.return_value = "middleware-signing-xyz"
        submit_signing_request("comp-a", "oci://quay.io/ta@sha256:aaa", "key-a", config)

    mock_create.assert_called_once()
    call_kwargs = mock_create.call_args
    assert call_kwargs.args[0] == "middleware-signing"
    params = call_kwargs.kwargs["params"]
    assert params["sourceDataArtifact"] == "oci://quay.io/ta@sha256:aaa"
    assert params["keyname"] == "key-a"
    assert params["onbehalfof"] == "tester"
    assert params["kerberos_keytab_secret"] == "keytab-secret"
    assert params["kerberos_keytab"] == "/etc/keytab"
    assert params["kerberos_principal"] == "svc@REALM"
    assert params["taskGitUrl"] == ("https://gitlab.cee.redhat.com/signing/signing.git")
    assert params["taskGitRevision"] == "main"
    assert params["ociStorage"] == "quay.io/org/ta"
    assert "exclude" not in params
    labels = call_kwargs.kwargs["labels"]
    assert labels["internal-services.appstudio.openshift.io/group-id"] == "task-uid-123"
    assert labels["internal-services.appstudio.openshift.io/pipelinerun-uid"] == "pr-uid-456"
    assert labels["internal-services.appstudio.openshift.io/intention"] == "release"
    assert labels["internal-services.appstudio.openshift.io/rate-limited"] == "true"
    assert (
        labels["internal-services.appstudio.openshift.io/rate-limiting-group"]
        == "signing-server"
    )
    assert call_kwargs.kwargs["sync"] is True
    assert call_kwargs.kwargs["timeout"] == 1800
    assert call_kwargs.kwargs["service_account"] == "signing-pipeline-sa"
    assert call_kwargs.kwargs["cleanup"] is False


def test_submit_signing_request_includes_exclude_when_set() -> None:
    """submit_signing_request adds exclude param when config has patterns."""
    config = _make_config(exclude=[".*\\.md$", "checksums\\.txt"])

    with patch(f"{TASK}.create_internal_request") as mock_create:
        mock_create.return_value = "ir-name"
        submit_signing_request("comp-a", "oci://ta@sha256:aaa", "key-a", config)

    params = mock_create.call_args.kwargs["params"]
    assert json.loads(params["exclude"]) == [
        ".*\\.md$",
        "checksums\\.txt",
    ]


def test_submit_signing_request_raises_on_wait_error() -> None:
    """submit_signing_request wraps InternalRequestWaitError."""
    from release_service_utils.helpers.internal_request import (
        InternalRequestWaitError,
    )

    config = _make_config()

    with patch(
        f"{TASK}.create_internal_request",
        side_effect=InternalRequestWaitError("timed out", 124),
    ):
        with pytest.raises(RuntimeError, match="Signing failed for 'comp-a'"):
            submit_signing_request("comp-a", "oci://ta@sha256:aaa", "key-a", config)


# --- submit_all_signing_requests ---


def test_submit_all_signing_requests_succeeds() -> None:
    """submit_all_signing_requests completes without error."""
    prepared = {
        "a": "oci://ta@sha256:aaa",
        "b": "oci://ta@sha256:bbb",
    }
    config = _make_config()

    with patch(f"{TASK}.submit_signing_request") as mock_submit:
        submit_all_signing_requests(prepared, ["key-1"], config)

    assert mock_submit.call_count == 2


def test_submit_all_signing_requests_multiple_keys() -> None:
    """submit_all_signing_requests creates one IR per component x key."""
    prepared = {"a": "oci://ta@sha256:aaa"}
    config = _make_config()

    with patch(f"{TASK}.submit_signing_request") as mock_submit:
        submit_all_signing_requests(prepared, ["key-1", "key-2"], config)

    assert mock_submit.call_count == 2
    keys_submitted = {call.args[2] for call in mock_submit.call_args_list}
    assert keys_submitted == {"key-1", "key-2"}


def test_submit_all_signing_requests_raises_on_failure() -> None:
    """submit_all_signing_requests raises when any request fails."""
    prepared = {
        "a": "oci://ta@sha256:aaa",
        "b": "oci://ta@sha256:bbb",
    }
    config = _make_config()

    def side_effect(name: str, artifact: str, key: str, cfg: SubmitConfig) -> None:
        if name == "b":
            raise RuntimeError("signing failed")

    with patch(f"{TASK}.submit_signing_request", side_effect=side_effect):
        with pytest.raises(RuntimeError, match="1 signing request"):
            submit_all_signing_requests(prepared, ["key-1"], config)


# --- main ---


def test_main_returns_zero_on_success(tmp_path: Path) -> None:
    """Main returns 0 when signing completes successfully."""
    snap = tmp_path / "snap.json"
    snap.write_text(
        json.dumps(
            {
                "components": [
                    {"name": "c1", "containerImage": "img@sha256:abc"},
                ]
            }
        )
    )
    data = tmp_path / "data.json"
    data.write_text(
        json.dumps(
            {
                "sign": {"configMapName": "my-cm"},
                "intention": "release",
            }
        )
    )

    with (
        patch(
            "sys.argv",
            [
                "prog",
                "--snapshot",
                str(snap),
                "--data-file",
                str(data),
                "--requester",
                "user1",
                "--pipelinerun-uid",
                "uid-1",
                "--oci-storage",
                "quay.io/org/ta",
            ],
        ),
        patch(f"{TASK}.get_configmap", return_value=FULL_CONFIGMAP),
        patch(
            f"{TASK}.prepare_all_components",
            return_value={"c1": "oci://quay.io/org/ta@sha256:abc"},
        ),
        patch(f"{TASK}.submit_all_signing_requests"),
    ):
        assert main() == 0


def test_main_returns_zero_no_components(tmp_path: Path) -> None:
    """Main returns 0 when snapshot has no components."""
    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps({"components": []}))
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"sign": {"configMapName": "my-cm"}}))

    with (
        patch(
            "sys.argv",
            [
                "prog",
                "--snapshot",
                str(snap),
                "--data-file",
                str(data),
                "--requester",
                "user1",
                "--oci-storage",
                "quay.io/org/ta",
            ],
        ),
        patch(f"{TASK}.get_configmap", return_value=FULL_CONFIGMAP),
    ):
        assert main() == 0


def test_main_returns_one_on_error(tmp_path: Path) -> None:
    """Main returns 1 when an unexpected error occurs."""
    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps({"components": [{"name": "c1", "containerImage": "img"}]}))
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"sign": {"configMapName": "my-cm"}}))

    with (
        patch(
            "sys.argv",
            [
                "prog",
                "--snapshot",
                str(snap),
                "--data-file",
                str(data),
                "--requester",
                "user1",
                "--oci-storage",
                "quay.io/org/ta",
            ],
        ),
        patch(
            f"{TASK}.get_configmap",
            side_effect=RuntimeError("cluster error"),
        ),
    ):
        assert main() == 1


def test_main_reads_configmap_name_from_data_file(tmp_path: Path) -> None:
    """Main reads the signing ConfigMap name from data_file."""
    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps({"components": []}))
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"sign": {"configMapName": "custom-cm"}}))

    with (
        patch(
            "sys.argv",
            [
                "prog",
                "--snapshot",
                str(snap),
                "--data-file",
                str(data),
                "--requester",
                "user1",
                "--oci-storage",
                "quay.io/org/ta",
            ],
        ),
        patch(f"{TASK}.get_configmap", return_value=FULL_CONFIGMAP) as mock_cm,
    ):
        main()

    mock_cm.assert_called_once_with("custom-cm")


def test_main_defaults_configmap_name(tmp_path: Path) -> None:
    """Main defaults ConfigMap name to 'signing-config-map'."""
    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps({"components": []}))
    data = tmp_path / "data.json"
    data.write_text(json.dumps({}))

    with (
        patch(
            "sys.argv",
            [
                "prog",
                "--snapshot",
                str(snap),
                "--data-file",
                str(data),
                "--requester",
                "user1",
                "--oci-storage",
                "quay.io/org/ta",
            ],
        ),
        patch(f"{TASK}.get_configmap", return_value=FULL_CONFIGMAP) as mock_cm,
    ):
        main()

    mock_cm.assert_called_once_with("signing-config-map")


def test_main_reads_oras_options_from_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Main reads ORAS_OPTIONS from environment for prepare phase."""
    monkeypatch.setenv("ORAS_OPTIONS", "--registry-config /mnt/auth.json")
    snap = tmp_path / "snap.json"
    snap.write_text(
        json.dumps(
            {
                "components": [
                    {"name": "c1", "containerImage": "img@sha256:abc"},
                ]
            }
        )
    )
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"sign": {"configMapName": "my-cm"}}))

    captured_args: list[list[str] | None] = []

    def fake_prepare(components, storage, extra_args, limit):
        captured_args.append(extra_args)
        return {"c1": "oci://ta@sha256:abc"}

    with (
        patch(
            "sys.argv",
            [
                "prog",
                "--snapshot",
                str(snap),
                "--data-file",
                str(data),
                "--requester",
                "user1",
                "--oci-storage",
                "quay.io/org/ta",
            ],
        ),
        patch(f"{TASK}.get_configmap", return_value=FULL_CONFIGMAP),
        patch(f"{TASK}.prepare_all_components", side_effect=fake_prepare),
        patch(f"{TASK}.submit_all_signing_requests"),
    ):
        assert main() == 0

    assert captured_args[0] == ["--registry-config", "/mnt/auth.json"]
