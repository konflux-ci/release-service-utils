"""Test the gpg_sign_and_push_to_cdn managed task."""

from __future__ import annotations

import io
import json
import logging
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.helpers.oras_utils import (
    copy_all_flat_artifact_files,
    copy_all_layered_image_files,
)
from release_service_utils.tasks.managed.gpg_sign_and_push_to_cdn import (
    SubmitConfig,
    main,
    prepare_all_components,
    prepare_component,
    pull_and_extract,
    setup_argparser,
    submit_all_signing_requests,
    submit_publish_request,
    submit_signing_request,
)

TASK = (
    "release_service_utils.tasks.managed" ".gpg_sign_and_push_to_cdn.gpg_sign_and_push_to_cdn"
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
        pipeline="generic-signing",
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
        intention="staging",
        oci_storage="quay.io/org/ta",
        oras_options="",
        sign_method="detachsign",
        ta_dockerconfig_secret=("quay-token-konflux-release-trusted-artifacts-secret"),
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


def _cli_args(snap: Path, data: Path, **extras: str) -> list[str]:
    """Build a sys.argv list with required args and optional overrides."""
    args = [
        "prog",
        "--snapshot",
        str(snap),
        "--data-file",
        str(data),
        "--requester",
        extras.pop("requester", "user1"),
        "--oci-storage",
        extras.pop("oci_storage", "quay.io/org/ta"),
        "--task-git-url",
        extras.pop("task_git_url", "https://github.com/test/repo"),
        "--task-git-revision",
        extras.pop("task_git_revision", "main"),
    ]
    for key, val in extras.items():
        args.extend([f"--{key.replace('_', '-')}", val])
    return args


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
            "--task-git-url",
            "https://github.com/test/repo",
            "--task-git-revision",
            "main",
        ]
    )

    assert args.pipeline == "generic-signing"
    assert args.service_account == "signing-pipeline-sa"
    assert args.request_timeout == "1800"
    assert args.concurrent_limit == 4
    assert args.oci_storage == "empty"
    assert args.oras_options == ""
    assert args.sign_method == "detachsign"
    assert args.verbose is False
    assert args.task_git_url == "https://github.com/test/repo"
    assert args.task_git_revision == "main"


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
                "--task-git-url",
                "https://github.com/test/repo",
                "--task-git-revision",
                "main",
            ]
        )


def test_setup_argparser_requires_task_git_url(tmp_path: Path) -> None:
    """--task-git-url is mandatory."""
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
                "--requester",
                "tester",
                "--task-git-revision",
                "main",
            ]
        )


def test_copy_all_flat_artifact_files(tmp_path: Path) -> None:
    """Flat ORAS blobs are extracted by title annotation."""
    image_dir = tmp_path / "image"
    image_dir.mkdir()
    _write_flat_oras_image(image_dir, {"binary.tar.gz": b"data"})
    manifest = json.loads((image_dir / "manifest.json").read_text())

    dest = tmp_path / "out"
    dest.mkdir()
    copy_all_flat_artifact_files(manifest, image_dir, dest)

    assert (dest / "binary.tar.gz").read_bytes() == b"data"


def test_copy_all_flat_artifact_files_rejects_path_traversal(
    tmp_path: Path,
) -> None:
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
        copy_all_flat_artifact_files(manifest, image_dir, dest)


def test_copy_all_flat_artifact_files_skips_missing_title(
    tmp_path: Path,
) -> None:
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
    copy_all_flat_artifact_files(manifest, image_dir, dest)
    assert list(dest.iterdir()) == []


def test_copy_all_layered_image_files(tmp_path: Path) -> None:
    """Tar layers are extracted via safe_extract_archive."""
    image_dir = tmp_path / "image"
    image_dir.mkdir()
    _write_layered_image(image_dir, {"hello.txt": b"world"})
    manifest = json.loads((image_dir / "manifest.json").read_text())

    dest = tmp_path / "out"
    dest.mkdir()
    copy_all_layered_image_files(manifest, image_dir, dest)

    assert (dest / "hello.txt").read_bytes() == b"world"


# --- pull_and_extract ---


def test_pull_and_extract_flat_oras(tmp_path: Path) -> None:
    """pull_and_extract handles flat ORAS artifacts."""

    def fake_copy(src: str, dest: Path, **_kwargs: object) -> None:
        _write_flat_oras_image(dest, {"tool.bin": b"binary"})

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    with patch(f"{TASK}.skopeo.copy", side_effect=fake_copy):
        result = pull_and_extract("quay.io/org/img@sha256:abc", work_dir)

    assert (result / "tool.bin").read_bytes() == b"binary"


def test_pull_and_extract_layered_image(tmp_path: Path) -> None:
    """pull_and_extract handles standard layered images."""

    def fake_copy(src: str, dest: Path, **_kwargs: object) -> None:
        _write_layered_image(dest, {"app.bin": b"content"})

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    with patch(f"{TASK}.skopeo.copy", side_effect=fake_copy):
        result = pull_and_extract("quay.io/org/img@sha256:abc", work_dir)

    assert (result / "app.bin").read_bytes() == b"content"


# --- prepare_component ---


def test_prepare_component_extracts_and_pushes(tmp_path: Path) -> None:
    """prepare_component pushes extracted files directory via oras."""

    def fake_copy(src: str, dest: Path, **_kwargs: object) -> None:
        _write_flat_oras_image(dest, {"file.bin": b"data"})

    pushed_args: dict[str, object] = {}

    def fake_oras_push(
        tag: str,
        directory: Path,
        subdir: str,
        name: str,
        *,
        extra_args: list[str] | None = None,
    ) -> str:
        pushed_args["tag"] = tag
        pushed_args["subdir"] = subdir
        pushed_args["extra_args"] = extra_args
        assert (directory / subdir).is_dir()
        return "sha256:deadbeef"

    with (
        patch(f"{TASK}.skopeo.copy", side_effect=fake_copy),
        patch(f"{TASK}.oras_push", side_effect=fake_oras_push),
    ):
        uri = prepare_component(
            "my-comp",
            "quay.io/img@sha256:abc",
            "quay.io/org/ta",
            None,
            tmp_path,
        )

    assert uri == "oci://quay.io/org/ta@sha256:deadbeef"
    assert pushed_args["tag"] == "quay.io/org/ta:my-comp"
    assert pushed_args["subdir"] == "."
    assert pushed_args["extra_args"] is None
    assert not (tmp_path / "my-comp").exists()


def test_prepare_component_passes_extra_args(tmp_path: Path) -> None:
    """prepare_component forwards oras_extra_args to oras_push."""

    def fake_copy(src: str, dest: Path, **_kwargs: object) -> None:
        _write_flat_oras_image(dest, {"f.bin": b"x"})

    captured_extra: list[list[str] | None] = []

    def fake_oras_push(
        tag: str,
        directory: Path,
        subdir: str,
        name: str,
        *,
        extra_args: list[str] | None = None,
    ) -> str:
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
            tmp_path,
        )

    assert captured_extra[0] == ["--registry-config", "/auth.json"]


def test_prepare_component_raises_on_empty_extract(tmp_path: Path) -> None:
    """prepare_component raises if no files are extracted."""

    def fake_copy(src: str, dest: Path, **_kwargs: object) -> None:
        manifest = {
            "config": {"mediaType": FLAT_MEDIA_TYPE},
            "layers": [],
        }
        (dest / "manifest.json").write_text(json.dumps(manifest))

    with patch(f"{TASK}.skopeo.copy", side_effect=fake_copy):
        with pytest.raises(RuntimeError, match="No files extracted"):
            prepare_component(
                "c",
                "img@sha256:a",
                "quay.io/ta",
                None,
                tmp_path,
            )


# --- prepare_all_components ---


def test_prepare_all_components_succeeds(tmp_path: Path) -> None:
    """prepare_all_components returns a mapping of name to TA URI."""
    components = [
        {"name": "a", "containerImage": "img-a@sha256:aaa"},
        {"name": "b", "containerImage": "img-b@sha256:bbb"},
    ]

    def fake_prepare(
        name: str,
        image: str,
        storage: str,
        args: list[str] | None,
        work_root: Path,
    ) -> str:
        return f"oci://{storage}@sha256:{name}"

    with patch(f"{TASK}.prepare_component", side_effect=fake_prepare):
        result = prepare_all_components(
            components,
            "quay.io/ta",
            None,
            4,
            tmp_path,
        )

    assert result["a"] == "oci://quay.io/ta@sha256:a"
    assert result["b"] == "oci://quay.io/ta@sha256:b"


def test_prepare_all_components_raises_on_failure(tmp_path: Path) -> None:
    """prepare_all_components raises when any component fails."""
    components = [
        {"name": "a", "containerImage": "img-a@sha256:aaa"},
        {"name": "b", "containerImage": "img-b@sha256:bbb"},
    ]

    def fake_prepare(
        name: str,
        image: str,
        storage: str,
        args: list[str] | None,
        work_root: Path,
    ) -> str:
        if name == "b":
            raise RuntimeError("pull failed")
        return f"oci://{storage}@sha256:{name}"

    with patch(f"{TASK}.prepare_component", side_effect=fake_prepare):
        with pytest.raises(RuntimeError, match="1 component"):
            prepare_all_components(
                components,
                "quay.io/ta",
                None,
                4,
                tmp_path,
            )


# --- submit_signing_request ---


def test_submit_signing_request_builds_signing_request() -> None:
    """submit_signing_request creates a SigningRequest with cleanup=False."""
    config = _make_config()

    with patch(f"{TASK}.submit_signing") as mock_submit:
        mock_submit.return_value = {"sourceDataArtifact": "oci://out@sha256:signed"}
        result = submit_signing_request(
            "comp-a",
            "oci://quay.io/ta@sha256:aaa",
            "key-a",
            config,
        )

    assert result == "oci://out@sha256:signed"
    mock_submit.assert_called_once()
    request = mock_submit.call_args.args[0]
    assert request.source_data_artifact == "oci://quay.io/ta@sha256:aaa"
    assert request.keyname == "key-a"
    assert request.sign_method == "detachsign"
    assert request.onbehalfof == "tester"
    assert request.cleanup is False
    assert request.kerberos_keytab_secret == "keytab-secret"
    assert request.kerberos_keytab == "/etc/keytab"
    assert request.kerberos_principal == "svc@REALM"


def test_submit_signing_request_raises_on_wait_error() -> None:
    """submit_signing_request wraps InternalRequestWaitError."""
    from release_service_utils.helpers.internal_request import (
        InternalRequestWaitError,
    )

    config = _make_config()

    with patch(
        f"{TASK}.submit_signing",
        side_effect=InternalRequestWaitError("timed out", 124),
    ):
        with pytest.raises(RuntimeError, match="Signing failed for 'comp-a'"):
            submit_signing_request(
                "comp-a",
                "oci://ta@sha256:aaa",
                "key-a",
                config,
            )


def test_submit_signing_request_raises_on_empty_result() -> None:
    """submit_signing_request raises when sourceDataArtifact is missing."""
    config = _make_config()

    with patch(f"{TASK}.submit_signing") as mock_submit:
        mock_submit.return_value = {}
        with pytest.raises(RuntimeError, match="No sourceDataArtifact"):
            submit_signing_request(
                "comp-a",
                "oci://ta@sha256:aaa",
                "key-a",
                config,
            )


# --- submit_all_signing_requests ---


def test_submit_all_signing_requests_succeeds() -> None:
    """submit_all_signing_requests returns signed TA URI mapping."""
    prepared = {
        "a": "oci://ta@sha256:aaa",
        "b": "oci://ta@sha256:bbb",
    }
    config = _make_config()

    def fake_submit(name: str, artifact: str, key: str, cfg: SubmitConfig) -> str:
        return f"oci://out@sha256:{name}-signed"

    with patch(f"{TASK}.submit_signing_request", side_effect=fake_submit):
        result = submit_all_signing_requests(prepared, ["key-1"], config)

    assert "a" in result
    assert "b" in result
    assert result["a"] == ["oci://out@sha256:a-signed"]
    assert result["b"] == ["oci://out@sha256:b-signed"]


def test_submit_all_signing_requests_multiple_keys() -> None:
    """submit_all_signing_requests creates one IR per component x key."""
    prepared = {"a": "oci://ta@sha256:aaa"}
    config = _make_config()

    def fake_submit(name: str, artifact: str, key: str, cfg: SubmitConfig) -> str:
        return f"oci://out@sha256:{name}-{key}"

    with patch(
        f"{TASK}.submit_signing_request",
        side_effect=fake_submit,
    ):
        result = submit_all_signing_requests(
            prepared,
            ["key-1", "key-2"],
            config,
        )

    assert len(result["a"]) == 2


def test_submit_all_signing_requests_raises_on_failure() -> None:
    """submit_all_signing_requests raises when any request fails."""
    prepared = {
        "a": "oci://ta@sha256:aaa",
        "b": "oci://ta@sha256:bbb",
    }
    config = _make_config()

    def side_effect(name: str, artifact: str, key: str, cfg: SubmitConfig) -> str:
        if name == "b":
            raise RuntimeError("signing failed")
        return f"oci://out@sha256:{name}"

    with patch(f"{TASK}.submit_signing_request", side_effect=side_effect):
        with pytest.raises(RuntimeError, match="1 signing request"):
            submit_all_signing_requests(prepared, ["key-1"], config)


# --- submit_publish_request ---


def test_submit_publish_request_succeeds() -> None:
    """submit_publish_request creates an IR and checks results."""
    snapshot = {"components": [{"name": "c1"}]}
    signed = {"c1": ["oci://out@sha256:signed"]}
    cdn_config = {
        "exodusGwSecret": "exodus-prod-secret",
        "exodusGwEnv": "pre",
        "cgwHostname": "https://cgw.example.com",
        "cgwSecret": "cgw-stage-secret",
    }

    with patch(f"{TASK}.internal_request") as mock_ir:
        mock_ir.create.return_value = "ir-name"
        mock_ir.fetch_results.return_value = {"result": "Success"}
        mock_ir.PIPELINERUN_UID_LABEL = (
            "internal-services.appstudio.openshift.io/pipelinerun-uid"
        )
        mock_ir.duration_to_seconds.return_value = 7200
        mock_ir.SPAWN_OVERHEAD_SECONDS = 300
        mock_ir.InternalRequestWaitError = MagicMock

        submit_publish_request(
            snapshot,
            signed,
            cdn_config,
            "https://github.com/test/repo",
            "main",
            "pr-uid-456",
        )

    mock_ir.create.assert_called_once()
    params = mock_ir.create.call_args.kwargs["params"]
    assert params["exodusGwSecret"] == "exodus-prod-secret"
    assert params["exodusGwEnv"] == "pre"
    assert params["cgwHostname"] == "https://cgw.example.com"
    assert params["cgwSecret"] == "cgw-stage-secret"
    assert params["taskGitUrl"] == "https://github.com/test/repo"
    assert params["taskGitRevision"] == "main"
    assert json.loads(params["snapshot_json"]) == snapshot
    assert json.loads(params["signed_artifacts_json"]) == signed


def test_submit_publish_request_raises_on_failure() -> None:
    """submit_publish_request raises when the IR result is not Success."""
    with patch(f"{TASK}.internal_request") as mock_ir:
        mock_ir.create.return_value = "ir-name"
        mock_ir.fetch_results.return_value = {"result": "CDN push failed"}
        mock_ir.PIPELINERUN_UID_LABEL = (
            "internal-services.appstudio.openshift.io/pipelinerun-uid"
        )
        mock_ir.duration_to_seconds.return_value = 7200
        mock_ir.SPAWN_OVERHEAD_SECONDS = 300
        mock_ir.InternalRequestWaitError = MagicMock

        with pytest.raises(RuntimeError, match="Publishing to CDN/CGW failed"):
            submit_publish_request(
                {"components": []},
                {},
                {
                    "exodusGwSecret": "s",
                    "exodusGwEnv": "pre",
                    "cgwHostname": "h",
                    "cgwSecret": "c",
                },
                "https://github.com/test/repo",
                "main",
                "pr-uid",
            )


def test_submit_publish_request_raises_on_wait_error() -> None:
    """submit_publish_request wraps InternalRequestWaitError."""
    from release_service_utils.helpers.internal_request import (
        InternalRequestWaitError,
    )

    with patch(f"{TASK}.internal_request") as mock_ir:
        mock_ir.create.side_effect = InternalRequestWaitError("timed out", 124)
        mock_ir.PIPELINERUN_UID_LABEL = (
            "internal-services.appstudio.openshift.io/pipelinerun-uid"
        )
        mock_ir.duration_to_seconds.return_value = 7200
        mock_ir.SPAWN_OVERHEAD_SECONDS = 300
        mock_ir.InternalRequestWaitError = InternalRequestWaitError

        with pytest.raises(RuntimeError, match="timed out"):
            submit_publish_request(
                {"components": []},
                {},
                {
                    "exodusGwSecret": "s",
                    "exodusGwEnv": "pre",
                    "cgwHostname": "h",
                    "cgwSecret": "c",
                },
                "https://github.com/test/repo",
                "main",
                "pr-uid",
            )


# --- main ---


def test_main_succeeds(tmp_path: Path) -> None:
    """Verify main completes without error on a valid snapshot."""
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
    data.write_text(json.dumps({"sign": {"configMapName": "my-cm"}, "cdn": {"env": "stage"}}))

    with (
        patch("sys.argv", _cli_args(snap, data)),
        patch(f"{TASK}.get_configmap", return_value=FULL_CONFIGMAP),
        patch(
            f"{TASK}.prepare_all_components",
            return_value={"c1": "oci://quay.io/org/ta@sha256:abc"},
        ),
        patch(
            f"{TASK}.submit_all_signing_requests",
            return_value={"c1": ["oci://out@sha256:signed"]},
        ),
        patch(f"{TASK}.submit_publish_request") as mock_publish,
    ):
        main()

    mock_publish.assert_called_once()
    call_args = mock_publish.call_args.args
    cdn_config = call_args[2]
    task_git_url = call_args[3]
    task_git_revision = call_args[4]
    assert task_git_url == "https://github.com/test/repo"
    assert task_git_revision == "main"
    assert cdn_config["exodusGwEnv"] == "pre"


def test_main_no_components(tmp_path: Path) -> None:
    """Verify main completes without error when snapshot has no components."""
    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps({"components": []}))
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"sign": {"configMapName": "my-cm"}}))

    with (
        patch("sys.argv", _cli_args(snap, data)),
        patch(f"{TASK}.get_configmap", return_value=FULL_CONFIGMAP),
    ):
        main()


def test_main_raises_on_error(tmp_path: Path) -> None:
    """Verify main propagates exceptions."""
    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps({"components": [{"name": "c1", "containerImage": "img"}]}))
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"sign": {"configMapName": "my-cm"}}))

    with (
        patch("sys.argv", _cli_args(snap, data)),
        patch(
            f"{TASK}.get_configmap",
            side_effect=RuntimeError("cluster error"),
        ),
    ):
        with pytest.raises(RuntimeError, match="cluster error"):
            main()


def test_main_reads_configmap_name_from_data_file(tmp_path: Path) -> None:
    """Main reads the signing ConfigMap name from data_file."""
    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps({"components": []}))
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"sign": {"configMapName": "custom-cm"}}))

    with (
        patch("sys.argv", _cli_args(snap, data)),
        patch(
            f"{TASK}.get_configmap",
            return_value=FULL_CONFIGMAP,
        ) as mock_cm,
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
        patch("sys.argv", _cli_args(snap, data)),
        patch(
            f"{TASK}.get_configmap",
            return_value=FULL_CONFIGMAP,
        ) as mock_cm,
    ):
        main()

    mock_cm.assert_called_once_with("signing-config-map")


def test_main_reads_oras_options_from_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    data.write_text(json.dumps({"sign": {"configMapName": "my-cm"}, "cdn": {"env": "stage"}}))

    captured_args: list[list[str] | None] = []

    def fake_prepare(
        components: list[dict[str, object]],
        storage: str,
        extra_args: list[str] | None,
        limit: int,
        work_root: Path,
    ) -> dict[str, str]:
        captured_args.append(extra_args)
        return {"c1": "oci://ta@sha256:abc"}

    with (
        patch("sys.argv", _cli_args(snap, data)),
        patch(f"{TASK}.get_configmap", return_value=FULL_CONFIGMAP),
        patch(
            f"{TASK}.prepare_all_components",
            side_effect=fake_prepare,
        ),
        patch(
            f"{TASK}.submit_all_signing_requests",
            return_value={"c1": ["oci://out@sha256:signed"]},
        ),
        patch(f"{TASK}.submit_publish_request"),
    ):
        main()

    assert captured_args[0] == ["--registry-config", "/mnt/auth.json"]


def test_main_resolves_cdn_config_from_data_file(tmp_path: Path) -> None:
    """Main resolves CDN environment secrets from the data file."""
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
        json.dumps({"sign": {"configMapName": "my-cm"}, "cdn": {"env": "production"}})
    )

    with (
        patch("sys.argv", _cli_args(snap, data)),
        patch(f"{TASK}.get_configmap", return_value=FULL_CONFIGMAP),
        patch(
            f"{TASK}.prepare_all_components",
            return_value={"c1": "oci://ta@sha256:abc"},
        ),
        patch(
            f"{TASK}.submit_all_signing_requests",
            return_value={"c1": ["oci://out@sha256:signed"]},
        ),
        patch(f"{TASK}.submit_publish_request") as mock_publish,
    ):
        main()

    cdn_config = mock_publish.call_args.args[2]
    assert cdn_config["exodusGwEnv"] == "live"
    assert cdn_config["exodusGwSecret"] == "exodus-prod-secret"
    assert cdn_config["cgwSecret"] == "cgw-service-account-prod-secret"
