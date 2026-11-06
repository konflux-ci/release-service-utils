"""Test the gpg_sign_and_push_to_cdn internal task."""

from __future__ import annotations

import io
import json
import logging
import tarfile
from pathlib import Path
from unittest.mock import patch

import pytest

from release_service_utils.tasks.internal.gpg_sign_and_push_to_cdn import (
    build_content_dir,
    main,
    parse_args,
    publish_all_components,
    publish_to_cgw,
    pull_and_extract,
)

TASK = "release_service_utils.tasks.internal.gpg_sign_and_push_to_cdn.gpg_sign_and_push_to_cdn"

FLAT_MEDIA_TYPE = "application/vnd.oci.empty.v1+json"


@pytest.fixture(autouse=True)
def _propagate_release_logger() -> None:
    """Allow caplog to capture records from the 'release' logger."""
    release_logger = logging.getLogger("release")
    release_logger.propagate = True
    yield
    release_logger.propagate = False


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


# --- parse_args ---


def test_parse_args_required() -> None:
    """Verify all required arguments are accepted."""
    args = parse_args(
        [
            "--snapshot-json",
            '{"components": []}',
            "--signed-artifacts-json",
            "{}",
            "--exodus-gw-env",
            "live",
            "--cgw-hostname",
            "https://cgw.example.com",
        ]
    )
    assert args.exodus_gw_env == "live"
    assert args.cgw_hostname == "https://cgw.example.com"


def test_parse_args_rejects_invalid_env() -> None:
    """Invalid --exodus-gw-env is rejected."""
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--snapshot-json",
                "{}",
                "--signed-artifacts-json",
                "{}",
                "--exodus-gw-env",
                "invalid",
                "--cgw-hostname",
                "https://cgw.example.com",
            ]
        )


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


# --- build_content_dir ---


def test_build_content_dir_merges_originals_and_signatures(
    tmp_path: Path,
) -> None:
    """build_content_dir uses results.json to map signatures to <file>.sig."""
    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()
    (extract_dir / "binary.tar.gz").write_bytes(b"archive")

    def fake_oras_pull(pull_spec: str, dest: Path) -> None:
        sig_dir = dest / "signatures"
        sig_dir.mkdir()
        (sig_dir / "0_a.sig").write_bytes(b"signature")
        (dest / "results.json").write_text(
            json.dumps(
                {
                    "results": [
                        {
                            "file": "binary.tar.gz",
                            "signature_file": "signatures/0_a.sig",
                        }
                    ]
                }
            )
        )

    content_dir = tmp_path / "content"
    with patch(f"{TASK}.oras_pull", side_effect=fake_oras_pull):
        sig_names = build_content_dir(
            extract_dir,
            ["oci://out@sha256:signed"],
            content_dir,
        )

    assert (content_dir / "binary.tar.gz").read_bytes() == b"archive"
    assert (content_dir / "binary.tar.gz.sig").read_bytes() == b"signature"
    assert sig_names == ["binary.tar.gz.sig"]


def test_build_content_dir_no_results_json(tmp_path: Path) -> None:
    """build_content_dir raises when signed TA has no results.json."""
    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()
    (extract_dir / "file.bin").write_bytes(b"data")

    def fake_oras_pull(pull_spec: str, dest: Path) -> None:
        pass

    content_dir = tmp_path / "content"
    with patch(f"{TASK}.oras_pull", side_effect=fake_oras_pull):
        with pytest.raises(RuntimeError, match="No results.json"):
            build_content_dir(
                extract_dir,
                ["oci://out@sha256:signed"],
                content_dir,
            )


def test_build_content_dir_raises_on_missing_signature_file(
    tmp_path: Path,
) -> None:
    """build_content_dir raises when a referenced signature file is missing."""
    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()
    (extract_dir / "binary.tar.gz").write_bytes(b"archive")

    def fake_oras_pull(pull_spec: str, dest: Path) -> None:
        (dest / "results.json").write_text(
            json.dumps(
                {
                    "results": [
                        {
                            "file": "binary.tar.gz",
                            "signature_file": "signatures/missing.sig",
                        }
                    ]
                }
            )
        )

    content_dir = tmp_path / "content"
    with patch(f"{TASK}.oras_pull", side_effect=fake_oras_pull):
        with pytest.raises(RuntimeError, match="Signature file.*not found"):
            build_content_dir(
                extract_dir,
                ["oci://out@sha256:signed"],
                content_dir,
            )


def test_build_content_dir_raises_on_incomplete_entry(
    tmp_path: Path,
) -> None:
    """build_content_dir raises when a results entry has empty fields."""
    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()
    (extract_dir / "file.bin").write_bytes(b"data")

    def fake_oras_pull(pull_spec: str, dest: Path) -> None:
        (dest / "results.json").write_text(
            json.dumps({"results": [{"file": "", "signature_file": ""}]})
        )

    content_dir = tmp_path / "content"
    with patch(f"{TASK}.oras_pull", side_effect=fake_oras_pull):
        with pytest.raises(RuntimeError, match="Incomplete signing result"):
            build_content_dir(
                extract_dir,
                ["oci://out@sha256:signed"],
                content_dir,
            )


# --- publish_to_cgw ---


def test_publish_to_cgw_calls_process_component(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """publish_to_cgw calls process_component for cgw-enabled components."""
    cgw_mount = tmp_path / "cgw"
    cgw_mount.mkdir()
    (cgw_mount / "username").write_text("user")
    (cgw_mount / "token").write_text("tok")
    monkeypatch.setenv("CGW_SECRET_MOUNT", str(cgw_mount))

    content_dir = tmp_path / "content"
    content_dir.mkdir()

    components = [
        {"name": "a", "contentGateway": {"productName": "prod"}},
        {"name": "b"},
    ]

    with patch(
        f"{TASK}.publish_to_cgw_wrapper.process_component",
    ) as mock_pc:
        publish_to_cgw(
            components,
            {"a": content_dir},
            {"a": ["binary.tar.gz.sig"]},
            "https://cgw.example.com",
        )

    mock_pc.assert_called_once()
    call_kwargs = mock_pc.call_args.kwargs
    assert call_kwargs["host"] == "https://cgw.example.com"
    assert call_kwargs["component_index"] == 0
    assert call_kwargs["component"]["contentGateway"]["contentDir"] == str(content_dir)
    files = call_kwargs["component"]["files"]
    assert any(f["source"] == "binary.tar.gz.sig" for f in files)


def test_publish_to_cgw_skips_without_content_gateway(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """publish_to_cgw skips components without contentGateway data."""
    cgw_mount = tmp_path / "cgw"
    cgw_mount.mkdir()
    (cgw_mount / "username").write_text("user")
    (cgw_mount / "token").write_text("tok")
    monkeypatch.setenv("CGW_SECRET_MOUNT", str(cgw_mount))

    components = [{"name": "a"}]

    with patch(
        f"{TASK}.publish_to_cgw_wrapper.process_component",
    ) as mock_pc:
        publish_to_cgw(components, {}, {}, "https://cgw.example.com")

    mock_pc.assert_not_called()


# --- publish_all_components ---


def test_publish_all_components_builds_and_pushes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """publish_all_components extracts, builds content, pushes to CDN, and publishes CGW."""
    exodus_mount = tmp_path / "exodus"
    exodus_mount.mkdir()
    (exodus_mount / "cert").write_text("CERT")
    (exodus_mount / "key").write_text("KEY")
    (exodus_mount / "url").write_text("https://gw.example.com")
    monkeypatch.setenv("EXODUS_GW_SECRET_MOUNT", str(exodus_mount))

    cgw_mount = tmp_path / "cgw"
    cgw_mount.mkdir()
    (cgw_mount / "username").write_text("user")
    (cgw_mount / "token").write_text("tok")
    monkeypatch.setenv("CGW_SECRET_MOUNT", str(cgw_mount))

    components = [
        {
            "name": "comp-a",
            "containerImage": "quay.io/org/img@sha256:abc",
            "contentGateway": {"productName": "prod"},
        },
    ]

    def fake_pull_and_extract(image: str, work_dir: Path) -> Path:
        extract_dir = work_dir / "extracted"
        extract_dir.mkdir(parents=True)
        (extract_dir / "file.bin").write_bytes(b"data")
        return extract_dir

    with (
        patch(f"{TASK}.pull_and_extract", side_effect=fake_pull_and_extract),
        patch(f"{TASK}.build_content_dir", return_value=[]) as mock_bcd,
        patch(f"{TASK}.push_files_to_cdn") as mock_cdn,
        patch(f"{TASK}.publish_to_cgw") as mock_cgw,
        patch(f"{TASK}.create_exodus_conf"),
    ):
        publish_all_components(
            components,
            {"comp-a": ["oci://out@sha256:signed"]},
            "live",
            "https://cgw.example.com",
        )

    mock_bcd.assert_called_once()
    mock_cdn.assert_called_once()
    mock_cgw.assert_called_once()


def test_publish_all_components_skips_no_container_image(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """publish_all_components skips components without containerImage."""
    exodus_mount = tmp_path / "exodus"
    exodus_mount.mkdir()
    (exodus_mount / "cert").write_text("CERT")
    (exodus_mount / "key").write_text("KEY")
    (exodus_mount / "url").write_text("https://gw.example.com")
    monkeypatch.setenv("EXODUS_GW_SECRET_MOUNT", str(exodus_mount))

    components = [{"name": "comp-a"}]

    with (
        patch(f"{TASK}.pull_and_extract") as mock_pull,
        patch(f"{TASK}.push_files_to_cdn"),
        patch(f"{TASK}.publish_to_cgw"),
        patch(f"{TASK}.create_exodus_conf"),
    ):
        publish_all_components(components, {}, "live", "https://cgw.example.com")

    mock_pull.assert_not_called()


# --- main ---


def test_main_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify main writes Success on a valid run."""
    result_path = tmp_path / "result"
    monkeypatch.setenv("RESULT_RESULT", str(result_path))

    snapshot = {"components": [{"name": "c1", "containerImage": "img@sha256:abc"}]}
    signed = {"c1": ["oci://out@sha256:signed"]}

    with patch(f"{TASK}.publish_all_components"):
        main(
            [
                "prog",
                "--snapshot-json",
                json.dumps(snapshot),
                "--signed-artifacts-json",
                json.dumps(signed),
                "--exodus-gw-env",
                "live",
                "--cgw-hostname",
                "https://cgw.example.com",
            ]
        )

    assert result_path.read_text() == "Success"


def test_main_no_components(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify main writes Success when snapshot has no components."""
    result_path = tmp_path / "result"
    monkeypatch.setenv("RESULT_RESULT", str(result_path))

    with patch(f"{TASK}.publish_all_components") as mock_pub:
        main(
            [
                "prog",
                "--snapshot-json",
                json.dumps({"components": []}),
                "--signed-artifacts-json",
                "{}",
                "--exodus-gw-env",
                "live",
                "--cgw-hostname",
                "https://cgw.example.com",
            ]
        )

    mock_pub.assert_not_called()
    assert result_path.read_text() == "Success"


def test_main_writes_failure_on_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify main writes a failure result and exits 0 on exception."""
    result_path = tmp_path / "result"
    monkeypatch.setenv("RESULT_RESULT", str(result_path))

    snapshot = {"components": [{"name": "c1", "containerImage": "img@sha256:abc"}]}
    signed = {"c1": ["oci://out@sha256:signed"]}

    with patch(
        f"{TASK}.publish_all_components",
        side_effect=RuntimeError("CDN push failed"),
    ):
        rc = main(
            [
                "prog",
                "--snapshot-json",
                json.dumps(snapshot),
                "--signed-artifacts-json",
                json.dumps(signed),
                "--exodus-gw-env",
                "live",
                "--cgw-hostname",
                "https://cgw.example.com",
            ]
        )

    assert rc == 0
    assert "CDN push failed" in result_path.read_text()


def test_main_writes_failure_on_invalid_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify main handles invalid JSON input gracefully."""
    result_path = tmp_path / "result"
    monkeypatch.setenv("RESULT_RESULT", str(result_path))

    rc = main(
        [
            "prog",
            "--snapshot-json",
            "not-json",
            "--signed-artifacts-json",
            "{}",
            "--exodus-gw-env",
            "live",
            "--cgw-hostname",
            "https://cgw.example.com",
        ]
    )

    assert rc == 0
    assert result_path.read_text() != "Success"
