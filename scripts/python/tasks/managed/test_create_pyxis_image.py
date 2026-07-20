"""Tests for ``create_pyxis_image`` task script — 100 % coverage target."""

from __future__ import annotations

import gzip
import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import create_pyxis_image
import pytest

# ── fixtures & helpers ──────────────────────────────────────────────


def _snapshot(
    components: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {"components": components or []}


def _component(
    image: str = "quay.io/org/img@sha256:abc123",
    repositories: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    comp: dict[str, Any] = {"containerImage": image}
    if repositories is not None:
        comp["repositories"] = repositories
    if metadata is not None:
        comp["metadata"] = metadata
    return comp


def _repo(
    url: str = "quay.io/redhat-prod/product----image",
    tags: list[str] | None = None,
) -> dict[str, Any]:
    return {"url": url, "tags": tags or ["v1.0"]}


def _data(
    include_layers: bool = False,
    append_tags: bool = False,
) -> dict[str, Any]:
    return {
        "pyxis": {
            "includeLayers": include_layers,
            "appendTags": append_tags,
        },
    }


def _write(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _setup(
    tmp_path: Path,
    snap: dict | None = None,
    data: dict | None = None,
) -> tuple[Path, Path, Path, Path, Path]:
    """Create standard test files and directories. Returns paths."""
    data_dir = tmp_path / "release"
    data_dir.mkdir(parents=True)
    snap_dir = data_dir / "results"
    snap_dir.mkdir()

    snapshot_file = snap_dir / "snapshot.json"
    data_file = snap_dir / "data.json"
    _write(snapshot_file, snap or _snapshot())
    _write(data_file, data or _data())

    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    (secret_dir / "cert").write_text("CERT_DATA", encoding="utf-8")
    (secret_dir / "key").write_text("KEY_DATA", encoding="utf-8")

    result_file = tmp_path / "pyxisDataPath"
    return data_dir, snapshot_file, data_file, secret_dir, result_file


def _skopeo_result(
    stdout: str = "{}",
    rc: int = 0,
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        ["skopeo"],
        rc,
        stdout=stdout,
        stderr=stderr,
    )


def _raw_manifest(
    media_type: str = "application/vnd.oci.image.manifest.v1+json",
    layers: list[dict] | None = None,
) -> str:
    return json.dumps({"mediaType": media_type, "layers": layers or []})


# ── _write_auth_file ────────────────────────────────────────────────


class TestWriteAuthFile:
    """Tests for ``_write_auth_file``."""

    def test_writes_select_oci_auth_output(self, tmp_path: Path) -> None:
        """Verify auth file is written from select-oci-auth stdout."""
        auth = tmp_path / "auth.json"
        with patch(
            "create_pyxis_image.subprocess.run",
            return_value=subprocess.CompletedProcess(
                [],
                0,
                stdout='{"auths":{}}',
                stderr="",
            ),
        ):
            create_pyxis_image._write_auth_file("quay.io/org/img", auth)
        assert json.loads(auth.read_text()) == {"auths": {}}


# ── _try_pull_dockerfile ────────────────────────────────────────────


class TestTryPullDockerfile:
    """Tests for ``_try_pull_dockerfile``."""

    def test_success(self, tmp_path: Path) -> None:
        """Return Dockerfile path on successful pull."""

        def fake_pull(pull_spec, download_dir):
            (download_dir / "Dockerfile").write_text("FROM ubi9")

        with patch("create_pyxis_image.oras_utils.oras_pull", side_effect=fake_pull):
            result = create_pyxis_image._try_pull_dockerfile(
                "quay.io/org/img",
                "sha256:abc",
            )
        assert result is not None
        assert result.name == "Dockerfile"

    def test_pull_fails_returns_empty(self) -> None:
        """Return None when oras pull fails."""
        with patch(
            "create_pyxis_image.oras_utils.oras_pull",
            side_effect=subprocess.CalledProcessError(1, "oras"),
        ):
            result = create_pyxis_image._try_pull_dockerfile(
                "quay.io/org/img",
                "sha256:abc",
            )
        assert result is None

    def test_pull_ok_but_no_dockerfile_raises(self) -> None:
        """Raise when pull succeeds but Dockerfile is missing."""
        with patch("create_pyxis_image.oras_utils.oras_pull"):
            with pytest.raises(RuntimeError, match="Dockerfile was not saved"):
                create_pyxis_image._try_pull_dockerfile(
                    "quay.io/org/img",
                    "sha256:abc",
                )


# ── _decompress_gzip_layer ─────────────────────────────────────────


class TestDecompressGzipLayer:
    """Tests for ``_decompress_gzip_layer``."""

    def test_decompresses_and_measures(self, tmp_path: Path) -> None:
        """Decompress a gzip blob and return digest and size."""
        auth = tmp_path / "auth.json"
        auth.write_text("{}", encoding="utf-8")
        payload = b"hello world"
        compressed = gzip.compress(payload)

        def fake_blob_fetch(pullspec, output_path, auth_file):
            output_path.write_bytes(compressed)

        with patch(
            "create_pyxis_image.oras_utils.oras_blob_fetch",
            side_effect=fake_blob_fetch,
        ):
            result = create_pyxis_image._decompress_gzip_layer(
                "sha256:layerdigest",
                "quay.io/org/img",
                auth,
                0,
            )
        assert result["digest"].startswith("sha256:")
        assert result["size"] == len(payload)


# ── _build_cci_namespace ────────────────────────────────────────────


class TestBuildCciArgs:
    """Tests for ``_build_cci_args``."""

    def test_builds_args(self) -> None:
        """Build a ContainerImageArgs from RunConfig and ComponentContext."""
        config = create_pyxis_image.RunConfig(
            pyxis_url="https://pyxis.example.com/",
            pyxis_graphql_url="https://graphql.example.com/graphql/",
            certified="false",
            is_latest="false",
            rh_push="false",
            append_tags="false",
            include_layers=False,
            process_helm_charts=False,
            data_dir=Path("/tmp"),
            snapshot_dir=Path("results"),
        )
        component = create_pyxis_image.ComponentContext(
            index=0,
            digest="sha256:abc",
            auth_path=Path("/tmp/auth"),
            dockerfile_path=None,
            metadata_path=None,
        )
        cci_args = create_pyxis_image._build_cci_args(
            config=config,
            component=component,
            tags="v1.0",
            oras_manifest_fetch="/tmp/manifest.json",
            name="quay.io/org/img",
            media_type="application/vnd.oci.image.manifest.v1+json",
            architecture_digest="sha256:abc",
            architecture="amd64",
        )
        assert cci_args.pyxis_url == "https://pyxis.example.com/"
        assert cci_args.digest == "sha256:abc"


# ── process_component ───────────────────────────────────────────────


class TestProcessComponent:
    """Tests for ``process_component``."""

    def _call(
        self,
        tmp_path: Path,
        snap: dict | None = None,
        component_index: int = 0,
        rh_push: str = "false",
        process_helm_charts: bool = False,
        include_layers: bool = False,
        append_tags: str = "false",
    ) -> dict[str, Any] | None:
        data_dir = tmp_path / "release"
        data_dir.mkdir(exist_ok=True)
        snap_dir = data_dir / "results"
        snap_dir.mkdir(exist_ok=True)
        config = create_pyxis_image.RunConfig(
            pyxis_url="https://pyxis.example.com/",
            pyxis_graphql_url="https://graphql.example.com/graphql/",
            certified="false",
            is_latest="false",
            rh_push=rh_push,
            append_tags=append_tags,
            include_layers=include_layers,
            process_helm_charts=process_helm_charts,
            data_dir=data_dir,
            snapshot_dir=Path("results"),
        )
        return create_pyxis_image.process_component(
            component_index,
            snap
            or _snapshot(
                [
                    _component(repositories=[_repo()]),
                ]
            ),
            config=config,
        )

    @patch("create_pyxis_image.cleanup_tags_with_retry")
    @patch("create_pyxis_image.create_or_update", return_value="img123")
    @patch(
        "create_pyxis_image.oras_utils.oras_manifest_fetch",
        return_value='{"layers": []}',
    )
    @patch(
        "create_pyxis_image.image_architectures.get_image_architectures",
        return_value=[
            {
                "platform": {"architecture": "amd64", "os": "linux"},
                "digest": "sha256:archdig",
                "multiarch": False,
            }
        ],
    )
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result(_raw_manifest()),
    )
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch("create_pyxis_image._write_auth_file")
    def test_single_arch(
        self,
        mock_auth,
        mock_docker,
        mock_skopeo,
        mock_arch,
        mock_oras,
        mock_create,
        mock_cleanup,
        tmp_path: Path,
    ) -> None:
        """Process a single-arch image."""
        result = self._call(tmp_path)
        assert result is not None
        assert result["componentIndex"] == 0
        assert len(result["pyxisImages"]) == 1
        assert result["pyxisImages"][0]["imageId"] == "img123"
        mock_cleanup.assert_not_called()

    @patch("create_pyxis_image.cleanup_tags_with_retry")
    @patch("create_pyxis_image.create_or_update", return_value="img456")
    @patch(
        "create_pyxis_image.oras_utils.oras_manifest_fetch",
        return_value='{"layers": []}',
    )
    @patch(
        "create_pyxis_image.image_architectures.get_image_architectures",
        return_value=[
            {
                "platform": {"architecture": "amd64", "os": "linux"},
                "digest": "sha256:amd",
                "multiarch": True,
            },
            {
                "platform": {"architecture": "arm64", "os": "linux"},
                "digest": "sha256:arm",
                "multiarch": True,
            },
        ],
    )
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result(
            _raw_manifest("application/vnd.docker.distribution.manifest.list.v2+json"),
        ),
    )
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch("create_pyxis_image._write_auth_file")
    def test_multi_arch(
        self,
        mock_auth,
        mock_docker,
        mock_skopeo,
        mock_arch,
        mock_oras,
        mock_create,
        mock_cleanup,
        tmp_path: Path,
    ) -> None:
        """Process a multi-arch manifest list image."""
        result = self._call(tmp_path)
        assert result is not None
        assert len(result["pyxisImages"]) == 2

    @patch("create_pyxis_image.cleanup_tags_with_retry")
    @patch("create_pyxis_image.create_or_update", return_value="img789")
    @patch(
        "create_pyxis_image.oras_utils.oras_manifest_fetch",
        return_value='{"layers": []}',
    )
    @patch(
        "create_pyxis_image.image_architectures.get_image_architectures",
        return_value=[
            {
                "platform": {"architecture": "amd64", "os": "linux"},
                "digest": "sha256:archdig",
                "multiarch": False,
            }
        ],
    )
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result(_raw_manifest()),
    )
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch("create_pyxis_image._write_auth_file")
    def test_rh_push_calls_cleanup(
        self,
        mock_auth,
        mock_docker,
        mock_skopeo,
        mock_arch,
        mock_oras,
        mock_create,
        mock_cleanup,
        tmp_path: Path,
    ) -> None:
        """Trigger tag cleanup when rh_push is enabled."""
        result = self._call(tmp_path, rh_push="true")
        assert result is not None
        mock_cleanup.assert_called_once_with(
            "https://graphql.example.com/graphql/",
            "img789",
            "product/image",
        )

    @patch("create_pyxis_image.create_or_update")
    @patch("create_pyxis_image.oras_utils.oras_manifest_fetch")
    @patch(
        "create_pyxis_image.image_architectures.get_image_architectures",
        return_value=[
            {
                "platform": {"architecture": "amd64", "os": "linux"},
                "digest": "sha256:archdig",
                "multiarch": False,
                "configMediaType": "application/vnd.cncf.helm.config.v1+json",
            }
        ],
    )
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result(_raw_manifest()),
    )
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch("create_pyxis_image._write_auth_file")
    def test_helm_chart_skipped(
        self,
        mock_auth,
        mock_docker,
        mock_skopeo,
        mock_arch,
        mock_oras,
        mock_create,
        tmp_path: Path,
    ) -> None:
        """Skip helm chart artifacts when processing is disabled."""
        result = self._call(tmp_path, process_helm_charts=False)
        assert result is None
        mock_create.assert_not_called()

    @patch("create_pyxis_image.cleanup_tags_with_retry")
    @patch("create_pyxis_image.create_or_update", return_value="imghelm")
    @patch(
        "create_pyxis_image.oras_utils.oras_manifest_fetch",
        return_value='{"layers": []}',
    )
    @patch(
        "create_pyxis_image.image_architectures.get_image_architectures",
        return_value=[
            {
                "platform": {"architecture": "amd64", "os": "linux"},
                "digest": "sha256:archdig",
                "multiarch": False,
                "configMediaType": "application/vnd.cncf.helm.config.v1+json",
            }
        ],
    )
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result(_raw_manifest()),
    )
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch("create_pyxis_image._write_auth_file")
    def test_helm_chart_processed_when_enabled(
        self,
        mock_auth,
        mock_docker,
        mock_skopeo,
        mock_arch,
        mock_oras,
        mock_create,
        mock_cleanup,
        tmp_path: Path,
    ) -> None:
        """Process helm chart artifacts when enabled."""
        result = self._call(tmp_path, process_helm_charts=True)
        assert result is not None

    @patch("create_pyxis_image.cleanup_tags_with_retry")
    @patch("create_pyxis_image.create_or_update", return_value="imglayer")
    @patch("create_pyxis_image._decompress_gzip_layer")
    @patch("create_pyxis_image.oras_utils.oras_manifest_fetch")
    @patch(
        "create_pyxis_image.image_architectures.get_image_architectures",
        return_value=[
            {
                "platform": {"architecture": "amd64", "os": "linux"},
                "digest": "sha256:archdig",
                "multiarch": False,
            }
        ],
    )
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result(_raw_manifest()),
    )
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch("create_pyxis_image._write_auth_file")
    def test_include_layers_true_with_gzip(
        self,
        mock_auth,
        mock_docker,
        mock_skopeo,
        mock_arch,
        mock_oras,
        mock_decompress,
        mock_create,
        mock_cleanup,
        tmp_path: Path,
    ) -> None:
        """Decompress gzip layers when includeLayers is true."""
        mock_oras.return_value = json.dumps(
            {
                "layers": [
                    {
                        "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                        "digest": "sha256:layer1",
                        "size": 1000,
                    },
                ],
            }
        )
        mock_decompress.return_value = {"digest": "sha256:expanded", "size": 2000}
        result = self._call(tmp_path, include_layers=True)
        assert result is not None
        mock_decompress.assert_called_once()

    @patch("create_pyxis_image.cleanup_tags_with_retry")
    @patch("create_pyxis_image.create_or_update", return_value="imgnolayer")
    @patch("create_pyxis_image.oras_utils.oras_manifest_fetch")
    @patch(
        "create_pyxis_image.image_architectures.get_image_architectures",
        return_value=[
            {
                "platform": {"architecture": "amd64", "os": "linux"},
                "digest": "sha256:archdig",
                "multiarch": False,
            }
        ],
    )
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result(_raw_manifest()),
    )
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch("create_pyxis_image._write_auth_file")
    def test_include_layers_false_clears_layers(
        self,
        mock_auth,
        mock_docker,
        mock_skopeo,
        mock_arch,
        mock_oras,
        mock_create,
        mock_cleanup,
        tmp_path: Path,
    ) -> None:
        """Clear layers when includeLayers is false."""
        mock_oras.return_value = json.dumps(
            {
                "layers": [
                    {
                        "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                        "digest": "sha256:layer1",
                        "size": 1000,
                    },
                ],
            }
        )
        result = self._call(tmp_path, include_layers=False)
        assert result is not None

    @patch("create_pyxis_image.cleanup_tags_with_retry")
    @patch("create_pyxis_image.create_or_update", return_value="imgmeta")
    @patch(
        "create_pyxis_image.oras_utils.oras_manifest_fetch",
        return_value='{"layers": []}',
    )
    @patch(
        "create_pyxis_image.image_architectures.get_image_architectures",
        return_value=[
            {
                "platform": {"architecture": "amd64", "os": "linux"},
                "digest": "sha256:archdig",
                "multiarch": False,
            }
        ],
    )
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result(_raw_manifest()),
    )
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch("create_pyxis_image._write_auth_file")
    def test_with_metadata(
        self,
        mock_auth,
        mock_docker,
        mock_skopeo,
        mock_arch,
        mock_oras,
        mock_create,
        mock_cleanup,
        tmp_path: Path,
    ) -> None:
        """Include component metadata labels in the result."""
        snap = _snapshot(
            [
                _component(
                    repositories=[_repo()],
                    metadata={"labels": [{"name": "foo", "value": "bar"}]},
                ),
            ]
        )
        result = self._call(tmp_path, snap=snap)
        assert result is not None

    @patch("create_pyxis_image._write_auth_file")
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result("", rc=1, stderr="not found"),
    )
    def test_skopeo_failure_raises(
        self,
        mock_skopeo,
        mock_docker,
        mock_auth,
        tmp_path: Path,
    ) -> None:
        """Raise RuntimeError when skopeo inspect fails."""
        with pytest.raises(RuntimeError, match="skopeo inspect --raw failed"):
            self._call(tmp_path)

    @patch("create_pyxis_image.cleanup_tags_with_retry")
    @patch("create_pyxis_image.create_or_update", return_value="imgnogzip")
    @patch("create_pyxis_image.oras_utils.oras_manifest_fetch")
    @patch(
        "create_pyxis_image.image_architectures.get_image_architectures",
        return_value=[
            {
                "platform": {"architecture": "amd64", "os": "linux"},
                "digest": "sha256:archdig",
                "multiarch": False,
            }
        ],
    )
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result(_raw_manifest()),
    )
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch("create_pyxis_image._write_auth_file")
    def test_non_gzip_layers_not_decompressed(
        self,
        mock_auth,
        mock_docker,
        mock_skopeo,
        mock_arch,
        mock_oras,
        mock_create,
        mock_cleanup,
        tmp_path: Path,
    ) -> None:
        """Skip decompression for non-gzip layer media types."""
        mock_oras.return_value = json.dumps(
            {
                "layers": [
                    {
                        "mediaType": "application/vnd.oci.image.layer.v1.tar",
                        "digest": "sha256:plainlayer",
                        "size": 500,
                    },
                ],
            }
        )
        result = self._call(tmp_path, include_layers=True)
        assert result is not None

    @patch("create_pyxis_image.cleanup_tags_with_retry")
    @patch("create_pyxis_image.create_or_update", return_value="imgtag")
    @patch(
        "create_pyxis_image.oras_utils.oras_manifest_fetch",
        return_value='{"layers": []}',
    )
    @patch(
        "create_pyxis_image.image_architectures.get_image_architectures",
        return_value=[
            {
                "platform": {"architecture": "amd64", "os": "linux"},
                "digest": "sha256:archdig",
                "multiarch": False,
            }
        ],
    )
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result(_raw_manifest()),
    )
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch("create_pyxis_image._write_auth_file")
    def test_repo_url_with_tag_stripped(
        self,
        mock_auth,
        mock_docker,
        mock_skopeo,
        mock_arch,
        mock_oras,
        mock_create,
        mock_cleanup,
        tmp_path: Path,
    ) -> None:
        """Strip tag from repository URL before processing."""
        snap = _snapshot(
            [
                _component(
                    repositories=[
                        _repo(url="quay.io/redhat-prod/product----image:latest"),
                    ]
                ),
            ]
        )
        result = self._call(tmp_path, snap=snap)
        assert result is not None

    @patch("create_pyxis_image.cleanup_tags_with_retry")
    @patch("create_pyxis_image.create_or_update", return_value="imgnorepo")
    @patch(
        "create_pyxis_image.oras_utils.oras_manifest_fetch",
        return_value='{"layers": []}',
    )
    @patch(
        "create_pyxis_image.image_architectures.get_image_architectures",
        return_value=[
            {
                "platform": {"architecture": "amd64", "os": "linux"},
                "digest": "sha256:archdig",
                "multiarch": False,
            }
        ],
    )
    @patch(
        "create_pyxis_image.skopeo.inspect",
        return_value=_skopeo_result(_raw_manifest()),
    )
    @patch("create_pyxis_image._try_pull_dockerfile", return_value=None)
    @patch("create_pyxis_image._write_auth_file")
    def test_component_with_no_repos(
        self,
        mock_auth,
        mock_docker,
        mock_skopeo,
        mock_arch,
        mock_oras,
        mock_create,
        mock_cleanup,
        tmp_path: Path,
    ) -> None:
        """Return empty pyxisImages when component has no repositories."""
        snap = _snapshot([_component(repositories=[])])
        result = self._call(tmp_path, snap=snap)
        assert result is not None
        assert result["pyxisImages"] == []


# ── run ─────────────────────────────────────────────────────────────


class TestRun:
    """Tests for the ``run`` orchestration function."""

    def test_missing_snapshot_raises(self, tmp_path: Path) -> None:
        """Raise FileNotFoundError when snapshot file is missing."""
        _, _, data_file, _, result_file = _setup(tmp_path)
        with pytest.raises(FileNotFoundError):
            create_pyxis_image.run(
                server="production",
                snapshot_file=tmp_path / "nonexistent.json",
                data_file=data_file,
                certified="false",
                is_latest="false",
                rh_push="false",
                process_helm_charts=False,
                concurrent_limit=4,
                pyxis_data_path_result=result_file,
                data_dir=tmp_path / "release",
                snapshot_path_relative="results/snapshot.json",
            )

    def test_missing_data_raises(self, tmp_path: Path) -> None:
        """Raise FileNotFoundError when data file is missing."""
        data_dir, snapshot_file, _, _, result_file = _setup(tmp_path)
        with pytest.raises(FileNotFoundError):
            create_pyxis_image.run(
                server="production",
                snapshot_file=snapshot_file,
                data_file=tmp_path / "nonexistent.json",
                certified="false",
                is_latest="false",
                rh_push="false",
                process_helm_charts=False,
                concurrent_limit=4,
                pyxis_data_path_result=result_file,
                data_dir=data_dir,
                snapshot_path_relative="results/snapshot.json",
            )

    def test_invalid_server_raises(self, tmp_path: Path) -> None:
        """Raise ValueError for an unknown server name."""
        data_dir, snapshot_file, data_file, _, result_file = _setup(tmp_path)
        with pytest.raises(ValueError, match="Invalid server parameter"):
            create_pyxis_image.run(
                server="invalid",
                snapshot_file=snapshot_file,
                data_file=data_file,
                certified="false",
                is_latest="false",
                rh_push="false",
                process_helm_charts=False,
                concurrent_limit=4,
                pyxis_data_path_result=result_file,
                data_dir=data_dir,
                snapshot_path_relative="results/snapshot.json",
            )

    @patch("create_pyxis_image.process_component")
    def test_single_component(
        self,
        mock_process,
        tmp_path: Path,
    ) -> None:
        """Process a single component and write output."""
        snap = _snapshot([_component()])
        data_dir, snapshot_file, data_file, _, result_file = _setup(
            tmp_path,
            snap=snap,
        )
        mock_process.return_value = {
            "containerImage": "quay.io/org/img@sha256:abc123",
            "componentIndex": 0,
            "pyxisImages": [{"imageId": "id1"}],
        }
        create_pyxis_image.run(
            server="production",
            snapshot_file=snapshot_file,
            data_file=data_file,
            certified="false",
            is_latest="false",
            rh_push="false",
            process_helm_charts=False,
            concurrent_limit=4,
            pyxis_data_path_result=result_file,
            data_dir=data_dir,
            snapshot_path_relative="results/snapshot.json",
        )
        output = json.loads((data_dir / "results" / "pyxis.json").read_text())
        assert len(output["components"]) == 1
        assert result_file.read_text() == "results/pyxis.json"

    @patch("create_pyxis_image.process_component")
    def test_skipped_component_excluded(
        self,
        mock_process,
        tmp_path: Path,
    ) -> None:
        """Exclude components that return None from the output."""
        snap = _snapshot([_component()])
        data_dir, snapshot_file, data_file, _, result_file = _setup(
            tmp_path,
            snap=snap,
        )
        mock_process.return_value = None
        create_pyxis_image.run(
            server="production",
            snapshot_file=snapshot_file,
            data_file=data_file,
            certified="false",
            is_latest="false",
            rh_push="false",
            process_helm_charts=False,
            concurrent_limit=4,
            pyxis_data_path_result=result_file,
            data_dir=data_dir,
            snapshot_path_relative="results/snapshot.json",
        )
        output = json.loads((data_dir / "results" / "pyxis.json").read_text())
        assert output["components"] == []

    @patch("create_pyxis_image.process_component")
    def test_duplicate_digests_grouped(
        self,
        mock_process,
        tmp_path: Path,
    ) -> None:
        """Group components with the same digest for sequential processing."""
        snap = _snapshot(
            [
                _component(image="quay.io/org/img@sha256:same"),
                _component(image="quay.io/org/img2@sha256:same"),
            ]
        )
        data_dir, snapshot_file, data_file, _, result_file = _setup(
            tmp_path,
            snap=snap,
        )

        def side_effect(idx, *a, **kw):
            return {
                "containerImage": f"img{idx}",
                "componentIndex": idx,
                "pyxisImages": [{"imageId": f"id{idx}"}],
            }

        mock_process.side_effect = side_effect
        create_pyxis_image.run(
            server="production",
            snapshot_file=snapshot_file,
            data_file=data_file,
            certified="false",
            is_latest="false",
            rh_push="false",
            process_helm_charts=False,
            concurrent_limit=4,
            pyxis_data_path_result=result_file,
            data_dir=data_dir,
            snapshot_path_relative="results/snapshot.json",
        )
        output = json.loads((data_dir / "results" / "pyxis.json").read_text())
        assert len(output["components"]) == 2

    @patch("create_pyxis_image.process_component")
    def test_process_failure_raises(
        self,
        mock_process,
        tmp_path: Path,
    ) -> None:
        """Raise RuntimeError when component processing fails."""
        snap = _snapshot([_component()])
        data_dir, snapshot_file, data_file, _, result_file = _setup(
            tmp_path,
            snap=snap,
        )
        mock_process.side_effect = RuntimeError("Pyxis error")
        with pytest.raises(RuntimeError, match="One or more component"):
            create_pyxis_image.run(
                server="production",
                snapshot_file=snapshot_file,
                data_file=data_file,
                certified="false",
                is_latest="false",
                rh_push="false",
                process_helm_charts=False,
                concurrent_limit=4,
                pyxis_data_path_result=result_file,
                data_dir=data_dir,
                snapshot_path_relative="results/snapshot.json",
            )

    @patch("create_pyxis_image.process_component")
    def test_include_layers_from_data(
        self,
        mock_process,
        tmp_path: Path,
    ) -> None:
        """Pass includeLayers and appendTags from data file to config."""
        snap = _snapshot([_component()])
        data = _data(include_layers=True, append_tags=True)
        data_dir, snapshot_file, data_file, _, result_file = _setup(
            tmp_path,
            snap=snap,
            data=data,
        )
        mock_process.return_value = {
            "containerImage": "img",
            "componentIndex": 0,
            "pyxisImages": [],
        }
        create_pyxis_image.run(
            server="stage",
            snapshot_file=snapshot_file,
            data_file=data_file,
            certified="false",
            is_latest="false",
            rh_push="false",
            process_helm_charts=False,
            concurrent_limit=4,
            pyxis_data_path_result=result_file,
            data_dir=data_dir,
            snapshot_path_relative="results/snapshot.json",
        )
        _, kwargs = mock_process.call_args
        cfg = kwargs["config"]
        assert cfg.include_layers is True
        assert cfg.append_tags == "true"


# ── _parse_args ─────────────────────────────────────────────────────


class TestParseArgs:
    """Tests for ``_parse_args``."""

    def test_required_args(self) -> None:
        """Parse all required arguments correctly."""
        args = create_pyxis_image._parse_args(
            [
                "--server",
                "production",
                "--snapshot-file",
                "/snap.json",
                "--data-file",
                "/data.json",
                "--pyxis-data-path-result",
                "/result",
                "--snapshot-path-relative",
                "results/snapshot.json",
            ]
        )
        assert args.server == "production"
        assert args.snapshot_file == "/snap.json"
        assert args.concurrent_limit == 16

    def test_defaults(self) -> None:
        """Verify default values for optional arguments."""
        args = create_pyxis_image._parse_args(
            [
                "--server",
                "stage",
                "--snapshot-file",
                "/snap.json",
                "--data-file",
                "/data.json",
                "--pyxis-data-path-result",
                "/result",
                "--snapshot-path-relative",
                "results/snapshot.json",
            ]
        )
        assert args.certified == "false"
        assert args.is_latest == "false"
        assert args.rh_push == "false"
        assert args.process_helm_charts == "false"
        assert args.pyxis_secret_path == "/etc/secrets"
        assert args.data_dir == "/var/workdir/release"


# ── main ────────────────────────────────────────────────────────────


class TestMain:
    """Tests for the ``main`` entry point."""

    @patch("create_pyxis_image.run")
    def test_success(self, mock_run: MagicMock) -> None:
        """Return 0 on successful run."""
        result = create_pyxis_image.main(
            [
                "--server",
                "production",
                "--snapshot-file",
                "/snap.json",
                "--data-file",
                "/data.json",
                "--pyxis-data-path-result",
                "/result",
                "--snapshot-path-relative",
                "results/snapshot.json",
            ]
        )
        assert result == 0
        mock_run.assert_called_once()

    @patch("create_pyxis_image.run")
    def test_passes_correct_types(self, mock_run: MagicMock) -> None:
        """Convert CLI strings to correct Python types before calling run."""
        create_pyxis_image.main(
            [
                "--server",
                "production",
                "--snapshot-file",
                "/snap.json",
                "--data-file",
                "/data.json",
                "--pyxis-data-path-result",
                "/result",
                "--snapshot-path-relative",
                "results/snapshot.json",
                "--process-helm-charts",
                "true",
                "--concurrent-limit",
                "8",
            ]
        )
        _, kwargs = mock_run.call_args
        assert kwargs["process_helm_charts"] is True
        assert kwargs["concurrent_limit"] == 8
        assert isinstance(kwargs["snapshot_file"], Path)
