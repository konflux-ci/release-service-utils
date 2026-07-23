"""Unit tests for ContainerImage Pydantic model."""

from __future__ import annotations

import json
from datetime import datetime, timezone
import pytest
from rsmodels import ContainerImage, ContainerImageConfig, ContainerImageRaw


@pytest.fixture
def sample_container_data() -> dict:
    """Return sample skopeo inspect dictionary data for testing."""
    d1 = "sha256:2ae81599c5e4d682162cc4825c81d11edb6e56314bff64cf3d7ddf54aef1f38b"
    d2 = "sha256:837b9d7bd4c8301d318ec8c5cd7e5aab81e392d60e90b733f39c67bbadc97aef"
    return {
        "Name": "quay.io/redhat-prod/discovery----discovery-server-rhel9",
        "Tag": "latest",
        "Digest": d1,
        "Created": "2026-06-29T20:23:52.866061036Z",
        "DockerVersion": "1.44.0",
        "Labels": {
            "architecture": "x86_64",
            "com.redhat.component": "discovery-container",
        },
        "Architecture": "amd64",
        "Os": "linux",
        "Layers": ["sha256:837b9d7bd4c8301d318ec8c5cd7e5aab81e392d60e90b733f39c67bbadc97aef"],
        "LayersData": [
            {
                "MIMEType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                "Digest": d2,
                "Size": 40689274,
                "Annotations": {"key": "value"},
            }
        ],
        "Env": ["container=oci", "LANG=C.UTF-8"],
        "RepoTags": ["latest", "v2.6.3"],
    }


def test_container_image_from_dict(sample_container_data: dict) -> None:
    """Test loading ContainerImage using the from_dict method."""
    img = ContainerImage.from_dict(sample_container_data)

    assert isinstance(img, ContainerImage)
    assert img.name == "quay.io/redhat-prod/discovery----discovery-server-rhel9"
    assert img.tag == "latest"
    assert (
        img.digest == "sha256:2ae81599c5e4d682162cc4825c81d11edb6e56314bff64cf3d7ddf54aef1f38b"
    )
    assert img.architecture == "amd64"
    assert img.os == "linux"
    assert img.created == datetime(2026, 6, 29, 20, 23, 52, 866061, tzinfo=timezone.utc)
    assert img.docker_version == "1.44.0"
    assert img.labels == {
        "architecture": "x86_64",
        "com.redhat.component": "discovery-container",
    }
    assert img.layers == [
        "sha256:837b9d7bd4c8301d318ec8c5cd7e5aab81e392d60e90b733f39c67bbadc97aef"
    ]
    assert len(img.layers_data) == 1
    assert img.layers_data[0].media_type == "application/vnd.docker.image.rootfs.diff.tar.gzip"
    assert (
        img.layers_data[0].digest
        == "sha256:837b9d7bd4c8301d318ec8c5cd7e5aab81e392d60e90b733f39c67bbadc97aef"
    )
    assert img.layers_data[0].size == 40689274
    assert img.layers_data[0].annotations == {"key": "value"}
    assert img.env == ["container=oci", "LANG=C.UTF-8"]
    assert img.repo_tags == ["latest", "v2.6.3"]


def test_container_image_from_json(sample_container_data: dict) -> None:
    """Test loading ContainerImage using the from_json method."""
    json_str = json.dumps(sample_container_data)
    img = ContainerImage.from_json(json_str)

    assert isinstance(img, ContainerImage)
    assert img.name == "quay.io/redhat-prod/discovery----discovery-server-rhel9"
    assert (
        img.digest == "sha256:2ae81599c5e4d682162cc4825c81d11edb6e56314bff64cf3d7ddf54aef1f38b"
    )


def test_container_image_from_file(
    sample_container_data: dict, tmp_path: pytest.TempPathFactory
) -> None:
    """Test loading ContainerImage from a file using the from_file method."""
    file_path = tmp_path / "skopeo_inspect.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(sample_container_data, f)

    img = ContainerImage.from_file(file_path)

    assert isinstance(img, ContainerImage)
    assert img.name == "quay.io/redhat-prod/discovery----discovery-server-rhel9"
    assert (
        img.digest == "sha256:2ae81599c5e4d682162cc4825c81d11edb6e56314bff64cf3d7ddf54aef1f38b"
    )


@pytest.fixture
def sample_raw_manifest_data() -> dict:
    """Return sample skopeo inspect --raw manifest list dictionary data for testing."""
    d1 = "sha256:7f4f6ce62705fe518e3513957b4205f106992cd42d1b07975d11aff2dc6ced66"
    return {
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "manifests": [
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 596,
                "digest": d1,
                "platform": {
                    "architecture": "amd64",
                    "os": "linux",
                },
            }
        ],
    }


def test_container_image_raw_from_dict(sample_raw_manifest_data: dict) -> None:
    """Test loading ContainerImageRaw using the from_dict method."""
    img = ContainerImageRaw.from_dict(sample_raw_manifest_data)

    assert isinstance(img, ContainerImageRaw)
    assert img.schema_version == 2
    assert img.media_type == "application/vnd.docker.distribution.manifest.list.v2+json"
    assert len(img.manifests) == 1
    assert (
        img.manifests[0].media_type == "application/vnd.docker.distribution.manifest.v2+json"
    )
    assert img.manifests[0].size == 596
    assert (
        img.manifests[0].digest
        == "sha256:7f4f6ce62705fe518e3513957b4205f106992cd42d1b07975d11aff2dc6ced66"
    )
    assert img.manifests[0].platform.architecture == "amd64"
    assert img.manifests[0].platform.os == "linux"


def test_container_image_raw_from_json(sample_raw_manifest_data: dict) -> None:
    """Test loading ContainerImageRaw using the from_json method."""
    json_str = json.dumps(sample_raw_manifest_data)
    img = ContainerImageRaw.from_json(json_str)

    assert isinstance(img, ContainerImageRaw)
    assert img.schema_version == 2


def test_container_image_raw_from_file(
    sample_raw_manifest_data: dict, tmp_path: pytest.TempPathFactory
) -> None:
    """Test loading ContainerImageRaw from a file using the from_file method."""
    file_path = tmp_path / "raw_manifest.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(sample_raw_manifest_data, f)

    img = ContainerImageRaw.from_file(file_path)

    assert isinstance(img, ContainerImageRaw)
    assert img.schema_version == 2
    assert img.media_type == "application/vnd.docker.distribution.manifest.list.v2+json"


@pytest.fixture
def sample_config_data() -> dict:
    """Return sample skopeo inspect --config dictionary data for testing."""
    return {
        "created": "2026-06-29T20:23:52.866061036Z",
        "architecture": "amd64",
        "os": "linux",
        "config": {
            "User": "1001",
            "ExposedPorts": {"8000/tcp": {}},
            "Env": [
                "container=oci",
                "LANG=C.UTF-8",
                "PATH=/opt/venv/bin:/usr/local/bin:/usr/bin",
            ],
            "Entrypoint": ["/bin/bash"],
            "Cmd": ["/deploy/entrypoint_web.sh"],
            "Volumes": {"/var/data": {}, "/var/log": {}},
            "WorkingDir": "/app",
            "Labels": {
                "architecture": "x86_64",
                "com.redhat.component": "discovery-container",
                "version": "2.6.3",
            },
            "StopSignal": "SIGTERM",
        },
        "rootfs": {
            "type": "layers",
            "diff_ids": [
                "sha256:76c30a19831466a2388cd37770733c4a149dba7034565b568358b06088a23bf2",
                "sha256:4a59de577590d0a72d9d445d5ca61d1a214fc69c231a594485c0b9eb6997cc49",
            ],
        },
        "history": [
            {
                "created": "2026-06-25T05:47:54.833873537Z",
                "created_by": '/bin/sh -c #(nop) LABEL maintainer="Red Hat, Inc."',
                "empty_layer": True,
            }
        ],
    }


def test_container_image_config_from_dict(sample_config_data: dict) -> None:
    """Test loading ContainerImageConfig using the from_dict method."""
    img = ContainerImageConfig.from_dict(sample_config_data)

    assert isinstance(img, ContainerImageConfig)
    assert img.created == datetime(2026, 6, 29, 20, 23, 52, 866061, tzinfo=timezone.utc)
    assert img.architecture == "amd64"
    assert img.os == "linux"

    assert img.config is not None
    assert img.config.user == "1001"
    assert img.config.exposed_ports == {"8000/tcp": {}}
    assert img.config.env == [
        "container=oci",
        "LANG=C.UTF-8",
        "PATH=/opt/venv/bin:/usr/local/bin:/usr/bin",
    ]
    assert img.config.entrypoint == ["/bin/bash"]
    assert img.config.cmd == ["/deploy/entrypoint_web.sh"]
    assert img.config.volumes == {"/var/data": {}, "/var/log": {}}
    assert img.config.working_dir == "/app"
    assert img.config.labels == {
        "architecture": "x86_64",
        "com.redhat.component": "discovery-container",
        "version": "2.6.3",
    }
    assert img.config.stop_signal == "SIGTERM"

    assert img.rootfs is not None
    assert img.rootfs.type == "layers"
    assert len(img.rootfs.diff_ids) == 2

    assert img.history is not None
    assert len(img.history) == 1
    assert img.history[0].created_by == ('/bin/sh -c #(nop) LABEL maintainer="Red Hat, Inc."')
    assert img.history[0].empty_layer is True


def test_container_image_config_from_json(sample_config_data: dict) -> None:
    """Test loading ContainerImageConfig using the from_json method."""
    json_str = json.dumps(sample_config_data)
    img = ContainerImageConfig.from_json(json_str)

    assert isinstance(img, ContainerImageConfig)
    assert img.architecture == "amd64"
    assert img.config.labels["com.redhat.component"] == "discovery-container"


def test_container_image_config_from_file(
    sample_config_data: dict, tmp_path: pytest.TempPathFactory
) -> None:
    """Test loading ContainerImageConfig from a file using the from_file method."""
    file_path = tmp_path / "config_inspect.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(sample_config_data, f)

    img = ContainerImageConfig.from_file(file_path)

    assert isinstance(img, ContainerImageConfig)
    assert img.architecture == "amd64"
    assert img.config.user == "1001"


def test_container_image_config_optional_fields() -> None:
    """Test ContainerImageConfig with minimal data."""
    img = ContainerImageConfig.from_dict({"architecture": "arm64"})

    assert img.architecture == "arm64"
    assert img.config is None
    assert img.rootfs is None
    assert img.history is None
    assert img.created is None
    assert img.author is None


def test_container_image_config_dict_access(sample_config_data: dict) -> None:
    """Test dict-like access on ContainerImageConfig."""
    img = ContainerImageConfig.from_dict(sample_config_data)

    assert img["architecture"] == "amd64"
    assert img.get("os") == "linux"
    assert img.get("nonexistent", "default") == "default"
