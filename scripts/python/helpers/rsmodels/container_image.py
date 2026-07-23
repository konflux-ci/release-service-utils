"""Pydantic models representing the metadata of a container image from skopeo inspect."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from pydantic import BaseModel, Field


class LayerData(BaseModel):
    """Metadata for an individual image layer."""

    media_type: str | None = Field(
        None, alias="MIMEType", description="MIME type of the layer"
    )
    digest: str | None = Field(None, alias="Digest", description="Digest of the layer")
    size: int | None = Field(None, alias="Size", description="Size of the layer in bytes")
    annotations: dict[str, str] | None = Field(
        None, alias="Annotations", description="Layer annotations"
    )

    class Config:
        """Pydantic model configuration."""

        populate_by_name = True


class ContainerImage(BaseModel):
    """Model representing a container image returned by the `skopeo inspect` command."""

    name: str | None = Field(None, alias="Name", description="The image name")
    tag: str | None = Field(None, alias="Tag", description="The image tag")
    digest: str | None = Field(None, alias="Digest", description="The image digest")
    architecture: str | None = Field(
        None, alias="Architecture", description="The image CPU architecture"
    )
    os: str | None = Field(
        None, alias="Os", description="The target operating system of the image"
    )
    created: datetime | None = Field(
        None, alias="Created", description="The timestamp when the image was created"
    )
    docker_version: str | None = Field(
        None,
        alias="DockerVersion",
        description="The version of Docker used to build the image",
    )
    labels: dict[str, str] | None = Field(
        None, alias="Labels", description="A dictionary of image labels"
    )
    layers: list[str] | None = Field(
        None, alias="Layers", description="A list of layer digests in the image"
    )
    layers_data: list[LayerData] | None = Field(
        None, alias="LayersData", description="Detailed metadata for each layer"
    )
    env: list[str] | None = Field(
        None, alias="Env", description="Environment variables set in the image"
    )
    repo_tags: list[str] | None = Field(
        None, alias="RepoTags", description="Other tags associated with the repository"
    )

    config: ContainerImageConfig | None = Field(
        None, alias="Config", description="Container runtime configuration"
    )

    def __getitem__(self, key: str) -> Any:
        """Allow dict-like access to model fields by their field name or alias."""
        if hasattr(self, key) and key in self.model_fields:
            return getattr(self, key)
        for field_name, field in self.model_fields.items():
            if field.alias == key:
                return getattr(self, field_name)
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        """Allow dict-like .get() access."""
        try:
            return self[key]
        except KeyError:
            return default

    @classmethod
    def from_json(cls, json_str: str) -> ContainerImage:
        """Load ContainerImage from a JSON string.

        Args:
            json_str: JSON string representation of a ContainerImage

        Returns:
            ContainerImage instance

        """
        data = json.loads(json_str)
        return cls.model_validate(data)

    @classmethod
    def from_dict(cls, data: dict) -> ContainerImage:
        """Load ContainerImage from a dictionary.

        Args:
            data: Dictionary representation of a ContainerImage

        Returns:
            ContainerImage instance

        """
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, file_path: str | Path) -> ContainerImage:
        """Load ContainerImage from a JSON file.

        Args:
            file_path: Path to JSON file containing a ContainerImage

        Returns:
            ContainerImage instance

        """
        path = Path(file_path)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.model_validate(data)


class ManifestPlatform(BaseModel):
    """Platform configuration for a manifest in a manifest list."""

    architecture: str | None = Field(None, description="CPU architecture")
    os: str | None = Field(None, description="Operating system")
    variant: str | None = Field(None, description="Architecture variant")

    class Config:
        """Pydantic model configuration."""

        populate_by_name = True


class ManifestConfig(BaseModel):
    """Configuration reference in a container manifest."""

    media_type: str | None = Field(
        None, alias="mediaType", description="MIME type of the config"
    )
    size: int | None = Field(None, description="Size of the config in bytes")
    digest: str | None = Field(None, description="Digest of the config")

    class Config:
        """Pydantic model configuration."""

        populate_by_name = True


class ManifestLayer(BaseModel):
    """Layer metadata in a container manifest."""

    media_type: str | None = Field(
        None, alias="mediaType", description="MIME type of the layer"
    )
    size: int | None = Field(None, description="Size of the layer in bytes")
    digest: str | None = Field(None, description="Digest of the layer")

    class Config:
        """Pydantic model configuration."""

        populate_by_name = True


class RawManifestEntry(BaseModel):
    """An individual manifest entry within a manifest list."""

    media_type: str | None = Field(
        None, alias="mediaType", description="MIME type of the manifest"
    )
    size: int | None = Field(None, description="Size of the manifest in bytes")
    digest: str | None = Field(None, description="Digest of the manifest")
    platform: ManifestPlatform | None = Field(None, description="Platform details")

    class Config:
        """Pydantic model configuration."""

        populate_by_name = True


class ContainerImageRaw(BaseModel):
    """Model representing the raw JSON manifest returned by `skopeo inspect --raw`."""

    schema_version: int | None = Field(
        None, alias="schemaVersion", description="Schema version of the manifest"
    )
    media_type: str | None = Field(
        None, alias="mediaType", description="MIME type of the manifest or manifest list"
    )
    manifests: list[RawManifestEntry] | None = Field(
        None, description="List of manifests (for manifest lists)"
    )
    config: ManifestConfig | None = Field(
        None, description="Config object (for single manifests)"
    )
    layers: list[ManifestLayer] | None = Field(
        None, description="List of layer objects (for single manifests)"
    )

    class Config:
        """Pydantic model configuration."""

        populate_by_name = True

    def __getitem__(self, key: str) -> Any:
        """Allow dict-like access to model fields by their field name or alias."""
        if hasattr(self, key) and key in self.model_fields:
            return getattr(self, key)
        for field_name, field in self.model_fields.items():
            if field.alias == key:
                return getattr(self, field_name)
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        """Allow dict-like .get() access."""
        try:
            return self[key]
        except KeyError:
            return default

    @classmethod
    def from_json(cls, json_str: str) -> ContainerImageRaw:
        """Load ContainerImageRaw from a JSON string.

        Args:
            json_str: JSON string representation of a ContainerImageRaw

        Returns:
            ContainerImageRaw instance

        """
        data = json.loads(json_str)
        return cls.model_validate(data)

    @classmethod
    def from_dict(cls, data: dict) -> ContainerImageRaw:
        """Load ContainerImageRaw from a dictionary.

        Args:
            data: Dictionary representation of a ContainerImageRaw

        Returns:
            ContainerImageRaw instance

        """
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, file_path: str | Path) -> ContainerImageRaw:
        """Load ContainerImageRaw from a JSON file.

        Args:
            file_path: Path to JSON file containing a ContainerImageRaw

        Returns:
            ContainerImageRaw instance

        """
        path = Path(file_path)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.model_validate(data)


class ImageConfig(BaseModel):
    """Container runtime configuration from the .config section of skopeo inspect --config."""

    user: str | None = Field(None, alias="User", description="User to run as")
    exposed_ports: dict[str, dict] | None = Field(
        None, alias="ExposedPorts", description="Ports exposed by the container"
    )
    env: list[str] | None = Field(None, alias="Env", description="Environment variables")
    entrypoint: list[str] | None = Field(
        None, alias="Entrypoint", description="Container entrypoint command"
    )
    cmd: list[str] | None = Field(None, alias="Cmd", description="Default command to run")
    volumes: dict[str, dict] | None = Field(
        None, alias="Volumes", description="Volume mount points"
    )
    working_dir: str | None = Field(None, alias="WorkingDir", description="Working directory")
    labels: dict[str, str] | None = Field(None, alias="Labels", description="Image labels")
    stop_signal: str | None = Field(
        None, alias="StopSignal", description="Signal to stop the container"
    )

    class Config:
        """Pydantic model configuration."""

        populate_by_name = True


class RootFS(BaseModel):
    """Root filesystem metadata from skopeo inspect --config."""

    type: str | None = Field(None, description="Filesystem type")
    diff_ids: list[str] | None = Field(None, alias="diff_ids", description="Layer diff IDs")

    class Config:
        """Pydantic model configuration."""

        populate_by_name = True


class HistoryEntry(BaseModel):
    """A single history entry from skopeo inspect --config."""

    created: datetime | None = Field(None, description="Timestamp of the step")
    created_by: str | None = Field(
        None, alias="created_by", description="Command that produced the layer"
    )
    empty_layer: bool | None = Field(
        None, alias="empty_layer", description="Whether this step produced no layer"
    )
    comment: str | None = Field(None, description="Optional comment")

    class Config:
        """Pydantic model configuration."""

        populate_by_name = True


class ContainerImageConfig(BaseModel):
    """Model representing the output of ``skopeo inspect --config``."""

    author: str | None = Field(None, description="Image author")
    architecture: str | None = Field(None, description="CPU architecture")
    created: datetime | None = Field(None, description="Timestamp when the image was created")
    os: str | None = Field(None, description="Target operating system")
    config: ImageConfig | None = Field(None, description="Container runtime configuration")
    rootfs: RootFS | None = Field(None, description="Root filesystem metadata")
    history: list[HistoryEntry] | None = Field(None, description="Build history entries")

    class Config:
        """Pydantic model configuration."""

        populate_by_name = True

    def __getitem__(self, key: str) -> Any:
        """Allow dict-like access to model fields by their field name or alias."""
        if hasattr(self, key) and key in self.model_fields:
            return getattr(self, key)
        for field_name, field in self.model_fields.items():
            if field.alias == key:
                return getattr(self, field_name)
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        """Allow dict-like .get() access."""
        try:
            return self[key]
        except KeyError:
            return default

    @classmethod
    def from_json(cls, json_str: str) -> ContainerImageConfig:
        """Load ContainerImageConfig from a JSON string.

        Args:
            json_str: JSON string representation of a ContainerImageConfig

        Returns:
            ContainerImageConfig instance

        """
        data = json.loads(json_str)
        return cls.model_validate(data)

    @classmethod
    def from_dict(cls, data: dict) -> ContainerImageConfig:
        """Load ContainerImageConfig from a dictionary.

        Args:
            data: Dictionary representation of a ContainerImageConfig

        Returns:
            ContainerImageConfig instance

        """
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, file_path: str | Path) -> ContainerImageConfig:
        """Load ContainerImageConfig from a JSON file.

        Args:
            file_path: Path to JSON file containing a ContainerImageConfig

        Returns:
            ContainerImageConfig instance

        """
        path = Path(file_path)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.model_validate(data)
