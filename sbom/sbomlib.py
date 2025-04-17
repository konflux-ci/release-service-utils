"""
This library contains utility functions for SBOM generation and enrichment.
"""

from contextlib import contextmanager
import json
from typing import Optional, Any, Protocol, Union, Generator
from pathlib import Path
from dataclasses import dataclass
import re
import asyncio
import tempfile
import os


from packageurl import PackageURL
import pydantic as pdc

from sbom.logging import get_sbom_logger


logger = get_sbom_logger()


@dataclass
class Image:
    """
    Object representing a single image in some repository.
    """

    digest: str


@dataclass
class IndexImage:
    """
    Object representing an index image in some repository. It also contains
    references to child images.
    """

    digest: str
    children: list[Image]


@dataclass
class Component:
    """
    Internal representation of a Component for SBOM generation purposes.
    """

    name: str
    repository: str
    image: Union[Image, IndexImage]
    tags: list[str]


@dataclass
class Snapshot:
    """
    Internal representation of a Snapshot for SBOM generation purposes.
    """

    components: list[Component]


class SBOMError(Exception):
    """
    Exception that can be raised during SBOM generation and enrichment.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)


class ComponentModel(pdc.BaseModel):
    """
    Model representing a component from the Snapshot.
    """

    name: str
    image_digest: str = pdc.Field(alias="containerImage")
    rh_registry_repo: str = pdc.Field(alias="rh-registry-repo")
    tags: list[str]

    @pdc.field_validator("image_digest", mode="after")
    @classmethod
    def is_valid_digest_reference(cls, value: str) -> str:
        """
        Validates that the digest reference is in the correct format. Does NOT
        support references with a registry port.
        """
        if not re.match(r"^[^:]+@sha256:[0-9a-f]+$", value):
            raise ValueError(f"{value} is not a valid digest reference.")

        # strip repository
        return value.split("@")[1]


class SnapshotModel(pdc.BaseModel):
    """
    Model representing a Snapshot spec file after the apply-mapping task.
    Only the parts relevant to component sboms are parsed.
    """

    components: list[ComponentModel]


class SBOMHandler(Protocol):
    """
    Protocol ensuring that SBOM handlers implement the correct method.
    """

    @classmethod
    def update_sbom(
        cls, component: Component, image: Union[IndexImage, Image], sbom: dict[str, Any]
    ) -> None:
        """
        Update the specified SBOM in-place based on the provided component information.
        """
        raise NotImplementedError()


async def construct_image(repository: str, image_digest: str) -> Union[Image, IndexImage]:
    """
    Creates an Image or IndexImage object based on an image reference. Performs
    a registry call for index images, to parse all their child digests.
    """
    manifest = await get_image_manifest(repository, image_digest)
    media_type = manifest["mediaType"]

    if media_type in {
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    }:
        return Image(digest=image_digest)

    if media_type in {
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    }:
        children = []
        for submanifest in manifest["manifests"]:
            child_digest = submanifest["digest"]
            children.append(Image(child_digest))

        return IndexImage(digest=image_digest, children=children)

    raise SBOMError(f"Unsupported mediaType: {media_type}")


async def make_component(
    name: str, repository: str, image_digest: str, tags: list[str]
) -> Component:
    """
    Creates a component object from input data.
    """
    image: Union[Image, IndexImage] = await construct_image(repository, image_digest)
    return Component(name=name, repository=repository, image=image, tags=tags)


async def make_snapshot(snapshot_spec: Path) -> Snapshot:
    """
    Parse a snapshot spec from a JSON file and create an object representation
    of it. Multiarch images are handled by fetching their index image manifests
    and parsing their children as well.

    Args:
        snapshot_spec (Path): Path to a snapshot spec JSON file
    """
    with open(snapshot_spec, mode="r", encoding="utf-8") as snapshot_file:
        snapshot_model = SnapshotModel.model_validate_json(snapshot_file.read())

    component_tasks = []
    for component_model in snapshot_model.components:
        name = component_model.name
        repository = component_model.rh_registry_repo
        image_digest = component_model.image_digest
        tags = component_model.tags

        component_tasks.append(make_component(name, repository, image_digest, tags))

    components = await asyncio.gather(*component_tasks)

    return Snapshot(components=components)


def construct_purl(
    repository: str, digest: str, arch: Optional[str] = None, tag: Optional[str] = None
) -> str:
    """
    Construct an OCI PackageURL from image data.
    """
    repo_name = repository.split("/")[-1]

    optional_qualifiers = {}
    if arch is not None:
        optional_qualifiers["arch"] = arch

    if tag is not None:
        optional_qualifiers["tag"] = tag

    return PackageURL(
        type="oci",
        name=repo_name,
        version=digest,
        qualifiers={"repository_url": repository, **optional_qualifiers},
    ).to_string()


async def run_async_subprocess(
    cmd: list[str], env: Optional[dict[str, str]] = None, retry_times: int = 0
) -> tuple[int, bytes, bytes]:
    """
    Run command in subprocess asynchronously.

    Args:
        cmd (list[str]): command to run in subprocess.
        env (dict[str, str] | None): environ dict
        retry_times (int): number of retries if the process ends with non-zero return code
    """
    if retry_times < 0:
        raise ValueError("Retry count cannot be negative.")

    for _ in range(1 + retry_times):
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )

        stdout, stderr = await proc.communicate()
        assert proc.returncode is not None  # can't be None after proc.communicate is awaited
        code = proc.returncode
        if code == 0:
            return code, stdout, stderr

    # guaranteed to be bound, the loop runs at least once
    return code, stdout, stderr


async def get_image_manifest(repository: str, image_digest: str) -> dict[str, Any]:
    """
    Gets a dictionary containing the data from a manifest for an image in a
    repository.

    Args:
        repository (str): image repository URL
        image_digest (str): an image digest in the form sha256:<sha>
    """
    reference = make_reference(repository, image_digest)
    logger.info("Fetching manifest for %s", reference)

    with make_oci_auth_file(reference) as authfile:
        code, stdout, stderr = await run_async_subprocess(
            [
                "oras",
                "manifest",
                "fetch",
                "--registry-config",
                authfile,
                reference,
            ],
            retry_times=3,
        )
    if code != 0:
        raise SBOMError(f"Could not get manifest of {reference}: {stderr.decode()}")

    return json.loads(stdout)  # type: ignore


def make_reference(repository: str, image_digest: str) -> str:
    """
    Create a full reference to an image using a repository and image digest.

    Args:
        repository (str): image repository URL
        image_digest (str): an image digest in the form sha256:<sha>

    Examples:
        >>> make_reference("registry.redhat.io/repo", "sha256:deadbeef")
        'registry.redhat.io/repo@sha256:deadbeef'

    """
    return f"{repository}@{image_digest}"


@contextmanager
def make_oci_auth_file(
    reference: str, auth: Optional[Path] = None
) -> Generator[str, Any, None]:
    """
    Gets path to a temporary file containing the docker config JSON for
    <reference>.  Deletes the file after the with statement. If no path to the
    docker config is provided, tries using ~/.docker/config.json . Ports in the
    registry are NOT supported.

    Args:
        reference (str): Reference to an image in the form registry/repo@sha256-deadbeef
        auth (Path | None): Existing docker config.json

    Example:
        >>> with make_oci_auth_file(ref) as auth_path:
                perform_work_in_oci()
    """
    if auth is None:
        auth = Path(os.path.expanduser("~/.docker/config.json"))

    if not auth.is_file():
        raise ValueError(f"No docker config file at {auth}")

    if reference.count(":") > 1:
        logger.warning(
            "Multiple ':' symbols in %s. Registry ports are not supported.", reference
        )

    # Remove digest (e.g. @sha256:...)
    ref = reference.split("@", 1)[0]

    # Registry is up to the first slash
    registry = ref.split("/", 1)[0]

    with open(auth, mode="r", encoding="utf-8") as f:
        config = json.load(f)
    auths = config.get("auths", {})

    current_ref = ref

    try:
        tmpfile = tempfile.NamedTemporaryFile(mode="w", delete=False)
        while True:
            token = auths.get(current_ref)
            if token is not None:
                json.dump({"auths": {registry: token}}, tmpfile)
                tmpfile.close()
                yield tmpfile.name
                return

            if "/" not in current_ref:
                break
            current_ref = current_ref.rsplit("/", 1)[0]

        json.dump({"auths": {}}, tmpfile)
        tmpfile.close()
        yield tmpfile.name
    finally:
        # this also deletes the file
        tmpfile.close()


def without_sha_header(digest: str) -> str:
    """
    Returns a digest without the "sha256:" header.
    """
    return digest.removeprefix("sha256:")


def get_purl_arch(purl_str: str) -> Optional[str]:
    """
    Get the arch qualifier from a PackageURL.
    """
    purl = PackageURL.from_string(purl_str).to_dict()
    return purl["qualifiers"].get("arch")  # type: ignore


def get_purl_digest(purl_str: str) -> str:
    """
    Get the image digest from a PackageURL.
    """
    purl = PackageURL.from_string(purl_str)
    if purl.version is None:
        raise ValueError("SBOM contains invalid OCI Purl: %s", purl_str)
    return purl.version
