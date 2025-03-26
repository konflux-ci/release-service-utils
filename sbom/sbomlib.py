import json
from typing import IO, Optional, Any, TextIO, Union
from pathlib import Path
from dataclasses import dataclass
import re
import asyncio
import tempfile
import os


# FIXME: remove pydantic
from packageurl import PackageURL
import pydantic as pdc


@dataclass
class Image:
    digest: str


@dataclass
class IndexImage:
    digest: str
    children: list[Image]


@dataclass
class Component:
    """
    Internal representation of a Component for SBOM generation purposes.
    """

    # Original regex from:
    # https://github.com/konflux-ci/release-service-catalog/blob/0c97b5076ab70e5fdc2660eea2216de07f42c045/tasks/managed/populate-release-notes/populate-release-notes.yaml#L46
    # FIXME: is this right?
    unique_tag_regex = re.compile(r"(rhel-)?v?[0-9]+\.[0-9]+(\.[0-9]+)?-[0-9]{8,}")

    repository: str
    image: Union[Image, IndexImage]
    tags: list[str]

    @property
    def unique_tag(self) -> Optional[str]:
        for tag in self.tags:
            if self.unique_tag_regex.match(tag) is not None:
                return tag

        return None


@dataclass
class Snapshot:
    """
    Internal representation of a Snapshot for SBOM generation purposes.
    """

    components: list[Component]


class SBOMError(Exception):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)


class ComponentModel(pdc.BaseModel):
    """
    Model representing a component from the Snapshot.
    """

    image_digest: str = pdc.Field(alias="containerImage")
    rh_registry_repo: str = pdc.Field(alias="rh-registry-repo")
    tags: list[str]

    @pdc.field_validator("image_digest", mode="after")
    @classmethod
    def is_valid_digest_reference(cls, value: str) -> str:
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


async def construct_image(repository: str, image_digest: str) -> Union[Image, IndexImage]:
    manifest = await get_image_manifest(repository, image_digest)
    media_type = manifest["mediaType"]

    if (
        media_type == "application/vnd.oci.image.manifest.v1+json"
        or media_type == "application/vnd.docker.distribution.manifest.v2+json"
    ):
        return Image(digest=image_digest)

    if (
        media_type == "application/vnd.oci.image.index.v1+json"
        or media_type == "application/vnd.docker.distribution.manifest.list.v2+json"
    ):
        children = []
        for submanifest in manifest["manifests"]:
            child_digest = submanifest["digest"]
            children.append(Image(child_digest))

        return IndexImage(digest=image_digest, children=children)

    # unsupported mediaType
    # FIXME: log a warning and handle somehow
    assert False


async def make_component(repository: str, image_digest: str, tags: list[str]) -> Component:
    image: Union[Image, IndexImage] = await construct_image(repository, image_digest)
    return Component(repository=repository, image=image, tags=tags)


async def make_snapshot(snapshot_spec: Path) -> Snapshot:
    with open(snapshot_spec, "r") as snapshot_file:
        snapshot_model = SnapshotModel.model_validate_json(snapshot_file.read())

    component_tasks = []
    for component_model in snapshot_model.components:
        repository = component_model.rh_registry_repo
        image_digest = component_model.image_digest
        tags = component_model.tags

        component_tasks.append(make_component(repository, image_digest, tags))

    components = await asyncio.gather(*component_tasks)

    return Snapshot(components=components)


def construct_purl(
    repository: str, digest: str, arch: Optional[str] = None, tag: Optional[str] = None
) -> str:
    repo_name = repository.split("/")[-1]

    # encoded_digest = digest.replace(":", "%3A")

    optional_qualifiers = {}
    if arch is not None:
        optional_qualifiers["arch"] = arch

    if tag is not None:
        optional_qualifiers["tag"] = tag

    purl = PackageURL(
        type="oci",
        name=repo_name,
        version=digest,
        qualifiers={"repository_url": repository, **optional_qualifiers},
    )

    return str(purl)


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
    repository. Tries using the ~/.docker/config.json file for authentication.

    Args:
        repository (str): image repository URL
        image_digest (str): an image digest in the form sha256:<sha>
    """
    reference = make_reference(repository, image_digest)

    with tempfile.NamedTemporaryFile("+w") as authfile:
        if not get_oci_auth_file(
            reference,
            Path(os.path.expanduser("~/.docker/config.json")),
            authfile,
        ):
            raise ValueError(f"Could not get OCI auth for {reference}.")

        code, stdout, stderr = await run_async_subprocess(
            [
                "oras",
                "manifest",
                "fetch",
                "--registry-config",
                authfile.name,
                reference,
            ]
        )
    if code != 0:
        raise RuntimeError(f"Could not get manifest of {reference}: {stderr.decode()}")

    return json.loads(stdout)


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


def get_oci_auth_file(reference: str, auth: Path, fp: Any) -> bool:
    """
    Gets path to a temporary file containing the docker config JSON for <reference>.
    Returns True if a token was found, False otherwise.

    Args:
        reference (str): Reference to an image in the form registry/repo@sha256-deadbeef
        auth (Path): Existing docker config.json
        fp: File object to write the new auth file to
    """
    if not auth.is_file():
        raise ValueError(f"No docker config file at {auth}")

    # Remove digest (e.g. @sha256:...)
    ref = reference.split("@", 1)[0]

    # Registry is up to the first slash
    # FIXME: handle also no repository option
    registry = ref.split("/", 1)[0]

    with open(auth, "r") as f:
        config = json.load(f)
    auths = config.get("auths", {})

    current_ref = ref

    while True:
        token = auths.get(current_ref)
        if token is not None:
            json.dump({"auths": {registry: token}}, fp)
            fp.flush()
            return True

        if "/" not in current_ref:
            break
        current_ref = current_ref.rsplit("/", 1)[0]

    json.dump({"auths": {}}, fp)
    fp.flush()
    return False


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
    return purl["qualifiers"].get("arch")


def get_purl_digest(purl_str: str) -> str:
    """
    Get the image digest from a PackageURL.
    """
    purl = PackageURL.from_string(purl_str)
    if purl.version is None:
        raise ValueError()
    return purl.version
