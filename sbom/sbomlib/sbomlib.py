import json
from typing import IO, Optional, Any
from pathlib import Path
from dataclasses import dataclass
import re
import asyncio
import tempfile
import os


# TODO: maybe we don't need pydantic
import pydantic as pdc


@dataclass
class Image:
    digest: str
    children: list["Image"]

    @property
    def is_index(self) -> bool:
        return self.children != 0


@dataclass
class Component:
    """
    Internal representation of a Component for SBOM generation purposes.
    """

    repository: str
    image: Image


@dataclass
class Snapshot:
    """
    Internal representation of a Snapshot for SBOM generation purposes.
    """

    components: list[Component]
    tags: list[str]

    # TODO: this has to be somehow optional for pipelines without CPE
    cpe: str


class ComponentModel(pdc.BaseModel):
    """
    Model representing a component from the Snapshot.
    """

    image_digest: str = pdc.Field(alias="containerImage")
    rh_registry_repo: str = pdc.Field(alias="rh-registry-repo")

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


async def construct_image_tree(repository: str, image_digest: str) -> Image:
    """
    Get all references to images in the format <repository>@<digest> for which
    SBOMs should be fetched.

    If the image pointed to by <repository> and <image_digest> is an index image
    or a docker manifest V2, recurse and get also its child images.
    """
    manifest = await get_image_manifest(repository, image_digest)
    media_type = manifest["mediaType"]

    if (
        media_type == "application/vnd.oci.image.manifest.v1+json"
        or media_type == "application/vnd.docker.distribution.manifest.v2+json"
    ):
        return Image(digest=image_digest, children=[])

    if (
        media_type == "application/vnd.oci.image.index.v1+json"
        or media_type == "application/vnd.docker.distribution.manifest.list.v2+json"
    ):
        children_tasks = []
        for submanifest in manifest["manifests"]:
            child_digest = submanifest["digest"]
            children_tasks.append(construct_image_tree(repository, child_digest))

        children = await asyncio.gather(*children_tasks)
        return Image(digest=image_digest, children=children)

    # unsupported mediaType
    # FIXME: log a warning and handle somehow
    assert False


async def make_component(repository: str, image_digest: str) -> Component:
    image_tree = await construct_image_tree(repository, image_digest)
    return Component(repository=repository, image=image_tree)


async def make_snapshot(snapshot_spec: Path, rpa: Path) -> Snapshot:
    with open(snapshot_spec, "r") as snapshot_file:
        snapshot_model = SnapshotModel.model_validate_json(snapshot_file.read())

    component_tasks = []
    for component_model in snapshot_model.components:
        repository = component_model.rh_registry_repo
        image_digest = component_model.image_digest

        component_tasks.append(make_component(repository, image_digest))

    components = await asyncio.gather(*component_tasks)

    # with open(data, "r") as data_file:
    #     data_dict = json.load(data_file)

    # TODO: load tags
    tags = []
    cpe = ""  # data_dict["releaseNotes"]["cpe"]

    return Snapshot(components=components, tags=tags, cpe=cpe)


def construct_purl(component: Component, image: Image) -> str:
    # TODO: add tags ( which ones? )

    repo_name = component.repository.split("/")[-1]

    encoded_digest = image.digest.replace(":", "%3A")

    repository_url = "/".join(component.repository.split("/")[:-1])

    purl = f"pkg:oci/{repo_name}@{encoded_digest}?repository_url={repository_url}"

    return purl


async def run_async_subprocess(
    cmd: list[str], env: Optional[dict[str, str]] = None
) -> tuple[int, bytes, bytes]:
    """
    Run command in subprocess asynchronously.

    Args:
        cmd (list[str]): command to run in subprocess.
        env (dict[str, str] | None): environ dict
    """
    # TODO: implement retry mechanism

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )

    stdout, stderr = await proc.communicate()
    assert proc.returncode is not None
    return proc.returncode, stdout, stderr


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
        raise RuntimeError(f"Could not get manifest of {reference}: {stderr}")

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


def get_oci_auth_file(reference: str, auth: Path, fp: IO) -> bool:
    """
    Gets path to a temporary file containing the docker config JSON for <reference>.
    Returns True if a token was found, False otherwise.

    Args:
        reference (str): Reference to an image in the form registry/repo@sha256-deadbeef
        auth (Path): Existing docker config.json
        fp (IO): File object to write the new auth file to
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
