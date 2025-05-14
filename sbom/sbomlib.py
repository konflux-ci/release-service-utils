"""
This library contains utility functions for SBOM generation and enrichment.
"""

from contextlib import contextmanager
import hashlib
import json
from typing import Optional, Any, Protocol, Generator
from pathlib import Path
from dataclasses import dataclass
import re
import asyncio
import tempfile
import os
import typing
import base64
import datetime

import dateutil.parser
from packageurl import PackageURL
import pydantic as pdc

from sbom.logging import get_sbom_logger


logger = get_sbom_logger()


@dataclass
class Image:
    """
    Object representing a single image in a repository.
    """

    repository: str
    digest: str

    @property
    def reference(self) -> str:
        return f"{self.repository}@{self.digest}"

    def __str__(self) -> str:
        return self.reference


@dataclass
class IndexImage(Image):
    """
    Object representing an index image in a repository. It also contains child
    images.
    """

    children: list[Image]


@dataclass
class Component:
    """
    Internal representation of a Component for SBOM generation purposes.
    """

    name: str
    image: Image
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


class SBOMVerificationError(SBOMError):
    """
    Exception raised when an SBOM's digest does not match that in the provenance.
    """

    def __init__(self, expected: str, actual: str, *args: object, **kwargs: object) -> None:
        self.expected = expected
        self.actual = actual
        message = (
            "SBOM digest verification from provenance failed. "
            f"Expected digest: {expected}, actual digest: {actual}"
        )
        super().__init__(message, *args, **kwargs)


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

    def update_sbom(self, component: Component, image: Image, sbom: dict[str, Any]) -> None:
        """
        Update the specified SBOM in-place based on the provided component information.
        """
        raise NotImplementedError()

    @classmethod
    def supports(cls, sbom: dict) -> bool:
        """
        Returns true if the provided SBOM is supported by this handler.
        """
        raise NotImplementedError()


async def construct_image(repository: str, image_digest: str) -> Image:
    """
    Creates an Image or IndexImage object based on an image reference. Performs
    a registry call for index images, to parse all their child digests.
    """
    image = Image(repository, image_digest)
    manifest = await get_image_manifest(image)
    media_type = manifest["mediaType"]

    if media_type in {
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    }:
        return image

    if media_type in {
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    }:
        children = []
        for submanifest in manifest["manifests"]:
            child_digest = submanifest["digest"]
            children.append(Image(repository, child_digest))

        return IndexImage(repository, image_digest, children=children)

    raise SBOMError(f"Unsupported mediaType: {media_type}")


async def make_component(
    name: str, repository: str, image_digest: str, tags: list[str]
) -> Component:
    """
    Creates a component object from input data.
    """
    image: Image = await construct_image(repository, image_digest)
    return Component(name=name, image=image, tags=tags)


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


def construct_purl(image: Image, arch: Optional[str] = None, tag: Optional[str] = None) -> str:
    """
    Construct an OCI PackageURL string from image data.
    """
    purl = construct_purl_object(image, arch, tag)
    return purl.to_string()


def construct_purl_object(
    image: Image, arch: Optional[str] = None, tag: Optional[str] = None
) -> PackageURL:
    """
    Construct an OCI PackageURL from image data.
    """
    repo_name = image.repository.split("/")[-1]

    optional_qualifiers = {}
    if arch is not None:
        optional_qualifiers["arch"] = arch

    if tag is not None:
        optional_qualifiers["tag"] = tag

    return PackageURL(
        type="oci",
        name=repo_name,
        version=image.digest,
        qualifiers={"repository_url": image.repository, **optional_qualifiers},
    )


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


async def get_image_manifest(image: Image) -> dict[str, Any]:
    """
    Gets a dictionary containing the data from a manifest for an image in a
    repository.

    Args:
        repository (str): image repository URL
        image_digest (str): an image digest in the form sha256:<sha>
    """
    logger.info("Fetching manifest for %s", image)

    with make_oci_auth_file(image) as authfile:
        code, stdout, stderr = await run_async_subprocess(
            [
                "oras",
                "manifest",
                "fetch",
                "--registry-config",
                authfile,
                image.reference,
            ],
            retry_times=3,
        )
    if code != 0:
        raise SBOMError(f"Could not get manifest of {image}: {stderr.decode()}")

    return json.loads(stdout)  # type: ignore


@contextmanager
def make_oci_auth_file(image: Image, auth: Optional[Path] = None) -> Generator[str, Any, None]:
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

    if image.reference.count(":") > 1:
        logger.warning("Multiple ':' symbols in %s. Registry ports are not supported.", image)

    # Registry is up to the first slash
    registry = image.repository.split("/", 1)[0]

    with open(auth, mode="r", encoding="utf-8") as f:
        config = json.load(f)
    auths = config.get("auths", {})

    current_ref = image.repository

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
    purl = PackageURL.from_string(purl_str)
    if isinstance(purl.qualifiers, dict):
        return purl.qualifiers.get("arch")

    logger.warning("Parsed qualifiers from purl %s are not a dictionary.", purl_str)
    return None


def get_purl_digest(purl_str: str) -> str:
    """
    Get the image digest from a PackageURL.
    """
    purl = PackageURL.from_string(purl_str)
    if purl.version is None:
        raise SBOMError(f"SBOM contains invalid OCI Purl: {purl_str}")
    return purl.version


class Provenance02:
    """
    Object containing the data of an provenance attestation.
    """

    predicate_type = "https://slsa.dev/provenance/v0.2"

    def __init__(self, predicate: Any) -> None:
        self.predicate = predicate

    @staticmethod
    def from_cosign_output(raw: bytes) -> "Provenance02":
        encoded = json.loads(raw)
        att = json.loads(base64.b64decode(encoded["payload"]))
        if (pt := att.get("predicateType")) != Provenance02.predicate_type:
            raise ValueError(
                f"Cannot parse predicateType {pt}. Expected {Provenance02.predicate_type}"
            )

        predicate = att.get("predicate", {})
        return Provenance02(predicate)

    @property
    def build_finished_on(self) -> datetime.datetime:
        """
        Return datetime of the build being finished.
        If it's not available, fallback to datetime.min.
        """
        if self.predicate is None:
            raise ValueError("Cannot get build time from uninitialized provenance.")

        finished_on: Optional[str] = self.predicate.get("metadata", {}).get("buildFinishedOn")
        if finished_on:
            return dateutil.parser.isoparse(finished_on)

        return datetime.datetime.min

    def get_sbom_digest(self, image: Image) -> str:
        """
        Find the SBOM_BLOB_URL value in the provenance for the supplied image.
        """
        sbom_blob_urls: dict[str, str] = {}
        tasks = self.predicate.get("buildConfig", {}).get("tasks", [])
        for task in tasks:
            curr_digest, sbom_url = "", ""
            for result in task.get("results", []):
                if result.get("name") == "SBOM_BLOB_URL":
                    sbom_url = result.get("value")
                if result.get("name") == "IMAGE_DIGEST":
                    curr_digest = result.get("value")
            if not all([curr_digest, sbom_url]):
                continue
            sbom_blob_urls[curr_digest] = sbom_url

        blob_url = sbom_blob_urls.get(image.digest)
        if blob_url is None:
            raise SBOMError(f"No SBOM_BLOB_URL found in attestation for image {image}.")

        return blob_url.split("@", 1)[1]


class SBOM:
    def __init__(self, doc: dict[Any, Any], digest: str) -> None:
        """
        An SBOM downloaded using cosign.

        Attributes:
            doc (dict): The parsed SBOM dictionary
            digest (str): SHA256 digest of the raw SBOM data
        """
        self.doc = doc
        self.digest = digest

    @staticmethod
    async def from_cosign_output(raw: bytes) -> "SBOM":
        """
        Create an SBOM object from a line of raw "cosign download sbom" output.
        """
        doc = json.loads(raw)
        hexdigest = f"sha256:{hashlib.sha256(raw).hexdigest()}"
        return SBOM(doc, hexdigest)


class Cosign(typing.Protocol):
    async def fetch_provenances(self, image: Image) -> list[Provenance02]:
        return NotImplemented

    async def fetch_latest_provenance(self, image: Image) -> Provenance02:
        return NotImplemented

    async def fetch_sbom(self, image: Image) -> SBOM:
        return NotImplemented


class CosignClient(Cosign):
    """
    Client used to get OCI artifacts using Cosign.
    """

    def __init__(self, verification_key: Path) -> None:
        """
        Args:
            verification_key: Path to public key used to verify attestations.
        """
        self.verification_key = verification_key

    async def fetch_provenances(self, image: Image) -> list[Provenance02]:
        """
        Fetch all provenances for the supplied image.
        """
        with make_oci_auth_file(image) as authfile:
            cmd = [
                "cosign",
                "verify-attestation",
                f"--key={self.verification_key}",
                "--type=slsaprovenance02",
                "--insecure-ignore-tlog=true",
                image.reference,
            ]
            logger.debug("Fetching provenance for %s using '%s'", image, " ".join(cmd))
            code, stdout, stderr = await run_async_subprocess(
                cmd,
                env={"DOCKER_CONFIG": authfile},
                retry_times=3,
            )

        if code != 0:
            raise SBOMError(f"Failed to fetch provenance for {image}: {stderr.decode()}.")

        attestations: list[Provenance02] = []
        for raw_attestation in stdout.splitlines():
            att = Provenance02.from_cosign_output(raw_attestation)
            attestations.append(att)

        return attestations

    async def fetch_latest_provenance(self, image: Image) -> Provenance02:
        """
        Fetch the latest provenance based on the supplied image based on the
        time the image build finished.
        """
        provenances = await self.fetch_provenances(image)
        if len(provenances) == 0:
            raise SBOMError(f"No provenances parsed for image {image}.")

        return sorted(provenances, key=lambda x: x.build_finished_on, reverse=True)[0]

    async def fetch_sbom(self, image: Image) -> SBOM:
        """
        Fetch and save the SBOM for the supplied image to a directory.
        """
        with make_oci_auth_file(image) as authfile:
            code, stdout, stderr = await run_async_subprocess(
                ["cosign", "download", "sbom", image.reference],
                env={"DOCKER_CONFIG": authfile},
                retry_times=3,
            )

        if code != 0:
            raise SBOMError(f"Failed to fetch SBOM {image}: {stderr.decode()}")

        return await SBOM.from_cosign_output(stdout)
