"""Helpers for parsing and normalizing container image references."""

from __future__ import annotations

import json
import re

import requests

from release_service_utils.helpers import http_client
from release_service_utils.helpers.logger import logger

_QUAY_SHA_TAG = re.compile(r"^[0-9a-f]{40}$")
_MAX_QUAY_TAG_PAGES = 50

# (registry host prefix, quay.io temp-namespace prefix) pairs used by
# convert_to_quay/convert_to_registry to bridge the migration from quay.io
# temp namespaces to registry.redhat.io-style repository paths. This is a
# temporary mapping that should be removed once all repositories are
# migrated to registry.redhat.io.
_REGISTRY_QUAY_PAIRS: tuple[tuple[str, str], ...] = (
    ("registry.redhat.io", "quay.io/redhat-prod"),
    ("registry.stage.redhat.io", "quay.io/redhat-pending"),
    ("flatpaks.registry.redhat.io", "quay.io/rh-flatpaks-prod"),
    ("flatpaks.registry.stage.redhat.io", "quay.io/rh-flatpaks-stage"),
)


def split_image_ref(image: str) -> tuple[str, str]:
    """Split a container image reference into ``(repository, digest)``.

    Expects the format ``repository@algo:hex`` (e.g.
    ``quay.io/org/img@sha256:abc123``).  Returns ``(repository, digest)``
    where *digest* includes the algorithm prefix (``sha256:abc123``).

    Raises ``ValueError`` when the reference contains no ``@`` separator.
    """
    if "@" not in image:
        raise ValueError(f"image reference missing digest separator '@': {image!r}")
    repo, digest = image.split("@", 1)
    return repo, digest


def registry(ref: str) -> str:
    """Return the registry hostname from an OCI image reference.

    >>> registry("quay.io/org/repo@sha256:abc123")
    'quay.io'
    >>> registry("docker.io/library/nginx:latest")
    'docker.io'
    """
    return ref.split("/", 1)[0]


def repository(ref: str) -> str:
    """Return the full repository path (registry + path) without digest or tag.

    >>> repository("quay.io/org/repo@sha256:abc123")
    'quay.io/org/repo'
    >>> repository("quay.io/org/repo:v1.0")
    'quay.io/org/repo'
    """
    without_digest = ref.split("@", 1)[0]
    last_segment = without_digest.rsplit("/", 1)[-1]
    if ":" in last_segment:
        without_digest = without_digest.rsplit(":", 1)[0]
    return without_digest


def translate_delivery_repo(repo: str) -> list[dict[str, str]]:
    """Translate a Quay delivery-repo reference to public registry URLs.

    Return two dicts with `repo` and `url` keys: one for `redhat.io` and
    one for `access.redhat.com`.
    """
    if not repo.strip():
        msg = "Please pass a repo to translate like 'quay.io/redhat-prod/product----repo'"
        raise ValueError(msg)

    normalized = repo.replace("----", "/")
    io_url: str
    access_url: str

    if normalized.startswith("quay.io/redhat-prod/"):
        io_url = "registry.redhat.io" + normalized[len("quay.io/redhat-prod") :]
        access_url = "registry.access.redhat.com" + normalized[len("quay.io/redhat-prod") :]
    elif normalized.startswith("quay.io/redhat-pending/"):
        io_url = "registry.stage.redhat.io" + normalized[len("quay.io/redhat-pending") :]
        access_url = (
            "registry.access.stage.redhat.com" + normalized[len("quay.io/redhat-pending") :]
        )
    elif normalized.startswith("quay.io/rh-flatpaks-prod/"):
        io_url = "flatpaks.registry.redhat.io" + normalized[len("quay.io/rh-flatpaks-prod") :]
        access_url = (
            "registry.access.redhat.com" + normalized[len("quay.io/rh-flatpaks-prod") :]
        )
    elif normalized.startswith("quay.io/rh-flatpaks-stage/"):
        io_url = (
            "flatpaks.registry.stage.redhat.io"
            + normalized[len("quay.io/rh-flatpaks-stage") :]
        )
        access_url = (
            "registry.access.stage.redhat.com" + normalized[len("quay.io/rh-flatpaks-stage") :]
        )
    elif normalized.startswith("quay.io/redhat/"):
        io_url = "registry.redhat.io" + normalized[len("quay.io/redhat") :]
        access_url = "registry.access.redhat.com" + normalized[len("quay.io/redhat") :]
    else:
        logger.warning(
            "Repo to translate is not in expected format. If this is not "
            "an index image, the expected format is: "
            "quay.io/redhat-[prod,pending]/product----repo",
        )
        io_url = normalized
        access_url = ""

    return [
        {"repo": "redhat.io", "url": io_url},
        {"repo": "access.redhat.com", "url": access_url},
    ]


def resolve_quay_digest_to_git_sha(digest: str, container_image: str) -> str | None:
    """Resolve an image digest to a git commit SHA via the Quay public API.

    Returns `None` when resolution fails (non-quay image, no matching tag, etc).
    """
    try:
        repo_url = container_image.split("@", 1)[0]
        if not repo_url.startswith("quay.io/"):
            print("Not a quay.io image, skipping digest resolution")
            return None
        repo_path = repo_url.removeprefix("quay.io/")
        page = 1
        while page <= _MAX_QUAY_TAG_PAGES:
            url = (
                f"https://quay.io/api/v1/repository/{repo_path}/tag/" f"?limit=100&page={page}"
            )
            try:
                body = http_client.get_text(url, timeout=10)
            except requests.HTTPError as exc:
                code = exc.response.status_code if exc.response is not None else "?"
                print(f"Quay API returned {code}, skipping digest resolution")
                return None
            data = json.loads(body)
            for tag in data.get("tags", []):
                name = tag.get("name", "")
                if tag.get("manifest_digest") == digest and _QUAY_SHA_TAG.fullmatch(name):
                    print(f"Resolved {digest[:19]}... to git SHA {name}")
                    return str(name)
            if not data.get("has_additional", False):
                break
            page += 1
        print(f"No git SHA tag found for digest {digest[:19]}...")
        return None
    except Exception as exc:
        print(f"Failed to resolve digest to git SHA: {exc}")
        return None


def convert_to_quay(repository: str) -> str:
    """Convert a Red Hat registry repository path to its quay.io temp-namespace form.

    The registry host prefix is swapped for the matching quay.io temp
    namespace, and remaining ``/`` path separators are dash-encoded
    (``----``) since the temp namespaces are single-segment repos.
    Repositories that don't match a known Red Hat registry prefix are
    returned unchanged.
    """
    for registry_prefix, quay_prefix in _REGISTRY_QUAY_PAIRS:
        prefix = registry_prefix + "/"
        if repository.startswith(prefix):
            rest = repository[len(prefix) :]
            return f"{quay_prefix}/{rest.replace('/', '----')}"
    return repository


def convert_to_registry(repository: str) -> str:
    """Convert a quay.io temp-namespace repository path to its registry.redhat.io form.

    This is the inverse of :func:`convert_to_quay`. Repositories already in
    ``registry.redhat.io`` or ``registry.stage.redhat.io`` format are
    returned unchanged; any other unrecognized format returns an empty
    string.
    """
    for registry_prefix, quay_prefix in _REGISTRY_QUAY_PAIRS:
        prefix = quay_prefix + "/"
        if repository.startswith(prefix):
            rest = repository[len(prefix) :]
            return f"{registry_prefix}/{rest.replace('----', '/')}"
    if repository.startswith("registry.redhat.io/") or repository.startswith(
        "registry.stage.redhat.io/"
    ):
        return repository
    return ""


def convert_to_registry_access(repository: str) -> str:
    """Convert a ``registry(.stage).redhat.io`` repository to its ``registry.access`` form.

    Only the two plain Red Hat registry prefixes are handled (not the
    ``flatpaks.*`` prefixes); anything else returns an empty string.
    """
    if repository.startswith("registry.redhat.io/"):
        return "registry.access.redhat.com" + repository[len("registry.redhat.io") :]
    if repository.startswith("registry.stage.redhat.io/"):
        return (
            "registry.access.stage.redhat.com" + repository[len("registry.stage.redhat.io") :]
        )
    return ""


def pyxis_url_for_pull_spec(pyxis_url: str, pull_spec: str) -> str:
    """Build the Pyxis repository/tag API URL for `pull_spec`.

    `registry.redhat.io` is rewritten to `registry.access.redhat.com` to
    match Pyxis lookups.
    """
    normalized = pull_spec.replace("registry.redhat.io", "registry.access.redhat.com", 1)
    parts = normalized.split("/", 2)
    if len(parts) < 3:
        raise ValueError(f"invalid pull spec: {pull_spec!r}")
    registry, repo, image_and_tag = parts
    image, sep, tag = image_and_tag.partition(":")
    base = (
        f"{pyxis_url.rstrip('/')}/repositories/registry/{registry}/repository/"
        f"{repo}/{image}"
    )
    if sep and tag:
        return f"{base}/tag/{tag}"
    return base
