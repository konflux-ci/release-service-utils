#!/usr/bin/env python3
"""Merge a ReleasePlanAdmission mapping into a Snapshot's components.

The mapping is expected to live under the ``mapping`` key of the data file
(the merged ReleasePlanAdmission data). If the data file is missing, or has
no ``mapping`` key, the Snapshot spec file is left untouched and the task
reports ``mapped=false``. Otherwise, the mapping's components are merged with
the Snapshot's components by name, tag templates are expanded, image
metadata (labels, annotations, env vars, media type) is attached to each
component, and repository URLs are translated between the quay.io and
registry.redhat.io namespaces.

Supported tag template variables:
* ``{{ timestamp }}`` -- the image's ``build-date`` label (or ``Created``
  fallback), formatted with the component's ``timestampFormat`` (or the
  mapping-level default, or ``%s``).
* ``{{ release_timestamp }}`` -- the current time, formatted the same way.
* ``{{ git_sha }}`` / ``{{ git_short_sha }}`` -- the git revision that
  triggered the Snapshot (and its first 7 characters).
* ``{{ digest_sha }}`` -- the image digest (without the ``sha256:`` prefix).
* ``{{ oci_version }}`` -- ``org.opencontainers.image.version`` from
  annotations (falling back to labels), with ``+`` replaced by ``_``.
* ``{{ incrementer }}`` -- the next sequential numeric tag in the repository.
* ``{{ component-incrementer }}`` -- like ``incrementer``, but computed
  uniformly across every repository the component is pushed to.
* ``{{ labels.mylabel }}`` -- the value of image label ``mylabel``.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import date_format
import file
import image_ref
import json_merge
import skopeo
import tekton
from authentication import setup_ca_cert
from logger import logger
from subprocess_cmd import run_cmd

PROG = "apply_mapping.py"

_CONTAINER_IMAGE_RE = re.compile(r".+@sha256:[0-9a-f]+")
_OCI_IMAGE_CONFIG_MEDIA_TYPE = "application/vnd.oci.image.config.v1+json"
_DOCKER_IMAGE_CONFIG_MEDIA_TYPE = "application/vnd.docker.container.image.v1+json"

# This is a temporary bridge to support both quay.io and registry.redhat.io
# repository URLs. It should be removed once all repositories are migrated
# to registry.redhat.io.
_QUAY_TEMP_PREFIXES = (
    "quay.io/redhat-prod/",
    "quay.io/redhat-pending/",
    "quay.io/rh-flatpaks-prod/",
    "quay.io/rh-flatpaks-stage/",
)
_REGISTRY_PREFIXES = (
    "registry.redhat.io/",
    "registry.stage.redhat.io/",
    "flatpaks.registry.redhat.io/",
    "flatpaks.registry.stage.redhat.io/",
)

_INCREMENTER_PLACEHOLDER = re.compile(r"\{\{\s*incrementer\s*\}\}")
_COMPONENT_INCREMENTER_PLACEHOLDER = re.compile(r"\{\{\s*component-incrementer\s*\}\}")
_VALID_TAG_RE = re.compile(r"[a-zA-Z0-9._-]+")
_VAR_REF_RE = re.compile(r"\{\{\s*([A-Za-z0-9_.-]+)\s*\}\}")

ListTagsFn = Callable[[str], list[str]]
InspectFn = Callable[..., Any]
GetArchFn = Callable[[str], list[dict]]
FormatDateFn = Callable[[str, str], str]
CurrentTimestampFn = Callable[[], str]


def _skopeo_list_repo_tags(repo: str) -> list[str]:
    """List a repository's tags via ``skopeo list-tags``."""
    result = skopeo.list_tags(repo)
    if result.returncode != 0:
        raise RuntimeError(
            f"skopeo list-tags failed for {repo}: {(result.stderr or '').strip()}"
        )
    return json.loads(result.stdout).get("Tags") or []


def _inspect_json(inspect_fn: InspectFn, ref: str, **kwargs: Any) -> dict:
    """Run ``inspect_fn`` and parse its stdout as a JSON object, raising on failure."""
    result = inspect_fn(ref, **kwargs)
    if result.returncode != 0:
        raise RuntimeError(f"skopeo inspect failed for {ref}: {(result.stderr or '').strip()}")
    return json.loads(result.stdout)


def _get_image_architectures(image: str, *, retry_times: int | None = None) -> list[dict]:
    """Return architecture/platform/digest info for ``image`` via ``get-image-architectures``.

    ``get-image-architectures`` inspects a container image (including OCI
    artifacts) and reports one JSON object per architecture, handling the
    distinct manifest media types (OCI artifact, single-arch OCI/Docker
    image, Docker manifest list) that ``skopeo inspect`` can return. Rather
    than duplicating that logic in Python, this shells out to the existing,
    well-exercised utility and parses its newline-delimited JSON output.

    Each returned dict has at least ``platform`` (with ``architecture`` and
    ``os``) and ``digest`` keys; multi-arch images yield one dict per
    manifest, while single-arch images and OCI artifacts yield exactly one.

    Raises:
        RuntimeError: if the underlying command fails.

    """
    cmd = ["get-image-architectures"]
    if retry_times is not None:
        cmd += ["--skopeo-retries", str(retry_times)]
    cmd.append(image)

    result = run_cmd(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"get-image-architectures failed for {image}: {result.stderr.strip()}"
        )
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def _git_revision_str(component: dict) -> str:
    """Return the component's ``source.git.revision``, or ``"null"`` if absent.

    Mirrors ``jq -r '.source.git.revision'``, which prints the literal text
    ``null`` when the path doesn't resolve.
    """
    source = component.get("source")
    if isinstance(source, dict):
        git = source.get("git")
        if isinstance(git, dict) and git.get("revision") is not None:
            return str(git["revision"])
    return "null"


def _digest_sha(container_image: str) -> str:
    """Return the hex digest portion of a ``...@sha256:<hex>`` image reference."""
    return container_image.rsplit(":", 1)[-1]


def _set_metadata_field(component: dict, key: str, value: Any) -> None:
    """Set ``component["metadata"][key]``, creating the ``metadata`` dict if needed."""
    metadata = component.get("metadata")
    if metadata is None:
        metadata = {}
        component["metadata"] = metadata
    metadata[key] = value


def _validate_tag(tag: str) -> None:
    """Raise ``ValueError`` if ``tag`` contains characters outside the allowed tag charset."""
    if not _VALID_TAG_RE.fullmatch(tag):
        raise ValueError(f"Invalid tag format: {tag}")


def _max_increment(tags: list[str], prefix: str) -> int:
    """Return the highest 1-6 digit numeric suffix among ``tags`` that start with ``prefix``.

    7+ digit suffixes are ignored, so short commit SHAs aren't mistaken for
    incrementer values.
    """
    pattern = re.compile(rf"^{re.escape(prefix)}([0-9]{{1,6}})$")
    best = 0
    for tag in tags:
        match = pattern.fullmatch(tag)
        if match:
            best = max(best, int(match.group(1), 10))
    return best


def increment_tag(tag_template: str, repo: str, list_tags_fn: ListTagsFn) -> str:
    """Resolve a single ``{{ incrementer }}`` placeholder against ``repo``'s existing tags."""
    version_prefix = _INCREMENTER_PLACEHOLDER.sub("", tag_template)
    existing_tags = list_tags_fn(repo)
    increment = _max_increment(existing_tags, version_prefix) + 1
    tag = _INCREMENTER_PLACEHOLDER.sub(str(increment), tag_template)
    _validate_tag(tag)
    return tag


def component_increment_tag(
    tag_template: str,
    all_repos: list[str],
    inc_cache: dict[str, int],
    list_tags_fn: ListTagsFn,
) -> str:
    """Resolve a ``{{ component-incrementer }}`` placeholder across all of a component's repos.

    Results are cached in ``inc_cache`` (keyed by the tag's version prefix)
    so repeated calls for the same component reuse one uniform value instead
    of re-querying every repository.
    """
    version_prefix = _COMPONENT_INCREMENTER_PLACEHOLDER.sub("", tag_template)
    if version_prefix in inc_cache:
        increment = inc_cache[version_prefix]
    else:
        global_max = 0
        for repo in all_repos:
            existing_tags = list_tags_fn(repo)
            global_max = max(global_max, _max_increment(existing_tags, version_prefix))
        increment = global_max + 1
        inc_cache[version_prefix] = increment
    tag = _COMPONENT_INCREMENTER_PLACEHOLDER.sub(str(increment), tag_template)
    _validate_tag(tag)
    return tag


def _substitute_value(var_name: str, substitute_map: dict[str, str], labels: dict) -> str:
    """Look up a tag template variable's replacement value.

    ``labels.<name>`` variables are read from the image's labels; anything
    else is read from ``substitute_map``. Returns ``""`` when unset.
    """
    if var_name.startswith("labels."):
        return str(labels.get(var_name[len("labels.") :]) or "")
    return str(substitute_map.get(var_name) or "")


def translate_one_tag(
    tag: str,
    substitute_map: dict[str, str],
    labels: dict,
    repo: str,
    all_repos: list[str],
    inc_cache: dict[str, int],
    list_tags_fn: ListTagsFn,
) -> str:
    """Repeatedly expand ``{{ variable }}`` references in ``tag`` until none remain."""
    while True:
        match = _VAR_REF_RE.search(tag)
        if not match:
            break
        var_name = match.group(1)
        if var_name == "incrementer":
            tag = increment_tag(tag, repo, list_tags_fn)
        elif var_name == "component-incrementer":
            tag = component_increment_tag(tag, all_repos, inc_cache, list_tags_fn)
        else:
            replacement = _substitute_value(var_name, substitute_map, labels)
            if not replacement:
                raise ValueError(f"Substitution variable unknown or empty: {var_name}")
            pattern = re.compile(r"\{\{\s*" + re.escape(var_name) + r"\s*\}\}")
            tag = pattern.sub(lambda _m, r=replacement: r, tag, count=1)
    _validate_tag(tag)
    return tag


def translate_tags(
    tags: list[str],
    substitute_map: dict[str, str],
    labels: dict,
    repo: str,
    all_repos: list[str],
    inc_cache: dict[str, int],
    list_tags_fn: ListTagsFn,
) -> list[str]:
    """Translate each tag template in ``tags``, dropping duplicates (first occurrence wins)."""
    translated: list[str] = []
    for tag in tags:
        result = translate_one_tag(
            tag, substitute_map, labels, repo, all_repos, inc_cache, list_tags_fn
        )
        if result not in translated:
            translated.append(result)
    return translated


def ensure_implicit_timestamp_value(
    tags: list[str], timestamp: str, add_implicit_timestamp_tag: bool
) -> list[str]:
    """Append ``timestamp`` to ``tags`` (deduplicated) when requested.

    Raises ``ValueError`` if requested but ``timestamp`` is empty.
    """
    if not add_implicit_timestamp_tag:
        return tags
    if not timestamp:
        raise ValueError(
            "addImplicitTimestampTag is true but timestamp is empty "
            "(no build-date or Created)."
        )
    return json_merge.unique_sorted(list(tags) + [timestamp])


def merge_components(original: list[dict], mapping: list[dict]) -> list[dict]:
    """Merge ``original`` (Snapshot) and ``mapping`` components by name.

    Only components present in both lists are kept, mirroring
    ``group_by(.name) | select(length > 1)``. Matching components are merged
    with :func:`json_merge.jq_multiply`, giving the mapping's values
    precedence. Results are ordered by component name.

    Raises:
        ValueError: if any component is missing a non-empty string ``name``.

    """
    groups: dict[str, list[dict]] = {}
    for component in list(original) + list(mapping):
        name = component.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError(f"Component is missing a valid 'name' field: {component!r}")
        groups.setdefault(name, []).append(component)

    merged: list[dict] = []
    for name in sorted(groups):
        group = groups[name]
        if len(group) < 2:
            continue
        result: dict = {}
        for item in group:
            result = json_merge.jq_multiply(result, item)
        merged.append(result)
    return merged


@dataclass(frozen=True)
class ImageManifestInfo:
    """Metadata pulled from a component's image manifest and (when supported) its config."""

    annotations: dict
    config_media_type: str
    labels: dict
    build_date: str
    env_variables: list
    oci_version_raw: str


def _extract_manifest_info(
    inspect_fn: InspectFn,
    container_image: str,
    image_with_digest: str,
    arch: str,
    os_name: str,
) -> ImageManifestInfo:
    """Inspect a component's image and collect the metadata needed for tagging/attachment.

    Raw manifest inspection works for every artifact type and gives us
    annotations and the config media type, which tells us whether standard
    ``skopeo inspect`` (for labels/env/build-date) will work: only standard
    container images (OCI or Docker) support it. Non-standard artifacts
    (Helm charts, ML models, etc.) fall back to ``build_date`` from
    annotations, if present.
    """
    raw_manifest = _inspect_json(inspect_fn, image_with_digest, no_tags=True, raw=True)
    annotations = raw_manifest.get("annotations") or {}
    config_media_type = (raw_manifest.get("config") or {}).get("mediaType") or ""

    if config_media_type in (_OCI_IMAGE_CONFIG_MEDIA_TYPE, _DOCKER_IMAGE_CONFIG_MEDIA_TYPE):
        image_metadata = _inspect_json(
            inspect_fn, container_image, no_tags=True, override_os=os_name, override_arch=arch
        )
        labels = image_metadata.get("Labels") or {}
        build_date = labels.get("build-date") or image_metadata.get("Created") or ""
        env_variables = image_metadata.get("Env") or []
    else:
        build_date = annotations.get("org.opencontainers.image.created") or ""
        env_variables = []
        labels = {}

    oci_version_raw = (
        annotations.get("org.opencontainers.image.version")
        or labels.get("org.opencontainers.image.version")
        or ""
    )

    return ImageManifestInfo(
        annotations=annotations,
        config_media_type=config_media_type,
        labels=labels,
        build_date=build_date,
        env_variables=env_variables,
        oci_version_raw=oci_version_raw,
    )


def process_component(
    component: dict,
    *,
    default_tags: list[str],
    default_timestamp_format: str,
    current_timestamp: str,
    default_cgw_settings: dict,
    add_implicit_timestamp_tag: bool,
    inspect_fn: InspectFn = skopeo.inspect,
    list_tags_fn: ListTagsFn = _skopeo_list_repo_tags,
    get_arch_fn: GetArchFn = _get_image_architectures,
    format_date_fn: FormatDateFn = date_format.format_date,
) -> None:
    """Mutate ``component`` in place: attach metadata, expand tags, translate registry URLs."""
    name = component.get("name")
    container_image = component.get("containerImage") or ""
    if not _CONTAINER_IMAGE_RE.fullmatch(container_image):
        raise ValueError(
            f"Component {name} contains an invalid containerImage value. "
            f"sha reference is required: {container_image}"
        )

    git_sha = _git_revision_str(component)
    build_sha = _digest_sha(container_image)
    passed_timestamp_format = component.get("timestampFormat") or default_timestamp_format
    release_timestamp = format_date_fn(current_timestamp, passed_timestamp_format)

    # The build-date label and Created values are not the same per architecture, but we
    # don't support separate tags per arch, so we just use the first digest listed.
    arch_infos = get_arch_fn(container_image)
    if not arch_infos:
        raise RuntimeError(
            f"No architectures were discovered for component {name} ({container_image})"
        )
    arch = arch_infos[0]["platform"]["architecture"]
    os_name = arch_infos[0]["platform"]["os"]
    first_digest = arch_infos[0]["digest"]
    image_with_digest = f"{container_image.rsplit('@', 1)[0]}@{first_digest}"

    manifest_info = _extract_manifest_info(
        inspect_fn, container_image, image_with_digest, arch, os_name
    )
    labels = manifest_info.labels

    if manifest_info.env_variables:
        _set_metadata_field(component, "env_variables", manifest_info.env_variables)
    if manifest_info.annotations:
        _set_metadata_field(
            component,
            "annotations",
            [{"name": k, "value": v} for k, v in manifest_info.annotations.items()],
        )
    if labels:
        _set_metadata_field(
            component, "labels", [{"name": k, "value": v} for k, v in labels.items()]
        )
    if manifest_info.config_media_type:
        _set_metadata_field(component, "media_type", manifest_info.config_media_type)

    # Transform version to OCI tag format: replace + with _ (OCI compliance), and
    # default to "unknown" for regular images without OCI version annotations/labels.
    oci_version = (manifest_info.oci_version_raw or "").replace("+", "_") or "unknown"
    timestamp = (
        ""
        if not manifest_info.build_date
        else format_date_fn(manifest_info.build_date, passed_timestamp_format)
    )

    substitute_map = {
        "timestamp": timestamp,
        "release_timestamp": release_timestamp,
        "git_sha": git_sha,
        "git_short_sha": git_sha[:7],
        "digest_sha": build_sha,
        "oci_version": oci_version,
    }

    # Cache for {{ component-incrementer }} results, shared by staged files and
    # repositories below, cleared per-component so different components with the same
    # tag template query independent repo sets.
    inc_cache: dict[str, int] = {}

    for staged_file in (component.get("staged") or {}).get("files") or []:
        # {{ incrementer }} is not supported in staged.files values, so we pass "" as
        # the repo argument and an empty repo list for {{ component-incrementer }}.
        staged_file["filename"] = translate_one_tag(
            staged_file.get("filename") or "",
            substitute_map,
            labels,
            "",
            [],
            inc_cache,
            list_tags_fn,
        )

    component_cgw = json_merge.merge_deep_union_arrays(
        default_cgw_settings, component.get("contentGateway") or {}
    )
    if component_cgw:
        component["contentGateway"] = component_cgw

    # Used by {{ component-incrementer }} to query all repos and compute a uniform
    # increment value across registries.
    all_repos = [
        repository.get("url") or "" for repository in component.get("repositories") or []
    ]
    default_component_tags = json_merge.unique_sorted(
        list(default_tags) + list(component.get("componentTags") or [])
    )

    for repository in component.get("repositories") or []:
        url = repository.get("url") or ""
        repo_tags = repository.get("tags") or []
        tags_pre_substitution = json_merge.unique_sorted(
            list(default_component_tags) + list(repo_tags)
        )
        tags = translate_tags(
            tags_pre_substitution,
            substitute_map,
            labels,
            url,
            all_repos,
            inc_cache,
            list_tags_fn,
        )
        tags = ensure_implicit_timestamp_value(tags, timestamp, add_implicit_timestamp_tag)
        if tags:
            repository["tags"] = tags

        # This block is temporary to support both quay.io and registry.redhat.io.
        # It should be removed once all repositories are migrated to registry.redhat.io.
        if url.startswith(_QUAY_TEMP_PREFIXES):
            url = image_ref.convert_to_registry(url)

        if url.startswith(_REGISTRY_PREFIXES):
            repository["rh-registry-repo"] = url
            repository["registry-access-repo"] = image_ref.convert_to_registry_access(url)
            repository["url"] = image_ref.convert_to_quay(url)


def process_components(
    snapshot: dict,
    mapping: dict,
    add_implicit_timestamp_tag: bool,
    *,
    inspect_fn: InspectFn = skopeo.inspect,
    list_tags_fn: ListTagsFn = _skopeo_list_repo_tags,
    get_arch_fn: GetArchFn = _get_image_architectures,
    format_date_fn: FormatDateFn = date_format.format_date,
    current_timestamp_fn: CurrentTimestampFn = date_format.current_timestamp,
) -> None:
    """Process every component in ``snapshot``, applying mapping defaults from ``mapping``."""
    defaults = mapping.get("defaults") or {}
    default_tags = defaults.get("tags") or []
    default_timestamp_format = defaults.get("timestampFormat") or "%s"
    default_cgw_settings = defaults.get("contentGateway") or {}
    current_timestamp = current_timestamp_fn()

    for component in snapshot.get("components") or []:
        process_component(
            component,
            default_tags=default_tags,
            default_timestamp_format=default_timestamp_format,
            current_timestamp=current_timestamp,
            default_cgw_settings=default_cgw_settings,
            add_implicit_timestamp_tag=add_implicit_timestamp_tag,
            inspect_fn=inspect_fn,
            list_tags_fn=list_tags_fn,
            get_arch_fn=get_arch_fn,
            format_date_fn=format_date_fn,
        )


def apply_mapping(
    snapshot_path: Path,
    data_path: Path | None,
    *,
    fail_on_empty_result: bool = False,
    add_implicit_timestamp_tag: bool = False,
    inspect_fn: InspectFn = skopeo.inspect,
    list_tags_fn: ListTagsFn = _skopeo_list_repo_tags,
    get_arch_fn: GetArchFn = _get_image_architectures,
    format_date_fn: FormatDateFn = date_format.format_date,
    current_timestamp_fn: CurrentTimestampFn = date_format.current_timestamp,
) -> bool:
    """Apply a ReleasePlanAdmission mapping to a Snapshot spec file, in place.

    A ``.orig`` backup of the original Snapshot file is always created next
    to ``snapshot_path``. Returns ``True`` when a mapping was found and
    merged in (and ``snapshot_path`` was rewritten), ``False`` otherwise (in
    which case ``snapshot_path`` is left untouched).

    Raises:
        FileNotFoundError: if ``snapshot_path`` doesn't exist.
        ValueError: on malformed component data, or an empty merge result
            when ``fail_on_empty_result`` is set.

    """
    orig_path = Path(f"{snapshot_path}.orig")
    shutil.copyfile(snapshot_path, orig_path)

    if data_path is None or not data_path.is_file():
        logger.info("No data JSON file was found.")
        return False

    data = file.load_json_dict(data_path)
    mapping = data.get("mapping")
    if mapping is None:
        logger.info("Data file contains no mapping key.")
        return False

    snapshot = file.load_json_dict(snapshot_path)
    original_components = snapshot.get("components") or []
    mapping_components = mapping.get("components") or []
    merged_components = merge_components(original_components, mapping_components)
    snapshot["components"] = merged_components

    if fail_on_empty_result and not merged_components:
        raise ValueError(
            "Resulting snapshot contains 0 components. This means that there were 0 "
            "components present in both your Snapshot and your ReleasePlanAdmission "
            "mapping. Take a look at your component names and make sure that all "
            "components you want to release from the snapshot are present in the "
            "ReleasePlanAdmission (by the name field of the component). "
            f"Components in snapshot: {[c.get('name') for c in original_components]} "
            f"Components in mapping: {[c.get('name') for c in mapping_components]}"
        )

    process_components(
        snapshot,
        mapping,
        add_implicit_timestamp_tag,
        inspect_fn=inspect_fn,
        list_tags_fn=list_tags_fn,
        get_arch_fn=get_arch_fn,
        format_date_fn=format_date_fn,
        current_timestamp_fn=current_timestamp_fn,
    )

    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    return True


def _str_to_bool(value: str) -> bool:
    return value.strip().lower() == "true"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, prog=PROG)
    parser.add_argument(
        "--snapshot-file",
        required=True,
        help="Path to the Snapshot spec JSON file",
    )
    parser.add_argument(
        "--data-file",
        required=True,
        help="Path to the merged data JSON file",
    )
    parser.add_argument(
        "--fail-on-empty-result",
        type=_str_to_bool,
        default=False,
        help="Fail the task if the resulting snapshot contains 0 components",
    )
    parser.add_argument(
        "--add-implicit-timestamp-tag",
        type=_str_to_bool,
        default=False,
        help="Append the resolved {{ timestamp }} value to each repository's tag list",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments, resolve Tekton result paths, and run the mapping."""
    setup_ca_cert()
    args = _parse_args(argv)
    (result_mapped_path,) = tekton.result_paths_from_env("RESULT_MAPPED")

    mapped = apply_mapping(
        Path(args.snapshot_file),
        Path(args.data_file),
        fail_on_empty_result=args.fail_on_empty_result,
        add_implicit_timestamp_tag=args.add_implicit_timestamp_tag,
    )
    result_mapped_path.write_text("true" if mapped else "false", encoding="utf-8")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
