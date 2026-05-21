"""Decode advisory task payloads and content filtering (idempotency rules)."""

from __future__ import annotations

import base64
import gzip
import json
import re
from pathlib import Path
from typing import Any

import yaml


def _strip_checksum_from_purl(purl: str) -> str:
    """Strip `checksum` query/fragment parts from a package URL for comparison."""
    # purl checksum may appear as `&checksum=` (extra param), `?checksum=` in
    # the query, or trailing `?checksum=` before end — strip all three shapes.
    stripped = re.sub(r"&checksum=[^&]*", "", purl)
    stripped = re.sub(r"\?checksum=[^&]*&", "?", stripped)
    stripped = re.sub(r"\?checksum=[^&]*$", "", stripped)
    return stripped


def _filter_image(content: list[Any], existing: list[Any]) -> list[Any]:
    """Drop image rows that already exist (same image, tags, repository)."""
    # `tags` is compared as JSON lists (order and length must match).
    out: list[Any] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        container_image = item.get("containerImage")
        tags = item.get("tags")
        repo = item.get("repository")
        is_duplicate = False
        for existing_row in existing:
            if not isinstance(existing_row, dict):
                continue
            if (
                existing_row.get("containerImage") == container_image
                and existing_row.get("tags") == tags
                and existing_row.get("repository") == repo
            ):
                is_duplicate = True
                break
        if not is_duplicate:
            out.append(item)
    return out


def _filter_rpm(content: list[Any], existing: list[Any]) -> list[Any]:
    """Drop artifact rows whose `purl` exactly matches an existing row."""
    existing_purls = {
        existing_row.get("purl")
        for existing_row in existing
        if isinstance(existing_row, dict) and existing_row.get("purl") is not None
    }
    out: list[Any] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        purl = item.get("purl")
        # Without `purl` there is nothing to compare; skip the row.
        if purl is None or purl not in existing_purls:
            out.append(item)
    return out


def _filter_generic_binary(content: list[Any], existing: list[Any]) -> list[Any]:
    """Drop rows whose `purl` matches existing after stripping `checksum`."""
    # Re-signing can change checksum query params; compare logical purls only.
    stripped_existing = {
        _strip_checksum_from_purl(str(existing_row["purl"]))
        for existing_row in existing
        if isinstance(existing_row, dict) and existing_row.get("purl") is not None
    }
    out: list[Any] = []
    for item in content:
        if not isinstance(item, dict) or item.get("purl") is None:
            continue
        if _strip_checksum_from_purl(str(item["purl"])) not in stripped_existing:
            out.append(item)
    return out


def decode_advisory_param(advisory_b64gzip: str) -> dict[str, Any]:
    """Decode `ADVISORY_JSON` (base64 + gzip) to a dict."""
    # Task param is a single string; pipeline supplies gzip then base64.
    b64_decoded = base64.standard_b64decode(advisory_b64gzip.strip())
    gzip_decoded = gzip.decompress(b64_decoded)
    return json.loads(gzip_decoded.decode("utf-8"))


def content_array_from_decoded(root: dict[str, Any], content_list_path: str) -> list[Any]:
    """
    Return the list at *content_list_path* under advisory *root*, or `[]` if
    missing or not a list.

    *content_list_path* is a dotted path with an optional leading dot, for
    example `.content.images` or `.content.artifacts`.

    Each segment is the next dict key under *root*; the value stepped through
    along the path must stay a dict until the final key, which must hold a list.
    """
    # Decoded advisory JSON stores `content` at the top level (not under `spec`).
    segments = [s for s in content_list_path.strip(".").split(".") if s]
    current_value: Any = root
    for segment in segments:
        if not isinstance(current_value, dict):
            return []
        current_value = current_value.get(segment)
    if current_value is None:
        return []
    return current_value if isinstance(current_value, list) else []


def set_decoded_content_array(
    root: dict[str, Any],
    content_list_path: str,
    content_rows: list[Any],
) -> None:
    """Set `root['content'][images|artifacts]` from *content_list_path*."""
    # Only `.content.images` and `.content.artifacts` are valid paths here.
    segments = [s for s in content_list_path.strip(".").split(".") if s]
    if len(segments) != 2 or segments[0] != "content":
        msg = f"unsupported content list path for merge: {content_list_path!r}"
        raise ValueError(msg)
    images_or_artifacts_key = segments[1]
    if "content" not in root or not isinstance(root["content"], dict):
        root["content"] = {}
    root["content"][images_or_artifacts_key] = content_rows


def append_signing_key_to_content(
    root: dict[str, Any],
    content_list_path: str,
    signing_key: str,
) -> None:
    """Add `signingKey` to each element of the content array at *content_list_path*."""
    # Mutates *root* in place.
    for item in content_array_from_decoded(root, content_list_path):
        if isinstance(item, dict):
            item["signingKey"] = signing_key


def load_advisory_yaml(path: Path) -> dict[str, Any]:
    """Load `advisory.yaml` (or any YAML document) as a `dict`."""
    yaml_source = path.read_text(encoding="utf-8")
    data = yaml.safe_load(yaml_source)
    if data is None:
        return {}
    if not isinstance(data, dict):
        msg = f"YAML root must be a mapping: {path}"
        raise TypeError(msg)
    return data


def spec_content_array_from_advisory_yaml(
    doc: dict[str, Any],
    content_list_path: str,
) -> list[Any]:
    """
    Return the list at *content_list_path* under `doc['spec']` from an advisory
    YAML document (`metadata` / `spec` layout).

    Use the same dotted path as for decoded JSON (e.g. `.content.images`); it
    is applied under `spec`, not at the document root.

    Walking starts at `doc['spec']`; each segment is the next dict key, same
    rules as `content_array_from_decoded`.
    """
    # Repo advisories nest the payload under `spec` (alongside `metadata`).
    segments = [s for s in content_list_path.strip(".").split(".") if s]
    current_value: Any = doc.get("spec")
    if not isinstance(current_value, dict):
        return []
    for segment in segments:
        if not isinstance(current_value, dict):
            return []
        current_value = current_value.get(segment)
    if current_value is None:
        return []
    return current_value if isinstance(current_value, list) else []


def get_advisory_spec_type(doc: dict[str, Any]) -> str:
    """Return `spec.type` from an advisory YAML document."""
    spec = doc.get("spec")
    if isinstance(spec, dict) and spec.get("type") is not None:
        return str(spec["type"])
    return ""


def get_advisory_metadata_name(doc: dict[str, Any]) -> str:
    """Return `metadata.name` from an advisory YAML document."""
    metadata = doc.get("metadata")
    if isinstance(metadata, dict) and metadata.get("name") is not None:
        return str(metadata["name"])
    return ""


def template_data_for_apply(keyed_advisory: dict[str, Any]) -> dict[str, Any]:
    """Build the `{"advisory": {"spec": ...}}` object for `apply_template`."""
    return {"advisory": {"spec": keyed_advisory}}


def template_context_merge(
    tmpl_data: dict[str, Any],
    advisory_name: str,
    ship_date: str,
) -> dict[str, Any]:
    """
    Merge *advisory_name* and *ship_date* into *tmpl_data* for Jinja.

    Duplicate top-level keys keep the value already in *tmpl_data* (later dict
    in the merge wins).
    """
    # `{**a, **b}`: keys from *b* overwrite duplicates from *a*.
    return {
        **{"advisory_name": advisory_name, "advisory_ship_date": ship_date},
        **tmpl_data,
    }


def json_dict_to_yaml_text(document: Any) -> str:
    """Serialize *document* to multi-line YAML (readable advisory file)."""
    # `sort_keys=False` keeps stable-ish ordering for tag-preservation checks.
    return yaml.safe_dump(
        document,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )


def spec_content_json_pointer(content_type: str) -> str:
    """Return the dotted *content_list_path* for *content_type* (under `spec` in YAML)."""
    if content_type == "image":
        return ".content.images"
    if content_type in ("binary", "generic", "rpm"):
        return ".content.artifacts"
    msg = f"Unsupported contentType: {content_type}"
    raise ValueError(msg)


def advisory_url_prefix(git_repo: str) -> str:
    """Return the customer portal errata URL for *git_repo*."""
    if "/rhtap-release/" in git_repo:
        return "https://access.stage.redhat.com/errata"
    return "https://access.redhat.com/errata"


def list_existing_advisory_subdirs(advisory_base: Path) -> list[str]:
    """List `year/num` paths under *advisory_base*, newest leaf mtime first."""
    if not advisory_base.is_dir():
        return []
    pairs: list[tuple[float, str]] = []
    for year_dir in advisory_base.iterdir():
        if not year_dir.is_dir():
            continue
        for num_dir in year_dir.iterdir():
            if not num_dir.is_dir():
                continue
            rel = f"{year_dir.name}/{num_dir.name}"
            # Sort by leaf dir mtime so `year/num` matches `find … -printf %T@`.
            pairs.append((num_dir.stat().st_mtime, rel))
    pairs.sort(key=lambda mtime_and_relpath: -mtime_and_relpath[0])
    return [relpath for _mtime, relpath in pairs]


def filter_content_by_existing(
    content_type: str,
    content_file: Path,
    existing_file: Path,
    *,
    stderr_path: Path | None,
) -> str:
    """
    Return compact JSON array text: *content_file* rows not already in
    *existing_file* (idempotency rules for image / rpm / generic / binary).
    """
    try:
        content_rows = json.loads(content_file.read_text(encoding="utf-8"))
        existing_rows = json.loads(existing_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        if stderr_path is not None:
            with open(
                stderr_path,
                "a",
                encoding="utf-8",
                errors="replace",
            ) as errf:
                errf.write(f"\nfilter_content_by_existing: invalid JSON: {exc}\n")
        raise
    if not isinstance(content_rows, list) or not isinstance(existing_rows, list):
        msg = "content and existing JSON must be arrays"
        raise TypeError(msg)

    if content_type in ("generic", "binary"):
        filtered = _filter_generic_binary(content_rows, existing_rows)
    elif content_type == "rpm":
        filtered = _filter_rpm(content_rows, existing_rows)
    else:
        filtered = _filter_image(content_rows, existing_rows)

    # Compact JSON (no extra spaces) for small temp files in the idempotency loop.
    return json.dumps(filtered, separators=(",", ":"))
