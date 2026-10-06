#!/usr/bin/env python3
"""Plan, apply, and roll back verified RPM epoch corrections in advisory checkouts."""

from __future__ import annotations

import argparse
import bz2
import copy
import gzip
import hashlib
import io
import json
import logging
import lzma
import os
import re
import shutil
import tempfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import yaml
from packageurl import PackageURL

from release_service_utils.helpers import advisory_data

LOG = logging.getLogger(__name__)
REPO_NS = "{http://linux.duke.edu/metadata/repo}"
RPM_NS = "{http://linux.duke.edu/metadata/common}"
BLOCKED = frozenset({"unresolved", "ambiguous", "conflict", "invalid"})


class MigrationError(ValueError):
    """Reject an unverifiable migration or a stale checkout."""


class _Loader(getattr(yaml, "CSafeLoader", yaml.SafeLoader)):
    pass


def _mapping(loader: _Loader, node: yaml.MappingNode) -> dict[Any, Any]:
    keys = [loader.construct_object(key) for key, _ in node.value]
    if len(keys) != len(set(keys)):
        raise MigrationError("duplicate YAML keys are unsupported")
    return loader.construct_mapping(node)


_Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _within(root: Path, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise MigrationError(f"unsafe relative path: {relative}")
    result = root / path
    if not result.resolve().is_relative_to(root.resolve()):
        raise MigrationError(f"path escapes checkout: {relative}")
    for ancestor in [result, *result.parents]:
        if ancestor == root.parent:
            break
        if ancestor.is_symlink():
            raise MigrationError(f"symlink is unsupported: {relative}")
    return result


def load_repositories(config: Path) -> tuple[dict[tuple[str, ...], list[dict]], list[dict]]:
    """Index checksum-verified primary metadata by repository, name, version, release, arch."""
    index: dict[tuple[str, ...], list[dict]] = defaultdict(list)
    evidence = []
    seen = set()
    for item in _json(config):
        repo = item["repository_id"]
        if not repo:
            raise MigrationError("empty repository ID")
        repomd = (config.parent / item["repomd"]).read_bytes()
        primary_path = config.parent / item["primary"]
        primary = primary_path.read_bytes()
        snapshot = (repo, _sha256(primary))
        if snapshot in seen:
            raise MigrationError(f"duplicate repository snapshot: {repo}")
        seen.add(snapshot)
        doc = ET.fromstring(repomd)
        entries = doc.findall(f"{REPO_NS}data[@type='primary']")
        if len(entries) != 1:
            raise MigrationError(f"expected one primary entry for {repo}")
        entry = entries[0]
        location = entry.find(f"{REPO_NS}location")
        if location is None or not location.get("href"):
            raise MigrationError(f"primary location is required for {repo}")
        primary_location = location.get("href")
        checksum = entry.find(f"{REPO_NS}checksum")
        if checksum is None or checksum.get("type") != "sha256":
            raise MigrationError(f"primary SHA256 is required for {repo}")
        if _sha256(primary) != checksum.text:
            raise MigrationError(f"primary checksum mismatch for {repo}")
        # Parse the same bytes that were hashed, even if the input file changes.
        raw = io.BytesIO(primary)
        if primary.startswith(b"\x1f\x8b"):
            stream = gzip.GzipFile(fileobj=raw, mode="rb")
        elif primary.startswith(b"BZh"):
            stream = bz2.BZ2File(raw, "rb")
        elif primary.startswith(b"\xfd7zXZ"):
            stream = lzma.LZMAFile(raw, "rb")
        else:
            stream = raw
        count = 0
        with stream:
            for _, package in ET.iterparse(stream, events=("end",)):
                if package.tag != f"{RPM_NS}package":
                    continue
                version = package.find(f"{RPM_NS}version")
                checksum = package.find(f"{RPM_NS}checksum")
                location = package.find(f"{RPM_NS}location")
                name = package.findtext(f"{RPM_NS}name")
                arch = package.findtext(f"{RPM_NS}arch")
                if version is None or not name or not arch:
                    raise MigrationError(f"incomplete primary package in {repo}")
                epoch = version.get("epoch")
                if epoch is None or not re.fullmatch(r"[0-9]+", epoch):
                    raise MigrationError(f"unverified epoch for {name} in {repo}")
                ver, rel = version.get("ver"), version.get("rel")
                if not ver or not rel:
                    raise MigrationError(f"incomplete version for {name} in {repo}")
                key = (repo, name, ver, rel, arch)
                index[key].append(
                    {
                        "epoch": int(epoch),
                        "checksum_type": (
                            checksum.get("type") if checksum is not None else None
                        ),
                        "checksum": checksum.text if checksum is not None else None,
                        "location": location.get("href") if location is not None else None,
                    }
                )
                count += 1
                package.clear()
        if not count:
            raise MigrationError(f"no RPM packages in primary metadata for {repo}")
        evidence.append(
            {
                "repository_id": repo,
                "source": item["source"],
                "repomd_sha256": _sha256(repomd),
                "primary_sha256": _sha256(primary),
                "primary_location": primary_location,
                "packages": count,
            }
        )
    if not evidence:
        raise MigrationError("at least one verified repository is required")
    return index, evidence


def _resolve(row: dict, index: dict[tuple[str, ...], list[dict]]) -> dict:
    purl = row["purl"]
    parsed = PackageURL.from_string(purl)
    # PackageURL normalizes qualifiers; inspect the raw query for duplicate/empty keys.
    pairs = parse_qsl(purl.partition("?")[2], keep_blank_values=True)
    if len(pairs) != len({key.lower() for key, _ in pairs}) or parsed.subpath:
        raise MigrationError("duplicate qualifiers or subpaths are unsupported")
    qualifiers = parsed.qualifiers
    if not qualifiers.get("arch") or not qualifiers.get("repository_id"):
        raise MigrationError("arch and repository_id qualifiers are required")
    if not parsed.version or "-" not in parsed.version:
        raise MigrationError("RPM version-release is required")
    version, release = parsed.version.rsplit("-", 1)
    key = (qualifiers["repository_id"], parsed.name, version, release, qualifiers["arch"])
    candidates = index.get(key, [])
    digests = [value for value in (qualifiers.get("checksum"), row.get("checksum")) if value]
    if row.get("sha256"):
        if not isinstance(row["sha256"], str):
            raise MigrationError("artifact sha256 must be a string")
        digests.append("sha256:" + row["sha256"])
    for digest in digests:
        if isinstance(digest, dict) and set(digest) == {"sha256"}:
            if not isinstance(digest["sha256"], str):
                raise MigrationError("artifact sha256 must be a string")
            digest = "sha256:" + digest["sha256"]
        if not isinstance(digest, str):
            raise MigrationError("unsupported artifact checksum representation")
        match = re.fullmatch(r"sha256:([0-9a-fA-F]{64})", digest)
        if not match:
            raise MigrationError("only a single sha256 checksum qualifier is supported")
        candidates = [
            c
            for c in candidates
            if c["checksum_type"] == "sha256" and c["checksum"] == match[1].lower()
        ]
    epochs = {c["epoch"] for c in candidates}
    if not epochs:
        return {
            "status": "unresolved",
            "reason": "no matching published RPM identity/checksum",
        }
    if len(epochs) != 1:
        return {
            "status": "ambiguous",
            "reason": "multiple published epochs",
            "evidence": candidates,
        }
    epoch = epochs.pop()
    if "epoch" in qualifiers:
        if not re.fullmatch(r"[0-9]+", qualifiers["epoch"]):
            raise MigrationError("epoch must be a nonnegative integer")
        if int(qualifiers["epoch"]) != epoch:
            return {
                "status": "conflict",
                "reason": "explicit epoch differs from metadata",
                "evidence": candidates,
            }
        if qualifiers["epoch"] != str(epoch) or epoch == 0:
            raise MigrationError(
                "explicit epoch is not canonical for the corrected producer; review manually"
            )
        return {"status": "already-correct", "epoch": epoch, "evidence": candidates}
    if epoch == 0:
        return {"status": "verified-zero", "epoch": 0, "evidence": candidates}
    # Match the producer's arch/epoch order, preserving every other raw qualifier.
    return {
        "status": "change",
        "epoch": epoch,
        "after": _insert_epoch(purl, epoch),
        "evidence": candidates,
    }


def _insert_epoch(purl: str, epoch: int) -> str:
    head, _, query = purl.partition("?")
    raw = query.split("&")
    arch_index = next(i for i, part in enumerate(raw) if part.split("=", 1)[0] == "arch")
    raw.insert(arch_index + 1, f"epoch={epoch}")
    return head + "?" + "&".join(raw)


def _child(node: yaml.MappingNode, key: str) -> yaml.Node:
    return next(value for name, value in node.value if name.value == key)


def _patch(data: bytes, changes: list[dict]) -> bytes:
    text = data.decode("utf-8")
    document = yaml.load(text, Loader=_Loader)
    expected = copy.deepcopy(document)
    root = yaml.compose(text, Loader=_Loader)
    nodes = _child(_child(_child(root, "spec"), "content"), "artifacts").value
    patches = []
    for change in changes:
        row_index = change["artifact_index"]
        row = expected["spec"]["content"]["artifacts"][row_index]
        if row["purl"] != change["before"]:
            raise MigrationError("PURL changed since planning")
        before = PackageURL.from_string(change["before"])
        epoch = change["epoch"]
        if (
            type(epoch) is not int
            or epoch <= 0
            or before.type != "rpm"
            or "epoch" in before.qualifiers
        ):
            raise MigrationError("planned epoch is inconsistent")
        evidence = change.get("evidence", [])
        if not evidence or any(candidate.get("epoch") != epoch for candidate in evidence):
            raise MigrationError("planned epoch lacks consistent metadata evidence")
        if change["after"] != _insert_epoch(change["before"], epoch):
            raise MigrationError("plan changes unrelated identity fields or qualifiers")
        node = _child(nodes[row_index], "purl")
        if node.style in ("|", ">"):
            raise MigrationError("block PURL scalars are unsupported")
        replacement = change["after"]
        if node.style == '"':
            replacement = json.dumps(replacement, ensure_ascii=False)
        elif node.style == "'":
            replacement = "'" + replacement.replace("'", "''") + "'"
        patches.append((node.start_mark.index, node.end_mark.index, replacement))
        row["purl"] = change["after"]
    last_start = len(text) + 1
    for start, end, replacement in sorted(patches, reverse=True):
        if end > last_start:
            raise MigrationError("aliased PURL scalars overlap")
        text = text[:start] + replacement + text[end:]
        last_start = start
    if yaml.load(text, Loader=_Loader) != expected:
        raise MigrationError("scalar edits changed unrelated advisory content")
    return text.encode("utf-8")


def plan(root: Path, config: Path, scope: list[str] | None = None) -> dict:
    """Inventory advisories and plan verified nonzero epoch edits without writing them."""
    root = root.absolute()
    index, repositories = load_repositories(config)
    if scope and set(scope) - {r["repository_id"] for r in repositories}:
        raise MigrationError("every scoped repository needs verified metadata")
    documents = []
    counts: Counter[str] = Counter()
    paths = sorted(root.rglob("advisory.yaml"))
    if not paths:
        raise MigrationError("no advisory.yaml files found")
    for path in paths:
        relative = path.relative_to(root).as_posix()
        data = _within(root, relative).read_bytes()
        file = {
            "path": relative,
            "before_sha256": _sha256(data),
            "after_sha256": _sha256(data),
            "changes": [],
            "entries": [],
        }
        documents.append(file)
        try:
            document = yaml.load(data, Loader=_Loader)
            if not isinstance(document, dict):
                raise MigrationError("YAML root must be a mapping")
        except (yaml.YAMLError, MigrationError, UnicodeError) as exc:
            file["error"] = str(exc)
            counts["invalid"] += 1
            continue
        rows = advisory_data.spec_content_array_from_advisory_yaml(
            document, ".content.artifacts"
        )
        for i, row in enumerate(rows):
            if not isinstance(row, dict) or not str(row.get("purl", "")).startswith(
                "pkg:rpm/"
            ):
                continue
            entry = {"artifact_index": i, "before": row["purl"]}
            try:
                if (
                    scope
                    and PackageURL.from_string(row["purl"]).qualifiers.get("repository_id")
                    not in scope
                ):
                    result = {"status": "out-of-scope"}
                else:
                    result = _resolve(row, index)
            except (ValueError, StopIteration) as exc:
                result = {"status": "invalid", "reason": str(exc)}
            entry.update(result)
            file["entries"].append(entry)
            counts[entry["status"]] += 1
            if entry["status"] == "change":
                file["changes"].append(entry)
        try:
            patched = _patch(data, file["changes"]) if file["changes"] else data
        except (MigrationError, StopIteration, yaml.YAMLError, UnicodeError) as exc:
            file["error"] = str(exc)
            counts["invalid"] += 1
            patched = data
        file["after_sha256"] = _sha256(patched)
    return {
        "format": "rpm-advisory-epoch-migration-v1",
        "repository_scope": scope,
        "repositories": repositories,
        "summary": {
            "advisories": len(documents),
            "changed_advisories": sum(bool(f["changes"]) for f in documents),
            "rpm_entries": sum(len(f["entries"]) for f in documents),
            "statuses": dict(sorted(counts.items())),
            "blocked": sum(counts[s] for s in BLOCKED),
        },
        "files": documents,
    }


def _atomic_write(path: Path, data: bytes) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            shutil.copymode(path, temporary)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def apply(root: Path, report: Path, backup: Path) -> None:
    """Apply a clean reviewed plan after checking the complete inventory and saving backups."""
    root = root.absolute()
    audit = _json(report)
    if audit.get("format") != "rpm-advisory-epoch-migration-v1":
        raise MigrationError("unsupported report format")
    if audit["summary"]["blocked"] or any(f.get("error") for f in audit["files"]):
        raise MigrationError("resolve every blocked entry before applying this plan")
    if any(e["status"] in BLOCKED for f in audit["files"] for e in f["entries"]):
        raise MigrationError("report contains blocked entries")
    actual = {p.relative_to(root).as_posix() for p in root.rglob("advisory.yaml")}
    if actual != {f["path"] for f in audit["files"]} or len(actual) != len(audit["files"]):
        raise MigrationError("advisory inventory changed since planning")
    staged = []
    all_after = True
    for file in audit["files"]:
        path = _within(root, file["path"])
        before = path.read_bytes()
        digest = _sha256(before)
        expected_after = file.get("after_sha256", file["before_sha256"])
        if digest != expected_after:
            all_after = False
        if digest not in (file["before_sha256"], expected_after):
            raise MigrationError(f"stale advisory: {file['path']}")
        if file["changes"] and digest == file["before_sha256"]:
            after = _patch(before, file["changes"])
            if _sha256(after) != expected_after:
                raise MigrationError(f"planned output hash mismatch: {file['path']}")
            staged.append((file, path, before, after))
    if all_after:
        LOG.info("Plan already applied; no files changed")
        return
    if len(staged) != sum(bool(f["changes"]) for f in audit["files"]):
        raise MigrationError("partially applied plan: use its original backup to roll back")
    if backup.resolve().is_relative_to(root.resolve()):
        raise MigrationError("backup must be outside the advisory tree")
    backup.mkdir(parents=True, exist_ok=False)
    # Persist all original bytes and the manifest before replacing any advisory.
    for file, _, before, _ in staged:
        target = _within(backup, file["path"])
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as stream:
            stream.write(before)
            stream.flush()
            os.fsync(stream.fileno())
    _write_json(
        backup / "manifest.json",
        {
            "format": audit["format"],
            "files": [f for f, *_ in staged],
            "report_sha256": _sha256(report.read_bytes()),
        },
    )
    for _, path, _, after in staged:
        _atomic_write(path, after)
    LOG.info("Applied %d advisory corrections; backup: %s", len(staged), backup)


def rollback(root: Path, backup: Path) -> None:
    """Restore original bytes when all current files still match the migration."""
    root = root.absolute()
    manifest = _json(backup / "manifest.json")
    if manifest.get("format") != "rpm-advisory-epoch-migration-v1":
        raise MigrationError("unsupported backup format")
    staged = []
    for file in manifest["files"]:
        path = _within(root, file["path"])
        original = _within(backup, file["path"]).read_bytes()
        if _sha256(original) != file["before_sha256"]:
            raise MigrationError(f"corrupt backup: {file['path']}")
        current = _sha256(path.read_bytes())
        if current not in (file["before_sha256"], file["after_sha256"]):
            raise MigrationError(f"advisory changed after migration: {file['path']}")
        if current != file["before_sha256"]:
            staged.append((path, original))
    for path, data in staged:
        _atomic_write(path, data)
    LOG.info("Restored %d advisories", len(staged))


def main(argv: list[str] | None = None) -> int:
    """Run a dry-run plan or an explicit offline apply/rollback operation."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    planner = commands.add_parser(
        "plan", help="write an audit report without changing advisories"
    )
    planner.add_argument("--root", type=Path, required=True)
    planner.add_argument("--repositories", type=Path, required=True)
    planner.add_argument("--report", type=Path, required=True)
    planner.add_argument(
        "--repository-id", action="append", help="explicitly limit the rollout scope"
    )
    applier = commands.add_parser(
        "apply", help="apply a reviewed clean plan to a paused checkout"
    )
    applier.add_argument("--root", type=Path, required=True)
    applier.add_argument("--report", type=Path, required=True)
    applier.add_argument("--backup", type=Path, required=True)
    restorer = commands.add_parser("rollback", help="restore from a verified backup")
    restorer.add_argument("--root", type=Path, required=True)
    restorer.add_argument("--backup", type=Path, required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    if args.command == "plan":
        if args.report.resolve().is_relative_to(args.root.resolve()):
            raise MigrationError("report must be outside the advisory tree")
        audit = plan(args.root, args.repositories, args.repository_id)
        _write_json(args.report, audit)
        LOG.info("Audit summary: %s", json.dumps(audit["summary"], sort_keys=True))
        return 2 if audit["summary"]["blocked"] else 0
    if args.command == "apply":
        apply(args.root, args.report, args.backup)
    else:
        rollback(args.root, args.backup)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
