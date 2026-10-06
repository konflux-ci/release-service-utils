"""Verify audited epoch backfill, both matching contracts, and reversible file edits."""

from __future__ import annotations

import bz2
import gzip
import hashlib
import json
import lzma
import xml.etree.ElementTree as ET
from pathlib import Path

import jq
import pytest
import yaml

import migrate_rpm_advisory_epochs as migration
from release_service_utils.helpers import advisory_data

REPO = "public-hummingbird-x86_64-rpms"
DIGEST = "a" * 64


def _purl(
    name: str = "bind", arch: str = "x86_64", epoch: int | None = None, repository: str = REPO
) -> str:
    return advisory_data.generate_purl_rpm(
        name, "9.20.27", "0.1.hum1", arch, "hummingbird-20251124", repository, epoch=epoch
    )


def _metadata(
    directory: Path, packages: list[dict], encoding: str = "gzip", repository: str = REPO
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    common, repo = migration.RPM_NS, migration.REPO_NS
    primary = ET.Element(common + "metadata")
    for values in packages:
        package = ET.SubElement(primary, common + "package", type="rpm")
        ET.SubElement(package, common + "name").text = values.get("name", "bind")
        ET.SubElement(package, common + "arch").text = values.get("arch", "x86_64")
        version = {"ver": "9.20.27", "rel": "0.1.hum1"}
        if "epoch" in values:
            version["epoch"] = str(values["epoch"])
        ET.SubElement(package, common + "version", **version)
        ET.SubElement(package, common + "checksum", type="sha256").text = values.get(
            "checksum", DIGEST
        )
        ET.SubElement(package, common + "location", href="Packages/b/bind.rpm")
    data = ET.tostring(primary)
    data = {
        "gzip": gzip.compress,
        "bzip2": bz2.compress,
        "xz": lzma.compress,
        "plain": lambda x: x,
    }[encoding](data)
    (directory / "primary.xml").write_bytes(data)
    repomd = ET.Element(repo + "repomd")
    entry = ET.SubElement(repomd, repo + "data", type="primary")
    ET.SubElement(entry, repo + "checksum", type="sha256").text = hashlib.sha256(
        data
    ).hexdigest()
    ET.SubElement(entry, repo + "location", href="repodata/primary.xml")
    (directory / "repomd.xml").write_bytes(ET.tostring(repomd))
    config = directory / "repositories.json"
    config.write_text(
        json.dumps(
            [
                {
                    "repository_id": repository,
                    "source": "https://packages.example/x86_64/",
                    "repomd": "repomd.xml",
                    "primary": "primary.xml",
                }
            ]
        )
    )
    return config


def _advisory(root: Path, rows: list[dict], number: str = "1") -> Path:
    path = root / "2026" / number / "advisory.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "metadata": {"name": f"2026:{number}"},
                "spec": {"content": {"artifacts": rows}, "description": "Retain this text."},
            },
            sort_keys=False,
        )
    )
    return path


def _save(path: Path, value: dict | list) -> Path:
    path.write_text(json.dumps(value))
    return path


@pytest.fixture
def checkout(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Create a checkout, verified metadata, and a single legacy advisory."""
    root = tmp_path / "advisories"
    config = _metadata(tmp_path / "metadata", [{"epoch": 32}])
    path = _advisory(
        root, [{"purl": _purl(), "signingKey": "release4", "component": "bind-main"}]
    )
    return root, config, path


@pytest.mark.parametrize("encoding", ["gzip", "bzip2", "xz", "plain"])
def test_metadata_checksums_and_compression(tmp_path: Path, encoding: str) -> None:
    """Read supported primary encodings after checking the published checksum."""
    config = _metadata(tmp_path, [{"epoch": 32}], encoding)
    index, evidence = migration.load_repositories(config)
    assert index[(REPO, "bind", "9.20.27", "0.1.hum1", "x86_64")][0]["epoch"] == 32
    assert evidence[0]["packages"] == 1
    assert (
        evidence[0]["primary_sha256"]
        == hashlib.sha256((tmp_path / "primary.xml").read_bytes()).hexdigest()
    )


@pytest.mark.parametrize("epoch", [None, "", "-1", "oops"])
def test_metadata_unknown_epochs_rejected(tmp_path: Path, epoch: str | None) -> None:
    """Reject missing or invalid metadata epochs instead of treating them as zero."""
    config = _metadata(tmp_path, [{} if epoch is None else {"epoch": epoch}])
    with pytest.raises(migration.MigrationError, match="unverified epoch"):
        migration.load_repositories(config)


def test_corrupt_metadata_rejected(tmp_path: Path) -> None:
    """Reject primary metadata that differs from its repomd checksum."""
    config = _metadata(tmp_path, [{"epoch": 32}])
    (tmp_path / "primary.xml").write_bytes(b"different")
    with pytest.raises(migration.MigrationError, match="checksum mismatch"):
        migration.load_repositories(config)


def test_archived_repository_snapshots(tmp_path: Path) -> None:
    """Accept historical snapshots of the same repository and flag epoch collisions."""
    first = _metadata(tmp_path / "first", [{"epoch": 32}])
    second = _metadata(tmp_path / "second", [{"epoch": 33}])
    config = tmp_path / "all.json"
    items = []
    for directory, source in [("first", first), ("second", second)]:
        item = json.loads(source.read_text())[0]
        item.update(primary=f"{directory}/primary.xml", repomd=f"{directory}/repomd.xml")
        items.append(item)
    config.write_text(json.dumps(items))
    root = tmp_path / "advisories"
    _advisory(root, [{"purl": _purl()}])
    audit = migration.plan(root, config)
    assert audit["summary"]["statuses"] == {"ambiguous": 1}


def test_plan_is_read_only_and_apply_is_repeatable(checkout: tuple, tmp_path: Path) -> None:
    """Apply a verified edit once and make both subsequent plan and apply idempotent."""
    root, config, path = checkout
    original = path.read_bytes()
    audit = migration.plan(root, config)
    assert path.read_bytes() == original
    assert audit["summary"]["statuses"] == {"change": 1}
    assert audit == migration.plan(root, config)
    report = _save(tmp_path / "report.json", audit)
    backup = tmp_path / "backup"
    migration.apply(root, report, backup)
    corrected = path.read_bytes()
    assert corrected == original.replace(_purl().encode(), _purl(epoch=32).encode())
    assert (backup / "2026/1/advisory.yaml").read_bytes() == original
    migration.apply(root, report, tmp_path / "unused-backup")
    assert not (tmp_path / "unused-backup").exists()
    assert migration.plan(root, config)["summary"]["statuses"] == {"already-correct": 1}
    migration.rollback(root, backup)
    assert path.read_bytes() == original
    migration.rollback(root, backup)
    assert path.read_bytes() == original


@pytest.mark.parametrize(
    "arch,epoch",
    [("x86_64", 32), ("aarch64", 5), ("noarch", 4), ("src", 32), ("x86_64", 0), ("src", 0)],
)
def test_transition_matches_both_internal_paths(tmp_path: Path, arch: str, epoch: int) -> None:
    """Match migrated binaries, noarch and source rows through both exact PURL contracts."""
    root = tmp_path / "advisories"
    repo_arch = "source" if arch == "src" else "x86_64" if arch == "noarch" else arch
    repository = f"public-hummingbird-{repo_arch}-rpms"
    config = _metadata(
        tmp_path / "metadata", [{"arch": arch, "epoch": epoch}], repository=repository
    )
    path = _advisory(root, [{"purl": _purl(arch=arch, repository=repository)}])
    report = _save(tmp_path / "report.json", migration.plan(root, config))
    migration.apply(root, report, tmp_path / "backup")
    rows = yaml.safe_load(path.read_text())["spec"]["content"]["artifacts"]
    released = {"purl": _purl(arch=arch, epoch=epoch, repository=repository)}
    different = {"purl": _purl(arch=arch, epoch=epoch + 1, repository=repository)}
    content = _save(tmp_path / "content.json", [released, different])
    existing = _save(tmp_path / "existing.json", rows)
    assert json.loads(
        advisory_data.filter_content_by_existing("rpm", content, existing, stderr_path=None)
    ) == [different]
    query = (Path(__file__).parent / "fixtures/rpm_advisory_filter.jq").read_text()
    mapping = {row["purl"]: "2026/1/advisory.yaml" for row in rows}
    result = jq.compile(query, args={"map": [mapping]}).input([released, different]).first()
    assert result == {
        "unreleased": [different],
        "in_advisory": [released],
        "advisories": ["2026/1/advisory.yaml"],
    }
    audit = migration.plan(root, config)
    assert audit["summary"]["changed_advisories"] == 0
    assert audit["summary"]["statuses"] == {
        "verified-zero" if epoch == 0 else "already-correct": 1
    }


@pytest.mark.parametrize("style", ["plain", "single", "double"])
def test_preserve_yaml_and_unrelated_purl_bytes(
    checkout: tuple, tmp_path: Path, style: str
) -> None:
    """Preserve comments, Unicode, descriptions, fields, quoting and raw qualifiers."""
    root, config, path = checkout
    old = _purl() + "&download_url=https%3A%2F%2Fexample.test%2Fa%2Fb"
    scalar = {"plain": old, "single": "'" + old + "'", "double": json.dumps(old)}[style]
    text = (
        "# résumé\nmetadata:\n  name: '2026:1'\nspec:\n"
        f"  description: |-\n    {old}\n"
        "  content:\n    artifacts:\n"
        f"    - purl: {scalar} # comment\n"
        "      signingKey: release4\n    - purl: pkg:generic/unrelated@1\n"
    )
    path.write_bytes(text.replace("\n", "\r\n").encode())
    original = path.read_bytes()
    audit = migration.plan(root, config)
    migration.apply(root, _save(tmp_path / "report.json", audit), tmp_path / "backup")
    expected = original.replace(
        ("purl: " + scalar).encode(),
        ("purl: " + scalar.replace("&distro=", "&epoch=32&distro=")).encode(),
    )
    assert path.read_bytes() == expected


@pytest.mark.parametrize("kind", ["qualifier", "sha256", "checksum", "mapping"])
def test_checksum_disambiguates_epochs(tmp_path: Path, kind: str) -> None:
    """Use available checksum evidence to select one epoch from two published identities."""
    config = _metadata(
        tmp_path / "metadata", [{"epoch": 32}, {"epoch": 33, "checksum": "b" * 64}]
    )
    row = {"purl": _purl()}
    if kind == "qualifier":
        row["purl"] += "&checksum=sha256%3A" + DIGEST
    elif kind == "sha256":
        row["sha256"] = DIGEST
    elif kind == "checksum":
        row["checksum"] = "sha256:" + DIGEST
    else:
        row["checksum"] = {"sha256": DIGEST}
    root = tmp_path / "advisories"
    _advisory(root, [row])
    audit = migration.plan(root, config)
    assert audit["summary"]["statuses"] == {"change": 1}
    assert audit["files"][0]["changes"][0]["epoch"] == 32


@pytest.mark.parametrize(
    "case,status",
    [
        ("missing", "unresolved"),
        ("repository", "unresolved"),
        ("arch", "unresolved"),
        ("checksum", "unresolved"),
        ("ambiguous", "ambiguous"),
        ("conflict", "conflict"),
        ("qualifiers", "invalid"),
        ("subpath", "invalid"),
        ("noarch", "invalid"),
        ("bad-epoch", "invalid"),
        ("zero-qualifier", "conflict"),
    ],
)
def test_unsafe_identities_block_application(tmp_path: Path, case: str, status: str) -> None:
    """Report unknown, ambiguous, invalid and conflicting identities without changing files."""
    packages = [{"epoch": 32}]
    purl = _purl()
    if case == "missing":
        purl = _purl(name="absent")
    elif case == "repository":
        purl = purl.replace(REPO, "another-repository")
    elif case == "arch":
        purl = purl.replace("arch=x86_64", "arch=aarch64")
    elif case == "checksum":
        purl += "&checksum=sha256:" + "c" * 64
    elif case == "ambiguous":
        packages.append({"epoch": 33})
    elif case == "conflict":
        purl = _purl(epoch=33)
    elif case == "qualifiers":
        purl += "&arch=src"
    elif case == "subpath":
        purl += "#file"
    elif case == "noarch":
        purl = purl.replace("arch=x86_64&", "")
    elif case == "bad-epoch":
        purl += "&epoch=nope"
    elif case == "zero-qualifier":
        purl += "&epoch=0"
    config = _metadata(tmp_path / "metadata", packages)
    root = tmp_path / "advisories"
    path = _advisory(root, [{"purl": purl}])
    original = path.read_bytes()
    audit = migration.plan(root, config)
    assert audit["summary"]["statuses"] == {status: 1}
    assert audit["summary"]["blocked"] == 1
    with pytest.raises(migration.MigrationError, match="blocked"):
        migration.apply(root, _save(tmp_path / "report.json", audit), tmp_path / "backup")
    assert path.read_bytes() == original
    assert not (tmp_path / "backup").exists()


@pytest.mark.parametrize("case", ["edited", "added", "deleted", "symlink"])
def test_stale_inventory_rejected_before_writes(
    checkout: tuple, tmp_path: Path, case: str
) -> None:
    """Reject concurrent inventory changes or unsafe paths before making any edit."""
    root, config, path = checkout
    report = _save(tmp_path / "report.json", migration.plan(root, config))
    if case == "edited":
        path.write_bytes(path.read_bytes() + b"# concurrent edit\n")
    elif case == "added":
        _advisory(root, [{"purl": _purl()}], "2")
    elif case == "deleted":
        path.unlink()
    else:
        outside = tmp_path / "outside.yaml"
        outside.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(outside)
    with pytest.raises(migration.MigrationError):
        migration.apply(root, report, tmp_path / "backup")
    assert not (tmp_path / "backup").exists()


@pytest.mark.parametrize("case", ["backup", "advisory"])
def test_rollback_refuses_corruption_or_later_edits(
    checkout: tuple, tmp_path: Path, case: str
) -> None:
    """Refuse rollback when originals are corrupt or advisory changes would be overwritten."""
    root, config, path = checkout
    report = _save(tmp_path / "report.json", migration.plan(root, config))
    backup = tmp_path / "backup"
    migration.apply(root, report, backup)
    target = backup / "2026/1/advisory.yaml" if case == "backup" else path
    target.write_bytes(target.read_bytes() + b"# edit\n")
    current = path.read_bytes()
    with pytest.raises(migration.MigrationError):
        migration.rollback(root, backup)
    assert path.read_bytes() == current


def test_interrupted_apply_can_be_rolled_back(
    checkout: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Save every original before a write failure and restore a partially applied plan."""
    root, config, path = checkout
    second = _advisory(root, [{"purl": _purl()}], "2")
    originals = {path: path.read_bytes(), second: second.read_bytes()}
    report = _save(tmp_path / "report.json", migration.plan(root, config))
    writer = migration._atomic_write
    calls = 0

    def fail_second(target: Path, data: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated interruption")
        writer(target, data)

    monkeypatch.setattr(migration, "_atomic_write", fail_second)
    backup = tmp_path / "backup"
    with pytest.raises(OSError, match="interruption"):
        migration.apply(root, report, backup)
    monkeypatch.setattr(migration, "_atomic_write", writer)
    with pytest.raises(migration.MigrationError, match="partially applied"):
        migration.apply(root, report, tmp_path / "other-backup")
    migration.rollback(root, backup)
    assert all(p.read_bytes() == data for p, data in originals.items())


def test_scope_is_explicit_and_unknown_repos_remain_blocked(
    checkout: tuple, tmp_path: Path
) -> None:
    """Record scope exclusions and require metadata for every named rollout repository."""
    root, config, _ = checkout
    _advisory(root, [{"purl": _purl().replace(REPO, "private-repo")}], "2")
    assert migration.plan(root, config)["summary"]["blocked"] == 1
    scoped = migration.plan(root, config, [REPO])
    assert scoped["summary"]["statuses"] == {"change": 1, "out-of-scope": 1}
    assert scoped["repository_scope"] == [REPO]
    with pytest.raises(migration.MigrationError, match="scoped repository"):
        migration.plan(root, config, ["absent"])


def test_tampered_plan_cannot_edit_unrelated_content(checkout: tuple, tmp_path: Path) -> None:
    """Reject a reviewed plan edited to change another identity field or qualifier."""
    root, config, path = checkout
    audit = migration.plan(root, config)
    entry = audit["files"][0]["changes"][0]
    entry["after"] = entry["after"].replace("distro=hummingbird", "distro=unrelated")
    report = _save(tmp_path / "report.json", audit)
    original = path.read_bytes()
    with pytest.raises(migration.MigrationError, match="unrelated"):
        migration.apply(root, report, tmp_path / "backup")
    assert path.read_bytes() == original


@pytest.mark.parametrize(
    "text", ["purl: VALUE\npurl: VALUE\n", "purl: &shared VALUE\nother: *shared\n"]
)
def test_duplicate_keys_and_alias_side_effects_blocked(checkout: tuple, text: str) -> None:
    """Report duplicate keys or alias changes that could affect unrelated YAML content."""
    root, config, path = checkout
    body = text.replace("VALUE", _purl())
    path.write_text(
        "spec:\n  content:\n    artifacts:\n    - "
        + body.replace("\n", "\n      ").rstrip()
        + "\n"
    )
    assert migration.plan(root, config)["summary"]["blocked"] == 1


def test_cli_reports_blockers_and_never_writes_inside_inventory(
    checkout: tuple, tmp_path: Path
) -> None:
    """Return a distinct audit failure status and keep report output outside the checkout."""
    root, config, _ = checkout
    report = tmp_path / "audit.json"
    argv = [
        "plan",
        "--root",
        str(root),
        "--repositories",
        str(config),
        "--report",
        str(report),
    ]
    assert migration.main(argv) == 0
    _advisory(root, [{"purl": _purl(name="absent")}], "2")
    assert migration.main(argv) == 2
    argv[-1] = str(root / "report.json")
    with pytest.raises(migration.MigrationError, match="outside"):
        migration.main(argv)


def test_escaped_yaml_purl_is_inventoried(checkout: tuple, tmp_path: Path) -> None:
    """Find decoded RPM PURLs even when the raw YAML contains escaped slashes."""
    root, config, path = checkout
    scalar = json.dumps(_purl()).replace("/", r"\/")
    path.write_text(f"spec:\n  content:\n    artifacts:\n    - purl: {scalar}\n")
    audit = migration.plan(root, config)
    assert audit["summary"]["statuses"] == {"change": 1}
    migration.apply(root, _save(tmp_path / "report.json", audit), tmp_path / "backup")
    assert yaml.safe_load(path.read_text())["spec"]["content"]["artifacts"][0][
        "purl"
    ] == _purl(epoch=32)


@pytest.mark.parametrize("checksum", [{"sha256": 123}, {"md5": "invalid"}, 123])
def test_unsupported_checksums_block_application(checkout: tuple, checksum: object) -> None:
    """Report unsupported artifact checksum data instead of ignoring available evidence."""
    root, config, _ = checkout
    _advisory(root, [{"purl": _purl(), "checksum": checksum}])
    assert migration.plan(root, config)["summary"]["statuses"] == {"invalid": 1}


@pytest.mark.parametrize("epoch", ["0", "032"])
def test_noncanonical_explicit_epochs_need_review(checkout: tuple, epoch: str) -> None:
    """Block existing qualifiers that cannot match the corrected producer's output."""
    root, config, _ = checkout
    if epoch == "0":
        config = _metadata(config.parent, [{"epoch": 0}])
    _advisory(root, [{"purl": _purl() + "&epoch=" + epoch}])
    assert migration.plan(root, config)["summary"]["statuses"] == {"invalid": 1}


@pytest.mark.parametrize("relative", ["../outside/advisory.yaml", "/outside/advisory.yaml"])
def test_plan_path_traversal_rejected(checkout: tuple, tmp_path: Path, relative: str) -> None:
    """Refuse a report that points outside the chosen advisory checkout."""
    root, config, path = checkout
    audit = migration.plan(root, config)
    audit["files"][0]["path"] = relative
    report = _save(tmp_path / "report.json", audit)
    original = path.read_bytes()
    with pytest.raises(migration.MigrationError):
        migration.apply(root, report, tmp_path / "backup")
    assert path.read_bytes() == original


def test_backup_must_be_new_and_outside_inventory(checkout: tuple, tmp_path: Path) -> None:
    """Preserve backups and keep backup advisories outside the inventory."""
    root, config, path = checkout
    report = _save(tmp_path / "report.json", migration.plan(root, config))
    original = path.read_bytes()
    with pytest.raises(migration.MigrationError, match="outside"):
        migration.apply(root, report, root / "backup")
    backup = tmp_path / "backup"
    backup.mkdir()
    with pytest.raises(FileExistsError):
        migration.apply(root, report, backup)
    assert path.read_bytes() == original


def test_cli_apply_and_rollback(checkout: tuple, tmp_path: Path) -> None:
    """Exercise the operator entry points through a complete local apply and rollback."""
    root, config, path = checkout
    original = path.read_bytes()
    report = _save(tmp_path / "report.json", migration.plan(root, config))
    backup = tmp_path / "backup"
    assert (
        migration.main(
            ["apply", "--root", str(root), "--report", str(report), "--backup", str(backup)]
        )
        == 0
    )
    assert path.read_bytes() != original
    assert migration.main(["rollback", "--root", str(root), "--backup", str(backup)]) == 0
    assert path.read_bytes() == original


def test_metadata_file_change_cannot_replace_verified_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Parse the verified snapshot even if its file changes after it was read."""
    config = _metadata(tmp_path, [{"epoch": 32}])
    original = migration.ET.fromstring

    def mutate_after_read(data: bytes) -> ET.Element:
        document = original(data)
        (tmp_path / "primary.xml").write_bytes(b"different metadata")
        return document

    monkeypatch.setattr(migration.ET, "fromstring", mutate_after_read)
    index, _ = migration.load_repositories(config)
    assert index[(REPO, "bind", "9.20.27", "0.1.hum1", "x86_64")][0]["epoch"] == 32


def test_changed_epoch_in_report_must_agree_with_evidence(
    checkout: tuple, tmp_path: Path
) -> None:
    """Refuse an epoch edit whose recorded package evidence verifies a different epoch."""
    root, config, path = checkout
    audit = migration.plan(root, config)
    change = audit["files"][0]["changes"][0]
    change["epoch"] = 33
    change["after"] = _purl(epoch=33)
    original = path.read_bytes()
    with pytest.raises(migration.MigrationError, match="metadata evidence"):
        migration.apply(root, _save(tmp_path / "report.json", audit), tmp_path / "backup")
    assert path.read_bytes() == original
