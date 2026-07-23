"""Tests for ``filter_already_released_advisory_rpms``."""

from __future__ import annotations

import dataclasses
import json
import shutil
import subprocess
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import filter_already_released_advisory_rpms as filt
import pytest
import requests


def _write_json(path: Path, data: dict | list) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def _snapshot(components: list[dict] | None = None) -> dict:
    return {"components": components or []}


def _data(
    rpm_repos: list[dict] | None = None,
    intention: str = "",
    advisory_repo: str = "",
) -> dict:
    d: dict = {"mapping": {}}
    if rpm_repos is not None:
        d["mapping"]["rpm-repositories"] = rpm_repos
    if intention:
        d["intention"] = intention
    if advisory_repo:
        d["advisory"] = {"repo": advisory_repo}
    return d


def _rpa(origin: str = "test-origin") -> dict:
    return {"spec": {"origin": origin}}


def _toml_content(
    base_url: str = "https://pulp.example.com",
    username: str = "",
    password: str = "",
    client_id: str = "",
    client_secret: str = "",
) -> str:
    lines = ["[cli]", f'base_url = "{base_url}"']
    if username:
        lines.append(f'username = "{username}"')
    if password:
        lines.append(f'password = "{password}"')
    if client_id:
        lines.append(f'client_id = "{client_id}"')
    if client_secret:
        lines.append(f'client_secret = "{client_secret}"')
    return "\n".join(lines) + "\n"


def _rpm_repos() -> list[dict]:
    return [
        {
            "arch": "x86_64",
            "repository_id": "rpm-x86_64",
            "repository_name": "x86_64",
            "distro": "el9",
        },
        {
            "arch": "aarch64",
            "repository_id": "rpm-aarch64",
            "repository_name": "aarch64",
            "distro": "el9",
        },
        {
            "arch": "src",
            "repository_id": "rpm-src",
            "repository_name": "source",
            "distro": "el9",
        },
    ]


def _setup_base_files(
    tmp_path: Path,
    data: dict | None = None,
    snapshot: dict | None = None,
    rpa: dict | None = None,
    toml: str | None = None,
) -> tuple[Path, Path, Path, Path]:
    """Create JSON files and return (snapshot, data, rpa, pulp_config)."""
    snap_file = tmp_path / "snapshot.json"
    data_file = tmp_path / "data.json"
    rpa_file = tmp_path / "rpa.json"
    toml_file = tmp_path / "cli.toml"

    _write_json(snap_file, snapshot or _snapshot())
    _write_json(data_file, data or _data())
    _write_json(rpa_file, rpa or _rpa())
    toml_file.write_text(
        toml or _toml_content(username="user", password="pass"),
        encoding="utf-8",
    )
    return snap_file, data_file, rpa_file, toml_file


def _result_paths(tmp_path: Path) -> filt.ResultPaths:
    """Create result paths and return a ``ResultPaths``."""
    return filt.ResultPaths(
        skip_release=tmp_path / "skip_release",
        environment=tmp_path / "environment",
        latest_advisory_url=tmp_path / "advisory_url",
        latest_advisory_internal_url=tmp_path / "advisory_internal_url",
    )


def _make_config_and_results(
    tmp_path: Path,
    *,
    data: dict | None = None,
    snapshot: dict | None = None,
    rpa: dict | None = None,
    toml: str | None = None,
    default_excludes: list[str] | None = None,
    default_architectures: list[str] | None = None,
) -> tuple[filt.FilterConfig, filt.ResultPaths]:
    """Build a ``FilterConfig`` and ``ResultPaths``, writing files to *tmp_path*."""
    snap_file, data_file, rpa_file, toml_file = _setup_base_files(
        tmp_path,
        data=data,
        snapshot=snapshot,
        rpa=rpa,
        toml=toml,
    )
    cfg = filt.FilterConfig(
        snapshot_file=snap_file,
        data_file=data_file,
        rpa_file=rpa_file,
        pulp_config_file=toml_file,
        pulp_domain="dom",
        default_excludes=default_excludes if default_excludes is not None else [],
        default_architectures=default_architectures or ["x86_64"],
        pipeline_run_uid="uid",
        oci_storage="oci",
        oras_options="",
        task_git_url="git",
        task_git_revision="rev",
        synchronously=True,
    )
    return cfg, _result_paths(tmp_path)


def _make_filter_tarball(
    tmp_path: Path,
    unreleased: list[dict],
    in_advisory: list[dict] | None = None,
) -> Path:
    """Create a filter-results.tar.gz in *tmp_path* and return its path."""
    src_dir = tmp_path / "tar_src"
    src_dir.mkdir(exist_ok=True)
    _write_json(src_dir / "unreleased_rpms.json", unreleased)
    _write_json(
        src_dir / "in_advisory_rpms.json",
        in_advisory if in_advisory is not None else [],
    )
    tarball = tmp_path / "filter-results.tar.gz"
    with tarfile.open(tarball, "w:gz") as tf:
        tf.add(src_dir / "unreleased_rpms.json", arcname="unreleased_rpms.json")
        tf.add(src_dir / "in_advisory_rpms.json", arcname="in_advisory_rpms.json")
    return tarball


def _nevra_from_filename(fname: str) -> tuple[str, str, str, str]:
    """Parse an RPM filename into (name, version, release, arch).

    Used by mock runners to generate realistic ``rpm -qp`` output.
    Real ``rpm -qp --qf '%{ARCH}'`` returns the build architecture for
    source RPMs (e.g. ``x86_64``), **not** ``src``.  The production code
    must override the arch to ``src`` based on the ``.src.rpm`` suffix.
    """
    is_src = fname.endswith(".src.rpm")
    base = Path(fname).stem
    if is_src:
        base = Path(base).stem
    parts = base.rsplit(".", 1)
    arch = parts[1] if len(parts) > 1 else "x86_64"
    nvr = parts[0]
    if is_src:
        arch = "x86_64"
    release = nvr.rsplit("-", 1)[1] if "-" in nvr else "1"
    namever = nvr.rsplit("-", 1)[0] if "-" in nvr else nvr
    version = namever.rsplit("-", 1)[1] if "-" in namever else "1.0"
    name = namever.rsplit("-", 1)[0] if "-" in namever else namever
    return name, version, release, arch


def _mock_run_cmd_for_oras_and_rpm(
    rpm_files: list[str],
    ir_results: dict | None = None,
    filter_tarball: Path | None = None,
) -> MagicMock:
    """Return a mock for ``subprocess_cmd.run_cmd`` handling oras, rpm, IR, and kubectl."""
    if ir_results is None:
        ir_results = {
            "result": "Success",
            "filter_results_artifact": "oci:quay.io/r@sha256:abc",
            "advisory_url": "",
            "advisory_internal_url": "",
        }

    filter_artifact = ir_results.get("filter_results_artifact", "")
    filter_ref = filter_artifact.removeprefix("oci:")

    def side_effect(cmd, **kwargs):
        if cmd[0] == "select-oci-auth":
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="auth")
        if cmd[0] == "oras" and "pull" in cmd:
            cwd = kwargs.get("cwd")
            if cwd is not None:
                out_dir = Path(cwd)
            elif "-o" in cmd:
                out_dir = Path(cmd[cmd.index("-o") + 1])
            else:
                out_dir = Path(".")
            out_dir.mkdir(parents=True, exist_ok=True)
            pull_spec = cmd[-1]
            is_filter = filter_ref and pull_spec == filter_ref
            if is_filter and filter_tarball:
                shutil.copy2(filter_tarball, out_dir / "filter-results.tar.gz")
            elif not is_filter and rpm_files:
                for f in rpm_files:
                    (out_dir / f).touch()
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")
        if cmd[0] == "rpm":
            fname = cmd[-1]
            name, version, release, arch = _nevra_from_filename(fname)
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=0,
                stdout=f"{name}|0|{version}|{release}|{arch}\n",
            )
        if cmd[0] == "internal-request":
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=0,
                stdout="ir/'my-ir' created\n",
            )
        if cmd[0] == "kubectl":
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=0,
                stdout=json.dumps(ir_results),
            )
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")

    return MagicMock(side_effect=side_effect)


class TestShouldExcludeFile:
    """Test file exclusion by pattern."""

    def test_matches_pattern(self) -> None:
        """File containing an exclude pattern is excluded."""
        assert filt.should_exclude_file(
            "hello-debuginfo-1.0.rpm", ["-debuginfo-", "-debugsource-"]
        )

    def test_no_match(self) -> None:
        """Normal file is not excluded."""
        assert not filt.should_exclude_file("hello-1.0.rpm", ["-debuginfo-", "-debugsource-"])

    def test_empty_patterns(self) -> None:
        """Empty pattern list excludes nothing."""
        assert not filt.should_exclude_file("hello-1.0.rpm", [])

    def test_whitespace_pattern(self) -> None:
        """Whitespace-only patterns are skipped."""
        assert not filt.should_exclude_file("hello-1.0.rpm", ["", " "])


class TestDetermineEnvironment:
    """Test environment determination."""

    def test_staging_intention(self) -> None:
        """Intention 'staging' -> 'stage'."""
        assert filt.determine_environment({"intention": "staging"}) == "stage"

    def test_production_intention(self) -> None:
        """Intention 'production' -> 'production'."""
        assert filt.determine_environment({"intention": "production"}) == "production"

    def test_stage_advisory_repo(self) -> None:
        """Advisory repo containing 'stage' -> 'stage'."""
        d = {"advisory": {"repo": "https://stage.example.com"}}
        assert filt.determine_environment(d) == "stage"

    def test_rhtap_release_repo(self) -> None:
        """Advisory repo containing 'rhtap-release' -> 'stage'."""
        d = {"advisory": {"repo": "https://rhtap-release.example.com"}}
        assert filt.determine_environment(d) == "stage"

    def test_default_production(self) -> None:
        """No matching signals -> 'production'."""
        assert filt.determine_environment({}) == "production"


class TestExtractRpmMetadata:
    """Test RPM metadata extraction."""

    def test_success(self, tmp_path: Path) -> None:
        """Parse rpm -qp output into RpmNevra."""
        mock_run = MagicMock(
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="hello|0|2.12|1.fc44|x86_64\n",
            )
        )
        with patch.object(filt.subprocess_cmd, "run_cmd", mock_run):
            result = filt.extract_rpm_metadata(tmp_path / "hello.rpm")
        assert result == filt.RpmNevra(
            name="hello",
            epoch="0",
            version="2.12",
            release="1.fc44",
            arch="x86_64",
        )

    def test_none_epoch(self, tmp_path: Path) -> None:
        """(none) epoch normalizes to '0'."""
        mock_run = MagicMock(
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="hello|(none)|2.12|1.fc44|x86_64\n",
            )
        )
        with patch.object(filt.subprocess_cmd, "run_cmd", mock_run):
            result = filt.extract_rpm_metadata(tmp_path / "hello.rpm")
        assert result is not None
        assert result.epoch == "0"

    def test_failure_returns_none(self, tmp_path: Path) -> None:
        """Non-zero return code returns None."""
        mock_run = MagicMock(
            return_value=subprocess.CompletedProcess(args=[], returncode=1, stdout="")
        )
        with patch.object(filt.subprocess_cmd, "run_cmd", mock_run):
            assert filt.extract_rpm_metadata(tmp_path / "bad.rpm") is None

    def test_bad_output_returns_none(self, tmp_path: Path) -> None:
        """Malformed output returns None."""
        mock_run = MagicMock(
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="bad-output\n",
            )
        )
        with patch.object(filt.subprocess_cmd, "run_cmd", mock_run):
            assert filt.extract_rpm_metadata(tmp_path / "bad.rpm") is None

    def test_os_error_returns_none(self, tmp_path: Path) -> None:
        """OSError (missing binary) returns None."""
        mock_run = MagicMock(side_effect=OSError("not found"))
        with patch.object(filt.subprocess_cmd, "run_cmd", mock_run):
            assert filt.extract_rpm_metadata(tmp_path / "bad.rpm") is None


class TestBuildPurl:
    """Test purl construction."""

    def test_full_purl(self) -> None:
        """Build purl with distro and repo_id."""
        result = filt._build_purl("hello", "1.0", "1.el9", "x86_64", "el9", "repo-1")
        assert (
            result
            == "pkg:rpm/redhat/hello@1.0-1.el9?arch=x86_64&distro=el9&repository_id=repo-1"
        )

    def test_purl_no_distro(self) -> None:
        """Build purl without distro."""
        result = filt._build_purl("hello", "1.0", "1.el9", "x86_64", "", "repo-1")
        assert "distro" not in result

    def test_purl_no_repo_id(self) -> None:
        """Build purl without repository_id."""
        result = filt._build_purl("hello", "1.0", "1.el9", "x86_64", "el9", "")
        assert "repository_id" not in result


class TestBuildRpmEntries:
    """Test Phase 1: RPM extraction and entry building."""

    def _run_build(
        self,
        rpm_files: list[str],
        rpm_repos: list[dict] | None = None,
        default_excludes: list[str] | None = None,
        default_architectures: list[str] | None = None,
        components: list[dict] | None = None,
    ) -> list[filt.RpmEntry]:
        if components is None:
            components = [{"containerImage": "quay.io/test/img@sha256:abc", "name": "comp1"}]
        snapshot = _snapshot(components)
        data = _data(rpm_repos=_rpm_repos() if rpm_repos is None else rpm_repos)
        mock_run = _mock_run_cmd_for_oras_and_rpm(rpm_files)

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", mock_run),
            patch.object(filt.file_helper, "sha256", return_value="sha_abc"),
        ):
            return filt.build_rpm_entries(
                snapshot,
                data,
                ["-debuginfo-"] if default_excludes is None else default_excludes,
                (
                    ["x86_64", "aarch64"]
                    if default_architectures is None
                    else default_architectures
                ),
            )

    def test_single_component(self) -> None:
        """Build entries for a single component."""
        entries = self._run_build(["hello-1.0-1.el9.x86_64.rpm"])

        assert len(entries) == 1
        assert entries[0].component_name == "comp1"
        assert entries[0].sha256 == "sha_abc"

        rpms_map = filt.entries_to_rpms_map(entries)
        assert "comp1" in rpms_map

    def test_noarch_rpm(self) -> None:
        """Noarch RPMs are published to all default arch repos."""
        entries = self._run_build(
            ["hello-docs-1.0-1.el9.noarch.rpm"],
            default_excludes=[],
        )

        assert len(entries) == 2
        repo_names = [e.target_repo.get("repository_name", "") for e in entries]
        assert "x86_64" in repo_names
        assert "aarch64" in repo_names

    def test_src_rpm(self) -> None:
        """Source RPMs target the src repo despite rpm -qp returning build arch."""
        entries = self._run_build(
            ["hello-1.0-1.el9.src.rpm"],
            default_excludes=[],
        )

        assert len(entries) == 1
        assert entries[0].target_repo.get("repository_name") == "source"
        assert entries[0].nevra.arch == "src"
        assert "arch=src" in entries[0].purl

    def test_excludes_debuginfo(self) -> None:
        """Debug RPMs are excluded."""
        entries = self._run_build(
            ["hello-debuginfo-1.0-1.el9.x86_64.rpm"],
            default_excludes=["-debuginfo-"],
            default_architectures=["x86_64"],
        )
        assert entries == []

    def test_no_rpm_files(self) -> None:
        """Component with no RPM files is skipped."""
        entries = self._run_build(
            [],
            default_excludes=[],
            default_architectures=["x86_64"],
        )
        assert entries == []

    def test_non_rpm_files_and_directories_skipped(self) -> None:
        """Non-RPM files and subdirectories in the pull directory are ignored."""
        rpm_files = ["hello-1.0-1.el9.x86_64.rpm"]

        def mock_run(cmd, **kwargs):
            if cmd[0] == "select-oci-auth":
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="auth")
            if cmd[0] == "oras" and "pull" in cmd:
                cwd = kwargs.get("cwd")
                out_dir = Path(cwd) if cwd else Path(".")
                out_dir.mkdir(parents=True, exist_ok=True)
                for f in rpm_files:
                    (out_dir / f).touch()
                (out_dir / "manifest.json").touch()
                (out_dir / "logs").mkdir(exist_ok=True)
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")
            if cmd[0] == "rpm":
                fname = cmd[-1]
                name, version, release, arch = _nevra_from_filename(fname)
                return subprocess.CompletedProcess(
                    args=cmd,
                    returncode=0,
                    stdout=f"{name}|0|{version}|{release}|{arch}\n",
                )
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")

        components = [{"containerImage": "quay.io/test/img@sha256:abc", "name": "comp1"}]
        snapshot = _snapshot(components)
        data = _data(rpm_repos=_rpm_repos())

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", MagicMock(side_effect=mock_run)),
            patch.object(filt.file_helper, "sha256", return_value="sha_abc"),
        ):
            entries = filt.build_rpm_entries(snapshot, data, [], ["x86_64", "aarch64"])

        assert len(entries) == 1
        assert entries[0].nevra.name == "hello"

    def test_no_repo_mapping_for_arch(self) -> None:
        """RPMs for unmapped architectures are skipped."""
        entries = self._run_build(
            ["hello-1.0-1.el9.x86_64.rpm"],
            rpm_repos=[],
            default_excludes=[],
            default_architectures=["x86_64"],
        )
        assert entries == []

    def test_rpm_metadata_failure_skipped(self) -> None:
        """RPMs that fail metadata extraction are skipped."""

        def bad_run(cmd, **kwargs):
            if cmd[0] == "select-oci-auth":
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="auth")
            if cmd[0] == "oras" and "pull" in cmd:
                cwd = kwargs.get("cwd")
                out_dir = Path(cwd) if cwd else Path(".")
                out_dir.mkdir(parents=True, exist_ok=True)
                (out_dir / "hello-1.0-1.el9.x86_64.rpm").touch()
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")
            if cmd[0] == "rpm":
                return subprocess.CompletedProcess(args=cmd, returncode=1, stdout="")
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")

        snapshot = _snapshot(
            [{"containerImage": "quay.io/test/img@sha256:abc", "name": "comp1"}]
        )
        data = _data(rpm_repos=_rpm_repos())

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", MagicMock(side_effect=bad_run)),
            patch.object(filt.file_helper, "sha256", return_value="sha_abc"),
        ):
            entries = filt.build_rpm_entries(snapshot, data, [], ["x86_64"])

        assert entries == []

    def test_noarch_no_target_repos(self) -> None:
        """Noarch RPMs with no matching default arch repos are skipped."""
        entries = self._run_build(
            ["hello-docs-1.0-1.el9.noarch.rpm"],
            rpm_repos=[],
            default_excludes=[],
            default_architectures=["x86_64"],
        )
        assert entries == []

    def test_src_rpm_no_repo_mapping(self) -> None:
        """Source RPMs without a src repo mapping are skipped."""
        repos = [r for r in _rpm_repos() if r["arch"] != "src"]
        entries = self._run_build(
            ["hello-1.0-1.el9.src.rpm"],
            rpm_repos=repos,
            default_excludes=[],
            default_architectures=["x86_64"],
        )
        assert entries == []

    def test_mixed_binary_and_src_rpms(self) -> None:
        """Binary and source RPMs from the same component route correctly."""
        entries = self._run_build(
            [
                "hello-1.0-1.el9.x86_64.rpm",
                "hello-1.0-1.el9.src.rpm",
                "hello-data-1.0-1.el9.noarch.rpm",
            ],
            default_excludes=[],
            default_architectures=["x86_64", "aarch64"],
        )

        src_entries = [e for e in entries if e.nevra.arch == "src"]
        assert len(src_entries) == 1
        assert src_entries[0].target_repo.get("repository_name") == "source"
        assert src_entries[0].rpm_filename == "hello-1.0-1.el9.src.rpm"

        binary_entries = [e for e in entries if e.nevra.arch == "x86_64"]
        assert len(binary_entries) == 1
        assert binary_entries[0].target_repo.get("repository_name") == "x86_64"

        noarch_entries = [e for e in entries if e.nevra.arch == "noarch"]
        assert len(noarch_entries) == 2
        noarch_repos = sorted(e.target_repo.get("repository_name", "") for e in noarch_entries)
        assert noarch_repos == ["aarch64", "x86_64"]

        assert len(entries) == 4

    def test_multiple_components(self) -> None:
        """Build entries for multiple components, one with RPMs and one without."""
        oras_call_count = 0

        def side_effect(cmd, **kwargs):
            nonlocal oras_call_count
            if cmd[0] == "select-oci-auth":
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="auth")
            if cmd[0] == "oras" and "pull" in cmd:
                oras_call_count += 1
                cwd = kwargs.get("cwd")
                out_dir = Path(cwd) if cwd else Path(".")
                out_dir.mkdir(parents=True, exist_ok=True)
                if oras_call_count == 1:
                    (out_dir / "hello-1.0-1.el9.x86_64.rpm").touch()
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")
            if cmd[0] == "rpm":
                fname = cmd[-1]
                name, version, release, arch = _nevra_from_filename(fname)
                return subprocess.CompletedProcess(
                    args=cmd,
                    returncode=0,
                    stdout=f"{name}|0|{version}|{release}|{arch}\n",
                )
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")

        snapshot = _snapshot(
            [
                {"containerImage": "quay.io/test/img1@sha256:aaa", "name": "comp1"},
                {"containerImage": "quay.io/test/img2@sha256:bbb", "name": "comp2"},
            ]
        )
        data = _data(rpm_repos=_rpm_repos())

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", MagicMock(side_effect=side_effect)),
            patch.object(filt.file_helper, "sha256", return_value="sha_abc"),
        ):
            entries = filt.build_rpm_entries(
                snapshot,
                data,
                ["-debuginfo-"],
                ["x86_64", "aarch64"],
            )

        assert len(entries) == 1
        assert entries[0].component_name == "comp1"

        rpms_map = filt.entries_to_rpms_map(entries)
        assert "comp1" in rpms_map
        assert "comp2" not in rpms_map


class TestEntriesToIrPayload:
    """Test IR payload serialization from RpmEntry."""

    def test_serializes_all_fields(self) -> None:
        """All RpmEntry fields appear in the serialized dict."""
        entry = filt.RpmEntry(
            component_name="comp1",
            rpm_filename="hello-1.0-1.el9.x86_64.rpm",
            sha256="abc123",
            nevra=filt.RpmNevra("hello", "0", "1.0", "1.el9", "x86_64"),
            purl="pkg:rpm/redhat/hello@1.0-1.el9?arch=x86_64",
            target_repo={
                "repository_name": "x86_64",
                "repository_id": "repo-1",
                "distro": "el9",
            },
        )
        payload = filt.entries_to_ir_payload([entry])
        assert len(payload) == 1
        d = payload[0]
        assert d["name"] == "comp1"
        assert d["rpm"] == "hello-1.0-1.el9.x86_64.rpm"
        assert d["sha256"] == "abc123"
        assert d["rpmname"] == "hello"
        assert d["epoch"] == "0"
        assert d["version"] == "1.0"
        assert d["release"] == "1.el9"
        assert d["arch"] == "x86_64"
        assert d["purl"] == "pkg:rpm/redhat/hello@1.0-1.el9?arch=x86_64"
        assert d["repository_name"] == "x86_64"
        assert d["targetRepo"] == entry.target_repo

    def test_empty_list(self) -> None:
        """Empty input produces empty output."""
        assert filt.entries_to_ir_payload([]) == []


class TestEntriesToRpmsMap:
    """Test rpmsToPublish grouping from RpmEntry."""

    def _entry(
        self,
        component: str = "comp1",
        rpm: str = "hello-1.0-1.el9.x86_64.rpm",
        sha: str = "abc",
        repo_name: str = "x86_64",
    ) -> filt.RpmEntry:
        return filt.RpmEntry(
            component_name=component,
            rpm_filename=rpm,
            sha256=sha,
            nevra=filt.RpmNevra("hello", "0", "1.0", "1.el9", "x86_64"),
            purl="pkg:rpm/redhat/hello@1.0-1.el9?arch=x86_64",
            target_repo={"repository_name": repo_name},
        )

    def test_single_entry(self) -> None:
        """Single entry produces a single-element map."""
        rpms_map = filt.entries_to_rpms_map([self._entry()])
        assert "comp1" in rpms_map
        assert len(rpms_map["comp1"]) == 1
        assert rpms_map["comp1"][0]["rpm"] == "hello-1.0-1.el9.x86_64.rpm"
        assert rpms_map["comp1"][0]["targetRepos"] == [{"repository_name": "x86_64"}]

    def test_separate_entries_per_repo(self) -> None:
        """Multiple entries for the same RPM produce separate items."""
        entries = [
            self._entry(repo_name="x86_64"),
            self._entry(repo_name="aarch64"),
        ]
        rpms_map = filt.entries_to_rpms_map(entries)
        assert len(rpms_map["comp1"]) == 2
        repos = [item["targetRepos"] for item in rpms_map["comp1"]]
        assert [{"repository_name": "x86_64"}] in repos
        assert [{"repository_name": "aarch64"}] in repos

    def test_multiple_components(self) -> None:
        """Entries from different components are grouped separately."""
        entries = [
            self._entry(component="comp1"),
            self._entry(component="comp2", rpm="world-2.0-1.el9.x86_64.rpm"),
        ]
        rpms_map = filt.entries_to_rpms_map(entries)
        assert "comp1" in rpms_map
        assert "comp2" in rpms_map

    def test_empty_list(self) -> None:
        """Empty input produces empty map."""
        assert filt.entries_to_rpms_map([]) == {}


class TestCreateInternalRequest:
    """Test InternalRequest creation and result parsing."""

    def test_success(self) -> None:
        """Parse IR name and return results."""
        mock_run = _mock_run_cmd_for_oras_and_rpm([])
        with patch.object(filt.subprocess_cmd, "run_cmd", mock_run):
            results = filt.create_internal_request(
                "b64data",
                "origin",
                "secret",
                "uid",
                "oci-storage",
                "",
                "git-url",
                "rev",
                True,
            )
        assert results["result"] == "Success"

    def test_unparseable_name(self) -> None:
        """Raise when IR name cannot be parsed."""
        mock_run = MagicMock(
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="unexpected output\n",
            )
        )
        with (
            patch.object(filt.subprocess_cmd, "run_cmd", mock_run),
            pytest.raises(RuntimeError, match="Could not parse"),
        ):
            filt.create_internal_request(
                "b64data",
                "origin",
                "secret",
                "uid",
                "oci-storage",
                "",
                "git-url",
                "rev",
                True,
            )

    def test_empty_results(self) -> None:
        """Raise when no results in IR status."""
        call_count = 0

        def mock_run(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if cmd[0] == "internal-request":
                return subprocess.CompletedProcess(
                    args=cmd,
                    returncode=0,
                    stdout="ir/'my-ir' created\n",
                )
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", MagicMock(side_effect=mock_run)),
            pytest.raises(RuntimeError, match="No results found"),
        ):
            filt.create_internal_request(
                "b64data",
                "origin",
                "secret",
                "uid",
                "oci-storage",
                "",
                "git-url",
                "rev",
                True,
            )

    def test_failed_result(self) -> None:
        """Raise when IR result is not Success."""
        mock_run = _mock_run_cmd_for_oras_and_rpm(
            [],
            ir_results={"result": "Failure"},
        )
        with (
            patch.object(filt.subprocess_cmd, "run_cmd", mock_run),
            pytest.raises(RuntimeError, match="Filtering failed"),
        ):
            filt.create_internal_request(
                "b64data",
                "origin",
                "secret",
                "uid",
                "oci-storage",
                "",
                "git-url",
                "rev",
                True,
            )


class TestPullFilterResults:
    """Test filter results artifact pulling and extraction."""

    def test_success(self, tmp_path: Path) -> None:
        """Extract in_advisory and unreleased from tarball."""
        tarball = _make_filter_tarball(
            tmp_path,
            unreleased=[{"name": "comp1"}],
            in_advisory=[{"name": "comp2"}],
        )

        def mock_run(cmd, **kwargs):
            if cmd[0] == "select-oci-auth":
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="auth")
            if cmd[0] == "oras" and "pull" in cmd:
                cwd = kwargs.get("cwd")
                out_dir = Path(cwd) if cwd else Path(".")
                out_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(tarball, out_dir / "filter-results.tar.gz")
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")

        with patch.object(filt.subprocess_cmd, "run_cmd", MagicMock(side_effect=mock_run)):
            in_adv, unreleased = filt.pull_filter_results(
                "oci:quay.io/mock/results@sha256:abc",
            )
        assert len(in_adv) == 1
        assert len(unreleased) == 1

    def test_missing_unreleased_file(self, tmp_path: Path) -> None:
        """Raise when unreleased_rpms.json is missing."""
        src_dir = tmp_path / "tar_src"
        src_dir.mkdir()
        _write_json(src_dir / "in_advisory_rpms.json", [])
        tarball = tmp_path / "filter-results.tar.gz"
        with tarfile.open(tarball, "w:gz") as tf:
            tf.add(src_dir / "in_advisory_rpms.json", arcname="in_advisory_rpms.json")

        def mock_run(cmd, **kwargs):
            if cmd[0] == "select-oci-auth":
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="auth")
            if cmd[0] == "oras" and "pull" in cmd:
                cwd = kwargs.get("cwd")
                out_dir = Path(cwd) if cwd else Path(".")
                out_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(tarball, out_dir / "filter-results.tar.gz")
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", MagicMock(side_effect=mock_run)),
            pytest.raises(RuntimeError, match="No unreleased_rpms.json"),
        ):
            filt.pull_filter_results("oci:quay.io/mock/results@sha256:abc")


class TestValidatePulpDigests:
    """Test Pulp digest validation loop."""

    def _rpm_entry(self) -> dict:
        return {
            "rpmname": "hello",
            "epoch": "0",
            "version": "1.0",
            "release": "1.el9",
            "arch": "x86_64",
            "sha256": "abc",
            "repository_name": "myrepo",
        }

    def test_all_match(self) -> None:
        """No error when all digests match."""
        pulp = MagicMock(spec=filt.PulpClient)
        pulp.check_digest.return_value = filt.PulpDigestStatus.MATCH
        filt.validate_pulp_digests([self._rpm_entry()], pulp)

    def test_mismatch_raises(self) -> None:
        """Raise on digest mismatch."""
        pulp = MagicMock(spec=filt.PulpClient)
        pulp.check_digest.return_value = filt.PulpDigestStatus.MISMATCH
        with pytest.raises(RuntimeError, match="Cannot rebuild RPM"):
            filt.validate_pulp_digests([self._rpm_entry()], pulp)

    def test_error_raises(self) -> None:
        """Pulp API error propagates as RuntimeError with context."""
        pulp = MagicMock(spec=filt.PulpClient)
        pulp.check_digest.side_effect = requests.ConnectionError("timeout")
        with pytest.raises(RuntimeError, match="Pulp API error"):
            filt.validate_pulp_digests([self._rpm_entry()], pulp)

    def test_not_found_is_ok(self) -> None:
        """NOT_FOUND is acceptable (advisory is authoritative)."""
        pulp = MagicMock(spec=filt.PulpClient)
        pulp.check_digest.return_value = filt.PulpDigestStatus.NOT_FOUND
        filt.validate_pulp_digests([self._rpm_entry()], pulp)


class TestFilterSnapshot:
    """Test snapshot filtering."""

    def test_keeps_unreleased_components(self) -> None:
        """Only components with unreleased RPMs are kept."""
        snapshot = _snapshot(
            [
                {"name": "comp1", "containerImage": "img1"},
                {"name": "comp2", "containerImage": "img2"},
            ]
        )
        unreleased = [{"name": "comp1"}]
        rpms_map = {"comp1": [{"rpm": "hello.rpm"}]}

        result = filt.filter_snapshot(snapshot, unreleased, rpms_map)
        assert len(result["components"]) == 1
        assert result["components"][0]["name"] == "comp1"
        assert result["components"][0]["rpmsToPublish"] == [{"rpm": "hello.rpm"}]

    def test_empty_unreleased(self) -> None:
        """No unreleased RPMs results in empty components."""
        snapshot = _snapshot([{"name": "comp1"}])
        result = filt.filter_snapshot(snapshot, [], {})
        assert result["components"] == []

    def test_missing_component_skipped(self) -> None:
        """Unreleased name not in snapshot is skipped."""
        snapshot = _snapshot([{"name": "comp1"}])
        unreleased = [{"name": "missing"}]
        result = filt.filter_snapshot(snapshot, unreleased, {})
        assert result["components"] == []

    def test_no_rpms_in_map_skipped(self) -> None:
        """Component in unreleased but not in rpms_map is skipped."""
        snapshot = _snapshot([{"name": "comp1"}])
        unreleased = [{"name": "comp1"}]
        result = filt.filter_snapshot(snapshot, unreleased, {})
        assert result["components"] == []


class TestRun:
    """Test the run() orchestration."""

    def test_no_rpms_skip_release(self, tmp_path: Path) -> None:
        """Skip release when no RPMs are found."""
        cfg, res = _make_config_and_results(
            tmp_path,
            snapshot=_snapshot([{"containerImage": "quay.io/t/i@sha256:a", "name": "c1"}]),
            data=_data(rpm_repos=_rpm_repos()),
        )
        mock_run = _mock_run_cmd_for_oras_and_rpm(rpm_files=[])

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", mock_run),
            patch.object(filt.file_helper, "sha256", return_value="sha"),
        ):
            filt.run(cfg, res)

        assert res.skip_release.read_text(encoding="utf-8") == "true"
        snap = json.loads(cfg.snapshot_file.read_text(encoding="utf-8"))
        assert snap["components"] == []

    def test_all_released_skip(self, tmp_path: Path) -> None:
        """Skip release when all RPMs are already released."""
        tarball = _make_filter_tarball(tmp_path, unreleased=[], in_advisory=[])
        cfg, res = _make_config_and_results(
            tmp_path,
            snapshot=_snapshot([{"containerImage": "quay.io/t/i@sha256:a", "name": "c1"}]),
            data=_data(rpm_repos=_rpm_repos()),
        )
        ir_results = {
            "result": "Success",
            "filter_results_artifact": "oci:quay.io/r@sha256:abc",
            "advisory_url": "https://adv.example.com",
            "advisory_internal_url": "https://int.example.com",
        }
        mock_run = _mock_run_cmd_for_oras_and_rpm(
            ["hello-1.0-1.el9.x86_64.rpm"],
            ir_results=ir_results,
            filter_tarball=tarball,
        )

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", mock_run),
            patch.object(filt.file_helper, "sha256", return_value="sha"),
            patch.object(filt, "make_pulp_client", return_value=MagicMock()),
        ):
            filt.run(cfg, res)

        assert res.skip_release.read_text(encoding="utf-8") == "true"
        assert res.latest_advisory_url.read_text(encoding="utf-8") == "https://adv.example.com"

    def test_unreleased_rpms_filter(self, tmp_path: Path) -> None:
        """Unreleased RPMs produce a filtered snapshot."""
        tarball = _make_filter_tarball(
            tmp_path,
            unreleased=[{"name": "c1"}],
            in_advisory=[],
        )
        cfg, res = _make_config_and_results(
            tmp_path,
            snapshot=_snapshot([{"containerImage": "quay.io/t/i@sha256:a", "name": "c1"}]),
            data=_data(rpm_repos=_rpm_repos()),
        )
        mock_run = _mock_run_cmd_for_oras_and_rpm(
            ["hello-1.0-1.el9.x86_64.rpm"],
            filter_tarball=tarball,
        )

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", mock_run),
            patch.object(filt.file_helper, "sha256", return_value="sha"),
            patch.object(filt, "make_pulp_client", return_value=MagicMock()),
        ):
            filt.run(cfg, res)

        assert res.skip_release.read_text(encoding="utf-8") == "false"
        snap = json.loads(cfg.snapshot_file.read_text(encoding="utf-8"))
        assert len(snap["components"]) == 1
        assert "rpmsToPublish" in snap["components"][0]

    def test_in_advisory_rpms_validated(self, tmp_path: Path) -> None:
        """In-advisory RPMs trigger Pulp digest validation."""
        in_advisory = [
            {
                "name": "c1",
                "rpmname": "hello",
                "epoch": "0",
                "version": "1.0",
                "release": "1.el9",
                "arch": "x86_64",
                "sha256": "sha",
                "repository_name": "x86_64",
            }
        ]
        tarball = _make_filter_tarball(
            tmp_path,
            unreleased=[{"name": "c1"}],
            in_advisory=in_advisory,
        )
        cfg, res = _make_config_and_results(
            tmp_path,
            snapshot=_snapshot([{"containerImage": "quay.io/t/i@sha256:a", "name": "c1"}]),
            data=_data(rpm_repos=_rpm_repos()),
        )
        mock_run = _mock_run_cmd_for_oras_and_rpm(
            ["hello-1.0-1.el9.x86_64.rpm"],
            filter_tarball=tarball,
        )

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", mock_run),
            patch.object(filt.file_helper, "sha256", return_value="sha"),
            patch.object(filt, "make_pulp_client", return_value=MagicMock()),
            patch.object(filt, "validate_pulp_digests") as mock_validate,
        ):
            filt.run(cfg, res)

        mock_validate.assert_called_once()
        assert mock_validate.call_args[0][0] == in_advisory

    def test_in_advisory_all_released_skip(self, tmp_path: Path) -> None:
        """Validate digests then skip when in-advisory but unreleased is empty."""
        in_advisory = [
            {
                "name": "c1",
                "rpmname": "hello",
                "epoch": "0",
                "version": "1.0",
                "release": "1.el9",
                "arch": "x86_64",
                "sha256": "sha",
                "repository_name": "x86_64",
            }
        ]
        tarball = _make_filter_tarball(
            tmp_path,
            unreleased=[],
            in_advisory=in_advisory,
        )
        cfg, res = _make_config_and_results(
            tmp_path,
            snapshot=_snapshot([{"containerImage": "quay.io/t/i@sha256:a", "name": "c1"}]),
            data=_data(rpm_repos=_rpm_repos()),
        )
        ir_results = {
            "result": "Success",
            "filter_results_artifact": "oci:quay.io/r@sha256:abc",
            "advisory_url": "https://adv.example.com",
            "advisory_internal_url": "https://int.example.com",
        }
        mock_run = _mock_run_cmd_for_oras_and_rpm(
            ["hello-1.0-1.el9.x86_64.rpm"],
            ir_results=ir_results,
            filter_tarball=tarball,
        )

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", mock_run),
            patch.object(filt.file_helper, "sha256", return_value="sha"),
            patch.object(filt, "make_pulp_client", return_value=MagicMock()),
            patch.object(filt, "validate_pulp_digests") as mock_validate,
        ):
            filt.run(cfg, res)

        mock_validate.assert_called_once()
        assert mock_validate.call_args[0][0] == in_advisory
        assert res.skip_release.read_text(encoding="utf-8") == "true"
        assert res.latest_advisory_url.read_text(encoding="utf-8") == "https://adv.example.com"
        assert (
            res.latest_advisory_internal_url.read_text(encoding="utf-8")
            == "https://int.example.com"
        )
        snap = json.loads(cfg.snapshot_file.read_text(encoding="utf-8"))
        assert snap["components"] == []

    def test_missing_snapshot_raises(self, tmp_path: Path) -> None:
        """Raise when snapshot file is missing."""
        cfg, res = _make_config_and_results(tmp_path)
        cfg = dataclasses.replace(cfg, snapshot_file=tmp_path / "missing.json")
        with pytest.raises(FileNotFoundError):
            filt.run(cfg, res)

    def test_missing_data_raises(self, tmp_path: Path) -> None:
        """Raise when data file is missing."""
        cfg, res = _make_config_and_results(tmp_path)
        cfg = dataclasses.replace(cfg, data_file=tmp_path / "missing.json")
        with pytest.raises(FileNotFoundError):
            filt.run(cfg, res)

    def test_missing_rpa_raises(self, tmp_path: Path) -> None:
        """Raise when RPA file is missing."""
        cfg, res = _make_config_and_results(tmp_path)
        cfg = dataclasses.replace(cfg, rpa_file=tmp_path / "missing.json")
        with pytest.raises(FileNotFoundError):
            filt.run(cfg, res)

    def test_missing_origin_key_raises(self, tmp_path: Path) -> None:
        """Raise when origin key is absent from RPA."""
        cfg, res = _make_config_and_results(tmp_path, rpa={"spec": {}})
        with pytest.raises(KeyError):
            filt.run(cfg, res)

    def test_empty_origin_raises(self, tmp_path: Path) -> None:
        """Raise when origin is empty in RPA."""
        cfg, res = _make_config_and_results(tmp_path, rpa={"spec": {"origin": ""}})
        with pytest.raises(ValueError, match="origin.*empty"):
            filt.run(cfg, res)

    def test_empty_toml_raises(self, tmp_path: Path) -> None:
        """Raise when cli.toml content is empty."""
        cfg, res = _make_config_and_results(tmp_path)
        empty_toml = tmp_path / "empty.toml"
        empty_toml.write_text("", encoding="utf-8")
        cfg = dataclasses.replace(cfg, pulp_config_file=empty_toml)
        with pytest.raises(RuntimeError, match="Missing cli.toml"):
            filt.run(cfg, res)

    def test_missing_base_url_raises(self, tmp_path: Path) -> None:
        """Raise when base_url is missing in cli.toml."""
        cfg, res = _make_config_and_results(
            tmp_path,
            toml='[cli]\nusername = "u"\npassword = "p"\n',
        )
        with pytest.raises(RuntimeError, match="Missing required.*base_url"):
            filt.load_and_validate(cfg)

    def test_staging_environment(self, tmp_path: Path) -> None:
        """Staging intention sets environment to 'stage'."""
        cfg, res = _make_config_and_results(
            tmp_path,
            data=_data(intention="staging"),
            snapshot=_snapshot([]),
        )
        filt.run(cfg, res)
        assert res.environment.read_text(encoding="utf-8") == "stage"

    def test_pulp_digest_mismatch_propagates(self, tmp_path: Path) -> None:
        """Pulp digest mismatch in validate_pulp_digests propagates from run()."""
        in_advisory = [
            {
                "name": "c1",
                "rpmname": "hello",
                "epoch": "0",
                "version": "1.0",
                "release": "1.el9",
                "arch": "x86_64",
                "sha256": "sha",
                "repository_name": "x86_64",
            }
        ]
        tarball = _make_filter_tarball(
            tmp_path,
            unreleased=[{"name": "c1"}],
            in_advisory=in_advisory,
        )
        cfg, res = _make_config_and_results(
            tmp_path,
            snapshot=_snapshot([{"containerImage": "quay.io/t/i@sha256:a", "name": "c1"}]),
            data=_data(rpm_repos=_rpm_repos()),
        )
        mock_run = _mock_run_cmd_for_oras_and_rpm(
            ["hello-1.0-1.el9.x86_64.rpm"],
            filter_tarball=tarball,
        )

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", mock_run),
            patch.object(filt.file_helper, "sha256", return_value="sha"),
            patch.object(filt, "make_pulp_client", return_value=MagicMock()),
            patch.object(
                filt,
                "validate_pulp_digests",
                side_effect=RuntimeError("Cannot rebuild RPM"),
            ),
            pytest.raises(RuntimeError, match="Cannot rebuild RPM"),
        ):
            filt.run(cfg, res)

    def test_no_filter_artifact_raises(self, tmp_path: Path) -> None:
        """Raise when filter_results_artifact is missing."""
        cfg, res = _make_config_and_results(
            tmp_path,
            snapshot=_snapshot([{"containerImage": "quay.io/t/i@sha256:a", "name": "c1"}]),
            data=_data(rpm_repos=_rpm_repos()),
        )
        ir_results = {"result": "Success", "filter_results_artifact": ""}
        mock_run = _mock_run_cmd_for_oras_and_rpm(
            ["hello-1.0-1.el9.x86_64.rpm"],
            ir_results=ir_results,
        )

        with (
            patch.object(filt.subprocess_cmd, "run_cmd", mock_run),
            patch.object(filt.file_helper, "sha256", return_value="sha"),
            pytest.raises(RuntimeError, match="No filter_results_artifact"),
        ):
            filt.run(cfg, res)


class TestMakePulpClient:
    """Test PulpClient construction."""

    def test_builds_client_with_basic_auth(self, tmp_path: Path) -> None:
        """Build a PulpClient using basic auth credentials."""
        pulp_config = {
            "base_url": "https://pulp.example.com",
            "username": "user",
            "password": "pass",
            "client_id": "",
            "client_secret": "",
        }
        ctx = filt.LoadedContext(
            snapshot={},
            data={},
            origin="test",
            pulp_config=pulp_config,
            base_url="https://pulp.example.com",
            environment="production",
            advisory_secret_name="secret",
        )

        mock_session = MagicMock()
        with (
            patch.object(
                filt.http_client, "get_retry_session", return_value=mock_session
            ) as mock_get_session,
            patch("filter_already_released_advisory_rpms.PulpAuth") as mock_auth_cls,
        ):
            client = filt.make_pulp_client(ctx, "test-domain")

        mock_get_session.assert_called_once_with(
            total=3,
            connect=3,
            read=3,
            status=2,
            backoff_factor=0.4,
            allowed_methods=frozenset({"GET", "POST"}),
        )
        mock_auth_cls.assert_called_once_with(pulp_config)
        assert mock_session.auth == mock_auth_cls.return_value
        assert isinstance(client, filt.PulpClient)


class TestMain:
    """Test the CLI entry point."""

    def test_success(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Return 0 on success with all env vars set."""
        snap_file, data_file, rpa_file, toml_file = _setup_base_files(
            tmp_path,
            snapshot=_snapshot([]),
        )
        res = _result_paths(tmp_path)

        monkeypatch.setenv("SNAPSHOT_FILE", str(snap_file))
        monkeypatch.setenv("DATA_FILE", str(data_file))
        monkeypatch.setenv("RPA_FILE", str(rpa_file))
        monkeypatch.setenv("PULP_CONFIG_FILE", str(toml_file))
        monkeypatch.setenv("PULP_DOMAIN", "dom")
        monkeypatch.setenv("PIPELINE_RUN_UID", "uid")
        monkeypatch.setenv("TASK_GIT_URL", "git-url")
        monkeypatch.setenv("TASK_GIT_REVISION", "rev")
        monkeypatch.setenv("RESULT_SKIP_RELEASE", str(res.skip_release))
        monkeypatch.setenv("RESULT_ENVIRONMENT", str(res.environment))
        monkeypatch.setenv("RESULT_LATEST_ADVISORY_URL", str(res.latest_advisory_url))
        monkeypatch.setenv(
            "RESULT_LATEST_ADVISORY_INTERNAL_URL", str(res.latest_advisory_internal_url)
        )

        assert filt.main() == 0
        assert res.skip_release.read_text(encoding="utf-8") == "true"

    def test_missing_env_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """SystemExit when required env var is missing."""
        monkeypatch.delenv("SNAPSHOT_FILE", raising=False)
        monkeypatch.delenv("DATA_FILE", raising=False)
        monkeypatch.delenv("RPA_FILE", raising=False)
        monkeypatch.delenv("PULP_CONFIG_FILE", raising=False)
        monkeypatch.delenv("PULP_DOMAIN", raising=False)
        monkeypatch.delenv("PIPELINE_RUN_UID", raising=False)
        monkeypatch.delenv("TASK_GIT_URL", raising=False)
        monkeypatch.delenv("TASK_GIT_REVISION", raising=False)
        monkeypatch.delenv("RESULT_SKIP_RELEASE", raising=False)
        monkeypatch.delenv("RESULT_ENVIRONMENT", raising=False)
        monkeypatch.delenv("RESULT_LATEST_ADVISORY_URL", raising=False)
        monkeypatch.delenv("RESULT_LATEST_ADVISORY_INTERNAL_URL", raising=False)

        with pytest.raises(SystemExit):
            filt.main()

    def test_dunder_main_block(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Exercise the ``if __name__ == "__main__"`` block."""
        snap_file, data_file, rpa_file, toml_file = _setup_base_files(
            tmp_path,
            snapshot=_snapshot([]),
        )
        res = _result_paths(tmp_path)

        monkeypatch.setenv("SNAPSHOT_FILE", str(snap_file))
        monkeypatch.setenv("DATA_FILE", str(data_file))
        monkeypatch.setenv("RPA_FILE", str(rpa_file))
        monkeypatch.setenv("PULP_CONFIG_FILE", str(toml_file))
        monkeypatch.setenv("PULP_DOMAIN", "dom")
        monkeypatch.setenv("PIPELINE_RUN_UID", "uid")
        monkeypatch.setenv("TASK_GIT_URL", "git-url")
        monkeypatch.setenv("TASK_GIT_REVISION", "rev")
        monkeypatch.setenv("RESULT_SKIP_RELEASE", str(res.skip_release))
        monkeypatch.setenv("RESULT_ENVIRONMENT", str(res.environment))
        monkeypatch.setenv("RESULT_LATEST_ADVISORY_URL", str(res.latest_advisory_url))
        monkeypatch.setenv(
            "RESULT_LATEST_ADVISORY_INTERNAL_URL", str(res.latest_advisory_internal_url)
        )

        with pytest.raises(SystemExit) as exc_info:
            import runpy

            runpy.run_module(
                "filter_already_released_advisory_rpms",
                run_name="__main__",
            )
        assert exc_info.value.code == 0
