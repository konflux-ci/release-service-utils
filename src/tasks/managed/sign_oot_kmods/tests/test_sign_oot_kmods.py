"""Tests for sign_oot_kmods."""

from __future__ import annotations

import hashlib
import io
import logging
import subprocess
import tarfile
from pathlib import Path
from unittest import mock

import pytest

from release_service_utils.tasks.managed.sign_oot_kmods import sign_oot_kmods as sign

SIGN_KEY = "super-secret-key"
TASK_UID = "task-uid-123"
SIGNED_PATH = "signed-kmods"


class Remote:
    """Stand-in for GSSAPI ssh and scp."""

    def __init__(self) -> None:
        """Record calls and control download behavior."""
        self.calls: list[tuple[list[str], str | None]] = []
        self.tarballs: dict[str, bytes] = {}
        self.fail_on_stdin = False
        self.empty_download = False

    def __call__(
        self,
        argv: list[str],
        *,
        stdin: str | None = None,
        max_attempts: int = 3,
    ) -> subprocess.CompletedProcess[str]:
        """Handle one ssh or scp invocation."""
        command = [str(part) for part in argv]
        self.calls.append((command, stdin))
        program = command[0]
        if program == "ssh":
            if stdin is not None and self.fail_on_stdin:
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if program == "scp":
            positional = _positional(command)
            source, destination = positional[-2], positional[-1]
            if "@" in destination:
                name = Path(destination.split(":", 1)[1]).name
                self.tarballs[name] = Path(source).read_bytes()
            else:
                name = Path(source.split(":", 1)[1]).name
                upload_name = name.replace("signed_", "kmods_", 1)
                _write_download(
                    destination,
                    self.tarballs.get(upload_name),
                    empty=self.empty_download,
                )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        raise AssertionError(program)


def _positional(argv: list[str]) -> list[str]:
    """Drop ``-o value`` pairs from an ssh or scp argument list."""
    positional: list[str] = []
    index = 1
    while index < len(argv):
        if argv[index] == "-o":
            index += 2
            continue
        positional.append(argv[index])
        index += 1
    return positional


def _write_download(destination: str, payload: bytes | None, *, empty: bool) -> None:
    """Write a signed or empty tarball to *destination*."""
    if empty or payload is None:
        with tarfile.open(destination, "w:gz"):
            return
    with (
        tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as source,
        tarfile.open(destination, "w:gz") as output,
    ):
        for member in source.getmembers():
            if not member.isfile():
                continue
            extracted = source.extractfile(member)
            signed = b"SIGNED:" + (extracted.read() if extracted else b"")
            member.size = len(signed)
            output.addfile(member, io.BytesIO(signed))


def _patch_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Point secret and temporary paths at *tmp_path*."""
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    (secrets / "signKey").write_text(f"{SIGN_KEY}\n", encoding="utf-8")
    (secrets / "signHost").write_text("sign.example\n", encoding="utf-8")
    (secrets / "signUser").write_text("signer\n", encoding="utf-8")
    (tmp_path / "keytab").write_bytes(b"keytab")
    (tmp_path / "fingerprint").write_text("ssh-rsa AAAA host\n", encoding="utf-8")
    monkeypatch.setenv("SIGNING_SECRET_DIR", str(secrets))
    monkeypatch.setenv("KEYTAB_PATH", str(tmp_path / "keytab"))
    monkeypatch.setenv("FINGERPRINT_PATH", str(tmp_path / "fingerprint"))
    monkeypatch.setenv("SSH_DIR", str(tmp_path / "ssh"))


def _run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    remote: Remote,
    *,
    data_dir: Path | None = None,
    runner: object | None = None,
) -> mock.MagicMock:
    """Run signing with Kerberos and SSH patched."""
    _patch_paths(monkeypatch, tmp_path)
    if data_dir is None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()
    with (
        mock.patch.object(sign.authentication, "kinit_with_retry") as kinit,
        mock.patch.object(sign.gssapi_ssh, "run_ssh", side_effect=runner or remote),
    ):
        sign.run(
            data_dir=data_dir,
            signed_kmods_path=SIGNED_PATH,
            kerberos_realm="IPA.REDHAT.COM",
            signing_author="The dummy signer",
            task_run_uid=TASK_UID,
        )
    return kinit


def _ssh_commands(remote: Remote) -> list[str]:
    """Return remote command strings passed to ssh."""
    return [call[0][-1] for call in remote.calls if call[0][0] == "ssh"]


def _scp_remote_paths(remote: Remote) -> list[str]:
    """Return ``host:path`` arguments from scp calls."""
    paths: list[str] = []
    for command, _stdin in remote.calls:
        if command[0] != "scp":
            continue
        for argument in command:
            if "@" in argument and ":" in argument:
                paths.append(argument.split(":", 1)[1])
    return paths


def _write_module(directory: Path, relative: str, content: bytes = b"MODULE") -> Path:
    """Create one ``.ko`` file under *directory* and return it."""
    path = directory / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _arch_dir(
    data_dir: Path,
    arch: str,
    modules: dict[str, bytes],
    *,
    envfile: str | None = None,
) -> Path:
    """Build ``data_dir / signed-kmods / arch`` with the given modules."""
    directory = data_dir / SIGNED_PATH / arch
    for relative, content in modules.items():
        _write_module(directory, relative, content)
    if envfile is not None:
        (directory / "envfile").write_text(envfile, encoding="utf-8")
    return directory


def _unpack_archive(data_dir: Path) -> Path:
    """Extract ``signed-kmods.tar.gz`` and return the restored tree."""
    archive = data_dir / sign.ARCHIVE_NAME
    source = data_dir / SIGNED_PATH
    assert archive.is_file()
    assert not source.exists()
    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(data_dir, filter="data")
    return source


def _assert_no_archive(data_dir: Path) -> None:
    """Signing failures must not create the packaged archive."""
    assert not (data_dir / sign.ARCHIVE_NAME).exists()


def _assert_signed(arch_dir: Path, expected: dict[str, bytes]) -> None:
    """Assert modules were replaced with signed payloads and checksums match."""
    checksum = (arch_dir / f"signed_kmods_checksums_{arch_dir.name}.txt").read_text(
        encoding="utf-8"
    )
    for relative, original in expected.items():
        payload = (arch_dir / relative).read_bytes()
        assert payload == b"SIGNED:" + original
        digest = hashlib.sha256(payload).hexdigest()
        assert f"{digest}  ./{relative}" in checksum


def _assert_remote_arches(remote: Remote, *arches: str) -> None:
    """Assert each architecture used a distinct remote directory and tarball."""
    commands = _ssh_commands(remote)
    joined = [" ".join(call[0]) for call in remote.calls]
    scp_paths = _scp_remote_paths(remote)
    for arch in arches:
        assert any(f"kmods/{arch}" in command for command in commands)
        assert any(f"kmods_{arch}.tar.gz" in text for text in joined)
        assert any(f"signed_{arch}.tar.gz" in text for text in joined)
    assert scp_paths
    assert all(path.startswith(f"~/{TASK_UID}/") for path in scp_paths)
    assert all("$HOME" not in path for path in scp_paths)


def test_ko_files_ignores_missing_directory_and_non_files(tmp_path: Path) -> None:
    """Only regular files named ``*.ko`` are returned."""
    assert sign.ko_files(tmp_path / "missing") == []
    (tmp_path / "weird.ko").mkdir()
    module = tmp_path / "real.ko"
    module.write_bytes(b"data")
    assert sign.ko_files(tmp_path) == [module]


def test_build_remote_signing_script_quotes_key_and_does_not_trace() -> None:
    """The remote script fails closed and does not enable shell tracing."""
    script = sign.build_remote_signing_script(
        sign_key="key'quoted",
        arch_label="x86_64",
        signing_author="Ann Author",
        remote_dir='"$HOME"/uid/kmods',
    )
    assert "set -x" not in script
    assert "set -euo pipefail" in script
    assert "rh-signing-client" in script
    assert "key'quoted" not in script
    assert "exit 1" in script
    assert "found=$((found + 1))" in script
    assert "Successfully signed $found .ko files" in script


def test_write_checksums_empty_directory(tmp_path: Path) -> None:
    """An empty directory produces an empty checksum file."""
    destination = sign.write_checksums(tmp_path, "signed_kmods_checksums.txt")
    assert destination.read_text(encoding="utf-8") == ""


def test_tarball_round_trip_preserves_relative_paths(tmp_path: Path) -> None:
    """Packed modules extract back to the same relative paths."""
    _write_module(tmp_path, "drivers/nested.ko", b"nested")
    tarball = tmp_path / "mods.tar.gz"
    sign.write_ko_tarball(tmp_path, tarball)
    restored = tmp_path / "restored"
    restored.mkdir()
    sign.extract_tarball(tarball, restored)
    assert (restored / "drivers" / "nested.ko").read_bytes() == b"nested"


def test_one_architecture_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A single architecture subdirectory is signed and packed."""
    data_dir = tmp_path / "data"
    modules = {"mod.ko": b"MODULE", "drivers/nested.ko": b"NESTED"}
    arch = _arch_dir(data_dir, "x86_64", modules, envfile="ARCH=x86_64\n")
    (arch / "weird.ko").mkdir()
    remote = Remote()

    with caplog.at_level(logging.DEBUG, logger="release"):
        kinit = _run(tmp_path, monkeypatch, remote, data_dir=data_dir)

    root = _unpack_archive(data_dir)
    arch = root / "x86_64"
    assert kinit.call_args.args[0] == "signer@IPA.REDHAT.COM"
    assert kinit.call_args.args[1] == tmp_path / "keytab"
    _assert_signed(arch, modules)
    assert (arch / "envfile").read_text(encoding="utf-8") == "ARCH=x86_64\n"
    assert not (root / "signing_summary.txt").exists()
    _assert_remote_arches(remote, "x86_64")
    assert any('"$HOME"/' in command for command in _ssh_commands(remote))
    assert any("GSSAPIAuthentication=yes" in " ".join(call[0]) for call in remote.calls)
    assert SIGN_KEY not in caplog.text
    stdin_scripts = [call[1] for call in remote.calls if call[1]]
    assert stdin_scripts
    assert SIGN_KEY in stdin_scripts[0]
    assert "set -x" not in stdin_scripts[0]


def test_two_architecture_directories_without_arch_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two architecture subdirectories are signed without an arch_count.txt sidecar."""
    data_dir = tmp_path / "data"
    _arch_dir(data_dir, "aarch64", {"mod.ko": b"ARM"}, envfile="ARCH=aarch64\n")
    _arch_dir(data_dir, "x86_64", {"mod.ko": b"X86"}, envfile="ARCH=x86_64\n")
    assert not (data_dir / "arch_count.txt").exists()
    remote = Remote()

    _run(tmp_path, monkeypatch, remote, data_dir=data_dir)

    root = _unpack_archive(data_dir)
    arm = root / "aarch64"
    x86 = root / "x86_64"
    _assert_signed(arm, {"mod.ko": b"ARM"})
    _assert_signed(x86, {"mod.ko": b"X86"})
    assert (arm / "envfile").read_text(encoding="utf-8") == "ARCH=aarch64\n"
    assert (x86 / "envfile").read_text(encoding="utf-8") == "ARCH=x86_64\n"
    summary = (root / "signing_summary.txt").read_text(encoding="utf-8")
    assert "Total architectures processed: 2" in summary
    assert "aarch64: 1 signed .ko files" in summary
    assert "x86_64: 1 signed .ko files" in summary
    _assert_remote_arches(remote, "aarch64", "x86_64")


def test_empty_architecture_directory_is_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty architecture directory is skipped; others are signed and summarized."""
    data_dir = tmp_path / "data"
    root = data_dir / SIGNED_PATH
    root.mkdir(parents=True)
    (root / "notes.txt").write_text("ignore", encoding="utf-8")
    empty = root / "aarch64"
    empty.mkdir()
    _arch_dir(data_dir, "x86_64", {"mod.ko": b"MODULE"}, envfile="KEEP\n")
    remote = Remote()

    _run(tmp_path, monkeypatch, remote, data_dir=data_dir)

    root = _unpack_archive(data_dir)
    filled = root / "x86_64"
    empty = root / "aarch64"
    _assert_signed(filled, {"mod.ko": b"MODULE"})
    assert (filled / "envfile").read_text(encoding="utf-8") == "KEEP\n"
    assert not (empty / "signed_kmods_checksums_aarch64.txt").exists()
    summary = (root / "signing_summary.txt").read_text(encoding="utf-8")
    assert "Total architectures processed: 2" in summary
    assert "aarch64: 0 signed .ko files" in summary
    assert "x86_64: 1 signed .ko files" in summary
    _assert_remote_arches(remote, "x86_64")
    assert not any("kmods/aarch64" in command for command in _ssh_commands(remote))


def test_leftover_arch_count_file_is_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stale arch_count.txt does not override the directory layout."""
    data_dir = tmp_path / "data"
    _arch_dir(data_dir, "aarch64", {"mod.ko": b"ARM"})
    _arch_dir(data_dir, "x86_64", {"mod.ko": b"X86"})
    (data_dir / "arch_count.txt").write_text("1\n", encoding="utf-8")
    remote = Remote()

    _run(tmp_path, monkeypatch, remote, data_dir=data_dir)

    root = _unpack_archive(data_dir)
    arm = root / "aarch64"
    x86 = root / "x86_64"
    _assert_signed(arm, {"mod.ko": b"ARM"})
    _assert_signed(x86, {"mod.ko": b"X86"})
    summary = (root / "signing_summary.txt").read_text(encoding="utf-8")
    assert "Total architectures processed: 2" in summary
    _assert_remote_arches(remote, "aarch64", "x86_64")


def test_no_architecture_directories_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Signing fails when the signed tree has no architecture subdirectory."""
    data_dir = tmp_path / "data"
    (data_dir / SIGNED_PATH).mkdir(parents=True)
    remote = Remote()
    with pytest.raises(RuntimeError, match="No architecture directories found"):
        _run(tmp_path, monkeypatch, remote, data_dir=data_dir)
    assert any("rm -rf" in command for command in _ssh_commands(remote))
    _assert_no_archive(data_dir)


def test_missing_signed_kmods_path_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Signing fails when the signed-kmods path is not a directory."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    remote = Remote()
    with pytest.raises(RuntimeError, match="No architecture directories found"):
        _run(tmp_path, monkeypatch, remote, data_dir=data_dir)
    assert any("rm -rf" in command for command in _ssh_commands(remote))
    _assert_no_archive(data_dir)


def test_all_empty_architecture_directories_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Signing fails when every architecture directory has no ``.ko`` files."""
    data_dir = tmp_path / "data"
    empty = data_dir / SIGNED_PATH / "x86_64"
    empty.mkdir(parents=True)
    remote = Remote()
    with pytest.raises(RuntimeError, match="No .ko files found after signing process"):
        _run(tmp_path, monkeypatch, remote, data_dir=data_dir)
    assert not (empty / "signed_kmods_checksums_x86_64.txt").exists()
    assert any("rm -rf" in command for command in _ssh_commands(remote))
    assert not any("kmods/x86_64" in command for command in _ssh_commands(remote))
    _assert_no_archive(data_dir)


def test_modules_at_tree_root_are_not_an_architecture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``.ko`` files at the tree root do not count as an architecture directory."""
    data_dir = tmp_path / "data"
    root = data_dir / SIGNED_PATH
    _write_module(root, "mod.ko", b"MODULE")
    (root / "envfile").write_text("ROOT\n", encoding="utf-8")
    remote = Remote()

    with pytest.raises(RuntimeError, match="No architecture directories found"):
        _run(tmp_path, monkeypatch, remote, data_dir=data_dir)

    assert (root / "mod.ko").read_bytes() == b"MODULE"
    assert any("rm -rf" in command for command in _ssh_commands(remote))
    _assert_no_archive(data_dir)


def test_empty_copy_back_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty signed tarball fails and leaves the unsigned modules in place."""
    data_dir = tmp_path / "data"
    arch = _arch_dir(data_dir, "x86_64", {"mod.ko": b"MODULE"})
    remote = Remote()
    remote.empty_download = True
    with pytest.raises(RuntimeError, match="No signed .ko files copied back for x86_64"):
        _run(tmp_path, monkeypatch, remote, data_dir=data_dir)
    assert (arch / "mod.ko").read_bytes() == b"MODULE"
    assert not (arch / "signed_kmods_checksums_x86_64.txt").exists()
    _assert_no_archive(data_dir)


def test_empty_copy_back_fails_with_two_architectures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty signed tarball fails the whole run for a two-directory tree."""
    data_dir = tmp_path / "data"
    arm = _arch_dir(data_dir, "aarch64", {"mod.ko": b"ARM"})
    x86 = _arch_dir(data_dir, "x86_64", {"mod.ko": b"X86"})
    remote = Remote()
    remote.empty_download = True

    with pytest.raises(RuntimeError, match="No signed .ko files copied back"):
        _run(tmp_path, monkeypatch, remote, data_dir=data_dir)

    assert (arm / "mod.ko").read_bytes() == b"ARM"
    assert (x86 / "mod.ko").read_bytes() == b"X86"
    assert not (data_dir / SIGNED_PATH / "signing_summary.txt").exists()
    _assert_no_archive(data_dir)


def test_signing_failure_still_cleans_remote_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A remote signing failure still removes the TaskRun directory."""
    data_dir = tmp_path / "data"
    arch = _arch_dir(data_dir, "x86_64", {"mod.ko": b"MODULE"}, envfile="KEEP\n")
    remote = Remote()
    remote.fail_on_stdin = True

    with pytest.raises(subprocess.CalledProcessError):
        _run(tmp_path, monkeypatch, remote, data_dir=data_dir)

    assert (arch / "mod.ko").read_bytes() == b"MODULE"
    assert (arch / "envfile").read_text(encoding="utf-8") == "KEEP\n"
    assert any(
        "rm -rf" in command and TASK_UID in command for command in _ssh_commands(remote)
    )
    _assert_no_archive(data_dir)


def test_cleanup_failure_is_logged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed remote cleanup does not fail a successful signing run."""
    data_dir = tmp_path / "data"
    _arch_dir(data_dir, "x86_64", {"mod.ko": b"MODULE"})
    remote = Remote()

    def flaky(
        argv: list[str], *, stdin: str | None = None, max_attempts: int = 3
    ) -> subprocess.CompletedProcess[str]:
        if argv[0] == "ssh" and str(argv[-1]).startswith("rm -rf"):
            raise sign.gssapi_ssh.SSHConnectionError("cleanup failed")
        return remote(argv, stdin=stdin, max_attempts=max_attempts)

    with caplog.at_level(logging.WARNING, logger="release"):
        _run(tmp_path, monkeypatch, remote, data_dir=data_dir, runner=flaky)
    assert "Remote cleanup failed" in caplog.text
    arch = _unpack_archive(data_dir) / "x86_64"
    assert (arch / "mod.ko").read_bytes().startswith(b"SIGNED:")


def test_package_signed_files_packs_modules_and_removes_the_tree(tmp_path: Path) -> None:
    """The archive keeps relative paths and the unpacked tree is removed."""
    source = tmp_path / "signed-kmods" / "x86_64"
    source.mkdir(parents=True)
    (source / "mod.ko").write_bytes(b"SIGNED")
    nested = source / "drivers"
    nested.mkdir()
    (nested / "nested.ko").write_bytes(b"NESTED")

    sign.package_signed_files(tmp_path, "signed-kmods")

    archive_path = tmp_path / sign.ARCHIVE_NAME
    assert archive_path.is_file()
    assert not (tmp_path / "signed-kmods").exists()
    with tarfile.open(archive_path, "r:gz") as archive:
        names = set(archive.getnames())
    assert "signed-kmods/x86_64/mod.ko" in names
    assert "signed-kmods/x86_64/drivers/nested.ko" in names


def test_package_signed_files_fails_when_no_modules_exist(tmp_path: Path) -> None:
    """Packaging fails and leaves the tree in place when there are no modules."""
    source = tmp_path / "signed-kmods"
    source.mkdir()
    with pytest.raises(RuntimeError, match="No signed .ko files found to package"):
        sign.package_signed_files(tmp_path, "signed-kmods")
    assert source.is_dir()
    assert not (tmp_path / sign.ARCHIVE_NAME).exists()


def test_main_reads_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Forward Tekton environment variables to run()."""
    monkeypatch.setenv("PARAM_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PARAM_SIGNED_KMODS_PATH", SIGNED_PATH)
    monkeypatch.setenv("PARAM_KERBEROS_REALM", "IPA.REDHAT.COM")
    monkeypatch.setenv("PARAM_SIGNING_AUTHOR", "Ann Author")
    monkeypatch.setenv("TASK_RUN_UID", TASK_UID)
    with mock.patch.object(sign, "run") as mock_run:
        assert sign.main() == 0
    mock_run.assert_called_once_with(
        data_dir=tmp_path,
        signed_kmods_path=SIGNED_PATH,
        kerberos_realm="IPA.REDHAT.COM",
        signing_author="Ann Author",
        task_run_uid=TASK_UID,
    )


def test_module_entry_point_imports_main() -> None:
    """The package ``__main__`` module exposes the signing entry point."""
    from release_service_utils.tasks.managed.sign_oot_kmods import __main__ as entry

    assert entry.main is sign.main
