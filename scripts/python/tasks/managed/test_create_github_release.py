"""Tests for ``create_github_release``."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tarfile
from pathlib import Path
from unittest import mock

import create_github_release as cgr
import pytest
from vcs import github


def _write_snapshot(path: Path, components: list[dict]) -> None:
    """Write a snapshot JSON file with the given components.

    Adds required source.git fields if not present.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    full_components = []
    for comp in components:
        full_comp = dict(comp)
        if "source" not in full_comp:
            full_comp["source"] = {
                "git": {"revision": "abc123", "url": "https://github.com/org/repo.git"}
            }
        full_components.append(full_comp)
    path.write_text(json.dumps({"components": full_components}), encoding="utf-8")


def _make_layer_tar(dest: Path, base_dir: str, files: dict[str, bytes | str]) -> str:
    """Create a gzip tar at *dest* containing files under *base_dir*.

    Return the sha256 digest string (``sha256:<hex>``).
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, content in files.items():
            info = tarfile.TarInfo(name=f"{base_dir}/{name}")
            data = content if isinstance(content, bytes) else content.encode("utf-8")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    raw = buf.getvalue()
    digest = hashlib.sha256(raw).hexdigest()
    dest.mkdir(parents=True, exist_ok=True)
    (dest / digest).write_bytes(raw)
    return f"sha256:{digest}"


def _write_manifest(image_dir: Path, digests: list[str]) -> None:
    """Write a manifest.json with the given layer digests."""
    manifest = {"layers": [{"digest": d} for d in digests]}
    (image_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def _make_completed_process(
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    """Create a CompletedProcess for mocking subprocess calls."""
    return subprocess.CompletedProcess(
        args=["mock"],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


class TestCheckReleaseExists:
    """Tests for check_release_exists."""

    def test_release_exists_returns_html_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return html_url from API response when release exists."""
        release = {
            "tag_name": "v1.2.3",
            "html_url": "https://github.com/foo/bar/releases/tag/v1.2.3",
        }

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            assert "repos/foo/bar/releases/tags/v1.2.3" in cmd[2]
            return _make_completed_process(stdout=json.dumps(release))

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        result = cgr.check_release_exists("foo/bar", "1.2.3", "token")
        assert result == "https://github.com/foo/bar/releases/tag/v1.2.3"

    def test_release_exists_fallback_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return constructed URL when html_url is missing from response."""
        release = {"tag_name": "v1.2.3"}

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            return _make_completed_process(stdout=json.dumps(release))

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        result = cgr.check_release_exists("foo/bar", "1.2.3", "token")
        assert result == "https://github.com/foo/bar/releases/tag/v1.2.3"

    def test_release_not_found_http_404(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return None when API returns HTTP 404."""

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            return _make_completed_process(returncode=1, stderr="HTTP 404: Not Found")

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        result = cgr.check_release_exists("foo/bar", "1.2.3", "token")
        assert result is None

    def test_release_not_found_not_found_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Return None when stderr contains Not Found."""

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            return _make_completed_process(returncode=1, stderr="Not Found")

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        result = cgr.check_release_exists("foo/bar", "1.2.3", "token")
        assert result is None

    def test_api_failure_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raise RuntimeError when API call fails with non-404 error."""

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            return _make_completed_process(returncode=1, stderr="Internal Server Error")

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        with pytest.raises(RuntimeError, match="Failed to check release"):
            cgr.check_release_exists("foo/bar", "1.2.3", "token")

    def test_invalid_json_response_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raise RuntimeError when response is not valid JSON."""

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            return _make_completed_process(stdout="not json")

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        with pytest.raises(RuntimeError, match="Invalid JSON response"):
            cgr.check_release_exists("foo/bar", "1.2.3", "token")


class TestCopyBinariesToTemp:
    """Tests for copy_binaries_to_temp."""

    def test_copies_binary_files(self, tmp_path: Path) -> None:
        """Binary files (not SHA256SUMS or .sig) are copied and count returned."""
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        (src / "app.zip").write_bytes(b"zip content")
        (src / "data.json").write_text('{"key": "value"}', encoding="utf-8")
        (src / "app_SHA256SUMS").write_text("checksum", encoding="utf-8")
        (src / "app.sig").write_text("signature", encoding="utf-8")

        count = cgr.copy_binaries_to_temp(src, dst)

        assert count == 2
        assert (dst / "app.zip").exists()
        assert (dst / "data.json").exists()
        assert not (dst / "app_SHA256SUMS").exists()
        assert not (dst / "app.sig").exists()

    def test_nonexistent_source_dir(self, tmp_path: Path) -> None:
        """Non-existent source directory returns 0."""
        dst = tmp_path / "dst"
        dst.mkdir()

        count = cgr.copy_binaries_to_temp(tmp_path / "nope", dst)
        assert count == 0

    def test_nonexistent_source_dir_strict_raises(self, tmp_path: Path) -> None:
        """Non-existent source directory raises ValueError in strict mode."""
        dst = tmp_path / "dst"
        dst.mkdir()

        with pytest.raises(ValueError, match="Source directory does not exist"):
            cgr.copy_binaries_to_temp(tmp_path / "nope", dst, strict=True)

    def test_empty_source_dir(self, tmp_path: Path) -> None:
        """Empty source directory returns 0."""
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        count = cgr.copy_binaries_to_temp(src, dst)
        assert count == 0


class TestCreateRelease:
    """Tests for create_release."""

    def test_calls_gh_release_create(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Calls gh release create with correct arguments."""
        binaries_dir = tmp_path / "binaries"
        content_dir = tmp_path / "content"
        binaries_dir.mkdir()
        content_dir.mkdir()
        (binaries_dir / "app.zip").write_bytes(b"zip")
        (binaries_dir / "meta.json").write_text("{}", encoding="utf-8")
        (content_dir / "foo_SHA256SUMS").write_text("hash", encoding="utf-8")
        (content_dir / "foo.sig").write_text("sig", encoding="utf-8")

        captured_cmd: list[str] = []

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            captured_cmd.extend(cmd)
            return _make_completed_process(
                stdout="https://github.com/foo/bar/releases/tag/v1.0"
            )

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        result = cgr.create_release(
            "https://github.com/foo/bar",
            "1.0",
            binaries_dir,
            content_dir,
            "token",
        )

        assert result == "https://github.com/foo/bar/releases/tag/v1.0"
        assert "gh" in captured_cmd
        assert "release" in captured_cmd
        assert "create" in captured_cmd
        assert "v1.0" in captured_cmd
        assert "--repo" in captured_cmd
        assert "https://github.com/foo/bar" in captured_cmd


class TestWriteResultsJson:
    """Tests for write_results_json."""

    def test_writes_json_file(self, tmp_path: Path) -> None:
        """Creates the JSON results file with correct structure."""
        results_file = tmp_path / "results" / "create-github-release-results.json"

        cgr.write_results_json(results_file, "https://github.com/foo/bar/releases/tag/v1.0")

        assert results_file.exists()
        data = json.loads(results_file.read_text(encoding="utf-8"))
        assert data == {
            "github-release": {"url": "https://github.com/foo/bar/releases/tag/v1.0"}
        }


class TestRunCreateGithubRelease:
    """Tests for run_create_github_release.

    These tests mirror the catalog E2E tests:
    - test-create-github-release.yaml: new release (gh called 2 times)
    - test-create-github-exist-release.yaml: existing release (gh called 1 time)
    """

    def test_existing_release_gh_called_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When release exists, gh API is called only once (no create).

        Mirrors catalog test: test-create-github-exist-release.yaml
        """
        data_dir = tmp_path / "data"
        snapshot_path = data_dir / "snapshot.json"
        result_url_path = tmp_path / "result_url"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)
        _write_snapshot(snapshot_path, [{"containerImage": "registry.io/image:tag"}])

        gh_call_count = 0

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            nonlocal gh_call_count
            gh_call_count += 1
            if "api" in cmd:
                release = {
                    "tag_name": "v1.2.3",
                    "html_url": "https://github.com/foo/repo_with_release/releases/tag/v1.2.3",
                }
                return _make_completed_process(stdout=json.dumps(release))
            return _make_completed_process()

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        url = cgr.run_create_github_release(
            repository="https://github.com/foo/repo_with_release",
            release_version="1.2.3",
            content_directory="content",
            snapshot_path=snapshot_path,
            image_binaries_path="releases",
            results_dir_path="results",
            data_dir=data_dir,
            gh_token="mytoken",
            result_url_path=result_url_path,
        )

        assert gh_call_count == 1, f"gh was expected to be called 1 time, got {gh_call_count}"
        assert url == "https://github.com/foo/repo_with_release/releases/tag/v1.2.3"
        assert result_url_path.read_text(encoding="utf-8") == url

    def test_new_release_gh_called_twice(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When release does not exist, gh is called twice (API check + create).

        Mirrors catalog test: test-create-github-release.yaml
        """
        data_dir = tmp_path / "data"
        snapshot_path = data_dir / "snapshot.json"
        content_dir = data_dir / "content"
        result_url_path = tmp_path / "result_url"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)
        content_dir.mkdir(parents=True)
        _write_snapshot(snapshot_path, [{"containerImage": "registry.io/image:tag"}])
        (content_dir / "foo_SHA256SUMS").write_text("hash", encoding="utf-8")
        (content_dir / "foo_SHA256SUMS.sig").write_text("sig", encoding="utf-8")

        image_staging = tmp_path / "image"
        image_staging.mkdir()
        digest = _make_layer_tar(
            image_staging,
            "releases",
            {
                "foo.zip": b"binary",
                "foo.json": '{"example": "data"}',
                "foo_SHA256SUMS": "checksum",
                "foo_SHA256SUMS.sig": "sig",
            },
        )
        _write_manifest(image_staging, [digest])

        gh_call_count = 0
        gh_calls: list[list[str]] = []

        def mock_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
            import shutil

            assert "registry.io/image:tag" in source
            dest_path = Path(dest.removeprefix("dir:"))
            for item in image_staging.iterdir():
                if item.is_file():
                    shutil.copy2(item, dest_path)
            return _make_completed_process()

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            nonlocal gh_call_count
            gh_call_count += 1
            gh_calls.append(cmd)
            if "api" in cmd:
                return _make_completed_process(returncode=1, stderr="HTTP 404: Not Found")
            if "release" in cmd and "create" in cmd:
                assert "v1.2.3" in cmd
                return _make_completed_process(
                    stdout="https://github.com/foo/bar/releases/tag/v1.2.3"
                )
            return _make_completed_process()

        monkeypatch.setattr(cgr.skopeo, "copy", mock_copy)
        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        url = cgr.run_create_github_release(
            repository="https://github.com/foo/bar",
            release_version="1.2.3",
            content_directory="content",
            snapshot_path=snapshot_path,
            image_binaries_path="releases",
            results_dir_path="results",
            data_dir=data_dir,
            gh_token="mytoken",
            result_url_path=result_url_path,
        )

        assert gh_call_count == 2, f"gh was expected to be called 2 times, got {gh_call_count}"
        assert "api" in gh_calls[0]
        assert "release" in gh_calls[1] and "create" in gh_calls[1]
        assert url == "https://github.com/foo/bar/releases/tag/v1.2.3"
        assert result_url_path.read_text(encoding="utf-8") == url

    def test_existing_release_returns_url(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When release exists, return existing URL without creating."""
        data_dir = tmp_path / "data"
        snapshot_path = data_dir / "snapshot.json"
        result_url_path = tmp_path / "result_url"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)
        _write_snapshot(snapshot_path, [{"containerImage": "registry.io/img:v1"}])

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            if "api" in cmd:
                release = {
                    "tag_name": "v1.0.0",
                    "html_url": "https://github.com/foo/bar/releases/tag/v1.0.0",
                }
                return _make_completed_process(stdout=json.dumps(release))
            return _make_completed_process()

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        url = cgr.run_create_github_release(
            repository="https://github.com/foo/bar",
            release_version="1.0.0",
            content_directory="content",
            snapshot_path=snapshot_path,
            image_binaries_path="releases",
            results_dir_path="results",
            data_dir=data_dir,
            gh_token="token",
            result_url_path=result_url_path,
        )

        assert url == "https://github.com/foo/bar/releases/tag/v1.0.0"
        assert result_url_path.read_text(encoding="utf-8") == url

    def test_creates_new_release(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When release does not exist, create it and return URL."""
        data_dir = tmp_path / "data"
        snapshot_path = data_dir / "snapshot.json"
        content_dir = data_dir / "content"
        result_url_path = tmp_path / "result_url"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)
        content_dir.mkdir(parents=True)
        _write_snapshot(snapshot_path, [{"containerImage": "registry.io/img:v1"}])
        (content_dir / "foo_SHA256SUMS").write_text("hash", encoding="utf-8")

        image_staging = tmp_path / "image"
        image_staging.mkdir()
        digest = _make_layer_tar(
            image_staging,
            "releases",
            {"app.zip": b"binary", "app.json": "{}"},
        )
        _write_manifest(image_staging, [digest])

        api_called = False

        def mock_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
            import shutil

            dest_path = Path(dest.removeprefix("dir:"))
            for item in image_staging.iterdir():
                if item.is_file():
                    shutil.copy2(item, dest_path)
            return _make_completed_process()

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            nonlocal api_called
            if "api" in cmd:
                api_called = True
                return _make_completed_process(returncode=1, stderr="HTTP 404: Not Found")
            if "release" in cmd and "create" in cmd:
                return _make_completed_process(
                    stdout="https://github.com/foo/bar/releases/tag/v2.0.0"
                )
            return _make_completed_process()

        monkeypatch.setattr(cgr.skopeo, "copy", mock_copy)
        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        url = cgr.run_create_github_release(
            repository="https://github.com/foo/bar",
            release_version="2.0.0",
            content_directory="content",
            snapshot_path=snapshot_path,
            image_binaries_path="releases",
            results_dir_path="results",
            data_dir=data_dir,
            gh_token="token",
            result_url_path=result_url_path,
        )

        assert api_called
        assert url == "https://github.com/foo/bar/releases/tag/v2.0.0"
        assert result_url_path.read_text(encoding="utf-8") == url
        results_file = results_dir / "create-github-release-results.json"
        assert results_file.exists()

    def test_results_json_written_correctly(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Results JSON file has correct structure."""
        data_dir = tmp_path / "data"
        snapshot_path = data_dir / "snapshot.json"
        content_dir = data_dir / "content"
        result_url_path = tmp_path / "result_url"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)
        content_dir.mkdir(parents=True)
        _write_snapshot(snapshot_path, [{"containerImage": "registry.io/img:v1"}])
        (content_dir / "foo_SHA256SUMS").write_text("hash", encoding="utf-8")

        image_staging = tmp_path / "image"
        image_staging.mkdir()
        digest = _make_layer_tar(image_staging, "releases", {"app.zip": b"binary"})
        _write_manifest(image_staging, [digest])

        def mock_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
            import shutil

            dest_path = Path(dest.removeprefix("dir:"))
            for item in image_staging.iterdir():
                if item.is_file():
                    shutil.copy2(item, dest_path)
            return _make_completed_process()

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            if "api" in cmd:
                return _make_completed_process(returncode=1, stderr="HTTP 404: Not Found")
            return _make_completed_process(
                stdout="https://github.com/foo/bar/releases/tag/v1.0.0"
            )

        monkeypatch.setattr(cgr.skopeo, "copy", mock_copy)
        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        cgr.run_create_github_release(
            repository="https://github.com/foo/bar",
            release_version="1.0.0",
            content_directory="content",
            snapshot_path=snapshot_path,
            image_binaries_path="releases",
            results_dir_path="results",
            data_dir=data_dir,
            gh_token="token",
            result_url_path=result_url_path,
        )

        results_file = results_dir / "create-github-release-results.json"
        data = json.loads(results_file.read_text(encoding="utf-8"))
        assert data == {
            "github-release": {"url": "https://github.com/foo/bar/releases/tag/v1.0.0"}
        }

    def test_binaries_not_overwritten_by_image_checksums(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """SHA256SUMS and .sig from image are NOT copied; TA chain files are used."""
        data_dir = tmp_path / "data"
        snapshot_path = data_dir / "snapshot.json"
        content_dir = data_dir / "content"
        result_url_path = tmp_path / "result_url"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)
        content_dir.mkdir(parents=True)
        _write_snapshot(snapshot_path, [{"containerImage": "registry.io/img:v1"}])
        (content_dir / "foo_SHA256SUMS").write_text("ta_chain_hash", encoding="utf-8")
        (content_dir / "foo.sig").write_text("ta_chain_sig", encoding="utf-8")

        image_staging = tmp_path / "image"
        image_staging.mkdir()
        digest = _make_layer_tar(
            image_staging,
            "releases",
            {
                "foo.zip": b"binary",
                "foo_SHA256SUMS": "image_hash_should_not_be_used",
                "foo.sig": "image_sig_should_not_be_used",
            },
        )
        _write_manifest(image_staging, [digest])

        uploaded_files: list[str] = []

        def mock_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
            import shutil

            dest_path = Path(dest.removeprefix("dir:"))
            for item in image_staging.iterdir():
                if item.is_file():
                    shutil.copy2(item, dest_path)
            return _make_completed_process()

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            if "api" in cmd:
                return _make_completed_process(returncode=1, stderr="HTTP 404: Not Found")
            if "release" in cmd and "create" in cmd:
                for arg in cmd:
                    if arg.endswith(".zip") or "SHA256SUMS" in arg or arg.endswith(".sig"):
                        uploaded_files.append(Path(arg).name)
                return _make_completed_process(stdout="https://github.com/foo/bar/v1")

            return _make_completed_process()

        monkeypatch.setattr(cgr.skopeo, "copy", mock_copy)
        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        cgr.run_create_github_release(
            repository="https://github.com/foo/bar",
            release_version="1.0.0",
            content_directory="content",
            snapshot_path=snapshot_path,
            image_binaries_path="releases",
            results_dir_path="results",
            data_dir=data_dir,
            gh_token="token",
            result_url_path=result_url_path,
        )

        assert "foo.zip" in uploaded_files
        assert "foo_SHA256SUMS" in uploaded_files
        assert "foo.sig" in uploaded_files
        assert (content_dir / "foo_SHA256SUMS").read_text() == "ta_chain_hash"
        assert (content_dir / "foo.sig").read_text() == "ta_chain_sig"

    def test_skopeo_failure_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-zero skopeo exit code raises CalledProcessError."""
        data_dir = tmp_path / "data"
        snapshot_path = data_dir / "snapshot.json"
        content_dir = data_dir / "content"
        result_url_path = tmp_path / "result_url"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)
        content_dir.mkdir(parents=True)
        _write_snapshot(snapshot_path, [{"containerImage": "registry.io/img:v1"}])

        def mock_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
            return _make_completed_process(returncode=1, stderr="auth error")

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            return _make_completed_process(returncode=1, stderr="HTTP 404: Not Found")

        monkeypatch.setattr(cgr.skopeo, "copy", mock_copy)
        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        with pytest.raises(subprocess.CalledProcessError):
            cgr.run_create_github_release(
                repository="https://github.com/foo/bar",
                release_version="1.0.0",
                content_directory="content",
                snapshot_path=snapshot_path,
                image_binaries_path="releases",
                results_dir_path="results",
                data_dir=data_dir,
                gh_token="token",
                result_url_path=result_url_path,
            )

    def test_temp_dirs_cleaned_on_success(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Temporary directories are cleaned up after successful run."""
        data_dir = tmp_path / "data"
        snapshot_path = data_dir / "snapshot.json"
        content_dir = data_dir / "content"
        result_url_path = tmp_path / "result_url"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)
        content_dir.mkdir(parents=True)
        _write_snapshot(snapshot_path, [{"containerImage": "registry.io/img:v1"}])
        (content_dir / "foo_SHA256SUMS").write_text("hash", encoding="utf-8")

        image_staging = tmp_path / "image"
        image_staging.mkdir()
        digest = _make_layer_tar(image_staging, "releases", {"app.zip": b"binary"})
        _write_manifest(image_staging, [digest])

        created_tmp_dirs: list[Path] = []
        original_mkdtemp = cgr.tempfile.mkdtemp

        def tracking_mkdtemp() -> str:
            d = original_mkdtemp()
            created_tmp_dirs.append(Path(d))
            return d

        def mock_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
            import shutil

            dest_path = Path(dest.removeprefix("dir:"))
            for item in image_staging.iterdir():
                if item.is_file():
                    shutil.copy2(item, dest_path)
            return _make_completed_process()

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            if "api" in cmd:
                return _make_completed_process(returncode=1, stderr="HTTP 404: Not Found")
            return _make_completed_process(stdout="https://github.com/foo/bar/v1")

        monkeypatch.setattr(cgr.skopeo, "copy", mock_copy)
        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        with mock.patch.object(cgr.tempfile, "mkdtemp", tracking_mkdtemp):
            cgr.run_create_github_release(
                repository="https://github.com/foo/bar",
                release_version="1.0.0",
                content_directory="content",
                snapshot_path=snapshot_path,
                image_binaries_path="releases",
                results_dir_path="results",
                data_dir=data_dir,
                gh_token="token",
                result_url_path=result_url_path,
            )

        assert len(created_tmp_dirs) == 2
        for d in created_tmp_dirs:
            assert not d.exists(), f"Temp dir {d} was not cleaned up"


class TestCreateReleaseEdgeCases:
    """Additional edge case tests for create_release."""

    def test_gh_release_create_failure_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify gh release create failure raises CalledProcessError."""
        binaries_dir = tmp_path / "binaries"
        content_dir = tmp_path / "content"
        binaries_dir.mkdir()
        content_dir.mkdir()
        (binaries_dir / "app.zip").write_bytes(b"zip")
        (content_dir / "foo_SHA256SUMS").write_text("hash", encoding="utf-8")

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            if check:
                raise subprocess.CalledProcessError(1, cmd, stderr="permission denied")
            return _make_completed_process(returncode=1, stderr="permission denied")

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        with pytest.raises(subprocess.CalledProcessError):
            cgr.create_release(
                "https://github.com/foo/bar",
                "1.0",
                binaries_dir,
                content_dir,
                "token",
            )

    def test_empty_binaries_dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Release can be created with no binary files (only checksums)."""
        binaries_dir = tmp_path / "binaries"
        content_dir = tmp_path / "content"
        binaries_dir.mkdir()
        content_dir.mkdir()
        (content_dir / "foo_SHA256SUMS").write_text("hash", encoding="utf-8")

        captured_files: list[str] = []

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            for arg in cmd:
                if not arg.startswith("-") and "/" in arg:
                    captured_files.append(arg)
            return _make_completed_process(stdout="https://github.com/foo/bar/v1")

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        cgr.create_release(
            "https://github.com/foo/bar",
            "1.0",
            binaries_dir,
            content_dir,
            "token",
        )

        assert any("SHA256SUMS" in f for f in captured_files)


class TestRunCreateGithubReleaseEdgeCases:
    """Additional edge case tests for run_create_github_release."""

    def test_custom_image_binaries_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Custom image_binaries_path parameter is respected."""
        data_dir = tmp_path / "data"
        snapshot_path = data_dir / "snapshot.json"
        content_dir = data_dir / "content"
        result_url_path = tmp_path / "result_url"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)
        content_dir.mkdir(parents=True)
        _write_snapshot(snapshot_path, [{"containerImage": "registry.io/img:v1"}])
        (content_dir / "foo_SHA256SUMS").write_text("hash", encoding="utf-8")

        image_staging = tmp_path / "image"
        image_staging.mkdir()
        digest = _make_layer_tar(
            image_staging,
            "custom/binaries",
            {"app.zip": b"binary"},
        )
        _write_manifest(image_staging, [digest])

        def mock_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
            import shutil

            dest_path = Path(dest.removeprefix("dir:"))
            for item in image_staging.iterdir():
                if item.is_file():
                    shutil.copy2(item, dest_path)
            return _make_completed_process()

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            if "api" in cmd:
                return _make_completed_process(returncode=1, stderr="HTTP 404: Not Found")
            return _make_completed_process(stdout="https://github.com/foo/bar/v1")

        monkeypatch.setattr(cgr.skopeo, "copy", mock_copy)
        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        url = cgr.run_create_github_release(
            repository="https://github.com/foo/bar",
            release_version="1.0.0",
            content_directory="content",
            snapshot_path=snapshot_path,
            image_binaries_path="custom/binaries",
            results_dir_path="results",
            data_dir=data_dir,
            gh_token="token",
            result_url_path=result_url_path,
        )

        assert url == "https://github.com/foo/bar/v1"

    def test_missing_snapshot_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing snapshot file raises FileNotFoundError."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        result_url_path = tmp_path / "result_url"

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            return _make_completed_process(returncode=1, stderr="HTTP 404: Not Found")

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        with pytest.raises(FileNotFoundError):
            cgr.run_create_github_release(
                repository="https://github.com/foo/bar",
                release_version="1.0.0",
                content_directory="content",
                snapshot_path=data_dir / "missing.json",
                image_binaries_path="releases",
                results_dir_path="results",
                data_dir=data_dir,
                gh_token="token",
                result_url_path=result_url_path,
            )

    def test_empty_image_url_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Empty containerImage in snapshot raises ValueError."""
        data_dir = tmp_path / "data"
        snapshot_path = data_dir / "snapshot.json"
        result_url_path = tmp_path / "result_url"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)
        _write_snapshot(snapshot_path, [{"containerImage": ""}])

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            return _make_completed_process(returncode=1, stderr="HTTP 404: Not Found")

        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        with pytest.raises(ValueError, match="containerImage is empty"):
            cgr.run_create_github_release(
                repository="https://github.com/foo/bar",
                release_version="1.0.0",
                content_directory="content",
                snapshot_path=snapshot_path,
                image_binaries_path="releases",
                results_dir_path="results",
                data_dir=data_dir,
                gh_token="token",
                result_url_path=result_url_path,
            )

    def test_missing_binaries_dir_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing image_binaries_path directory after extraction raises ValueError."""
        data_dir = tmp_path / "data"
        snapshot_path = data_dir / "snapshot.json"
        result_url_path = tmp_path / "result_url"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)
        _write_snapshot(snapshot_path, [{"containerImage": "registry.io/img:v1"}])

        image_staging = tmp_path / "image"
        image_staging.mkdir()
        digest = _make_layer_tar(
            image_staging,
            "other_dir",
            {"app.zip": b"binary"},
        )
        _write_manifest(image_staging, [digest])

        def mock_copy(source: str, dest: str) -> subprocess.CompletedProcess[str]:
            import shutil

            dest_path = Path(dest.removeprefix("dir:"))
            for item in image_staging.iterdir():
                if item.is_file():
                    shutil.copy2(item, dest_path)
            return _make_completed_process()

        def mock_gh(cmd, *, gh_token, check=True, cwd=None):
            return _make_completed_process(returncode=1, stderr="HTTP 404: Not Found")

        monkeypatch.setattr(cgr.skopeo, "copy", mock_copy)
        monkeypatch.setattr(github, "run_gh_command", mock_gh)
        with pytest.raises(ValueError, match="does not contain the 'releases' directory"):
            cgr.run_create_github_release(
                repository="https://github.com/foo/bar",
                release_version="1.0.0",
                content_directory="content",
                snapshot_path=snapshot_path,
                image_binaries_path="releases",
                results_dir_path="results",
                data_dir=data_dir,
                gh_token="token",
                result_url_path=result_url_path,
            )


class TestMain:
    """Tests for main entry point."""

    def test_successful_run(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Successful run returns 0 and writes result."""
        result_url = tmp_path / "result_url"
        gh_token_path = tmp_path / "token"
        gh_token_path.write_text("secret-token", encoding="utf-8")
        data_dir = tmp_path / "data"
        results_dir = data_dir / "results"
        results_dir.mkdir(parents=True)

        monkeypatch.setenv("RESULT_URL", str(result_url))
        monkeypatch.setenv("REPOSITORY", "https://github.com/foo/bar")
        monkeypatch.setenv("RELEASE_VERSION", "1.0.0")
        monkeypatch.setenv("CONTENT_DIRECTORY", "content")
        monkeypatch.setenv("DATA_DIR", str(data_dir))
        monkeypatch.setenv("SNAPSHOT_PATH", "snapshot.json")
        monkeypatch.setenv("RESULTS_DIR_PATH", "results")
        monkeypatch.setenv("GH_TOKEN_PATH", str(gh_token_path))

        with mock.patch.object(
            cgr,
            "run_create_github_release",
            return_value="https://github.com/foo/bar/releases/tag/v1.0.0",
        ):
            rc = cgr.main()

        assert rc == 0

    def test_missing_result_env_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Missing RESULT_URL causes SystemExit."""
        monkeypatch.delenv("RESULT_URL", raising=False)
        with pytest.raises(SystemExit):
            cgr.main()

    def test_missing_repository_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Missing REPOSITORY causes SystemExit."""
        monkeypatch.setenv("RESULT_URL", str(tmp_path / "r"))
        monkeypatch.delenv("REPOSITORY", raising=False)
        with pytest.raises(SystemExit):
            cgr.main()

    def test_missing_token_file_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Missing token file raises ValueError."""
        monkeypatch.setenv("RESULT_URL", str(tmp_path / "r"))
        monkeypatch.setenv("REPOSITORY", "https://github.com/foo/bar")
        monkeypatch.setenv("RELEASE_VERSION", "1.0.0")
        monkeypatch.setenv("CONTENT_DIRECTORY", "content")
        monkeypatch.setenv("DATA_DIR", str(tmp_path))
        monkeypatch.setenv("SNAPSHOT_PATH", "snapshot.json")
        monkeypatch.setenv("RESULTS_DIR_PATH", "results")
        monkeypatch.setenv("GH_TOKEN_PATH", str(tmp_path / "nonexistent"))

        with pytest.raises(ValueError, match="token file not found"):
            cgr.main()
