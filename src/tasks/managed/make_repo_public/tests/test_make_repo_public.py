"""Tests for ``make_repo_public``."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import make_repo_public
import sys as _sys
import make_repo_public.make_repo_public  # ensure submodule is loaded

import pytest
import requests

_make_repo_public_mod = _sys.modules["make_repo_public.make_repo_public"]


def _write_data(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def _default_data() -> dict:
    return {"mapping": {"defaults": {}}}


def _snapshot_with_components(components: list[dict]) -> dict:
    return {"components": components}


def _mock_session(
    get_status: int = 200,
    post_ok: bool = True,
) -> MagicMock:
    session = MagicMock(spec=requests.Session)
    get_resp = MagicMock()
    get_resp.status_code = get_status
    session.get.return_value = get_resp

    post_resp = MagicMock()
    post_resp.ok = post_ok
    post_resp.status_code = 200 if post_ok else 403
    session.post.return_value = post_resp
    return session


class TestIsQuayRegistry:
    """Test Quay registry detection via the discovery endpoint."""

    def test_returns_true_on_200(self) -> None:
        """HTTP 200 on discovery means it is a Quay registry."""
        session = _mock_session(get_status=200)
        cache: dict[str, bool] = {}
        assert make_repo_public.is_quay_registry("quay.io", session, cache) is True
        session.get.assert_called_once_with("https://quay.io/api/v1/discovery", timeout=30)

    def test_returns_false_on_non_200(self) -> None:
        """Non-200 on discovery means not a Quay registry."""
        session = _mock_session(get_status=404)
        cache: dict[str, bool] = {}
        assert make_repo_public.is_quay_registry("other.io", session, cache) is False

    def test_returns_false_on_request_exception(self) -> None:
        """Request exceptions return False and are cached."""
        session = MagicMock(spec=requests.Session)
        session.get.side_effect = requests.ConnectionError("timeout")
        cache: dict[str, bool] = {}
        assert make_repo_public.is_quay_registry("bad.io", session, cache) is False
        assert cache == {"bad.io": False}

    def test_caches_results(self) -> None:
        """Second call for same registry uses cached value, no HTTP call."""
        session = _mock_session(get_status=200)
        cache: dict[str, bool] = {}
        make_repo_public.is_quay_registry("quay.io", session, cache)
        make_repo_public.is_quay_registry("quay.io", session, cache)
        assert session.get.call_count == 1
        assert cache == {"quay.io": True}

    def test_caches_false_result(self) -> None:
        """Non-200 responses (not exceptions) are cached as False."""
        session = _mock_session(get_status=500)
        cache: dict[str, bool] = {}
        make_repo_public.is_quay_registry("bad.io", session, cache)
        make_repo_public.is_quay_registry("bad.io", session, cache)
        assert session.get.call_count == 1
        assert cache == {"bad.io": False}


class TestMakeRepoPublic:
    """Test the POST call that changes repository visibility."""

    def test_success(self) -> None:
        """Successful POST does not raise."""
        session = _mock_session(post_ok=True)
        make_repo_public.make_repo_public("quay.io", "org/repo", "token123", session)
        session.post.assert_called_once()
        call_kwargs = session.post.call_args
        assert "changevisibility" in call_kwargs[0][0]

    def test_failure_raises(self) -> None:
        """Failed POST raises RuntimeError with helpful message."""
        session = _mock_session(post_ok=False)
        with pytest.raises(RuntimeError, match="Failed to make repo"):
            make_repo_public.make_repo_public("quay.io", "org/repo", "token123", session)

    def test_failure_includes_secret_name_hint(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Error message includes registry secret name from env."""
        monkeypatch.setenv("REGISTRY_SECRET_NAME", "my-secret")
        session = _mock_session(post_ok=False)
        with pytest.raises(RuntimeError, match="my-secret"):
            make_repo_public.make_repo_public(
                "quay.io",
                "org/repo",
                "token123",
                session,
            )

    def test_posts_correct_payload(self) -> None:
        """POST body is the visibility change JSON."""
        session = _mock_session(post_ok=True)
        make_repo_public.make_repo_public("quay.io", "org/repo", "mytoken", session)
        call_kwargs = session.post.call_args
        assert call_kwargs.kwargs["json"] == {"visibility": "public"}
        assert "Bearer mytoken" in call_kwargs.kwargs["headers"]["Authorization"]

    def test_network_error_raises_runtime_error(self) -> None:
        """Network failure during POST is wrapped as RuntimeError."""
        session = MagicMock(spec=requests.Session)
        session.post.side_effect = requests.ConnectionError("connection refused")
        with pytest.raises(RuntimeError, match="Failed to connect"):
            make_repo_public.make_repo_public("quay.io", "org/repo", "token123", session)


class TestRun:
    """Test the end-to-end ``run()`` orchestration."""

    def _setup_files(
        self,
        tmp_path: Path,
        data: dict | None = None,
        components: list[dict] | None = None,
    ) -> tuple[Path, Path, Path]:
        """Create data, snapshot, and secret files for tests."""
        data_file = tmp_path / "data.json"
        snapshot_file = tmp_path / "snapshot.json"
        secret_dir = tmp_path / "secrets"
        secret_dir.mkdir()
        (secret_dir / "token").write_text("testtoken", encoding="utf-8")

        _write_data(data_file, data or _default_data())
        _write_data(
            snapshot_file,
            _snapshot_with_components(components or []),
        )
        return data_file, snapshot_file, secret_dir

    def test_basic_flow(self, tmp_path: Path) -> None:
        """Components with public=true have their repos made public."""
        data_file, snapshot_file, secret_dir = self._setup_files(
            tmp_path,
            components=[
                {
                    "name": "comp1",
                    "public": True,
                    "repositories": [{"url": "quay.io/org/repo1"}],
                },
            ],
        )
        session = _mock_session(get_status=200, post_ok=True)
        with mock.patch(
            "make_repo_public.make_repo_public.http_client.get_retry_session",
            return_value=session,
        ):
            make_repo_public.run(
                data_file,
                snapshot_file,
                secret_dir,
                tmp_path / "no-ca.crt",
            )
        session.post.assert_called_once()
        assert "changevisibility" in session.post.call_args[0][0]

    def test_skips_non_public_components(self, tmp_path: Path) -> None:
        """Components without public=true are skipped."""
        data_file, snapshot_file, secret_dir = self._setup_files(
            tmp_path,
            components=[
                {
                    "name": "comp1",
                    "repositories": [{"url": "quay.io/org/repo1"}],
                },
            ],
        )
        session = _mock_session(get_status=200, post_ok=True)
        with mock.patch(
            "make_repo_public.make_repo_public.http_client.get_retry_session",
            return_value=session,
        ):
            make_repo_public.run(
                data_file,
                snapshot_file,
                secret_dir,
                tmp_path / "no-ca.crt",
            )
        session.post.assert_not_called()

    def test_uses_default_public_from_data(self, tmp_path: Path) -> None:
        """mapping.defaults.public=true makes all components public."""
        data_file, snapshot_file, secret_dir = self._setup_files(
            tmp_path,
            data={"mapping": {"defaults": {"public": True}}},
            components=[
                {
                    "name": "comp1",
                    "repositories": [{"url": "quay.io/org/repo1"}],
                },
            ],
        )
        session = _mock_session(get_status=200, post_ok=True)
        with mock.patch(
            "make_repo_public.make_repo_public.http_client.get_retry_session",
            return_value=session,
        ):
            make_repo_public.run(
                data_file,
                snapshot_file,
                secret_dir,
                tmp_path / "no-ca.crt",
            )
        session.post.assert_called_once()

    def test_skips_non_quay_registries(self, tmp_path: Path) -> None:
        """Non-Quay registries are skipped -- no POST is made."""
        data_file, snapshot_file, secret_dir = self._setup_files(
            tmp_path,
            components=[
                {
                    "name": "comp1",
                    "public": True,
                    "repositories": [
                        {"url": "dockerhub.io/org/repo1"},
                    ],
                },
            ],
        )
        session = _mock_session(get_status=404, post_ok=True)
        with mock.patch(
            "make_repo_public.make_repo_public.http_client.get_retry_session",
            return_value=session,
        ):
            make_repo_public.run(
                data_file,
                snapshot_file,
                secret_dir,
                tmp_path / "no-ca.crt",
            )
        session.post.assert_not_called()

    def test_fails_on_multiple_quay_registries(self, tmp_path: Path) -> None:
        """Error when repos span multiple Quay registries."""
        data_file, snapshot_file, secret_dir = self._setup_files(
            tmp_path,
            components=[
                {
                    "name": "comp1",
                    "public": True,
                    "repositories": [
                        {"url": "quay.io/org/repo1"},
                        {"url": "other-quay.io/org/repo2"},
                    ],
                },
            ],
        )
        session = _mock_session(get_status=200, post_ok=True)
        with mock.patch(
            "make_repo_public.make_repo_public.http_client.get_retry_session",
            return_value=session,
        ):
            with pytest.raises(RuntimeError, match="Multiple Quay registries"):
                make_repo_public.run(
                    data_file,
                    snapshot_file,
                    secret_dir,
                    tmp_path / "no-ca.crt",
                )

    def test_missing_data_file(self, tmp_path: Path) -> None:
        """Error when data file does not exist."""
        with pytest.raises(RuntimeError, match="No valid data file"):
            make_repo_public.run(
                tmp_path / "missing.json",
                tmp_path / "snapshot.json",
                tmp_path / "secrets",
                tmp_path / "ca.crt",
            )

    def test_missing_snapshot_file(self, tmp_path: Path) -> None:
        """Error when snapshot file does not exist."""
        data_file = tmp_path / "data.json"
        _write_data(data_file, _default_data())

        with pytest.raises(RuntimeError, match="No valid snapshot file"):
            make_repo_public.run(
                data_file,
                tmp_path / "missing.json",
                tmp_path / "secrets",
                tmp_path / "ca.crt",
            )

    def test_missing_token_file(self, tmp_path: Path) -> None:
        """Error when token file is missing from secret directory."""
        data_file = tmp_path / "data.json"
        snapshot_file = tmp_path / "snapshot.json"
        secret_dir = tmp_path / "secrets"
        secret_dir.mkdir()

        _write_data(data_file, _default_data())
        _write_data(snapshot_file, _snapshot_with_components([]))

        with pytest.raises(RuntimeError, match="token file not found"):
            make_repo_public.run(
                data_file,
                snapshot_file,
                secret_dir,
                tmp_path / "ca.crt",
            )

    def test_multiple_repos_same_registry(self, tmp_path: Path) -> None:
        """Multiple repos on the same Quay registry all get made public."""
        data_file, snapshot_file, secret_dir = self._setup_files(
            tmp_path,
            components=[
                {
                    "name": "comp1",
                    "public": True,
                    "repositories": [
                        {"url": "quay.io/org/repo1"},
                        {"url": "quay.io/org/repo2"},
                    ],
                },
            ],
        )
        session = _mock_session(get_status=200, post_ok=True)
        with mock.patch(
            "make_repo_public.make_repo_public.http_client.get_retry_session",
            return_value=session,
        ):
            make_repo_public.run(
                data_file,
                snapshot_file,
                secret_dir,
                tmp_path / "no-ca.crt",
            )
        assert session.post.call_count == 2
        assert "org/repo1" in session.post.call_args_list[0][0][0]
        assert "org/repo2" in session.post.call_args_list[1][0][0]

    def test_self_hosted_quay_with_port(self, tmp_path: Path) -> None:
        """Self-hosted Quay with port in URL is handled correctly."""
        data_file, snapshot_file, secret_dir = self._setup_files(
            tmp_path,
            components=[
                {
                    "name": "comp1",
                    "public": True,
                    "repositories": [
                        {
                            "url": "self-hosted-quay.example.com" ":8443/myorg/myrepo1",
                        },
                    ],
                },
            ],
        )
        session = _mock_session(get_status=200, post_ok=True)
        with mock.patch(
            "make_repo_public.make_repo_public.http_client.get_retry_session",
            return_value=session,
        ):
            make_repo_public.run(
                data_file,
                snapshot_file,
                secret_dir,
                tmp_path / "no-ca.crt",
            )
        session.post.assert_called_once()
        assert "self-hosted-quay.example.com:8443" in session.post.call_args[0][0]

    def test_missing_repo_url_raises(self, tmp_path: Path) -> None:
        """Repository entry without a url raises RuntimeError."""
        data_file, snapshot_file, secret_dir = self._setup_files(
            tmp_path,
            components=[
                {
                    "name": "comp1",
                    "public": True,
                    "repositories": [{}],
                },
            ],
        )
        session = _mock_session(get_status=200, post_ok=True)
        with mock.patch(
            "make_repo_public.make_repo_public.http_client.get_retry_session",
            return_value=session,
        ):
            with pytest.raises(RuntimeError, match="missing the 'url' field"):
                make_repo_public.run(
                    data_file,
                    snapshot_file,
                    secret_dir,
                    tmp_path / "no-ca.crt",
                )

    def test_invalid_data_json(self, tmp_path: Path) -> None:
        """Corrupt JSON in data file raises RuntimeError."""
        data_file = tmp_path / "data.json"
        data_file.write_text("not valid json", encoding="utf-8")
        snapshot_file = tmp_path / "snapshot.json"
        _write_data(snapshot_file, _snapshot_with_components([]))
        secret_dir = tmp_path / "secrets"
        secret_dir.mkdir()
        (secret_dir / "token").write_text("testtoken", encoding="utf-8")

        with pytest.raises(RuntimeError, match="Invalid JSON in data file"):
            make_repo_public.run(
                data_file,
                snapshot_file,
                secret_dir,
                tmp_path / "ca.crt",
            )

    def test_invalid_snapshot_json(self, tmp_path: Path) -> None:
        """Corrupt JSON in snapshot file raises RuntimeError."""
        data_file = tmp_path / "data.json"
        _write_data(data_file, _default_data())
        snapshot_file = tmp_path / "snapshot.json"
        snapshot_file.write_text("{bad json", encoding="utf-8")
        secret_dir = tmp_path / "secrets"
        secret_dir.mkdir()
        (secret_dir / "token").write_text("testtoken", encoding="utf-8")

        with pytest.raises(RuntimeError, match="Invalid JSON in snapshot file"):
            make_repo_public.run(
                data_file,
                snapshot_file,
                secret_dir,
                tmp_path / "ca.crt",
            )


class TestMain:
    """Test the CLI entry point."""

    def test_success(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """`main` returns 0 on success."""
        data_file = tmp_path / "data.json"
        snapshot_file = tmp_path / "snapshot.json"
        secret_dir = tmp_path / "secrets"
        secret_dir.mkdir()
        (secret_dir / "token").write_text("testtoken", encoding="utf-8")

        _write_data(data_file, _default_data())
        _write_data(snapshot_file, _snapshot_with_components([]))

        monkeypatch.setenv("DATA_FILE", str(data_file))
        monkeypatch.setenv("SNAPSHOT_FILE", str(snapshot_file))
        monkeypatch.setenv("REGISTRY_SECRET_PATH", str(secret_dir))
        monkeypatch.setenv("CA_CERT_PATH", str(tmp_path / "no-ca.crt"))

        assert make_repo_public.main() == 0

    def test_missing_data_file_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """`main` returns 1 when DATA_FILE is not set."""
        monkeypatch.delenv("DATA_FILE", raising=False)
        monkeypatch.setenv("SNAPSHOT_FILE", "/some/path")
        assert make_repo_public.main() == 1

    def test_missing_snapshot_file_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """`main` returns 1 when SNAPSHOT_FILE is not set."""
        monkeypatch.setenv("DATA_FILE", "/some/path")
        monkeypatch.delenv("SNAPSHOT_FILE", raising=False)
        assert make_repo_public.main() == 1

    def test_runtime_error_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """`main` raises RuntimeError when run() fails."""
        monkeypatch.setenv("DATA_FILE", str(tmp_path / "missing.json"))
        monkeypatch.setenv("SNAPSHOT_FILE", str(tmp_path / "snap.json"))
        monkeypatch.setenv("REGISTRY_SECRET_PATH", str(tmp_path))
        monkeypatch.setenv("CA_CERT_PATH", str(tmp_path / "no-ca.crt"))
        with pytest.raises(RuntimeError):
            make_repo_public.main()


class TestSetupCaBundle:
    """Test CA bundle setup and combination."""

    def test_combines_system_and_custom_ca(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When custom CA exists, both system and custom are combined."""
        custom_ca = tmp_path / "custom-ca.crt"
        custom_ca.write_text("CUSTOM_CERT", encoding="utf-8")
        system_ca = tmp_path / "system.crt"
        system_ca.write_text("SYSTEM_CERT", encoding="utf-8")

        with mock.patch.object(
            _make_repo_public_mod,
            "SYSTEM_CA_BUNDLE",
            str(system_ca),
        ):
            make_repo_public.setup_ca_bundle(custom_ca)

        combined = Path(os.environ["SSL_CERT_FILE"])
        content = combined.read_text(encoding="utf-8")
        assert "SYSTEM_CERT" in content
        assert "CUSTOM_CERT" in content

    def test_noop_when_ca_missing(self, tmp_path: Path) -> None:
        """No action when custom CA file does not exist."""
        make_repo_public.setup_ca_bundle(tmp_path / "nonexistent.crt")
