"""Tests for ``pulp_client`` helper."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from release_service_utils.helpers import pulp_client


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


class TestParsePulpConfig:
    """Test TOML parsing of Pulp cli.toml."""

    def test_basic_auth(self, tmp_path: Path) -> None:
        """Parse username/password credentials."""
        f = tmp_path / "cli.toml"
        f.write_text(
            _toml_content(
                base_url="https://pulp.test",
                username="admin",
                password="secret",
            ),
            encoding="utf-8",
        )
        cfg = pulp_client.parse_pulp_config(f)
        assert cfg["base_url"] == "https://pulp.test"
        assert cfg["username"] == "admin"
        assert cfg["password"] == "secret"

    def test_oauth_credentials(self, tmp_path: Path) -> None:
        """Parse client_id/client_secret credentials."""
        f = tmp_path / "cli.toml"
        f.write_text(
            _toml_content(client_id="cid", client_secret="csecret"),
            encoding="utf-8",
        )
        cfg = pulp_client.parse_pulp_config(f)
        assert cfg["client_id"] == "cid"
        assert cfg["client_secret"] == "csecret"

    def test_strips_trailing_slash(self, tmp_path: Path) -> None:
        """Trailing slashes on base_url are removed."""
        f = tmp_path / "cli.toml"
        f.write_text(
            _toml_content(base_url="https://pulp.test///"),
            encoding="utf-8",
        )
        cfg = pulp_client.parse_pulp_config(f)
        assert cfg["base_url"] == "https://pulp.test"

    def test_missing_cli_section_raises(self, tmp_path: Path) -> None:
        """Missing [cli] section raises because base_url is required."""
        f = tmp_path / "cli.toml"
        f.write_text("[other]\nfoo = 1\n", encoding="utf-8")
        with pytest.raises(RuntimeError, match="Missing required.*base_url"):
            pulp_client.parse_pulp_config(f)

    def test_missing_base_url_raises(self, tmp_path: Path) -> None:
        """Explicit [cli] section without base_url raises."""
        f = tmp_path / "cli.toml"
        f.write_text(
            '[cli]\nusername = "u"\npassword = "p"\n',
            encoding="utf-8",
        )
        with pytest.raises(RuntimeError, match="Missing required.*base_url"):
            pulp_client.parse_pulp_config(f)

    def test_empty_file_raises(self, tmp_path: Path) -> None:
        """Empty cli.toml raises RuntimeError."""
        f = tmp_path / "cli.toml"
        f.write_text("", encoding="utf-8")
        with pytest.raises(RuntimeError, match="Missing cli.toml"):
            pulp_client.parse_pulp_config(f)


class TestPulpAuth:
    """Test PulpAuth request authenticator."""

    def test_basic_auth_preferred(self) -> None:
        """Username/password produces Basic header."""
        config = {
            "username": "admin",
            "password": "pass",
            "client_id": "cid",
            "client_secret": "csec",
        }
        auth = pulp_client.PulpAuth(config)
        req = requests.Request("GET", "https://example.com").prepare()
        auth(req)
        assert req.headers["Authorization"].startswith("Basic ")

    def test_oauth_fallback(self) -> None:
        """When no username, uses client credentials with per-request refresh."""
        config = {
            "username": "",
            "password": "",
            "client_id": "cid",
            "client_secret": "csec",
        }
        mock_post = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"access_token": "tok123"}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        with patch(
            "release_service_utils.helpers.pulp_client.pulp_client.requests.post", mock_post
        ):
            auth = pulp_client.PulpAuth(config)
            assert mock_post.call_count == 1

            req = requests.Request("GET", "https://example.com").prepare()
            auth(req)
            assert req.headers["Authorization"] == "Bearer tok123"
            assert mock_post.call_count == 2

    def test_oauth_refreshes_per_request(self) -> None:
        """OAuth2 token is fetched fresh on every request."""
        config = {
            "username": "",
            "password": "",
            "client_id": "cid",
            "client_secret": "csec",
        }
        call_count = 0

        def mock_post_fn(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            resp.json.return_value = {"access_token": f"tok{call_count}"}
            resp.raise_for_status = MagicMock()
            return resp

        with patch(
            "release_service_utils.helpers.pulp_client.pulp_client.requests.post", mock_post_fn
        ):
            auth = pulp_client.PulpAuth(config)
            assert call_count == 1

            req1 = requests.Request("GET", "https://example.com/1").prepare()
            auth(req1)
            assert req1.headers["Authorization"] == "Bearer tok2"

            req2 = requests.Request("GET", "https://example.com/2").prepare()
            auth(req2)
            assert req2.headers["Authorization"] == "Bearer tok3"
            assert call_count == 3

    def test_no_credentials_raises(self) -> None:
        """Raise RuntimeError when no credentials are available."""
        config = {
            "username": "",
            "password": "",
            "client_id": "",
            "client_secret": "",
        }
        with pytest.raises(RuntimeError, match="No valid credentials"):
            pulp_client.PulpAuth(config)


class TestGetAccessToken:
    """Test OAuth2 token fetch."""

    @patch("time.sleep")
    def test_success(self, _mock_sleep: MagicMock) -> None:
        """Return access token from response."""
        mock_post = MagicMock()
        resp = MagicMock()
        resp.json.return_value = {"access_token": "mytoken"}
        resp.raise_for_status = MagicMock()
        mock_post.return_value = resp

        with patch(
            "release_service_utils.helpers.pulp_client.pulp_client.requests.post", mock_post
        ):
            token = pulp_client.pulp_client._get_access_token("cid", "csec")
        assert token == "mytoken"

    @patch("time.sleep")
    def test_failure_raises_after_retries(self, _mock_sleep: MagicMock) -> None:
        """HTTP error propagates after all retry attempts are exhausted."""
        mock_post = MagicMock()
        mock_post.return_value.raise_for_status.side_effect = requests.HTTPError("401")
        with patch(
            "release_service_utils.helpers.pulp_client.pulp_client.requests.post", mock_post
        ):
            with pytest.raises(requests.HTTPError):
                pulp_client.pulp_client._get_access_token("cid", "csec")
        assert mock_post.call_count == 3

    @patch("time.sleep")
    def test_retries_on_transient_failure(self, _mock_sleep: MagicMock) -> None:
        """Succeed after transient failures are retried."""
        fail_resp = MagicMock()
        fail_resp.raise_for_status.side_effect = requests.ConnectionError("timeout")

        ok_resp = MagicMock()
        ok_resp.json.return_value = {"access_token": "recovered"}
        ok_resp.raise_for_status = MagicMock()

        mock_post = MagicMock(side_effect=[fail_resp, ok_resp])

        with patch(
            "release_service_utils.helpers.pulp_client.pulp_client.requests.post", mock_post
        ):
            token = pulp_client.pulp_client._get_access_token("cid", "csec")
        assert token == "recovered"
        assert mock_post.call_count == 2


class TestPulpClient:
    """Test PulpClient: published version href resolution and digest checks."""

    def _session(self, responses: list[dict | Exception]) -> MagicMock:
        session = MagicMock(spec=requests.Session)
        resps = []
        for r in responses:
            if isinstance(r, Exception):
                resps.append(r)
            else:
                resp = MagicMock()
                resp.json.return_value = r
                resp.raise_for_status = MagicMock()
                resps.append(resp)
        session.get.side_effect = resps
        return session

    def _client(self, responses: list[dict | Exception]) -> pulp_client.PulpClient:
        return pulp_client.PulpClient(self._session(responses), "https://pulp.test", "dom")

    def test_direct_repository_version(self) -> None:
        """Return repository_version from distribution directly."""
        client = self._client(
            [
                {
                    "results": [
                        {
                            "repository_version": "/v3/rv/1/",
                            "publication": None,
                            "repository": None,
                        }
                    ]
                }
            ]
        )
        assert client.get_published_version_href("myrepo") == "/v3/rv/1/"

    def test_via_publication(self) -> None:
        """Resolve via publication href."""
        client = self._client(
            [
                {
                    "results": [
                        {
                            "repository_version": None,
                            "publication": "/pub/1/",
                            "repository": None,
                        }
                    ]
                },
                {"repository_version": "/v3/rv/2/"},
            ]
        )
        assert client.get_published_version_href("myrepo") == "/v3/rv/2/"

    def test_via_repository_latest(self) -> None:
        """Fallback to repository latest_version_href."""
        client = self._client(
            [
                {
                    "results": [
                        {
                            "repository_version": None,
                            "publication": None,
                            "repository": "/repo/1/",
                        }
                    ]
                },
                {"latest_version_href": "/v3/rv/3/"},
            ]
        )
        assert client.get_published_version_href("myrepo") == "/v3/rv/3/"

    def test_no_results(self) -> None:
        """Empty distribution results return empty string."""
        client = self._client([{"results": []}])
        assert client.get_published_version_href("myrepo") == ""

    def test_no_published_version(self) -> None:
        """All fields None returns empty string."""
        client = self._client(
            [
                {
                    "results": [
                        {
                            "repository_version": None,
                            "publication": None,
                            "repository": None,
                        }
                    ]
                }
            ]
        )
        assert client.get_published_version_href("myrepo") == ""

    def test_repository_latest_none(self) -> None:
        """Repository exists but latest_version_href is None."""
        client = self._client(
            [
                {
                    "results": [
                        {
                            "repository_version": None,
                            "publication": None,
                            "repository": "/repo/1/",
                        }
                    ]
                },
                {"latest_version_href": None},
            ]
        )
        assert client.get_published_version_href("myrepo") == ""

    def test_publication_rv_none(self) -> None:
        """Publication exists but repository_version is None -> falls through to repo."""
        client = self._client(
            [
                {
                    "results": [
                        {
                            "repository_version": None,
                            "publication": "/pub/1/",
                            "repository": "/repo/1/",
                        }
                    ]
                },
                {"repository_version": None},
                {"latest_version_href": "/v3/rv/4/"},
            ]
        )
        assert client.get_published_version_href("myrepo") == "/v3/rv/4/"

    def test_http_error_propagates(self) -> None:
        """HTTP error from the distribution API propagates."""
        session = MagicMock(spec=requests.Session)
        resp = MagicMock()
        resp.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
        session.get.return_value = resp

        client = pulp_client.PulpClient(session, "https://pulp.test", "dom")
        with pytest.raises(requests.HTTPError, match="500"):
            client.get_published_version_href("myrepo")

    def test_publication_http_error_propagates(self) -> None:
        """HTTP error when fetching publication href propagates."""
        session = MagicMock(spec=requests.Session)
        dist_resp = MagicMock()
        dist_resp.json.return_value = {
            "results": [
                {
                    "repository_version": None,
                    "publication": "/pub/1/",
                    "repository": None,
                }
            ]
        }
        dist_resp.raise_for_status = MagicMock()
        pub_resp = MagicMock()
        pub_resp.raise_for_status.side_effect = requests.HTTPError("502")
        session.get.side_effect = [dist_resp, pub_resp]

        client = pulp_client.PulpClient(session, "https://pulp.test", "dom")
        with pytest.raises(requests.HTTPError, match="502"):
            client.get_published_version_href("myrepo")

    def test_digest_match(self) -> None:
        """Matching digest returns MATCH."""
        client = self._client(
            [
                {"results": [{"repository_version": "/rv/1/"}]},
                {"count": 1, "results": [{"pulp_href": "/pkg/1/"}]},
                {"artifact": "/art/1/"},
                {"sha256": "abc123"},
            ]
        )
        assert (
            client.check_digest("myrepo", "hello", "0", "1.0", "1.el9", "x86_64", "abc123")
            == pulp_client.PulpDigestStatus.MATCH
        )

    def test_digest_not_found_no_published(self) -> None:
        """No published version returns NOT_FOUND."""
        client = self._client(
            [
                {
                    "results": [
                        {
                            "repository_version": None,
                            "publication": None,
                            "repository": None,
                        }
                    ]
                }
            ]
        )
        assert (
            client.check_digest("myrepo", "hello", "0", "1.0", "1.el9", "x86_64", "abc123")
            == pulp_client.PulpDigestStatus.NOT_FOUND
        )

    def test_digest_not_found_zero_count(self) -> None:
        """Zero count in packages query returns NOT_FOUND."""
        client = self._client(
            [
                {"results": [{"repository_version": "/rv/1/"}]},
                {"count": 0, "results": []},
            ]
        )
        assert (
            client.check_digest("myrepo", "hello", "0", "1.0", "1.el9", "x86_64", "abc123")
            == pulp_client.PulpDigestStatus.NOT_FOUND
        )

    def test_digest_mismatch(self) -> None:
        """Different digest returns MISMATCH."""
        client = self._client(
            [
                {"results": [{"repository_version": "/rv/1/"}]},
                {"count": 1, "results": [{"pulp_href": "/pkg/1/"}]},
                {"artifact": "/art/1/"},
                {"sha256": "different_hash"},
            ]
        )
        assert (
            client.check_digest("myrepo", "hello", "0", "1.0", "1.el9", "x86_64", "abc123")
            == pulp_client.PulpDigestStatus.MISMATCH
        )

    def test_digest_error_on_published_version(self) -> None:
        """Request error during published version check propagates."""
        session = MagicMock(spec=requests.Session)
        session.get.side_effect = requests.ConnectionError("fail")
        client = pulp_client.PulpClient(session, "https://pulp.test", "dom")
        with pytest.raises(requests.ConnectionError):
            client.check_digest("myrepo", "hello", "0", "1.0", "1.el9", "x86_64", "abc123")

    def test_digest_error_on_packages_query(self) -> None:
        """Request error during packages query propagates."""
        client = self._client(
            [
                {"results": [{"repository_version": "/rv/1/"}]},
                requests.ConnectionError("fail"),
            ]
        )
        with pytest.raises(requests.ConnectionError):
            client.check_digest("myrepo", "hello", "0", "1.0", "1.el9", "x86_64", "abc123")

    def test_digest_error_on_content_fetch(self) -> None:
        """Request error during content href fetch propagates."""
        client = self._client(
            [
                {"results": [{"repository_version": "/rv/1/"}]},
                {"count": 1, "results": [{"pulp_href": "/pkg/1/"}]},
                requests.ConnectionError("fail"),
            ]
        )
        with pytest.raises(requests.ConnectionError):
            client.check_digest("myrepo", "hello", "0", "1.0", "1.el9", "x86_64", "abc123")

    def test_digest_no_artifact_href(self) -> None:
        """Content without artifact href returns MISMATCH."""
        client = self._client(
            [
                {"results": [{"repository_version": "/rv/1/"}]},
                {"count": 1, "results": [{"pulp_href": "/pkg/1/"}]},
                {"artifact": None, "artifacts": []},
            ]
        )
        assert (
            client.check_digest("myrepo", "hello", "0", "1.0", "1.el9", "x86_64", "abc123")
            == pulp_client.PulpDigestStatus.MISMATCH
        )

    def test_digest_no_pulp_href_skipped(self) -> None:
        """Result with no pulp_href is skipped -> MISMATCH."""
        client = self._client(
            [
                {"results": [{"repository_version": "/rv/1/"}]},
                {"count": 1, "results": [{"pulp_href": ""}]},
            ]
        )
        assert (
            client.check_digest("myrepo", "hello", "0", "1.0", "1.el9", "x86_64", "abc123")
            == pulp_client.PulpDigestStatus.MISMATCH
        )

    def test_digest_artifacts_list_fallback(self) -> None:
        """Use artifacts[0] when artifact is None."""
        client = self._client(
            [
                {"results": [{"repository_version": "/rv/1/"}]},
                {"count": 1, "results": [{"pulp_href": "/pkg/1/"}]},
                {"artifact": None, "artifacts": ["/art/1/"]},
                {"sha256": "abc123"},
            ]
        )
        assert (
            client.check_digest("myrepo", "hello", "0", "1.0", "1.el9", "x86_64", "abc123")
            == pulp_client.PulpDigestStatus.MATCH
        )
