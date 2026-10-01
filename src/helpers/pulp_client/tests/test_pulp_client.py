"""Tests for ``pulp_client`` helper."""

from __future__ import annotations

import subprocess
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

    def test_top_level_keys(self, tmp_path: Path) -> None:
        """Read credentials from top-level keys when [cli] is absent."""
        f = tmp_path / "cli.toml"
        f.write_text(
            'base_url = "https://pulp.toplevel"\nusername = "admin"\npassword = "secret"\n',
            encoding="utf-8",
        )
        cfg = pulp_client.parse_pulp_config(f)
        assert cfg["base_url"] == "https://pulp.toplevel"
        assert cfg["username"] == "admin"
        assert cfg["password"] == "secret"

    def test_cli_section_preferred_over_top_level(self, tmp_path: Path) -> None:
        """Prefer [cli] values when both [cli] and top-level keys exist."""
        f = tmp_path / "cli.toml"
        f.write_text(
            'base_url = "https://ignored.example"\n'
            "[cli]\n"
            'base_url = "https://cli.example"\n',
            encoding="utf-8",
        )
        cfg = pulp_client.parse_pulp_config(f)
        assert cfg["base_url"] == "https://cli.example"


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
        assert auth.auth_method == "basic"

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
            assert auth.auth_method == "oauth2-bearer"

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
            "release_service_utils.helpers.pulp_client.pulp_client.requests.post",
            mock_post_fn,
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
        """Fallback to repository latest_version_href when requested."""
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
        assert (
            client.get_published_version_href("myrepo", fallback_to_latest=True) == "/v3/rv/3/"
        )

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
        assert client.get_published_version_href("myrepo", fallback_to_latest=True) == ""

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
        assert (
            client.get_published_version_href("myrepo", fallback_to_latest=True) == "/v3/rv/4/"
        )

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

    def test_no_fallback_to_latest(self) -> None:
        """Skip unpublished latest_version_href by default."""
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
                }
            ]
        )
        assert client.get_published_version_href("myrepo") == ""

    def test_digest_default_treats_unpublished_as_not_found(self) -> None:
        """check_digest does not search unpublished latest content by default."""
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
                }
            ]
        )
        assert (
            client.check_digest(
                "myrepo",
                "hello",
                "0",
                "1.0",
                "1.el9",
                "x86_64",
                "abc123",
            )
            == pulp_client.PulpDigestStatus.NOT_FOUND
        )

    def test_digest_fallback_to_latest_uses_unpublished(self) -> None:
        """fallback_to_latest=True searches unpublished latest_version_href."""
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
                {"latest_version_href": "/rv/latest/"},
                {"count": 1, "results": [{"pulp_href": "/pkg/1/"}]},
                {"artifact": "/art/1/"},
                {"sha256": "abc123"},
            ]
        )
        assert (
            client.check_digest(
                "myrepo",
                "hello",
                "0",
                "1.0",
                "1.el9",
                "x86_64",
                "abc123",
                fallback_to_latest=True,
            )
            == pulp_client.PulpDigestStatus.MATCH
        )

    def test_get_repo_href(self) -> None:
        """Return pulp_href from the repository list response."""
        client = self._client(
            [{"results": [{"pulp_href": "/api/pulp/dom/api/v3/repositories/rpm/rpm/uuid/"}]}]
        )
        assert (
            client.get_repo_href("x86_64") == "/api/pulp/dom/api/v3/repositories/rpm/rpm/uuid/"
        )

    def test_get_repo_href_missing_raises(self) -> None:
        """Raise when the repository list has no href."""
        client = self._client([{"results": []}])
        with pytest.raises(RuntimeError, match="Could not find repository href"):
            client.get_repo_href("missing")

    def test_get_repo_href_empty_href_raises(self) -> None:
        """Raise when pulp_href is blank."""
        client = self._client([{"results": [{"pulp_href": ""}]}])
        with pytest.raises(RuntimeError, match="Could not find repository href"):
            client.get_repo_href("blank")

    def test_ensure_domain_exists(self) -> None:
        """Matching domain name is accepted via the default listing domain."""
        client = self._client([{"results": [{"name": "dom"}]}])
        client.ensure_domain_exists()
        url = client._session.get.call_args.args[0]
        assert url == "https://pulp.test/api/pulp/default/api/v3/domains/?name=dom"

    def test_ensure_domain_exists_quotes_query_name(self) -> None:
        """Quote the target domain in the query; keep default in the path."""
        session = self._session([{"results": [{"name": "a b/c"}]}])
        client = pulp_client.PulpClient(session, "https://pulp.test", "a b/c")
        client.ensure_domain_exists()
        url = session.get.call_args.args[0]
        assert url == "https://pulp.test/api/pulp/default/api/v3/domains/?name=a%20b%2Fc"

    def test_ensure_domain_exists_missing_raises(self) -> None:
        """Empty domain list is treated as not found."""
        client = self._client([{"results": []}])
        with pytest.raises(RuntimeError, match="not found"):
            client.ensure_domain_exists()

    def test_ensure_domain_exists_null_results_raises(self) -> None:
        """A null results field is treated as not found."""
        client = self._client([{"results": None}])
        with pytest.raises(RuntimeError, match="not found"):
            client.ensure_domain_exists()

    def test_ensure_domain_exists_missing_results_key_raises(self) -> None:
        """A payload without results is treated as not found."""
        client = self._client([{}])
        with pytest.raises(RuntimeError, match="not found"):
            client.ensure_domain_exists()

    def test_ensure_domain_exists_non_dict_result_raises(self) -> None:
        """A non-object first result is treated as not found."""
        client = self._client([{"results": ["dom"]}])
        with pytest.raises(RuntimeError, match="not found"):
            client.ensure_domain_exists()

    def test_ensure_domain_exists_blank_name_raises(self) -> None:
        """A blank domain name in the payload is treated as not found."""
        client = self._client([{"results": [{"name": ""}]}])
        with pytest.raises(RuntimeError, match="not found"):
            client.ensure_domain_exists()

    def test_ensure_domain_exists_wrong_name_raises(self) -> None:
        """A different domain name is treated as not found."""
        client = self._client([{"results": [{"name": "other"}]}])
        with pytest.raises(RuntimeError, match="not found"):
            client.ensure_domain_exists()

    def test_list_rpm_repository_names_paginates(self) -> None:
        """Follow next links and collect repository names."""
        client = self._client(
            [
                {
                    "results": [{"name": "x86_64"}, {"name": ""}],
                    "next": "/api/pulp/dom/api/v3/repositories/rpm/rpm/?limit=100&offset=100",
                },
                {
                    "results": [{"name": "aarch64"}],
                    "next": None,
                },
            ]
        )
        assert client.list_rpm_repository_names() == {"x86_64", "aarch64"}
        second_url = client._session.get.call_args_list[1].args[0]
        assert second_url.startswith("https://pulp.test/api/pulp/dom/")

    def test_ensure_repos_exist_skips_when_empty(self) -> None:
        """Do not list repositories when no names are required."""
        client = self._client([])
        client.ensure_repos_exist([])
        client._session.get.assert_not_called()

    def test_ensure_repos_exist_all_present(self) -> None:
        """Succeed when every requested repo is listed."""
        client = self._client([{"results": [{"name": "x86_64"}], "next": None}])
        client.ensure_repos_exist(["x86_64"])

    def test_ensure_repos_exist_missing_raises(self) -> None:
        """Raise when a requested repo is absent."""
        client = self._client([{"results": [{"name": "x86_64"}, {"name": ""}], "next": None}])
        with pytest.raises(
            RuntimeError, match="Missing repos in Pulp domain dom: s390x"
        ) as exc:
            client.ensure_repos_exist(["x86_64", "s390x"])
        assert "DEFAULT_ARCHITECTURES" not in str(exc.value)

    def test_publication_none_no_fallback(self) -> None:
        """Do not use latest_version_href after an empty publication lookup."""
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
            ]
        )
        assert client.get_published_version_href("myrepo") == ""

    @patch("release_service_utils.helpers.pulp_client.pulp_client.time.sleep")
    def test_wait_for_task_completed(self, _mock_sleep: MagicMock) -> None:
        """Return when the task state is completed."""
        client = self._client([{"state": "running"}, {"state": "completed"}])
        client.wait_for_task("https://pulp.test/tasks/1/", timeout_seconds=30, poll_interval=5)
        _mock_sleep.assert_called_once_with(5)

    @patch("release_service_utils.helpers.pulp_client.pulp_client.time.sleep")
    def test_wait_for_task_failed(self, _mock_sleep: MagicMock) -> None:
        """Raise when the task state is failed."""
        client = self._client([{"state": "failed", "error": "boom"}])
        with pytest.raises(RuntimeError, match="Pulp task failed"):
            client.wait_for_task("https://pulp.test/tasks/1/", timeout_seconds=30)

    @patch("release_service_utils.helpers.pulp_client.pulp_client.time.sleep")
    def test_wait_for_task_failed_description(self, _mock_sleep: MagicMock) -> None:
        """Use description when error is missing."""
        client = self._client([{"state": "failed", "description": "nope"}])
        with pytest.raises(RuntimeError, match="nope"):
            client.wait_for_task("https://pulp.test/tasks/1/", timeout_seconds=30)

    @patch("release_service_utils.helpers.pulp_client.pulp_client.time.sleep")
    def test_wait_for_task_failed_no_details(self, _mock_sleep: MagicMock) -> None:
        """Use a generic detail string when error and description are missing."""
        client = self._client([{"state": "failed"}])
        with pytest.raises(RuntimeError, match="No details"):
            client.wait_for_task("https://pulp.test/tasks/1/", timeout_seconds=30)

    @patch("release_service_utils.helpers.pulp_client.pulp_client.time.sleep")
    def test_wait_for_task_timeout(self, mock_sleep: MagicMock) -> None:
        """Raise after timeout_seconds elapses without completion."""
        client = self._client([{"state": "running"}])
        with pytest.raises(RuntimeError, match="Timeout waiting for Pulp task"):
            client.wait_for_task(
                "https://pulp.test/tasks/1/", timeout_seconds=5, poll_interval=5
            )
        mock_sleep.assert_called_once_with(5)

    def test_add_content_empty_hrefs_noop(self) -> None:
        """Skip the modify call when no hrefs are provided."""
        session = MagicMock(spec=requests.Session)
        client = pulp_client.PulpClient(session, "https://pulp.test", "dom")
        client.add_content("x86_64", [], timeout_seconds=30)
        session.get.assert_not_called()
        session.post.assert_not_called()

    def test_add_content_relative_task_href(self) -> None:
        """POST modify and wait using a relative task href."""
        session = self._session(
            [
                {"results": [{"pulp_href": "/repos/1/"}]},
                {"state": "completed"},
            ]
        )
        post_resp = MagicMock()
        post_resp.status_code = 202
        post_resp.content = b'{"task": "/tasks/1/"}'
        post_resp.json.return_value = {"task": "/tasks/1/"}
        post_resp.text = '{"task": "/tasks/1/"}'
        session.post.return_value = post_resp
        client = pulp_client.PulpClient(session, "https://pulp.test", "dom")
        client.add_content("x86_64", ["/pkg/1/"], timeout_seconds=30)
        session.post.assert_called_once()
        assert session.get.call_args_list[-1].args[0] == "https://pulp.test/tasks/1/"

    def test_add_content_absolute_task_href(self) -> None:
        """Use an absolute task URL as-is."""
        session = self._session(
            [
                {"results": [{"pulp_href": "/repos/1/"}]},
                {"state": "completed"},
            ]
        )
        post_resp = MagicMock()
        post_resp.status_code = 200
        post_resp.content = b'{"task": "https://other/tasks/9/"}'
        post_resp.json.return_value = {"task": "https://other/tasks/9/"}
        session.post.return_value = post_resp
        client = pulp_client.PulpClient(session, "https://pulp.test", "dom")
        client.add_content("x86_64", ["/pkg/1/"], timeout_seconds=30)
        assert session.get.call_args_list[-1].args[0] == "https://other/tasks/9/"

    def test_add_content_no_task_href(self) -> None:
        """Return without waiting when modify has no task."""
        session = self._session([{"results": [{"pulp_href": "/repos/1/"}]}])
        post_resp = MagicMock()
        post_resp.status_code = 200
        post_resp.content = b"{}"
        post_resp.json.return_value = {}
        session.post.return_value = post_resp
        client = pulp_client.PulpClient(session, "https://pulp.test", "dom")
        client.add_content("x86_64", ["/pkg/1/"], timeout_seconds=30)
        assert session.get.call_count == 1

    def test_add_content_empty_body(self) -> None:
        """Treat an empty modify response body as having no task."""
        session = self._session([{"results": [{"pulp_href": "/repos/1/"}]}])
        post_resp = MagicMock()
        post_resp.status_code = 200
        post_resp.content = b""
        session.post.return_value = post_resp
        client = pulp_client.PulpClient(session, "https://pulp.test", "dom")
        client.add_content("x86_64", ["/pkg/1/"], timeout_seconds=30)
        assert session.get.call_count == 1

    def test_add_content_http_error(self) -> None:
        """Raise when modify returns an unexpected status."""
        session = self._session([{"results": [{"pulp_href": "/repos/1/"}]}])
        post_resp = MagicMock()
        post_resp.status_code = 500
        post_resp.text = "boom"
        post_resp.content = b"boom"
        session.post.return_value = post_resp
        client = pulp_client.PulpClient(session, "https://pulp.test", "dom")
        with pytest.raises(RuntimeError, match="modify returned HTTP 500"):
            client.add_content("x86_64", ["/pkg/1/"], timeout_seconds=30)

    def test_from_config_sets_auth(self) -> None:
        """from_config builds a session with PulpAuth attached."""
        config = {
            "base_url": "https://pulp.test",
            "username": "u",
            "password": "p",
            "client_id": "",
            "client_secret": "",
        }
        mock_session = MagicMock()
        with patch(
            "release_service_utils.helpers.pulp_client.pulp_client.http_client"
            ".get_retry_session",
            return_value=mock_session,
        ):
            client = pulp_client.PulpClient.from_config(config, "dom")
        assert client._base_url == "https://pulp.test"
        assert client._domain == "dom"
        assert mock_session.auth is not None
        assert client._session is mock_session
        assert client.auth_method == "basic"


class TestPulpClientAuthMethod:
    """Test PulpClient.auth_method when no PulpAuth is attached."""

    def test_unknown_without_pulp_auth(self) -> None:
        """A plain session reports unknown auth."""
        client = pulp_client.PulpClient(MagicMock(), "https://pulp.test", "dom")
        assert client.auth_method == "unknown"


class TestUploadRpm:
    """Test pulp CLI upload with retries."""

    _SLEEP = "release_service_utils.helpers.retry.retry.time.sleep"
    _RUN_CMD = "release_service_utils.helpers.pulp_client.pulp_client.subprocess_cmd.run_cmd"

    def _client(self) -> pulp_client.PulpClient:
        """Build a client with no REST session traffic."""
        return pulp_client.PulpClient(
            MagicMock(spec=requests.Session),
            "https://pulp.test",
            "mydomain",
        )

    def _rpm(self, tmp_path: Path) -> Path:
        """Create an empty RPM file for upload tests."""
        rpm = tmp_path / "hello-1.0-1.x86_64.rpm"
        rpm.write_bytes(b"")
        return rpm

    def _completed(
        self, stdout: str = "", returncode: int = 0, stderr: str = ""
    ) -> subprocess.CompletedProcess[str]:
        """Build a CompletedProcess for pulp CLI mocks."""
        return subprocess.CompletedProcess(
            args=["pulp"], returncode=returncode, stdout=stdout, stderr=stderr
        )

    @patch(_SLEEP)
    def test_success(self, _sleep: MagicMock, tmp_path: Path) -> None:
        """Return pulp_href from a successful upload."""
        rpm = self._rpm(tmp_path)
        client = self._client()
        with patch(self._RUN_CMD, return_value=self._completed('{"pulp_href": "/pkg/1/"}')):
            href = client.upload_rpm(rpm, "100MB", tmp_path / "cli.toml")
        assert href == "/pkg/1/"

    @patch(_SLEEP)
    def test_cli_argv(self, _sleep: MagicMock, tmp_path: Path) -> None:
        """Pass config, domain, chunk size, and relative path to the pulp CLI."""
        rpm = self._rpm(tmp_path)
        client = self._client()
        with patch(
            self._RUN_CMD, return_value=self._completed('{"pulp_href": "/pkg/1/"}')
        ) as mock_run:
            client.upload_rpm(rpm, "100MB", tmp_path / "cli.toml")
        mock_run.assert_called_once_with(
            [
                "pulp",
                "--config",
                str(tmp_path / "cli.toml"),
                "--domain",
                "mydomain",
                "rpm",
                "content",
                "upload",
                "--chunk-size",
                "100MB",
                "--file",
                str(rpm),
                "--relative-path",
                rpm.name,
            ],
            check=False,
        )

    @patch(_SLEEP)
    def test_retries_then_succeeds(self, mock_sleep: MagicMock, tmp_path: Path) -> None:
        """Retry after a failed attempt then return the href."""
        rpm = self._rpm(tmp_path)
        client = self._client()
        with patch(
            self._RUN_CMD,
            side_effect=[
                self._completed("", returncode=1),
                self._completed('{"pulp_href": "/pkg/2/"}'),
            ],
        ):
            href = client.upload_rpm(rpm, "100MB", tmp_path / "cli.toml")
        assert href == "/pkg/2/"
        mock_sleep.assert_called_once()

    @patch(_SLEEP)
    def test_invalid_json_retries_exhausted(self, _sleep: MagicMock, tmp_path: Path) -> None:
        """Invalid JSON is retried until attempts are exhausted."""
        rpm = self._rpm(tmp_path)
        client = self._client()
        with patch(self._RUN_CMD, return_value=self._completed("not-json")):
            with pytest.raises(RuntimeError, match="invalid JSON") as exc_info:
                client.upload_rpm(rpm, "100MB", tmp_path / "cli.toml")
        assert "not-json" in str(exc_info.value)

    @patch(_SLEEP)
    def test_missing_href_exhausted(self, _sleep: MagicMock, tmp_path: Path) -> None:
        """Missing pulp_href is retried until attempts are exhausted."""
        rpm = self._rpm(tmp_path)
        client = self._client()
        with patch(self._RUN_CMD, return_value=self._completed("{}")):
            with pytest.raises(RuntimeError, match="href missing"):
                client.upload_rpm(rpm, "100MB", tmp_path / "cli.toml")

    @patch(_SLEEP)
    def test_nonzero_exhausted(self, _sleep: MagicMock, tmp_path: Path) -> None:
        """A non-zero pulp CLI exit is retried until attempts are exhausted."""
        rpm = self._rpm(tmp_path)
        client = self._client()
        with patch(
            self._RUN_CMD,
            return_value=self._completed(
                "", returncode=1, stderr="Error: Connection refused\n"
            ),
        ):
            with pytest.raises(RuntimeError, match="Connection refused") as exc_info:
                client.upload_rpm(rpm, "100MB", tmp_path / "cli.toml")
        assert "(rc=1): Error: Connection refused" in str(exc_info.value)
        assert str(rpm) in str(exc_info.value)

    @patch(_SLEEP)
    def test_nonzero_falls_back_to_stdout(self, _sleep: MagicMock, tmp_path: Path) -> None:
        """Use stdout when the pulp CLI writes the error there instead of stderr."""
        rpm = self._rpm(tmp_path)
        client = self._client()
        with patch(
            self._RUN_CMD,
            return_value=self._completed("authentication failed", returncode=2),
        ):
            with pytest.raises(RuntimeError, match="authentication failed") as exc_info:
                client.upload_rpm(rpm, "100MB", tmp_path / "cli.toml")
        assert "(rc=2): authentication failed" in str(exc_info.value)

    @patch(_SLEEP)
    def test_empty_stdout_exhausted(self, _sleep: MagicMock, tmp_path: Path) -> None:
        """Empty upload stdout is treated as a missing href."""
        rpm = self._rpm(tmp_path)
        client = self._client()
        with patch(self._RUN_CMD, return_value=self._completed("")):
            with pytest.raises(RuntimeError, match="href missing"):
                client.upload_rpm(rpm, "100MB", tmp_path / "cli.toml")
