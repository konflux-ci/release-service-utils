"""Test the ``check_jira_project_component`` module."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import pytest
import requests
from release_service_utils.tasks.managed.check_jira_project_component import (
    check_jira_project_component as mod,
)


@pytest.fixture(autouse=True)
def _propagate_release_logger():
    """Allow caplog to capture records from the 'release' logger."""
    release_logger = logging.getLogger("release")
    release_logger.propagate = True
    yield
    release_logger.propagate = False


# ------------------------------------------------------------------ #
# Fakes
# ------------------------------------------------------------------ #
class _FakeResponse:
    def __init__(self, status_code: int, json_data: object = None) -> None:
        self.status_code = status_code
        self._json = json_data

    def json(self) -> object:
        return self._json

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class _FakeSession:
    """Route /myself and /project/<key>/components.

    ``projects`` maps a project key to its list of component names.
    A key absent from the mapping yields a 404.

    ``component_request_counts`` tracks how many times each project's
    component endpoint was hit, keyed by project key.
    """

    def __init__(
        self,
        *,
        myself_status: int = 200,
        projects: dict[str, list[str]] | None = None,
        raise_on: str | None = None,
    ) -> None:
        self.myself_status = myself_status
        self.projects = projects or {}
        self.raise_on = raise_on
        self.component_request_counts: dict[str, int] = {}

    def get(
        self,
        url: str,
        auth: object = None,
        timeout: float | None = None,
    ) -> _FakeResponse:
        if self.raise_on and self.raise_on in url:
            raise requests.ConnectionError("boom")
        if url.endswith("/rest/api/2/myself"):
            return _FakeResponse(self.myself_status, {"name": "svc"})
        match = re.search(r"/rest/api/2/project/([^/]+)/components$", url)
        if match:
            key = match.group(1)
            self.component_request_counts[key] = self.component_request_counts.get(key, 0) + 1
            if key not in self.projects:
                return _FakeResponse(404, {"errorMessages": ["no project"]})
            return _FakeResponse(
                200,
                [{"name": n} for n in self.projects[key]],
            )
        return _FakeResponse(404)


def _write_snapshot(
    path: Path,
    components: list[dict],
) -> None:
    path.write_text(
        json.dumps({"application": "myapp", "components": components}),
        encoding="utf-8",
    )


def _write_secret(tmp_path: Path) -> Path:
    secret = tmp_path / "secret"
    secret.mkdir()
    (secret / "email").write_text("svc@example.com", encoding="utf-8")
    (secret / "token").write_text("t0ken", encoding="utf-8")
    return secret


def _run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    components: list[dict],
    session: _FakeSession,
    *,
    enforce: bool,
) -> None:
    snapshot = tmp_path / "snapshot.json"
    _write_snapshot(snapshot, components)
    secret = _write_secret(tmp_path)
    monkeypatch.setattr(
        mod.http_client,
        "get_retry_session",
        lambda **_k: session,
    )
    mod.check_jira_project_component(
        snapshot_path=snapshot,
        secret_path=secret,
        enforce=enforce,
    )


def _component(
    image: str = "registry.io/app@sha256:1",
    name: str = "c1",
    labels: dict[str, str] | None = None,
) -> dict:
    """Build a snapshot component dict with metadata labels."""
    metadata_labels = [{"name": k, "value": v} for k, v in (labels or {}).items()]
    return {
        "name": name,
        "containerImage": image,
        "metadata": {"labels": metadata_labels},
    }


def _labels(
    *,
    project: str | None = "EXAMPLE",
    tracker_component: str | None = None,
    name: str | None = None,
    bz_component: str | None = None,
) -> dict[str, str]:
    labels: dict[str, str] = {}
    if project is not None:
        labels[mod.SECURITY_TRACKER_PROJECT_LABEL] = project
    if tracker_component is not None:
        labels[mod.SECURITY_TRACKER_COMPONENT_LABEL] = tracker_component
    if name is not None:
        labels["name"] = name
    if bz_component is not None:
        labels["com.redhat.component"] = bz_component
    return labels


# ------------------------------------------------------------------ #
# Tests — basic validation
# ------------------------------------------------------------------ #
def test_valid_tracker_component_passes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass when security-tracker-component matches."""
    session = _FakeSession(projects={"EXAMPLE": ["api-server", "central"]})
    _run(
        tmp_path,
        monkeypatch,
        [_component(labels=_labels(tracker_component="api-server"))],
        session,
        enforce=True,
    )


def test_component_match_is_case_insensitive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Component matching is case-insensitive."""
    session = _FakeSession(projects={"EXAMPLE": ["API-Server"]})
    _run(
        tmp_path,
        monkeypatch,
        [_component(labels=_labels(tracker_component="api-server"))],
        session,
        enforce=True,
    )


def test_lowercase_project_key_is_normalized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lowercase project keys are upper-cased before lookup."""
    session = _FakeSession(projects={"EXAMPLE": ["api-server"]})
    _run(
        tmp_path,
        monkeypatch,
        [
            _component(
                labels=_labels(
                    project="example",
                    tracker_component="api-server",
                )
            )
        ],
        session,
        enforce=True,
    )


# ------------------------------------------------------------------ #
# Tests — fallback chain
# ------------------------------------------------------------------ #
def test_name_label_satisfies_component_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass when the 'name' label matches a Jira component."""
    session = _FakeSession(projects={"EXAMPLE": ["api-server", "central"]})
    _run(
        tmp_path,
        monkeypatch,
        [_component(labels=_labels(name="api-server"))],
        session,
        enforce=True,
    )


def test_bz_component_label_satisfies_component_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass when com.redhat.component matches a Jira component."""
    session = _FakeSession(projects={"EXAMPLE": ["api-server", "central"]})
    _run(
        tmp_path,
        monkeypatch,
        [_component(labels=_labels(bz_component="api-server"))],
        session,
        enforce=True,
    )


def test_fallback_chain_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The name label wins when all three labels are present."""
    session = _FakeSession(
        projects={
            "EXAMPLE": [
                "from-name",
                "from-bz",
                "from-tracker",
            ]
        }
    )
    with caplog.at_level(logging.INFO):
        _run(
            tmp_path,
            monkeypatch,
            [
                _component(
                    labels=_labels(
                        name="from-name",
                        bz_component="from-bz",
                        tracker_component="from-tracker",
                    )
                )
            ],
            session,
            enforce=True,
        )
    assert "label 'name'" in caplog.text


def test_fallback_skips_to_bz_when_name_mismatches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Falls back to com.redhat.component when name mismatches."""
    session = _FakeSession(projects={"EXAMPLE": ["api-server"]})
    with caplog.at_level(logging.INFO):
        _run(
            tmp_path,
            monkeypatch,
            [
                _component(
                    labels=_labels(
                        name="wrong-name",
                        bz_component="api-server",
                        tracker_component="also-wrong",
                    )
                )
            ],
            session,
            enforce=True,
        )
    assert "label 'com.redhat.component'" in caplog.text


def test_fallback_skips_to_tracker_when_others_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Falls back to security-tracker-component last."""
    session = _FakeSession(projects={"EXAMPLE": ["api-server"]})
    with caplog.at_level(logging.INFO):
        _run(
            tmp_path,
            monkeypatch,
            [
                _component(
                    labels=_labels(
                        name="wrong",
                        bz_component="also-wrong",
                        tracker_component="api-server",
                    )
                )
            ],
            session,
            enforce=True,
        )
    assert "label 'com.redhat.security-tracker-component'" in caplog.text


def test_all_fallback_labels_mismatch_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail when none of the fallback labels match."""
    session = _FakeSession(projects={"EXAMPLE": ["api-server"]})
    with pytest.raises(mod.TrackerValidationError):
        _run(
            tmp_path,
            monkeypatch,
            [
                _component(
                    labels=_labels(
                        name="wrong",
                        bz_component="also-wrong",
                        tracker_component="still-wrong",
                    )
                )
            ],
            session,
            enforce=True,
        )


def test_no_fallback_labels_set_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail when no fallback labels are set at all."""
    session = _FakeSession(projects={"EXAMPLE": ["api-server"]})
    with pytest.raises(mod.TrackerValidationError):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels())],
            session,
            enforce=True,
        )


# ------------------------------------------------------------------ #
# Tests — project validation
# ------------------------------------------------------------------ #
def test_missing_project_raises_in_enforce(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail when the Jira project does not exist."""
    session = _FakeSession(projects={})
    with pytest.raises(mod.TrackerValidationError):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(tracker_component="api-server"))],
            session,
            enforce=True,
        )


def test_missing_project_warns_when_not_enforced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Warn (don't fail) when enforce is False."""
    session = _FakeSession(projects={})
    with caplog.at_level(logging.WARNING):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(tracker_component="api-server"))],
            session,
            enforce=False,
        )
    assert "does not exist on" in caplog.text


def test_project_with_no_components_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail when the project has zero components."""
    session = _FakeSession(projects={"EXAMPLE": []})
    with pytest.raises(mod.TrackerValidationError):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(tracker_component="api-server"))],
            session,
            enforce=True,
        )


# ------------------------------------------------------------------ #
# Tests — skip / edge cases
# ------------------------------------------------------------------ #
def test_missing_project_label_is_skipped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip images without a project label."""
    session = _FakeSession(projects={})
    _run(
        tmp_path,
        monkeypatch,
        [_component(labels=_labels(project=None))],
        session,
        enforce=True,
    )


def test_empty_project_label_is_skipped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Skip images whose project label is present but empty."""
    session = _FakeSession(projects={})
    with caplog.at_level(logging.WARNING):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(project=""))],
            session,
            enforce=True,
        )
    assert "empty/whitespace" in caplog.text


def test_whitespace_project_label_is_skipped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Skip images whose project label is whitespace-only."""
    session = _FakeSession(projects={})
    with caplog.at_level(logging.WARNING):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(project="   "))],
            session,
            enforce=True,
        )
    assert "empty/whitespace" in caplog.text


# ------------------------------------------------------------------ #
# Tests — Jira connection errors
# ------------------------------------------------------------------ #
def test_auth_failure_is_always_fatal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auth failure is fatal even with enforce=False."""
    session = _FakeSession(
        myself_status=401,
        projects={"EXAMPLE": ["api-server"]},
    )
    with pytest.raises(mod.JiraConnectionError):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(tracker_component="api-server"))],
            session,
            enforce=False,
        )


def test_unreachable_jira_is_always_fatal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Network failure is fatal even with enforce=False."""
    session = _FakeSession(
        raise_on="/myself",
        projects={"EXAMPLE": ["api-server"]},
    )
    with pytest.raises(mod.JiraConnectionError):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(tracker_component="api-server"))],
            session,
            enforce=False,
        )


# ------------------------------------------------------------------ #
# Tests — project component caching
# ------------------------------------------------------------------ #
def test_same_project_components_are_fetched_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two components sharing a project trigger one API call."""
    session = _FakeSession(projects={"EXAMPLE": ["api-server", "central"]})
    _run(
        tmp_path,
        monkeypatch,
        [
            _component(
                name="c1",
                labels=_labels(tracker_component="api-server"),
            ),
            _component(
                name="c2",
                labels=_labels(tracker_component="central"),
            ),
        ],
        session,
        enforce=True,
    )
    assert session.component_request_counts["EXAMPLE"] == 1


def test_different_projects_are_fetched_separately(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Components with different projects each trigger one call."""
    session = _FakeSession(
        projects={
            "ALPHA": ["api-server"],
            "BETA": ["central"],
        }
    )
    _run(
        tmp_path,
        monkeypatch,
        [
            _component(
                name="c1",
                labels=_labels(
                    project="ALPHA",
                    tracker_component="api-server",
                ),
            ),
            _component(
                name="c2",
                labels=_labels(
                    project="BETA",
                    tracker_component="central",
                ),
            ),
        ],
        session,
        enforce=True,
    )
    assert session.component_request_counts["ALPHA"] == 1
    assert session.component_request_counts["BETA"] == 1


# ------------------------------------------------------------------ #
# Tests — verify_jira_connection error paths
# ------------------------------------------------------------------ #
def test_verify_jira_connection_non_auth_http_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-auth HTTP errors during connection check are fatal."""
    session = _FakeSession(myself_status=500)
    with pytest.raises(mod.JiraConnectionError, match="connection check failed"):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(tracker_component="api-server"))],
            session,
            enforce=True,
        )


# ------------------------------------------------------------------ #
# Tests — fetch_project_components error paths
# ------------------------------------------------------------------ #
class _AuthFailProjectSession(_FakeSession):
    """Return 403 for project component requests."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.project_status: int = 403

    def get(
        self,
        url: str,
        auth: object = None,
        timeout: float | None = None,
    ) -> _FakeResponse:
        """Override to return configured status for project URLs."""
        if "/rest/api/2/project/" in url and url.endswith("/components"):
            return _FakeResponse(self.project_status)
        return super().get(url, auth=auth, timeout=timeout)


def test_fetch_project_components_auth_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auth failure when fetching project components is fatal."""
    session = _AuthFailProjectSession(projects={"EXAMPLE": ["a"]})
    session.project_status = 403
    with pytest.raises(mod.JiraConnectionError, match="authorization failed"):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(tracker_component="a"))],
            session,
            enforce=True,
        )


def test_fetch_project_components_http_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-auth HTTP errors when fetching components are fatal."""
    session = _AuthFailProjectSession(projects={"EXAMPLE": ["a"]})
    session.project_status = 500
    with pytest.raises(mod.JiraConnectionError, match="query failed"):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(tracker_component="a"))],
            session,
            enforce=True,
        )


def test_fetch_project_components_connection_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Connection errors when fetching project components are fatal."""
    session = _FakeSession(
        raise_on="/project/",
        projects={"EXAMPLE": ["a"]},
    )
    with pytest.raises(mod.JiraConnectionError, match="unable to query"):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(tracker_component="a"))],
            session,
            enforce=True,
        )


def test_fetch_project_components_non_list_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise when Jira returns a JSON object instead of a list."""

    class _ObjectResponseSession(_FakeSession):
        def get(self, url, auth=None, timeout=None):
            if url.endswith("/components"):
                return _FakeResponse(200, {"unexpected": "object"})
            return super().get(url, auth=auth, timeout=timeout)

    session = _ObjectResponseSession(projects={"EXAMPLE": ["a"]})
    with pytest.raises(mod.JiraConnectionError, match="unexpected response type"):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(tracker_component="a"))],
            session,
            enforce=True,
        )


def test_fetch_project_components_null_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise when Jira returns null (None) instead of a list."""

    class _NullResponseSession(_FakeSession):
        def get(self, url, auth=None, timeout=None):
            if url.endswith("/components"):
                return _FakeResponse(200, None)
            return super().get(url, auth=auth, timeout=timeout)

    session = _NullResponseSession(projects={"EXAMPLE": ["a"]})
    with pytest.raises(mod.JiraConnectionError, match="unexpected response type"):
        _run(
            tmp_path,
            monkeypatch,
            [_component(labels=_labels(tracker_component="a"))],
            session,
            enforce=True,
        )


# ------------------------------------------------------------------ #
# Tests — parse_args and main
# ------------------------------------------------------------------ #
def test_parse_args_required_flags() -> None:
    """Parse the required CLI flags."""
    args = mod.parse_args(
        [
            "--snapshot-file",
            "/tmp/snap.json",
            "--jira-secret-path",
            "/tmp/secret",
        ]
    )
    assert args.snapshot_file == "/tmp/snap.json"
    assert args.jira_secret_path == "/tmp/secret"
    assert args.enforce is True


def test_parse_args_enforce_false() -> None:
    """Parse --enforce false."""
    args = mod.parse_args(
        [
            "--snapshot-file",
            "/tmp/snap.json",
            "--jira-secret-path",
            "/tmp/secret",
            "--enforce",
            "false",
        ]
    )
    assert args.enforce is False


def test_main_invokes_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() wires CLI args through to check_jira_project_component."""
    snapshot = tmp_path / "snapshot.json"
    _write_snapshot(snapshot, [_component(labels=_labels(project=None))])
    secret = _write_secret(tmp_path)
    session = _FakeSession(projects={})
    monkeypatch.setattr(
        mod.http_client,
        "get_retry_session",
        lambda **_k: session,
    )
    rc = mod.main(
        [
            "prog",
            "--snapshot-file",
            str(snapshot),
            "--jira-secret-path",
            str(secret),
        ]
    )
    assert rc == 0


def test_dunder_main_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Importing __main__ exposes the ``main`` callable."""
    import release_service_utils.tasks.managed.check_jira_project_component.__main__ as entry  # noqa: E501

    assert callable(entry.main)
