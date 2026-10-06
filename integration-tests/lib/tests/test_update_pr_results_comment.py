"""Unit tests for ``update_pr_results_comment``."""

from __future__ import annotations

import json
from pathlib import Path
import runpy
import sys
from unittest import mock

import pytest
import requests

import update_pr_results_comment as uprc


def test_parse_args_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Positional CLI arguments are parsed and normalized."""
    monkeypatch.setattr(sys, "argv", ["update_pr_results_comment.py", "org/repo", "12"])
    assert uprc.parse_args() == ("org/repo", 12)


def test_parse_args_invalid_pr_number(monkeypatch: pytest.MonkeyPatch) -> None:
    """Argparse rejects a non-integer pull request number."""
    monkeypatch.setattr(sys, "argv", ["update_pr_results_comment.py", "org/repo", "abc"])
    with pytest.raises(SystemExit) as excinfo:
        uprc.parse_args()
    assert excinfo.value.code == 2


def test_load_metadata_rejects_invalid_json() -> None:
    """Invalid metadata JSON becomes a no-op."""
    assert uprc.load_metadata("not-json") is None


@pytest.mark.parametrize(
    "metadata_json",
    [
        '"not-an-object"',
        '{"its_name":"", "its_key":"abc", "result":"FAILURE"}',
        '{"its_name":"collectors", "its_key":"abc", "result":"RUNNING"}',
    ],
)
def test_load_metadata_rejects_invalid_shapes(metadata_json: str) -> None:
    """Non-final or malformed metadata is ignored."""
    assert uprc.load_metadata(metadata_json) is None


def test_load_metadata_defaults_its_key() -> None:
    """The ITS key falls back to the ITS name when omitted."""
    metadata = uprc.load_metadata('{"its_name":"collectors","result":"SUCCESS"}')
    assert metadata is not None
    assert metadata["its_key"] == "collectors"


def test_extract_state_round_trip() -> None:
    """Hidden state encoding and decoding round-trip cleanly."""
    state = [
        {
            "its_key": "collectors:abc",
            "its_name": "collectors",
            "failure_label": "create-advisory",
            "details_url": "https://example.com/plr",
            "details_text": "too many requests",
        }
    ]
    body = uprc.render_comment_body(state)
    assert uprc.extract_state(body) == state


@pytest.mark.parametrize(
    "body",
    [
        f"{uprc.STATE_PREFIX}%%%{uprc.STATE_SUFFIX}",
        f"{uprc.STATE_PREFIX}{uprc.encode_state({'bad': 'shape'})}{uprc.STATE_SUFFIX}",
        uprc.MARKER,
    ],
)
def test_extract_state_invalid_variants_raise(body: str) -> None:
    """Invalid stored state is rejected instead of treated as empty."""
    with pytest.raises(uprc.UnreadableCommentStateError):
        uprc.extract_state(body)


def test_extract_state_without_marker_returns_empty() -> None:
    """Comments without any hidden state marker are treated as empty state."""
    assert uprc.extract_state("no state here") == []


def test_extract_state_skips_non_dict_rows() -> None:
    """Non-dict rows are ignored while valid rows are normalized."""
    state = ["bad-row", {"its_name": "collectors", "failure_label": "task"}]
    body = (
        f"{uprc.MARKER}\n" f"{uprc.STATE_PREFIX}{uprc.encode_state(state)}{uprc.STATE_SUFFIX}"
    )
    assert uprc.extract_state(body) == [
        {
            "its_key": "collectors",
            "its_name": "collectors",
            "failure_label": "task",
            "details_url": "",
            "details_text": "",
        }
    ]


@pytest.mark.parametrize(
    ("state", "metadata", "expected"),
    [
        (
            [
                {
                    "its_key": "collectors:one",
                    "its_name": "collectors",
                    "failure_label": "create-advisory",
                    "details_url": "https://example.com/plr",
                    "details_text": "failed",
                }
            ],
            {
                "its_key": "collectors:one",
                "its_name": "collectors",
                "result": "FAILURE",
                "failure_label": "create-advisory",
                "details_url": "https://example.com/plr",
                "details_text": "failed",
            },
            True,
        ),
        (
            [
                {
                    "its_name": "collectors",
                    "failure_label": "create-advisory",
                    "details_url": "https://example.com/plr",
                    "details_text": "failed",
                }
            ],
            {
                "its_key": "collectors",
                "its_name": "collectors",
                "result": "FAILURE",
                "failure_label": "create-advisory",
                "details_url": "https://example.com/plr",
                "details_text": "failed",
            },
            True,
        ),
        (
            [
                {
                    "its_key": "push-to-external-registry:idempotent",
                    "its_name": "push-to-external-registry",
                    "failure_label": "task-a",
                    "details_url": "",
                    "details_text": "",
                }
            ],
            {
                "its_key": "push-to-external-registry:idempotent",
                "its_name": "push-to-external-registry",
                "result": "SUCCESS",
                "failure_label": "",
                "details_url": "",
                "details_text": "",
            },
            False,
        ),
        (
            [
                {
                    "its_name": "push-to-external-registry",
                    "failure_label": "task-a",
                    "details_url": "",
                    "details_text": "",
                }
            ],
            {
                "its_key": "push-to-external-registry",
                "its_name": "push-to-external-registry",
                "result": "SUCCESS",
                "failure_label": "",
                "details_url": "",
                "details_text": "",
            },
            False,
        ),
    ],
)
def test_state_matches_expected_supports_keyed_and_legacy_rows(
    state: list[dict[str, str]],
    metadata: dict[str, str],
    expected: bool,
) -> None:
    """Validation handles both keyed rows and legacy name-only rows."""
    assert uprc.state_matches_expected(state, metadata) is expected


def test_state_matches_expected_failure_with_non_matching_row() -> None:
    """Failure validation returns false when no row matches the ITS key or name."""
    state = [{"its_key": "other", "its_name": "collectors", "failure_label": "task"}]
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "task",
        "details_url": "",
        "details_text": "",
    }
    assert uprc.state_matches_expected(state, metadata) is False


def test_merge_state_replaces_matching_key() -> None:
    """A failed rerun updates only the matching ITS row."""
    existing = [
        {
            "its_key": "collectors:one",
            "its_name": "collectors",
            "failure_label": "old",
            "details_url": "",
            "details_text": "",
        },
        {
            "its_key": "collectors:two",
            "its_name": "collectors",
            "failure_label": "sibling",
            "details_url": "",
            "details_text": "",
        },
    ]
    metadata = {
        "its_key": "collectors:one",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "create-advisory",
        "details_url": "https://example.com/plr",
        "details_text": "failed",
    }
    merged = uprc.merge_state(existing, metadata)
    assert len(merged) == 2
    assert any(
        row["its_key"] == "collectors:one" and row["failure_label"] == "create-advisory"
        for row in merged
    )
    assert any(row["its_key"] == "collectors:two" for row in merged)


def test_merge_state_success_removes_only_matching_key() -> None:
    """A passing rerun clears only its own ITS row."""
    existing = [
        {
            "its_key": "push-to-external-registry:idempotent",
            "its_name": "push-to-external-registry",
            "failure_label": "task-a",
            "details_url": "",
            "details_text": "",
        },
        {
            "its_key": "push-to-external-registry:idempotent-multiarch",
            "its_name": "push-to-external-registry",
            "failure_label": "task-b",
            "details_url": "",
            "details_text": "",
        },
    ]
    metadata = {
        "its_key": "push-to-external-registry:idempotent",
        "its_name": "push-to-external-registry",
        "result": "SUCCESS",
        "failure_label": "",
        "details_url": "",
        "details_text": "",
    }
    merged = uprc.merge_state(existing, metadata)
    assert merged == [existing[1]]


def test_merge_state_skipped_removes_matching_failure_row() -> None:
    """A skipped rerun clears only its own ITS row just like a success does."""
    existing = [
        {
            "its_key": "collectors:no-cve",
            "its_name": "collectors",
            "failure_label": "create-advisory",
            "details_url": "https://example.com/plr",
            "details_text": "failed before retry",
        },
        {
            "its_key": "collectors:with-cve",
            "its_name": "collectors",
            "failure_label": "component build",
            "details_url": "",
            "details_text": "",
        },
    ]
    metadata = {
        "its_key": "collectors:no-cve",
        "its_name": "collectors",
        "result": "SKIPPED",
        "failure_label": "",
        "details_url": "",
        "details_text": "",
    }
    merged = uprc.merge_state(existing, metadata)
    assert merged == [existing[1]]


def test_render_row_without_link_or_details() -> None:
    """Rows with plain text and no details render empty cells."""
    row = {
        "its_key": "collectors",
        "its_name": "collectors",
        "failure_label": "task-a",
        "details_url": "not-a-link",
        "details_text": "",
    }
    rendered = uprc.render_row(row)
    assert "<a href=" not in rendered
    assert "<details>" not in rendered


def test_render_row_escapes_and_wraps_details() -> None:
    """HTML output escapes text and wraps details in ``<details>``."""
    row = {
        "its_key": "collectors",
        "its_name": "<collectors>",
        "failure_label": "build & publish",
        "details_url": "https://example.com/?a=1&b=2",
        "details_text": "line 1\nline <2>",
    }
    rendered = uprc.render_row(row)
    assert "&lt;collectors&gt;" in rendered
    assert "build &amp; publish" in rendered
    assert "summary>Show</summary>" in rendered
    assert "line 1<br>line &lt;2&gt;" in rendered


def test_render_row_http_link_is_rendered() -> None:
    """HTTP links are rendered the same way as HTTPS links."""
    row = {
        "its_key": "collectors",
        "its_name": "collectors",
        "failure_label": "task-a",
        "details_url": "http://example.com/plr",
        "details_text": "",
    }
    rendered = uprc.render_row(row)
    assert '<a href="http://example.com/plr">Open</a>' in rendered


def test_find_existing_comment_returns_match_on_second_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Comment lookup paginates until it finds a matching marker owned by the bot."""
    page_counter = {"page": 0}

    def fake_list_issue_comments(
        _session: object,
        _repo_name: str,
        _pr_number: int,
        *,
        per_page: int = 100,
        page: int = 1,
    ) -> list[dict[str, object]]:
        assert per_page == 100
        page_counter["page"] = page
        if page == 1:
            return [{"user": {"login": "someone-else"}, "body": uprc.MARKER}] * 100
        return [{"id": 9, "user": {"login": "bot-user"}, "body": uprc.MARKER}]

    monkeypatch.setattr(uprc.github, "get_authenticated_user_login", lambda _s: "bot-user")
    monkeypatch.setattr(uprc.github, "list_issue_comments", fake_list_issue_comments)
    comment = uprc.find_existing_comment(object(), "org/repo", 12)
    assert comment is not None
    assert comment["id"] == 9
    assert page_counter["page"] == 2


def test_find_existing_comment_returns_none_on_short_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Comment lookup stops when the current page is shorter than 100 comments."""
    monkeypatch.setattr(uprc.github, "get_authenticated_user_login", lambda _s: "bot-user")
    monkeypatch.setattr(
        uprc.github,
        "list_issue_comments",
        lambda *_args, **_kwargs: [{"user": {"login": "other"}, "body": uprc.MARKER}],
    )
    assert uprc.find_existing_comment(object(), "org/repo", 12) is None


def test_github_error_text_variants() -> None:
    """GitHub API error text prefers structured messages and safe fallbacks."""
    no_response = requests.HTTPError("plain error")
    assert uprc._github_error_text(no_response) == "plain error"

    response = mock.Mock()
    response.text = "raw body"
    response.json.side_effect = ValueError("bad json")
    raw_body_error = requests.HTTPError("x", response=response)
    assert uprc._github_error_text(raw_body_error) == "raw body"

    response = mock.Mock()
    response.text = '{"error":"boom"}'
    response.json.return_value = {"error": "boom"}
    error_field = requests.HTTPError("x", response=response)
    assert uprc._github_error_text(error_field) == "boom"

    response = mock.Mock()
    response.text = '{"errors":[{"message":"nested"}]}'
    response.json.return_value = {"errors": [{"message": "nested"}]}
    nested_error = requests.HTTPError("x", response=response)
    assert uprc._github_error_text(nested_error) == "nested"

    response = mock.Mock()
    response.text = '{"unexpected":"shape"}'
    response.json.return_value = {"unexpected": "shape"}
    unexpected_shape = requests.HTTPError("x", response=response)
    assert uprc._github_error_text(unexpected_shape) == '{"unexpected":"shape"}'

    response = mock.Mock()
    response.text = ""
    blank_error = requests.HTTPError("fallback", response=response)
    assert uprc._github_error_text(blank_error) == "fallback"


def test_upsert_comment_skips_empty_success_without_existing_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A passing result with no existing sticky comment does nothing."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "SUCCESS",
        "failure_label": "",
        "details_url": "",
        "details_text": "",
    }
    create_comment = mock.Mock()
    update_comment = mock.Mock()
    with (
        mock.patch.object(uprc, "find_existing_comment", return_value=None),
        mock.patch.object(uprc.github, "create_issue_comment", create_comment),
        mock.patch.object(uprc.github, "update_issue_comment", update_comment),
    ):
        assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 0
    create_comment.assert_not_called()
    update_comment.assert_not_called()


def test_upsert_comment_skips_empty_skipped_without_existing_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A skipped result with no existing sticky comment does nothing."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "SKIPPED",
        "failure_label": "",
        "details_url": "",
        "details_text": "",
    }
    create_comment = mock.Mock()
    update_comment = mock.Mock()
    with (
        mock.patch.object(uprc, "find_existing_comment", return_value=None),
        mock.patch.object(uprc.github, "create_issue_comment", create_comment),
        mock.patch.object(uprc.github, "update_issue_comment", update_comment),
    ):
        assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 0
    create_comment.assert_not_called()
    update_comment.assert_not_called()


def test_upsert_comment_retries_after_initial_read_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient read failure before any write retries cleanly."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "create-advisory",
        "details_url": "https://example.com/plr",
        "details_text": "rate limited",
    }
    attempts = {"count": 0}
    written_body = {"value": ""}

    def fake_find_existing_comment(*_args: object) -> dict[str, object] | None:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise requests.RequestException("temporary read failure")
        if not written_body["value"]:
            return None
        return {"id": 10, "body": written_body["value"]}

    def fake_create_issue_comment(
        _session: object,
        _repo_name: str,
        _pr_number: int,
        body: str,
    ) -> dict[str, object]:
        written_body["value"] = body
        return {"id": 10, "body": body}

    monkeypatch.setattr(
        "release_service_utils.helpers.retry.retry.time.sleep",
        lambda _: None,
    )
    with (
        mock.patch.object(
            uprc,
            "find_existing_comment",
            side_effect=fake_find_existing_comment,
        ),
        mock.patch.object(
            uprc.github,
            "create_issue_comment",
            side_effect=fake_create_issue_comment,
        ),
    ):
        assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 0


def test_upsert_comment_retries_after_initial_read_json_decode_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed comment-read response retries through the normal path."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "create-advisory",
        "details_url": "https://example.com/plr",
        "details_text": "rate limited",
    }
    attempts = {"count": 0}
    written_body = {"value": ""}

    def fake_find_existing_comment(*_args: object) -> dict[str, object] | None:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise json.JSONDecodeError("bad json", "not-json", 0)
        if not written_body["value"]:
            return None
        return {"id": 10, "body": written_body["value"]}

    def fake_create_issue_comment(
        _session: object,
        _repo_name: str,
        _pr_number: int,
        body: str,
    ) -> dict[str, object]:
        written_body["value"] = body
        return {"id": 10, "body": body}

    monkeypatch.setattr(
        "release_service_utils.helpers.retry.retry.time.sleep",
        lambda _: None,
    )
    with (
        mock.patch.object(
            uprc,
            "find_existing_comment",
            side_effect=fake_find_existing_comment,
        ),
        mock.patch.object(
            uprc.github,
            "create_issue_comment",
            side_effect=fake_create_issue_comment,
        ),
    ):
        assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 0


def test_upsert_comment_retries_after_write_json_decode_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful write with malformed JSON response retries from a fresh read."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "create-advisory",
        "details_url": "https://example.com/plr",
        "details_text": "rate limited",
    }
    comment_state = {"body": ""}
    read_counter = {"count": 0}
    write_counter = {"count": 0}

    def fake_find_existing_comment(*_args: object) -> dict[str, object] | None:
        read_counter["count"] += 1
        if read_counter["count"] == 1:
            return None
        if not comment_state["body"]:
            return None
        return {"id": 10, "body": comment_state["body"]}

    def fake_create_issue_comment(
        _session: object,
        _repo_name: str,
        _pr_number: int,
        body: str,
    ) -> dict[str, object]:
        write_counter["count"] += 1
        if write_counter["count"] == 1:
            comment_state["body"] = uprc.render_comment_body(
                [
                    {
                        "its_key": metadata["its_key"],
                        "its_name": metadata["its_name"],
                        "failure_label": metadata["failure_label"],
                        "details_url": metadata["details_url"],
                        "details_text": metadata["details_text"],
                    }
                ]
            )
            raise json.JSONDecodeError("bad json", "not-json", 0)
        comment_state["body"] = body
        return {"id": 10, "body": body}

    def fake_update_issue_comment(
        _session: object,
        _comment_id: int,
        body: str,
    ) -> dict[str, object]:
        write_counter["count"] += 1
        comment_state["body"] = body
        return {"id": 10, "body": body}

    monkeypatch.setattr(
        "release_service_utils.helpers.retry.retry.time.sleep",
        lambda _: None,
    )
    with (
        mock.patch.object(
            uprc,
            "find_existing_comment",
            side_effect=fake_find_existing_comment,
        ),
        mock.patch.object(
            uprc.github,
            "create_issue_comment",
            side_effect=fake_create_issue_comment,
        ),
        mock.patch.object(
            uprc.github,
            "update_issue_comment",
            side_effect=fake_update_issue_comment,
        ),
    ):
        assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 0
    assert write_counter["count"] == 2


def test_upsert_comment_fails_closed_for_unreadable_existing_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Corrupted existing state retries, then stops without overwriting the comment."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "SUCCESS",
        "failure_label": "",
        "details_url": "",
        "details_text": "",
    }
    existing_comment = {"id": 10, "body": uprc.MARKER}
    create_comment = mock.Mock()
    update_comment = mock.Mock()

    monkeypatch.setattr(uprc, "find_existing_comment", lambda *_args: existing_comment)
    monkeypatch.setattr(uprc.github, "create_issue_comment", create_comment)
    monkeypatch.setattr(uprc.github, "update_issue_comment", update_comment)
    monkeypatch.setattr(
        "release_service_utils.helpers.retry.retry.time.sleep",
        lambda _: None,
    )
    assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 1
    create_comment.assert_not_called()
    update_comment.assert_not_called()


def test_upsert_comment_updates_existing_comment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Existing sticky comments are updated in place."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "create-advisory",
        "details_url": "https://example.com/plr",
        "details_text": "rate limited",
    }
    existing_body = uprc.render_comment_body([])
    updated_comment: dict[str, object] = {"id": 99, "body": existing_body}

    responses = [
        {"id": 99, "body": existing_body},
        updated_comment,
    ]

    def fake_find_existing_comment(*_args: object) -> dict[str, object]:
        return responses.pop(0)

    def fake_update_issue_comment(
        _session: object,
        comment_id: int,
        body: str,
    ) -> dict[str, object]:
        assert comment_id == 99
        updated_comment["body"] = body
        return {"id": 99, "body": body}

    monkeypatch.setattr(uprc, "find_existing_comment", fake_find_existing_comment)
    monkeypatch.setattr(uprc.github, "update_issue_comment", fake_update_issue_comment)
    monkeypatch.setattr(uprc.github, "create_issue_comment", mock.Mock())
    assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 0


def test_upsert_comment_retries_then_succeeds_on_request_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-HTTP request failure while writing is retried."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "create-advisory",
        "details_url": "https://example.com/plr",
        "details_text": "rate limited",
    }
    created_comment = {"id": 100, "body": ""}
    comment_state = {"body": None}
    write_counter = {"count": 0}

    def fake_find_existing_comment(*_args: object) -> dict[str, object] | None:
        if comment_state["body"] is None:
            return None
        created_comment["body"] = comment_state["body"]
        return created_comment

    def fake_create_issue_comment(
        _session: object,
        _repo_name: str,
        _pr_number: int,
        body: str,
    ) -> dict[str, object]:
        write_counter["count"] += 1
        if write_counter["count"] == 1:
            raise requests.RequestException("boom")
        comment_state["body"] = body
        return {"id": 100, "body": body}

    monkeypatch.setattr(uprc, "find_existing_comment", fake_find_existing_comment)
    monkeypatch.setattr(uprc.github, "create_issue_comment", fake_create_issue_comment)
    monkeypatch.setattr(
        "release_service_utils.helpers.retry.retry.time.sleep",
        lambda _: None,
    )
    assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 0


def test_upsert_comment_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Transient write failures are retried."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "create-advisory",
        "details_url": "https://example.com/plr",
        "details_text": "rate limited",
    }
    created_comment = {"id": 100, "body": ""}
    comment_state = {"body": None}
    read_counter = {"count": 0}
    write_counter = {"count": 0}

    def fake_find_existing_comment(*_args: object) -> dict[str, object] | None:
        read_counter["count"] += 1
        if read_counter["count"] == 1:
            return None
        if comment_state["body"] is None:
            return None
        created_comment["body"] = comment_state["body"]
        return created_comment

    def fake_create_issue_comment(
        _session: object,
        _repo_name: str,
        _pr_number: int,
        body: str,
    ) -> dict[str, object]:
        write_counter["count"] += 1
        if write_counter["count"] == 1:
            response = mock.Mock()
            response.status_code = 403
            response.text = '{"message":"Must have admin rights to Repository."}'
            response.json.return_value = {"message": "Must have admin rights to Repository."}
            error = requests.HTTPError("bad", response=response)
            raise error
        comment_state["body"] = body
        return {"id": 100, "body": body}

    def fake_update_issue_comment(
        _session: object,
        _comment_id: int,
        body: str,
    ) -> dict[str, object]:
        write_counter["count"] += 1
        comment_state["body"] = body
        return {"id": 100, "body": body}

    monkeypatch.setattr(uprc, "find_existing_comment", fake_find_existing_comment)
    monkeypatch.setattr(uprc.github, "create_issue_comment", fake_create_issue_comment)
    monkeypatch.setattr(uprc.github, "update_issue_comment", fake_update_issue_comment)
    monkeypatch.setattr(
        "release_service_utils.helpers.retry.retry.time.sleep",
        lambda _: None,
    )
    assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 0
    assert write_counter["count"] == 2


def test_upsert_comment_retries_when_post_write_read_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient readback failure after write retries the whole upsert."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "create-advisory",
        "details_url": "https://example.com/plr",
        "details_text": "rate limited",
    }
    comment_state = {"body": None}
    read_counter = {"count": 0}
    write_counter = {"count": 0}

    def fake_find_existing_comment(*_args: object) -> dict[str, object] | None:
        read_counter["count"] += 1
        if read_counter["count"] == 2:
            raise requests.RequestException("temporary readback failure")
        if comment_state["body"] is None:
            return None
        return {"id": 100, "body": comment_state["body"]}

    def fake_create_issue_comment(
        _session: object,
        _repo_name: str,
        _pr_number: int,
        body: str,
    ) -> dict[str, object]:
        write_counter["count"] += 1
        comment_state["body"] = body
        return {"id": 100, "body": body}

    def fake_update_issue_comment(
        _session: object,
        _comment_id: int,
        body: str,
    ) -> dict[str, object]:
        write_counter["count"] += 1
        comment_state["body"] = body
        return {"id": 100, "body": body}

    monkeypatch.setattr(uprc, "find_existing_comment", fake_find_existing_comment)
    monkeypatch.setattr(uprc.github, "create_issue_comment", fake_create_issue_comment)
    monkeypatch.setattr(uprc.github, "update_issue_comment", fake_update_issue_comment)
    monkeypatch.setattr(
        "release_service_utils.helpers.retry.retry.time.sleep",
        lambda _: None,
    )
    assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 0
    assert write_counter["count"] == 2


def test_upsert_comment_retries_when_post_write_state_is_unreadable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unreadable stored state after write retries and then fails closed."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "create-advisory",
        "details_url": "https://example.com/plr",
        "details_text": "rate limited",
    }
    read_counter = {"count": 0}
    write_counter = {"count": 0}

    def fake_find_existing_comment(*_args: object) -> dict[str, object] | None:
        read_counter["count"] += 1
        if read_counter["count"] % 2 == 1:
            return None
        return {"id": 100, "body": uprc.MARKER}

    def fake_create_issue_comment(
        _session: object,
        _repo_name: str,
        _pr_number: int,
        _body: str,
    ) -> dict[str, object]:
        write_counter["count"] += 1
        return {"id": 100, "body": "written"}

    monkeypatch.setattr(uprc, "find_existing_comment", fake_find_existing_comment)
    monkeypatch.setattr(uprc.github, "create_issue_comment", fake_create_issue_comment)
    monkeypatch.setattr(
        "release_service_utils.helpers.retry.retry.time.sleep",
        lambda _: None,
    )
    assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 1
    assert write_counter["count"] == uprc.MAX_UPDATE_ATTEMPTS


def test_upsert_comment_returns_failure_when_readback_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exhausted retries after unreadable readback return a nonzero result."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "create-advisory",
        "details_url": "https://example.com/plr",
        "details_text": "rate limited",
    }

    monkeypatch.setattr(
        uprc,
        "find_existing_comment",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        uprc.github,
        "create_issue_comment",
        lambda *_args, **_kwargs: {"id": 100, "body": "written"},
    )
    monkeypatch.setattr(
        "release_service_utils.helpers.retry.retry.time.sleep",
        lambda _: None,
    )
    assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 1


def test_upsert_comment_returns_failure_when_state_mismatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exhausted retries after stale state validation return a nonzero result."""
    session = object()
    metadata = {
        "its_key": "collectors",
        "its_name": "collectors",
        "result": "FAILURE",
        "failure_label": "create-advisory",
        "details_url": "https://example.com/plr",
        "details_text": "rate limited",
    }
    calls = {"count": 0}

    def fake_find_existing_comment(*_args: object) -> dict[str, object] | None:
        calls["count"] += 1
        if calls["count"] % 2 == 1:
            return None
        return {"id": 100, "body": uprc.render_comment_body([])}

    monkeypatch.setattr(uprc, "find_existing_comment", fake_find_existing_comment)
    monkeypatch.setattr(
        uprc.github,
        "create_issue_comment",
        lambda *_args, **_kwargs: {"id": 100, "body": "written"},
    )
    monkeypatch.setattr(
        "release_service_utils.helpers.retry.retry.time.sleep",
        lambda _: None,
    )
    assert uprc.upsert_comment(session, "org/repo", 12, metadata) == 1


def test_main_returns_zero_when_metadata_is_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Main exits successfully without requiring a token path when metadata is skipped."""
    monkeypatch.delenv("RUN_TEST_METADATA_JSON", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN_PATH", raising=False)
    monkeypatch.setattr(uprc, "parse_args", lambda: ("org/repo", 12))
    bearer = mock.Mock()
    monkeypatch.setattr(uprc.github, "bearer_token_session", bearer)
    assert uprc.main() == 0
    bearer.assert_not_called()


def test_load_metadata_accepts_skipped_result() -> None:
    """Skipped metadata is accepted as a final result."""
    metadata = uprc.load_metadata('{"its_name":"collectors","result":"SKIPPED"}')
    assert metadata is not None
    assert metadata["its_key"] == "collectors"
    assert metadata["result"] == "SKIPPED"


def test_main_reads_token_file_and_calls_upsert(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Main reads the mounted token path and passes the stripped token onward."""
    token_path = tmp_path / "token"
    token_path.write_text(" token-value \n", encoding="utf-8")
    monkeypatch.setenv("GITHUB_TOKEN_PATH", str(token_path))
    monkeypatch.setenv(
        "RUN_TEST_METADATA_JSON",
        '{"its_name":"collectors","result":"FAILURE","failure_label":"task"}',
    )
    monkeypatch.setattr(uprc, "parse_args", lambda: ("org/repo", 12))
    session = object()
    bearer = mock.Mock(return_value=session)
    upsert = mock.Mock(return_value=0)
    monkeypatch.setattr(uprc.github, "bearer_token_session", bearer)
    monkeypatch.setattr(uprc, "upsert_comment", upsert)
    assert uprc.main() == 0
    bearer.assert_called_once_with("token-value")
    upsert.assert_called_once()


def test_module_main_guard_exits_zero_for_skipped_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Running the module as `__main__` exits through the main guard."""
    token_path = tmp_path / "token"
    token_path.write_text("token-value", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["update_pr_results_comment.py", "org/repo", "12"])
    monkeypatch.setenv("GITHUB_TOKEN_PATH", str(token_path))
    monkeypatch.delenv("RUN_TEST_METADATA_JSON", raising=False)
    with pytest.raises(SystemExit) as excinfo:
        runpy.run_module("update_pr_results_comment", run_name="__main__")
    assert excinfo.value.code == 0
