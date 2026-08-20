"""Unit tests for sign_index_image."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from rh_direct_sign_image import PyxisSignature
from sign_index_image import (
    FbcSigningItem,
    SignIndexSubmitConfig,
    batch_items,
    collect_fbc_items,
    filter_already_signed_items,
    find_existing_signatures_with_retry,
    main,
    resolve_umb_topics,
    submit_all_batches,
    submit_batch,
)

TRANSLATE_OUTPUT = json.dumps(
    [
        {"repo": "quay.io", "url": "quay.io/redhat/redhat----fbc-index:v4.23"},
        {"repo": "redhat.io", "url": "registry.redhat.io/redhat/fbc-index:v4.23"},
    ]
)

CONFIGMAP = {
    "data": {
        "SIG_KEY_NAME": "test-signing-key",
        "SIGNER_TYPE": "batch",
        "UMB_LISTEN_TOPIC": "VirtualTopic.eng.listen",
        "UMB_PUBLISH_TOPIC": "VirtualTopic.eng.publish",
        "UMB_BATCH_LISTEN_TOPIC": "VirtualTopic.eng.batch.listen",
        "UMB_BATCH_PUBLISH_TOPIC": "VirtualTopic.eng.batch.publish",
        "PYXIS_URL": "https://pyxis.example.com",
        "PYXIS_SSL_CERT_SECRET_NAME": "pyxis-cert-secret",
        "UMB_CLIENT_NAME": "test-umb-client",
        "UMB_URL": "umb.example.com",
        "UMB_SSL_CERT_SECRET_NAME": "umb-cert-secret",
    }
}

FBC_RESULTS = {
    "components": [
        {
            "target_index": "quay.io/redhat/redhat----fbc-index:v4.23",
            "rh-registry-repo": "registry.redhat.io/redhat/fbc-index",
            "image_digests": ["sha256:aaa", "sha256:bbb"],
        }
    ]
}


# --- resolve_umb_topics ---


def test_resolve_umb_topics_batch_uses_batch_keys() -> None:
    """Batch signer type resolves UMB_BATCH_*_TOPIC keys."""
    listen, publish = resolve_umb_topics(CONFIGMAP["data"], "batch")

    assert listen == "VirtualTopic.eng.batch.listen"
    assert publish == "VirtualTopic.eng.batch.publish"


def test_resolve_umb_topics_batch_falls_back_to_non_batch() -> None:
    """Batch signer falls back to non-batch keys when batch keys are absent."""
    data = {
        "UMB_LISTEN_TOPIC": "fallback.listen",
        "UMB_PUBLISH_TOPIC": "fallback.publish",
    }
    listen, publish = resolve_umb_topics(data, "batch")

    assert listen == "fallback.listen"
    assert publish == "fallback.publish"


def test_resolve_umb_topics_single_uses_non_batch_keys() -> None:
    """Single signer type uses UMB_LISTEN_TOPIC and UMB_PUBLISH_TOPIC."""
    listen, publish = resolve_umb_topics(CONFIGMAP["data"], "single")

    assert listen == "VirtualTopic.eng.listen"
    assert publish == "VirtualTopic.eng.publish"


# --- collect_fbc_items ---


@patch("sign_index_image.translate_reference")
def test_collect_fbc_items_basic(mock_translate) -> None:
    """Creates an FbcSigningItem for each digest."""
    mock_translate.return_value = "registry.redhat.io/redhat/fbc-index:v4.23"
    items = collect_fbc_items(FBC_RESULTS)

    assert len(items) == 2
    assert items[0] == FbcSigningItem(
        "registry.redhat.io/redhat/fbc-index:v4.23",
        "sha256:aaa",
        "redhat/fbc-index",
    )
    assert items[1] == FbcSigningItem(
        "registry.redhat.io/redhat/fbc-index:v4.23",
        "sha256:bbb",
        "redhat/fbc-index",
    )


@patch("sign_index_image.translate_reference")
def test_collect_fbc_items_multiple_components(mock_translate) -> None:
    """Items are collected from all components."""
    mock_translate.side_effect = [
        "registry.redhat.io/redhat/index-a:v4.23",
        "registry.redhat.io/redhat/index-b:v4.23",
    ]
    fbc = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----index-a:v4.23",
                "rh-registry-repo": "registry.redhat.io/redhat/index-a",
                "image_digests": ["sha256:aaa"],
            },
            {
                "target_index": "quay.io/redhat/redhat----index-b:v4.23",
                "rh-registry-repo": "registry.redhat.io/redhat/index-b",
                "image_digests": ["sha256:bbb"],
            },
        ]
    }
    items = collect_fbc_items(fbc)

    assert len(items) == 2
    assert items[0].reference == "registry.redhat.io/redhat/index-a:v4.23"
    assert items[0].repository == "redhat/index-a"
    assert items[1].reference == "registry.redhat.io/redhat/index-b:v4.23"
    assert items[1].repository == "redhat/index-b"


@patch("sign_index_image.translate_reference")
def test_collect_fbc_items_empty_components(mock_translate) -> None:
    """No items when components list is empty."""
    items = collect_fbc_items({"components": []})

    assert items == []
    mock_translate.assert_not_called()


@patch("sign_index_image.translate_reference")
def test_collect_fbc_items_no_digests(mock_translate) -> None:
    """A component with no image_digests produces no items."""
    mock_translate.return_value = "registry.redhat.io/redhat/fbc-index:v4.23"
    fbc = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-index:v4.23",
                "rh-registry-repo": "registry.redhat.io/redhat/fbc-index",
                "image_digests": [],
            }
        ]
    }
    items = collect_fbc_items(fbc)

    assert items == []


@patch("sign_index_image.translate_reference")
def test_collect_fbc_items_missing_rh_registry_repo(mock_translate) -> None:
    """Missing rh-registry-repo defaults to empty string for repository."""
    mock_translate.return_value = "registry.redhat.io/redhat/fbc-index:v4.23"
    fbc = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-index:v4.23",
                "image_digests": ["sha256:aaa"],
            }
        ]
    }
    items = collect_fbc_items(fbc)

    assert items[0].repository == ""


# --- find_existing_signatures_with_retry ---


@patch("sign_index_image.wait_for_memory")
@patch("sign_index_image.find_signatures_for_repository")
def test_find_existing_signatures_with_retry_success(mock_find, _mock_wait) -> None:
    """Successful lookup returns signatures."""
    sigs = {PyxisSignature("ref:tag", "key-a")}
    mock_find.return_value = sigs

    result = find_existing_signatures_with_retry(
        "https://pyxis.example.com/graphql/",
        {("sha256:aaa", "repo/img")},
        max_workers=1,
        max_attempts=1,
    )

    assert result == {("sha256:aaa", "repo/img"): sigs}
    mock_find.assert_called_once()


@patch("sign_index_image.wait_for_memory")
@patch("sign_index_image.retry_with_exponential_backoff")
def test_find_existing_signatures_with_retry_uses_retry(mock_retry, _mock_wait) -> None:
    """Retry helper is invoked for each lookup."""
    sigs = {PyxisSignature("ref:tag", "key-a")}
    mock_retry.return_value = (("sha256:aaa", "repo/img"), sigs)

    find_existing_signatures_with_retry(
        "https://pyxis.example.com/graphql/",
        {("sha256:aaa", "repo/img")},
        max_workers=1,
        max_attempts=3,
    )

    mock_retry.assert_called_once()
    assert mock_retry.call_args.kwargs["max_attempts"] == 3
    assert mock_retry.call_args.kwargs["base_sleep_seconds"] == 2


@patch("sign_index_image.wait_for_memory")
@patch("sign_index_image.find_signatures_for_repository")
def test_find_existing_signatures_with_retry_propagates_error(mock_find, _mock_wait) -> None:
    """Errors propagate after retries are exhausted."""
    mock_find.side_effect = RuntimeError("Pyxis down")

    with pytest.raises(RuntimeError, match="Pyxis down"):
        find_existing_signatures_with_retry(
            "https://pyxis.example.com/graphql/",
            {("sha256:aaa", "repo/img")},
            max_workers=1,
            max_attempts=1,
        )


@patch("sign_index_image.wait_for_memory")
@patch("sign_index_image.find_signatures_for_repository")
def test_find_existing_signatures_with_retry_calls_wait_for_memory(
    mock_find, mock_wait
) -> None:
    """Each lookup worker checks memory pressure before executing."""
    mock_find.return_value = set()

    find_existing_signatures_with_retry(
        "https://pyxis.example.com/graphql/",
        {("sha256:aaa", "repo/img")},
        max_workers=1,
        max_attempts=1,
    )

    mock_wait.assert_called_once()


# --- filter_already_signed_items ---


def test_filter_already_signed_items_all_signed() -> None:
    """Items with all keys signed are filtered out."""
    items = [FbcSigningItem("ref:tag", "sha256:aaa", "repo")]
    sigs = {
        PyxisSignature("ref:tag", "key-a"),
        PyxisSignature("ref:tag", "key-b"),
    }
    with patch(
        "sign_index_image.find_existing_signatures_with_retry",
        return_value={("sha256:aaa", "repo"): sigs},
    ):
        result = filter_already_signed_items(
            items, ["key-a", "key-b"], "https://pyxis.example.com/graphql/"
        )

    assert result == []


def test_filter_already_signed_items_partially_signed() -> None:
    """Items with only some keys signed are kept."""
    items = [FbcSigningItem("ref:tag", "sha256:aaa", "repo")]
    sigs = {PyxisSignature("ref:tag", "key-a")}
    with patch(
        "sign_index_image.find_existing_signatures_with_retry",
        return_value={("sha256:aaa", "repo"): sigs},
    ):
        result = filter_already_signed_items(
            items, ["key-a", "key-b"], "https://pyxis.example.com/graphql/"
        )

    assert result == items


def test_filter_already_signed_items_no_signatures() -> None:
    """Items with no existing signatures are kept."""
    items = [FbcSigningItem("ref:tag", "sha256:aaa", "repo")]
    with patch(
        "sign_index_image.find_existing_signatures_with_retry",
        return_value={("sha256:aaa", "repo"): set()},
    ):
        result = filter_already_signed_items(
            items, ["key-a"], "https://pyxis.example.com/graphql/"
        )

    assert result == items


def test_filter_already_signed_items_pyxis_failure_fail_true() -> None:
    """Pyxis failure raises when fail_on_error is True."""
    items = [FbcSigningItem("ref:tag", "sha256:aaa", "repo")]
    with (
        patch(
            "sign_index_image.find_existing_signatures_with_retry",
            side_effect=RuntimeError("Pyxis down"),
        ),
        pytest.raises(RuntimeError, match="Pyxis down"),
    ):
        filter_already_signed_items(
            items,
            ["key-a"],
            "https://pyxis.example.com/graphql/",
            fail_on_error=True,
        )


def test_filter_already_signed_items_pyxis_failure_fail_false() -> None:
    """Pyxis failure returns all items when fail_on_error is False."""
    items = [
        FbcSigningItem("ref:tag", "sha256:aaa", "repo"),
        FbcSigningItem("ref:tag", "sha256:bbb", "repo"),
    ]
    with patch(
        "sign_index_image.find_existing_signatures_with_retry",
        side_effect=RuntimeError("Pyxis down"),
    ):
        result = filter_already_signed_items(
            items,
            ["key-a"],
            "https://pyxis.example.com/graphql/",
            fail_on_error=False,
        )

    assert result == items


def test_filter_already_signed_items_missing_digest_repo_pair() -> None:
    """Items whose (digest, repo) pair has no Pyxis entry are kept."""
    items = [FbcSigningItem("ref:tag", "sha256:aaa", "repo")]
    with patch(
        "sign_index_image.find_existing_signatures_with_retry",
        return_value={},
    ):
        result = filter_already_signed_items(
            items, ["key-a"], "https://pyxis.example.com/graphql/"
        )

    assert result == items


# --- batch_items ---


def test_batch_items_single_batch() -> None:
    """All items fit in one batch."""
    items = [
        FbcSigningItem("ref1:tag", "sha256:aaa", "repo1"),
        FbcSigningItem("ref2:tag", "sha256:bbb", "repo2"),
    ]
    batches = batch_items(items, batch_limit=4096)

    assert len(batches) == 1
    assert batches[0] == items


def test_batch_items_split_by_limit() -> None:
    """Items split across batches when exceeding limit."""
    items = [
        FbcSigningItem("ref1:tag", "sha256:aaa", "repo1"),
        FbcSigningItem("ref2:tag", "sha256:bbb", "repo2"),
    ]
    batches = batch_items(items, batch_limit=15)

    assert len(batches) == 2
    assert batches[0] == [items[0]]
    assert batches[1] == [items[1]]


def test_batch_items_empty_input() -> None:
    """Empty input returns empty batches."""
    assert batch_items([], batch_limit=4096) == []


def test_batch_items_split_on_digests_exceeding_limit() -> None:
    """Batch splits when digests exceed the limit even if references fit."""
    items = [
        FbcSigningItem("r", "sha256:aaaaaaaaaaa", "repo"),
        FbcSigningItem("r", "sha256:bbbbbbbbbbb", "repo"),
    ]
    batches = batch_items(items, batch_limit=20)

    assert len(batches) == 2


def test_batch_items_exact_limit_no_split() -> None:
    """Two items whose joined references exactly equal the limit stay in one batch."""
    items = [
        FbcSigningItem("ab", "cd", "ef"),
        FbcSigningItem("gh", "ij", "kl"),
    ]
    batches = batch_items(items, batch_limit=5)

    assert len(batches) == 1
    assert batches[0] == items


# --- submit_batch / submit_all_batches ---


@pytest.fixture
def submit_config() -> SignIndexSubmitConfig:
    """Default SignIndexSubmitConfig for submit/batch tests."""
    return SignIndexSubmitConfig(
        request_type="internal-request",
        pipeline="simple-signing-pipeline",
        requester="testuser",
        config_map_name="signing-config-map",
        umb_listen_topic="topic.listen",
        umb_publish_topic="topic.publish",
        signing_pyxis_url="https://pyxis.example.com",
        signing_umb_url="umb.example.com",
        signing_umb_client="client",
        signing_pyxis_ssl_secret="pyxis-cert",
        signing_umb_ssl_secret="umb-cert",
        signer_type="batch",
        signing_key_names="key-a",
        task_git_url="https://git.example.com",
        task_git_revision="main",
        task_id="uid-123",
        pipelinerun_uid="pr-456",
        intention="staging",
        request_timeout="1800",
    )


def test_submit_batch_success(submit_config) -> None:
    """Successful submission does not raise."""
    items = [FbcSigningItem("ref:tag", "sha256:aaa", "repo")]
    with (
        patch("sign_index_image.wait_for_memory"),
        patch("sign_index_image.run_cmd") as mock_run,
    ):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        submit_batch(items, submit_config)

    mock_run.assert_called_once()
    cmd = mock_run.call_args.args[0]
    assert cmd[0] == "internal-request"
    assert "--pipeline" in cmd
    assert "-s" in cmd


def test_submit_batch_failure_raises(submit_config) -> None:
    """Failed submission raises RuntimeError."""
    items = [FbcSigningItem("ref:tag", "sha256:aaa", "repo")]
    with (
        patch("sign_index_image.wait_for_memory"),
        patch("sign_index_image.run_cmd") as mock_run,
        pytest.raises(RuntimeError, match="Batch submission failed"),
    ):
        mock_run.return_value = MagicMock(returncode=1, stderr="connection refused")
        submit_batch(items, submit_config)


def test_submit_batch_includes_extra_args(submit_config) -> None:
    """Extra args like --service-account are appended to the command."""
    config = replace(
        submit_config,
        request_type="internal-pipelinerun",
        extra_args=["--service-account", "release-sa"],
    )
    items = [FbcSigningItem("ref:tag", "sha256:aaa", "repo")]
    with (
        patch("sign_index_image.wait_for_memory"),
        patch("sign_index_image.run_cmd") as mock_run,
    ):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        submit_batch(items, config)

    cmd = mock_run.call_args.args[0]
    assert cmd[0] == "internal-pipelinerun"
    sa_idx = cmd.index("--service-account")
    assert cmd[sa_idx + 1] == "release-sa"


def test_submit_batch_calls_wait_for_memory(submit_config) -> None:
    """submit_batch checks memory pressure before executing."""
    items = [FbcSigningItem("ref:tag", "sha256:aaa", "repo")]
    with (
        patch("sign_index_image.wait_for_memory") as mock_wait,
        patch("sign_index_image.run_cmd") as mock_run,
    ):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        submit_batch(items, submit_config)

    mock_wait.assert_called_once()


def test_submit_all_batches_success(submit_config) -> None:
    """All batches submitted successfully."""
    batch = [FbcSigningItem("ref", "dig", "repo")]
    with patch("sign_index_image.submit_batch") as mock_sub:
        submit_all_batches([batch], submit_config)

    mock_sub.assert_called_once_with(batch, submit_config)


def test_submit_all_batches_partial_failure_raises(submit_config) -> None:
    """RuntimeError raised when at least one batch fails."""
    batches = [
        [FbcSigningItem("ref1", "dig1", "repo1")],
        [FbcSigningItem("ref2", "dig2", "repo2")],
    ]
    with (
        patch(
            "sign_index_image.submit_batch",
            side_effect=[None, RuntimeError("fail")],
        ),
        pytest.raises(RuntimeError, match="1 batch"),
    ):
        submit_all_batches(batches, submit_config)


# --- main ---


def _setup_files(
    tmp_path: Path,
    data: dict | None = None,
    fbc: dict | None = None,
    rpa: dict | None = None,
) -> tuple[Path, Path, Path]:
    """Create input files and return (data_path, fbc_path, rpa_path)."""
    data_content = data or {
        "intention": "staging",
        "sign": {"configMapName": "signing-config-map"},
    }
    data_path = tmp_path / "data.json"
    data_path.write_text(json.dumps(data_content))

    fbc_content = fbc or FBC_RESULTS
    fbc_path = tmp_path / "fbc_results.json"
    fbc_path.write_text(json.dumps(fbc_content))

    rpa_content = rpa or {
        "spec": {"pipeline": {"serviceAccountName": "release-service-account"}}
    }
    rpa_path = tmp_path / "rpa.json"
    rpa_path.write_text(json.dumps(rpa_content))

    (tmp_path / "cert").write_text("dummy-cert")
    (tmp_path / "key").write_text("dummy-key")

    return data_path, fbc_path, rpa_path


def _base_env(data_path: Path, fbc_path: Path, rpa_path: Path) -> dict[str, str]:
    """Build a minimal env dict for main()."""
    cert_dir = data_path.parent
    return {
        "DATA_FILE": str(data_path),
        "FBC_RESULTS_FILE": str(fbc_path),
        "RPA_FILE": str(rpa_path),
        "PYXIS_SERVER": "stage",
        "REQUESTER": "testuser",
        "REQUEST_TIMEOUT": "1800",
        "PIPELINE_RUN_UID": "pr-uid-123",
        "CONCURRENT_LIMIT": "8",
        "BATCH_LIMIT": "4096",
        "FAIL_ON_SIGNATURE_LOOKUP_ERROR": "true",
        "SIGNATURE_LOOKUP_MAX_ATTEMPTS": "3",
        "TASK_RUN_UID": "task-uid-456",
        "TASK_GIT_URL": "https://git.example.com",
        "TASK_GIT_REVISION": "main",
        "PYXIS_CERT_PATH": str(cert_dir / "cert"),
        "PYXIS_KEY_PATH": str(cert_dir / "key"),
    }


def test_main_happy_path(tmp_path, monkeypatch) -> None:
    """Main submits batches for unsigned items."""
    data_path, fbc_path, rpa_path = _setup_files(tmp_path)
    env = _base_env(data_path, fbc_path, rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    item = FbcSigningItem("ref:tag", "sha256:aaa", "repo")
    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("sign_index_image.collect_fbc_items", return_value=[item]),
        patch(
            "sign_index_image.filter_already_signed_items",
            return_value=[item],
        ),
        patch("sign_index_image.submit_all_batches") as mock_submit,
        patch("sign_index_image.pyxis") as mock_pyxis,
    ):
        main()

    mock_submit.assert_called_once()
    mock_pyxis._get_session.assert_called_once()


def test_main_no_candidates_succeeds(tmp_path, monkeypatch) -> None:
    """Main succeeds when no signing candidates exist."""
    data_path, fbc_path, rpa_path = _setup_files(tmp_path, fbc={"components": []})
    env = _base_env(data_path, fbc_path, rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("sign_index_image.submit_all_batches") as mock_submit,
        patch("sign_index_image.pyxis"),
    ):
        main()

    mock_submit.assert_not_called()


def test_main_all_already_signed_succeeds(tmp_path, monkeypatch) -> None:
    """Main succeeds when all items are already signed."""
    data_path, fbc_path, rpa_path = _setup_files(tmp_path)
    env = _base_env(data_path, fbc_path, rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    item = FbcSigningItem("ref:tag", "sha256:aaa", "repo")
    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("sign_index_image.collect_fbc_items", return_value=[item]),
        patch("sign_index_image.filter_already_signed_items", return_value=[]),
        patch("sign_index_image.submit_all_batches") as mock_submit,
        patch("sign_index_image.pyxis"),
    ):
        main()

    mock_submit.assert_not_called()


def test_main_invalid_pyxis_server(tmp_path, monkeypatch) -> None:
    """Main raises ValueError for an invalid PYXIS_SERVER."""
    data_path, fbc_path, rpa_path = _setup_files(tmp_path)
    env = _base_env(data_path, fbc_path, rpa_path)
    env["PYXIS_SERVER"] = "invalid"
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    with pytest.raises(ValueError, match="Invalid PYXIS_SERVER"):
        main()


def test_main_missing_data_file(tmp_path, monkeypatch) -> None:
    """Main raises FileNotFoundError when data file does not exist."""
    _, fbc_path, rpa_path = _setup_files(tmp_path)
    env = _base_env(tmp_path / "nonexistent.json", fbc_path, rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.pyxis"),
        pytest.raises(FileNotFoundError),
    ):
        main()


def test_main_missing_fbc_results_file(tmp_path, monkeypatch) -> None:
    """Main raises FileNotFoundError when FBC results file does not exist."""
    data_path, _, rpa_path = _setup_files(tmp_path)
    env = _base_env(data_path, tmp_path / "nonexistent.json", rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.pyxis"),
        pytest.raises(FileNotFoundError),
    ):
        main()


def test_main_internal_pipelinerun_reads_rpa(tmp_path, monkeypatch) -> None:
    """Main reads RPA file and uses service account for internal-pipelinerun."""
    data_path, fbc_path, rpa_path = _setup_files(
        tmp_path,
        data={
            "intention": "prod",
            "requestType": "internal-pipelinerun",
            "sign": {"configMapName": "signing-config-map"},
        },
    )
    env = _base_env(data_path, fbc_path, rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    item = FbcSigningItem("ref:tag", "sha256:aaa", "repo")
    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("sign_index_image.collect_fbc_items", return_value=[item]),
        patch(
            "sign_index_image.filter_already_signed_items",
            return_value=[item],
        ),
        patch("sign_index_image.submit_all_batches") as mock_submit,
        patch("sign_index_image.pyxis"),
    ):
        main()

    config = mock_submit.call_args.args[1]
    assert config.request_type == "internal-pipelinerun"
    assert config.extra_args == [
        "--service-account",
        "release-service-account",
    ]


def test_main_internal_pipelinerun_missing_rpa(tmp_path, monkeypatch) -> None:
    """Main raises FileNotFoundError when RPA file is missing for internal-pipelinerun."""
    data_path, fbc_path, _ = _setup_files(
        tmp_path,
        data={
            "requestType": "internal-pipelinerun",
            "sign": {"configMapName": "signing-config-map"},
        },
    )
    env = _base_env(data_path, fbc_path, tmp_path / "missing_rpa.json")
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("sign_index_image.pyxis"),
        pytest.raises(FileNotFoundError),
    ):
        main()


def test_main_uses_configmap_name_from_data(tmp_path, monkeypatch) -> None:
    """ConfigMap name is read from data.sign.configMapName."""
    data_path, fbc_path, rpa_path = _setup_files(
        tmp_path,
        data={"sign": {"configMapName": "custom-cm"}},
        fbc={"components": []},
    )
    env = _base_env(data_path, fbc_path, rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP) as mock_cm,
        patch("sign_index_image.pyxis"),
    ):
        main()

    mock_cm.assert_called_once_with("custom-cm")


def test_main_defaults_configmap_name(tmp_path, monkeypatch) -> None:
    """Default ConfigMap name is used when sign.configMapName is absent."""
    data_path, fbc_path, rpa_path = _setup_files(tmp_path, data={}, fbc={"components": []})
    env = _base_env(data_path, fbc_path, rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP) as mock_cm,
        patch("sign_index_image.pyxis"),
    ):
        main()

    mock_cm.assert_called_once_with("signing-config-map")


def test_main_pipeline_config_map_fallback_to_fbc(tmp_path, monkeypatch) -> None:
    """Pipeline configMapName falls back to fbc.configMapName."""
    data_path, fbc_path, rpa_path = _setup_files(
        tmp_path,
        data={"fbc": {"configMapName": "fbc-cm"}},
        fbc={"components": []},
    )
    env = _base_env(data_path, fbc_path, rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    item = FbcSigningItem("ref:tag", "sha256:aaa", "repo")
    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("sign_index_image.collect_fbc_items", return_value=[item]),
        patch(
            "sign_index_image.filter_already_signed_items",
            return_value=[item],
        ),
        patch("sign_index_image.submit_all_batches") as mock_submit,
        patch("sign_index_image.pyxis"),
    ):
        main()

    config = mock_submit.call_args.args[1]
    assert config.config_map_name == "fbc-cm"


def test_main_invalid_max_attempts_defaults_to_3(tmp_path, monkeypatch) -> None:
    """Invalid SIGNATURE_LOOKUP_MAX_ATTEMPTS defaults to 3."""
    data_path, fbc_path, rpa_path = _setup_files(tmp_path)
    env = _base_env(data_path, fbc_path, rpa_path)
    env["SIGNATURE_LOOKUP_MAX_ATTEMPTS"] = "0"
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    item = FbcSigningItem("ref:tag", "sha256:aaa", "repo")
    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("sign_index_image.collect_fbc_items", return_value=[item]),
        patch(
            "sign_index_image.filter_already_signed_items",
            return_value=[item],
        ) as mock_filter,
        patch("sign_index_image.submit_all_batches"),
        patch("sign_index_image.pyxis"),
    ):
        main()

    assert mock_filter.call_args.kwargs["max_attempts"] == 3


def test_main_nonnumeric_max_attempts_defaults_to_3(tmp_path, monkeypatch) -> None:
    """Non-numeric SIGNATURE_LOOKUP_MAX_ATTEMPTS defaults to 3."""
    data_path, fbc_path, rpa_path = _setup_files(tmp_path)
    env = _base_env(data_path, fbc_path, rpa_path)
    env["SIGNATURE_LOOKUP_MAX_ATTEMPTS"] = "abc"
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    item = FbcSigningItem("ref:tag", "sha256:aaa", "repo")
    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("sign_index_image.collect_fbc_items", return_value=[item]),
        patch(
            "sign_index_image.filter_already_signed_items",
            return_value=[item],
        ) as mock_filter,
        patch("sign_index_image.submit_all_batches"),
        patch("sign_index_image.pyxis"),
    ):
        main()

    assert mock_filter.call_args.kwargs["max_attempts"] == 3


def test_main_submit_config_fields(tmp_path, monkeypatch) -> None:
    """Verify SubmitConfig fields are populated correctly."""
    data_path, fbc_path, rpa_path = _setup_files(
        tmp_path,
        data={
            "intention": "staging",
            "sign": {
                "configMapName": "signing-config-map",
                "request": "custom-pipeline",
            },
        },
    )
    env = _base_env(data_path, fbc_path, rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    item = FbcSigningItem("ref:tag", "sha256:aaa", "repo")
    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("sign_index_image.collect_fbc_items", return_value=[item]),
        patch(
            "sign_index_image.filter_already_signed_items",
            return_value=[item],
        ),
        patch("sign_index_image.submit_all_batches") as mock_submit,
        patch("sign_index_image.pyxis"),
    ):
        main()

    config = mock_submit.call_args.args[1]
    assert config.pipeline == "custom-pipeline"
    assert config.requester == "testuser"
    assert config.umb_listen_topic == "VirtualTopic.eng.batch.listen"
    assert config.signing_pyxis_url == "https://pyxis.example.com"
    assert config.signer_type == "batch"
    assert config.signing_key_names == "test-signing-key"
    assert config.intention == "staging"
    assert config.task_id == "task-uid-456"
    assert config.pipelinerun_uid == "pr-uid-123"


def test_main_signing_key_names_newline_separated(tmp_path, monkeypatch) -> None:
    """Multiple signing keys are joined with newlines to match shell jq output."""
    multi_key_configmap = {
        "data": {
            **CONFIGMAP["data"],
            "SIG_KEY_NAMES": "key-a, key-b",
        }
    }
    data_path, fbc_path, rpa_path = _setup_files(tmp_path)
    env = _base_env(data_path, fbc_path, rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    item = FbcSigningItem("ref:tag", "sha256:aaa", "repo")
    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=multi_key_configmap),
        patch("sign_index_image.collect_fbc_items", return_value=[item]),
        patch(
            "sign_index_image.filter_already_signed_items",
            return_value=[item],
        ),
        patch("sign_index_image.submit_all_batches") as mock_submit,
        patch("sign_index_image.pyxis"),
    ):
        main()

    config = mock_submit.call_args.args[1]
    assert config.signing_key_names == "key-a\nkey-b"


def test_main_fail_on_lookup_false(tmp_path, monkeypatch) -> None:
    """FAIL_ON_SIGNATURE_LOOKUP_ERROR=false is passed through."""
    data_path, fbc_path, rpa_path = _setup_files(tmp_path)
    env = _base_env(data_path, fbc_path, rpa_path)
    env["FAIL_ON_SIGNATURE_LOOKUP_ERROR"] = "false"
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    item = FbcSigningItem("ref:tag", "sha256:aaa", "repo")
    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("sign_index_image.collect_fbc_items", return_value=[item]),
        patch(
            "sign_index_image.filter_already_signed_items",
            return_value=[item],
        ) as mock_filter,
        patch("sign_index_image.submit_all_batches"),
        patch("sign_index_image.pyxis"),
    ):
        main()

    assert mock_filter.call_args.kwargs["fail_on_error"] is False


def test_main_missing_pyxis_cert(tmp_path, monkeypatch) -> None:
    """Main raises FileNotFoundError when PYXIS_CERT_PATH points to a missing file."""
    data_path, fbc_path, rpa_path = _setup_files(tmp_path)
    env = _base_env(data_path, fbc_path, rpa_path)
    env["PYXIS_CERT_PATH"] = str(tmp_path / "nonexistent_cert")
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    with pytest.raises(FileNotFoundError, match="nonexistent_cert"):
        main()


def test_main_empty_pyxis_key(tmp_path, monkeypatch) -> None:
    """Main raises FileNotFoundError when PYXIS_KEY_PATH points to an empty file."""
    data_path, fbc_path, rpa_path = _setup_files(tmp_path)
    empty_key = tmp_path / "empty_key"
    empty_key.write_text("")
    env = _base_env(data_path, fbc_path, rpa_path)
    env["PYXIS_KEY_PATH"] = str(empty_key)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    with pytest.raises(FileNotFoundError, match="empty_key"):
        main()


def test_main_raises_when_signing_keys_empty(tmp_path, monkeypatch) -> None:
    """Main raises ValueError when configmap yields empty signing keys."""
    empty_keys_configmap = {
        "data": {
            **CONFIGMAP["data"],
            "SIG_KEY_NAMES": "   ",
        }
    }
    data_path, fbc_path, rpa_path = _setup_files(tmp_path)
    env = _base_env(data_path, fbc_path, rpa_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    with (
        patch("sign_index_image.log_memory_throttle_status"),
        patch("sign_index_image.get_configmap", return_value=empty_keys_configmap),
        patch("sign_index_image.pyxis"),
        pytest.raises(ValueError, match="No signing keys found"),
    ):
        main()
