import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

import pulp_push_wrapper


@pytest.fixture()
def mock_gw_env_vars():
    with patch.dict(os.environ, {k: "test" for k in pulp_push_wrapper.EXODUS_ENV_VARS_STRICT}):
        yield


def test_no_args(capsys):
    with pytest.raises(SystemExit):
        pulp_push_wrapper.main()

    _, err = capsys.readouterr()
    assert (
        "pulp_push_wrapper: error: the following arguments are required: --source, --pulp-url"
        in err
    )


def test_dry_run(caplog, mock_gw_env_vars):
    args = [
        "",
        "--dry-run",
        "--source",
        "/test/1",
        "--source",
        "/test/2",
        "--pulp-url",
        "https://pulp-test.dev",
    ]

    with patch.object(sys, "argv", args):
        with caplog.at_level(logging.INFO):
            pulp_push_wrapper.main()
            assert "This is a dry-run!" in caplog.messages
            assert (
                "Would have run: pubtools-pulp-push --pulp-url https://pulp-test.dev"
                " --source staged:/test/1,/test/2 --clean"
            ) in caplog.messages


@patch("subprocess.run")
def test_basic_command(mock_run, caplog, mock_gw_env_vars):
    args = [
        "",
        "--source",
        "/test/1",
        "--source",
        "/test/2",
        "--pulp-url",
        "https://pulp-test.dev",
    ]

    with patch.object(sys, "argv", args):
        with caplog.at_level(logging.INFO):
            pulp_push_wrapper.main()
            assert "This is a dry-run!" not in caplog.messages
            assert (
                "Running pubtools-pulp-push --pulp-url https://pulp-test.dev"
                " --source staged:/test/1,/test/2 --clean"
            ) in caplog.messages

    mock_run.assert_called_once_with(
        [
            "pubtools-pulp-push",
            "--pulp-url",
            "https://pulp-test.dev",
            "--source",
            "staged:/test/1,/test/2",
            "--clean",
        ],
        check=True,
    )


@patch("subprocess.run")
def test_no_clean_flag(mock_run, caplog, mock_gw_env_vars):
    args = [
        "",
        "--source",
        "/test/1",
        "--pulp-url",
        "https://pulp-test.dev",
        "--no-clean",
    ]

    with patch.object(sys, "argv", args):
        with caplog.at_level(logging.INFO):
            pulp_push_wrapper.main()
            assert (
                "Running pubtools-pulp-push --pulp-url https://pulp-test.dev"
                " --source staged:/test/1"
            ) in caplog.messages

    mock_run.assert_called_once_with(
        [
            "pubtools-pulp-push",
            "--pulp-url",
            "https://pulp-test.dev",
            "--source",
            "staged:/test/1",
        ],
        check=True,
    )


def test_build_timestamp_search_patterns():
    patterns = pulp_push_wrapper.build_timestamp_search_patterns(
        "releng-test-product-1.7-1777068929-x86_64-boot.iso.gz"
    )
    assert r"^releng\-test\-product\-1\.7\-1777068929\-x86_64\-boot\.iso\.gz$" in patterns
    assert r"^releng-test-product-1\.7-\d{8,14}-x86_64-boot\.iso\.gz$" in patterns
    assert r"^releng\-test\-product\-1\.7\-x86_64\-boot\.iso\.gz$" in patterns


def test_get_source_dirs():
    assert pulp_push_wrapper.get_source_dirs("staged:/tmp/a,/tmp/b") == ["/tmp/a", "/tmp/b"]
    assert pulp_push_wrapper.get_source_dirs("docker://foo/bar") == []


def test_normalize_timestamped_name():
    assert (
        pulp_push_wrapper.normalize_timestamped_name("releng-test-product-1.7-1777068929-x86_64-boot.iso.gz")
        == "releng-test-product-1.7-x86_64-boot.iso.gz"
    )
    assert (
        pulp_push_wrapper.normalize_timestamped_name("releng-test-product-binaries-linux-amd64-1.7.0.tar.gz")
        == "releng-test-product-binaries-linux-amd64-1.7.0.tar.gz"
    )


def test_build_repo_file_map(tmp_path):
    repo = tmp_path / "repo-a" / "FILES"
    repo.mkdir(parents=True)
    (repo / "a.txt").write_text("a")
    (repo / "b.txt").write_text("b")
    (tmp_path / "repo-empty" / "FILES").mkdir(parents=True)

    result = pulp_push_wrapper.build_repo_file_map([str(tmp_path), "/does/not/exist"])
    assert result == {"repo-a": {"a.txt", "b.txt"}}


@patch("pulp_push_wrapper.ssl.create_default_context")
def test_make_ssl_context(mock_create_context):
    mock_ctx = MagicMock()
    mock_create_context.return_value = mock_ctx

    ctx = pulp_push_wrapper.make_ssl_context("/tmp/cert", "/tmp/key")
    assert ctx is mock_ctx
    mock_ctx.load_cert_chain.assert_called_once_with(certfile="/tmp/cert", keyfile="/tmp/key")


@patch("pulp_push_wrapper.request.urlopen")
def test_pulp_request_with_payload(mock_urlopen):
    mock_response = MagicMock()
    mock_response.read.return_value = b'{"ok": true}'
    mock_urlopen.return_value.__enter__.return_value = mock_response

    result = pulp_push_wrapper.pulp_request("https://example.com", context="ctx", payload={"a": 1})
    assert result == {"ok": True}


@patch("pulp_push_wrapper.request.urlopen")
def test_pulp_request_empty_body(mock_urlopen):
    mock_response = MagicMock()
    mock_response.read.return_value = b""
    mock_urlopen.return_value.__enter__.return_value = mock_response

    result = pulp_push_wrapper.pulp_request("https://example.com", context="ctx")
    assert result is None


@patch("pulp_push_wrapper.time.sleep")
@patch("pulp_push_wrapper.time.time")
@patch("pulp_push_wrapper.pulp_request")
def test_wait_for_task_success(mock_pulp_request, mock_time, _mock_sleep):
    mock_time.side_effect = [0, 1, 2]
    mock_pulp_request.side_effect = [{"state": "running"}, {"state": "finished"}]

    pulp_push_wrapper.wait_for_task("https://example.com/task", context="ctx")
    assert mock_pulp_request.call_count == 2


@patch("pulp_push_wrapper.time.sleep")
@patch("pulp_push_wrapper.time.time")
@patch("pulp_push_wrapper.pulp_request")
def test_wait_for_task_timeout(mock_pulp_request, mock_time, _mock_sleep):
    mock_time.side_effect = [0, 200]
    mock_pulp_request.return_value = {"state": "running"}

    with pytest.raises(TimeoutError):
        pulp_push_wrapper.wait_for_task("https://example.com/task", context="ctx")


@patch("subprocess.run")
@patch("pulp_push_wrapper.wait_for_task")
@patch("pulp_push_wrapper.make_ssl_context", return_value="ctx")
@patch("pulp_push_wrapper.pulp_request")
def test_prune_matching_content_before_push(
    mock_pulp_request,
    _mock_ctx,
    _mock_wait,
    mock_run,
    tmp_path,
    mock_gw_env_vars,
):
    repo = "konflux-release-e2e-1_DOT_0-for-rhel-10-x86_64-files"
    files_dir = tmp_path / repo / "FILES"
    files_dir.mkdir(parents=True)
    (files_dir / "releng-test-product-1.7-1777068929-x86_64-boot.iso.gz").write_text("boot")
    (files_dir / "releng-test-product-1.7-1777068929-x86_64-kvm.qcow2").write_text("kvm")

    mock_pulp_request.side_effect = [
        [
            {"metadata": {"name": "releng-test-product-1.7-1777068929-x86_64-boot.iso.gz"}},
            {"metadata": {"name": "releng-test-product-1.7-1777067342-x86_64-boot.iso.gz"}},
        ],
        [],
        [],
        [
            {"metadata": {"name": "releng-test-product-1.7-1777068929-x86_64-kvm.qcow2"}},
            {"metadata": {"name": "releng-test-product-1.7-1777067342-x86_64-kvm.qcow2"}},
        ],
        [],
        [],
        {"spawned_tasks": []},
    ]

    args = [
        "",
        "--source",
        str(tmp_path),
        "--pulp-url",
        "https://pulp-test.dev",
        "--pulp-cert",
        "/tmp/test.crt",
        "--pulp-key",
        "/tmp/test.key",
    ]

    with patch.object(sys, "argv", args):
        pulp_push_wrapper.main()

    unassociate_call = next(
        call
        for call in mock_pulp_request.call_args_list
        if "$in"
        in call.kwargs.get("payload", {})
        .get("criteria", {})
        .get("filters", {})
        .get("unit", {})
        .get("name", {})
    )
    payload = unassociate_call.kwargs["payload"]
    names = payload["criteria"]["filters"]["unit"]["name"]["$in"]
    assert set(names) == {
        "releng-test-product-1.7-1777068929-x86_64-boot.iso.gz",
        "releng-test-product-1.7-1777067342-x86_64-boot.iso.gz",
        "releng-test-product-1.7-1777068929-x86_64-kvm.qcow2",
        "releng-test-product-1.7-1777067342-x86_64-kvm.qcow2",
    }

    mock_run.assert_called_once()


@patch("pulp_push_wrapper.pulp_request")
def test_prune_matching_content_before_push_skips_on_no_clean(mock_pulp_request):
    args = MagicMock()
    args.no_clean = True
    pulp_push_wrapper.prune_matching_content_before_push(args)
    mock_pulp_request.assert_not_called()
