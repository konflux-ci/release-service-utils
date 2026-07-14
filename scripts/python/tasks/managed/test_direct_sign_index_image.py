"""Unit tests for direct_sign_index_image."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from direct_sign_index_image import (
    collect_fbc_signing_items,
    main,
    setup_argparser,
    translate_reference,
)
from rh_direct_sign_image import SigningItem

TRANSLATE_OUTPUT = json.dumps(
    [
        {"repo": "quay.io", "url": "quay.io/redhat/redhat----fbc-target-index:v4.23"},
        {"repo": "redhat.io", "url": "registry.redhat.io/redhat/fbc-target-index:v4.23"},
    ]
)

FBC_RESULTS = {
    "components": [
        {
            "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.23",
            "rh-registry-repo": "registry.redhat.io/redhat/fbc-target-index",
            "image_digests": ["sha256:aaa", "sha256:bbb"],
        }
    ]
}

CONFIGMAP = {
    "data": {
        "SIG_KEY_NAME": "test-signing-key",
        "PYXIS_SSL_CERT_SECRET_NAME": "pyxis-cert-secret",
        "PYXIS_GRAPHQL_URL": "https://graphql-pyxis.example.com/graphql/",
        "KERBEROS_PRINCIPAL": "svc@REALM",
        "KERBEROS_KEYTAB": "/etc/keytab",
        "KERBEROS_KEYTAB_SECRET": "keytab-secret",
    }
}


# --- translate_reference ---


def test_translate_reference_returns_redhat_io_url() -> None:
    """The redhat.io entry URL is returned from translate-delivery-repo output."""
    with patch("direct_sign_index_image.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout=TRANSLATE_OUTPUT)
        result = translate_reference("quay.io/redhat/redhat----fbc-target-index:v4.23")

    assert result == "registry.redhat.io/redhat/fbc-target-index:v4.23"


def test_translate_reference_calls_translate_delivery_repo() -> None:
    """translate-delivery-repo is called with the target_index as the argument."""
    with patch("direct_sign_index_image.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout=TRANSLATE_OUTPUT)
        translate_reference("quay.io/some-org/some-image:v1.0")

    mock_run.assert_called_once_with(
        ["translate-delivery-repo", "quay.io/some-org/some-image:v1.0"]
    )


def test_translate_reference_raises_when_no_redhat_io_entry() -> None:
    """ValueError is raised when translate-delivery-repo returns no redhat.io entry."""
    output = json.dumps([{"repo": "quay.io", "url": "quay.io/some/image:tag"}])
    with patch("direct_sign_index_image.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout=output)
        with pytest.raises(ValueError, match="No redhat.io entry"):
            translate_reference("quay.io/some/image:tag")


def test_translate_reference_raises_on_empty_output() -> None:
    """ValueError is raised when translate-delivery-repo returns an empty list."""
    with patch("direct_sign_index_image.run_cmd") as mock_run:
        mock_run.return_value = MagicMock(stdout="[]")
        with pytest.raises(ValueError, match="No redhat.io entry"):
            translate_reference("quay.io/some/image:tag")


# --- collect_fbc_signing_items ---


@patch("direct_sign_index_image.translate_reference")
def test_collect_fbc_signing_items_basic(mock_translate) -> None:
    """Creates a SigningItem for each (digest, key) combination."""
    mock_translate.return_value = "registry.redhat.io/redhat/fbc-target-index:v4.23"
    items = collect_fbc_signing_items(FBC_RESULTS, ["key-a"])

    assert len(items) == 2
    assert items[0] == SigningItem(
        "registry.redhat.io/redhat/fbc-target-index:v4.23",
        "sha256:aaa",
        "redhat/fbc-target-index",
        "key-a",
    )
    assert items[1] == SigningItem(
        "registry.redhat.io/redhat/fbc-target-index:v4.23",
        "sha256:bbb",
        "redhat/fbc-target-index",
        "key-a",
    )


@patch("direct_sign_index_image.translate_reference")
def test_collect_fbc_signing_items_multiple_keys(mock_translate) -> None:
    """Each digest is combined with every signing key."""
    mock_translate.return_value = "registry.redhat.io/redhat/fbc-target-index:v4.23"
    fbc = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.23",
                "rh-registry-repo": "registry.redhat.io/redhat/fbc-target-index",
                "image_digests": ["sha256:aaa", "sha256:bbb"],
            }
        ]
    }
    items = collect_fbc_signing_items(fbc, ["key-a", "key-b"])

    assert len(items) == 4
    assert items[0] == SigningItem(
        "registry.redhat.io/redhat/fbc-target-index:v4.23",
        "sha256:aaa",
        "redhat/fbc-target-index",
        "key-a",
    )
    assert items[1] == SigningItem(
        "registry.redhat.io/redhat/fbc-target-index:v4.23",
        "sha256:aaa",
        "redhat/fbc-target-index",
        "key-b",
    )
    assert items[2] == SigningItem(
        "registry.redhat.io/redhat/fbc-target-index:v4.23",
        "sha256:bbb",
        "redhat/fbc-target-index",
        "key-a",
    )
    assert items[3] == SigningItem(
        "registry.redhat.io/redhat/fbc-target-index:v4.23",
        "sha256:bbb",
        "redhat/fbc-target-index",
        "key-b",
    )


@patch("direct_sign_index_image.translate_reference")
def test_collect_fbc_signing_items_multiple_components(mock_translate) -> None:
    """Items are collected from all components."""
    mock_translate.side_effect = [
        "registry.redhat.io/redhat/fbc-index-a:v4.23",
        "registry.redhat.io/redhat/fbc-index-b:v4.23",
    ]
    fbc = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-index-a:v4.23",
                "rh-registry-repo": "registry.redhat.io/redhat/fbc-index-a",
                "image_digests": ["sha256:aaa"],
            },
            {
                "target_index": "quay.io/redhat/redhat----fbc-index-b:v4.23",
                "rh-registry-repo": "registry.redhat.io/redhat/fbc-index-b",
                "image_digests": ["sha256:bbb"],
            },
        ]
    }
    items = collect_fbc_signing_items(fbc, ["key-a"])

    assert len(items) == 2
    assert items[0].reference == "registry.redhat.io/redhat/fbc-index-a:v4.23"
    assert items[0].repository == "redhat/fbc-index-a"
    assert items[1].reference == "registry.redhat.io/redhat/fbc-index-b:v4.23"
    assert items[1].repository == "redhat/fbc-index-b"
    assert mock_translate.call_count == 2


@patch("direct_sign_index_image.translate_reference")
def test_collect_fbc_signing_items_empty_components(mock_translate) -> None:
    """No items are returned when the components list is empty."""
    items = collect_fbc_signing_items({"components": []}, ["key-a"])

    assert items == []
    mock_translate.assert_not_called()


@patch("direct_sign_index_image.translate_reference")
def test_collect_fbc_signing_items_no_digests(mock_translate) -> None:
    """A component with no image_digests produces no items."""
    mock_translate.return_value = "registry.redhat.io/redhat/fbc-target-index:v4.23"
    fbc = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.23",
                "rh-registry-repo": "registry.redhat.io/redhat/fbc-target-index",
                "image_digests": [],
            }
        ]
    }
    items = collect_fbc_signing_items(fbc, ["key-a"])

    assert items == []
    mock_translate.assert_called_once()


@patch("direct_sign_index_image.translate_reference")
def test_collect_fbc_signing_items_missing_rh_registry_repo(mock_translate) -> None:
    """Missing rh-registry-repo defaults to empty string for repository."""
    mock_translate.return_value = "registry.redhat.io/redhat/fbc-target-index:v4.23"
    fbc = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.23",
                "image_digests": ["sha256:aaa"],
            }
        ]
    }
    items = collect_fbc_signing_items(fbc, ["key-a"])

    assert items[0].repository == ""


@patch("direct_sign_index_image.translate_reference")
def test_collect_fbc_signing_items_with_timestamp(mock_translate) -> None:
    """Items are created for both target_index and target_index_with_timestamp."""
    mock_translate.side_effect = [
        "registry.redhat.io/redhat/fbc-target-index:v4.23",
        "registry.redhat.io/redhat/fbc-target-index:v4.23-1783502029",
    ]
    fbc = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.23",
                "target_index_with_timestamp": (
                    "quay.io/redhat/redhat----fbc-target-index:v4.23-1783502029"
                ),
                "rh-registry-repo": "registry.redhat.io/redhat/fbc-target-index",
                "image_digests": ["sha256:aaa"],
            }
        ]
    }
    items = collect_fbc_signing_items(fbc, ["key-a"])

    assert len(items) == 2
    assert items[0].reference == "registry.redhat.io/redhat/fbc-target-index:v4.23"
    assert items[1].reference == "registry.redhat.io/redhat/fbc-target-index:v4.23-1783502029"
    assert mock_translate.call_count == 2


@patch("direct_sign_index_image.translate_reference")
def test_collect_fbc_signing_items_timestamp_empty(mock_translate) -> None:
    """Empty target_index_with_timestamp is skipped."""
    mock_translate.return_value = "registry.redhat.io/redhat/fbc-target-index:v4.23"
    fbc = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.23",
                "target_index_with_timestamp": "",
                "rh-registry-repo": "registry.redhat.io/redhat/fbc-target-index",
                "image_digests": ["sha256:aaa"],
            }
        ]
    }
    items = collect_fbc_signing_items(fbc, ["key-a"])

    assert len(items) == 1
    assert items[0].reference == "registry.redhat.io/redhat/fbc-target-index:v4.23"
    mock_translate.assert_called_once()


@patch("direct_sign_index_image.translate_reference")
def test_collect_fbc_signing_items_timestamp_equals_target(mock_translate) -> None:
    """target_index_with_timestamp equal to target_index does not create duplicates."""
    mock_translate.return_value = "registry.redhat.io/redhat/fbc-target-index:v4.23"
    fbc = {
        "components": [
            {
                "target_index": "quay.io/redhat/redhat----fbc-target-index:v4.23",
                "target_index_with_timestamp": (
                    "quay.io/redhat/redhat----fbc-target-index:v4.23"
                ),
                "rh-registry-repo": "registry.redhat.io/redhat/fbc-target-index",
                "image_digests": ["sha256:aaa"],
            }
        ]
    }
    items = collect_fbc_signing_items(fbc, ["key-a"])

    assert len(items) == 1
    assert items[0].reference == "registry.redhat.io/redhat/fbc-target-index:v4.23"
    mock_translate.assert_called_once()


# --- setup_argparser ---


def test_setup_argparser_defaults(tmp_path) -> None:
    """Default values are set for optional arguments."""
    fbc = tmp_path / "fbc.json"
    fbc.write_text("{}")
    data = tmp_path / "data.json"
    data.write_text("{}")

    parser = setup_argparser()
    args = parser.parse_args(
        [
            "--fbc-results",
            str(fbc),
            "--pyxis-server",
            "stage",
            "--data-file",
            str(data),
            "--requester",
            "testuser",
            "--pipeline-image",
            "quay.io/signing:latest",
        ]
    )

    assert args.batch_max_size == 14 * 1024
    assert args.fail_on_lookup_error == "true"
    assert args.max_workers == 10
    assert args.verbose is False
    assert args.output is None
    assert args.pipeline == "container-signing"
    assert args.service_account == "signing-pipeline-sa"
    assert args.request_timeout == "1800"
    assert args.concurrent_limit == 8


def test_setup_argparser_rejects_invalid_pyxis_server(tmp_path) -> None:
    """An invalid pyxis-server value causes a parse error."""
    fbc = tmp_path / "fbc.json"
    fbc.write_text("{}")
    data = tmp_path / "data.json"
    data.write_text("{}")

    parser = setup_argparser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--fbc-results",
                str(fbc),
                "--pyxis-server",
                "invalid-server",
                "--data-file",
                str(data),
                "--requester",
                "testuser",
                "--pipeline-image",
                "quay.io/signing:latest",
            ]
        )


def test_setup_argparser_validates_fbc_results_file(tmp_path) -> None:
    """--fbc-results raises FileNotFoundError for a missing file."""
    data = tmp_path / "data.json"
    data.write_text("{}")

    parser = setup_argparser()
    with pytest.raises(FileNotFoundError):
        parser.parse_args(
            [
                "--fbc-results",
                str(tmp_path / "nonexistent.json"),
                "--pyxis-server",
                "stage",
                "--data-file",
                str(data),
                "--requester",
                "testuser",
                "--pipeline-image",
                "quay.io/signing:latest",
            ]
        )


# --- main ---


def _setup_main_fixtures(
    tmp_path: Path,
    fbc_results: dict | None = None,
    data: dict | None = None,
) -> tuple[Path, Path, Path]:
    """Create input files and return (fbc_path, data_path, output_dir)."""
    fbc_path = tmp_path / "fbc_results.json"
    fbc_path.write_text(json.dumps(fbc_results or FBC_RESULTS))

    data_content = data or {"sign": {"configMapName": "signing-config-map"}}
    data_path = tmp_path / "data.json"
    data_path.write_text(json.dumps(data_content))

    output_dir = tmp_path / "batches"
    return fbc_path, data_path, output_dir


def _base_argv(
    fbc_path: Path,
    data_path: Path,
    output_dir: Path | None = None,
    **overrides: str,
) -> list[str]:
    """Build a sys.argv list with required and optional flags."""
    argv = [
        "direct_sign_index_image",
        "--fbc-results",
        str(fbc_path),
        "--pyxis-server",
        overrides.pop("pyxis_server", "stage"),
        "--data-file",
        str(data_path),
        "--requester",
        overrides.pop("requester", "testuser"),
        "--pipeline-image",
        overrides.pop("pipeline_image", "quay.io/signing:latest"),
    ]
    if output_dir is not None:
        argv += ["--output", str(output_dir)]
    for key, value in overrides.items():
        argv += [f"--{key.replace('_', '-')}", str(value)]
    return argv


def test_main_happy_path_writes_batches_and_submits(tmp_path) -> None:
    """Main writes batch files and calls submit_batches."""
    fbc_path, data_path, output_dir = _setup_main_fixtures(tmp_path)

    item = SigningItem(
        "registry.redhat.io/redhat/fbc-target-index:v4.23",
        "sha256:aaa",
        "redhat/fbc-target-index",
        "test-signing-key",
    )
    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("direct_sign_index_image.collect_fbc_signing_items", return_value=[item]),
        patch("direct_sign_index_image.filter_already_signed", return_value=[item]),
        patch("direct_sign_index_image.submit_batches") as mock_submit,
        patch("sys.argv", _base_argv(fbc_path, data_path, output_dir)),
    ):
        exit_code = main()

    assert exit_code == 0
    batch_files = list(output_dir.glob("batch_*.txt"))
    assert len(batch_files) == 1
    mock_submit.assert_called_once()
    assert mock_submit.call_args.args[0] == output_dir


def test_main_no_candidates_returns_0_without_submitting(tmp_path) -> None:
    """Main returns 0 and skips submission when no signing candidates are found."""
    fbc_path, data_path, output_dir = _setup_main_fixtures(
        tmp_path, fbc_results={"components": []}
    )

    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("direct_sign_index_image.submit_batches") as mock_submit,
        patch("sys.argv", _base_argv(fbc_path, data_path, output_dir)),
    ):
        exit_code = main()

    assert exit_code == 0
    mock_submit.assert_not_called()


def test_main_all_already_signed_returns_0_without_submitting(tmp_path) -> None:
    """Main returns 0 and skips submission when all items are already signed."""
    fbc_path, data_path, output_dir = _setup_main_fixtures(tmp_path)

    item = SigningItem("ref", "sha256:aaa", "repo", "key")
    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("direct_sign_index_image.collect_fbc_signing_items", return_value=[item]),
        patch("direct_sign_index_image.filter_already_signed", return_value=[]),
        patch("direct_sign_index_image.submit_batches") as mock_submit,
        patch("sys.argv", _base_argv(fbc_path, data_path, output_dir)),
    ):
        exit_code = main()

    assert exit_code == 0
    mock_submit.assert_not_called()


def test_main_pyxis_failure_with_fail_on_error_raises(tmp_path) -> None:
    """Pyxis lookup failure raises when fail_on_lookup_error is true."""
    fbc_path, data_path, output_dir = _setup_main_fixtures(tmp_path)

    item = SigningItem("ref", "sha256:aaa", "repo", "key")
    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("direct_sign_index_image.collect_fbc_signing_items", return_value=[item]),
        patch(
            "direct_sign_index_image.filter_already_signed",
            side_effect=RuntimeError("Pyxis connection failed"),
        ),
        patch("direct_sign_index_image.submit_batches") as mock_submit,
        patch(
            "sys.argv",
            _base_argv(fbc_path, data_path, output_dir, fail_on_lookup_error="true"),
        ),
        pytest.raises(RuntimeError, match="Pyxis connection failed"),
    ):
        main()

    mock_submit.assert_not_called()


def test_main_pyxis_failure_with_fail_on_error_false_submits_all(tmp_path) -> None:
    """Pyxis lookup failure submits all items when fail_on_lookup_error is false."""
    fbc_path, data_path, output_dir = _setup_main_fixtures(tmp_path)

    items = [
        SigningItem("ref1", "sha256:aaa", "repo", "key"),
        SigningItem("ref2", "sha256:bbb", "repo", "key"),
    ]
    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("direct_sign_index_image.collect_fbc_signing_items", return_value=items),
        patch(
            "direct_sign_index_image.filter_already_signed",
            side_effect=RuntimeError("Pyxis connection failed"),
        ),
        patch("direct_sign_index_image.submit_batches") as mock_submit,
        patch(
            "sys.argv",
            _base_argv(fbc_path, data_path, output_dir, fail_on_lookup_error="false"),
        ),
    ):
        exit_code = main()

    assert exit_code == 0
    mock_submit.assert_called_once()
    batch_files = list(output_dir.glob("batch_*.txt"))
    assert len(batch_files) >= 1


def test_main_uses_configmap_name_from_data_file(tmp_path) -> None:
    """The ConfigMap name is read from data.sign.configMapName."""
    fbc_path, data_path, output_dir = _setup_main_fixtures(
        tmp_path,
        data={"sign": {"configMapName": "custom-config-map"}},
        fbc_results={"components": []},
    )

    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP) as mock_cm,
        patch("sys.argv", _base_argv(fbc_path, data_path, output_dir)),
    ):
        main()

    mock_cm.assert_called_once_with("custom-config-map")


def test_main_defaults_configmap_name_when_missing(tmp_path) -> None:
    """The default ConfigMap name is used when data.sign.configMapName is absent."""
    fbc_path, data_path, output_dir = _setup_main_fixtures(
        tmp_path,
        data={"other": "field"},
        fbc_results={"components": []},
    )

    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP) as mock_cm,
        patch("sys.argv", _base_argv(fbc_path, data_path, output_dir)),
    ):
        main()

    mock_cm.assert_called_once_with("signing-config-map")


def test_main_passes_max_workers_to_filter(tmp_path) -> None:
    """The --max-workers value is forwarded to filter_already_signed."""
    fbc_path, data_path, output_dir = _setup_main_fixtures(tmp_path)

    item = SigningItem("ref", "sha256:aaa", "repo", "key")
    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("direct_sign_index_image.collect_fbc_signing_items", return_value=[item]),
        patch(
            "direct_sign_index_image.filter_already_signed", return_value=[item]
        ) as mock_filter,
        patch("direct_sign_index_image.submit_batches"),
        patch(
            "sys.argv",
            _base_argv(fbc_path, data_path, output_dir, max_workers="5"),
        ),
    ):
        main()

    _, pyxis_url = mock_filter.call_args.args
    mock_filter.assert_called_once_with([item], pyxis_url, max_workers=5)


def test_main_translate_failure_raises(tmp_path) -> None:
    """A translate_reference failure during collection raises."""
    fbc_path, data_path, output_dir = _setup_main_fixtures(tmp_path)

    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch(
            "direct_sign_index_image.collect_fbc_signing_items",
            side_effect=ValueError("No redhat.io entry"),
        ),
        patch("direct_sign_index_image.submit_batches") as mock_submit,
        patch("sys.argv", _base_argv(fbc_path, data_path, output_dir)),
        pytest.raises(ValueError, match="No redhat.io entry"),
    ):
        main()

    mock_submit.assert_not_called()


def test_main_uses_temp_dir_when_output_not_specified(tmp_path) -> None:
    """Main uses a temporary directory for batches when --output is omitted."""
    fbc_path, data_path, _ = _setup_main_fixtures(tmp_path)

    item = SigningItem("ref", "sha256:aaa", "repo", "key")
    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("direct_sign_index_image.collect_fbc_signing_items", return_value=[item]),
        patch("direct_sign_index_image.filter_already_signed", return_value=[item]),
        patch("direct_sign_index_image.submit_batches") as mock_submit,
        patch("sys.argv", _base_argv(fbc_path, data_path)),
    ):
        exit_code = main()

    assert exit_code == 0
    mock_submit.assert_called_once()
    batch_dir = mock_submit.call_args.args[0]
    assert batch_dir.is_dir()
    assert len(list(batch_dir.glob("batch_*.txt"))) == 1


def test_main_passes_submit_config_to_submit_batches(tmp_path) -> None:
    """Main builds SubmitConfig from configmap and args and passes it to submit_batches."""
    fbc_path, data_path, output_dir = _setup_main_fixtures(tmp_path)

    item = SigningItem("ref", "sha256:aaa", "repo", "key")
    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("direct_sign_index_image.collect_fbc_signing_items", return_value=[item]),
        patch("direct_sign_index_image.filter_already_signed", return_value=[item]),
        patch("direct_sign_index_image.submit_batches") as mock_submit,
        patch(
            "sys.argv",
            _base_argv(
                fbc_path,
                data_path,
                output_dir,
                pipeline="custom-signing",
                service_account="custom-sa",
                task_id="uid-123",
                pipelinerun_uid="pr-456",
            ),
        ),
    ):
        exit_code = main()

    assert exit_code == 0
    submit_config = mock_submit.call_args.args[1]
    assert submit_config.pipeline == "custom-signing"
    assert submit_config.service_account == "custom-sa"
    assert submit_config.task_id == "uid-123"
    assert submit_config.pipelinerun_uid == "pr-456"
    assert submit_config.requester == "testuser"
    assert submit_config.pipeline_image == "quay.io/signing:latest"
    assert submit_config.pyxis_ssl_cert_secret_name == "pyxis-cert-secret"
    assert submit_config.kerberos_principal == "svc@REALM"


def test_main_submit_failure_raises(tmp_path) -> None:
    """Main raises when submit_batches fails."""
    fbc_path, data_path, output_dir = _setup_main_fixtures(tmp_path)

    item = SigningItem("ref", "sha256:aaa", "repo", "key")
    with (
        patch("direct_sign_index_image.get_configmap", return_value=CONFIGMAP),
        patch("direct_sign_index_image.collect_fbc_signing_items", return_value=[item]),
        patch("direct_sign_index_image.filter_already_signed", return_value=[item]),
        patch(
            "direct_sign_index_image.submit_batches",
            side_effect=RuntimeError("submission failed"),
        ),
        patch("sys.argv", _base_argv(fbc_path, data_path, output_dir)),
        pytest.raises(RuntimeError, match="submission failed"),
    ):
        main()
