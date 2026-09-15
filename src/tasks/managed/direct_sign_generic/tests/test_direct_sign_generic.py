"""Unit tests for direct_sign_generic."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from release_service_utils.tasks.managed.direct_sign_generic.direct_sign_generic import (
    SigningRequest,
    main,
    setup_argparser,
    submit,
)

MODULE = "release_service_utils.tasks.managed.direct_sign_generic.direct_sign_generic"


# --- submit ---


def test_submit_builds_params_and_labels() -> None:
    """Submit passes required params, splits git repos, and returns results."""
    with patch(f"{MODULE}.internal_request") as ir:
        ir.PIPELINERUN_UID_LABEL = "pl-uid"
        ir.create.return_value = "generic-signing-abc"
        ir.fetch_results.return_value = {"sourceDataArtifact": "oci://out"}

        results = submit(
            SigningRequest(
                source_data_artifact="oci://in",
                keyname="mykey",
                sign_method="detachsign",
                onbehalfof="alice",
                signing_repo="https://gitlab/signing.git",
                signing_revision="main",
                ta_task_git_url="https://github/catalog.git",
                ta_task_git_revision="production",
                pipelinerun_uid="uid-1",
                task_id="task-1",
            )
        )

    assert results == {"sourceDataArtifact": "oci://out"}
    ir.create.assert_called_once()
    call = ir.create.call_args
    assert call.args[0] == "generic-signing"
    params = call.kwargs["params"]
    assert params["sourceDataArtifact"] == "oci://in"
    assert params["keyname"] == "mykey"
    assert params["signMethod"] == "detachsign"
    assert params["onbehalfof"] == "alice"
    # signing repo -> taskGitUrl; catalog repo -> taTaskGitUrl
    assert params["taskGitUrl"] == "https://gitlab/signing.git"
    assert params["taskGitRevision"] == "main"
    assert params["taTaskGitUrl"] == "https://github/catalog.git"
    assert params["taTaskGitRevision"] == "production"

    labels = call.kwargs["labels"]
    assert labels["pl-uid"] == "uid-1"
    assert labels["internal-services.appstudio.openshift.io/rate-limited"] == "true"
    assert (
        labels["internal-services.appstudio.openshift.io/rate-limiting-group"]
        == "signing-server"
    )


def test_submit_omits_empty_optional_params() -> None:
    """Empty optional params are not forwarded so pipeline defaults apply."""
    with patch(f"{MODULE}.internal_request") as ir:
        ir.PIPELINERUN_UID_LABEL = "pl-uid"
        ir.create.return_value = "ir"
        ir.fetch_results.return_value = {}

        submit(
            SigningRequest(
                source_data_artifact="oci://in",
                keyname="k",
                sign_method="gpgsign",
                onbehalfof="bob",
                signing_repo="repo",
                signing_revision="rev",
            )
        )

    params = ir.create.call_args.kwargs["params"]
    assert "ociStorage" not in params
    assert "orasOptions" not in params
    assert "ociArtifactExpiresAfter" not in params
    assert "trustedArtifactsDebug" not in params
    assert "dataDir" not in params
    assert "taTaskGitUrl" not in params
    assert "taTaskGitRevision" not in params
    assert "caTrustConfigMapName" not in params
    assert "caTrustConfigMapKey" not in params
    assert "kerberos_keytab_secret" not in params
    assert "kerberos_keytab" not in params
    assert "kerberos_principal" not in params
    assert "trusted_artifacts_dockerconfig_secret" not in params
    assert "verbose" not in params


def test_submit_verbose_sets_flag() -> None:
    """verbose=True adds a 'verbose'='true' param."""
    with patch(f"{MODULE}.internal_request") as ir:
        ir.PIPELINERUN_UID_LABEL = "pl-uid"
        ir.create.return_value = "ir"
        ir.fetch_results.return_value = {}

        submit(
            SigningRequest(
                source_data_artifact="oci://in",
                keyname="k",
                sign_method="detachsign",
                onbehalfof="bob",
                signing_repo="repo",
                signing_revision="rev",
                verbose=True,
            )
        )

    assert ir.create.call_args.kwargs["params"]["verbose"] == "true"


# --- main / CLI ---


def test_main_writes_output_uri(tmp_path) -> None:
    """Main writes the output TA URI to --output."""
    out = tmp_path / "uri.txt"
    argv = [
        "--source-data-artifact",
        "oci://in",
        "--keyname",
        "k",
        "--sign-method",
        "detachsign",
        "--onbehalfof",
        "alice",
        "--output",
        str(out),
    ]
    with patch(f"{MODULE}.submit", return_value={"sourceDataArtifact": "oci://out"}):
        with patch("sys.argv", ["direct_sign_generic.py", *argv]):
            assert main() == 0
    assert out.read_text() == "oci://out"


def test_main_forwards_advanced_request_options(tmp_path) -> None:
    """Main forwards advanced Trusted Artifact and signing options."""
    argv = [
        "--source-data-artifact",
        "oci://in",
        "--keyname",
        "k",
        "--sign-method",
        "detachsign",
        "--onbehalfof",
        "alice",
        "--oci-artifact-expires-after",
        "7d",
        "--trusted-artifacts-debug",
        "true",
        "--data-dir",
        "/tmp/data",
        "--ta-task-git-url",
        "https://github/catalog.git",
        "--ta-task-git-revision",
        "main",
        "--ca-trust-config-map-name",
        "trusted-ca",
        "--ca-trust-config-map-key",
        "ca-bundle.crt",
        "--trusted-artifacts-dockerconfig-secret",
        "ta-registry-auth",
        "--kerberos-keytab-secret",
        "signing-keytab",
        "--kerberos-keytab",
        "keytab",
        "--kerberos-principal",
        "signer@EXAMPLE.COM",
    ]
    with patch(
        f"{MODULE}.submit", return_value={"sourceDataArtifact": "oci://out"}
    ) as submit_mock:
        with patch("sys.argv", ["direct_sign_generic.py", *argv]):
            assert main() == 0

    request = submit_mock.call_args.args[0]
    assert request.oci_artifact_expires_after == "7d"
    assert request.trusted_artifacts_debug == "true"
    assert request.data_dir == "/tmp/data"
    assert request.ta_task_git_url == "https://github/catalog.git"
    assert request.ca_trust_config_map_name == "trusted-ca"
    assert request.trusted_artifacts_dockerconfig_secret == "ta-registry-auth"
    assert request.kerberos_principal == "signer@EXAMPLE.COM"


def test_main_empty_result_raises() -> None:
    """Main raises when the pipeline returns no sourceDataArtifact."""
    argv = [
        "--source-data-artifact",
        "oci://in",
        "--keyname",
        "k",
        "--sign-method",
        "detachsign",
        "--onbehalfof",
        "alice",
    ]
    with patch(f"{MODULE}.submit", return_value={}):
        with patch("sys.argv", ["direct_sign_generic.py", *argv]):
            with pytest.raises(RuntimeError, match="empty sourceDataArtifact"):
                main()


def test_setup_argparser_rejects_bad_sign_method() -> None:
    """--sign-method only accepts gpgsign or detachsign."""
    parser = setup_argparser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--source-data-artifact",
                "oci://in",
                "--keyname",
                "k",
                "--sign-method",
                "bogus",
                "--onbehalfof",
                "a",
            ]
        )
