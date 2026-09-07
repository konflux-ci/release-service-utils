"""Test generic content signing."""

from __future__ import annotations

from unittest.mock import patch

from release_service_utils.helpers.direct_sign_generic.direct_sign_generic import (
    SigningRequest,
    submit,
)

MODULE = "release_service_utils.helpers.direct_sign_generic.direct_sign_generic"


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
                oci_storage="oci://storage",
                oras_options="--insecure",
                oci_artifact_expires_after="1d",
                trusted_artifacts_debug="true",
                data_dir="/var/workdir/release",
                ta_task_git_url="https://github/catalog.git",
                ta_task_git_revision="production",
                ca_trust_config_map_name="trusted-ca",
                ca_trust_config_map_key="ca-bundle.crt",
                trusted_artifacts_dockerconfig_secret="dockerconfig",
                kerberos_keytab_secret="keytab-secret",
                kerberos_keytab="keytab",
                kerberos_principal="signer@EXAMPLE.COM",
                verbose=True,
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
    assert params["ociStorage"] == "oci://storage"
    assert params["orasOptions"] == "--insecure"
    assert params["ociArtifactExpiresAfter"] == "1d"
    assert params["trustedArtifactsDebug"] == "true"
    assert params["dataDir"] == "/var/workdir/release"
    assert params["taTaskGitUrl"] == "https://github/catalog.git"
    assert params["taTaskGitRevision"] == "production"
    assert params["caTrustConfigMapName"] == "trusted-ca"
    assert params["caTrustConfigMapKey"] == "ca-bundle.crt"
    assert params["trusted_artifacts_dockerconfig_secret"] == "dockerconfig"
    assert params["kerberos_keytab_secret"] == "keytab-secret"
    assert params["kerberos_keytab"] == "keytab"
    assert params["kerberos_principal"] == "signer@EXAMPLE.COM"
    assert params["verbose"] == "true"
    assert "cleanup" not in call.kwargs

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
