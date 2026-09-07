#!/usr/bin/env python3
"""Sign generic content via the generic-signing pipeline.

Fires an InternalRequest against the signing service's Trusted Artifact based
``generic-signing`` pipeline, which signs every file in the input Trusted
Artifact and returns a new Trusted Artifact containing ``results.json`` and the
detached/gpg signatures.

Fetching that output Trusted Artifact is intentionally left to the caller (a
``use-trusted-artifact`` stepaction in the catalog task); this module only
submits the request and returns the InternalRequest results, which include the
output artifact's OCI URI.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_service_utils.helpers import internal_request
from release_service_utils.helpers.logger import logger as LOGGER

PIPELINE = "generic-signing"

# Labels mirroring rh_direct_sign_image so the signing server can rate-limit us.
_TASK_LABEL = "internal-services.appstudio.openshift.io/group-id"
_RATE_LIMITED_LABEL = "internal-services.appstudio.openshift.io/rate-limited"
_RATE_LIMITING_GROUP_LABEL = "internal-services.appstudio.openshift.io/rate-limiting-group"
_RATE_LIMITING_GROUP = "signing-server"


@dataclass(frozen=True)
class SigningRequest:
    """Parameters for a ``generic-signing`` InternalRequest.

    Required fields identify what to sign and how; the remaining fields are
    optional and only forwarded to the pipeline when non-empty so the
    pipeline's own defaults apply otherwise.

    Attributes:
        source_data_artifact: OCI URI of the input Trusted Artifact to sign.
        keyname: Signing key name passed to rh-signing-client.
        sign_method: Either "gpgsign" or "detachsign".
        onbehalfof: Requester identity for auditing. MUST be a human user.
        signing_repo: Git URL of the signing repo (pipeline ``taskGitUrl``).
        signing_revision: Git revision of the signing repo (``taskGitRevision``).
        pipeline: Internal pipeline name (default: "generic-signing").
        oci_storage: OCI repo for the output Trusted Artifact.
        oras_options: Extra oras CLI options.
        oci_artifact_expires_after: Expiration for the output Trusted Artifact.
        trusted_artifacts_debug: Non-empty to enable TA debug logging.
        data_dir: Working directory for Trusted Artifact data.
        ta_task_git_url: Git URL for resolving TA stepactions (catalog repo).
        ta_task_git_revision: Git revision for resolving TA stepactions.
        ca_trust_config_map_name: ConfigMap holding the CA bundle.
        ca_trust_config_map_key: Key in the CA bundle ConfigMap.
        trusted_artifacts_dockerconfig_secret: Secret with the OCI dockerconfig.
        kerberos_keytab_secret: Secret containing the Kerberos keytab.
        kerberos_keytab: Key within the keytab secret.
        kerberos_principal: Kerberos principal to use for signing.
        verbose: Enable verbose Kerberos diagnostics in the pipeline.
        task_id: Task run UID used as a label on the InternalRequest.
        pipelinerun_uid: Pipeline run UID used as a label on the InternalRequest.
        request_timeout: InternalRequest timeout in seconds.
        pipeline_timeout: Pipeline timeout in XhYmZs format.
        task_timeout: Task timeout in XhYmZs format.
        service_account: Service account for the signing PipelineRun.

    """

    source_data_artifact: str
    keyname: str
    sign_method: str
    onbehalfof: str
    signing_repo: str
    signing_revision: str
    pipeline: str = PIPELINE
    oci_storage: str = ""
    oras_options: str = ""
    oci_artifact_expires_after: str = ""
    trusted_artifacts_debug: str = ""
    data_dir: str = ""
    ta_task_git_url: str = ""
    ta_task_git_revision: str = ""
    ca_trust_config_map_name: str = ""
    ca_trust_config_map_key: str = ""
    trusted_artifacts_dockerconfig_secret: str = ""
    kerberos_keytab_secret: str = ""
    kerberos_keytab: str = ""
    kerberos_principal: str = ""
    verbose: bool = False
    task_id: str = ""
    pipelinerun_uid: str = ""
    request_timeout: int = 1800
    pipeline_timeout: str = "0h30m0s"
    task_timeout: str = "0h25m0s"
    service_account: str = "signing-pipeline-sa"


def submit(request: SigningRequest) -> dict[str, Any]:
    """Submit a generic-signing InternalRequest and return its results.

    Builds the ``generic-signing`` pipeline params from *request*, creates a
    synchronous InternalRequest, and returns its ``status.results`` mapping
    (which includes ``sourceDataArtifact`` -- the OCI URI of the output Trusted
    Artifact).

    Args:
        request: The signing request parameters.

    Returns:
        The InternalRequest ``status.results`` mapping.

    """
    params: dict[str, str] = {
        "sourceDataArtifact": request.source_data_artifact,
        "keyname": request.keyname,
        "signMethod": request.sign_method,
        "onbehalfof": request.onbehalfof,
        "taskGitUrl": request.signing_repo,
        "taskGitRevision": request.signing_revision,
    }

    optional_params = {
        "ociStorage": request.oci_storage,
        "orasOptions": request.oras_options,
        "ociArtifactExpiresAfter": request.oci_artifact_expires_after,
        "trustedArtifactsDebug": request.trusted_artifacts_debug,
        "dataDir": request.data_dir,
        "taTaskGitUrl": request.ta_task_git_url,
        "taTaskGitRevision": request.ta_task_git_revision,
        "caTrustConfigMapName": request.ca_trust_config_map_name,
        "caTrustConfigMapKey": request.ca_trust_config_map_key,
        "trusted_artifacts_dockerconfig_secret": (
            request.trusted_artifacts_dockerconfig_secret
        ),
        "kerberos_keytab_secret": request.kerberos_keytab_secret,
        "kerberos_keytab": request.kerberos_keytab,
        "kerberos_principal": request.kerberos_principal,
    }
    params.update({name: value for name, value in optional_params.items() if value})
    if request.verbose:
        params["verbose"] = "true"

    labels = {
        _TASK_LABEL: request.task_id,
        internal_request.PIPELINERUN_UID_LABEL: request.pipelinerun_uid,
        _RATE_LIMITED_LABEL: "true",
        _RATE_LIMITING_GROUP_LABEL: _RATE_LIMITING_GROUP,
    }

    LOGGER.info(
        "Submitting %s InternalRequest (signMethod=%s, onbehalfof=%s).",
        request.pipeline,
        request.sign_method,
        request.onbehalfof,
    )
    ir_name = internal_request.create(
        request.pipeline,
        params=params,
        labels=labels,
        sync=True,
        timeout=request.request_timeout,
        service_account=request.service_account,
        pipeline_timeout=request.pipeline_timeout,
        task_timeout=request.task_timeout,
        cleanup=False,
    )
    return internal_request.fetch_results(ir_name)


def setup_argparser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser.

    Returns:
        Configured argument parser.

    """
    parser = argparse.ArgumentParser(description="Sign generic content.")
    parser.add_argument(
        "--source-data-artifact",
        required=True,
        help="OCI URI of the input Trusted Artifact to sign",
    )
    parser.add_argument(
        "--keyname",
        required=True,
        help="Signing key name passed to rh-signing-client",
    )
    parser.add_argument(
        "--sign-method",
        required=True,
        choices=["gpgsign", "detachsign"],
        help="Signing method to use",
    )
    parser.add_argument(
        "--onbehalfof",
        required=True,
        help="Requester identity for auditing (must be a human user)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="File to write the output Trusted Artifact URI to (default: stdout)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    request = parser.add_argument_group("request submission")
    request.add_argument(
        "--pipeline",
        default=PIPELINE,
        help="Internal pipeline name for signing (default: %(default)s)",
    )
    request.add_argument(
        "--oci-storage",
        default="",
        help="OCI repository for the output Trusted Artifact",
    )
    request.add_argument(
        "--oras-options",
        default="",
        help="Extra oras CLI options",
    )
    request.add_argument(
        "--oci-artifact-expires-after",
        default="",
        help="Expiration for the output Trusted Artifact",
    )
    request.add_argument(
        "--trusted-artifacts-debug",
        default="",
        help="Enable Trusted Artifact debug logging",
    )
    request.add_argument(
        "--data-dir",
        default="",
        help="Working directory for Trusted Artifact data",
    )
    request.add_argument(
        "--ta-task-git-url",
        default="",
        help="Git URL for resolving Trusted Artifact stepactions",
    )
    request.add_argument(
        "--ta-task-git-revision",
        default="",
        help="Git revision for resolving Trusted Artifact stepactions",
    )
    request.add_argument(
        "--ca-trust-config-map-name",
        default="",
        help="ConfigMap containing the trusted CA bundle",
    )
    request.add_argument(
        "--ca-trust-config-map-key",
        default="",
        help="Key containing the trusted CA bundle",
    )
    request.add_argument(
        "--trusted-artifacts-dockerconfig-secret",
        default="",
        help="Secret containing Trusted Artifact registry credentials",
    )
    request.add_argument(
        "--kerberos-keytab-secret",
        default="",
        help="Secret containing the Kerberos keytab",
    )
    request.add_argument(
        "--kerberos-keytab",
        default="",
        help="Key within the Kerberos keytab Secret",
    )
    request.add_argument(
        "--kerberos-principal",
        default="",
        help="Kerberos principal used for signing",
    )
    request.add_argument(
        "--request-timeout",
        type=int,
        default=1800,
        help="InternalRequest timeout in seconds (default: %(default)s)",
    )
    request.add_argument(
        "--pipeline-timeout",
        default="0h30m0s",
        help="Pipeline timeout (default: %(default)s)",
    )
    request.add_argument(
        "--task-timeout",
        default="0h25m0s",
        help="Task timeout (default: %(default)s)",
    )
    request.add_argument(
        "--service-account",
        default="signing-pipeline-sa",
        help="Service account for the signing pipeline (default: %(default)s)",
    )
    request.add_argument(
        "--task-id",
        default="",
        help="Task run UID used as a label on internal requests",
    )
    request.add_argument(
        "--pipelinerun-uid",
        default="",
        help="Pipeline run UID used as a label on internal requests",
    )
    request.add_argument(
        "--signing-repo",
        default="https://gitlab.cee.redhat.com/signing/signing.git",
        help="Git repository URL for signing tasks (default: %(default)s)",
    )
    request.add_argument(
        "--signing-revision",
        default="main",
        help="Git revision in the signing repository (default: %(default)s)",
    )

    return parser


def main() -> int:
    """Entry point for standalone generic content signing."""
    parser = setup_argparser()
    args = parser.parse_args()
    LOGGER.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    request = SigningRequest(
        source_data_artifact=args.source_data_artifact,
        keyname=args.keyname,
        sign_method=args.sign_method,
        onbehalfof=args.onbehalfof,
        signing_repo=args.signing_repo,
        signing_revision=args.signing_revision,
        pipeline=args.pipeline,
        oci_storage=args.oci_storage,
        oras_options=args.oras_options,
        oci_artifact_expires_after=args.oci_artifact_expires_after,
        trusted_artifacts_debug=args.trusted_artifacts_debug,
        data_dir=args.data_dir,
        ta_task_git_url=args.ta_task_git_url,
        ta_task_git_revision=args.ta_task_git_revision,
        ca_trust_config_map_name=args.ca_trust_config_map_name,
        ca_trust_config_map_key=args.ca_trust_config_map_key,
        trusted_artifacts_dockerconfig_secret=args.trusted_artifacts_dockerconfig_secret,
        kerberos_keytab_secret=args.kerberos_keytab_secret,
        kerberos_keytab=args.kerberos_keytab,
        kerberos_principal=args.kerberos_principal,
        verbose=args.verbose,
        task_id=args.task_id,
        pipelinerun_uid=args.pipelinerun_uid,
        request_timeout=args.request_timeout,
        pipeline_timeout=args.pipeline_timeout,
        task_timeout=args.task_timeout,
        service_account=args.service_account,
    )
    results = submit(request)

    output_ta_uri = results.get("sourceDataArtifact", "")
    if not output_ta_uri:
        raise RuntimeError("generic-signing returned an empty sourceDataArtifact result.")

    if args.output:
        args.output.write_text(output_ta_uri, encoding="utf-8")
    else:
        print(output_ta_uri)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
