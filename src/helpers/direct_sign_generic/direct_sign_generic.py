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

from dataclasses import dataclass
from typing import Any

from release_service_utils.helpers import internal_request
from release_service_utils.helpers.logger import logger

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

    logger.info(
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
