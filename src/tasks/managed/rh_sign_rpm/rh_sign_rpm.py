#!/usr/bin/env python3
"""Create an InternalRequest to sign RPMs via the direct-rpm-signing pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from release_service_utils.helpers import file, internal_request, tekton
from release_service_utils.helpers.logger import logger

_DEFAULT_KERBEROS_KEYTAB = "keytab"
_DEFAULT_ARTIFACT_STORAGE_SECRET = "signing-artifact-storage-preprod"
_DEFAULT_SERVICE_ACCOUNT = "release-service-account"


def _tekton_bool_str(value: Any) -> str:
    """Render *value* the way Tekton string params and ``jq -r`` render booleans."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def build_sign_request_params(
    data: dict[str, Any],
    artifacts_json: dict[str, Any],
    *,
    oci_storage: str,
    destination_artifact_storage_domain: str,
    use_target_arch_repo: str,
    create_oci_artifact: str,
    oci_artifact_expires_after: str,
    requester: str,
    signing_repo: str,
    signing_revision: str,
) -> tuple[dict[str, str], str]:
    """Build the direct-rpm-signing InternalRequest params and service account."""
    sign_options = data.get("signOptions", {})
    keytab = sign_options["signKeytab"]

    params = {
        "artifact_json": file.encode_json_gzip_b64(artifacts_json),
        "oci_storage": oci_storage,
        "kerberos_principal": keytab["principal"],
        "kerberos_keytab": keytab.get("name", _DEFAULT_KERBEROS_KEYTAB),
        "kerberos_keytab_secret": keytab["secret"],
        "signing_alias": sign_options["signKeyAlias"]["key"],
        "artifact_storage_secret": sign_options.get(
            "artifactStorageSecret", _DEFAULT_ARTIFACT_STORAGE_SECRET
        ),
        "destination_artifact_storage_domain": destination_artifact_storage_domain,
        "use_target_arch_repo": use_target_arch_repo,
        "skip_upload_to_pulp": _tekton_bool_str(sign_options.get("skipUploadToPulp", False)),
        "create_oci_artifact": create_oci_artifact,
        "oci_artifact_expires_after": oci_artifact_expires_after,
        "requester": requester,
        "force": "false",
        "taskGitUrl": signing_repo,
        "taskGitRevision": signing_revision,
    }
    service_account = sign_options.get("signPipelineServiceAccount", _DEFAULT_SERVICE_ACCOUNT)
    return params, service_account


def run(
    *,
    data_dir: Path,
    data_path: str,
    artifacts_json_path: str,
    requester: str,
    request_timeout: str,
    pipeline_timeout: str,
    task_timeout: str,
    pipeline_run_uid: str,
    oci_storage: str,
    destination_artifact_storage_domain: str,
    use_target_arch_repo: str,
    create_oci_artifact: str,
    oci_artifact_expires_after: str,
    signing_repo: str,
    signing_revision: str,
    result_path: Path,
) -> None:
    """Create a direct-rpm-signing InternalRequest and record the signed OCI artifact."""
    data = file.load_json_dict(data_dir / data_path)
    artifacts_json = file.load_json_dict(data_dir / artifacts_json_path)

    params, service_account = build_sign_request_params(
        data,
        artifacts_json,
        oci_storage=oci_storage,
        destination_artifact_storage_domain=destination_artifact_storage_domain,
        use_target_arch_repo=use_target_arch_repo,
        create_oci_artifact=create_oci_artifact,
        oci_artifact_expires_after=oci_artifact_expires_after,
        requester=requester,
        signing_repo=signing_repo,
        signing_revision=signing_revision,
    )

    logger.info("Creating InternalRequest to sign RPMs...")
    try:
        ir_name = internal_request.create(
            "direct-rpm-signing",
            params=params,
            labels={internal_request.PIPELINERUN_UID_LABEL: pipeline_run_uid},
            sync=True,
            timeout=int(request_timeout),
            service_account=service_account,
            pipeline_timeout=pipeline_timeout,
            task_timeout=task_timeout,
        )
    except internal_request.InternalRequestWaitError as err:
        raise RuntimeError(str(err)) from err
    logger.info("InternalRequest %s completed", ir_name)

    results = internal_request.fetch_results(ir_name)
    signed_rpms_oci_artifact = results.get("signed_rpms_oci_artifact", "")
    if not signed_rpms_oci_artifact:
        raise RuntimeError("signed_rpms_oci_artifact not found in InternalRequest results")
    logger.info("Signed RPMs OCI artifact: %s", signed_rpms_oci_artifact)
    result_path.write_text(signed_rpms_oci_artifact, encoding="utf-8")


def main() -> int:
    """Read Tekton env vars, create the signing InternalRequest, and write results."""
    (result_path,) = tekton.result_paths_from_env("RESULT_SIGNED_RPMS_OCI_ARTIFACTS")
    run(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        data_path=tekton.require_env("PARAM_DATA_PATH"),
        artifacts_json_path=tekton.require_env("PARAM_ARTIFACTS_JSON_PATH"),
        requester=tekton.require_env("PARAM_REQUESTER"),
        request_timeout=tekton.require_env("PARAM_REQUEST_TIMEOUT"),
        pipeline_timeout=tekton.require_env("PARAM_PIPELINE_TIMEOUT"),
        task_timeout=tekton.require_env("PARAM_TASK_TIMEOUT"),
        pipeline_run_uid=tekton.require_env("PARAM_PIPELINE_RUN_UID"),
        oci_storage=tekton.require_env("PARAM_OCI_STORAGE"),
        destination_artifact_storage_domain=tekton.require_env(
            "PARAM_DESTINATION_ARTIFACT_STORAGE_DOMAIN"
        ),
        use_target_arch_repo=tekton.require_env("PARAM_USE_TARGET_ARCH_REPO"),
        create_oci_artifact=tekton.require_env("PARAM_CREATE_OCI_ARTIFACT"),
        oci_artifact_expires_after=tekton.require_env("PARAM_OCI_ARTIFACT_EXPIRES_AFTER"),
        signing_repo=tekton.require_env("PARAM_SIGNING_REPO"),
        signing_revision=tekton.require_env("PARAM_SIGNING_REVISION"),
        result_path=result_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
