#!/usr/bin/env bash
#
# run-test.sh - Submit a Tekton PipelineRun for utils-e2e-catalog-pipeline
#
# Overview:
#   Thin wrapper that passes the same parameters an IntegrationTestScenario would use for
#   integration-tests/pipelines/utils-e2e-catalog-pipeline.yaml in release-service-utils. It
#   kubectl-creates a PipelineRun in your cluster (requires kubectl and jq).
#
# Required environment variables:
#   SNAPSHOT_FILE
#     Path to a Konflux Snapshot JSON file (same shape as the SNAPSHOT pipeline parameter). It
#     must describe the utils build under components[0] (containerImage and source.git url and
#     revision), which the pipeline uses to clone the utils repo and diff against main.
#
#   PIPELINE_TEST_SUITE
#     Name of the catalog integration-tests/<name>/ directory to exercise (e.g. e2e). Same pipeline
#     parameter name as the catalog IntegrationTestScenario (PIPELINE_TEST_SUITE).
#
#   PIPELINE_USED
#     Basename of the managed pipeline under catalog pipelines/managed/<name>/ (e.g. fbc-release).
#     Same pipeline parameter name as the catalog IntegrationTestScenario (PIPELINE_USED).
#
# Optional (omit env vars to use Tekton defaults from the resolved pipeline YAML):
#   NAMESPACE (default rhtap-release-2-tenant only for kubectl; not a pipeline param).
#   CATALOG_REPO, CATALOG_REF, DEST_REPO_PREFIX, CATALOG_E2E_RUNNER_IMAGE, VAULT_PASSWORD_SECRET_NAME,
#   GITHUB_TOKEN_SECRET_NAME, KUBECONFIG_SECRET_NAME, E2E_WAIT_TIMEOUT (pipeline param e2eWaitTimeout).
#   With --wait, E2E_WAIT_TIMEOUT is required (kubectl wait --timeout); pick a duration that fits your run
#   (the pipeline default for e2eWaitTimeout is in utils-e2e-catalog-pipeline.yaml).
#   RUN_TEST_KEEP_PIPELINERUN=1  With --wait, skip deleting the PipelineRun when finished (debugging).
#
# Local kubeconfig (required for real runs, not --dry-run):
#   Before creating the PipelineRun, the script uploads your local kubeconfig (first path in $KUBECONFIG,
#   else ~/.kube/config) into a temporary Secret in NAMESPACE (key kubeconfig), sets pipeline param
#   orchestrationKubeconfigSecretName to that name, then patches the Secret with an ownerReference to the
#   PipelineRun so the Secret is garbage-collected when the PipelineRun is deleted (--wait deletes the PLR
#   when finished). If PipelineRun creation fails before the patch, the script deletes the Secret on exit.
#
# Options:
#   --dry-run          kubectl create --dry-run=client -o yaml (no PipelineRun created)
#   --wait             After create, block until the PipelineRun finishes (success or failure);
#                      requires E2E_WAIT_TIMEOUT (kubectl wait --timeout). When the run finishes, deletes the PipelineRun so runs
#                      do not accumulate; set RUN_TEST_KEEP_PIPELINERUN=1 to skip deletion.
#   (default)          Prints how to watch logs / status; does not wait (PipelineRun remains).
#
set -euo pipefail

_resolve_local_kubeconfig_file() {
  local first
  if [[ -n "${KUBECONFIG:-}" ]]; then
    first="${KUBECONFIG%%:*}"
    if [[ -f "${first}" ]]; then
      echo "${first}"
      return 0
    fi
  fi
  if [[ -f "${HOME}/.kube/config" ]]; then
    echo "${HOME}/.kube/config"
    return 0
  fi
  return 1
}

DRY=false
WAIT=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) awk 'NR==1{next} /^set -euo pipefail$/{exit} {print}' "$0"; exit 0 ;;
    --dry-run) DRY=true ;;
    --wait) WAIT=true ;;
    *) echo "run-test.sh: unknown arg: $1 (try --help)" >&2; exit 1 ;;
  esac
  shift
done

command -v kubectl >/dev/null 2>&1 || { echo "run-test.sh: kubectl is required" >&2; exit 1; }

: "${SNAPSHOT_FILE:?run-test.sh: SNAPSHOT_FILE is required (path to Konflux Snapshot JSON)}"
: "${PIPELINE_TEST_SUITE:?run-test.sh: PIPELINE_TEST_SUITE is required (catalog integration-tests/<name>/)}"
: "${PIPELINE_USED:?run-test.sh: PIPELINE_USED is required (catalog pipelines/managed/<name>/ basename)}"
[[ -f "${SNAPSHOT_FILE}" ]] || { echo "run-test.sh: no such file: ${SNAPSHOT_FILE}" >&2; exit 1; }
jq -e . "${SNAPSHOT_FILE}" >/dev/null || exit 1

# Git resolver for this PipelineRun: snapshot components[0].source.git url/revision (jq // defaults).
SNAP_GIT_URL=$(jq -r '.components[0].source.git.url // "https://github.com/konflux-ci/release-service-utils.git"' "${SNAPSHOT_FILE}")
SNAP_GIT_REV=$(jq -r '.components[0].source.git.revision // "development"' "${SNAPSHOT_FILE}")
if [[ "${SNAP_GIT_URL}" == git@github.com:* ]]; then
  SNAP_GIT_URL="https://github.com/${SNAP_GIT_URL#git@github.com:}"
fi
if [[ "${SNAP_GIT_URL}" == https://github.com/* && "${SNAP_GIT_URL}" != *.git ]]; then
  SNAP_GIT_URL="${SNAP_GIT_URL}.git"
fi

NAMESPACE="${NAMESPACE:-rhtap-release-2-tenant}"
readonly _UTILS_PIPELINE_PATH_IN_REPO='integration-tests/pipelines/utils-e2e-catalog-pipeline.yaml'

# Optional spec.params: omit unset env vars so Tekton uses pipeline defaults. A param set to the
# empty string on the PipelineRun still overrides the default (Tekton treats it as an explicit value).

# Append {name, value} to OPTIONAL_PLR_PARAMS when value is non-empty.
optional_plr_param_add_if_set() {
  local _param_name=$1
  local _param_value=${2:-}
  [[ -z "${_param_value}" ]] && return 0
  OPTIONAL_PLR_PARAMS=$(jq -cn \
    --argjson arr "${OPTIONAL_PLR_PARAMS}" \
    --arg n "${_param_name}" \
    --arg v "${_param_value}" \
    '$arr + [{name: $n, value: $v}]')
}

ORCH_SECRET_NAME=""
ORCH_SECRET_PATCHED=""
cleanup_orch_secret_if_unpatched() {
  if [[ -n "${ORCH_SECRET_NAME:-}" && -z "${ORCH_SECRET_PATCHED:-}" ]]; then
    echo "run-test.sh: deleting orchestration Secret ${ORCH_SECRET_NAME} (PipelineRun was not linked)" >&2
    kubectl delete secret "${ORCH_SECRET_NAME}" -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  fi
}

if [[ "${DRY}" == true ]]; then
  ORCHESTRATION_KUBECONFIG_SECRET_NAME="utils-e2e-orch-dry-run-placeholder"
else
  trap cleanup_orch_secret_if_unpatched EXIT
  KCFG_LOCAL=$(_resolve_local_kubeconfig_file) || {
    echo "run-test.sh: could not find a local kubeconfig (set KUBECONFIG to a file, or create ~/.kube/config)" >&2
    exit 1
  }
  if command -v openssl >/dev/null 2>&1; then
    ORCH_SECRET_NAME="utils-e2e-orch-$(openssl rand -hex 6)"
  else
    ORCH_SECRET_NAME="utils-e2e-orch-${RANDOM}${RANDOM}"
  fi
  echo "run-test.sh: creating orchestration Secret ${ORCH_SECRET_NAME} from ${KCFG_LOCAL}" >&2
  kubectl create secret generic "${ORCH_SECRET_NAME}" -n "${NAMESPACE}" --from-file=kubeconfig="${KCFG_LOCAL}"
  ORCHESTRATION_KUBECONFIG_SECRET_NAME="${ORCH_SECRET_NAME}"
fi

echo "run-test.sh: pipelineRef from git url=${SNAP_GIT_URL} revision=${SNAP_GIT_REV} path=${_UTILS_PIPELINE_PATH_IN_REPO}" >&2

OPTIONAL_PLR_PARAMS='[]'
optional_plr_param_add_if_set catalogRepo "${CATALOG_REPO:-}"
optional_plr_param_add_if_set catalogRef "${CATALOG_REF:-}"
optional_plr_param_add_if_set destRepoPrefix "${DEST_REPO_PREFIX:-}"
optional_plr_param_add_if_set catalogE2eRunnerImage "${CATALOG_E2E_RUNNER_IMAGE:-}"
optional_plr_param_add_if_set VAULT_PASSWORD_SECRET_NAME "${VAULT_PASSWORD_SECRET_NAME:-}"
optional_plr_param_add_if_set GITHUB_TOKEN_SECRET_NAME "${GITHUB_TOKEN_SECRET_NAME:-}"
optional_plr_param_add_if_set KUBECONFIG_SECRET_NAME "${KUBECONFIG_SECRET_NAME:-}"
optional_plr_param_add_if_set e2eWaitTimeout "${E2E_WAIT_TIMEOUT:-}"

PR_JSON=$(jq -n \
  --arg ns "${NAMESPACE}" --rawfile snap "${SNAPSHOT_FILE}" \
  --arg pts "${PIPELINE_TEST_SUITE}" --arg pu "${PIPELINE_USED}" \
  --arg okc "${ORCHESTRATION_KUBECONFIG_SECRET_NAME}" \
  --argjson optional_params "${OPTIONAL_PLR_PARAMS}" \
  --arg pgu "${SNAP_GIT_URL}" --arg pgr "${SNAP_GIT_REV}" --arg pgp "${_UTILS_PIPELINE_PATH_IN_REPO}" \
  '
  {
    apiVersion: "tekton.dev/v1",
    kind: "PipelineRun",
    metadata: {generateName: "utils-e2e-orchestrator-", namespace: $ns},
    spec: {
      pipelineRef: {
        resolver: "git",
        params: [
          {name: "url", value: $pgu},
          {name: "revision", value: $pgr},
          {name: "pathInRepo", value: $pgp}
        ]
      },
      params:
        [
          {name: "SNAPSHOT", value: $snap},
          {name: "PIPELINE_TEST_SUITE", value: $pts},
          {name: "PIPELINE_USED", value: $pu},
          {name: "orchestrationKubeconfigSecretName", value: $okc}
        ]
        + $optional_params
    }
  }
')

if [[ "${DRY}" == true ]]; then
  echo "${PR_JSON}" | kubectl create --dry-run=client -f - -o yaml
  exit 0
fi

PR_NAME=$(echo "${PR_JSON}" | kubectl create -f - -o jsonpath='{.metadata.name}')
echo "Created PipelineRun ${PR_NAME} in namespace ${NAMESPACE}"

if [[ -n "${ORCH_SECRET_NAME}" ]]; then
  PR_UID=$(kubectl get pipelinerun "${PR_NAME}" -n "${NAMESPACE}" -o jsonpath='{.metadata.uid}')
  ORCH_PATCH_JSON=$(jq -n \
    --arg name "${PR_NAME}" --arg uid "${PR_UID}" \
    '{"metadata":{"ownerReferences":[{"apiVersion":"tekton.dev/v1","kind":"PipelineRun","name":$name,"uid":$uid,"blockOwnerDeletion":false}]}}')
  kubectl patch secret "${ORCH_SECRET_NAME}" -n "${NAMESPACE}" --type=merge -p "${ORCH_PATCH_JSON}" >/dev/null
  ORCH_SECRET_PATCHED=1
  trap - EXIT
fi

delete_pipelinerun() {
  [[ "${RUN_TEST_KEEP_PIPELINERUN:-}" == 1 ]] && return 0
  local pr=$1
  echo "run-test.sh: deleting pipelinerun ${pr} in ${NAMESPACE}" >&2
  kubectl delete pipelinerun "${pr}" -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
}

print_monitoring_hint() {
  local pr=$1
  cat <<EOF

Monitor this run:
  kubectl get pipelinerun "${pr}" -n "${NAMESPACE}" -w
  kubectl describe pipelinerun "${pr}" -n "${NAMESPACE}"
  (if tkn is installed)  tkn pipelinerun logs "${pr}" -n "${NAMESPACE}" -f

EOF
}

print_monitoring_hint "${PR_NAME}"

if [[ "${WAIT}" == true ]]; then
  : "${E2E_WAIT_TIMEOUT:?run-test.sh: E2E_WAIT_TIMEOUT is required with --wait (seconds for kubectl wait --timeout)}"
  echo "Waiting for completion (timeout ${E2E_WAIT_TIMEOUT}s)..."
  if ! kubectl wait --for=jsonpath='{.status.completionTime}' "pipelinerun/${PR_NAME}" -n "${NAMESPACE}" \
    --timeout="${E2E_WAIT_TIMEOUT}s"; then
    echo "run-test.sh: wait failed or timed out" >&2
    delete_pipelinerun "${PR_NAME}"
    exit 1
  fi
  ok=$(kubectl get pipelinerun "${PR_NAME}" -n "${NAMESPACE}" -o jsonpath='{.status.conditions[?(@.type=="Succeeded")].status}')
  if [[ "${ok}" != "True" ]]; then
    echo "run-test.sh: PipelineRun ${PR_NAME} did not succeed (Succeeded=${ok:-empty})" >&2
    delete_pipelinerun "${PR_NAME}"
    exit 1
  fi
  echo "PipelineRun ${PR_NAME} succeeded."
  delete_pipelinerun "${PR_NAME}"
fi
