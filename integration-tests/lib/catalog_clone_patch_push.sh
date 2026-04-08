#!/usr/bin/env bash
# Clone release-service-catalog, replace konflux-ci release-service-utils image refs with UTILS_IMAGE,
# push to a new GitHub repo.
# Outputs results for Tekton (stdout markers + optional result files).
#
# Required env: GITHUB_TOKEN, UTILS_IMAGE
# Optional: CATALOG_REPO (default konflux-ci/release-service-catalog), CATALOG_REF (development),
#           DEST_REPO_PREFIX (default hacbs-release-tests/catalog-e2e), PIPELINE_UID
set -euo pipefail

: "${GITHUB_TOKEN:?GITHUB_TOKEN is required}"
: "${UTILS_IMAGE:?UTILS_IMAGE is required}"

CATALOG_REPO="${CATALOG_REPO:-konflux-ci/release-service-catalog}"
CATALOG_REF="${CATALOG_REF:-development}"
DEST_REPO="${DEST_REPO_PREFIX:-hacbs-release-tests/utils-e2e}-${PIPELINE_UID:-$(date +%s)}"
BRANCH_NAME="${BRANCH_NAME:-patched-catalog}"

CATALOG_CLONE_DIR="$(mktemp -d)"
trap 'rm -rf "${CATALOG_CLONE_DIR}"' EXIT

echo "Cloning ${CATALOG_REPO}@${CATALOG_REF} into ${CATALOG_CLONE_DIR}..."
git clone --depth 1 --branch "${CATALOG_REF}" \
  "https://${GITHUB_TOKEN}@github.com/${CATALOG_REPO}.git" "${CATALOG_CLONE_DIR}"
cd "${CATALOG_CLONE_DIR}"
# Shallow clone + push to an empty repo often yields "did not receive expected object" / index-pack
# on the remote; need a complete object graph for git push to pack correctly.
if [[ -f "$(git rev-parse --git-dir)/shallow" ]]; then
  echo "Fetching full history (git fetch --unshallow) for a reliable push..."
  git fetch --unshallow
fi

CATALOG_BASE_SHA="$(git rev-parse HEAD)"
echo "Recorded CATALOG_BASE_SHA=${CATALOG_BASE_SHA}"

# GitHub rejects PAT pushes that touch workflow YAML without `workflow` scope. We push HEAD~1 to
# `development` for find_release_pipelines_from_pr; that ref must not contain workflow files, so
# remove workflows in their own commit *before* the image patch (HEAD~1 is then pushable).
if [[ -d .github/workflows ]]; then
  echo "Removing .github/workflows before image patch (required for PAT push of development ref)."
  rm -rf .github/workflows
  git config user.email "catalog-e2e@konflux-ci"
  git config user.name "konflux-release-team"
  git add -A
  git commit -m "chore(e2e): drop workflows for GitHub PAT push"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "${SCRIPT_DIR}/catalog_e2e_helpers.py"

echo "${CATALOG_BASE_SHA}" > .catalog-clone-base-sha

git config user.email "konflux-release-team@redhat.com"
git config user.name "konflux-release-team"
git add -A
git commit -m "chore(e2e): use release-service-utils PR image for integration tests"

INTEGRATION_SCRIPTS_DIR="${CATALOG_CLONE_DIR}/integration-tests/scripts"
if [[ ! -f "${INTEGRATION_SCRIPTS_DIR}/create-github-repo.sh" ]]; then
  echo "ERROR: create-github-repo.sh not found at ${INTEGRATION_SCRIPTS_DIR}" >&2
  exit 1
fi

echo "Creating GitHub repo ${DEST_REPO}..."
bash "${INTEGRATION_SCRIPTS_DIR}/create-github-repo.sh" "${DEST_REPO}" \
  "Temporary catalog fork for release-service-utils e2e (auto-deleted)" false

git remote add dest "https://${GITHUB_TOKEN}@github.com/${DEST_REPO}.git"
# HEAD~1 is the workflow-free commit (when .github/workflows existed); find_release_pipelines_from_pr
# uses origin/development...HEAD (image patch only when two commits; else HEAD~1 is upstream).
git push dest "HEAD~1:refs/heads/development"
git checkout -b "${BRANCH_NAME}"
git push -u dest "${BRANCH_NAME}"

CATALOG_GIT_URL="https://github.com/${DEST_REPO}"
echo "Pushed development (catalog base) and ${BRANCH_NAME} (patched) to ${CATALOG_GIT_URL}"

# Optional Tekton result paths: $1 $2 $3 $4 = CATALOG_BASE_SHA, CATALOG_GIT_URL, CATALOG_GIT_REVISION, TEMP_REPO_NAME
if [[ -n "${1:-}" ]]; then echo -n "${CATALOG_BASE_SHA}" > "$1"; fi
if [[ -n "${2:-}" ]]; then echo -n "${CATALOG_GIT_URL}" > "$2"; fi
if [[ -n "${3:-}" ]]; then echo -n "${BRANCH_NAME}" > "$3"; fi
if [[ -n "${4:-}" ]]; then echo -n "${DEST_REPO}" > "$4"; fi
