#!/usr/bin/env bash
set -e

# Function to make a string JSON-safe
make_json_safe() {
  local json_safe_string
  json_safe_string=$(jq -s -R -r @json <<< "$1")
  echo "$json_safe_string"
}

print_help(){
    echo -e "$0 --source-overlay SOURCE_OVERLAY --target-overlay TARGET_OVERLAY --fork-owner FORK_OWNER \
            [ --commit-to COMMIT_SHA ] [ --skip-cleanup ] [ --no-grafana ] [ --no-release ]\n"
    echo -e "\t--source-overlay SOURCE_OVERLAY\tName of the source overlay to promote to target"
    echo -e "\t--target-overlay TARGET_OVERLAY\tName of the overlay to target for promotion"
    echo -e "\t--fork-owner FORK_OWNER\tName of the owner of your infra-deployments fork in Github"
    echo -e "\t--commit-to COMMIT_SHA\tPromote up to this specific commit"
    echo -e "\t--skip-cleanup\tDisable cleanup after test. Useful for debugging"
    echo -e "\t--no-grafana\tSkip promoting the Grafana Dashboard overlay"
    echo -e "\t--no-release\tSkip promoting the Release Service overlay"
}

require_arg() {
    local var_name="$1"
    local var_value="$2"
    local arg_name="$3"

    if [ -z "$var_value" ]; then
        echo -e "Error: missing '$arg_name' argument\n\n"
        print_help
        exit 1
    fi
}

OPTIONS=$(getopt -l "skip-cleanup,no-grafana,no-release,commit-to:,source-overlay:,target-overlay:,fork-owner:,help" -o "sc,ng,nr,ct:,src:,tgt:,fo:,h" -a -- "$@")
eval set -- "$OPTIONS"
while true; do
    case "$1" in
        -sc|--skip-cleanup)
            CLEANUP="true"
            shift
            ;;
        -ng|--no-grafana)
            NO_GRAFANA="true"
            shift
            ;;
        -nr|--no-release)
            NO_RELEASE="true"
            shift
            ;;
        -ct|--commit-to)
            COMMIT_TO="$2"
            shift 2
            ;;
        -src|--source-overlay)
            SOURCE_OVERLAY="$2"
            shift 2
            ;;
        -tgt|--target-overlay)
            TARGET_OVERLAY="$2"
            shift 2
            ;;
        -fo|--fork-owner)
            FORK_OWNER="$2"
            shift 2
            ;;
        -h|--help)
            print_help
            exit
            ;;
        --)
            shift
            break
            ;;
        *) echo "Error: Unexpected option: $1" >&2
    esac
done

require_arg "SOURCE_OVERLAY" "${SOURCE_OVERLAY}" "source-overlay"
require_arg "TARGET_OVERLAY" "${TARGET_OVERLAY}" "target-overlay"
require_arg "FORK_OWNER" "${FORK_OWNER}" "fork-owner"
require_arg "GITHUB_TOKEN" "${GITHUB_TOKEN}" "GITHUB_TOKEN environment variable"

if [ "$NO_RELEASE" == "true" ] && [ "$NO_GRAFANA" == "true" ]; then
  echo "Error: --no-release and --no-grafana cannot both be set"
  exit 1
fi

# GitHub repository details
owner="redhat-appstudio"
repo="infra-deployments"

# Personal access token with appropriate permissions
token="${GITHUB_TOKEN}"

# Branch and commit details
new_branch="release-service-${TARGET_OVERLAY}-update-"$(date '+%Y_%m_%d__%H_%M_%S')

# Fork repository and branch parameters
fork_repo="infra-deployments"  # Change this to your fork's repository
base_branch="main"                # Change this to the base branch you want to create the PR against

# PR description
description="Included PRs:\r\n"

# Clone the repository
tmpDir=$(mktemp -d)
infraDeploymentDir=${tmpDir}/infra-deployments
releaseServiceDir=${tmpDir}/release-service
mkdir -p ${infraDeploymentDir}
mkdir -p ${releaseServiceDir}

if [ "${CLEANUP}" != "true" ]; then
  trap "rm -rf ${tmpDir}" EXIT
else
  echo "Temporary git clone directory: ${tmpDir}"
fi

echo -e "---\nPromoting release-service ${SOURCE_OVERLAY} to ${TARGET_OVERLAY} in ${owner}/${repo}\n---\n"
cd ${tmpDir}

echo -e "Sync fork with upstream:"
sync_fork_json=$(curl -s -L \
  -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${token}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/${FORK_OWNER}/infra-deployments/merge-upstream \
  -d '{"branch":"'${base_branch}'"}')

echo "$sync_fork_json"

git clone "git@github.com:$FORK_OWNER/$repo.git"
git clone "git@github.com:$owner/release-service.git"
cd ${infraDeploymentDir}

git fetch --all --tags --prune

# Create a new branch
git reset --hard HEAD
git checkout -b "$new_branch" origin/"$base_branch"

if [ "${NO_RELEASE}" != "true" ]; then
  RS_SOURCE_OVERLAY_COMMIT=$(yq '.images[0].newTag' < components/release/${SOURCE_OVERLAY}/kustomization.yaml)
  RS_TARGET_OVERLAY_COMMIT=$(yq '.images[0].newTag' < components/release/${TARGET_OVERLAY}/kustomization.yaml)

  if [ -n "$COMMIT_TO" ]; then
    RS_SOURCE_OVERLAY_COMMIT="$COMMIT_TO"
  fi

  echo ""
  echo 'release-service source overlay commit -> '"$RS_SOURCE_OVERLAY_COMMIT"
  echo 'release-service target overlay commit -> '"$RS_TARGET_OVERLAY_COMMIT"
  echo ""
fi

if [ "${NO_GRAFANA}" != "true" ]; then
  GRAFANA_SOURCE_COMMIT=$(grep -oE '[0-9a-f]{40}' components/monitoring/grafana/${SOURCE_OVERLAY}/dashboards/release/kustomization.yaml)
  GRAFANA_TARGET_COMMIT=$(grep -oE '[0-9a-f]{40}' components/monitoring/grafana/${TARGET_OVERLAY}/dashboards/release/kustomization.yaml)

  if [ -n "$COMMIT_TO" ]; then
    GRAFANA_SOURCE_COMMIT="$COMMIT_TO"
  fi

  echo 'grafana-dashboard source ref -> '"$GRAFANA_SOURCE_COMMIT"
  echo 'grafana-dashboard target ref -> '"$GRAFANA_TARGET_COMMIT"
  echo ""
fi

if [ "${NO_RELEASE}" == "true" ]; then
  commit_message="Promote release grafana-dashboard from ${SOURCE_OVERLAY} to ${TARGET_OVERLAY}"
elif [ "${NO_GRAFANA}" == "true" ]; then
  commit_message="Promote release-service from ${SOURCE_OVERLAY} to ${TARGET_OVERLAY}"
else
  commit_message="Promote release-service and grafana-dashboard from ${SOURCE_OVERLAY} to ${TARGET_OVERLAY}"
fi

cd  ${releaseServiceDir}
git fetch --all --tags --prune

# Validate that --commit-to is not behind either target overlay
if [[ -n "$COMMIT_TO" && "$NO_RELEASE" != "true" ]] && ! git merge-base --is-ancestor "$RS_TARGET_OVERLAY_COMMIT" "$COMMIT_TO"; then
  echo "Error: --commit-to (${COMMIT_TO:0:12}) is behind the ${TARGET_OVERLAY} release overlay (${RS_TARGET_OVERLAY_COMMIT:0:12})"
  exit 1
fi

if [[ -n "$COMMIT_TO" && "$NO_GRAFANA" != "true" ]] && ! git merge-base --is-ancestor "$GRAFANA_TARGET_COMMIT" "$COMMIT_TO"; then
  echo "Error: --commit-to (${COMMIT_TO:0:12}) is behind the ${TARGET_OVERLAY} grafana overlay (${GRAFANA_TARGET_COMMIT:0:12})"
  exit 1
fi

# Validate that the release-service image is available
if [ "${NO_RELEASE}" != "true" ]; then
  RS_IMAGE="quay.io/konflux-ci/release-service:${RS_SOURCE_OVERLAY_COMMIT}"
  echo "Checking image is available: ${RS_IMAGE}"
  if ! skopeo inspect --raw "docker://${RS_IMAGE}" > /dev/null; then
    echo "Error: image ${RS_IMAGE} is not available"
    exit 1
  fi
  echo "Image ${RS_IMAGE} is available"
fi

# Determine commit range for PR listing
if [ "${NO_RELEASE}" != "true" ]; then
  RANGE_FROM="$RS_TARGET_OVERLAY_COMMIT"
  RANGE_TO="$RS_SOURCE_OVERLAY_COMMIT"
else
  RANGE_FROM="$GRAFANA_TARGET_COMMIT"
  RANGE_TO="$GRAFANA_SOURCE_COMMIT"
fi

COMMITS=($(git rev-list --first-parent --ancestry-path "$RANGE_FROM"'...'"$RANGE_TO"))

echo "Fetching PR information for ${#COMMITS[@]} commits..."

graphql_request() {
  local query="$1"
  local retries=3
  local response

  for ((i=1; i<=retries; i++)); do
    response=$(curl -sf -X POST https://api.github.com/graphql \
      -H "Authorization: bearer ${token}" \
      -H "Content-Type: application/json" \
      -d "$(jq -n --arg q "$query" '{query: $q}')")

    if [ $? -eq 0 ] && ! echo "$response" | jq -e '.errors' >/dev/null 2>&1; then
      echo "$response"
      return 0
    fi

    if [ $i -lt $retries ]; then
      echo "Request failed. Retrying $i/$retries..." >&2
      sleep 10
    fi
  done

  echo "Error: GraphQL request failed after $retries attempts" >&2
  return 1
}

# Process commits in batches of 50
if [ ${#COMMITS[@]} -eq 0 ]; then
  echo "No commits to process"
else
  batch_size=50

  for ((i=0; i<${#COMMITS[@]}; i+=batch_size)); do
    # Build GraphQL query for this batch
    query="query {"
    batch_end=$((i + batch_size))
    [ $batch_end -gt ${#COMMITS[@]} ] && batch_end=${#COMMITS[@]}

    for ((j=i; j<batch_end; j++)); do
      query="$query
      c$j: search(query: \"repo:konflux-ci/release-service is:pr sha:${COMMITS[$j]}\", type: ISSUE, first: 1) {
        edges {
          node {
            ... on PullRequest {
              url
              labels(first: 10) {
                nodes {
                  name
                }
              }
            }
          }
        }
      }"
    done
    query="$query
    }"

    response=$(graphql_request "$query")
    if [ $? -ne 0 ]; then
      echo "Error: Failed to fetch PR information"
      exit 1
    fi

    for ((j=i; j<batch_end; j++)); do
      pr_url=$(echo "$response" | jq -r ".data.c$j.edges[0]?.node.url // empty")

      if [ -n "$pr_url" ]; then
        breaking_change=$(echo "$response" | jq -r ".data.c$j.edges[0]?.node.labels.nodes[]?.name | select(contains(\"breaking-change\")) // empty")

        label=""
        [[ -z "$breaking_change" ]] || label="(breaking-change)"

        description="$description"' - '"$pr_url $label"'\r\n'
      fi
    done
  done

  echo "Successfully processed ${#COMMITS[@]} commits"
fi

cd ${infraDeploymentDir}

if [ "${NO_RELEASE}" != "true" ]; then
  sed -i "s/$RS_TARGET_OVERLAY_COMMIT/$RS_SOURCE_OVERLAY_COMMIT/g" components/release/${TARGET_OVERLAY}/kustomization.yaml
  git add components/release/${TARGET_OVERLAY}/kustomization.yaml
fi

if [ "${NO_GRAFANA}" != "true" ]; then
  sed -i "s/$GRAFANA_TARGET_COMMIT/$GRAFANA_SOURCE_COMMIT/g" components/monitoring/grafana/${TARGET_OVERLAY}/dashboards/release/kustomization.yaml
  git add components/monitoring/grafana/${TARGET_OVERLAY}/dashboards/release/kustomization.yaml
fi

if git diff --cached --quiet; then
  echo "Nothing to commit overlays are already up to date."
  exit 0
fi

git commit -m "$commit_message"

git push origin "$new_branch"

# Create a pull request using GitHub API
pr_creation_json=$(curl -s -X POST "https://api.github.com/repos/$owner/$repo/pulls" \
  -H "Authorization: token $token" \
  -d '{
    "title": "'"$commit_message"'",
    "head": "'"$FORK_OWNER:$new_branch"'",
    "base": "'"$base_branch"'",
    "body": "'"$description"'"
  }')

pr_url=$(echo "$pr_creation_json" | jq -r .html_url)

if [ "${pr_url}" == "null" ]; then
  echo -e "\nError: failed to create PR. See output: \n${pr_creation_json}"
  exit 1
fi


echo -e "\n=================================="
echo -e "Pull request created successfully:\n- ${pr_url}"
echo "=================================="
