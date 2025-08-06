#!/usr/bin/env bash
set -e

# Function to make a string JSON-safe
make_json_safe() {
  local json_safe_string
  json_safe_string=$(jq -s -R -r @json <<< "$1")
  echo "$json_safe_string"
}

OPTIONS=$(getopt -l "skip-cleanup,source-overlay:,target-overlay:,fork-owner:,help" -o "sc,src:,tgt:,fo:,h" -a -- "$@")
eval set -- "$OPTIONS"
while true; do
    case "$1" in
        -sc|--skip-cleanup)
            CLEANUP="true"
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
        *) echo "Error: Unexpected option: $1" % >2
    esac
done

print_help(){
    echo -e "$0 --source-overlay SOURCE_OVERLAY --target-overlay TARGET_OVERLAY --fork-owner FORK_OWNER \
            [ --skip-cleanup ]\n"
    echo -e "\t--source-overlay SOURCE_OVERLAY\tName of the source overlay to promote to target"
    echo -e "\t--target-overlay TARGET_OVERLAY\tName of the overlay to target for promotion"
    echo -e "\t--fork-owner FORK_OWNER\tName of the owner of your infra-deployments fork in Github"
    echo -e "\t--skip-cleanup\tDisable cleanup after test. Useful for debugging"
}

if [ -z "${SOURCE_OVERLAY}" ]; then
  echo -e "Error: missing 'source-overlay' argument\n\n"
  print_help
  exit 1
fi
if [ -z "${TARGET_OVERLAY}" ]; then
  echo -e "Error: missing 'target-overlay' argument\n\n"
  print_help
  exit 1
fi
if [ -z "${FORK_OWNER}" ]; then
  echo -e "Error: missing 'fork-owner' argument\n\n"
  print_help
  exit 1
fi
if [ -z "${GITHUB_TOKEN}" ]; then
  echo -e "Error: missing 'GITHUB_TOKEN' environment variable\n\n"
  print_help
  exit 1
fi

UPDATE_BRANCH_NAME="release-service-${TARGET_OVERLAY}-update-"$(date '+%Y_%m_%d__%H_%M_%S')

# GitHub repository details
owner="redhat-appstudio"
repo="infra-deployments"

# Personal access token with appropriate permissions
token="${GITHUB_TOKEN}"

# New branch and commit details
new_branch=${UPDATE_BRANCH_NAME}
commit_message="Promote release-service from ${SOURCE_OVERLAY} to ${TARGET_OVERLAY}"

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
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/${FORK_OWNER}/infra-deployments/merge-upstream \
  -d '{"branch":"'${base_branch}'"}')

echo $sync_fork_json

git clone "git@github.com:$FORK_OWNER/$repo.git"
git clone "git@github.com:$owner/release-service.git"
cd ${infraDeploymentDir}

git fetch --all --tags --prune

# Create a new branch
git reset --hard HEAD
git checkout -b "$new_branch" origin/"$base_branch"

RS_SOURCE_OVERLAY_COMMIT=$(yq '.images[0].newTag' < components/release/${SOURCE_OVERLAY}/kustomization.yaml)
RS_TARGET_OVERLAY_COMMIT=$(yq '.images[0].newTag' < components/release/${TARGET_OVERLAY}/kustomization.yaml)

echo ""
echo 'release-service source overlay commit -> '"$RS_SOURCE_OVERLAY_COMMIT"
echo 'release-service target overlay commit -> '"$RS_TARGET_OVERLAY_COMMIT"
echo ""

cd  ${releaseServiceDir}
git fetch --all --tags --prune
RS_COMMITS=($(git rev-list --first-parent --ancestry-path "$RS_TARGET_OVERLAY_COMMIT"'...'"$RS_SOURCE_OVERLAY_COMMIT"))
## now loop through the above array
for RS_COMMIT in "${RS_COMMITS[@]}"
do
  PR_INFO="$(curl -s   -H 'Authorization: token  '"$token"  'https://api.github.com/search/issues?q=is:pr+sha:'"$RS_COMMIT")"
  PR_URL="$(jq -r '.items[0].pull_request.html_url' <<< "$PR_INFO")"
  LABEL="$(jq -r '.items[0].labels[].name | select(. | contains("breaking-change"))' <<< "$PR_INFO")"
  [[ -z "$LABEL" ]] || LABEL="($LABEL)"
   # or do whatever with individual element of the array
  description="$description"' - '"$PR_URL $LABEL"'\r\n'
done

cd ${infraDeploymentDir}
sed -i "s/$RS_TARGET_OVERLAY_COMMIT/$RS_SOURCE_OVERLAY_COMMIT/g" components/release/${TARGET_OVERLAY}/kustomization.yaml

git add components/release/${TARGET_OVERLAY}/kustomization.yaml
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

pr_url=$(echo $pr_creation_json | jq -r .html_url)

if [ "${pr_url}" == "null" ]; then
  echo -e "\nError: failed to create PR. See output: \n${pr_creation_json}"
  exit 1
fi


echo -e "\n=================================="
echo -e "Pull request created successfully:\n- ${pr_url}"
echo "=================================="
