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
            [ --skip-cleanup ]\n"
    echo -e "\t-i|--infra-repo-url INFRA_REPO_URL\tURL of the infra repository to promote the changes"
    echo -e "\t-s|--source-overlay SOURCE_OVERLAY\tName of the source overlay to promote to target"
    echo -e "\t-t|--target-overlay TARGET_OVERLAY\tName of the overlay to target for promotion"
    echo -e "\t-c|--component-name COMPONENT_NAME\tName of the component to be promoted"
    echo -e "\t-r|--promote-repo-url PROMOTE_REPO_URL\tGit Repo URL of the component to be promoted"
    echo -e "\t-o|--fork-owner FORK_OWNER\tName of the owner of your infra-deployments fork in Github"
    echo -e "\t-f|--update-file-path UPDATE_FILE_PATH\tLocation of the file to be read/updated"
    echo -e "\t-j|--update-json-path UPDATE_JSON_PATH\tJSON path where update should be done"
    echo -e "\t-k|--skip-cleanup\tDisable cleanup after test. Useful for debugging"
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

OPTIONS=$(getopt -l "infra-repo-url:,source-overlay:,target-overlay:,fork-owner:,component-name:,promote-repo-url:,skip-cleanup,update-file-path:,update-json-path:,help" \
	         -o "i:s:t:c:r:o:kf:j:h" -a -- "$@")
eval set -- "$OPTIONS"
while true; do
    case "$1" in
        -k|--skip-cleanup)
            CLEANUP="true"
            shift
            ;;
        -s|--source-overlay)
            SOURCE_OVERLAY="$2"
            shift 2
            ;;
        -t|--target-overlay)
            TARGET_OVERLAY="$2"
            shift 2
            ;;
	-c|--component-name)
	    COMPONENT_NAME="$2"
	    shift 2
	    ;;
	-i|--infra-repo-url)
	    INFRA_REPO_URL="$2"
	    shift 2
	    ;;
        -r|--promote-repo-url)
            PROMOTE_REPO_URL="$2"
	    shift 2
	    ;;
        -f|--update-file-path)
            UPDATE_FILE_PATH="$2"
	    shift 2
	    ;;
        -j|--update-json-path)
            UPDATE_JSON_PATH="$2"
	    shift 2
	    ;;
        -o|--fork-owner)
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

# The source and target overlays are only necessary if the update file path is not set
if [ -n $UPDATE_FILE_PATH ]; then
    require_arg "UPDATE_JSON_PATH" "${UPDATE_JSON_PATH}" "update-json-path"
else
    require_arg "SOURCE_OVERLAY" "${SOURCE_OVERLAY}" "source-overlay"
    require_arg "TARGET_OVERLAY" "${TARGET_OVERLAY}" "target-overlay"
fi

require_arg "INFRA_REPO_URL" "${INFRA_REPO_URL}" "infra-repo-url"
require_arg "FORK_OWNER" "${FORK_OWNER}" "fork-owner"
require_arg "GITHUB_TOKEN" "${GITHUB_TOKEN}" "GITHUB_TOKEN environment variable"
require_arg "PROMOTE_REPO_URL" "${PROMOTE_REPO_URL}" "PROMOTE_REPO_URL promoting repository url"

# Github infra-deployments repository details
read -r INFRA_OWNER INFRA_REPO <<< $(echo "$INFRA_REPO_URL" | awk '{
  repo=substr($1, index($1, ":")+1)
  owner=substr(repo, 0, index(repo, "/")-1)
  fork=substr(repo, index(repo, "/")+1)
  print owner" "fork
}')

# Personal access TOKEN with appropriate permissions
TOKEN="${GITHUB_TOKEN}"

# Parsing the repository into useful vars
read -r PROMOTE_FORK_OWNER PROMOTE_FORK_NAME <<< $(echo "$PROMOTE_REPO_URL" | awk '{
  repo=substr($1, index($1, ":")+1)
  owner=substr(repo, 0, index(repo, "/")-1)
  fork=substr(repo, index(repo, "/")+1)
  print owner" "fork
}')

# removing .git from PROMOTE_FORK_NAME
PROMOTE_FORK_NAME=$(basename "$PROMOTE_FORK_NAME" .git)

# Branch and commit details
NEW_BRANCH="${PROMOTE_FORK_NAME}-${TARGET_OVERLAY}-update-"$(date '+%Y_%m_%d__%H_%M_%S')
COMMIT_MESSAGE="Promote ${PROMOTE_FORK_NAME} from ${SOURCE_OVERLAY} to ${TARGET_OVERLAY}"

# Fork repository and branch parameters
BASE_BRANCH="main"             # Change this to the base branch you want to create the PR against

# PR DESCRIPTION
DESCRIPTION="Included PRs:\r\n"

# Clone the repository
TMPDIR=$(mktemp -d)
INFRA_DIR=${TMPDIR}/$INFRA_REPO
PROMOTE_SERVICE_DIR=${TMPDIR}/$PROMOTE_FORK_NAME

mkdir -p ${INFRA_DIR}
mkdir -p ${PROMOTE_SERVICE_DIR}

if [ "${CLEANUP}" != "true" ]; then
  trap "rm -rf ${TMPDIR}" EXIT
else
  echo "Temporary git clone directory: ${TMPDIR}"
fi

echo -e "---\nPromoting $FORK ${SOURCE_OVERLAY} to ${TARGET_OVERLAY} in ${INFRA_OWNER}/${INFRA_REPO}\n---\n"
cd ${TMPDIR}

echo -e "Sync fork with upstream:"
SYNC_FORK_JSON=$(curl -s -L \
  -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/${FORK_OWNER}/${INFRA_REPO}/merge-upstream \
  -d '{"branch":"'${BASE_BRANCH}'"}')

echo "$SYNC_FORK_JSON"

INFRA_REPO_URL="git@github.com:${INFRA_OWNER}/${INFRA_REPO}.git"
PROMOTE_REPO_URL="git@github.com:${PROMOTE_FORK_OWNER}/${PROMOTE_FORK_NAME}.git"

# clone infra-deployments
git clone "$INFRA_REPO_URL"
git clone "$PROMOTE_REPO_URL"

cd ${INFRA_DIR}

git fetch --all --tags --prune

# Create a new branch
git reset --hard HEAD
git checkout -b "$NEW_BRANCH" origin/"$BASE_BRANCH"

if [ -n $UPDATE_FILE_PATH ] && [ -n $UPDATE_JSON_PATH ]; then
    SOURCE_IMAGE=$(yq "$UPDATE_JSON_PATH" "$UPDATE_FILE_PATH")
    RS_SOURCE_OVERLAY_COMMIT=$(echo "$SOURCE_IMAGE" |awk -F: '{print $2}')
    RS_TARGET_OVERLAY_COMMIT=$(cd $PROMOTE_SERVICE_DIR; git log -1 --format=%H)
    TARGET_IMAGE=$(sed "s/$RS_SOURCE_OVERLAY_COMMIT/$RS_TARGET_OVERLAY_COMMIT/g" <<< "$SOURCE_IMAGE")
else
    RS_SOURCE_OVERLAY_COMMIT=$(yq '.images[0].newTag' < components/${COMPONENT_NAME}/${SOURCE_OVERLAY}/kustomization.yaml)
    RS_TARGET_OVERLAY_COMMIT=$(yq '.images[0].newTag' < components/${COMPONENT_NAME}/${TARGET_OVERLAY}/kustomization.yaml)
fi

echo ""
echo "$PROMOTE_FORK_NAME"' source overlay commit -> '"$RS_SOURCE_OVERLAY_COMMIT"
echo "$PROMOTE_FORK_NAME"' target overlay commit -> '"$RS_TARGET_OVERLAY_COMMIT"
echo ""

cd  ${PROMOTE_SERVICE_DIR}
git fetch --all --tags --prune
RS_COMMITS=($(git rev-list --first-parent --ancestry-path "$RS_TARGET_OVERLAY_COMMIT"'...'"$RS_SOURCE_OVERLAY_COMMIT"))

echo "Fetching PR information for ${#RS_COMMITS[@]} commits..."

graphql_request() {
  local query="$1"
  local retries=3
  local response

  for ((i=1; i<=retries; i++)); do
    response=$(curl -sf -X POST https://api.github.com/graphql \
      -H "Authorization: bearer ${TOKEN}" \
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
if [ ${#RS_COMMITS[@]} -eq 0 ]; then
  echo "No commits to process"
else
  batch_size=50

  for ((i=0; i<${#RS_COMMITS[@]}; i+=batch_size)); do
    # Build GraphQL query for this batch
    query="query {"
    batch_end=$((i + batch_size))
    [ $batch_end -gt ${#RS_COMMITS[@]} ] && batch_end=${#RS_COMMITS[@]}

    for ((j=i; j<batch_end; j++)); do
      query="$query
      c$j: search(query: \"repo:${PROMOTE_FORK_OWNER}/${PROMOTE_FORK_NAME} is:pr sha:${RS_COMMITS[$j]}\", type: ISSUE, first: 1) {
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

        DESCRIPTION="$DESCRIPTION"' - '"$pr_url $label"'\r\n'
      fi
    done
  done

  echo "Successfully processed ${#RS_COMMITS[@]} commits"
fi

# Updating the files in the $INFRA_DIR
cd ${INFRA_DIR}

if [ -n $UPDATE_FILE_PATH ] && [ -n $UPDATE_JSON_PATH ]; then
   COMMIT_MESSAGE="Promote ${PROMOTE_FORK_NAME} to production"
   yq -i "${UPDATE_JSON_PATH}=\"${TARGET_IMAGE}\"" $UPDATE_FILE_PATH
   git add $UPDATE_FILE_PATH
else
   sed -i "s/$RS_TARGET_OVERLAY_COMMIT/$RS_SOURCE_OVERLAY_COMMIT/g" components/${COMPONENT_NAME}/${TARGET_OVERLAY}/kustomization.yaml
   git add components/${COMPONENT_NAME}/${TARGET_OVERLAY}/kustomization.yaml
fi

git commit -m "$COMMIT_MESSAGE"
git push origin "$NEW_BRANCH"

# Create a pull request using GitHub API
pr_creation_json=$(curl -s -X POST "https://api.github.com/repos/$INFRA_OWNER/$INFRA_REPO/pulls" \
  -H "Authorization: TOKEN $TOKEN" \
  -d '{
    "title": "'"$COMMIT_MESSAGE"'",
    "head": "'"$FORK_OWNER:$NEW_BRANCH"'",
    "base": "'"$BASE_BRANCH"'",
    "body": "'"$DESCRIPTION"'"
  }')

pr_url=$(echo "$pr_creation_json" | jq -r .html_url)

if [ "${pr_url}" == "null" ]; then
  echo -e "\nError: failed to create PR. See output: \n${pr_creation_json}"
  exit 1
fi


echo -e "\n=================================="
echo -e "Pull request created successfully:\n- ${pr_url}"
echo "=================================="
