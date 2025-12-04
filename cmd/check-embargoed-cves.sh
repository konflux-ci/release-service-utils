#!/usr/bin/env bash

# This script checks if one or more CVEs are embargoed by querying the OSIDB
# API. The script authenticates using a service account with Kerberos
# authentication and queries the embargoed status for each provided CVE.
#
# The script will exit with:
#   - Exit code 0: All CVEs are accessible and not embargoed
#   - Exit code 1: At least one CVE is embargoed or inaccessible
#
# Results are written to files based on the following environment variables
# This is done so that the script is compatible with tekton; just set the tekton
# result path as an env variable when using the script.
#   - RESULT_EMBARGOED_CVES: Contains space-separated list of embargoed CVE IDs (if any)
#   - RESULT_RESULT: Contains success or the error if one occurs
#
# Usage:
#   ./check-embargoed-cves.sh --cves "CVE-2024-1234 CVE-2024-5678"
#
# Parameters:
#   --cves          The CVEs to check if they are embargoed. They must be passed
#                   together, via quotes, as a space-separated list.
#   -h              Display this help message.
#
# Prerequisites:
#   - curl: The curl command line tool must be installed for making HTTP requests.
#   - jq: This script uses jq to parse JSON responses. It must be installed on
#     the system running the script.
#   - kinit: Kerberos authentication tools must be available for service account
#     authentication.
#   - The following files must exist with respective content:
#     * /mnt/osidb-service-account/name: Contains the service account name
#     * /mnt/osidb-service-account/base64_keytab: Contains base64-encoded keytab
#     * /mnt/osidb-service-account/osidb_url: Contains the OSIDB API URL

set -eo pipefail

SERVICE_ACCOUNT_NAME="$(cat /mnt/osidb-service-account/name)"
SERVICE_ACCOUNT_KEYTAB="$(cat /mnt/osidb-service-account/base64_keytab)"
OSIDB_URL="$(cat /mnt/osidb-service-account/osidb_url)"

function usage {
    echo "Usage: $0 [--cves 'CVE-1 CVE-2']"
    echo
    echo "  --cves The CVEs to check if they are embargoed. They must be passed together, via quotes."
    echo "  -h     Display this help message."
    exit 1
}

OPTIONS=$(getopt -l "cves:" -o "h" -a -- "$@")
eval set -- "$OPTIONS"
while true; do
    case "$1" in
        --cves)
            shift
            CVES=$1
            ;;
        -h)
            shift
            usage
            ;;
        --)
            shift
            break
            ;;
    esac
    shift
done

# Check if mandatory parameters are set
if [ -z "$CVES" ]
then
    usage
fi

# shellcheck disable=SC2317 # shellcheck calls all the commands in exitfunc unreachable because it is called
# via trap
exitfunc() {
    local err="$1"
    local line="$2"
    local command="$3"
    if [ "$err" -eq 0 ] ; then
        echo -n "Success" > "$RESULT_RESULT"
    else
        echo -n \
          "$0: ERROR '$command' failed at line $line - exited with status $err" > "$RESULT_RESULT"
    fi
    exit 0 # exit the script cleanly as there is no point in proceeding past an error or exit call
}
# due to set -e, this catches all EXIT and ERR calls and the task should never fail with nonzero exit code
trap 'exitfunc $? $LINENO "$BASH_COMMAND"' EXIT

echo -n "" > "$RESULT_EMBARGOED_CVES"

# write keytab to file
echo -n "${SERVICE_ACCOUNT_KEYTAB}" | base64 --decode > /tmp/keytab
# workaround kinit: Invalid UID in persistent keyring name while getting default ccache
KRB5CCNAME=$(mktemp)
export KRB5CCNAME
KRB5_CONFIG=$(mktemp)
export KRB5_CONFIG
export KRB5_TRACE=/dev/stderr
sed '/\[libdefaults\]/a\    dns_canonicalize_hostname = false' /etc/krb5.conf > "${KRB5_CONFIG}"
retry 5 kinit "${SERVICE_ACCOUNT_NAME}" -k -t /tmp/keytab

RC=0
for CVE in ${CVES}; do # If ${CVES} is quoted, all CVEs will improperly be processed at once
    echo "Checking CVE ${CVE}"
    # Get token. They are short lived, so get one for before each request
    TOKEN=$(curl --retry 3 --negotiate -u : "${OSIDB_URL}"/auth/token | jq -r '.access')
    EMBARGOED=$(curl --retry 3 -H 'Content-Type: application/json' -H "Authorization: Bearer ${TOKEN}" \
        "${OSIDB_URL}/osidb/api/v2/flaws?cve_id=${CVE}&include_fields=cve_id,embargoed" \
        | jq .results[0].embargoed)
    # null would mean no access to the CVE, which may mean embargoed, and true means embargoed
    if [ "$EMBARGOED" != "false" ] ; then
        echo "CVE ${CVE} is embargoed"
        echo -n "${CVE} " >> "$RESULT_EMBARGOED_CVES"
        RC=1
    fi
done
exit $RC
