#!/bin/bash

# This script collects input needed to create a dockerconfigjson secret
# and link it to a newly created service account using SPI

# Defaults
#
export TARGET_NAMESPACE_DEFAULT=managed-release-team-tenant
export QUAY_ORG_URL_EXAMPLE=https://quay.io/repository/hacbs-release-tests
export SERVICE_ACCOUNT_DEFAULT=release-service-account

export token=$(oc whoami -t)

if [ -z "${token}" ]; then
  echo "Error: Not logged in to cluster"
  exit 1
fi

read -p "Please enter the target workspace: [${TARGET_NAMESPACE_DEFAULT}] " target_namespace
target_namespace=${target_namespace:-${TARGET_NAMESPACE_DEFAULT}}

read -p "Please enter the service account name: [${SERVICE_ACCOUNT_DEFAULT}] " service_account
service_account=${service_account:-${SERVICE_ACCOUNT_DEFAULT}}

read -p "Please enter a quay organization URL where to push content to: [${QUAY_ORG_URL_EXAMPLE}] " quay_org_url
quay_org_url=${quay_org_url:-${QUAY_ORG_URL_EXAMPLE}}

read -p "Please enter your quay robot username: [] " quay_oauth_user

read -s -p "Please enter your quay robot token: [] " quay_oauth_token

export binding_name=binding-dockerconfigjson-$$
export robot_secret_name=robot-account-pull-secret-$$

echo ""
echo "Going to create SPIAccessTokenBinding in $target_namespace"

cat <<EOF | kubectl apply -n $target_namespace -f -
apiVersion: appstudio.redhat.com/v1beta1
kind: SPIAccessTokenBinding
metadata:
  name: ${binding_name}
spec:
  permissions:
    required:
      - type: rw
        area: registry
      - type: rw
        area: registryMetadata
  repoUrl: $quay_org_url
  secret:
    type: kubernetes.io/dockerconfigjson
    name: ${robot_secret_name}
    linkedTo:
      - serviceAccount:
          managed:
            name: ${service_account}
EOF
echo "Binding created"
sleep 3
echo "Waiting for AwaitingTokenData phase"
kubectl wait --for=jsonpath='{.status.phase}'=AwaitingTokenData Spiaccesstokenbinding/${binding_name} -n $target_namespace --timeout=60s

if [ $? -ne 0 ] ; then
  echo "Error: could get token data after 60s. Verify your input."
  exit 1
fi

upload_url=$(kubectl get spiaccesstokenbinding/${binding_name} -n $target_namespace -o json | jq -r .status.uploadUrl)
echo "Upload url: ${upload_url}"
curl --insecure \
  -H 'Content-Type: application/json' \
  -H "Authorization: bearer $token" \
  -d "{ \"access_token\": \"${quay_oauth_token}\" ,  \"username\": \"${quay_oauth_user}\" }" \
  ${upload_url}
echo "Waiting for Injected phase"
kubectl wait --for=jsonpath='{.status.phase}'=Injected Spiaccesstokenbinding/${binding_name} -n $target_namespace --timeout=60s

if [ $? -ne 0 ] ; then
  echo "Error: could verify if secret was injected after 60s. Verify your input."
  exit 1
fi

linked_secret_name=$(kubectl get spiaccesstokenbinding/${binding_name} -n  ${target_namespace}   -o  json | jq -r  .status.syncedObjectRef.name)
echo "Linked secret: ${linked_secret_name}"
