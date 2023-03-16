#!/bin/bash

# This script collects input needed to create a dockerconfigjson secret
# and link it to a service account using SPI.
# If the SA exists, it will be linked to it.
# If it does not exist, a new managed SA will be created
# and the secret will be linked.

# Defaults
#
export TARGET_NAMESPACE_DEFAULT=managed-release-team-tenant
export QUAY_ORG_URL_EXAMPLE=https://quay.io/repository/hacbs-release-tests
export SERVICE_ACCOUNT_DEFAULT=release-service-account

read -p "Please enter the target workspace: [${TARGET_NAMESPACE_DEFAULT}] " target_namespace
target_namespace=${target_namespace:-${TARGET_NAMESPACE_DEFAULT}}

read -p "Please enter the service account name: [${SERVICE_ACCOUNT_DEFAULT}] " service_account
service_account=${service_account:-${SERVICE_ACCOUNT_DEFAULT}}

read -p "Please enter a quay organization URL where to push content to: [${QUAY_ORG_URL_EXAMPLE}] " quay_org_url
quay_org_url=${quay_org_url:-${QUAY_ORG_URL_EXAMPLE}}

read -p "Please enter your quay robot username: [] " quay_oauth_user

read -s -p "Please enter your quay robot token: [] " quay_oauth_token

echo ""
echo ""
echo "Checking to see if ServiceAccount $service_account exists in $target_namespace"
service_account_type="reference"
if [ -z "$(oc get sa/${service_account} 2> /dev/null)" ] ; then
  service_account_type="managed"
  echo "SA/$service_account does not exist, SA will be of $service_account_type type"
else
  echo "SA/$service_account exists, SA will be of $service_account_type type"
fi

echo ""
echo "Going to create temporary Secret to upload token in $target_namespace"

cat <<EOF | kubectl create -n $target_namespace -f -
apiVersion: v1
kind: Secret
metadata:
  generateName: upload-secret-
  labels:
    spi.appstudio.redhat.com/upload-secret: token
    spi.appstudio.redhat.com/token-name: quay-token-$$
type: Opaque
stringData:
  providerUrl: https://quay.io/
  userName: ${quay_oauth_user}
  tokenData: ${quay_oauth_token}
EOF
echo "Temporary Secret created"

echo "Going to create SPIAccessTokenBinding in $target_namespace"

binding_name=${service_account}-${service_account_type}-binding
cat <<EOF | kubectl apply -n $target_namespace -f -
apiVersion: appstudio.redhat.com/v1beta1
kind: SPIAccessTokenBinding
metadata:
  name: ${binding_name}
spec:
  permissions:
    required:
    - area: repository
      type: r
  repoUrl: ${quay_org_url}
  secret:
    type: kubernetes.io/dockerconfigjson
    linkedTo:
    - serviceAccount:
        ${service_account_type}:
          name: ${service_account}
EOF

echo "Binding created"
sleep 2
kubectl wait  --for=jsonpath='{.status.phase}'=Injected  SPIAccessTokenBinding/${binding_name} -n $target_namespace  --timeout=60s
