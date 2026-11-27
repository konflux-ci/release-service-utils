#!/usr/bin/env bash
#
# test-spdx-sbom.sh - Check both SPDX SBOM download paths with the shipped Cosign binaries.
#
# The test-spdx-sbom Tekton task runs this script in the candidate utils image
# against a disposable Zot sidecar. Unlike run-test.sh, it does not submit a
# PipelineRun or run a catalog E2E suite. It does not contact Pyxis.
#
# Both cases use the same fixture image and SPDX document:
#   - Legacy .sbom tag: download succeeds without a verification key.
#   - Signed OCI referrer only: download succeeds with the matching public
#     key and rejects a different key at once, without retrying and without
#     writing the SBOM.
#
# Not covered: k8s:// public keys (production ECPs) and multi-arch indexes
# (the --platform path). The fixture is a single-arch image and the key is a
# local file.
#
# Usage: bash test-spdx-sbom.sh [localhost:port]
# Use a disposable HTTP registry on localhost (default: localhost:5000). Cosign
# only falls back to plain HTTP for localhost. This script pushes fixtures to
# it and only cleans up its local temporary files.
set -euo pipefail

work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT
cd "${work_dir}"

registry=${1:-localhost:5000}
curl --fail --silent --show-error --retry 10 --retry-connrefused --retry-max-time 30 --max-time 5 \
    "http://${registry}/v2/" >/dev/null

# Let Skopeo package an empty tarball as an OCI image, without an external pull.
tar -cf fixture.tar --files-from /dev/null
skopeo copy --format oci --dest-tls-verify=false --digestfile fixture.digest \
    tarball:fixture.tar "docker://${registry}/spdx-legacy:fixture"
# Copy the published image so both repositories share one digest.
oras cp --from-plain-http --to-plain-http \
    "${registry}/spdx-legacy:fixture" "${registry}/spdx-referrer:fixture"
digest=$(<fixture.digest)

cat > spdx.json <<'EOF'
{"spdxVersion":"SPDX-2.3","name":"spdx-attachment-fixture","packages":[]}
EOF
oras push --plain-http "${registry}/spdx-legacy:${digest/:/-}.sbom" spdx.json:text/spdx+json

# Use a fresh key and a signing config with no external Sigstore services.
COSIGN_PASSWORD='' cosign3 generate-key-pair --output-key-prefix signer
COSIGN_PASSWORD='' cosign3 generate-key-pair --output-key-prefix other
cat > signing-config.json <<'EOF'
{"mediaType":"application/vnd.dev.sigstore.signingconfig.v0.2+json"}
EOF
cat > trusted-root.json <<'EOF'
{"mediaType":"application/vnd.dev.sigstore.trustedroot+json;version=0.1"}
EOF
COSIGN_PASSWORD='' cosign3 attest --yes --key signer.key --type spdxjson \
    --signing-config signing-config.json --trusted-root trusted-root.json \
    --predicate spdx.json "${registry}/spdx-referrer@${digest}"

# Require the native referrers API and no legacy attachment tags (including .att).
curl --fail --silent --show-error "http://${registry}/v2/spdx-referrer/referrers/${digest}" \
    | jq -e 'any(.manifests[]; .artifactType == "application/vnd.dev.sigstore.bundle.v0.3+json")'
curl --fail --silent --show-error "http://${registry}/v2/spdx-referrer/tags/list" \
    | jq -e '.tags == ["fixture"]'

python3 - "${registry}" "${digest}" <<'PY'
import json
import sys
from pathlib import Path

from release_service_utils.tasks.managed.push_rpm_data_to_pyxis import push_rpm_data_to_pyxis as task

registry, digest = sys.argv[1:]
task.SBOM_DIR = Path.cwd() / "sboms"
expected_sbom = json.loads(Path("spdx.json").read_text())

legacy_job = task.ImageJob("abc", f"{registry}/spdx-legacy@{digest}", None, digest)
task.download_sbom(legacy_job, retries=0, enterprise_contract_policy=None)
assert json.loads((task.SBOM_DIR / "abc.json").read_text()) == expected_sbom

job = task.ImageJob("def", f"{registry}/spdx-referrer@{digest}", None, digest)
output = task.SBOM_DIR / "def.json"

wrong_policy = json.dumps({"spec": {"publicKey": str(Path("other.pub").resolve())}})
try:
    # Allow a retry, so the test fails if a wrong key is treated as transient.
    task.download_sbom(job, retries=1, enterprise_contract_policy=wrong_policy)
except RuntimeError as exc:
    message = str(exc)
    # Require a signature mismatch, not any cosign failure (network, auth, missing referrer).
    assert "Failed to verify SPDX attestation" in message, message
    assert "accepted signatures do not match threshold" in message, message
    assert " attempts" not in message, f"A wrong key was retried: {message}"
else:
    raise AssertionError("SPDX attestation unexpectedly verified with the wrong public key")
# The legacy --output-file flag may create an empty file before the lookup fails.
assert not output.exists() or output.stat().st_size == 0, "An unverified SPDX document was written"

policy = json.dumps({"spec": {"publicKey": str(Path("signer.pub").resolve())}})
task.download_sbom(job, retries=0, enterprise_contract_policy=policy)
assert json.loads(output.read_text()) == expected_sbom
PY
echo "Both SPDX formats matched; wrong key rejected"
