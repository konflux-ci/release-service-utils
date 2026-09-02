FROM quay.io/konflux-ci/oras:latest@sha256:6cea0b9e142c2e18429f5cd30d716715d932047cbf1631334c5c31f7e47c3a19 as oras

FROM --platform=linux/amd64 registry.redhat.io/rhtas/ec-rhel9:0.7@sha256:bed04e826cd8b2638f14704b1b06ea5d99771f146cab0d5b46bdacdf48756c3c as conforma-cli

FROM --platform=linux/amd64 registry.redhat.io/rhtas/cosign-rhel9:1.3.3-1773309431 as cosign

# No --platform pin here (unlike cosign/ec-rhel9 above): we copy the roxctl binary
# itself, not an arch-bundled blob, so this must resolve to the target build platform.
FROM registry.redhat.io/advanced-cluster-security/rhacs-roxctl-rhel8:4.10.4-1 as roxctl

FROM registry.access.redhat.com/ubi10/ubi:10.2-1788218897

ARG COSIGN_VERSION=2.4.1
ARG COSIGN3_VERSION=3.0.4
ARG KUBECTL_VERSION=1.27.2
ARG OPM_VERSION=v1.50.0
ARG YQ_VERSION=4.34.1
ARG GLAB_VERSION=1.51.0
ARG GH_VERSION=2.82.1
ARG SYFT_VERSION=1.19.0
ARG KUBEARCHIVE_VERSION=1.17.3

RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        GO_ARCH="amd64"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        GO_ARCH="arm64"; \
    fi && \
    # -f: fail (non-zero exit) on HTTP errors instead of saving the error page as the binary
    # -sS: silent but still show errors; -L: follow redirects
    curl -fsSL https://github.com/mikefarah/yq/releases/download/v${YQ_VERSION}/yq_linux_${GO_ARCH} -o /usr/bin/yq &&\
    curl -fsSL https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/${GO_ARCH}/kubectl -o /usr/bin/kubectl &&\
    curl -fsSL https://github.com/operator-framework/operator-registry/releases/download/${OPM_VERSION}/linux-${GO_ARCH}-opm -o /usr/bin/opm &&\
    curl -fsSL https://gitlab.com/gitlab-org/cli/-/releases/v${GLAB_VERSION}/downloads/glab_${GLAB_VERSION}_linux_${GO_ARCH}.tar.gz | tar -C /usr -xzf - bin/glab &&\
    curl -fsSL https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_${GO_ARCH}.tar.gz  | tar -C /usr -xzf - --strip=1 gh_${GH_VERSION}_linux_${GO_ARCH}/bin/gh &&\
    curl -fsSL https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}/syft_${SYFT_VERSION}_linux_${GO_ARCH}.tar.gz | tar -C /usr/bin/ -xzf - syft &&\
    curl -fsSL https://github.com/kubearchive/kubearchive/releases/download/v${KUBEARCHIVE_VERSION}/kubectl-ka-linux-${GO_ARCH} -o /usr/bin/kubectl-ka &&\
    chmod +x /usr/bin/{yq,kubectl,opm,glab,gh,syft,kubectl-ka} && \
    # Verify each binary is actually a working executable, not a truncated/error-page
    # download that a bare curl exit code wouldn't catch.
    yq --version && \
    kubectl version --client && \
    opm version && \
    glab --version && \
    gh --version && \
    syft version && \
    kubectl-ka version

RUN dnf install -y https://dl.fedoraproject.org/pub/epel/10/Everything/$(arch)/Packages/e/epel-release-10-9.el10_3.noarch.rpm

COPY --from=oras /usr/bin/oras /usr/bin/oras
COPY --from=oras /usr/local/bin/select-oci-auth /usr/local/bin/select-oci-auth
COPY --from=oras /usr/local/bin/get-reference-base /usr/local/bin/get-reference-base
RUN oras version
COPY --from=conforma-cli /usr/local/bin/ec_linux_*.gz /tmp/
COPY --from=cosign /usr/local/bin/cosign-linux-*.gz /tmp/
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        COSIGN_ARCH="amd64"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        COSIGN_ARCH="arm64"; \
    elif [ "$ARCH" = "ppc64le" ]; then \
        COSIGN_ARCH="ppc64le"; \
    elif [ "$ARCH" = "s390x" ]; then \
        COSIGN_ARCH="s390x"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    gunzip -c /tmp/cosign-linux-${COSIGN_ARCH}.gz > /usr/local/bin/cosign && \
    chmod +x /usr/local/bin/cosign && \
    rm -f /tmp/cosign-linux-*.gz && \
    cosign version

RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        EC_ARCH="amd64"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        EC_ARCH="arm64"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    gunzip -c /tmp/ec_linux_${EC_ARCH}.gz > /usr/bin/ec && \
    chmod +x /usr/bin/ec && \
    rm -f /tmp/ec_linux_*.gz && \
    ec version

RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then ARCH=amd64; fi && \
    if [ "$ARCH" = "aarch64" ]; then ARCH=arm64; fi && \
    curl -LsSf https://github.com/sigstore/cosign/releases/download/v${COSIGN3_VERSION}/cosign-linux-${ARCH} -o /usr/local/bin/cosign3 && \
    chmod +x /usr/local/bin/cosign3 && \
    /usr/local/bin/cosign3 version


COPY --from=roxctl /usr/bin/roxctl /usr/bin/roxctl
RUN roxctl version

# Install uv via curl
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/uv && \
    uv --version

RUN dnf install -y 'dnf-command(config-manager)' && \
    dnf config-manager --set-enabled codeready-builder-for-ubi-10-$(arch)-rpms

RUN dnf -y --setopt=tsflags=nodocs install \
    git \
    git-lfs \
    jq \
    python3-devel \
    diffutils \
    python3-pip \
    python3-requests \
    python3-rpm \
    rpm-build \
    skopeo \
    krb5-libs \
    krb5-devel \
    krb5-workstation \
    openssl \
    rsync \
    gcc \
    python3-qpid-proton \
    zip \
    && dnf clean all

# exodus-rsync only publishes an amd64 binary upstream (no arm64 build exists:
# https://github.com/release-engineering/exodus-rsync/releases). On arm64, leave the
# plain system rsync installed above as-is: callers using exodus-specific flags (e.g.
# --exodus-conf) will get a clear "unknown option" error from it instead of a crash,
# and generic rsync usage keeps working normally.
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        curl -fLO https://github.com/release-engineering/exodus-rsync/releases/latest/download/exodus-rsync && \
        chmod +x exodus-rsync && mv exodus-rsync /usr/local/bin/rsync && \
        rsync --help > /dev/null; \
    fi

# Install Python dependencies using uv
COPY . ./
RUN pip install . && \
    # Remove PyPI's python-qpid-proton so the system RPM (python3-qpid-proton) takes precedence.
    # The PyPI wheel bundles its own OpenSSL which doesn't use the system CA trust store.
    pip uninstall -y python-qpid-proton && \
    # pip install/uninstall above can clobber the RPM's files on disk (no manylinux wheel
    # exists for python-qpid-proton, so pip's behavior here is inconsistent build-to-build).
    # Reinstall the RPM to guarantee its files are actually present afterward, regardless
    # of what pip did to the shared site-packages path.
    dnf reinstall -y python3-qpid-proton

# remove gcc, required only for compiling gssapi indirect dependency of pubtools-pulp via pushsource
RUN dnf -y remove gcc

ADD data/certs/2015-IT-Root-CA.pem data/certs/2022-IT-Root-CA.pem /etc/pki/ca-trust/source/anchors/
RUN update-ca-trust

COPY pyxis /home/pyxis
COPY utils /home/utils
COPY integration-tests /home/integration-tests
COPY src /home/src

# TODO: remove when fixed in release-service-catalog
RUN mkdir -p /home/scripts/python/tasks/managed && \
    mkdir -p /home/scripts/python/tasks/internal

RUN ln -s /home/src/tasks/managed/cleanup_workspace/cleanup_workspace.py /home/scripts/python/tasks/managed/cleanup_workspace.py && \
    ln -s /home/src/tasks/managed/base64_encode_checksum/base64_encode_checksum.py /home/scripts/python/tasks/managed/base64_encode_checksum.py && \
    ln -s /home/src/tasks/managed/check_data_keys/check_data_keys.py /home/scripts/python/tasks/managed/check_data_keys.py && \
    ln -s /home/src/tasks/managed/check_labels/check_labels.py /home/scripts/python/tasks/managed/check_labels.py && \
    ln -s /home/src/tasks/managed/cleanup_internal_requests/cleanup_internal_requests.py /home/scripts/python/tasks/managed/cleanup_internal_requests.py && \
    ln -s /home/src/tasks/managed/close_advisory_issues/close_advisory_issues.py /home/scripts/python/tasks/managed/close_advisory_issues.py && \
    ln -s /home/src/tasks/managed/collect_charon_params/collect_charon_params.py /home/scripts/python/tasks/managed/collect_charon_params.py && \
    ln -s /home/src/tasks/managed/collect_gh_params/collect_gh_params.py /home/scripts/python/tasks/managed/collect_gh_params.py && \
    ln -s /home/src/tasks/managed/collect_index_images/collect_index_images.py /home/scripts/python/tasks/managed/collect_index_images.py && \
    ln -s /home/src/tasks/managed/collect_slack_notification_params/collect_slack_notification_params.py /home/scripts/python/tasks/managed/collect_slack_notification_params.py && \
    ln -s /home/src/tasks/managed/extract_index_image/extract_index_image.py /home/scripts/python/tasks/managed/extract_index_image.py && \
    ln -s /home/src/tasks/managed/filter_already_released_images/filter_already_released_images.py /home/scripts/python/tasks/managed/filter_already_released_images.py && \
    ln -s /home/src/tasks/managed/make_repo_public/make_repo_public.py /home/scripts/python/tasks/managed/make_repo_public.py && \
    ln -s /home/src/tasks/managed/publish_pyxis_repository/publish_pyxis_repository.py /home/scripts/python/tasks/managed/publish_pyxis_repository.py && \
    ln -s /home/src/tasks/managed/update_infra_deployments/update_infra_deployments.py /home/scripts/python/tasks/managed/update_infra_deployments.py && \
    ln -s /home/src/tasks/managed/update_trusted_tasks/update_trusted_tasks.py /home/scripts/python/tasks/managed/update_trusted_tasks.py && \
    ln -s /home/src/tasks/managed/validate_single_component/validate_single_component.py /home/scripts/python/tasks/managed/validate_single_component.py && \
    ln -s /home/src/tasks/managed/extract_checksums_from_image/extract_checksums_from_image.py /home/scripts/python/tasks/managed/extract_checksums_from_image.py && \
    ln -s /home/src/tasks/managed/publish_to_nrrc/publish_to_nrrc.py /home/scripts/python/tasks/managed/publish_to_nrrc.py && \
    ln -s /home/src/tasks/managed/rh_direct_sign_image/rh_direct_sign_image.py /home/scripts/python/tasks/managed/rh_direct_sign_image.py && \
    ln -s /home/src/tasks/managed/direct_sign_index_image/direct_sign_index_image.py /home/scripts/python/tasks/managed/direct_sign_index_image.py && \
    ln -s /home/src/tasks/managed/request_advisory_creation/request_advisory_creation.py /home/scripts/python/tasks/managed/request_advisory_creation.py && \
    ln -s /home/src/tasks/managed/embargo_check/embargo_check.py /home/scripts/python/tasks/managed/embargo_check.py && \
    ln -s /home/src/tasks/managed/collect_registry_token_secret/collect_registry_token_secret.py /home/scripts/python/tasks/managed/collect_registry_token_secret.py && \
    ln -s /home/src/tasks/managed/collect_signing_params/collect_signing_params.py /home/scripts/python/tasks/managed/collect_signing_params.py && \
    ln -s /home/src/tasks/managed/collect_task_params/collect_task_params.py /home/scripts/python/tasks/managed/collect_task_params.py && \
    ln -s /home/src/tasks/managed/collect_tpa_params/collect_tpa_params.py /home/scripts/python/tasks/managed/collect_tpa_params.py && \
    ln -s /home/src/tasks/managed/publish_to_mrrc_prepare_repo/publish_to_mrrc_prepare_repo.py /home/scripts/python/tasks/managed/publish_to_mrrc_prepare_repo.py && \
    ln -s /home/src/tasks/managed/publish_to_mrrc_push_merged/publish_to_mrrc_push_merged.py /home/scripts/python/tasks/managed/publish_to_mrrc_push_merged.py && \
    ln -s /home/src/tasks/managed/filter_already_released_advisory_rpms/filter_already_released_advisory_rpms.py /home/scripts/python/tasks/managed/filter_already_released_advisory_rpms.py && \
    ln -s /home/src/tasks/managed/collect_data/collect_data.py /home/scripts/python/tasks/managed/collect_data.py && \
    ln -s /home/src/tasks/managed/extract_oot_kmods/extract_oot_kmods.py /home/scripts/python/tasks/managed/extract_oot_kmods.py && \
    ln -s /home/src/tasks/managed/marketplacesvm_push_disk_images/marketplacesvm_push_disk_images.py /home/scripts/python/tasks/managed/marketplacesvm_push_disk_images.py && \
    ln -s /home/src/tasks/managed/push_artifacts_to_storage/push_artifacts_to_storage.py /home/scripts/python/tasks/managed/push_artifacts_to_storage.py && \
    ln -s /home/src/tasks/managed/get_ocp_version/get_ocp_version.py /home/scripts/python/tasks/managed/get_ocp_version.py && \
    ln -s /home/src/tasks/managed/populate_release_notes/populate_release_notes.py /home/scripts/python/tasks/managed/populate_release_notes.py && \
    ln -s /home/src/tasks/managed/push_disk_images/push_disk_images.py /home/scripts/python/tasks/managed/push_disk_images.py && \
    ln -s /home/src/tasks/managed/push_snapshot/push_snapshot.py /home/scripts/python/tasks/managed/push_snapshot.py && \
    ln -s /home/src/tasks/managed/reduce_snapshot/reduce_snapshot.py /home/scripts/python/tasks/managed/reduce_snapshot.py && \
    ln -s /home/src/tasks/managed/send_slack_notification/send_slack_notification.py /home/scripts/python/tasks/managed/send_slack_notification.py && \
    ln -s /home/src/tasks/managed/apply_mapping/apply_mapping.py /home/scripts/python/tasks/managed/apply_mapping.py && \
    ln -s /home/src/tasks/managed/create_pyxis_image/create_pyxis_image.py /home/scripts/python/tasks/managed/create_pyxis_image.py && \
    ln -s /home/src/tasks/managed/add_fbc_contribution/add_fbc_contribution.py /home/scripts/python/tasks/managed/add_fbc_contribution.py && \
    ln -s /home/src/tasks/internal/filter_already_released_advisory_images/filter_already_released_advisory_images.py /home/scripts/python/tasks/internal/filter_already_released_advisory_images.py && \
    ln -s /home/src/tasks/internal/check_embargoed_cves/check_embargoed_cves.py /home/scripts/python/tasks/internal/check_embargoed_cves.py && \
    ln -s /home/src/tasks/internal/check_fbc_opt_in/check_fbc_opt_in.py /home/scripts/python/tasks/internal/check_fbc_opt_in.py && \
    ln -s /home/src/tasks/internal/create_advisory/create_advisory.py /home/scripts/python/tasks/internal/create_advisory.py && \
    ln -s /home/src/tasks/internal/get_advisory_severity/get_advisory_severity.py /home/scripts/python/tasks/internal/get_advisory_severity.py && \
    ln -s /home/src/tasks/internal/process_file_updates/process_file_updates.py /home/scripts/python/tasks/internal/process_file_updates.py && \
    ln -s /home/src/tasks/internal/pulp_push_disk_images/pulp_push_disk_images.py /home/scripts/python/tasks/internal/pulp_push_disk_images.py && \
    ln -s /home/src/tasks/internal/push_artifacts_to_cdn/push_artifacts_to_cdn.py /home/scripts/python/tasks/internal/push_artifacts_to_cdn.py && \
    ln -s /home/src/tasks/internal/update_fbc_catalog/update_fbc_catalog.py /home/scripts/python/tasks/internal/update_fbc_catalog.py

##############################################################

COPY templates /home/templates
COPY kafka /home/kafka
COPY pubtools-pulp-wrapper /home/pubtools-pulp-wrapper
COPY pubtools-marketplacesvm-wrapper /home/pubtools-marketplacesvm-wrapper
COPY developer-portal-wrapper /home/developer-portal-wrapper
COPY publish-to-cgw-wrapper /home/publish-to-cgw-wrapper
COPY schemas /home/schemas

# It is mandatory to set these labels
LABEL name="Konflux Release Service Utils"
LABEL description="Konflux Release Service Utils"
LABEL io.k8s.description="Konflux Release Service Utils"
LABEL io.k8s.display-name="release-service-utils"
LABEL io.openshift.tags="konflux"
LABEL summary="Konflux Release Service Utils"
LABEL com.redhat.component="release-service-utils"

# Configure non-root user (UID 1001) for security and compatibility.
# Note: release-service-catalog unit tests with user 1001 can't write to "/var/workdir" and "/tekton/*" directories
# And openShift may assign a random UID/GID at runtime.
# So, below part also sets directory ownership and permissions to ensure write access for unit tests and runtime.
RUN groupadd -g 1001 group1 && \
    useradd -m -u 1001 -g 1001 -d /tekton/home user1 && \
    # Change ownership on directories to ensure write permissions for unit tests
    mkdir -p /var/workdir && \
    mkdir -p /tekton/home && \
    mkdir -p /tekton/results && \
    chown -R 1001:1001 /var/workdir && \
    chown -R 1001:1001 /tekton/home /tekton/results && \
    # Make all files group-owned by root to allow OpenShift's random UID to work
    chgrp -R 0 /home /tekton && \
    chmod -R g+rwX /var/workdir /tekton /home && \
    # Ensure group permissions are inherited by new subdirectories
    find /var/workdir /home /tekton -type d -exec chmod g+s {} +

# Switch to a non-root user
USER 1001

# Set HOME variable to something else than `/` to avoid 'permission denied' problems when writing files.
ENV HOME=/tekton/home
WORKDIR $HOME
ENV PATH="$PATH:/home/pyxis"
ENV PATH="$PATH:/home/utils"
ENV PATH="$PATH:/home/pubtools-pulp-wrapper"
ENV PATH="$PATH:/home/pubtools-marketplacesvm-wrapper"
ENV PATH="$PATH:/home/developer-portal-wrapper"
ENV PATH="$PATH:/home/publish-to-cgw-wrapper"

# TODO: remove when fixed in release-service-catalog
ENV PATH="$PATH:/home/scripts/python/tasks/managed"
ENV PATH="$PATH:/home/scripts/python/tasks/internal"
# Flat imports: helpers and task scripts must be importable.
# Tests use the same layout via pyproject [tool.pytest.ini_options] pythonpath.
# Keep /home for other modules (e.g. pyxis, sbom) that expect it.
ENV PYTHONPATH="/home:/home/pyxis:/home/utils:/home/scripts/python/tasks/internal:/home/scripts/python/tasks/managed:/home/pubtools-pulp-wrapper:/home/publish-to-cgw-wrapper"

# uv installs newer requests and certifi which don't use the system CA like the one installed via
# dnf. So we need to point requests to the system CA bundle explicitly.
ENV REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt
