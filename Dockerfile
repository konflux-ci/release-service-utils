FROM quay.io/konflux-ci/oras:latest@sha256:cb5a53f2842ecb2ee4d614fc5821a9d89ece501760a72ce964bfbae032c1868c as oras

FROM registry.redhat.io/rhtas/cosign-rhel9:1.3.1-1763546693 as cosign

FROM registry.redhat.io/advanced-cluster-security/rhacs-roxctl-rhel8:4.9.3 as roxctl

FROM registry.access.redhat.com/ubi9/ubi:9.7-1770238273

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
    curl -L https://github.com/mikefarah/yq/releases/download/v${YQ_VERSION}/yq_linux_${GO_ARCH} -o /usr/bin/yq &&\
    curl -L https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/${GO_ARCH}/kubectl -o /usr/bin/kubectl &&\
    curl -L https://github.com/operator-framework/operator-registry/releases/download/${OPM_VERSION}/linux-${GO_ARCH}-opm -o /usr/bin/opm &&\
    curl -L https://gitlab.com/gitlab-org/cli/-/releases/v${GLAB_VERSION}/downloads/glab_${GLAB_VERSION}_linux_${GO_ARCH}.tar.gz | tar -C /usr -xzf - bin/glab &&\
    curl -L https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_${GO_ARCH}.tar.gz  | tar -C /usr -xzf - --strip=1 gh_${GH_VERSION}_linux_${GO_ARCH}/bin/gh &&\
    curl -L https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}/syft_${SYFT_VERSION}_linux_${GO_ARCH}.tar.gz | tar -C /usr/bin/ -xzf - syft &&\
    curl -L https://github.com/kubearchive/kubearchive/releases/download/v${KUBEARCHIVE_VERSION}/kubectl-ka-linux-${GO_ARCH} -o /usr/bin/kubectl-ka &&\
    chmod +x /usr/bin/{yq,kubectl,opm,glab,gh,syft,kubectl-ka}

RUN dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm

COPY --from=oras /usr/bin/oras /usr/bin/oras
COPY --from=oras /usr/local/bin/select-oci-auth /usr/local/bin/select-oci-auth
COPY --from=oras /usr/local/bin/get-reference-base /usr/local/bin/get-reference-base
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
    rm -f /tmp/cosign-linux-*.gz

RUN ARCH=$(uname -m) && if [ "$ARCH" == "x86_64" ]; then ARCH=amd64; fi && \
    curl -L https://github.com/sigstore/cosign/releases/download/${COSIGN3_VERSION}/cosign-linux-${ARCH} -o /usr/local/bin/cosign3 && \
    chmod +x /usr/local/bin/cosign3


COPY --from=roxctl /usr/bin/roxctl /usr/bin/roxctl

# Install uv via curl
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/uv

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
    rsync \
    gcc \
    python3-qpid-proton \
    zip \
    && dnf clean all

RUN curl -LO https://github.com/release-engineering/exodus-rsync/releases/latest/download/exodus-rsync && \
    chmod +x exodus-rsync && mv exodus-rsync /usr/local/bin/rsync

# Install Python dependencies using uv
COPY pyproject.toml uv.lock ./
RUN uv pip install -r pyproject.toml --system && \
    # Remove PyPI's python-qpid-proton so the system RPM (python3-qpid-proton) takes precedence.
    # The PyPI wheel (0.40.0) causes SSL failures because it bundles its own OpenSSL which
    # doesn't use the system CA trust store. The system RPM (0.37.0) is properly linked to
    # UBI9's OpenSSL and respects /etc/pki/ca-trust.
    pip uninstall -y python-qpid-proton

# remove gcc, required only for compiling gssapi indirect dependency of pubtools-pulp via pushsource
RUN dnf -y remove gcc

ADD data/certs/2015-IT-Root-CA.pem data/certs/2022-IT-Root-CA.pem /etc/pki/ca-trust/source/anchors/
RUN update-ca-trust

COPY pyxis /home/pyxis
COPY utils /home/utils
COPY scripts /home/scripts
COPY templates /home/templates
COPY kafka /home/kafka
COPY pubtools-pulp-wrapper /home/pubtools-pulp-wrapper
COPY pubtools-marketplacesvm-wrapper /home/pubtools-marketplacesvm-wrapper
COPY developer-portal-wrapper /home/developer-portal-wrapper
COPY publish-to-cgw-wrapper /home/publish-to-cgw-wrapper

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
# Need to set PYTHONPATH to be able to run sbom scripts as modules
ENV PYTHONPATH="$PYTHONPATH:/home"

# uv installs newer requests and certifi which don't use the system CA like the one installed via
# dnf. So we need to point requests to the system CA bundle explicitly.
ENV REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt
