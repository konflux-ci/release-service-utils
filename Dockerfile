FROM quay.io/konflux-ci/oras:latest@sha256:4542f5a2a046ca36653749a8985e46744a5d2d36ee10ca14409be718ce15129e as oras
FROM registry.redhat.io/rhtas/cosign-rhel9:1.1.1-1736785731 as cosign
FROM registry.access.redhat.com/ubi9/ubi:9.6-1760340943

ARG KUBECTL_VERSION=1.27.2
ARG OPM_VERSION=v1.50.0
ARG PUBTOOLS_CGW_VERSION=0.5.4
ARG PUBTOOLS_PULP_VERSION=1.33.2
ARG PUBTOOLS_EXODUS_VERSION=1.5.2
ARG PUBTOOLS_MARKETPLACESVM_VERSION=1.7.0
ARG PUBTOOLS_SIGN_VERSION=1.0.0
ARG PUBTOOLS_PYXIS_VERSION=1.3.7
ARG YQ_VERSION=4.34.1
ARG GLAB_VERSION=1.51.0
ARG GH_VERSION=2.32.1
ARG SYFT_VERSION=1.19.0

RUN curl -L https://github.com/mikefarah/yq/releases/download/v${YQ_VERSION}/yq_linux_amd64 -o /usr/bin/yq &&\
    curl -L https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/amd64/kubectl -o /usr/bin/kubectl &&\
    curl -L https://github.com/operator-framework/operator-registry/releases/download/${OPM_VERSION}/linux-amd64-opm -o /usr/bin/opm &&\
    curl -L https://gitlab.com/gitlab-org/cli/-/releases/v${GLAB_VERSION}/downloads/glab_${GLAB_VERSION}_linux_amd64.tar.gz | tar -C /usr -xzf - bin/glab &&\
    curl -L https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_amd64.tar.gz  | tar -C /usr -xzf - --strip=1 gh_${GH_VERSION}_linux_amd64/bin/gh &&\
    curl -L https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}/syft_${SYFT_VERSION}_linux_amd64.tar.gz | tar -C /usr/bin/ -xzf - syft &&\
    chmod +x /usr/bin/{yq,kubectl,opm,glab,gh}

RUN dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm

COPY --from=oras /usr/bin/oras /usr/bin/oras
COPY --from=oras /usr/local/bin/select-oci-auth /usr/local/bin/select-oci-auth
COPY --from=oras /usr/local/bin/get-reference-base /usr/local/bin/get-reference-base
COPY --from=cosign /usr/local/bin/cosign /usr/local/bin/cosign

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
    && dnf clean all

RUN curl -LO https://github.com/release-engineering/exodus-rsync/releases/latest/download/exodus-rsync && \
    chmod +x exodus-rsync && mv exodus-rsync /usr/local/bin/rsync

RUN pip3 install jinja2 \
    check-jsonschema \
    jinja2-ansible-filters \
    packaging \
    packageurl-python \
    pubtools-content-gateway==${PUBTOOLS_CGW_VERSION} \
    pubtools-pulp==${PUBTOOLS_PULP_VERSION} \
    pubtools-exodus==${PUBTOOLS_EXODUS_VERSION} \
    pubtools-marketplacesvm==${PUBTOOLS_MARKETPLACESVM_VERSION} \
    pubtools-sign==${PUBTOOLS_SIGN_VERSION} \
    pubtools-pyxis==${PUBTOOLS_PYXIS_VERSION} \
    pulp-cli

# remove gcc, required only for compiling gssapi indirect dependency of pubtools-pulp via pushsource
RUN dnf -y remove gcc

ADD data/certs/2015-IT-Root-CA.pem data/certs/2022-IT-Root-CA.pem /etc/pki/ca-trust/source/anchors/
RUN update-ca-trust

COPY pyxis /home/pyxis
COPY utils /home/utils
COPY templates /home/templates
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

# Non-root user setup: OpenShift may override UID/GID at runtime (best practices)
RUN groupadd -g 1001 group1 && \
    useradd -u 1001 -g 1001 -m -d /tekton/home user1 && \
    mkdir -p /tekton/home && \
    chmod -R g+rwX /tekton/home /home && \
    # Make all files group-owned by root to allow OpenShift's random UID to work
    chgrp -R 0 /tekton/home /home && \
    # Ensure group permissions are inherited by new subdirectories
    find /tekton/home /home -type d -exec chmod g+s {} +

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
