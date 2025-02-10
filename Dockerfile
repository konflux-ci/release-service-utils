FROM quay.io/konflux-ci/oras:latest@sha256:610cbf25f870375f5b83f379a327f8f637d78eb04864c5eb715ea186d71a5ed4 as oras
FROM registry.redhat.io/rhtas/cosign-rhel9:1.0.2-1719417920 as cosign
FROM registry.access.redhat.com/ubi9/ubi:9.5-1738814488

ARG COSIGN_VERSION=2.4.0
ARG KUBECTL_VERSION=1.27.2
ARG OPM_VERSION=v1.38.0
ARG PUBTOOLS_CGW_VERSION=0.5.4
ARG YQ_VERSION=4.34.1
ARG GLAB_VERSION=1.48.0
ARG GH_VERSION=2.32.1
ARG SYFT_VERSION=1.12.2

RUN curl -L https://github.com/mikefarah/yq/releases/download/v${YQ_VERSION}/yq_linux_amd64 -o /usr/bin/yq &&\
    curl -L https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/amd64/kubectl -o /usr/bin/kubectl &&\
    curl -L https://github.com/operator-framework/operator-registry/releases/download/${OPM_VERSION}/linux-amd64-opm -o /usr/bin/opm &&\
    curl -L https://gitlab.com/gitlab-org/cli/-/releases/v${GLAB_VERSION}/downloads/glab_${GLAB_VERSION}_linux_amd64.tar.gz | tar -C /usr -xzf - bin/glab &&\
    curl -L https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_amd64.tar.gz  | tar -C /usr -xzf - --strip=1 gh_${GH_VERSION}_linux_amd64/bin/gh &&\
    curl -L https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}/syft_${SYFT_VERSION}_linux_amd64.tar.gz | tar -C /usr/bin/ -xzf - syft &&\
    chmod +x /usr/bin/{yq,kubectl,opm,glab,gh}

COPY --from=oras /usr/bin/oras /usr/bin/oras
COPY --from=oras /usr/local/bin/select-oci-auth /usr/local/bin/select-oci-auth
COPY --from=oras /usr/local/bin/get-reference-base /usr/local/bin/get-reference-base
COPY --from=cosign /usr/local/bin/cosign /usr/local/bin/cosign

RUN dnf -y --setopt=tsflags=nodocs install \
    git \
    jq \
    python3-devel \
    diffutils \
    python3-pip \
    python3-requests \
    python3-rpm \
    skopeo \
    krb5-libs \
    krb5-devel \
    krb5-workstation \
    rsync \
    gcc \
    && dnf clean all

RUN curl -LO https://github.com/release-engineering/exodus-rsync/releases/latest/download/exodus-rsync && \
    chmod +x exodus-rsync && mv exodus-rsync /usr/local/bin/rsync

RUN pip3 install jinja2 \
    check-jsonschema \
    jinja2-ansible-filters \
    packaging \
    packageurl-python \
    pubtools-content-gateway==${PUBTOOLS_CGW_VERSION} \
    pubtools-pulp \
    pubtools-exodus \
    pubtools-marketplacesvm \
    pubtools-sign \
    pubtools-pyxis

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
COPY sbom /home/sbom

# It is mandatory to set these labels
LABEL name="Konflux Release Service Utils"
LABEL description="Konflux Release Service Utils"
LABEL io.k8s.description="Konflux Release Service Utils"
LABEL io.k8s.display-name="release-service-utils"
LABEL io.openshift.tags="konflux"
LABEL summary="Konflux Release Service Utils"
LABEL com.redhat.component="release-service-utils"

# Set HOME variable to something else than `/` to avoid 'permission denied' problems when writing files.
ENV HOME=/tekton/home
ENV PATH="$PATH:/home/pyxis"
ENV PATH="$PATH:/home/utils"
ENV PATH="$PATH:/home/pubtools-pulp-wrapper"
ENV PATH="$PATH:/home/pubtools-marketplacesvm-wrapper"
ENV PATH="$PATH:/home/developer-portal-wrapper"
ENV PATH="$PATH:/home/sbom"
