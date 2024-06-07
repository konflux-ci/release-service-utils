FROM registry.access.redhat.com/ubi8/ubi:8.9-1028

ARG COSIGN_VERSION=2.1.1
ARG KUBECTL_VERSION=1.27.2
ARG OPM_VERSION=v1.38.0
ARG YQ_VERSION=4.34.1
ARG GLAB_VERSION=1.31.0
ARG GH_VERSION=2.32.1

RUN curl -L https://github.com/mikefarah/yq/releases/download/v${YQ_VERSION}/yq_linux_amd64 -o /usr/bin/yq &&\
    curl -L https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/amd64/kubectl -o /usr/bin/kubectl &&\
    curl -L https://github.com/operator-framework/operator-registry/releases/download/${OPM_VERSION}/linux-amd64-opm -o /usr/bin/opm &&\
    curl -L https://gitlab.com/gitlab-org/cli/-/releases/v${GLAB_VERSION}/downloads/glab_${GLAB_VERSION}_Linux_x86_64.tar.gz | tar -C /usr -xzf - bin/glab &&\
    curl -L https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_amd64.tar.gz  | tar -C /usr -xzf - --strip=1 gh_${GH_VERSION}_linux_amd64/bin/gh &&\
    chmod +x /usr/bin/{yq,kubectl,opm,glab,gh} &&\
    rpm -ivh https://github.com/sigstore/cosign/releases/download/v${COSIGN_VERSION}/cosign-${COSIGN_VERSION}.x86_64.rpm

RUN dnf -y --setopt=tsflags=nodocs install \
    git \
    jq \
    python39-devel \
    diffutils \
    python39-pip \
    python39-requests \
    skopeo \
    krb5-workstation \
    && dnf clean all

RUN pip3 install jinja2 \
    jinja2-ansible-filters \
    packageurl-python \
    signtractions

ADD data/certs/2015-IT-Root-CA.pem data/certs/2022-IT-Root-CA.pem /etc/pki/ca-trust/source/anchors/
RUN update-ca-trust

COPY pyxis /home/pyxis
COPY utils /home/utils
COPY templates /home/templates

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
ENV PATH="$PATH:/home/pyxis:/home/utils"
