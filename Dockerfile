FROM registry.access.redhat.com/ubi8/ubi

ARG COSIGN_VERSION=1.13.1
ARG KUBECTL_VERSION=1.27.2
ARG OCP_VERSION=4.13.3
ARG YQ_VERSION=4.34.1

RUN curl -L https://github.com/mikefarah/yq/releases/download/v${YQ_VERSION}/yq_linux_amd64 -o /usr/bin/yq &&\
    curl -L https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/amd64/kubectl -o /usr/bin/kubectl &&\
    curl -L https://mirror.openshift.com/pub/openshift-v4/x86_64/clients/ocp/${OCP_VERSION}/opm-linux.tar.gz |tar -C /usr/bin -xzf - &&\
    chmod +x /usr/bin/{yq,kubectl,opm} &&\
    rpm -ivh https://github.com/sigstore/cosign/releases/download/v${COSIGN_VERSION}/cosign-${COSIGN_VERSION}.x86_64.rpm

RUN dnf -y --setopt=tsflags=nodocs install \
    git \
    jq \
    python39-devel \
    python39-requests \
    skopeo \
    && dnf clean all


# The ~ dir seems to be mounted over in tekton tasks, so put in /home
RUN git clone https://github.com/redhat-appstudio/release-service-utils /home/release-service-utils &&\
# Copy the create_container_image and upload_sbom scripts so we can use them without extension in release-service-bundles
    cp /home/release-service-utils/pyxis/create_container_image.py /home/release-service-utils/pyxis/create_container_image &&\
    cp /home/release-service-utils/pyxis/upload_sbom.py /home/release-service-utils/pyxis/upload_sbom

# Set HOME variable to something else than `/` to avoid 'permission denied' problems when writing files.
ENV HOME=/tekton/home
ENV PATH=$PATH:/home/release-service-utils/pyxis
