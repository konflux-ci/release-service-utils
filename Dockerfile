FROM registry.access.redhat.com/ubi8/ubi

ARG COSIGNVERSION=1.13.1

RUN dnf -y --setopt=tsflags=nodocs install \
    git \
    jq \
    python39-devel \
    python39-requests \
    skopeo \
    && dnf clean all

# cosign is used for sbom download
RUN rpm -ivh https://github.com/sigstore/cosign/releases/download/v${COSIGNVERSION}/cosign-${COSIGNVERSION}.x86_64.rpm

# Set HOME variable to something else than `/` to avoid 'permission denied' problems when writing files.
ENV HOME=/tekton/home

# The ~ dir seems to be mounted over in tekton tasks, so put in /home
RUN git clone https://github.com/redhat-appstudio/release-service-utils /home/release-service-utils

# Copy the create_container_image and upload_sbom scripts so we can use them without extension in release-service-bundles
RUN cp /home/release-service-utils/pyxis/create_container_image.py /home/release-service-utils/pyxis/create_container_image
RUN cp /home/release-service-utils/pyxis/upload_sbom.py /home/release-service-utils/pyxis/upload_sbom

ENV PATH=$PATH:/home/release-service-utils/pyxis
