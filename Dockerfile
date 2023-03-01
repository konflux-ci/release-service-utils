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
RUN git clone https://github.com/hacbs-release/release-utils /home/release-utils

# Copy the create_container_image script so we can use it without extension in release-bundles
RUN cp /home/release-utils/pyxis/create_container_image.py /home/release-utils/pyxis/create_container_image

ENV PATH=$PATH:/home/release-utils/pyxis
