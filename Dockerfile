FROM registry.access.redhat.com/ubi8/ubi

RUN dnf -y --setopt=tsflags=nodocs install \
    git \
    python3 \
    && dnf clean all

# Set HOME variable to something else than `/` to avoid 'permission denied' problems when writing files.
ENV HOME=/tekton/home

RUN git clone https://github.com/hacbs-release/release-utils ~/release-utils
