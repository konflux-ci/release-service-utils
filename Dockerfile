FROM registry.access.redhat.com/ubi8/ubi

RUN dnf -y --setopt=tsflags=nodocs install \
    git \
    python39-devel \
    python39-requests \
    && dnf clean all

# Set HOME variable to something else than `/` to avoid 'permission denied' problems when writing files.
ENV HOME=/tekton/home

# The ~ dir seems to be mounted over in tekton tasks, so put in /home
RUN git clone https://github.com/hacbs-release/release-utils /home/release-utils

ENV PATH=$PATH:/home/release-utils/pyxis
