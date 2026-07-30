"""Authentication oriented helpers for task scripts.

Store reusable, task-agnostic pieces here: krb5 configuration for container
runs, ``kinit`` with keytabs from the filesystem, and reading typical mounted
service-account / secret file layouts.
"""

from .authentication import (  # noqa: F401
    create_container_auth_config,
    kerberos_login,
    kinit_with_retry,
    load_keytab_from_mount,
    load_service_account,
    patch_krb5_config,
    read_mounted_text,
    setup_ca_cert,
    setup_docker_config,
    write_docker_config,
)
