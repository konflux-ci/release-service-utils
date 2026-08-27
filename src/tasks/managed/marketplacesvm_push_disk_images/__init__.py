"""Push VM disk images to cloud marketplaces via pubtools-marketplacesvm.

Validates marketplace credentials, pulls OCI disk-image artifacts with oras,
stages them for pushsource, then invokes ``marketplacesvm_push_wrapper``.

CLI arguments map to Tekton task parameters. ``CLOUD_CREDENTIALS`` is set from
validated secret files under ``--secrets-dir`` (default ``/etc/secrets``).
``UPLOAD_CONTAINER_NAME`` remains an environment variable consumed by the
underlying pubtools-marketplacesvm tooling.
"""

from .marketplacesvm_push_disk_images import (  # noqa: F401
    DEFAULT_SECRETS_DIR,
    DEFAULT_WORKDIR,
    MEMORY_THRESHOLD,
    PROG,
    build_date_from_respin,
    copy_artifacts,
    decompress_gzip_source,
    image_type_for_filename,
    log_command_failure,
    main,
    parse_architecture,
    parse_args,
    parse_build_name,
    parse_build_respin,
    prepare_component,
    prepare_components,
    require_field,
    run,
    run_marketplacesvm_push,
    set_cloud_credentials,
    strip_extensions,
    validate_credentials,
    validate_staged_structure,
    write_resources_yaml,
    write_starmap_file,
)
