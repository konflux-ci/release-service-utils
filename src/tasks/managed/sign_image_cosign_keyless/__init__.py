"""Sign container images in a Konflux release snapshot using keyless cosign."""

from .sign_image_cosign_keyless import (  # noqa: F401
    DEFAULT_CA_CERT_PATH,
    DEFAULT_OIDC_TOKEN_PATH,
    MANIFEST_LIST_MEDIA_TYPES,
    KeylessConfig,
    SignItem,
    certificate_identity_from_oidc_token,
    check_existing_cosign_signature,
    collect_component_sign_items,
    get_manifest_digests,
    initialize_tuf,
    main,
    run_cosign_with_retry,
    setup_argparser,
    sign_all,
    sign_item,
)
