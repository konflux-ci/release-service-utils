"""Decode advisory task payloads and content filtering (idempotency rules)."""

from .advisory_data import (  # noqa: F401
    ADVISORY_SECRET_PROD,
    ADVISORY_SECRET_STAGE,
    ARTIFACT_CONTENT_TYPES,
    DEFAULT_ADVISORY_TYPE,
    ERRATA_SECRET_PROD,
    ERRATA_SECRET_STAGE,
    VALID_ADVISORY_TYPES,
    AdvisoryData,
)

# Singleton instance for backward compatibility
_instance = AdvisoryData()

# Export function wrappers for backward compatibility
advisory_secret_name = _instance.advisory_secret_name
decode_advisory_param = _instance.decode_advisory_param
encode_advisory_param = _instance.encode_advisory_param
first_mapping_content_type = _instance.first_mapping_content_type
content_array_from_decoded = _instance.content_array_from_decoded
set_decoded_content_array = _instance.set_decoded_content_array
append_signing_key_to_content = _instance.append_signing_key_to_content
load_advisory_yaml = _instance.load_advisory_yaml
spec_content_array_from_advisory_yaml = _instance.spec_content_array_from_advisory_yaml
get_advisory_spec_type = _instance.get_advisory_spec_type
get_advisory_metadata_name = _instance.get_advisory_metadata_name
template_data_for_apply = _instance.template_data_for_apply
template_context_merge = _instance.template_context_merge
json_dict_to_yaml_text = _instance.json_dict_to_yaml_text
spec_content_json_pointer = _instance.spec_content_json_pointer
advisory_url_prefix = _instance.advisory_url_prefix
list_existing_advisory_subdirs = _instance.list_existing_advisory_subdirs
filter_content_by_existing = _instance.filter_content_by_existing
generate_purl_rpm = _instance.generate_purl_rpm

__all__ = [
    # Constants
    "ADVISORY_SECRET_PROD",
    "ADVISORY_SECRET_STAGE",
    "ARTIFACT_CONTENT_TYPES",
    "DEFAULT_ADVISORY_TYPE",
    "ERRATA_SECRET_PROD",
    "ERRATA_SECRET_STAGE",
    "VALID_ADVISORY_TYPES",
    # Class
    "AdvisoryData",
    # Backward-compatible functions
    "advisory_secret_name",
    "decode_advisory_param",
    "encode_advisory_param",
    "first_mapping_content_type",
    "content_array_from_decoded",
    "set_decoded_content_array",
    "append_signing_key_to_content",
    "load_advisory_yaml",
    "spec_content_array_from_advisory_yaml",
    "get_advisory_spec_type",
    "get_advisory_metadata_name",
    "template_data_for_apply",
    "template_context_merge",
    "json_dict_to_yaml_text",
    "spec_content_json_pointer",
    "advisory_url_prefix",
    "list_existing_advisory_subdirs",
    "filter_content_by_existing",
    "generate_purl_rpm",
]
