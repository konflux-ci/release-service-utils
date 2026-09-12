"""Jinja2 template rendering with YAML-to-JSON conversion for advisory data.

Two-pass rendering with AnsibleCoreFiltersExtension support. Outputs JSON to
prevent YAML type coercion (e.g., version strings like "33158e1" being
parsed as numbers).
"""

from .apply_template import (  # noqa: F401
    render_template_to_json_file,
    setup_argparser,
    setup_logger,
    main,
)
