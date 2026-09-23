"""File, path, and temporary-file helpers for task scripts."""

from __future__ import annotations

from .file import (  # noqa: F401
    contained_regular_files,
    decompress_gzip_bounded,
    encode_json_gzip_b64,
    is_gzip_or_tar_archive,
    load_json_dict,
    make_tempfile_path,
    path_from_env_variable,
    read_bounded,
    replace_directory,
    resolve_path_under_base,
    restore_directory,
    sha256,
    swap_directory,
)
