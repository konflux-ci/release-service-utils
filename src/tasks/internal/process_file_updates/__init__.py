#!/usr/bin/env python3
"""Apply file edits to a GitLab repository and open a merge request with the changes.

Intended to be invoked by a release pipeline to automatically propagate release
artifacts (e.g. updated image digests, version strings) into downstream
configuration repositories, creating an MR for human review.

* Reads credentials from ``/mnt/file-updates-secret/`` (or
  ``FILE_UPDATES_SECRET_MOUNT``): ``gitlab_host``, ``gitlab_access_token``,
  ``git_author_name``, ``git_author_email``.
* Clones ``--repo`` at ``--ref``, rebases on ``--upstream-repo``, applies path
  seeds and YAML replacements (``yq`` for key lookup), then commits or reuses an MR.
* Writes ``RESULT_FILE_UPDATES_*`` and internal-request name results.
* After a valid run with result env vars, almost always exits ``0``; failures use
  ``RESULT_FILE_UPDATES_STATE``. Invalid YAML on a replacement target exits ``1``.
  Bad CLI flags exit before result handling (``1``; argparse uses ``2`` for
  malformed argv).

Pass ``gitlab_client`` to ``run_file_updates`` to inject a client in unit tests.
Catalog Tekton tests mock ``git`` via ``tests/mocks/git`` on ``PATH``.
"""

from . import process_file_updates  # noqa: F401
from .process_file_updates import (  # noqa: F401
    FILE_UPDATES_SECRET_MOUNT_ENV,
    load_file_updates_secrets,
    git_functions_init,
    list_konflux_open_mrs,
    blank_lines_before_yaml,
    parse_replacement_expression,
    apply_replacement_block,
    write_error_result,
    PathProcessingState,
    configure_git_environment,
    write_paths_manifest,
    sparse_dirs_from_paths,
    prepare_repository,
    resolve_target_file,
    seed_target_file,
    apply_replacements_for_entry,
    process_all_paths,
    outcome_after_path_processing,
    get_cached_diff,
    find_existing_mr_with_same_diff,
    commit_and_create_mr,
    run_file_updates,
    main,
)
