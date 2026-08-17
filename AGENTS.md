# AGENTS.md

Container image (UBI9) with Python scripts, wrappers, and templates used by Tekton tasks in release-service-catalog.

## Repository Structure

- `pyxis/` — Pyxis API wrapper (container images, signatures, metadata, GraphQL)
- `pubtools-pulp-wrapper/` — CDN push via Pulp/Exodus
- `pubtools-marketplacesvm-wrapper/` — Cloud marketplace pushes via StArMap
- `publish-to-cgw-wrapper/` — Content Gateway publishing (for push-artifacts-to-cdn)
- `developer-portal-wrapper/` — Content Gateway publishing (for push-disk-images)
- `kafka/` — Kafka producer/consumer
- `scripts/python/helpers/` — Shared helpers (auth, HTTP, retry, tekton results, file I/O, OSIDB)
- `scripts/python/tasks/` — Python task implementations (e.g., check_embargoed_cves)
- `templates/` — Jinja2 templates (advisory.yaml.jinja, GraphQL)
- `utils/` — General utilities (apply_template, get_resource, find_matching_purl)
- `integration-tests/` — Orchestration harness that detects which catalog E2E suites are affected by utils changes, patches a temporary catalog fork with the new image, and runs the matching tests
- `schemas/` - Schema files to describe structs such as the `data` one used in various python scripts

## Python

- Use `|` union syntax, not `Union[]` or `Optional[]`.
- `from __future__ import annotations` at the top of every module.
- Type hints on all function signatures.
- Docstrings on every module and public function (triple-double-quote, imperative mood).
- Prefer some duplication over premature abstractions, but check existing helpers and modules before writing code that may already exist.
- Use `logging` instead of `print()`: `from logger import logger` in task scripts; `logging.getLogger("module_name")` in standalone helper submodules.
- argparse for CLI arguments; env vars for Tekton-injected config.
- Entry point: `if __name__ == "__main__": raise SystemExit(main())`.
- Exception chaining: always use `raise ... from e`. All exception paths should be handled explicitly.
- File-based credentials via mounted secrets, not bare env vars for secrets.
- Retry with exponential backoff: use `retry.retry_with_exponential_backoff()` from helpers.
- HTTP requests: use `http_client.get_text()` from helpers (retries, 429/404 backoff built in).
- Tekton result files: use `tekton.result_paths_from_env()` to read env var paths, `tekton.CheckStepError` for step failures.
- Cross-cutting helpers go in `scripts/python/helpers/`; task scripts go in `scripts/python/tasks/`.
- Flag any function that is not called elsewhere; the unit tests for it do not count as being called elsewhere.
- Run validations with: `uv run black . && uv run flake8 && ruff check --select D <changed files>`.

## Testing

- Unit tests are co-located with source files; exception: `utils/` has tests in `utils/tests/`.
- Tekton result tests: use `tmp_path` for result files and `monkeypatch.setenv("RESULT_*", str(path))`.
- Integration tests live in `integration-tests/` and are not run by pytest.

## Commits

- Conventional commits: `(chore|docs|feat|fix|refactor|revert|style|test)(<JIRA-id>): <lowercase message>`
- Max 72 characters for both title and body lines. Enforced by gitlint in CI.
- All commits must be cryptographically signed (`git commit -S`).
- When generating commits with the assistance of an AI tool, add an `Assisted-by: <AI-agent>` trailer.

## Key Patterns

- Internal task scripts (`scripts/python/tasks/internal/`) must catch all exceptions in `main()` and save errors to Tekton result files (the script itself must succeed).
- Managed task scripts (`scripts/python/tasks/managed/`) must not catch exceptions in `main()` — let the script fail with traceback.
