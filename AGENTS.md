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
- `scripts/python/tasks/internal/` — Python task implementations (e.g., check_embargoed_cves)
- `scripts/bash/tasks/internal/` — Bash task implementations (e.g., check-embargoed-cves)
- `templates/` — Jinja2 templates (advisory.yaml.jinja, GraphQL)
- `utils/` — General utilities (apply_template, get_resource, find_matching_purl)
- `integration-tests/` — E2E test suite for catalog integration

## Python

- Python 3.12+. Use `|` union syntax, not `Union[]` or `Optional[]`.
- `from __future__ import annotations` at the top of every module.
- Type hints on all function signatures.
- Docstrings on every module and public function (triple-double-quote, imperative mood).
- Black formatter, line length 95. Flake8 linter, line length 95, E203 ignored.
- Named loggers: `LOGGER = logging.getLogger("module_name")`.
- argparse for CLI arguments; env vars for Tekton-injected config.
- Entry point: `if __name__ == "__main__": raise SystemExit(main())`.
- Exception chaining: always use `raise ... from e`.
- File-based credentials via mounted secrets, not bare env vars for secrets.
- Retry with exponential backoff: use `retry.retry_with_exponential_backoff()` from helpers.
- HTTP requests: use `http_client.get_text()` from helpers (retries, 429/404 backoff built in).
- Tekton result files: use `tekton.result_paths()` to read env var paths, `tekton.CheckStepError` for step failures.
- Cross-cutting helpers go in `scripts/python/helpers/`; task scripts go in `scripts/python/tasks/internal/`.
- PYTHONPATH is set for flat imports: helpers and task modules are importable by name (e.g., `import retry`).

## Testing

- pytest with pytest-cov. Run: `uv sync --dev && uv run pytest`.
- Unit tests are co-located with source files (e.g., `pyxis/test_pyxis.py` next to `pyxis/pyxis.py`).
- Exception: `utils/` has tests in `utils/tests/`.
- Mocking with `unittest.mock`: use `patch`, `MagicMock`, `monkeypatch` for env vars.
- Inject dependencies as callable kwargs with real defaults, replaced only in tests.
- Integration tests live in `integration-tests/` and are not run by pytest.
- Test function names: `test_<what_is_being_tested>`. Type-hint test functions with `-> None`.

## Commits

- Conventional commits: `(chore|docs|feat|fix|refactor|revert|style|test)(<JIRA-id>): <lowercase message>`
- Max 72 characters for both title and body lines. Enforced by gitlint in CI.
- All commits must be cryptographically signed (`git commit -S`).
- Always add an `Assisted-by: <AI-agent>` trailer.

## Build

- Container image based on UBI. Dependencies managed with `uv` (see pyproject.toml).
- CI runs Black, Flake8, pytest with coverage, yamllint, and gitlint on every PR.
- PYTHONPATH in container: `/home:/home/scripts/python/helpers:/home/scripts/python/tasks/internal`.
- Wrappers are on PATH in the container (e.g., `/home/pyxis/`).

## Key Patterns

- Task scripts write results to Tekton result files; they exit 0 on success and exit 1 on failure.
- Wrapper scripts call external tools (pubtools-pulp, pubtools-marketplacesvm, etc.) via subprocess or library APIs.
