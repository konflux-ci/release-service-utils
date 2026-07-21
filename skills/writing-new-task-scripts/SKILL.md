---
name: writing-new-task-scripts
description: >-
  Use when creating a new Python task script in release-service-utils, converting
  a Tekton task from release-service-catalog, adding scripts/python/tasks/ code,
  wiring Tekton params/results, or writing co-located pytest tests for a task.
---

# Writing New Task Scripts

Create Python task scripts under `scripts/python/tasks/` that Tekton tasks in
**release-service-catalog** invoke via `command` + `args` + `env` from the utils
container image (`quay.io/konflux-ci/release-service-utils`).

**Before writing anything:** read `using-helpers` — reuse helpers instead of
duplicating auth, HTTP, file I/O, or Tekton plumbing.

---

## File placement

| Task type | Catalog path | Utils script path |
|-----------|--------------|-------------------|
| **Managed** (runs on managed cluster, secrets available) | `tasks/managed/<name>/` | `scripts/python/tasks/managed/<snake_name>.py` |
| **Internal** (runs via internal-request on another cluster) | `tasks/internal/<name>/` | `scripts/python/tasks/internal/<snake_name>.py` |

Naming: catalog uses kebab-case (`check-data-keys`); utils uses snake_case
(`check_data_keys.py`). Co-located test: `test_<snake_name>.py` in the same directory.

Helpers live in `scripts/python/helpers/` — never put reusable logic in a task file.

---

## Module boilerplate

Every task script follows this skeleton:

```python
#!/usr/bin/env python3
"""One-line summary of what the task does."""

from __future__ import annotations

import os
from pathlib import Path

import file
import tekton
from logger import logger

PROG = "my_task.py"


def run(...) -> ...:
    """Core logic — pure enough to unit-test without Tekton."""
    ...


def main() -> int:
    """Read Tekton env, call run(), write results."""
    ...
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

**Required conventions** (also in `AGENTS.md`):

- `from __future__ import annotations` at top
- Type hints on all function signatures; docstrings on module + public functions
- `PROG = "<filename>"` for error messages and Tekton result text
- Entry point: `if __name__ == "__main__": raise SystemExit(main())`
- Named loggers via `from logger import logger` (or `logging.getLogger` for submodules)
- Exception chaining: `raise ... from e`
- Black, line length 95

**Imports:** helpers and sibling task modules are on `PYTHONPATH` (see `Dockerfile`
and `pyproject.toml` `[tool.pytest.ini_options]`). Import directly:

```python
import file
import tekton
import authentication
```

Do not use relative imports from task scripts.

---

## Managed vs internal: error handling

This is the most important behavioral split:

### Managed tasks (`scripts/python/tasks/managed/`)

- **`main()` must NOT catch exceptions** — let the script fail with traceback.
- Tekton marks the step failed; no result-file dance needed.
- Example: `check_data_keys.py` — `main()` calls `run_check_data_keys()` and returns 0.

```python
def main() -> int:
    run_check_data_keys(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        data_path=Path(tekton.require_env("PARAM_DATA_PATH")),
        ...
    )
    return 0
```

### Internal tasks (`scripts/python/tasks/internal/`)

- **Catch all exceptions in `main()`** and write failure text to Tekton result files.
- The script process must **exit 0** so Tekton publishes results (catalog documents this).
- Use `tekton.CheckStepError(action, cause)` to attach a human step name to failures.
- Use `tekton.write_failure_result()` for the one-line `result` body.

Example pattern from `check_embargoed_cves.py`:

```python
def main(argv: list[str] | None = None) -> int:
    rpath, epath = tekton.result_paths_from_env("RESULT_RESULT", "RESULT_EMBARGOED_CVES")
    try:
        problem, out_rc = run_check(cve_list, mount=mount)
    except tekton.CheckStepError as e:
        out_rc, step_err = 1, e
    ...
    if not out_rc:
        rpath.write_text("Success", encoding="utf-8")
    elif step_err is not None:
        tekton.write_failure_result(rpath, name, step_err)
    return 0  # always 0 for internal tasks after writing results
```

`check_fbc_opt_in.py` is a simpler internal variant: catch `ValueError` / `CheckStepError`
in `main()`, raise `SystemExit` with a message (still exits non-zero — acceptable when
the catalog task only has one result and no "always exit 0" requirement; match siblings).

**Rule of thumb:** read the catalog task YAML and sibling internal scripts; match their
result/exit contract exactly.

---

## Tekton wiring: params, env, results

Catalog tasks pass configuration via **environment variables**, not only CLI flags.

| Catalog YAML | Utils env var | Helper |
|--------------|---------------|--------|
| `$(params.dataDir)` | `PARAM_DATA_DIR` | `tekton.require_env("PARAM_DATA_DIR")` |
| `$(params.dataPath)` | `PARAM_DATA_PATH` | `tekton.require_env("PARAM_DATA_PATH")` |
| `$(results.foo.path)` | `RESULT_FOO` | `tekton.result_paths_from_env("RESULT_FOO")` |

CLI flags (`--cves`, `--concurrent-limit`) are used when the catalog task passes
`args:` — parse with `tekton.tekton_argument_parser(PROG)` for consistent `--help`
and missing-flag behavior (stderr + exit 1).

Mount paths for secrets: use `file.path_from_env_variable("MOUNT_VAR", "/default/path")`
so tests can override via env.

Credentials: read from **mounted secret files** (`authentication.load_service_account`,
`authentication.read_mounted_text`), not bare env vars for secret values.

---

## Structure: separate `run()` from `main()`

Extract testable logic into `run()` or named functions so `main()` stays a thin
Tekton-env-to-function wrapper.

**Do not add callable kwargs to a function's signature just to make it
mockable** (e.g. `kinit: Any = authentication.kinit_with_retry`). This was an
old convention — still visible in `check_embargoed_cves.py` — but reviewers
now consistently ask authors to hardcode the real call and drop the injected
parameter instead. If `AGENTS.md` still shows the old convention on this
point, treat it as stale and follow this skill.

The same principle applies more broadly: don't shape production code around
testing needs at all (env-var switches like `USE_SHELL=1`, fake/mock classes
built into a task module, etc.). Real test tooling covers this — `mock.patch`/
`monkeypatch` for unit tests, and the catalog's `tests/mocks/<name>` directory
(auto-prepended to `PATH`) for integration-test mocking of external CLIs like
`git` or `skopeo`.

Hardcode the real call and patch it at the **call site's module** in tests
instead:

```python
# production code — call the real function directly
def run_check(cve_ids: Sequence[str], mount: Path) -> tuple[list[str], int]:
    authentication.kinit_with_retry(princ, keytab_path, kenv, max_attempts=5)
    token = osidb.get_access_token(osidb_url)
    ...
```

```python
# test — patch where the name is looked up (the task module), not where it's defined
from unittest.mock import patch

@patch("check_embargoed_cves.osidb.get_access_token", return_value="tok")
@patch("check_embargoed_cves.authentication.kinit_with_retry")
def test_run_check_success(mock_kinit, mock_token, tmp_path):
    ...
```

`check_embargoed_cves.py` and `check_fbc_opt_in.py` still use the old
callable-kwarg pattern — that's legacy, don't copy it into new task scripts.

Reference implementations:

| Task | Path | Notes |
|------|------|-------|
| Managed, schema validation | `scripts/python/tasks/managed/check_data_keys.py` | `run_check_data_keys()`, no catch in `main()` |
| Managed, JSON + single result | `scripts/python/tasks/managed/make_repo_public.py` | Thin `main()`, writes one result |
| Internal, OSIDB + results | `scripts/python/tasks/internal/check_embargoed_cves.py` | `CheckStepError`, always exit 0; **predates** the no-callable-kwargs convention |
| Internal, Pyxis queries | `scripts/python/tasks/internal/check_fbc_opt_in.py` | JSON result array, mount overrides; **predates** the no-callable-kwargs convention |

---

## Testing

- Co-locate: `test_<task>.py` next to `<task>.py`
- Run: `uv sync --dev && uv run pytest scripts/python/tasks/<managed|internal>/test_<task>.py -v`
- Function names: `test_<behavior>` with `-> None` return type
- Use `pytest`, `unittest.mock.patch`, `monkeypatch` for env vars
- Test `run()` and parsers directly; test `main()` only for wiring smoke tests
- Fixture data: minimal JSON under `tmp_path`; copy patterns from `test_check_data_keys.py`
- Mock HTTP/subprocess/auth calls with `@patch("<task_module>.<helper_module>.<function>")`
  at the task module's namespace — do **not** add callable kwargs to production
  code to make mocking easier (see "Structure" above)
- Cover every type/branch a validation function accepts, not just one (e.g. if a
  flag can be JSON `true` or the string `"true"`, test both — a real review
  comment caught a bool-only test suite for a function that also handled strings)

Prefer co-located tests whenever you add non-trivial logic — most merged
conversion PRs ship with full test coverage, so treat that as the default.
Skip tests only for a genuinely mechanical port with no new behavior, or when
the Jira ticket explicitly defers testing to a separate follow-up story
(matching the two-phase bash-extraction-then-python-rewrite pattern some
epics use). When in doubt, write the tests.

---

## Common review feedback — read before opening a PR

These are patterns that keep coming up across conversion PRs from multiple
authors and reviewers, not a one-off comment on a single PR. Check your diff
against these before requesting review:

1. **Don't re-validate what a helper already checks.** If `file.load_json_dict()`
   already raises `FileNotFoundError` when the file is missing, don't add your
   own `if not path.is_file(): raise ...` first — see `using-helpers`.
2. **Don't add callable kwargs to a function just so tests can mock it.**
   Hardcode the call, `patch`/`monkeypatch` it in tests. See "Structure" above.
3. **Let Python raise naturally when that's clear enough.** `mapping["key"]`
   raising `KeyError`, or `rpa["spec"]["origin"]` raising on a missing field,
   is often fine — don't wrap it in your own exception unless the default
   error message would be genuinely confusing in a Tekton result.
4. **Don't write comments/docstrings that describe the bash task being
   replaced** (e.g. "mirrors the bash task's X logic"). The bash is deleted once
   the catalog PR merges — the comment goes stale immediately. Describe what the
   Python code does, not what it used to look like in bash.
5. **If the same logic, constant, or validation shows up in more than one task**
   (component `public` flags, secret/mount names, file-existence checks, auth
   setup, checksum extraction, ...), it belongs in a shared helper, not a
   private function copy-pasted into each task file. This is the single most
   frequent piece of review feedback across this repo's conversion PRs — see
   `using-helpers`.
6. **Use `logger` (from the `logger` helper), never bare `print()`** — even if
   the bash task being replaced printed raw text and matching that output
   exactly seems convenient. Consistency across tasks' logs wins over
   replicating old bash formatting.
7. **Module-level constants go at the top of the file**, not inline near
   where they're first used.

---

## Dual-PR workflow (catalog + utils)

1. **Utils PR first** — add script; wait for Konflux `on-pr-<commit>` image build.
2. **Catalog PR** — thin YAML calling `/home/scripts/python/tasks/<managed|internal>/<script>.py`,
   bump `image:` to the PR digest, update `tests/pre-apply-task-hook.sh` / mocks if needed.
3. **Merge utils first**, then bump catalog image to `quay.io/konflux-ci/release-service-utils@sha256:<main>`.

Run catalog local tests: `./scripts/run-local-tests.sh tasks/<managed|internal>/<task-dir>`.

---

## Commits

```
feat(RELEASE-xxxx): add <task_name> python script

Assisted-by: cursor-agent
```

- Conventional commits: `(feat|fix|refactor|test)(RELEASE-xxxx): <lowercase summary>`
- Max 72 characters per line (gitlint)
- Cryptographically signed (`git commit -S`)
- Add `Assisted-by: <agent>` trailer when AI-assisted
