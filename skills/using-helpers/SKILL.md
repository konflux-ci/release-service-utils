---
name: using-helpers
description: >-
  Use when implementing task logic in release-service-utils and tempted to write
  a new utility function — check scripts/python/helpers/ first. Also use when
  working with Tekton results, HTTP, auth, file I/O, Pyxis, OSIDB, git, or
  subprocess wrappers.
---

# Using Shared Helpers

**Before writing any new utility function, check if a helper already exists in
`scripts/python/helpers/`.** Task scripts should orchestrate; helpers should
implement reusable, task-agnostic behavior.

Helpers are on `PYTHONPATH` in the container and in pytest — import by module name:

```python
import file
import tekton
import http_client
```

Subpackage `vcs` provides host-specific git operations:

```python
from vcs import git, github, gitlab
```

---

## Don't duplicate what a helper already does

Before guarding a call with your own check, confirm the helper doesn't already
do it. This is one of the most frequent review comments across conversion PRs
in this repo — reviewers have called it out on unrelated tasks written by
different authors, months apart:

```python
# Don't — file.load_json_dict already raises if the file doesn't exist
if not data_file.is_file():
    raise FileNotFoundError("No valid data file was provided.")
data = file.load_json_dict(data_file)
```

```python
# Do — trust the helper, let it raise
data = file.load_json_dict(data_file)
```

This applies broadly: `tekton.require_env` already exits on a missing var,
`file.load_json_dict` already raises `TypeError` if the root isn't a dict, etc.
Read the helper's docstring before adding a check it already performs — the
duplicate check is dead code plus an extra test to maintain.

## If you write it more than once, it belongs in a helper

The single most repeated piece of review feedback in this repo's history is
some version of: *"we already do this in several other tasks — please move it
to a helper instead of adding another copy."* It's been said about file
existence/validation, per-component snapshot logic, secret and mount names,
auth/cert setup, and checksum extraction — all things one author wrote inline
in a task file before a reviewer pointed out other tasks needed the same
logic. Concretely: a shared credential-redaction helper (`redact.py`) exists
today *because* three separate modules each had their own private copy of the
same substring-replacement logic, until a reviewer pushed to consolidate them
into one.

Before writing task-specific logic, ask "would a second task plausibly need
this too?" If yes, write it in the closest matching helper module (or a new
one) from the start, with its own test — don't wait for a reviewer to ask you
to move it later.

---

## How to find the right helper

This skill lists **what module to reach for**, not every function it exposes —
that detail lives in the module itself and would go stale here the moment
someone adds a function. Before writing new logic:

1. Scan the index below for a module that matches the domain (file I/O, HTTP,
   Tekton, auth, git, ...).
2. Confirm the exact function/signature by reading the module's source or
   docstring, e.g. `rg "^def " scripts/python/helpers/file.py`.
3. Check the module's co-located `test_<module>.py` for real usage examples.
4. If nothing fits, `rg` the helpers directory for related keywords — a
   helper may exist under a name you didn't expect.
5. New helpers get added over time; if you don't see something here that
   plausibly exists, check `scripts/python/helpers/` directly rather than
   assuming this list is exhaustive.

---

## Helper index

Import by module name — helpers are on `PYTHONPATH` in the container and in
pytest.

### Core infrastructure

| Module | Purpose | Import |
|--------|---------|--------|
| `logger` | Configured stderr logger for task scripts | `from logger import logger` |
| `file` | JSON/path/temp-file/gzip helpers (`load_json_dict` raises if missing/not-a-dict) | `import file` |
| `tekton` | Tekton result files, step errors, CLI parsing (`require_env`, `result_paths_from_env`) | `import tekton` |
| `retry` | Exponential backoff for transient failures | `import retry` |
| `redact` | Strip credentials from text before logging/results | `from redact import redact_secrets` |
| `subprocess_cmd` | Run subprocess commands with optional stderr log capture | `import subprocess_cmd` |
| `kubectl` | Kubernetes ConfigMap fetch via kubectl CLI | `import kubectl` |
| `internal_request` | Write internal-request Tekton result paths | `import internal_request` |

```python
import file
import tekton

data = file.load_json_dict(data_dir / data_path)
data_path = tekton.require_env("PARAM_DATA_DIR")
```

### HTTP and authentication

| Module | Purpose | Import |
|--------|---------|--------|
| `http_client` | GET with retries (429/404 backoff), session builder | `import http_client` |
| `authentication` | Kerberos, keytabs, mounted service-account layouts | `import authentication` |

```python
import authentication

princ, keytab, text = authentication.load_service_account(
    mount, ("osidb_url",), principal_file="name", keytab_b64_file="base64_keytab"
)
```

### Domain APIs

| Module | Purpose |
|--------|---------|
| `osidb` | OSIDB flaw API — token fetch and flaw queries |
| `pyxis_api` | Pyxis URL mapping, repository GET/PATCH, catalog prefixes |
| `image_ref` | Image ref parsing; Quay digest → git SHA; Pyxis URL builder |
| `snapshot` | Read Konflux snapshot JSON (component fields) |
| `iib` | IIB REST client; gzip/base64 for Tekton's 4KB result limit |
| `skopeo` | Skopeo CLI wrapper (`inspect`, `copy`) |
| `oras_utils` | OCI artifact operations via `oras` CLI |

```python
import pyxis_api

repo_json = pyxis_api.get_repository_json(pyxis_url, repo_id, auth=auth)
```

### Advisory and artifact pipeline

Full orchestration scripts for the push-artifacts / signing workflow — prefer
calling their `run()` over reimplementing routing, cert checks, or component
loops: `advisory_data`, `extract_artifacts`, `push_unsigned`, `sign_mac`,
`sign_windows`, `compress_artifacts`, `generate_checksums`,
`build_checksum_map`, `push_artifacts`. Read each module's docstring for
specifics — this pipeline changes often enough that a static table here would
drift quickly.

### Version control (`helpers/vcs/`)

| Module | Purpose |
|--------|---------|
| `vcs.git` | Host-agnostic git CLI (clone, commit, push, retry) |
| `vcs.github` | GitHub App auth, REST API, PR workflow |
| `vcs.gitlab` | GitLab OAuth git auth, sparse clone, raw file URLs |

```python
from vcs import git

repo_dir = git.clone(parent_dir, clone_url, revision="main")
```

### Other repo packages (not under `helpers/`)

Task scripts also call into top-level packages copied into the image:
`pyxis/` (Pyxis container image API), `utils/` (`apply_template`,
`get_resource`, `find_matching_purl`), `pubtools-pulp-wrapper/`,
`publish-to-cgw-wrapper/`, `developer-portal-wrapper/`. Use existing wrappers
via `subprocess_cmd.run_cmd` or their Python APIs — do not shell out with raw
paths unless matching an existing task pattern.

---

## Decision checklist

1. Need file/JSON/env-path handling? → `file`
2. Need Tekton results or CLI parsing? → `tekton`
3. Need HTTP GET/PATCH with retries? → `http_client`
4. Need Kerberos / mounted secrets? → `authentication`
5. Need subprocess with stderr capture? → `subprocess_cmd`
6. Need git/GitHub/GitLab? → `vcs.*`
7. Logic reads/interprets fields from a shared domain object (e.g. a snapshot
   component's `public` flag)? → put it in that object's helper (e.g.
   `snapshot`), not your task file — other tasks likely need it too.
8. None of the above, or unsure? → grep `scripts/python/helpers/` for the
   domain first; only add a **new function to the closest existing module**
   (with a co-located test) if nothing fits — new module only if the domain
   is genuinely distinct.
