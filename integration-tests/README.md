# release-service-utils integration tests (catalog integration-test orchestration)

**`utils-e2e-catalog-pipeline.yaml`** runs one `PipelineRun` that:

1. **`extract-snapshot`** — reads **SNAPSHOT** for the utils container image and git URL/revision.

2. **`find-affected-tasks`** — from the utils repo diff (vs `main`), decides which catalog integration suites are implicated and writes task results `pipelineTestSuite` / `pipelineUsed` (space-separated tokens when several suites apply).

3. **`match-affected-suite`** — sets **`runCatalogE2e`** to `yes` only if find-affected produced a non-empty **`pipelineUsed`** (task result) and this scenario’s **`PIPELINE_TEST_SUITE`** param value appears among the suite tokens in **`pipelineTestSuite`**. Otherwise later work is skipped (success without fork or catalog child run).

4. **`git-clone-catalog-patch-git-push`** — clones catalog, patches utils image refs, pushes a temporary GitHub fork (only when the `when:` clauses pass: non-empty **`pipelineUsed`** and **`runCatalogE2e`** is `yes`).

5. **`run-catalog-e2e`** — creates a child `PipelineRun` for catalog’s integration-test pipeline (`integration-tests/pipelines/e2e-tests-staging-pipeline.yaml` in **release-service-catalog**), using pipeline params **`PIPELINE_TEST_SUITE`** / **`PIPELINE_USED`** (same names as the catalog IntegrationTestScenario; not the raw find-affected strings), then waits for success.

Callers that target one catalog suite per check pass different **`PIPELINE_TEST_SUITE`** / **`PIPELINE_USED`** pairs while reusing this pipeline file.

**When steps are skipped:** `find-affected-tasks` writes empty **`pipelineTestSuite`** / **`pipelineUsed`** (nothing to test for this diff), **`find-affected-tasks`** fails (for example when merge-base against `main` cannot be computed), or **`match-affected-suite`** sets **`runCatalogE2e`** to `no` because **`pipelineUsed`** is empty or this scenario’s **`PIPELINE_TEST_SUITE`** is not among the suite tokens in **`pipelineTestSuite`**.

**`finally`** always runs: it tries to delete the per-run temp GitHub fork and may warn if the catalog branch moved since the run started.

**Task results:** `find-affected-tasks` exposes `pipelineTestSuite` and `pipelineUsed` for gating; **`match-affected-suite`** sets **`runCatalogE2e`**.

Implementation details are in `utils-e2e-catalog-pipeline.yaml` and `lib/find_catalog_suite_from_utils_diff.py`.

---

## Layout

| Path | Purpose |
|------|---------|
| `pipelines/utils-e2e-catalog-pipeline.yaml` | Params mirror catalog ITS (`PIPELINE_TEST_SUITE`, `PIPELINE_USED`, secret names, …); optional params and defaults are in the YAML; tasks above. |
| `run-test.sh` | Submits a `PipelineRun` with the same param names as catalog ITS (`PIPELINE_TEST_SUITE`, `PIPELINE_USED`, …); optional `--wait` (`./run-test.sh --help`). |
| `lib/find_catalog_suite_from_utils_diff.py` | Maps a utils diff to affected suites; `--print-all-pairs` for tooling. |
| `lib/catalog_clone_patch_push.sh` | Clone catalog, patch image refs, temp repo, push. |
| `lib/run_single_catalog_e2e_suite.py` | Creates the child catalog `PipelineRun` and waits. |
| `lib/catalog_cleanup.py` | Optional standalone cleanup (same idea as the pipeline `finally` task: delete temp fork, catalog drift warning). |

Files under `integration-tests/lib/` are copied into the utils image at `/home/integration-tests/lib/`.

---

## Prerequisites (Konflux / cluster)

- GitHub access for the temp fork: token with `repo` and `delete_repo`, and permissions to create repos under the org used by **`destRepoPrefix`** (see pipeline param).

- **`VAULT_PASSWORD_SECRET_NAME`**, **`GITHUB_TOKEN_SECRET_NAME`**, **`KUBECONFIG_SECRET_NAME`**: pipeline parameters (same names as catalog ITS); values are **Kubernetes Secret names** passed through to the **child** catalog **`e2e-tests-staging-pipeline`** (keys `password`, `token`, `kubeconfig`).

  Those Secrets must exist in **`rhtap-release-2-tenant`** where the child `PipelineRun` runs. The parent **`run-catalog-e2e`** step does **not** mount a kubeconfig Secret; it uses **in-cluster** `kubectl` like any other Tekton task.

- **`PIPELINE_TEST_SUITE`** / **`PIPELINE_USED`**: must match a real `integration-tests/<dir>/` and RPA `pipelines/managed/<name>/` pairing, as for catalog’s own integration tests.

- **`e2eWaitTimeout`**, **`catalogE2eRunnerImage`**: optional pipeline params; defaults are in `utils-e2e-catalog-pipeline.yaml` (same pattern as other optional Tekton params).

- Child catalog `PipelineRun` is created in **`rhtap-release-2-tenant`** on the **same cluster** as the parent PLR (in-cluster `kubectl`), alongside catalog **`simple-e2e-test`** and e2e ExternalSecrets.

---

## How this runs

**Production:** Konflux **IntegrationTestScenario** on **release-service-utils** with `pathInRepo: integration-tests/pipelines/utils-e2e-catalog-pipeline.yaml`, supplying the same param names as catalog ITS (**`PIPELINE_TEST_SUITE`**, **`PIPELINE_USED`**, **`VAULT_PASSWORD_SECRET_NAME`**, …), plus **`SNAPSHOT`**. Omit optional params to use pipeline defaults.

### Run locally (`run-test.sh`)

1. **Cluster**

   `kubectl` context targets the right cluster. **`NAMESPACE`** must exist (default `rhtap-release-2-tenant`). In that namespace, ensure the GitHub token Secret named by **`GITHUB_TOKEN_SECRET_NAME`** (default `e2e-test-github-token`) exists for clone/push/`finally`.

   In **`rhtap-release-2-tenant`**, ensure the three e2e Secrets exist (defaults `e2e-test-vault-password`, `e2e-test-github-token`, `e2e-test-service-account-kubeconfig`) for the **child** catalog run, or override the corresponding env vars when invoking **`run-test.sh`**.

2. **Catalog fork/branch for this pipeline** (clone for find-affected + patch/push)

   Defaults are **`CATALOG_REPO`**=`konflux-ci/release-service-catalog`, **`CATALOG_REF`**=`development`.

   For a fork, e.g. `export CATALOG_REPO=my-org/release-service-catalog` and `export CATALOG_REF=my-feature-branch`.

3. **Snapshot JSON (`SNAPSHOT_FILE`)**

   Same *kind* of payload as Konflux’s **`SNAPSHOT`** pipeline parameter (a JSON string), but it does **not** have to be a complete or “real” snapshot.

   **`run-test.sh` and `extract-snapshot` only `jq` three paths** — `components[0].containerImage`, `components[0].source.git.url`, and `components[0].source.git.revision`. You do **not** need `apiVersion`/`kind`, `metadata`, `application`, or any other field a **Snapshot** custom resource might carry on the cluster; extra keys from a full Konflux snapshot are harmless and ignored.

   For **`components[0]`** you must supply:

   - **`containerImage`** — digest of the **release-service-utils** image to exercise (e.g. `quay.io/.../release-service-utils@sha256:...`).
   - **`source.git.url`** and **`source.git.revision`** — repo URL and commit the pipeline should treat as the utils source (used for diffing against `main` and for catalog patch metadata).

   Create **`snapshot.json`** by hand from your pushed **release-service-utils** image digest and the matching git URL and commit (e.g. `git rev-parse HEAD`). The following is **complete** for this pipeline (no `application`, no CR envelope):

   ```json
   {
     "components": [
       {
         "containerImage": "quay.io/your-org/release-service-utils@sha256:...",
         "source": {
           "git": {
             "url": "https://github.com/your-org/release-service-utils.git",
             "revision": "full40charcommitsha..."
           }
         }
       }
     ]
   }
   ```

4. **Required env** before `./run-test.sh`

   **`SNAPSHOT_FILE`** (path to that JSON), **`PIPELINE_TEST_SUITE`** (e.g. `e2e`), **`PIPELINE_USED`** (e.g. `fbc-release`).

   See `./run-test.sh --help` for optional env vars. Unset optionals are omitted from the submitted `PipelineRun` so Tekton uses defaults from `utils-e2e-catalog-pipeline.yaml`. With **`--wait`**, **`E2E_WAIT_TIMEOUT`** is required (for `kubectl wait`); if you also want that value as pipeline **`e2eWaitTimeout`**, export it before invoking the script (same env var).

5. **Commands** (from `integration-tests/`)

   ```bash
   export SNAPSHOT_FILE=/path/to/snapshot.json
   export PIPELINE_TEST_SUITE=e2e
   export PIPELINE_USED=fbc-release
   ./run-test.sh                      # add --wait to block; with --wait the PipelineRun is deleted when done
   ```

   The child catalog e2e step uses the git resolver in **`lib/run_single_catalog_e2e_suite.py`** (upstream catalog `development` today); overriding that for a fork requires changing that script.
