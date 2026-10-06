# Migrate historical RPM advisory epochs

HUM-9401 provides an **offline** migration utility. It reads a local advisory
checkout and saved RPM repository metadata. It does not call Pulp, push Git
commits, publish advisories, or activate producers. Production execution belongs
to HUM-9405 and requires a coordinated rollout after staging validation.

An omitted epoch in a historical advisory is unknown. The tool inserts a
nonzero epoch only after resolving the exact name, version, release,
architecture and `repository_id` against published primary metadata. It checks
the primary file's SHA256 against its saved `repomd.xml`. If an advisory carries
a SHA256 checksum qualifier or artifact `sha256`/`checksum` field, that checksum
must also match. Multiple matching epochs, missing packages, unsupported input
or an explicit epoch that disagrees with metadata block application. The tool
does not infer an epoch from another release of the same package.

Repository IDs and metadata origins are supplied by the operator and must be
reviewed. The metadata hash establishes the relationship between two saved
files; it is not a signature or proof that an operator supplied the correct
repository. Capture both files from the authenticated published repository or
a trusted historical snapshot and retain them with the audit.

## Initial Hummingbird audit

The [2026-10-06 aggregate audit](rpm-advisory-epoch-audit-2026-10-06.json) records
3,064 RPM advisories in `hummingbird-tenant`, all with product name
`Red Hat Hardened Images` and stream `hummingbird-1`. The captured public
metadata verifies 5,932 nonzero-epoch corrections in 420 advisories and 42,546
zero-epoch entries. It cannot resolve 2,354 public entries; metadata for another
31 entries across six private Hummingbird repositories was not captured. The
full plan is blocked. The full per-row report contains private identities and
must be retained internally rather than committed to the public repository.

A local rehearsal on an independently copied, fully verified subset of 2,965
RPM advisories applied all 5,932 proposed corrections. Its 48,165 distinct
producer PURLs matched both filtering contracts, epoch-only differences stayed
distinct, a repeat application changed nothing, and rollback restored the
original bytes. The authoritative advisory checkout was unchanged. Staging
Tekton execution and rendered/distributed artifact verification remain pending.

## Prepare evidence and a dry run

1. Obtain a disposable checkout of the authoritative advisory repository at a
   recorded commit. Use the Hummingbird origin directory as the migration root,
   for example `data/advisories/hummingbird-tenant`. Preserve the original
   checkout for comparison. Do not use a checkout with an automatic publish
   hook for this preparation.
2. Save each repository's `repodata/repomd.xml` and its referenced `primary`
   file. Preserve the compressed primary file as downloaded. Gzip, bzip2, xz
   and uncompressed XML are supported. SHA256 in repomd is required. Use
   authenticated tooling with credentials in files for private repositories.
3. Create `repositories.json` outside the advisory root. Paths are absolute or
   relative to this JSON file. An example public mapping is below. Read the
   referenced primary location from the corresponding repomd; its name is not
   stable between publications.

```json
[
  {
    "repository_id": "public-hummingbird-x86_64-rpms",
    "source": "https://packages.redhat.com/api/pulp-content/public-hummingbird/x86_64/",
    "repomd": "x86_64/repomd.xml",
    "primary": "x86_64/primary.xml.gz"
  },
  {
    "repository_id": "public-hummingbird-aarch64-rpms",
    "source": "https://packages.redhat.com/api/pulp-content/public-hummingbird/aarch64/",
    "repomd": "aarch64/repomd.xml",
    "primary": "aarch64/primary.xml.gz"
  },
  {
    "repository_id": "public-hummingbird-source-rpms",
    "source": "https://packages.redhat.com/api/pulp-content/public-hummingbird/source/",
    "repomd": "source/repomd.xml",
    "primary": "source/primary.xml.gz"
  }
]
```

Run from a release-service-utils checkout with its locked Python environment:

```bash
uv run python utils/migrate_rpm_advisory_epochs.py plan \
  --root /path/to/advisories/data/advisories/hummingbird-tenant \
  --repositories /path/to/evidence/repositories.json \
  --report /path/to/evidence/audit.json
```

`plan` does not modify advisories. The report must be outside the advisory tree.
Exit status 0 means the inventory is clean; 2 means entries need investigation.
The JSON report records every advisory's before/after SHA256, each RPM entry's
outcome, proposed PURL edits, package checksums/locations, and repository
metadata hashes. IDs, descriptions, signing keys, unrelated content, comments,
quoting and line endings are preserved. Only the relevant PURL scalar is edited.
The epoch is inserted immediately after `arch`, matching HUM-9398's producer;
every other raw qualifier is retained. Verified zero-epoch rows are unchanged.

Resolve every blocked entry before application. Missing identities may need
older repository snapshots or metadata generated from verified RPM headers
with proven repository membership. Append historical snapshots to the same
configuration; multiple snapshots for one repository are supported. A different
epoch for the same NEVRA is ambiguous unless an advisory checksum identifies
the correct RPM. Do not substitute the current epoch of another build, strip
epochs for matching, or edit the report to bypass a blocker.

`--repository-id ID` can be repeated to inventory an explicitly limited rollout
scope. Excluded RPMs are counted as `out-of-scope`, not verified. Each scoped
repository needs metadata. An excluded repository must remain paused if it
shares the producers being activated. A public-only audit cannot establish
readiness for private repositories. Explicit zero or noncanonical epoch
qualifiers require manual review because the corrected producer omits zero and
uses canonical decimal nonzero epochs.

Treat reports and backups as potentially sensitive: private package identities
may be present. Retain the full reports in the appropriate internal ticket or
restricted release evidence location; publish aggregate findings only.

## Validate the transition in staging

Complete this before HUM-9404 end-to-end upgrade validation:

1. Pause advisory writers and releases in the affected staging origin. Fix and
   resolve the complete audit for every repository being activated. Pin the
   metadata snapshots and advisory commit. Confirm the two producer changes
   (HUM-9399 and HUM-9400, using HUM-9398) are available in the staged image.
2. Generate and review a fresh plan. Apply only to the staging checkout with a
   new backup directory outside the advisory tree:

   ```bash
   uv run python utils/migrate_rpm_advisory_epochs.py apply \
     --root /path/to/staging/advisories/data/advisories/hummingbird-tenant \
     --report /path/to/evidence/staging-audit.json \
     --backup /path/to/evidence/staging-backup
   ```

   Application checks the entire inventory before writing. The selected origin
   root includes its container advisories too. Keep all origin writers paused
   and confirm the checkout is current with its upstream branch before merging.
   New, removed or
   edited advisories invalidate the plan, including unrelated advisory edits.
   All originals and a manifest are saved before any replacement. Each file
   replacement is atomic; the whole multi-file migration is not a transaction.
   Keep writers paused. If interrupted, roll back using the original backup and
   generate a fresh plan instead of attempting a partially applied plan again.
3. Review and commit the changed YAML using the advisory repository's normal
   process. Identify its render/publish hooks and downstream consumers before
   merging. Re-render and redistribute all affected advisory artifacts under
   their existing IDs, including customer-facing representations where
   applicable. Record what was regenerated and verify the distributed PURLs.
   Publishing behavior is not part of this utility.
4. Activate the corrected producers while writers remain paused. Rerun a
   previously released nonzero-epoch binary, noarch and source RPM through both
   internal filtering and advisory creation. It must match the original
   advisory instead of creating another one. Repeat the release and verify
   idempotency. Verify epoch-only differences remain distinct and zero-epoch
   rows still match their unchanged PURLs.
5. Run `plan` again against the migrated checkout. It must propose no new edits.
   Reapplying the original report must be a no-op. Exercise rollback in staging
   and retain the resulting hashes and task results. Resume staging releases
   only after the producer and distributed-data checks agree.

The unit suite exercises the real `create-advisory` shared filtering helper and
a pinned copy of the catalog task's jq categorization query. It covers source,
noarch, zero and nonzero epochs, repeat application, byte preservation,
checksum-based disambiguation, conflicting epochs, stale inventories, and
recovery after interrupted application. These tests do not replace staging
Tekton execution and downstream publishing validation.

## Production sequencing and rollback (HUM-9405)

After staging validation and HUM-9404 pass, obtain the production rollout
approval. Then pause all old writers for the affected scope, capture a fresh
checkout and metadata evidence, resolve and review a fresh plan, migrate and
republish the historical advisories, activate both corrected producers, and
verify both task paths and distributed artifacts before resuming releases.
Do not let old producers recreate epoch-less PURLs between migration and
activation. Merging the utility does not initiate this process.

The corrected nonzero PURLs are an identity change. Old producers omit epoch
and will not match migrated rows. New producers will not match unmigrated
nonzero rows. Coordinate both producers and historical data in one paused
cutover; changing one side first during active releases can create duplicate
advisories. Zero-epoch producer output remains compatible.

For rollback, pause writers again and restore the producer image/configuration
together with the advisory data. On the checkout containing the migration:

```bash
uv run python utils/migrate_rpm_advisory_epochs.py rollback \
  --root /path/to/advisories/data/advisories/hummingbird-tenant \
  --backup /path/to/evidence/production-backup
```

Rollback verifies every original backup hash and every current advisory hash
before changing any file. It refuses to overwrite post-migration edits. It can
recover a partially applied plan and is repeatable. Review the resulting diff,
restore/revert the migration through the advisory repository's normal process,
then regenerate and redistribute the original artifacts under the same IDs.
The utility restores local bytes only; it cannot reverse an already published
artifact or release notification. If advisories changed after cutover, reconcile
those changes separately rather than forcing the rollback. Verify both exact
matching paths against the restored producers before resuming.
