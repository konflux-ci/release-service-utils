# release-service-utils

Collection of scripts needed by Release Service.

## Python Package Management

This project uses [uv](https://docs.astral.sh/uv/) for Python package management.

### Setup Local Environment

```bash
uv sync --all-groups
```

This installs all dependencies including dev dependencies.

### Managing Dependencies

Add or change a pinned dependency:
```bash
uv add "package-name==version"
```

## RPM Advisory Epoch Migration

Use the offline migration utility to audit and backfill verified epochs in historical
RPM advisory PURLs. See [the migration procedure](docs/rpm-advisory-epoch-migration.md)
for evidence preparation, dry-run reporting, compatibility, staging validation and rollback.
