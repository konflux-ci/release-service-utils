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
