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

## Certificate Expiration Checker

The `utils/check_cert_expiration` script checks certificate expiration and optionally sends Slack notifications.

### Usage

```bash
check_cert_expiration <cert_file> [warn_days]
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SLACK_WEBHOOK_URL` | Slack webhook for notifications | (none - no notifications) |
| `CERT_IDENTIFIER` | Custom name for the cert in notifications | File path |
| `NOTIFICATION_CONFIGMAP` | ConfigMap for rate limiting | `cert-expiry-notifications` |
| `NOTIFICATION_NAMESPACE` | Namespace for ConfigMap | `internal-services` |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Certificate valid (or expiring soon - warning logged) |
| 1 | Certificate expired |
| 2 | Error (file not found, invalid format) |
