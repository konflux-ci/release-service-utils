"""Per-environment secret and hostname config for the CDN delivery stack.

Covers the systems involved in pushing released artifacts to customers:
Exodus GW, Pulp, UDCache, and Content Gateway.
"""

from __future__ import annotations

# The Exodus GW secret is the same for production and stage — only the env
# (live vs pre) and the Pulp URL differ. The stage CGW uses the 'qa' host:
# developers.qa.redhat.com.
_CDN_ENV_SECRETS: dict[str, dict[str, str]] = {
    "production": {
        "exodusGwSecret": "exodus-prod-secret",
        "exodusGwEnv": "live",
        "pulpSecret": "rhsm-pulp-prod-secret",
        "udcacheSecret": "udcache-prod-secret",
        "cgwHostname": "https://developers.redhat.com/content-gateway/rest/admin",
        "cgwSecret": "cgw-service-account-prod-secret",
    },
    "stage": {
        "exodusGwSecret": "exodus-prod-secret",
        "exodusGwEnv": "pre",
        "pulpSecret": "rhsm-pulp-stage-secret",
        "udcacheSecret": "udcache-stage-secret",
        "cgwHostname": "https://developers.qa.redhat.com/content-gateway/rest/admin",
        "cgwSecret": "cgw-service-account-stage-secret",
    },
    "qa": {
        "exodusGwSecret": "exodus-stage-secret",
        "exodusGwEnv": "live",
        "pulpSecret": "rhsm-pulp-qa-secret",
        "udcacheSecret": "udcache-qa-secret",
        "cgwHostname": "https://developers.qa.redhat.com/content-gateway/rest/admin",
        "cgwSecret": "cgw-service-account-stage-secret",
    },
}


def cdn_env_secrets(env: str) -> dict[str, str]:
    """Return Exodus/Pulp/UDCache/Content-Gateway secret and hostname config for *env*.

    Raise ``ValueError`` when *env* is not one of production, stage, or qa.
    """
    config = _CDN_ENV_SECRETS.get(env)
    if config is None:
        msg = f"cdn.env in the data file must be one of [production, stage, qa], got {env!r}"
        raise ValueError(msg)
    return dict(config)
