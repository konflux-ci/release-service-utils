"""Tests for cdn helpers."""

from __future__ import annotations

import pytest
from release_service_utils.helpers.cdn import cdn


def test_cdn_env_secrets_production() -> None:
    """Production environment returns the public Exodus/Pulp/CGW config."""
    config = cdn.cdn_env_secrets("production")
    assert config["exodusGwSecret"] == "exodus-prod-secret"
    assert config["exodusGwEnv"] == "live"
    assert config["pulpSecret"] == "rhsm-pulp-prod-secret"
    assert config["udcacheSecret"] == "udcache-prod-secret"
    assert "developers.redhat.com" in config["cgwHostname"]
    assert config["cgwSecret"] == "cgw-service-account-prod-secret"


def test_cdn_env_secrets_stage() -> None:
    """Stage environment returns the preprod Exodus/Pulp/CGW config."""
    config = cdn.cdn_env_secrets("stage")
    assert config["exodusGwSecret"] == "exodus-prod-secret"
    assert config["exodusGwEnv"] == "pre"
    assert config["pulpSecret"] == "rhsm-pulp-stage-secret"
    assert config["udcacheSecret"] == "udcache-stage-secret"
    assert "developers.qa.redhat.com" in config["cgwHostname"]
    assert config["cgwSecret"] == "cgw-service-account-stage-secret"


def test_cdn_env_secrets_qa() -> None:
    """QA environment returns the stage-Exodus, qa-Pulp/CGW config."""
    config = cdn.cdn_env_secrets("qa")
    assert config["exodusGwSecret"] == "exodus-stage-secret"
    assert config["exodusGwEnv"] == "live"
    assert config["pulpSecret"] == "rhsm-pulp-qa-secret"
    assert config["udcacheSecret"] == "udcache-qa-secret"
    assert "developers.qa.redhat.com" in config["cgwHostname"]
    assert config["cgwSecret"] == "cgw-service-account-stage-secret"


def test_cdn_env_secrets_invalid_raises() -> None:
    """An unrecognized environment raises ValueError."""
    with pytest.raises(ValueError, match="cdn.env.*must be one of"):
        cdn.cdn_env_secrets("invalid")


def test_cdn_env_secrets_empty_raises() -> None:
    """An empty environment string raises ValueError."""
    with pytest.raises(ValueError, match="cdn.env.*must be one of"):
        cdn.cdn_env_secrets("")


def test_cdn_env_secrets_returns_copy() -> None:
    """Mutating a returned config does not affect subsequent lookups."""
    config1 = cdn.cdn_env_secrets("production")
    config2 = cdn.cdn_env_secrets("production")
    config1["exodusGwSecret"] = "modified"
    assert config2["exodusGwSecret"] == "exodus-prod-secret"
