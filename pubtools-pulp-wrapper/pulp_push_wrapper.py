#!/usr/bin/env python3
"""
Python script to push staged content to CDN using rhsm-pulp and exodus-gw integration.

This is a simple wrapper pubtools-pulp-push command that is able to push and publish
content via Pulp. This wrapper supports pushing content only from staged source
with unit types supported in rhsm-pulp (as of July 2024).

For more information please refer to documentation:
* https://release-engineering.github.io/pubtools-pulp/
* https://release-engineering.github.io/pubtools-exodus/
* https://release-engineering.github.io/pushsource/

Required env vars for exodus-gw integration:
* EXODUS_PULP_HOOK_ENABLED
* EXODUS_GW_CERT
* EXODUS_GW_KEY
* EXODUS_GW_URL
* EXODUS_GW_ENV

Optional env vars:
* EXODUS_GW_TIMEOUT

UD cache flush credentials:
* either use username/password via CLI args
* or set path to cert/key with env vars: UDCACHE_CERT and UDCACHE_KEY

"""
import argparse
import logging
import os
import re
import subprocess

LOG = logging.getLogger("pubtools-pulp-wrapper")
DEFAULT_LOG_FMT = "%(asctime)s [%(levelname)-8s] %(message)s"
DEFAULT_DATE_FMT = "%Y-%m-%d %H:%M:%S %z"
COMMAND = "pubtools-pulp-push"
EXODUS_ENV_VARS_STRICT = (
    "EXODUS_PULP_HOOK_ENABLED",
    "EXODUS_GW_CERT",
    "EXODUS_GW_KEY",
    "EXODUS_GW_URL",
    "EXODUS_GW_ENV",
)
EXODUS_ENV_VARS_OTHERS = ("EXODUS_GW_TIMEOUT",)


def parse_args():
    parser = argparse.ArgumentParser(
        prog="pulp_push_wrapper",
        description="Push staged content to CDN via rhsm-pulp and exodus-gw.",
    )

    parser.add_argument("--dry-run", action="store_true", help="Log command to be executed")
    parser.add_argument(
        "--debug",
        "-d",
        action="count",
        default=0,
        help=("Show debug logs; can be provided up to three times to enable more logs"),
    )

    parser.add_argument(
        "--source",
        action="append",
        help="Path(s) to staging directory",
        required=True,
    )

    pulp = parser.add_argument_group("Pulp settings")
    pulp.add_argument("--pulp-url", help="Pulp server URL", required=True)
    pulp.add_argument(
        "--pulp-cert",
        help="Pulp certificate. Can also be a single file (.pem)",
        default=None,
    )
    pulp.add_argument(
        "--pulp-key",
        help="Pulp certificate key",
        default=None,
    )

    ud = parser.add_argument_group(
        "UD Cache settings",
        (
            "Set UDCACHE_CERT and UDCACHE_KEY for cert/key auth."
            " or only UDCACHE_CERT if cert in PEM format"
        ),
    )
    ud.add_argument(
        "--udcache-url",
        help=(
            "Base URL of UD cache flush API; "
            "if omitted, UD cache flush features are disabled."
        ),
    )
    ud.add_argument("--udcache-user", help="Username for UD cache flush")
    ud.add_argument(
        "--udcache-password",
        help="Password for UD cache flush (or set UDCACHE_PASSWORD)",
        default="",
    )

    return parser.parse_args()


def get_source_url(stagedirs):
    for item in stagedirs:
        if not re.match(r"^/[^,]{1,4000}$", item):
            raise ValueError("Not a valid staging directory: %s" % item)

    return f"staged:{','.join(stagedirs)}"


def settings_to_args(parsed):
    settings_to_arg_map = {
        "pulp_url": "--pulp-url",
        "pulp_cert": "--pulp-certificate",
        "pulp_key": "--pulp-certificate-key",
        "udcache_url": "--udcache-url",
        "udcache_user": "--udcache-user",
        "udcache_password": "--udcache-password",
        "source": "--source",
    }
    out = []
    for setting, arg in settings_to_arg_map.items():
        if value := getattr(parsed, setting):
            out.extend([arg, value])

    for _ in range(parsed.debug):
        out.append("--debug")

    return out


def log_exodus_env():
    for item in EXODUS_ENV_VARS_STRICT + EXODUS_ENV_VARS_OTHERS:
        LOG.debug("%s:%s", item, os.getenv(item, "UNSET"))


def validate_args(args):
    assert all([os.getenv(item) for item in EXODUS_ENV_VARS_STRICT]), (
        f"Provide all required exodus-gw environment variables: "
        f"{', '.join(EXODUS_ENV_VARS_STRICT)}"
    )

    args.source = get_source_url(args.source)
    return args


def main():
    args = validate_args(parse_args())

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format=DEFAULT_LOG_FMT,
        datefmt=DEFAULT_DATE_FMT,
    )

    if args.dry_run:
        LOG.info("This is a dry-run!")

    if args.debug:
        log_exodus_env()

    cmd_args = settings_to_args(args)
    command = [COMMAND] + cmd_args
    cmd_str = " ".join(command)

    if args.dry_run:
        LOG.info("Would have run: %s", cmd_str)
    else:
        try:
            LOG.info("Running %s", cmd_str)
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError:
            LOG.exception("Command %s failed, check exception for details", cmd_str)
            raise
        except Exception as exc:
            LOG.exception("Unknown error occurred")
            raise RuntimeError from exc


def entrypoint():
    main()


if __name__ == "__main__":
    entrypoint()
