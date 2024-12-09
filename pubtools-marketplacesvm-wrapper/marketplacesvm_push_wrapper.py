#!/usr/bin/env python3
"""
Python script to push staged content to various cloud marketplaces.

This is a simple wrapper pubtools-marketplacesvm-push command that is able to push and publish
content to various cloud marketplaces. This wrapper supports pushing cloud images only from
staged source using the cloud schema.

For more information please refer to documentation:
* https://github.com/release-engineering/pubtools-marketplacesvm
* https://release-engineering.github.io/pushsource/sources/staged.html#root-destination-cloud-images  # noqa:E501
* https://release-engineering.github.io/pushsource/schema/cloud.html#cloud-schema

Red Hat Slack channel:
#stratosphere-bau
"""
import argparse
import logging
import os
import re
import subprocess
import sys

LOG = logging.getLogger("pubtools-marketplacesvm-wrapper")
DEFAULT_LOG_FMT = "%(asctime)s [%(levelname)-8s] %(message)s"
DEFAULT_DATE_FMT = "%Y-%m-%d %H:%M:%S %z"
COMMAND = "pubtools-marketplacesvm-marketplace-push"
CLOUD_MKTS_ENV_VARS_STRICT = ("CLOUD_CREDENTIALS",)


def parse_args():
    parser = argparse.ArgumentParser(
        prog="marketplacesvm_push_wrapper",
        description="Push staged cloud images to various cloud marketplaces.",
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

    parser.add_argument(
        "--nochannel",
        action="store_true",
        help=(
            "Do as much as possible without making content available to end-users, then stop."
            "May be used to improve the performance of a subsequent full push."
        ),
    )

    starmap = parser.add_argument_group("Content mapping settings")
    starmap.add_argument(
        "--starmap-file",
        help="YAML file containing the content mappings on StArMap APIv2 format.",
        required=True,
    )

    return parser.parse_args()


def get_source_url(stagedirs):
    regex = re.compile(r"^/([^/\0]+(/)?)+$")
    for item in stagedirs:
        if not regex.match(item):
            raise ValueError("Not a valid staging directory: %s" % item)

    return f"staged:{','.join(stagedirs)}"


def settings_to_args(parsed):
    settings_to_arg_map = {
        "starmap_file": "--repo-file",
        "source": "",
    }
    out = ["--offline"]  # The "offline" arg is used to prevent invoking a StArMap server
    if parsed.nochannel:
        out.append("--pre-push")

    for setting, arg in settings_to_arg_map.items():
        if value := getattr(parsed, setting):
            if arg:
                out.extend([arg])
            out.extend([value])

    for _ in range(parsed.debug):
        out.append("--debug")

    return out


def validate_env_vars(args):
    assert all([os.getenv(item) for item in CLOUD_MKTS_ENV_VARS_STRICT]), (
        "Provide all required CLOUD_MKTS environment variables: "
        f"{', '.join(CLOUD_MKTS_ENV_VARS_STRICT)}"
    )

    args.source = get_source_url(args.source)
    return args


def main():
    args = validate_env_vars(parse_args())

    loglevel = logging.DEBUG if args.debug else logging.INFO

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(loglevel)

    logging.basicConfig(
        level=loglevel,
        format=DEFAULT_LOG_FMT,
        datefmt=DEFAULT_DATE_FMT,
        handlers=[stream_handler],
    )

    if args.dry_run:
        LOG.info("This is a dry-run!")

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


if __name__ == "__main__":
    main()
