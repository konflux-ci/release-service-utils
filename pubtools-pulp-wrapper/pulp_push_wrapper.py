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
import json
import logging
import os
import re
import ssl
import subprocess
import sys
import time
from urllib import parse, request

LOG = logging.getLogger("pubtools-pulp-wrapper")
DEFAULT_LOG_FMT = "%(asctime)s [%(levelname)-8s] %(message)s"
DEFAULT_DATE_FMT = "%Y-%m-%d %H:%M:%S %z"
COMMAND = "pubtools-pulp-push"
TIMESTAMP_TOKEN_RE = re.compile(r"^\d{8,14}$")
POLL_INTERVAL_SECONDS = 2
POLL_TIMEOUT_SECONDS = 120
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

    publish = parser.add_argument_group("Publish options")
    publish.add_argument(
        "--no-clean",
        action="store_true",
        help="Disable cleaning remote content not in the staged source. By default, "
        "remote content not present in the current push will be removed.",
        default=False,
    )

    return parser.parse_args()


def get_source_url(stagedirs):
    for item in stagedirs:
        if not re.match(r"^/[^,]{1,4000}$", item):
            raise ValueError("Not a valid staging directory: %s" % item)

    return f"staged:{','.join(stagedirs)}"


def get_source_dirs(source_url):
    if not source_url.startswith("staged:"):
        return []
    return [item for item in source_url.removeprefix("staged:").split(",") if item]


def normalize_timestamped_name(filename):
    tokens = filename.split("-")
    for idx, token in enumerate(tokens):
        # Remove a timestamp-like token between stable filename parts.
        if TIMESTAMP_TOKEN_RE.match(token) and 0 < idx < (len(tokens) - 1):
            return "-".join(tokens[:idx] + tokens[idx + 1 :])
    return filename


def build_timestamp_search_patterns(filename):
    tokens = filename.split("-")
    patterns = set()

    # Always match the exact filename variant being pushed.
    patterns.add(f"^{re.escape(filename)}$")

    has_timestamp = any(TIMESTAMP_TOKEN_RE.match(token) for token in tokens)
    if has_timestamp:
        # Match other timestamp variants in the same filename position.
        pattern_tokens = [
            r"\d{8,14}" if TIMESTAMP_TOKEN_RE.match(token) else re.escape(token)
            for token in tokens
        ]
        patterns.add("^" + "-".join(pattern_tokens) + "$")

        # Also match the equivalent non-timestamped name if present.
        normalized = normalize_timestamped_name(filename)
        patterns.add(f"^{re.escape(normalized)}$")

    return sorted(patterns)


def build_repo_file_map(stagedirs):
    repo_to_files = {}
    for stagedir in stagedirs:
        if not os.path.isdir(stagedir):
            LOG.warning("Skipping non-directory staged source: %s", stagedir)
            continue
        for repo_name in os.listdir(stagedir):
            files_dir = os.path.join(stagedir, repo_name, "FILES")
            if not os.path.isdir(files_dir):
                continue
            files = {
                filename
                for filename in os.listdir(files_dir)
                if os.path.isfile(os.path.join(files_dir, filename))
            }
            if not files:
                continue
            repo_to_files.setdefault(repo_name, set()).update(files)
    return repo_to_files


def make_ssl_context(cert_file=None, key_file=None):
    ctx = ssl.create_default_context()
    if cert_file and key_file:
        ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
    elif cert_file:
        ctx.load_cert_chain(certfile=cert_file)
    return ctx


def pulp_request(url, context, payload=None):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=data, headers=headers)
    with request.urlopen(req, context=context) as response:
        body = response.read()
    if not body:
        return None
    return json.loads(body.decode("utf-8"))


def wait_for_task(task_href, context):
    task_url = task_href
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    while time.time() < deadline:
        task = pulp_request(task_url, context=context)
        state = task.get("state")
        if state == "finished":
            if task.get("error") or task.get("exception") or task.get("traceback"):
                raise RuntimeError(f"Pulp task failed: {task_url}: {task}")
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"Timed out waiting for Pulp task: {task_url}")


def prune_matching_content_before_push(parsed):
    if parsed.no_clean:
        return

    stagedirs = get_source_dirs(parsed.source)
    repo_to_files = build_repo_file_map(stagedirs)
    if not repo_to_files:
        LOG.info("No staged FILES content found for pre-push cleanup.")
        return

    if not parsed.pulp_cert:
        LOG.warning("Skipping pre-push cleanup: --pulp-cert is required for mTLS Pulp calls.")
        return

    pulp_base = parsed.pulp_url.rstrip("/")
    context = make_ssl_context(cert_file=parsed.pulp_cert, key_file=parsed.pulp_key)

    for repo_name, files in sorted(repo_to_files.items()):
        matched_existing = set()
        repo_path = parse.quote(repo_name, safe="")
        search_url = f"{pulp_base}/pulp/api/v2/repositories/{repo_path}/search/units/"

        for file_name in sorted(files):
            for pattern in build_timestamp_search_patterns(file_name):
                payload = {
                    "criteria": {
                        "filters": {
                            "unit": {
                                "name": {
                                    "$regex": pattern,
                                }
                            }
                        }
                    },
                    "include_repos": True,
                }
                response = pulp_request(search_url, context=context, payload=payload) or []
                for unit in response:
                    name = unit.get("metadata", {}).get("name")
                    if name:
                        matched_existing.add(name)

        # Remove all matched units first so re-push is deterministic.
        names_to_remove = sorted(matched_existing)
        if not names_to_remove:
            LOG.info("No existing matching units to remove in repo %s.", repo_name)
            continue

        LOG.info(
            "Removing %d existing matching unit(s) from repo %s before re-push.",
            len(names_to_remove),
            repo_name,
        )
        unassociate_url = f"{pulp_base}/pulp/api/v2/repositories/{repo_path}/actions/unassociate/"
        unassociate_payload = {
            "criteria": {
                "filters": {
                    "unit": {
                        "name": {
                            "$in": names_to_remove,
                        }
                    }
                }
            }
        }
        response = pulp_request(unassociate_url, context=context, payload=unassociate_payload) or {}
        for task in response.get("spawned_tasks", []):
            href = task.get("_href")
            if href:
                wait_for_task(parse.urljoin(pulp_base, href), context=context)


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

    if not parsed.no_clean:
        out.append("--clean")

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

    if args.debug:
        log_exodus_env()

    cmd_args = settings_to_args(args)
    command = [COMMAND] + cmd_args
    cmd_str = " ".join(command)

    if args.dry_run:
        LOG.info("Would have run: %s", cmd_str)
    else:
        try:
            prune_matching_content_before_push(args)
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
