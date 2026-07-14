#!/usr/bin/env python3
"""Collect charon configuration from data, snapshot, and release files.

Extract parameters needed by charon (MRRC/NRRC publishing tool) and
write them as a shell-sourceable env file, a config file, and Tekton
result files.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import shlex
from pathlib import Path
from typing import Any

import tekton
from file import load_json_dict
from logger import logger

PROG = "collect_charon_params.py"


@dataclasses.dataclass(frozen=True)
class CharonParams:
    """Extracted charon parameters ready for downstream tasks."""

    target: str
    product_name: str
    product_version: str
    sign_key: str
    oci_registry: str
    aws_secret: str
    sign_ca_secret: str
    author: str
    config: Any


def collect_charon_params(
    data: dict[str, Any],
    snapshot: dict[str, Any],
    release: dict[str, Any],
) -> CharonParams:
    """Extract charon configuration from loaded JSON dicts."""
    charon: dict[str, Any] = data["charon"]

    environment: str = charon["environment"]
    release_val: str = charon["release"]
    package_type: str = charon.get("packageType", "maven")
    target = f"{environment}-{package_type}-{release_val}"

    product_name: str = data["releaseNotes"]["product_name"]
    product_version: str = data["releaseNotes"]["product_version"]

    signing = charon.get("signing") or {}
    sign_key: str = signing.get("signKey", "")
    sign_ca_secret: str = signing.get("signCASecret", "")

    aws_secret: str = charon["awsSecret"]

    components = snapshot.get("components", [])
    try:
        oci_registry = "%".join(c["containerImage"] for c in components)
    except KeyError as e:
        msg = "One or more components are missing the 'containerImage' key"
        logger.error(msg)
        raise KeyError(msg) from e

    author: str = release["status"]["attribution"]["author"]

    config: Any = charon["config"]

    return CharonParams(
        target=target,
        product_name=product_name,
        product_version=product_version,
        sign_key=sign_key,
        oci_registry=oci_registry,
        aws_secret=aws_secret,
        sign_ca_secret=sign_ca_secret,
        author=author,
        config=config,
    )


def write_charon_env(env_path: Path, params: CharonParams) -> None:
    """Write charon parameters as a shell-sourceable env file."""
    lines = [
        f"export CHARON_TARGET={shlex.quote(params.target)}",
        f"export CHARON_PRODUCT_NAME={shlex.quote(params.product_name)}",
        f"export CHARON_PRODUCT_VERSION={shlex.quote(params.product_version)}",
    ]
    if params.sign_key:
        lines.append(f"export CHARON_SIGN_KEY={shlex.quote(params.sign_key)}")
    lines.append(f"export CHARON_OCI_REGISTRY={shlex.quote(params.oci_registry)}")
    lines.append(f"export CHARON_AUTHOR={shlex.quote(params.author)}")
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_charon_config(config_path: Path, config: Any) -> None:
    """Write the charon config file.

    String values are written as-is (matching ``jq -r`` behaviour);
    non-string values are serialised as JSON.
    """
    if isinstance(config, str):
        text = config
    else:
        text = json.dumps(config)
    config_path.write_text(text + "\n", encoding="utf-8")


def run(
    work_dir: Path,
    data_json_path: str,
    snapshot_path: str,
    release_path: str,
    result_charon_param_file_path: Path,
    result_charon_config_file_path: Path,
    result_charon_aws_secret: Path,
    result_charon_sign_ca_secret: Path,
) -> None:
    """Orchestrate collection of charon parameters."""
    data = load_json_dict(work_dir / data_json_path)
    snapshot = load_json_dict(work_dir / snapshot_path)
    release_data = load_json_dict(work_dir / release_path)

    params = collect_charon_params(data, snapshot, release_data)

    env_rel = str(Path(data_json_path).parent / "charon.env")
    cfg_rel = str(Path(data_json_path).parent / "charon-config.yaml")

    write_charon_env(work_dir / env_rel, params)
    write_charon_config(work_dir / cfg_rel, params.config)

    result_charon_param_file_path.write_text(env_rel, encoding="utf-8")
    result_charon_config_file_path.write_text(cfg_rel, encoding="utf-8")
    result_charon_aws_secret.write_text(params.aws_secret, encoding="utf-8")
    result_charon_sign_ca_secret.write_text(params.sign_ca_secret, encoding="utf-8")

    logger.info("Charon parameters collected successfully")


def _parse_args(
    argv: list[str] | None = None,
) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, prog=PROG)
    parser.add_argument(
        "--work-dir",
        required=True,
        help="Base directory for data files",
    )
    parser.add_argument(
        "--data-json-path",
        required=True,
        help="Relative path to the data JSON file",
    )
    parser.add_argument(
        "--snapshot-path",
        required=True,
        help="Relative path to the snapshot JSON file",
    )
    parser.add_argument(
        "--release-path",
        required=True,
        help="Relative path to the release JSON file",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run charon parameter collection."""
    args = _parse_args(argv)
    r_param, r_cfg, r_aws, r_ca = tekton.result_paths_from_env(
        "RESULT_CHARON_PARAM_FILE_PATH",
        "RESULT_CHARON_CONFIG_FILE_PATH",
        "RESULT_CHARON_AWS_SECRET",
        "RESULT_CHARON_SIGN_CA_SECRET",
    )
    run(
        work_dir=Path(args.work_dir),
        data_json_path=args.data_json_path,
        snapshot_path=args.snapshot_path,
        release_path=args.release_path,
        result_charon_param_file_path=r_param,
        result_charon_config_file_path=r_cfg,
        result_charon_aws_secret=r_aws,
        result_charon_sign_ca_secret=r_ca,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
