#!/usr/bin/env python3
import argparse
import logging
import os
import tempfile

import yaml

from macos_commands import MacOSCommands
from oras_commands import OrasCommands
from ssh_connection import SSHConnection
from utils import zip_files

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(description="macOS Signing and Notarization Script")
    parser.add_argument(
        "config_file",
        help="Path to the file containing the secrets of the mac host and the OCI registry",
    )
    parser.add_argument("digest", help="ORAS digest of the unsigned content")
    return parser.parse_args()


def load_config(config_file):
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


def main():
    args = parse_arguments()
    config = load_config(args.config_file)

    try:
        with SSHConnection(
            config["mac_host"], config["mac_user"], config["mac_password"]
        ) as ssh:
            # with SSHConnection(
            # config['mac_host'], config['mac_user'], config['key_filename']) as ssh:
            with tempfile.TemporaryDirectory() as temp_dir:
                oras = OrasCommands(
                    ssh,
                    config["oci_registry_repo"],
                    config["quay_username"],
                    config["quay_password"],
                )
                macos = MacOSCommands(ssh)

                logger.info("Pulling unsigned content...")
                oras.pull_content(args.digest, temp_dir)

                logger.info("Unlocking keychain...")
                macos.unlock_keychain(config["keychain_password"])

                logger.info("Signing binaries...")
                signed_files = macos.sign_binaries(config["signing_identity"], temp_dir)

                logger.info(f"Binaries signed: {signed_files}")

                # Files needs to be zipped for the notarization command.
                zip_file_name = "signed_binaries.zip"
                zip_path = os.path.join(temp_dir, zip_file_name)
                zip_files(ssh, temp_dir, zip_path)

                logger.info("Submitting for notarization...")
                notarization_result = macos.notarize_binaries(
                    config["apple_id"],
                    config["app_specific_password"],
                    config["team_id"],
                    zip_path,
                )
                logger.info(f"Notarization succesful, results: {notarization_result}")

                logger.info("Pushing signed content...")
                signed_digest = oras.push_zip(zip_path)

                logger.info(f"Signed zipped file pushed. New digest: {signed_digest}")
                return signed_digest

    except Exception as e:
        logger.error(f"Operation failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
