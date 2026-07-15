"""Entry point for the push-artifacts-to-cdn Tekton task.

Called as a single step from the catalog task, this script runs each stage
in sequence: extract, push unsigned, sign (Mac and Windows), compress,
generate checksums, push to CDN, and build the advisory checksum map.

Any exception raised by a stage is caught here: the Tekton result file
receives a short error description and the script exits with code 0 so
Tekton records the result text rather than masking it with a generic
step-failure message.

## Shared file-system layout

All stages read and write under ``CONTENT_DIR`` (default ``/shared/artifacts``).
The tree below shows how a component directory evolves across stages::

  /shared/
  ├── snapshot.json          ← written by compress_artifacts; read by generate_checksums
  │                             and build_checksum_map (contains updated Windows filenames)
  └── artifacts/
      └── <component>/
          │
          │  [after extract_artifacts]
          ├── has_mac                    ← flag: component has macOS binaries
          ├── has_windows                ← flag: component has Windows binaries
          ├── has_linux                  ← flag: component has Linux binaries
          ├── unsigned/
          │   ├── macos/amd64/           ← raw macOS binaries (pre-signing)
          │   ├── windows/amd64/         ← raw Windows binaries (pre-signing)
          │   └── linux/amd64/           ← raw Linux binaries
          │
          │  [after push_unsigned — Mac/Windows uploaded to Quay as OCI artifacts]
          ├── unsigned_mac_digest.txt    ← Quay digest of pushed unsigned Mac OCI artifact
          ├── unsigned_windows_digest.txt
          ├── supplementary/             ← readme/license/changelog held out during signing
          │   ├── macos/
          │   └── windows/
          │
          │  [after sign_mac / sign_windows — signed artifacts pulled back from Quay]
          ├── signed_mac_digest.txt      ← Quay digest of signed Mac OCI artifact
          ├── signed_windows_digest.txt
          ├── signed/
          │   ├── macos/                 ← signed macOS binaries
          │   └── windows/              ← signed Windows binaries
          │
          │  [after compress_artifacts — supplementary/ restored, archives created]
          └── ready_for_distribution/
              ├── product-macos-amd64.tar.gz
              ├── product-windows-amd64.zip
              ├── product-linux-amd64.tar.gz
              │
              │  [after generate_checksums]
              ├── sha256sum.txt          ← merged checksums for all components
              ├── sha256sum.txt.sig      ← GPG clearsign
              └── sha256sum.txt.gpg      ← GPG detached signature
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from release_service_utils.helpers import tekton
from release_service_utils.helpers import build_checksum_map
from release_service_utils.helpers import compress_artifacts
from release_service_utils.helpers import extract_artifacts
from release_service_utils.helpers import generate_checksums
from release_service_utils.helpers import push_artifacts as push_artifacts_mod
from release_service_utils.helpers import push_unsigned
from release_service_utils.helpers import sign_mac
from release_service_utils.helpers import sign_windows

PROG = "push_artifacts_to_cdn.py"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Return parsed arguments for the wrapper."""
    p = argparse.ArgumentParser(
        prog=PROG,
        description="Run all push-artifacts-to-cdn steps sequentially.",
    )
    p.add_argument(
        "--concurrent-limit",
        type=int,
        default=3,
        help="Maximum number of images to pull at once",
    )
    p.add_argument("--quay-url", required=True, help="Quay repository URL base")
    p.add_argument(
        "--pipeline-run-uid",
        required=True,
        help="Unique ID for this pipeline run",
    )
    p.add_argument(
        "--kerberos-realm",
        default="IPA.REDHAT.COM",
        help="Kerberos realm for the checksum host",
    )
    p.add_argument(
        "--exodus-gw-env",
        required=True,
        help="Environment to use in the Exodus Gateway",
    )
    p.add_argument("--cgw-hostname", required=True, help="Content Gateway hostname")
    p.add_argument(
        "--cert-expiration-warn-days",
        type=int,
        default=7,
        help="Days before certificate expiration to warn",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point: run every step in order and write Tekton results."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    args = parse_args(argv[1:] if argv is not None else None)

    rpath, cmap_path, published_path = tekton.result_paths_from_env(
        "RESULT_RESULT",
        "RESULT_CHECKSUM_MAP",
        "RESULT_PUBLISHED_FILES",
    )

    # push_artifacts.run() reads RESULT_PUBLISHED_FILES to know where to
    # write the published-files list.
    os.environ["RESULT_PUBLISHED_FILES"] = str(published_path)

    checksum_map_ref = ""
    try:
        extract_artifacts.run(args.concurrent_limit)
        push_unsigned.run(args.quay_url, args.pipeline_run_uid)
        sign_mac.run(args.quay_url, args.pipeline_run_uid)
        sign_windows.run(args.quay_url, args.pipeline_run_uid)
        compress_artifacts.run(args.quay_url)
        generate_checksums.run(args.kerberos_realm, args.pipeline_run_uid)
        push_artifacts_mod.run(
            args.exodus_gw_env,
            args.cgw_hostname,
            args.cert_expiration_warn_days,
        )
        checksum_map_ref = build_checksum_map.run()
    except Exception as exc:
        rpath.write_text(f"{PROG}: ERROR {exc}", encoding="utf-8")
        cmap_path.write_text(checksum_map_ref, encoding="utf-8")
        return 0

    rpath.write_text("Success", encoding="utf-8")
    cmap_path.write_text(checksum_map_ref, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
