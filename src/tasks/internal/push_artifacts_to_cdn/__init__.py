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

from . import push_artifacts_to_cdn  # noqa: F401
from .push_artifacts_to_cdn import main  # noqa: F401
