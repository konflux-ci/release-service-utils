"""Python client for skopeo container image operations."""

from __future__ import annotations

import json
from logging import Logger
import subprocess
from typing import Any, Optional

from rsmodels import ContainerImage, ContainerImageConfig, ContainerImageRaw
from rsmodels.secret import Secret


def inspect(
    image_ref: str,
    *,
    config: bool = False,
    raw: bool = False,
    retry_times: int = 3,
) -> subprocess.CompletedProcess[str]:
    """Run ``skopeo inspect`` on a container image reference."""
    cmd = ["skopeo", "inspect", "--retry-times", str(retry_times)]
    if config:
        cmd.append("--config")
    if raw:
        cmd.append("--raw")
    cmd.append(f"docker://{image_ref}")
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def copy(
    source: str,
    dest: str,
    *,
    retry_times: int = 3,
) -> subprocess.CompletedProcess[str]:
    """Run ``skopeo copy`` to copy an image between transports."""
    cmd = ["skopeo", "copy", "--retry-times", str(retry_times), source, dest]
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


class SkopeoClientError(Exception):
    """Exception raised when skopeo command fails.

    Attributes:
        command: The command that was executed
        returncode: The exit code of the process
        stdout: Standard output from the command
        stderr: Standard error from the command

    """

    def __init__(
        self,
        message: str,
        command: list[str],
        returncode: int,
        stdout: str,
        stderr: str,
    ):
        """Initialize the SkopeoClientError with command details."""
        super().__init__(message)
        self.command = command
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    def __str__(self) -> str:
        """Return a detailed error message including command and outputs."""
        return (
            f"{super().__str__()}\n"
            f"    Command: {' '.join(self.command)}\n"
            f"    Return code: {self.returncode}\n"
            f"    Stderr: {self.stderr}"
        )


class SkopeoClient:
    """Client for executing skopeo container image operations.

    This client wraps skopeo CLI commands and provides a Python API for
    common operations like inspect and copy.

    Args:
        debug: Enable debug output
        insecure_policy: Run without policy check
        tmpdir: Directory for temporary files
        command_timeout: Timeout for command execution (e.g., "5m", "30s")
        override_arch: Override architecture for image selection
        override_os: Override OS for image selection
        override_variant: Override architecture variant

    Example:
        client = SkopeoClient(debug=True)

        # Inspect an image
        data = client.inspect("docker://quay.io/image:tag")
        print(data["Digest"])

        # Copy with credentials
        from secret import Secret
        client.copy(
            "docker://source/image:tag",
            "docker://dest/image:tag",
            src_creds=Secret("user:pass"),
            dest_creds=Secret("user:pass"),
            all=True,
        )

    """

    def __init__(
        self,
        *,
        debug: bool = False,
        insecure_policy: bool = False,
        tmpdir: Optional[str] = None,
        command_timeout: Optional[str] = None,
        override_arch: Optional[str] = None,
        override_os: Optional[str] = None,
        override_variant: Optional[str] = None,
        logger: Optional[Logger] = None,
    ):
        """Initialize the SkopeoClient with optional global settings."""
        self.debug = debug
        self.insecure_policy = insecure_policy
        self.tmpdir = tmpdir
        self.command_timeout = command_timeout
        self.override_arch = override_arch
        self.override_os = override_os
        self.override_variant = override_variant
        self.logger = logger

    def _build_global_flags(self) -> list[str]:
        """Build global flags that apply to all skopeo commands."""
        flags: list[str] = []

        if self.debug:
            flags.append("--debug")
        if self.insecure_policy:
            flags.append("--insecure-policy")
        if self.tmpdir is not None:
            flags.extend(["--tmpdir", self.tmpdir])
        if self.command_timeout is not None:
            flags.extend(["--command-timeout", self.command_timeout])
        if self.override_arch is not None:
            flags.extend(["--override-arch", self.override_arch])
        if self.override_os is not None:
            flags.extend(["--override-os", self.override_os])
        if self.override_variant is not None:
            flags.extend(["--override-variant", self.override_variant])

        return flags

    def _run_command(self, cmd: list[str]) -> subprocess.CompletedProcess:
        """Run a skopeo command and handle errors.

        Args:
            cmd: Complete command including 'skopeo' and all arguments

        Returns:
            CompletedProcess with stdout/stderr captured

        Raises:
            SkopeoClientError: If command exits with non-zero status

        """
        try:
            result = subprocess.run(
                [c.unveil() if isinstance(c, Secret) else c for c in cmd],
                capture_output=True,
                text=True,
                check=True,
            )
            return result
        except subprocess.CalledProcessError as e:
            raise SkopeoClientError(
                f"Skopeo command failed with exit code {e.returncode}",
                command=cmd,
                returncode=e.returncode,
                stdout=e.stdout or "",
                stderr=e.stderr or "",
            ) from e

    def inspect(
        self,
        image: str,
        *,
        format: Optional[str] = None,
        retry_times: Optional[int] = None,
        creds: Optional[Secret] = None,
        tls_verify: Optional[bool] = None,
        config: bool = False,
        raw: bool = False,
        no_tags: bool = False,
        authfile: Optional[str] = None,
        cert_dir: Optional[str] = None,
        registry_token: Optional[Secret] = None,
    ) -> ContainerImage | ContainerImageConfig | ContainerImageRaw | str:
        """Inspect a container image and return metadata.

        Args:
            image: Image reference (e.g., "docker://quay.io/image:tag")
            format: Output format template (e.g., "{{.Digest}}")
                   If None, returns parsed JSON as dict
                   If specified, returns formatted string
            retry_times: Number of retry attempts for network operations
            creds: Registry credentials as Secret
            tls_verify: Whether to verify TLS certificates
            config: Output configuration instead of manifest
            raw: Output raw manifest or configuration
            no_tags: Don't list available tags in output
            authfile: Path to authentication file
            cert_dir: Directory containing certificates
            registry_token: Bearer token for registry access

        Returns:
            ContainerImage, ContainerImageConfig, ContainerImageRaw, or str

        Raises:
            SkopeoClientError: If inspect command fails

        Example:
            # Get full metadata as dict
            data = client.inspect("docker://quay.io/image:tag")

            # Get just the digest
            digest = client.inspect(
                "docker://quay.io/image:tag",
                format="{{.Digest}}"
            )

        """
        cmd = ["skopeo", *self._build_global_flags(), "inspect"]

        if format is not None:
            cmd.extend(["--format", format])
        if retry_times is not None:
            cmd.extend(["--retry-times", str(retry_times)])
        if creds is not None:
            cmd.extend(["--creds", creds])
        if tls_verify is not None:
            cmd.extend(["--tls-verify", str(tls_verify).lower()])
        if config:
            cmd.append("--config")
        if raw:
            cmd.append("--raw")
        if no_tags:
            cmd.append("--no-tags")
        if authfile is not None:
            cmd.extend(["--authfile", authfile])
        if cert_dir is not None:
            cmd.extend(["--cert-dir", cert_dir])
        if registry_token is not None:
            cmd.extend(["--registry-token", registry_token])

        cmd.append(image)

        result = self._run_command(cmd)
        output = result.stdout.strip()
        #print("OUTPUT", output)

        # If format was specified, return the formatted string
        if format is not None:
            return output

        # Otherwise parse and return JSON as the appropriate model
        try:
            if raw:
                return ContainerImageRaw.from_json(output)
            if config:
                return ContainerImageConfig.from_json(output)
            return ContainerImage.from_json(output)
        except Exception as e:
            if raw:
                model_name = "ContainerImageRaw"
            elif config:
                model_name = "ContainerImageConfig"
            else:
                model_name = "ContainerImage"
            raise SkopeoClientError(
                f"Failed to parse skopeo output as JSON ({model_name}): {e}",
                command=cmd,
                returncode=0,
                stdout=output,
                stderr=result.stderr,
            ) from e

    def copy(
        self,
        source: str,
        destination: str,
        *,
        all: Optional[bool] = None,
        preserve_digests: Optional[bool] = None,
        format: Optional[str] = None,
        retry_times: Optional[int] = None,
        quiet: bool = False,
        src_creds: Optional[Secret] = None,
        src_tls_verify: Optional[bool] = None,
        src_no_creds: bool = False,
        src_cert_dir: Optional[str] = None,
        src_authfile: Optional[str] = None,
        dest_creds: Optional[Secret] = None,
        dest_tls_verify: Optional[bool] = None,
        dest_no_creds: bool = False,
        dest_cert_dir: Optional[str] = None,
        dest_authfile: Optional[str] = None,
        remove_signatures: bool = False,
    ) -> None:
        """Copy a container image from source to destination.

        Args:
            source: Source image reference (e.g., "docker://quay.io/src:tag")
            destination: Destination image reference
            all: Copy all images if source is a multi-arch manifest list
            preserve_digests: Preserve digests during copy
            format: Manifest type to use (oci, v2s1, v2s2)
            retry_times: Number of retry attempts for network operations
            quiet: Suppress output information
            src_creds: Source registry credentials
            src_tls_verify: Verify source TLS certificates
            src_no_creds: Access source registry anonymously
            src_cert_dir: Source certificate directory
            src_authfile: Source authentication file
            dest_creds: Destination registry credentials
            dest_tls_verify: Verify destination TLS certificates
            dest_no_creds: Access destination registry anonymously
            dest_cert_dir: Destination certificate directory
            dest_authfile: Destination authentication file
            remove_signatures: Don't copy signatures from source

        Raises:
            SkopeoClientError: If copy command fails

        Example:
            from secret import Secret

            client.copy(
                "docker://quay.io/src/image:tag",
                "docker://quay.io/dest/image:tag",
                all=True,
                preserve_digests=True,
                src_creds=Secret("user:pass"),
                dest_creds=Secret("user:pass"),
                retry_times=5,
            )

        """
        cmd = ["skopeo", *self._build_global_flags(), "copy"]

        if all is not None:
            if all:
                cmd.append("--all")
        if preserve_digests is not None:
            if preserve_digests:
                cmd.append("--preserve-digests")
        if format is not None:
            cmd.extend(["--format", format])
        if retry_times is not None:
            cmd.extend(["--retry-times", str(retry_times)])
        if quiet:
            cmd.append("--quiet")
        if remove_signatures:
            cmd.append("--remove-signatures")

        # Source options
        if src_creds is not None:
            cmd.extend(["--src-creds", src_creds])
        if src_tls_verify is not None:
            cmd.extend([f"--src-tls-verify={str(src_tls_verify).lower()}"])
        if src_no_creds:
            cmd.append("--src-no-creds")
        if src_cert_dir is not None:
            cmd.extend(["--src-cert-dir", src_cert_dir])
        if src_authfile is not None:
            cmd.extend(["--src-authfile", src_authfile])

        # Destination options
        if dest_creds is not None:
            cmd.extend(["--dest-creds", dest_creds])
        if dest_tls_verify is not None:
            cmd.extend([f"--dest-tls-verify={str(dest_tls_verify).lower()}"])
        if dest_no_creds:
            cmd.append("--dest-no-creds")
        if dest_cert_dir is not None:
            cmd.extend(["--dest-cert-dir", dest_cert_dir])
        if dest_authfile is not None:
            cmd.extend(["--dest-authfile", dest_authfile])

        cmd.extend([source, destination])
        if self.logger:
            self.logger.info(f"Running skopeo copy command: {cmd}")
        self._run_command(cmd)
