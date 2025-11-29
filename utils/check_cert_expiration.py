#!/usr/bin/env python3
"""
check_cert_expiration - Check if a certificate is expired and log its expiration date

This utility validates X.509 certificates and provides detailed expiration information.
It can read certificates from files or stdin and will fail if a certificate is expired.

Usage:
    check_cert_expiration.py <cert_file_path> [cert_name]
    echo "$cert_content" | check_cert_expiration.py - [cert_name]

Arguments:
    cert_file_path - Path to the certificate file, or "-" to read from stdin
    cert_name      - Optional friendly name for the certificate (for logging)

Example:
    check_cert_expiration.py /path/to/cert.pem "UMB Certificate"
    cat /path/to/cert.pem | check_cert_expiration.py - "UMB Certificate"

Behavior:
- Reads the certificate from a file or stdin
- Extracts and logs the expiration date (notAfter) in human-readable format
- Checks if the certificate has expired
- Exits with 0 if the certificate is valid (not expired)
- Exits with 1 if the certificate is expired or if there's an error

Output:
- Logs certificate expiration information to stderr
- Sets exit code based on certificate validity
"""

import argparse
import sys
from datetime import datetime, timezone
from typing import Optional

from cryptography import x509
from cryptography.hazmat.backends import default_backend


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    :return: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Check if a certificate is expired and log its expiration date",
        epilog="Exit codes: 0 = valid certificate, 1 = expired/error",
    )
    parser.add_argument(
        "cert_source",
        help='Path to certificate file, or "-" to read from stdin',
    )
    parser.add_argument(
        "cert_name",
        nargs="?",
        default="Certificate",
        help="Optional friendly name for the certificate (for logging)",
    )
    return parser.parse_args()


def read_certificate_content(cert_source: str) -> bytes:
    """Read certificate content from file or stdin.

    Args:
        cert_source: Path to certificate file or "-" for stdin

    :return: Certificate content as bytes

    Raises:
        SystemExit: If file not found or content is empty
    """
    try:
        if cert_source == "-":
            content = sys.stdin.buffer.read()
        else:
            with open(cert_source, "rb") as f:
                content = f.read()

        if not content:
            print("Error: No certificate content provided", file=sys.stderr)
            sys.exit(1)

        return content

    except FileNotFoundError:
        print(f"Error: Certificate file not found: {cert_source}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading certificate: {e}", file=sys.stderr)
        sys.exit(1)


def parse_certificate(cert_content: bytes) -> x509.Certificate:
    """Parse certificate content into an x509.Certificate object.

    Args:
        cert_content: Raw certificate content as bytes

    :return: Parsed x509.Certificate object

    Raises:
        ValueError: If certificate parsing fails
    """
    try:
        cert = x509.load_pem_x509_certificate(cert_content, default_backend())
        return cert
    except Exception:
        try:
            cert = x509.load_der_x509_certificate(cert_content, default_backend())
            return cert
        except Exception as e:
            raise ValueError(
                f"Failed to parse certificate. Ensure it's a valid PEM or DER format: {e}"
            ) from e


def check_certificate_file(
    cert_path: str, cert_name: Optional[str] = None, log_to_stderr: bool = True
) -> bool:
    """Check if a certificate file is valid and not expired.

    This is a convenience function for programmatic use in Python code.
    It reads, parses, and validates a certificate file in one call.

    Args:
        cert_path: Path to the certificate file
        cert_name: Optional friendly name for logging (defaults to filename)
        log_to_stderr: Whether to log status to stderr (default: True)

    :return: True if certificate is valid, False if expired or invalid

    Raises:
        FileNotFoundError: If certificate file doesn't exist
        ValueError: If certificate cannot be parsed

    Example:
        >>> if not check_certificate_file("/path/to/cert.pem", "My Cert"):
        ...     raise Exception("Certificate is expired!")
    """
    import os

    if cert_name is None:
        cert_name = os.path.basename(cert_path)

    try:
        with open(cert_path, "rb") as f:
            cert_content = f.read()

        if not cert_content:
            raise ValueError("Certificate file is empty")

        cert = parse_certificate(cert_content)
        is_valid, status_message = check_certificate_expiration(cert, cert_name)

        if log_to_stderr:
            print(status_message, file=sys.stderr)

        return is_valid

    except FileNotFoundError:
        raise
    except Exception as e:
        if log_to_stderr:
            print(f"Error checking certificate {cert_path}: {e}", file=sys.stderr)
        raise ValueError(f"Failed to check certificate: {e}") from e


def check_certificate_expiration(cert: x509.Certificate, cert_name: str) -> tuple[bool, str]:
    """Check if a certificate is expired and generate status message.

    Args:
        cert: The x509.Certificate to check
        cert_name: Friendly name for logging

    :return: Tuple of (is_valid, status_message)
    """
    now = datetime.now(timezone.utc)
    not_before = cert.not_valid_before_utc
    not_after = cert.not_valid_after_utc

    time_diff = not_after - now
    days_diff = time_diff.days
    seconds_diff = int(time_diff.total_seconds())

    expiration_str = not_after.strftime("%b %d %H:%M:%S %Y %Z")

    # Build status message
    lines = []
    lines.append("=" * 60)
    lines.append(f"Checking certificate: {cert_name}")
    lines.append("=" * 60)
    lines.append(f"Subject: {cert.subject.rfc4514_string()}")
    lines.append(f"Issuer: {cert.issuer.rfc4514_string()}")
    lines.append(f"Valid from: {not_before.strftime('%b %d %H:%M:%S %Y %Z')}")
    lines.append(f"Expiration date: {expiration_str}")

    # Check certificate validity
    is_valid = True

    if now < not_before:
        lines.append("Status: NOT YET VALID")
        valid_from = not_before.strftime("%Y-%m-%d %H:%M:%S %Z")
        lines.append(f"ERROR: Certificate is not yet valid! Valid from {valid_from}")
        is_valid = False
    elif seconds_diff < 0:
        days_expired = abs(days_diff)
        lines.append("Status: EXPIRED")
        lines.append(f"ERROR: Certificate expired {days_expired} day(s) ago!")
        is_valid = False
    elif days_diff == 0:
        hours_remaining = seconds_diff // 3600
        lines.append("Status: EXPIRES TODAY")
        lines.append(
            f"WARNING: Certificate expires in approximately {hours_remaining} hour(s)!"
        )
    elif days_diff <= 7:
        lines.append("Status: EXPIRES SOON")
        lines.append(f"WARNING: Certificate expires in {days_diff} day(s)!")
    elif days_diff <= 30:
        lines.append(f"Status: Valid (expires in {days_diff} days)")
        lines.append("NOTE: Certificate expires in less than a month")
    else:
        lines.append(f"Status: Valid (expires in {days_diff} days)")

    lines.append("=" * 60)

    return is_valid, "\n".join(lines)


def main() -> None:
    """Main function."""
    args = parse_arguments()

    try:
        cert_content = read_certificate_content(args.cert_source)
        cert = parse_certificate(cert_content)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    is_valid, status_message = check_certificate_expiration(cert, args.cert_name)

    print(status_message, file=sys.stderr)

    sys.exit(0 if is_valid else 1)


if __name__ == "__main__":
    main()
