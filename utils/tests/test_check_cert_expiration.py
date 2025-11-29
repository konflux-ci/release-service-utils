"""Tests for check_cert_expiration utility."""

import sys
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

sys.path.insert(0, "utils")

from check_cert_expiration import (  # noqa: E402
    check_certificate_expiration,
    check_certificate_file,
    main,
    parse_arguments,
    parse_certificate,
    read_certificate_content,
)


def generate_test_certificate(days_valid: int = 365, not_before_offset: int = 0) -> bytes:
    """Generate a test X.509 certificate.

    Args:
        days_valid: Number of days the certificate should be valid from not_before date
        not_before_offset: Days offset for not_before (negative = past, positive = future)

    :return: PEM-encoded certificate as bytes
    """
    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )

    # Build certificate subject and issuer
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Test State"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "Test City"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Test Org"),
            x509.NameAttribute(NameOID.COMMON_NAME, "test.example.com"),
        ]
    )

    # Set validity dates
    not_before = datetime.now(timezone.utc) + timedelta(days=not_before_offset)
    not_after = not_before + timedelta(days=days_valid)

    # Build certificate
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .sign(private_key, hashes.SHA256(), default_backend())
    )

    # Return PEM-encoded certificate
    return cert.public_bytes(serialization.Encoding.PEM)


class TestParseArguments:
    """Test argument parsing."""

    @patch(
        "argparse._sys.argv",
        ["check_cert_expiration.py", "/path/to/cert.pem", "Test Cert"],
    )
    def test_parse_arguments_with_name(self):
        """Test parsing arguments with certificate name."""
        args = parse_arguments()
        assert args.cert_source == "/path/to/cert.pem"
        assert args.cert_name == "Test Cert"

    @patch("argparse._sys.argv", ["check_cert_expiration.py", "/path/to/cert.pem"])
    def test_parse_arguments_without_name(self):
        """Test parsing arguments without certificate name."""
        args = parse_arguments()
        assert args.cert_source == "/path/to/cert.pem"
        assert args.cert_name == "Certificate"

    @patch("argparse._sys.argv", ["check_cert_expiration.py", "-", "Stdin Cert"])
    def test_parse_arguments_stdin(self):
        """Test parsing arguments for stdin input."""
        args = parse_arguments()
        assert args.cert_source == "-"
        assert args.cert_name == "Stdin Cert"

    @patch("argparse._sys.argv", ["check_cert_expiration.py"])
    def test_parse_arguments_missing_required(self):
        """Test that missing required argument raises SystemExit."""
        with pytest.raises(SystemExit):
            parse_arguments()


class TestReadCertificateContent:
    """Test reading certificate content."""

    def test_read_from_file(self):
        """Test reading certificate from a file."""
        cert_data = generate_test_certificate()
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(cert_data)
            temp_path = f.name

        try:
            content = read_certificate_content(temp_path)
            assert content == cert_data
        finally:
            import os

            os.unlink(temp_path)

    def test_read_from_stdin(self):
        """Test reading certificate from stdin."""
        cert_data = generate_test_certificate()
        with patch("sys.stdin.buffer.read", return_value=cert_data):
            content = read_certificate_content("-")
            assert content == cert_data

    def test_read_nonexistent_file(self):
        """Test reading from non-existent file raises SystemExit."""
        with pytest.raises(SystemExit) as exc_info:
            read_certificate_content("/nonexistent/path/to/cert.pem")
        assert exc_info.value.code == 1

    def test_read_empty_content_from_stdin(self):
        """Test reading empty content from stdin raises SystemExit."""
        with patch("sys.stdin.buffer.read", return_value=b""):
            with pytest.raises(SystemExit) as exc_info:
                read_certificate_content("-")
            assert exc_info.value.code == 1

    def test_read_empty_file(self):
        """Test reading empty file raises SystemExit."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            temp_path = f.name

        try:
            with pytest.raises(SystemExit) as exc_info:
                read_certificate_content(temp_path)
            assert exc_info.value.code == 1
        finally:
            import os

            os.unlink(temp_path)


class TestParseCertificate:
    """Test certificate parsing."""

    def test_parse_valid_pem_certificate(self):
        """Test parsing a valid PEM certificate."""
        cert_data = generate_test_certificate()
        cert = parse_certificate(cert_data)
        assert isinstance(cert, x509.Certificate)
        assert cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value == (
            "test.example.com"
        )

    def test_parse_valid_der_certificate(self):
        """Test parsing a valid DER certificate."""
        # Generate PEM certificate first
        pem_cert_data = generate_test_certificate()
        pem_cert = x509.load_pem_x509_certificate(pem_cert_data, default_backend())

        # Convert to DER
        der_cert_data = pem_cert.public_bytes(serialization.Encoding.DER)

        # Parse DER certificate
        cert = parse_certificate(der_cert_data)
        assert isinstance(cert, x509.Certificate)

    def test_parse_invalid_certificate(self):
        """Test parsing invalid certificate data raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            parse_certificate(b"This is not a valid certificate")
        assert "Failed to parse certificate" in str(exc_info.value)


class TestCheckCertificateExpiration:
    """Test certificate expiration checking."""

    def test_valid_certificate_long_expiry(self):
        """Test a valid certificate with long expiry period."""
        cert_data = generate_test_certificate(days_valid=365)
        cert = parse_certificate(cert_data)

        is_valid, message = check_certificate_expiration(cert, "Test Cert")

        assert is_valid is True
        assert "Test Cert" in message
        assert "Status: Valid" in message

    def test_expired_certificate(self):
        """Test an expired certificate."""
        # Create a certificate that expired 10 days ago
        cert_data = generate_test_certificate(days_valid=1, not_before_offset=-11)
        cert = parse_certificate(cert_data)

        is_valid, message = check_certificate_expiration(cert, "Expired Cert")

        assert is_valid is False
        assert "Expired Cert" in message
        assert "Status: EXPIRED" in message
        assert "expired" in message.lower()

    def test_certificate_expiring_soon(self):
        """Test a certificate expiring within a week."""
        # Create a certificate expiring in 3 days
        cert_data = generate_test_certificate(days_valid=3)
        cert = parse_certificate(cert_data)

        is_valid, message = check_certificate_expiration(cert, "Soon Expiry")

        assert is_valid is True
        assert "Soon Expiry" in message
        assert "Status: EXPIRES SOON" in message
        assert "WARNING" in message

    def test_certificate_expiring_today(self):
        """Test a certificate expiring today."""
        # Create a certificate expiring in a few hours
        cert_data = generate_test_certificate(days_valid=0)
        cert = parse_certificate(cert_data)

        is_valid, message = check_certificate_expiration(cert, "Today Expiry")

        assert is_valid is True
        assert "Today Expiry" in message
        # The status could be either "EXPIRES TODAY" or "EXPIRES SOON" depending on timing
        assert "WARNING" in message or "EXPIRES" in message

    def test_certificate_expiring_within_month(self):
        """Test a certificate expiring within a month."""
        cert_data = generate_test_certificate(days_valid=20)
        cert = parse_certificate(cert_data)

        is_valid, message = check_certificate_expiration(cert, "Month Expiry")

        assert is_valid is True
        assert "Month Expiry" in message
        assert "Status: Valid" in message
        assert "NOTE" in message

    def test_certificate_not_yet_valid(self):
        """Test a certificate that is not yet valid."""
        # Create a certificate that becomes valid in 10 days
        cert_data = generate_test_certificate(days_valid=365, not_before_offset=10)
        cert = parse_certificate(cert_data)

        is_valid, message = check_certificate_expiration(cert, "Future Cert")

        assert is_valid is False
        assert "Future Cert" in message
        assert "NOT YET VALID" in message


class TestCheckCertificateFile:
    """Test the convenience function for programmatic use."""

    def test_check_valid_certificate_file(self):
        """Test checking a valid certificate file."""
        cert_data = generate_test_certificate()
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(cert_data)
            temp_path = f.name

        try:
            is_valid = check_certificate_file(temp_path, "Test Cert")
            assert is_valid is True
        finally:
            import os

            os.unlink(temp_path)

    def test_check_expired_certificate_file(self):
        """Test checking an expired certificate file."""
        cert_data = generate_test_certificate(days_valid=1, not_before_offset=-11)
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(cert_data)
            temp_path = f.name

        try:
            is_valid = check_certificate_file(temp_path, "Expired Cert")
            assert is_valid is False
        finally:
            import os

            os.unlink(temp_path)

    def test_check_certificate_file_not_found(self):
        """Test checking a non-existent certificate file."""
        with pytest.raises(FileNotFoundError):
            check_certificate_file("/nonexistent/cert.pem")

    def test_check_certificate_file_invalid_content(self):
        """Test checking a file with invalid certificate content."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(b"invalid certificate content")
            temp_path = f.name

        try:
            with pytest.raises(ValueError):
                check_certificate_file(temp_path, "Invalid Cert")
        finally:
            import os

            os.unlink(temp_path)

    def test_check_certificate_file_silent_mode(self):
        """Test checking a certificate file without stderr logging."""
        cert_data = generate_test_certificate()
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(cert_data)
            temp_path = f.name

        try:
            # Test with log_to_stderr=False
            is_valid = check_certificate_file(temp_path, log_to_stderr=False)
            assert is_valid is True
        finally:
            import os

            os.unlink(temp_path)

    def test_check_certificate_file_default_name(self):
        """Test checking a certificate file with default name (filename)."""
        cert_data = generate_test_certificate()
        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".pem", prefix="test_"
        ) as f:
            f.write(cert_data)
            temp_path = f.name

        try:
            # Test without providing cert_name (should use filename)
            is_valid = check_certificate_file(temp_path)
            assert is_valid is True
        finally:
            import os

            os.unlink(temp_path)


class TestMain:
    """Test main function."""

    def test_main_with_valid_certificate_file(self):
        """Test main function with a valid certificate file."""
        cert_data = generate_test_certificate()
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(cert_data)
            temp_path = f.name

        try:
            with patch("argparse._sys.argv", ["check_cert_expiration.py", temp_path]):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 0
        finally:
            import os

            os.unlink(temp_path)

    def test_main_with_expired_certificate(self):
        """Test main function with an expired certificate."""
        # Create expired certificate
        cert_data = generate_test_certificate(days_valid=1, not_before_offset=-11)
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(cert_data)
            temp_path = f.name

        try:
            with patch("argparse._sys.argv", ["check_cert_expiration.py", temp_path]):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 1
        finally:
            import os

            os.unlink(temp_path)

    def test_main_with_stdin(self):
        """Test main function reading from stdin."""
        cert_data = generate_test_certificate()
        with patch("argparse._sys.argv", ["check_cert_expiration.py", "-"]):
            with patch("sys.stdin.buffer.read", return_value=cert_data):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 0

    def test_main_with_custom_name(self):
        """Test main function with custom certificate name."""
        cert_data = generate_test_certificate()
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(cert_data)
            temp_path = f.name

        try:
            with patch(
                "argparse._sys.argv",
                ["check_cert_expiration.py", temp_path, "My Custom Cert"],
            ):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 0
        finally:
            import os

            os.unlink(temp_path)

    def test_main_with_invalid_certificate(self):
        """Test main function with invalid certificate content."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(b"invalid certificate content")
            temp_path = f.name

        try:
            with patch("argparse._sys.argv", ["check_cert_expiration.py", temp_path]):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 1
        finally:
            import os

            os.unlink(temp_path)

    def test_main_with_nonexistent_file(self):
        """Test main function with non-existent file."""
        with patch(
            "argparse._sys.argv", ["check_cert_expiration.py", "/nonexistent/cert.pem"]
        ):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
