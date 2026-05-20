"""Test utilities for generating various X.509 certificates and OCSP responses.

This module provides helper functions and pytest fixtures to create self-signed root CAs,
intermediate CAs, leaf certificates (including expired or revoked ones), and corresponding
OCSP responses for testing certificate validation logic.
"""

import datetime
import importlib
import tempfile
import os

from cryptography import x509
from cryptography.x509.oid import (
    NameOID,
    AuthorityInformationAccessOID,
)
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509 import ocsp

from unittest.mock import patch, Mock

import pytest

cert_check = importlib.import_module("check-cert")  # replace with your actual module name


def generate_key():
    """Generate an RSA private key.

    This function generates an RSA private key with a key
    size of 2048 bits and a public exponent of 65537.

    Returns:
        cryptography.hazmat.primitives.asymmetric.rsa.RSAPrivateKey:
        The generated RSA private key.

    """
    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )


def build_name(common_name):
    """Build an X.509 Name object.

    This function constructs an X.509 Name object using the provided common name.

    Args:
        common_name (str): The common name to be used in the X.509 Name.

    Returns:
        cryptography.x509.Name: The constructed X.509 Name object.

    """
    return x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ]
    )


def create_root_ca():
    """Generate a self-signed root CA certificate and its corresponding private key.

    This function creates an RSA private key and then uses it to sign a
    self-issued X.509 certificate, establishing it as a root Certificate Authority.
    The certificate is valid for 365 days and includes basic constraints for a CA.

    Returns:
        tuple: A tuple containing:
            - cryptography.hazmat.primitives.asymmetric.rsa.RSAPrivateKey:
              The generated RSA private key for the root CA.
            - cryptography.x509.Certificate: The self-signed X.509 certificate for the root CA.

    """
    key = generate_key()
    subject = build_name("Test Root CA")

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(tz=datetime.timezone.utc))
        .not_valid_after(
            datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=365)
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )

    return key, cert


def create_intermediate_ca(root_key, root_cert):
    """Generate an intermediate CA certificate and its corresponding private key.

    This function creates an RSA private key for an intermediate Certificate Authority
    and then issues an X.509 certificate for it, signed by the provided root CA.
    The certificate is valid for 365 days and includes basic constraints for a CA
    .

    Returns:
        tuple: A tuple containing:
            - cryptography.hazmat.primitives.asymmetric.rsa.RSAPrivateKey:
              The generated RSA private key for the intermediate CA.
            - cryptography.x509.Certificate: The X.509 certificate for the intermediate
              CA, signed by the root CA.

    """
    key = generate_key()
    subject = build_name("Test Intermediate CA")

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(root_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(tz=datetime.timezone.utc))
        .not_valid_after(
            datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=365)
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(root_key.public_key()),
            critical=False,
        )
        .sign(root_key, hashes.SHA256())
    )

    return key, cert


def create_leaf_cert(intermediate_key, intermediate_cert, expired=False, ocsp_enabled=True):
    """Generate a leaf (end-entity) certificate and its corresponding private key.

    This function creates an RSA private key for a leaf certificate and then
    issues an X.509 certificate for it, signed by the provided intermediate CA.
    The certificate's validity period can be set to be expired. It also
    includes basic constraints for an end-entity certificate and can optionally
    include an Authority Information Access (AIA) extension for OCSP.

    Args:
        intermediate_key (cryptography.hazmat.primitives.asymmetric.rsa.RSAPrivateKey):
            The private key of the intermediate CA.
        intermediate_cert (cryptography.x509.Certificate):
            The certificate of the intermediate CA.
        expired (bool, optional): If True, the certificate will be generated as expired.
            Defaults to False.
        ocsp_enabled (bool, optional): If True, an OCSP AIA extension will be added.
            Defaults to True.

    Returns:
        tuple: A tuple containing:
            - cryptography.hazmat.primitives.asymmetric.rsa.RSAPrivateKey:
            The generated RSA private key for the leaf certificate.
            - cryptography.x509.Certificate: The X.509 certificate for the leaf, signed by
            the intermediate CA.

    """
    key = generate_key()
    subject = build_name("www.example.test")
    ocsp_url = "http://ocsp.test.local"

    if not expired:
        not_valid_after = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(
            days=10
        )
        not_valid_before = datetime.datetime.now(tz=datetime.timezone.utc)
    else:
        not_valid_after = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(
            days=10
        )
        not_valid_before = datetime.datetime.now(
            tz=datetime.timezone.utc
        ) - datetime.timedelta(days=20)

    cert_builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(intermediate_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_valid_before)
        .not_valid_after(not_valid_after)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(intermediate_key.public_key()),
            critical=False,
        )
    )
    if ocsp_enabled:
        # Authority Information Access
        cert_builder = cert_builder.add_extension(
            x509.AuthorityInformationAccess(
                [
                    x509.AccessDescription(
                        AuthorityInformationAccessOID.OCSP,
                        x509.UniformResourceIdentifier(ocsp_url),
                    ),
                    # Optional: CA Issuers URL
                    x509.AccessDescription(
                        AuthorityInformationAccessOID.CA_ISSUERS,
                        x509.UniformResourceIdentifier(
                            "http://ca.test.local/intermediate.crt"
                        ),
                    ),
                ]
            ),
            critical=False,
        )
    cert = cert_builder.sign(intermediate_key, hashes.SHA256())

    return key, cert


def build_ocsp_response(issuer_cert, issuer_key, leaf_cert, status):
    """Build an OCSP request for a given leaf certificate.

    This function constructs an OCSP request for the provided leaf certificate,
    using its issuer's certificate to identify the certificate to be checked.

    Args:
        leaf_cert (cryptography.x509.Certificate):
            The leaf certificate for which to build the OCSP request.
        issuer_cert (cryptography.x509.Certificate):
            The issuer certificate of the leaf certificate.
        issuer_key ((cryptography.hazmat.primitives.asymmetric.rsa.RSAPrivateKey):
            The private key of the OCSP responder.
        status (cryptography.x509.ocsp.OCSPCertStatus):
            The revocation status of the leaf certificate.

    Returns:
        bytes: The DER-encoded OCSP request.

    """
    builder = ocsp.OCSPResponseBuilder()

    builder = builder.add_response(
        cert=leaf_cert,
        issuer=issuer_cert,
        algorithm=hashes.SHA1(),
        cert_status=status,
        this_update=datetime.datetime.now(tz=datetime.timezone.utc),
        next_update=datetime.datetime.now(tz=datetime.timezone.utc)
        + datetime.timedelta(days=1),
        revocation_time=(
            datetime.datetime.now(tz=datetime.timezone.utc)
            if status == ocsp.OCSPCertStatus.REVOKED
            else None
        ),
        revocation_reason=(
            x509.ReasonFlags.key_compromise if status == ocsp.OCSPCertStatus.REVOKED else None
        ),
    )

    builder = builder.responder_id(ocsp.OCSPResponderEncoding.NAME, issuer_cert)

    response = builder.sign(private_key=issuer_key, algorithm=hashes.SHA256())

    return response.public_bytes(Encoding.DER)


def _save_certs(cert, key, inter_cert):
    """Save certificates and key to temporary files.

    This function takes a leaf certificate, its private key, and an intermediate
    certificate, and saves each of them into separate temporary files in PEM format.
    The private key is saved without encryption.

    Args:
        cert (cryptography.x509.Certificate): The leaf certificate to save.
        key (cryptography.hazmat.primitives.asymmetric.rsa.RSAPrivateKey):
            The private key for the leaf certificate
        inter_cert (cryptography.x509.Certificate):
            The intermediate certificate to save.

    Returns:
        tuple: A tuple containing the file paths of the saved leaf certificate,
        private key, and intermediate certificate.

    """
    with tempfile.NamedTemporaryFile(delete=False) as cert_file:
        cert_file.write(cert.public_bytes(Encoding.PEM))
    with tempfile.NamedTemporaryFile(delete=False) as key_file:
        key_file.write(
            key.private_bytes(
                encoding=Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    with tempfile.NamedTemporaryFile(delete=False) as inter_cert_file:
        inter_cert_file.write(inter_cert.public_bytes(Encoding.PEM))

    return cert_file.name, key_file.name, inter_cert_file.name


@pytest.fixture
def generate_revoked_cert():
    """Generate a set of temporary certificate files and a revoked OCSP response.

    This fixture creates a root CA, an intermediate CA, and a leaf certificate.
    It then generates an OCSP response indicating that the leaf certificate is
    REVOKED. All certificates and the private key are saved to temporary files.

    Yields:
        tuple: A tuple containing:
            - str: Path to the temporary leaf certificate file.
            - str: Path to the temporary leaf private key file.
            - str: Path to the temporary intermediate certificate file.
            - bytes: The DER-encoded OCSP response for the revoked certificate.

    """
    root_key, root_cert = create_root_ca()
    inter_key, inter_cert = create_intermediate_ca(root_key, root_cert)
    leaf_key, leaf_cert = create_leaf_cert(inter_key, inter_cert)
    ocsp_bytes = build_ocsp_response(
        inter_cert,
        inter_key,
        leaf_cert,
        ocsp.OCSPCertStatus.REVOKED,
    )
    cert_file_path, key_file_path, inter_cert_file_path = _save_certs(
        leaf_cert, leaf_key, inter_cert
    )
    yield (cert_file_path, key_file_path, inter_cert_file_path, ocsp_bytes)

    os.remove(cert_file_path)
    os.remove(key_file_path)
    os.remove(inter_cert_file_path)


@pytest.fixture
def generate_ok_cert():
    """Generate a set of temporary certificate files and an "OK" OCSP response.

    This fixture creates a root CA, an intermediate CA, and a leaf certificate.
    It then generates an OCSP response indicating that the leaf certificate is
    GOOD. All certificates and the private key are saved to temporary files.

    Yields:
        tuple: A tuple containing:
            - str: Path to the temporary leaf certificate file.
            - str: Path to the temporary leaf private key file.
            - str: Path to the temporary intermediate certificate file.
            - bytes: The DER-encoded OCSP response for the good certificate.

    """
    root_key, root_cert = create_root_ca()
    inter_key, inter_cert = create_intermediate_ca(root_key, root_cert)
    leaf_key, leaf_cert = create_leaf_cert(inter_key, inter_cert, expired=False)
    ocsp_bytes = build_ocsp_response(
        inter_cert,
        inter_key,
        leaf_cert,
        ocsp.OCSPCertStatus.GOOD,
    )

    cert_file_path, key_file_path, inter_cert_file_path = _save_certs(
        leaf_cert, leaf_key, inter_cert
    )
    yield (cert_file_path, key_file_path, inter_cert_file_path, ocsp_bytes)

    os.remove(cert_file_path)
    os.remove(key_file_path)
    os.remove(inter_cert_file_path)


@pytest.fixture
def generate_expired_cert():
    """Generate a set of temporary certificate files and an "OK" OCSP response.

    for an expired certificate

    This fixture creates a root CA, an intermediate CA, and a leaf certificate
    that is intentionally generated as expired. It then generates an OCSP
    response indicating that the leaf certificate is GOOD (despite being expired).
    All certificates and the private key are saved to temporary files.

    Yields:
        tuple: A tuple containing:
            - str: Path to the temporary leaf certificate file.
            - str: Path to the temporary leaf private key file.
            - str: Path to the temporary intermediate certificate file.
            - bytes: The DER-encoded OCSP response for the good certificate
            (despite being expired).

    """
    root_key, root_cert = create_root_ca()
    inter_key, inter_cert = create_intermediate_ca(root_key, root_cert)
    leaf_key, leaf_cert = create_leaf_cert(inter_key, inter_cert, expired=True)
    ocsp_bytes = build_ocsp_response(
        inter_cert,
        inter_key,
        leaf_cert,
        ocsp.OCSPCertStatus.GOOD,
    )

    cert_file_path, key_file_path, inter_cert_file_path = _save_certs(
        leaf_cert, leaf_key, inter_cert
    )
    yield (cert_file_path, key_file_path, inter_cert_file_path, ocsp_bytes)

    os.remove(cert_file_path)
    os.remove(key_file_path)
    os.remove(inter_cert_file_path)


@pytest.fixture
def generate_no_ocsp_cert():
    """Generate a set of temporary certificate files for a leaf certificate.

    without OCSP AIA

    This fixture creates a root CA, an intermediate CA, and a leaf certificate
    that is intentionally generated *without* an Authority Information Access (AIA)
    extension for OCSP. It still generates an "OK" OCSP response for the
    good certificate.

    Yields:
        tuple: A tuple containing:
            - str: Path to the temporary leaf certificate file.
            - str: Path to the temporary leaf private key file.
            - str: Path to the temporary intermediate certificate file.
            - bytes: The DER-encoded OCSP response for the good certificate.

    """
    root_key, root_cert = create_root_ca()
    inter_key, inter_cert = create_intermediate_ca(root_key, root_cert)
    leaf_key, leaf_cert = create_leaf_cert(
        inter_key, inter_cert, expired=False, ocsp_enabled=False
    )
    ocsp_bytes = build_ocsp_response(
        inter_cert,
        inter_key,
        leaf_cert,
        ocsp.OCSPCertStatus.GOOD,
    )

    cert_file_path, key_file_path, inter_cert_file_path = _save_certs(
        leaf_cert, leaf_key, inter_cert
    )
    yield (cert_file_path, key_file_path, inter_cert_file_path, ocsp_bytes)

    os.remove(cert_file_path)
    os.remove(key_file_path)
    os.remove(inter_cert_file_path)


@patch("requests.post")
def test_revoked_certificate(mock_post, generate_revoked_cert):
    """Test the scenario where a certificate is revoked."""
    cert_file_path, key_file_path, inter_cert_file_path, ocsp_bytes = generate_revoked_cert

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = ocsp_bytes
    mock_post.return_value = mock_response

    details, ok = cert_check.cert_info(
        cert_file_path,
        key_file_path,
        inter_cert_file_path,
    )
    print(details)
    assert details["cert_key_match"] is True
    assert details["expired"] is False
    assert details["cert_ocsp_details"]["validation_status"] == "OCSPResponseStatus.SUCCESSFUL"
    assert details["cert_ocsp_details"]["cert_status"] == "OCSPCertStatus.REVOKED"
    assert ok is False


@patch("requests.post")
def test_expired_certificate(mock_post, generate_expired_cert):
    """Test the scenario where a certificate is expired."""
    cert_file_path, key_file_path, inter_cert_file_path, ocsp_bytes = generate_expired_cert

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = ocsp_bytes
    mock_post.return_value = mock_response

    details, ok = cert_check.cert_info(
        cert_file_path,
        key_file_path,
        inter_cert_file_path,
    )
    print(details)
    assert details["cert_key_match"] is True
    assert details["expired"] is True
    assert details["cert_ocsp_details"]["validation_status"] == "OCSPResponseStatus.SUCCESSFUL"
    assert details["cert_ocsp_details"]["cert_status"] == "OCSPCertStatus.GOOD"
    assert ok is False


@patch("requests.post")
def test_ok(mock_post, generate_ok_cert):
    """Test the scenario where a certificate is valid and not expired."""
    cert_file_path, key_file_path, inter_cert_file_path, ocsp_bytes = generate_ok_cert

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = ocsp_bytes
    mock_post.return_value = mock_response

    details, ok = cert_check.cert_info(
        cert_file_path,
        key_file_path,
        inter_cert_file_path,
    )
    print(details)
    assert details["cert_key_match"] is True
    assert details["expired"] is False
    assert details["cert_ocsp_details"]["validation_status"] == "OCSPResponseStatus.SUCCESSFUL"
    assert details["cert_ocsp_details"]["cert_status"] == "OCSPCertStatus.GOOD"
    assert ok is True


@patch("requests.post")
def test_no_ocsp_available(mock_post, generate_no_ocsp_cert):
    """Test the scenario where a certificate does not have."""
    cert_file_path, key_file_path, inter_cert_file_path, ocsp_bytes = generate_no_ocsp_cert

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = ocsp_bytes
    mock_post.return_value = mock_response

    details, ok = cert_check.cert_info(
        cert_file_path,
        key_file_path,
        inter_cert_file_path,
    )
    print(details)
    assert details["cert_key_match"] is True
    assert details["expired"] is False
    assert details["cert_ocsp_details"] == {}
    assert ok is True
