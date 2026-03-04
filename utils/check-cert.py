#!/usr/bin/env python3

import argparse
import datetime
import requests
import sys
import traceback

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.x509 import ocsp


def load_cert(path):
    with open(path, "rb") as f:
        return x509.load_pem_x509_certificate(f.read(), default_backend())


def cert_info(cert_path, cert_key_path=None, issuer_path=None):
    try:
        # 1. Load and Validate Certificate
        cert = load_cert(cert_path)
        # 2. Load and Validate Private Key

        cert_key_match = None
        cert_status_details = {}
        expired = cert.not_valid_after_utc < datetime.datetime.now(tz=datetime.timezone.utc)
        already_valid = cert.not_valid_before_utc < datetime.datetime.now(
            tz=datetime.timezone.utc
        )

        if cert_key_path:
            with open(cert_key_path, "rb") as f:
                key_data = f.read()
                # If your key has a password, provide it in 'password='
                private_key = serialization.load_pem_private_key(key_data, password=None)
            # 3. Check if they match
            # We compare the public key derived from the cert vs the one from the private key
            cert_pub_key = cert.public_key().public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            key_pub_key = private_key.public_key().public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            if cert_pub_key == key_pub_key:
                cert_key_match = True
            else:
                cert_key_match = False

        if issuer_path:  # and ca_path:
            # if issuer and CA was provided, we can also check OCSP status (revocation)

            with open(issuer_path, "rb") as f:
                issuer_cert = load_cert(issuer_path)
            # with open(ca_path, "rb") as f:
            #    ca_cert = load_cert(ca_path)

            builder = ocsp.OCSPRequestBuilder()
            builder = builder.add_certificate(cert, issuer_cert, hashes.SHA1())
            ocsp_request = builder.build()

            # Extract OCSP URL from the certificate
            try:
                ocsp_urls = cert.extensions.get_extension_for_class(
                    x509.AuthorityInformationAccess
                ).value
            except x509.extensions.ExtensionNotFound:
                ocsp_urls = []
            if ocsp_urls:
                ocsp_url = [
                    access.access_location.value
                    for access in ocsp_urls
                    if access.access_method == x509.AuthorityInformationAccessOID.OCSP
                ][0]

                # Encode request in DER format
                ocsp_request_bytes = ocsp_request.public_bytes(serialization.Encoding.DER)

                headers = {
                    "Content-Type": "application/ocsp-request",
                    "Accept": "application/ocsp-response",
                }

                response = requests.post(ocsp_url, data=ocsp_request_bytes, headers=headers)

                if response.status_code != 200:
                    print(
                        f"OCSP request failed with status code {response.status_code}",
                        file=sys.stderr,
                    )
                else:
                    # === Parse OCSP Response ===
                    ocsp_response = ocsp.load_der_ocsp_response(response.content)

                    cert_status_details["validation_status"] = str(
                        ocsp_response.response_status
                    )
                    cert_status_details["cert_status"] = str(ocsp_response.certificate_status)
                    cert_status_details["this_update"] = (
                        ocsp_response.this_update_utc.isoformat()
                        if ocsp_response.this_update_utc
                        else None
                    )
                    cert_status_details["next_update"] = (
                        ocsp_response.next_update_utc.isoformat()
                        if ocsp_response.next_update_utc
                        else None
                    )
                    cert_status_details["revocation_time"] = (
                        ocsp_response.revocation_time_utc.isoformat()
                        if ocsp_response.revocation_time_utc
                        else None
                    )
                    cert_status_details["revocation_reason"] = ocsp_response.revocation_reason

        return (
            {
                "expired": expired,
                "cert_key_match": cert_key_match,
                "serial_number": cert.serial_number,
                "issuer": cert.issuer.rfc4514_string(),
                "subject": cert.subject.rfc4514_string(),
                "not_valid_before": cert.not_valid_before_utc.isoformat(),
                "not_valid_after": cert.not_valid_after_utc.isoformat(),
                "cert_ocsp_details": cert_status_details,
            },
            (
                not expired
                and already_valid
                and cert_key_match is not False
                and cert_status_details.get("cert_status") != "OCSPCertStatus.REVOKED"
            ),
        )

    except Exception:
        traceback.print_exc()
        return {}, False


def make_parser():
    parser = argparse.ArgumentParser(description="Certificate Checker")
    parser.add_argument(
        "--cert", required=True, help="Path to the certificate file (PEM format)"
    )
    parser.add_argument("--key", help="Path to the private key file (PEM format)")
    parser.add_argument("--issuer", help="Path to the issuer certificate file (PEM format)")
    parser.add_argument("--ca", help="Path to the CA certificate file (PEM format)")
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()
    cert_info_result, is_valid = cert_info(
        args.cert, cert_key_path=args.key, issuer_path=args.issuer
    )
    print(cert_info_result)
    if is_valid:
        print("Certification check succesfull", file=sys.stderr)
    else:
        print("Certification check failed", file=sys.stderr)
        sys.exit(1)
