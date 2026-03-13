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


def cert_info(cert_path, cert_key_path=None, issuer_path=None, ocsp_timeout=10):
    try:
        # 1. Load and Validate Certificate
        cert = load_cert(cert_path)

        cert_key_match = None
        cert_status_details = {}
        expired = cert.not_valid_after_utc < datetime.datetime.now(tz=datetime.timezone.utc)
        already_valid = cert.not_valid_before_utc < datetime.datetime.now(
            tz=datetime.timezone.utc
        )

        # 2. Load and Validate Private Key
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

        if issuer_path:
            # if issuer provided, we can also check OCSP status (revocation)
            issuer_cert = load_cert(issuer_path)

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

                response = requests.post(
                    ocsp_url, data=ocsp_request_bytes, headers=headers, timeout=ocsp_timeout
                )

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
            else:
                print(
                    "No OCSP URL found in certificate. Cannot check revocation",
                    file=sys.stderr,
                )

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
    parser = argparse.ArgumentParser(
        description="""Certificate Checker
This script checks the validity of a certificate by performing the following checks:
1. Expiration Check: Verifies if the certificate is currently valid based on its
   'not valid before' and 'not valid after' dates.
   This check is performed every time the script is run.
2. Certificate-Key Match: If a private key is provided, the script checks
   if it matches the public key in the certificate.
3. OCSP Revocation Check: If an issuer certificate is provided, the script performs
   an OCSP check to determine if the certificate has been revoked.

Script produce following json to stdout and return 0 if all checks are successful,
otherwise return 1

{'expired': <boolean>,
 'cert_key_match': <boolean>,
 'serial_number': <serial_number>,
 'issuer': <issuer>,
 'subject': <subject>,
 'not_valid_before': <YYYY-MM-DDThh:mm:ss+tz:tz>,
 'not_valid_after': <YYYY-MM-DDThh:mm:ss+tz:tz>,
 'cert_ocsp_details': {'validation_status': 'OCSPResponseStatus.<STATUS>',
                       'cert_status': 'OCSPCertStatus.<STATUS>',
                       'this_update': '<YYYY-MM-DDThh:mm:ss+tz:tz>',
                       'next_update': <YYYY-MM-DDThh:mm:ss+tz:tz> or null,
                       'revocation_time': <YYYY-MM-DDThh:mm:ss+tz:tz> or null,
                       'revocation_reason': <reason>}}
"""
    )
    parser.add_argument(
        "--cert", required=True, help="Path to the certificate file (PEM format)"
    )
    parser.add_argument("--key", help="Path to the private key file (PEM format)")
    parser.add_argument("--issuer", help="Path to the issuer certificate file (PEM format)")
    parser.add_argument(
        "--ocsp-timeout",
        type=int,
        default=10,
        help="Timeout for OCSP request in seconds (default: 10)",
    )
    return parser


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()
    cert_info_result, is_valid = cert_info(
        args.cert,
        cert_key_path=args.key,
        issuer_path=args.issuer,
        ocsp_timeout=args.ocsp_timeout,
    )
    print(cert_info_result)
    if is_valid:
        print("Certification check succesful", file=sys.stderr)
    else:
        print("Certification check failed", file=sys.stderr)
        sys.exit(1)
