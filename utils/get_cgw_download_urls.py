#!/usr/bin/env python3
"""
This script returns all of the download URLs for a given product and version combination
from the Content Gateway (CGW). These URLs are required for advisory generation for
artifacts of generic content types.

A Tekton task in the release-service-catalog calls this script after artifacts
have been signed, pushed to the CDN, and published to the Developer Portal. The task uses
the output of this script to populate release notes with proper PURLs before advisory
creation.
"""

import os
import argparse
import requests
import time
from requests.auth import HTTPBasicAuth


def get_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def call_cgw_api(host, endpoint, session, retries=5, delay=1):
    url = f"{host.rstrip('/')}{endpoint}"
    for attempt in range(retries):
        try:
            response = session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise RuntimeError(f"API call failed after {retries} attempts: {e}")


def get_product_id(host, session, product):
    products = call_cgw_api(host, "/products", session)
    for p in products:
        if p.get("productCode") == product or p.get("name") == product:
            return p["id"]
    raise ValueError(f"Product '{product}' not found")


def get_version_id(host, session, product_id, version):
    versions = call_cgw_api(host, f"/products/{product_id}/versions", session)
    for v in versions:
        if v.get("versionName") == version:
            return v["id"]
    raise ValueError(f"Version '{version}' not found for product ID {product_id}")


def list_download_urls(host, session, product_id, version_id):
    files = call_cgw_api(host, f"/products/{product_id}/versions/{version_id}/files", session)
    for f in files:
        print(f["downloadURL"])


def main():
    parser = argparse.ArgumentParser(description="Get download URLs from CGW")
    parser.add_argument("--product", required=True, help="Product code or name")
    parser.add_argument("--version", required=True, help="Product version")
    args = parser.parse_args()

    host = get_env("CGW_HOST")
    username = get_env("CGW_USERNAME")
    token = get_env("CGW_TOKEN")

    session = requests.Session()
    session.auth = HTTPBasicAuth(username, token)
    session.headers.update({"Accept": "application/json"})

    product_id = get_product_id(host, session, args.product)
    version_id = get_version_id(host, session, product_id, args.version)
    list_download_urls(host, session, product_id, version_id)


if __name__ == "__main__":
    main()
