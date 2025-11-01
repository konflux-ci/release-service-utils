#!/usr/bin/env python3
"""
This script finds a matching PURL (Package URL) from JSON input data based on a
target repository URL. It reads JSON data from stdin, searches for items with a PURL
that has a matching repository_url qualifier, and prints the impact value of the
first matching item.

The input JSON should be a list of objects, each containing at least:
- 'purl': A Package URL string
- 'impact': The value to print when a match is found

Example usage:
    echo '[{"purl": "pkg:generic/test@1.0.0?repository_url=repo1", "impact": "high"}]' | \\
        python3 find_matching_purl.py repo1
"""

import json
import sys

from packageurl import PackageURL


def find_matching_purl(data, target_repo):
    """Find the first item in data with a PURL matching the target repository URL.

    Args:
        data: List of items containing 'purl' and 'impact' keys
        target_repo: The repository URL to match against

    :return: The impact value of the first matching item, or None if no match found
    """
    for item in data:
        try:
            purl = item["purl"]
            pkg = PackageURL.from_string(purl)
            repo_url = pkg.to_dict().get("qualifiers", {}).get("repository_url", "")
            if repo_url == target_repo:
                return item["impact"]
        except (KeyError, ValueError, AttributeError):
            continue
    return None


if __name__ == "__main__":
    data = json.load(sys.stdin)
    target_repo = sys.argv[1]
    impact = find_matching_purl(data, target_repo)
    if impact is not None:
        print(impact)
