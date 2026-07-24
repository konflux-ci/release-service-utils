#!/usr/bin/env python3
"""Collect Slack notification parameters from Release CRs and the data file."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, NamedTuple

import tekton
from file import load_json_dict
from logger import logger


class ReleaseMetadata(NamedTuple):
    """Subset of release CR fields needed for the Slack message."""

    origin_workspace: str
    target_workspace: str
    release_name: str
    release_pipeline_name: str


def validate_input_files(
    data_file: Path,
    snapshot_file: Path,
    release_file: Path,
) -> None:
    """Raise RuntimeError when any required input file is missing."""
    for label, path in [
        ("data", data_file),
        ("snapshot", snapshot_file),
        ("release", release_file),
    ]:
        if not path.is_file():
            raise RuntimeError(f"No valid {label} file was provided.")


def _get_slack_field(data: dict[str, Any], key: str) -> str | None:
    """Return a string value from data['slack'][key], or None if absent."""
    slack = data.get("slack")
    if isinstance(slack, dict) and key in slack:
        return str(slack[key])
    return None


def extract_slack_secret(data: dict[str, Any]) -> str | None:
    """Return the slack-notification-secret value, or None if absent."""
    return _get_slack_field(data, "slack-notification-secret")


def extract_slack_keyname(data: dict[str, Any]) -> str | None:
    """Return the slack-webhook-notification-secret-keyname, or None."""
    return _get_slack_field(data, "slack-webhook-notification-secret-keyname")


def extract_release_metadata(release: dict[str, Any]) -> ReleaseMetadata:
    """Extract release metadata needed for the Slack message."""
    metadata = release.get("metadata", {})
    status = release.get("status", {})

    origin_namespace = metadata.get("namespace", "")
    target_namespace = status.get("target", "")

    return ReleaseMetadata(
        origin_workspace=origin_namespace.replace("-tenant", ""),
        target_workspace=target_namespace.replace("-tenant", ""),
        release_name=metadata.get("name", ""),
        release_pipeline_name=status.get("managedProcessing", {}).get("pipelineRun", ""),
    )


def build_urls(
    hac_url: str,
    meta: ReleaseMetadata,
    component_group: str,
) -> tuple[str, str]:
    """Build the release detail and pipeline-run URLs."""
    release_url = (
        f"{hac_url}/{meta.origin_workspace}/applications"
        f"/{component_group}/releases/{meta.release_name}"
    )
    release_plr_url = (
        f"{hac_url}/{meta.target_workspace}/applications"
        f"/{component_group}/pipelineruns"
        f"/{meta.release_pipeline_name}"
    )
    return release_url, release_plr_url


def build_slack_message(
    meta: ReleaseMetadata,
    component_group: str,
    release_url: str,
    release_plr_url: str,
) -> str:
    """Build a Slack Block Kit JSON message string."""
    message: dict[str, Any] = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "RHTAP Release Service\n",
                    "emoji": True,
                },
            },
            {"type": "divider"},
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {
                                "type": "text",
                                "text": "Release ",
                                "style": {"bold": True},
                            },
                            {
                                "type": "text",
                                "text": (
                                    f"{meta.origin_workspace}"
                                    f"/{component_group}"
                                    f"/{meta.release_name}"
                                ),
                            },
                        ],
                    }
                ],
            },
            {"type": "divider"},
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {
                                "type": "emoji",
                                "name": "@@CIRCLE_TYPE@@",
                            },
                            {"type": "text", "text": " "},
                            {
                                "type": "text",
                                "text": " @@STATUS_TEXT@@ ",
                                "style": {"bold": True},
                            },
                        ],
                    }
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"<{release_url}|Release Details>",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (f"<{release_plr_url}|Release PipelineRun Logs>"),
                },
            },
            {"type": "divider"},
        ]
    }
    return json.dumps(message)


def _write_empty(
    result_message: Path,
    result_secret: Path,
    result_keyname: Path,
) -> None:
    """Write empty strings to Tekton result files."""
    result_message.write_text("", encoding="utf-8")
    result_secret.write_text("", encoding="utf-8")
    result_keyname.write_text("", encoding="utf-8")


def collect_params(
    *,
    data_file: Path,
    snapshot_file: Path,
    release_file: Path,
    hac_url: str,
    result_message: Path,
    result_secret: Path,
    result_keyname: Path,
) -> int:
    """Collect Slack notification parameters and write result files."""
    validate_input_files(data_file, snapshot_file, release_file)

    data = load_json_dict(data_file)

    secret = extract_slack_secret(data)
    if secret is None:
        logger.info(
            "No secret name provided via 'slack.slack-notification-secret' key in Data."
        )
        _write_empty(result_message, result_secret, result_keyname)
        return 0

    result_secret.write_text(secret, encoding="utf-8")

    keyname = extract_slack_keyname(data)
    if keyname is None:
        logger.info(
            "No secret key name provided via"
            " 'slack.slack-webhook-notification-secret-keyname'"
            " key in Data."
        )
        _write_empty(result_message, result_secret, result_keyname)
        return 0

    result_keyname.write_text(keyname, encoding="utf-8")

    release = load_json_dict(release_file)
    snapshot = load_json_dict(snapshot_file)

    meta = extract_release_metadata(release)
    component_group = snapshot.get("componentGroup", "")
    release_url, release_plr_url = build_urls(hac_url, meta, component_group)

    message = build_slack_message(meta, component_group, release_url, release_plr_url)
    result_message.write_text(message, encoding="utf-8")

    return 0


def main() -> int:
    """Read environment, collect Slack notification params, write results."""
    data_dir = Path(tekton.require_env("DATA_DIR"))
    data_path = tekton.require_env("DATA_PATH")
    snapshot_path = tekton.require_env("SNAPSHOT_PATH")
    release_path = tekton.require_env("RELEASE_PATH")
    hac_url = tekton.require_env("HAC_URL")

    (
        result_message,
        result_secret,
        result_keyname,
    ) = tekton.result_paths_from_env(
        "RESULT_MESSAGE",
        "RESULT_SLACK_NOTIFICATION_SECRET",
        "RESULT_SLACK_NOTIFICATION_SECRET_KEYNAME",
    )

    return collect_params(
        data_file=data_dir / data_path,
        snapshot_file=data_dir / snapshot_path,
        release_file=data_dir / release_path,
        hac_url=hac_url,
        result_message=result_message,
        result_secret=result_secret,
        result_keyname=result_keyname,
    )


if __name__ == "__main__":
    raise SystemExit(main())
