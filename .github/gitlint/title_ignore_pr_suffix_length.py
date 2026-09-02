# -*- coding: utf-8 -*-
"""Enforce a max title length that ignores GitHub's squash-merge PR suffix."""

from __future__ import annotations

import re

from gitlint.git import GitCommit
from gitlint.rules import CommitRule, RuleViolation

PR_SUFFIX_RE = re.compile(r" \(#\d+\)$")
MAX_LENGTH = 72


class TitleMaxLengthIgnoringPrSuffix(CommitRule):
    """Enforce a max title length, ignoring GitHub's " (#NNN)" suffix."""

    id = "UC2"
    name = "title-max-length-ignoring-pr-suffix"

    def validate(self, commit: GitCommit) -> list[RuleViolation] | None:
        """Fail if the title (minus any GitHub PR suffix) is too long."""
        title = PR_SUFFIX_RE.sub("", commit.message.title)
        if len(title) > MAX_LENGTH:
            return [
                RuleViolation(
                    self.id,
                    f"Title exceeds max length ({len(title)}>{MAX_LENGTH})",
                    line_nr=1,
                )
            ]
        return None
