"""Validate AI skill layout, frontmatter, and agent discovery symlinks."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SKILLS_DIR = REPO_ROOT / "skills"
AGENTS_MD = REPO_ROOT / "AGENTS.md"

FRONTMATTER_RE = re.compile(
    r"^---\s*\nname:\s*(?P<name>\S+)\s*\ndescription:\s*>-\s*\n(?P<desc>(?:\s+.+\n)+)---",
    re.MULTILINE,
)

SYMLINK_ROOTS = (
    REPO_ROOT / ".claude" / "skills",
    REPO_ROOT / ".cursor" / "skills",
)


@pytest.fixture(scope="session")
def skill_names() -> tuple[str, ...]:
    """Return skill directory names that contain a SKILL.md file."""
    names: list[str] = []
    for child in sorted(SKILLS_DIR.iterdir()):
        if child.is_dir() and (child / "SKILL.md").is_file():
            names.append(child.name)
    return tuple(names)


def test_skills_directory_exists() -> None:
    """skills/ must exist at the repository root."""
    assert SKILLS_DIR.is_dir()


def test_at_least_one_skill_exists(skill_names: tuple[str, ...]) -> None:
    """At least one repo-specific skill must be present."""
    assert len(skill_names) >= 1


@pytest.mark.parametrize("skill_name", ("writing-new-task-scripts", "using-helpers"))
def test_release_2703_skills_present(skill_name: str, skill_names: tuple[str, ...]) -> None:
    """RELEASE-2703 deliverables must remain present."""
    assert skill_name in skill_names


def test_skill_frontmatter(skill_names: tuple[str, ...]) -> None:
    """Each SKILL.md must follow agentskills.io frontmatter (name + description)."""
    for skill_name in skill_names:
        content = (SKILLS_DIR / skill_name / "SKILL.md").read_text(encoding="utf-8")
        match = FRONTMATTER_RE.match(content)
        assert match is not None, f"{skill_name}: missing or invalid frontmatter"
        assert match.group("name") == skill_name
        assert match.group("desc").strip()


def test_skill_symlinks(skill_names: tuple[str, ...]) -> None:
    """Every skill must be symlinked for Claude and Cursor agent discovery."""
    for root in SYMLINK_ROOTS:
        for skill_name in skill_names:
            link = root / skill_name
            assert link.is_symlink(), f"missing symlink: {link}"
            assert link.resolve() == (SKILLS_DIR / skill_name).resolve()


def test_agents_md_references_skills(skill_names: tuple[str, ...]) -> None:
    """AGENTS.md must document the skills directory and list available skills."""
    text = AGENTS_MD.read_text(encoding="utf-8")
    assert "## Skills" in text
    assert "`skills/`" in text
    assert ".claude/skills/" in text
    assert ".cursor/skills/" in text
    for name in skill_names:
        assert f"`{name}`" in text
