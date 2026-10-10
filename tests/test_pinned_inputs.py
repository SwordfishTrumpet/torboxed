#!/usr/bin/env python3
"""Build inputs must be pinned to fixed versions (issue #27).

The Python dependencies are pinned and hashed in ``uv.lock``, but the container base
image and every GitHub Action were referenced by tags that can move under the same name.
A rebuild could then differ from the reviewed image, and a taken-over tag would run
inside this project's pipeline with its token. These checks fail when a moving reference
comes back.
"""

import os
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"
DOCKERFILE = REPO_ROOT / "Dockerfile"

USES_PATTERN = re.compile(r"^\s*(?:-\s*)?uses:\s*(\S+)")
SHA_PATTERN = re.compile(r"^[^@]+@[0-9a-f]{40}$")
VERSION_COMMENT_PATTERN = re.compile(r"#\s*v?\d+\S*\s*$")


class TestWorkflowActionsArePinned(unittest.TestCase):
    """Every action reference must name a full commit, with the version in a comment."""

    def test_every_uses_is_a_full_commit_sha(self):
        workflows = sorted(WORKFLOW_DIR.glob("*.yml")) + sorted(WORKFLOW_DIR.glob("*.yaml"))
        self.assertTrue(workflows, "the repository should have a CI workflow")

        checked = 0
        for path in workflows:
            for number, line in enumerate(path.read_text().splitlines(), start=1):
                match = USES_PATTERN.match(line)
                if not match:
                    continue
                checked += 1
                reference = match.group(1)
                self.assertRegex(
                    reference, SHA_PATTERN,
                    f"{path.name}:{number} uses {reference}, which is not a full "
                    f"40-character commit SHA (issue #27)",
                )
                self.assertRegex(
                    line, VERSION_COMMENT_PATTERN,
                    f"{path.name}:{number} pins a commit but does not name the version "
                    f"in a comment, so nobody can tell what to update (issue #27)",
                )

        self.assertGreater(checked, 0)


class TestContainerImageIsPinned(unittest.TestCase):
    """The base image must be a digest and the uv install a fixed release."""

    def test_base_image_is_pinned_by_digest(self):
        text = DOCKERFILE.read_text()
        from_lines = [l for l in text.splitlines() if l.strip().upper().startswith("FROM ")]
        self.assertTrue(from_lines, "Dockerfile should have a FROM line")
        self.assertRegex(
            from_lines[0], r"@sha256:[0-9a-f]{64}",
            "the Dockerfile base image needs a sha256 digest, not just a tag (issue #27)",
        )

    def test_uv_install_is_a_fixed_version(self):
        text = DOCKERFILE.read_text()
        installs = [
            l for l in text.splitlines()
            if "pip install" in l and "uv pip install" not in l
        ]
        self.assertTrue(installs, "Dockerfile should install uv")
        for line in installs:
            self.assertRegex(
                line, r"\buv==\d",
                f"the Dockerfile installs uv without a fixed version: {line.strip()} "
                f"(issue #27)",
            )


if __name__ == "__main__":
    unittest.main()
