#!/usr/bin/env python3
"""Documentation must describe the code that ships (issue #26).

The docs told maintainers that every WebDAV request needs a password and that a
``WEBDAV_ENABLED`` setting starts the server. Neither was true: ``/health`` is
deliberately public, and no code reads ``WEBDAV_ENABLED``. These checks read the
shipped source and fail when a document names a setting that does not exist, or
contradicts the authentication model they describe.
"""

import os
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE_PATH = REPO_ROOT / "torboxed.py"

# Documents a user or a maintainer acts on. AGENTS.md is a local-only file in this
# repository, so it is checked whenever it is present.
DOC_FILES = ("README.md", ".env.example", "AGENTS.md")

WEBDAV_SETTING_PATTERN = re.compile(r"\bWEBDAV_[A-Z_]+\b")


class TestDocumentedWebDAVSettings(unittest.TestCase):
    """A documented setting must be one the code actually reads."""

    def test_every_documented_setting_is_read_by_the_code(self):
        source = SOURCE_PATH.read_text()
        checked = 0

        for name in DOC_FILES:
            path = REPO_ROOT / name
            if not path.exists():
                continue
            documented = sorted(set(WEBDAV_SETTING_PATTERN.findall(path.read_text())))
            self.assertTrue(documented, f"{name} should document at least one WebDAV setting")
            for setting in documented:
                checked += 1
                self.assertIn(
                    setting, source,
                    f"{name} documents {setting}, which torboxed.py never reads",
                )

        self.assertGreater(checked, 0)


class TestWebDAVAuthenticationWording(unittest.TestCase):
    """The docs must agree that /health is public and media paths are not."""

    def test_documents_do_not_claim_every_request_needs_credentials(self):
        for name in ("README.md", "AGENTS.md"):
            path = REPO_ROOT / name
            if not path.exists():
                continue
            text = path.read_text()
            self.assertNotIn(
                "All requests", text,
                f"{name} claims every request needs credentials; /health is public by design",
            )
            if "WEBDAV_AUTH_USER" in text:
                self.assertIn(
                    "/health", text,
                    f"{name} describes WebDAV authentication but never names the public /health path",
                )


class TestAgentGuideClaims(unittest.TestCase):
    """AGENTS.md guides the next change, so its claims must not drift silently."""

    def setUp(self):
        self.path = REPO_ROOT / "AGENTS.md"
        if not self.path.exists():
            self.skipTest("AGENTS.md is a local-only file in this repository")
        self.text = self.path.read_text()

    def test_no_hard_coded_size_or_test_count(self):
        self.assertIsNone(
            re.search(r"\(\s*~?\s*\d[\d,]*\s*lines?\s*\)", self.text),
            "describe the script's purpose instead of a line count that goes stale",
        )
        self.assertIsNone(
            re.search(r"\b\d[\d,]* tests\b", self.text),
            "point at the test command instead of a test count that goes stale",
        )


if __name__ == "__main__":
    unittest.main()
