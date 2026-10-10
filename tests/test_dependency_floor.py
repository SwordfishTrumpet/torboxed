#!/usr/bin/env python3
"""The locked anyio pin must stay at or above the fixed release (issue #30).

anyio below 4.14.2 carries two published advisories (PYSEC-2026-4024 and
PYSEC-2026-4025). The weekly ``pip-audit`` job installs whatever the lockfile
names, so a downgrade -- or a second, older marker for an interpreter the fix no
longer supports -- quietly turns that job red again. These checks read
``uv.lock`` and ``pyproject.toml`` directly and fail when that happens.
"""

import os
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REPO_ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = REPO_ROOT / "uv.lock"
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"

# First anyio release that fixes both advisories named in issue #30.
ANYIO_MIN_VERSION = (4, 14, 2)
# Python 3.9 cannot use anyio 4.14.2+, so its marker must not come back either.
MINIMUM_PYTHON = (3, 10)


def _parse_version(text):
    return tuple(int(part) for part in re.findall(r"\d+", text)[:3])


def _locked_anyio_versions(lock_text):
    """Every version pinned for anyio, one per marker uv resolved."""
    return re.findall(r'name = "anyio"\nversion = "([^"]+)"', lock_text)


class TestLockedAnyioIsFixed(unittest.TestCase):
    """No lockfile marker may resolve anyio below the fixed release."""

    def test_lockfile_pins_anyio_at_or_above_the_fix(self):
        versions = _locked_anyio_versions(LOCK_PATH.read_text())
        self.assertTrue(versions, "uv.lock should pin anyio")

        for version in versions:
            self.assertGreaterEqual(
                _parse_version(version), ANYIO_MIN_VERSION,
                f"uv.lock pins anyio {version}, below the fixed release "
                f"{'.'.join(map(str, ANYIO_MIN_VERSION))} (issue #30)",
            )


class TestPythonFloorAllowsTheFix(unittest.TestCase):
    """The declared Python floor must let every marker reach that release."""

    def test_requires_python_is_at_least_3_10(self):
        text = PYPROJECT_PATH.read_text()
        match = re.search(r'^requires-python\s*=\s*">=(\d+)\.(\d+)"', text, re.MULTILINE)
        self.assertIsNotNone(match, "pyproject.toml should declare requires-python")
        floor = (int(match.group(1)), int(match.group(2)))
        self.assertGreaterEqual(
            floor, MINIMUM_PYTHON,
            "anyio 4.14.2+ requires Python 3.10+, so a lower floor would add a "
            "marker that cannot receive the fix (issue #30)",
        )


if __name__ == "__main__":
    unittest.main()
