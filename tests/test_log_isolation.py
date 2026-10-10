#!/usr/bin/env python3
"""The test suite must never write to the repository's real log (issue #24).

``tests/conftest.py`` points ``LOG_PATH`` at a temporary file for every test and
fails the run if the repository log changes. These checks state that contract in
the suite itself: the configured path is temporary, and a log call made during a
test cannot reach the file the user reads to diagnose real problems.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import torboxed

REPO_ROOT = Path(__file__).resolve().parent.parent
REAL_LOG_PATH = REPO_ROOT / "torboxed.log"


def _log_state(path):
    """Return ``(size, modification time)`` for *path*, or None when it is absent."""
    try:
        stat = path.stat()
    except OSError:
        return None
    return (stat.st_size, stat.st_mtime_ns)


class TestSuiteLogIsolation(unittest.TestCase):
    """A log call during the suite lands in a temporary file, never the real log."""

    def test_log_path_is_a_temporary_file(self):
        log_path = Path(torboxed.LOG_PATH).resolve()

        self.assertNotEqual(log_path, REAL_LOG_PATH.resolve(),
                            "the suite must not log to the repository's own file")
        self.assertTrue(
            str(log_path).startswith(str(Path(tempfile.gettempdir()).resolve())),
            f"expected a temporary log path, got {log_path}",
        )

    def test_a_log_call_leaves_the_repository_log_untouched(self):
        state_before = _log_state(REAL_LOG_PATH)
        probe = "log isolation probe: this line must never reach the repository log"

        logger = torboxed.setup_logging(verbose=False, log_to_file=True)
        logger.error(probe)
        for handler in logger.handlers:
            handler.flush()

        self.assertEqual(
            _log_state(REAL_LOG_PATH), state_before,
            "a test log call must not change the repository log",
        )
        self.assertIn(probe, Path(torboxed.LOG_PATH).read_text(),
                      "the probe should have reached the temporary log")


if __name__ == "__main__":
    unittest.main()
