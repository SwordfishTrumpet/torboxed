"""Shared test configuration.

The suite isolated the database path per test but never pointed the log path
anywhere but the repository's own ``torboxed.log``, so a failing test wrote
fabricated errors such as "Fatal error during sync: corrupt config JSON" into
the file the user reads to diagnose real problems (issue #24). Every test now
runs with ``LOG_PATH`` pointing at a temporary file, and the suite fails if the
repository log changes while it runs.
"""

import sys
import tempfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
REAL_LOG_PATH = REPO_ROOT / "torboxed.log"

TEST_LOG_DIR = Path(tempfile.mkdtemp(prefix="torboxed-test-logs-"))
TEST_LOG_PATH = TEST_LOG_DIR / "torboxed-test.log"


def log_state(path: Path):
    """Return ``(size, modification time)`` for *path*, or None when it is absent."""
    try:
        stat = path.stat()
    except OSError:
        return None
    return (stat.st_size, stat.st_mtime_ns)


# Captured before any test runs, so the final check covers the whole session.
REAL_LOG_STATE_BEFORE = log_state(REAL_LOG_PATH)


@pytest.fixture(autouse=True)
def isolated_log_path(monkeypatch):
    """Point torboxed's log file at a temporary path for the duration of a test."""
    import torboxed

    monkeypatch.setattr(torboxed, "LOG_PATH", TEST_LOG_PATH, raising=False)
    return TEST_LOG_PATH


def pytest_sessionfinish(session, exitstatus):
    """Fail the run if any test wrote to the repository's real log file."""
    if log_state(REAL_LOG_PATH) != REAL_LOG_STATE_BEFORE:
        print(
            "\nLOG ISOLATION FAILURE: the test run changed "
            f"{REAL_LOG_PATH}. Point LOG_PATH at a temporary file instead.",
            file=sys.stderr,
        )
        session.exitstatus = 1
