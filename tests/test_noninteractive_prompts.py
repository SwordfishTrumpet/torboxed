#!/usr/bin/env python3
"""Non-interactive runs must never crash on an input prompt (issue #23).

The documented Docker first step (``torboxed.py --init``) and the cleanup
commands read from the terminal. Run without a terminal - a pipe, /dev/null, a
container - they raised EOFError and exited with a traceback. These tests cover
the shared prompt helper, the two cleanup confirmations, and a real
``--init`` run with stdin closed.
"""

import contextlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import torboxed

REPO_ROOT = Path(__file__).resolve().parent.parent


def _non_interactive_stdin():
    """A stand-in for stdin with no terminal attached."""
    stdin = Mock()
    stdin.isatty.return_value = False
    return stdin


def _interactive_stdin():
    """A stand-in for stdin with a terminal attached."""
    stdin = Mock()
    stdin.isatty.return_value = True
    return stdin


class TestPromptOrDefault(unittest.TestCase):
    """prompt_or_default() supplies the safe answer when nobody can answer."""

    def test_returns_default_without_a_terminal(self):
        with patch.object(torboxed.sys, "stdin", _non_interactive_stdin()):
            self.assertEqual(torboxed.prompt_or_default("Remove? ", "n"), "n")

    def test_does_not_read_input_without_a_terminal(self):
        def explode(*_args, **_kwargs):
            raise AssertionError("input() must not be called without a terminal")

        with patch.object(torboxed.sys, "stdin", _non_interactive_stdin()), \
             patch("builtins.input", side_effect=explode):
            self.assertEqual(torboxed.prompt_or_default("Remove? ", ""), "")

    def test_returns_default_when_input_reports_end_of_file(self):
        with patch.object(torboxed.sys, "stdin", _interactive_stdin()), \
             patch("builtins.input", side_effect=EOFError):
            self.assertEqual(torboxed.prompt_or_default("Remove? ", "n"), "n")

    def test_returns_the_typed_answer_with_a_terminal(self):
        with patch.object(torboxed.sys, "stdin", _interactive_stdin()), \
             patch("builtins.input", return_value="yes"):
            self.assertEqual(torboxed.prompt_or_default("Remove? ", "n"), "yes")


class TestCleanupCommandsWithoutTerminal(unittest.TestCase):
    """The cleanup commands cancel safely instead of crashing without a terminal."""

    IMDB = "tt0903747"

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = torboxed.DB_PATH
        torboxed.DB_PATH = Path(self.temp_dir.name) / "test.db"
        torboxed.init_db()
        self.removed = []

    def tearDown(self):
        torboxed.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def _fake_client(self, torrents):
        removed = self.removed

        class FakeDebridClient:
            def get_my_torrents(self_inner):
                return list(torrents)

            def remove_torrent(self_inner, torrent_id):
                removed.append(str(torrent_id))
                return True

        return FakeDebridClient()

    def _run_without_terminal(self, cleanup, torrents):
        output = io.StringIO()
        with patch.object(torboxed, "create_debrid_client",
                          return_value=self._fake_client(torrents)), \
             patch.object(torboxed.sys, "stdin", _non_interactive_stdin()), \
             contextlib.redirect_stdout(output):
            cleanup(force=False)
        return output.getvalue()

    def test_unmatched_cleanup_cancels_without_a_terminal(self):
        torrents = [
            {"id": "unmatched-1", "name": "Some.Movie.2024.1080p.WEB-DL", "hash": "a" * 40},
        ]

        output = self._run_without_terminal(torboxed.cleanup_unmatched_torrents, torrents)

        self.assertIn("Cancelled", output)
        self.assertEqual(self.removed, [], "a cancelled cleanup must remove nothing")

    def test_duplicate_cleanup_cancels_without_a_terminal(self):
        torboxed.record_processed(
            self.IMDB, "Breaking Bad", 2008, "show", "added", "test",
            debrid_id="unrelated-id", season="S01",
        )
        torrents = [
            {"id": "pack-1080p", "name": "Breaking.Bad.S01.1080p.BluRay.x264",
             "size": 30_000_000_000},
            {"id": "pack-720p", "name": "Breaking.Bad.S01.720p.WEB-DL.x264",
             "size": 10_000_000_000},
        ]

        output = self._run_without_terminal(torboxed.cleanup_duplicate_torrents, torrents)

        self.assertIn("Cancelled", output)
        self.assertEqual(self.removed, [], "a cancelled cleanup must remove nothing")


class TestTelegramSetupWithoutTerminal(unittest.TestCase):
    """--init skips the optional Telegram setup when nobody can answer."""

    def test_setup_is_skipped_and_writes_no_env_file(self):
        temp_dir = Path(tempfile.mkdtemp())
        env_file = temp_dir / ".env"
        original_env_path = torboxed.ENV_PATH
        torboxed.ENV_PATH = env_file

        try:
            with patch.object(torboxed.sys, "stdin", _non_interactive_stdin()):
                torboxed._setup_telegram_interactive()
        finally:
            torboxed.ENV_PATH = original_env_path

        self.assertFalse(env_file.exists(), "skipped setup must not write an env file")


class TestInitWithoutTerminal(unittest.TestCase):
    """The documented Docker first step finishes instead of crashing (issue #23)."""

    def test_init_with_closed_stdin_exits_zero(self):
        temp_dir = Path(tempfile.mkdtemp())
        env = dict(os.environ)
        env.update({
            "DB_PATH": str(temp_dir / "probe.db"),
            "LOG_PATH": str(temp_dir / "probe.log"),
            "ENV_PATH": str(temp_dir / ".env"),
        })

        with open(os.devnull, "rb") as devnull:
            result = subprocess.run(
                [sys.executable, "torboxed.py", "--init"],
                cwd=str(REPO_ROOT),
                env=env,
                stdin=devnull,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=180,
            )

        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertNotIn("Traceback", result.stdout)
        self.assertTrue((temp_dir / "probe.db").exists(), "the database must be created")


if __name__ == "__main__":
    unittest.main()
