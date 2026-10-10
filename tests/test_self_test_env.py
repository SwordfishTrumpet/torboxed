#!/usr/bin/env python3
"""The self-test must judge the settings the app actually uses (issue #25).

``--test`` read the .env file by hand and treated Torbox as the only debrid
service, so a Real Debrid user - or anyone supplying keys as real environment
variables - was told their working setup was broken. These checks run the real
command and read the environment line it reports.
"""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Keys a developer's own environment could carry into the child process; the
# checks below supply exactly the configuration they mean to test.
SETTING_KEYS = (
    "DEBRID_SERVICE",
    "TORBOX_API_KEY",
    "REAL_DEBRID_API_KEY",
    "TRAKT_CLIENT_ID",
    "TRAKT_CLIENT_SECRET",
    "TRAKT_ACCESS_TOKEN",
)


class SelfTestEnvironmentCase(unittest.TestCase):
    """Run ``torboxed.py --test`` in a controlled environment and inspect its report."""

    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="torboxed-selftest-"))
        self.env_path = self.temp_dir / ".env"
        self.db_path = self.temp_dir / "probe.db"
        self.log_path = self.temp_dir / "probe.log"

    def _run(self, env_file_content=None, extra_env=None, configure=True):
        """Run --test; return the combined output. ``env_file_content`` None = no file."""
        if env_file_content is None:
            self.env_path.unlink(missing_ok=True)
        else:
            self.env_path.write_text(env_file_content)

        env = dict(os.environ)
        for key in SETTING_KEYS:
            env.pop(key, None)
        env.update({
            "ENV_PATH": str(self.env_path),
            "DB_PATH": str(self.db_path),
            "LOG_PATH": str(self.log_path),
        })
        env.update(extra_env or {})

        if configure:
            with open(os.devnull, "rb") as devnull:
                subprocess.run(
                    [sys.executable, "torboxed.py", "--init"],
                    cwd=str(REPO_ROOT), env=env, stdin=devnull,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=180,
                )

        result = subprocess.run(
            [sys.executable, "torboxed.py", "--test"],
            cwd=str(REPO_ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=180,
        )
        return result

    def _environment_line(self, output):
        for line in output.splitlines():
            if "Environment file" in line:
                return line
        self.fail(f"no environment line in output:\n{output}")

    def test_real_debrid_only_configuration_passes(self):
        result = self._run(
            env_file_content=(
                "DEBRID_SERVICE=real_debrid\n"
                "REAL_DEBRID_API_KEY=placeholder_real_debrid_key\n"
                "TRAKT_CLIENT_ID=placeholder_trakt_id\n"
                "TRAKT_CLIENT_SECRET=placeholder_trakt_secret\n"
            ),
        )

        line = self._environment_line(result.stdout)
        self.assertIn("PASS", line, result.stdout)
        self.assertIn("real_debrid", line)
        self.assertNotIn("TORBOX_API_KEY", result.stdout)
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_keys_supplied_as_environment_variables_are_honored(self):
        result = self._run(
            env_file_content=None,
            extra_env={
                "DEBRID_SERVICE": "real_debrid",
                "REAL_DEBRID_API_KEY": "placeholder_real_debrid_key",
                "TRAKT_CLIENT_ID": "placeholder_trakt_id",
                "TRAKT_CLIENT_SECRET": "placeholder_trakt_secret",
            },
        )

        line = self._environment_line(result.stdout)
        self.assertIn("PASS", line, result.stdout)

    def test_missing_settings_name_the_key_for_the_selected_service(self):
        result = self._run(env_file_content="")

        line = self._environment_line(result.stdout)
        self.assertIn("FAIL", line)
        self.assertIn("TORBOX_API_KEY", result.stdout)
        self.assertNotEqual(result.returncode, 0)

    def test_missing_settings_name_the_real_debrid_key_when_selected(self):
        result = self._run(
            env_file_content="DEBRID_SERVICE=real_debrid\n",
        )

        line = self._environment_line(result.stdout)
        self.assertIn("FAIL", line)
        self.assertIn("REAL_DEBRID_API_KEY", line)


if __name__ == "__main__":
    unittest.main()
