# Changelog

All notable changes to this project are documented here.

## Version 1.1.1 (2026-10-10, revision ca844e1)

### Fixed

- Setting up from a script or a container no longer stops with an error. `--init`,
  `--cron-setup`, and the cleanup commands asked their questions by reading the keyboard
  directly, so a run with no keyboard attached (the documented `docker run ... --init`, a
  scheduled job, a shell pipeline) ended in an `EOFError` traceback. Each question now
  falls back to a safe default when there is no terminal (cleanup cancels, cron and Telegram
  setup skip), and the log records which default was used.
- Running the test suite no longer writes fake errors into your real log file. Tests log to
  a temporary file, so `torboxed.log` only holds what an actual run did.
- `--test` now checks the debrid service you configured. It looked only for a Torbox key, so
  a Real Debrid user always saw the environment check fail; it now looks for
  `REAL_DEBRID_API_KEY` when `DEBRID_SERVICE=real_debrid`, and it reads settings from the
  process environment as well as from the `.env` file.
- Documentation corrected: the WebDAV section no longer mentions the removed
  `WEBDAV_ENABLED` switch, and the security note now states that `/health` is the one path
  that answers without credentials (media paths still require them).

### Changed

- Version bumped to 1.1.1.

## Version 1.1.0 (2026-08-26, revision 4ae6fa7)

### Added
- New `--sources` CLI flag to select which Trakt lists to sync, e.g.
  `torboxed.py --sources "movies/trending,shows/trending"`. The selection is
  validated against the 24 known source strings and persists to the database
  config. It is a config-only command: it does not start a sync (run
  `torboxed.py` afterwards to sync with the new selection).
- `validate_sources()` / `update_sources()` helpers and `DEFAULT_SOURCES` /
  `ALL_SOURCES` constants for source selection and validation.
- Tests covering the new default sources, source validation, the valid-source
  set, and config persistence (5 new tests; 427 total).

### Changed
- Default Trakt sources on a fresh `--init` are now the curated lists only
  (trending, popular, anticipated for both movies and shows). Previously the
  default enabled all 23 public lists, whose `watched/*` and `collected/*`
  entries each return up to 10,000 items — a ~160k-item queue that starved TV
  shows (processed after movies) and made syncs effectively never complete.
  The full list set remains available via `--sources`.
- `init_db()` default config insert is now parameterized from `DEFAULT_SOURCES`
  instead of an inline JSON blob.
- README and FAQ updated to document the new flag and default behavior.
