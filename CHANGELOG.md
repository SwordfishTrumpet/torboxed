# Changelog

All notable changes to this project are documented here.

## 2026-08-26

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
