# Changelog

All notable changes to CBBpy are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.0] - Unreleased

### Added
- ESPN's JSON API as a second transport for game data, selectable per call with
  `source="api"` (new default) or `source="html"`. Both sources emit an identical
  output schema.
- `game_datetime` on game info and schedule frames: the scheduled tipoff instant as an
  ISO-8601 UTC string, so consumers can convert to any timezone.
- `period` and `secs_left_period` in play-by-play: format-agnostic replacements for the
  half/quarter column pairs, always populated regardless of era or gender.
- `home_win_prob` in play-by-play, plus point spread, over/under, and moneyline columns
  in game info.
- `cbbpy` command-line interface (`[project.scripts]`).
- `get_team_logos()`.
- `play_type`, `play_type_id`, and per-play player names/ids in play-by-play.
- `throttle` and `n_jobs` parameters on the bulk scraping functions, and a 30s
  per-request timeout so a hung connection raises into the retry loop (#70, #79).

### Changed
- HTTP now goes through `curl_cffi` with browser TLS impersonation. ESPN's AWS WAF
  fingerprints TLS clients and serves flagged clients an empty challenge response;
  `requests` no longer gets through.
- Logging overhaul: user-facing notices are `warnings` (category `CBBpyWarning`) rather
  than prints, and the library no longer suppresses warnings globally (#78).
- Overtime count is now read from the periods present in the data, falling back to the
  rule-change calculation, and an unexpected count warns rather than asserting (#71).
- Men's and women's team maps updated through the 2026 season, with a season fallback
  for unmapped years (#66).
- Worker count is floored at 1 so single-core machines can scrape in parallel mode.

### Deprecated
- `game_day` and `game_time` (US/Pacific) on game info and schedule frames, in favor of
  `game_datetime`. Removal in 3.0.
- `half`/`secs_left_half` and `quarter`/`secs_left_qt` in play-by-play, in favor of
  `period`/`secs_left_period`. Removal in 3.0.

### Fixed
- Conference-game labelling change that broke the scraper (#64).
- Shot chart coordinates are matched to plays by id rather than by position.
- Home team detection.
- Duplicate intra-conference games returned by `get_games_conference()` (#84).
- Play-by-play clock parsing against missing or malformed clocks (#82).
- Failure-path return types (#83).
- Latent `NameError`s in the retry except-blocks (#69).

## [2.1.2] - 2025-01-20

### Fixed
- Hotfix for #61, where the `utils` folder was not installing with the rest of the
  package via `pip`.

## [2.1.1] - 2025-01-05

### Changed
- Relicensed under Apache 2.0.

### Fixed
- Fix for #59, where the team map CSVs were not part of the package.

## [2.1.0] - 2024-12-05

### Added
- Scraping of team and conference schedules (#33).
- Scraping of games for a team or for all teams in a conference (#34).
- Player biographical information.
- Support for in-progress games and for cancelled/postponed games.
- Point spread as part of game info.

### Changed
- Logging enhancements (#46).
- Support for the NCAAW rule change to quarters instead of halves (#50).
- DataFrames are sorted for consistent output across runs.

### Fixed
- Neutral site determination (#43, #52).

## [2.0.2] - 2024-01-09

### Fixed
- Compatibility with newer versions of the `requests` library (#45).
- Added `bs4` and `requests` to the declared dependencies.

## [2.0.1] - 2024-01-01

### Fixed
- Fix for #37.

## [2.0.0] - 2023-12-27

### Added
- Scraper for NCAA women's basketball data.
- Shot coordinates in play-by-play for games with shot data available.
- Multiprocessing, speeding up scraping proportionally to the number of CPU cores.

## [1.1.0] - 2022-12-22

### Added
- Scraping of date ranges.
- All functions now let the caller skip play-by-play, boxscore, or metadata.
- Support for Python >= 3.7.

### Fixed
- Major fix for ESPN's change to page structure.

## [1.0.5] - 2022-09-30

### Fixed
- Games with no location, and irregular play-by-plays (#20, #21).

## [1.0.4] - 2022-09-29

### Added
- `is_conference`, `is_neutral`, and `is_postseason` flags in game info (#17).
- Team total row in boxscores, for resolving cases where player aggregates don't match
  the team total (#16).

### Fixed
- Handling for alternate game information layouts (#19).
- Better handling of boxscores with unexpected or missing values.

## [1.0.3] - 2022-09-10

### Added
- Rotating headers and retry attempts for pages that don't load properly (#1).

### Fixed
- Scraper was only scraping top-25 games (#13).
- Play-by-plays with video replays attached (#11).
- Mixed-type columns (#12).

## [1.0.2] - 2022-09-03

### Added
- Support for suspended/TBD games (#5).

### Fixed
- `get_game_ids` can be called with a string date (#6).
- Exceptions are logged rather than printed, and requests retry when the site doesn't
  load (#1).

## [1.0.1] - 2022-08-10

### Added
- Traceback detail in the log file.

### Fixed
- Type casting for DataFrame columns that caused some games to error.

## [1.0.0] - 2022-08-10

Initial release of CBBpy.

[2.2.0]: https://github.com/dcstats/CBBpy/compare/v2.1.2...HEAD
[2.1.2]: https://github.com/dcstats/CBBpy/compare/v2.1.1...v2.1.2
[2.1.1]: https://github.com/dcstats/CBBpy/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/dcstats/CBBpy/compare/v2.0.2...v2.1.0
[2.0.2]: https://github.com/dcstats/CBBpy/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/dcstats/CBBpy/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/dcstats/CBBpy/compare/v1.1.0...v2.0.0
[1.1.0]: https://github.com/dcstats/CBBpy/compare/v1.0.5...v1.1.0
[1.0.5]: https://github.com/dcstats/CBBpy/compare/v1.0.4...v1.0.5
[1.0.4]: https://github.com/dcstats/CBBpy/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/dcstats/CBBpy/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/dcstats/CBBpy/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/dcstats/CBBpy/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/dcstats/CBBpy/releases/tag/v1.0.0
