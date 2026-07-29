[![PyPi Version](https://img.shields.io/pypi/v/cbbpy.svg)](https://pypi.org/project/cbbpy/) [![Downloads](https://img.shields.io/pypi/dm/cbbpy?color=be94e4
)](https://pypistats.org/packages/cbbpy)

# CBBpy: A Python-based web scraper for NCAA basketball

## Purpose
This package is designed to bridge the gap between data and analysis for NCAA D1 basketball. CBBpy can grab play-by-play, boxscore, and other game metadata for any NCAA D1 men's or women's basketball game. Inspired by the [ncaahoopR package](https://github.com/lbenz730/ncaahoopR) by Luke Benz - check that out if you are an R user!

## Installation and import
CBBpy requires Python >= 3.9 as well as the following packages:
* pandas>=2.0.0
* numpy>=1.21.6
* python-dateutil>=2.8.2
* pytz>=2022.1
* tqdm>=4.63.0
* lxml>=4.9.2
* joblib>=1.0.0
* beautifulsoup4>=4.11.0
* curl_cffi>=0.10.0
* rapidfuzz>=2.14.0
* platformdirs>=4.0.0


Install using pip:
```shell
pip install cbbpy
```

Or upgrade an existing installation:
```shell
pip install --upgrade cbbpy
```

The men's and women's scrapers can be imported as such:
```python
import cbbpy.mens_scraper as s
import cbbpy.womens_scraper as s
```

## Command line

Installing CBBpy also provides a `cbbpy` command for pulling data to files without writing Python. One file is written per frame and its path printed to stdout.

Subcommands:

- `game GAME_ID ...` — scrape info, boxscore, and pbp for one or more games
- `info GAME_ID ...` — scrape only game metadata
- `box GAME_ID ...` — scrape only the boxscore
- `pbp GAME_ID ...` — scrape only play-by-play
- `range START END` — scrape every game in a date range
- `season [SEASON]` — scrape a whole season (default: current)
- `team TEAM` — scrape a team's season
- `conference CONFERENCE` — scrape a conference's season
- `ids DATE` — print game IDs for a date (stdout, no files)
- `player PLAYER_ID` — scrape a player's bio
- `schedule --team T | --conference C` — scrape a team or conference schedule

Options (which commands they apply to):

- `-g/--gender` (`mens` default, or `womens`) — every command
- `--source` (`api` default, or `html`) — game/info/box/pbp/range/season/team/conference/ids
- `-o/--output-dir`, `--format` (`csv` default, or `parquet`) — every command that writes files
- `--stdout` — print instead of writing a file (info/player as a transposed `field: value` block; box/pbp/schedule open a scrollable view via `less -S` in an interactive terminal — arrow keys to pan, `q` to quit — or emit plain CSV when piped/redirected): info/box/pbp/player/schedule
- `--no-info/--no-box/--no-pbp` — skip a frame: game/range/season/team/conference
- `--throttle`, `--n-jobs` — bulk commands: range/season/team/conference
- `-s/--season` — team/conference/schedule (season is a positional on `season`)

```shell
# scrape one game (writes info, boxscore, and pbp files to the current dir)
cbbpy game 401522202

# just the metadata for one game, printed to the terminal
cbbpy info 401522202 --stdout

# pbp in the terminal opens a scrollable table automatically (q to quit); piping yields CSV
cbbpy pbp 401581583 --stdout
cbbpy pbp 401581583 --stdout | column -s, -t   # CSV when redirected

# a women's team's 2022 season as parquet, without play-by-play
cbbpy team davidson -s 2022 -g womens --no-pbp --format parquet -o ./data

# all games in a date range
cbbpy range 11-30-2022 12-10-2022 -o ./data

# list game IDs for a date (printed to stdout, no files)
cbbpy ids 04-03-2021
```

## Known Issues
<!-- - Plenty of games are, for whatever reason, not available on ESPN's site due to a 'Page not found' error. Sometimes these errors appear randomly and resolve themselves in due time (either hours or days), and other games have had this error for years, as far as I can tell, and are not able to be scraped. -->
- Sometimes an issue might cause the scraper to take longer than expected. If it seems to be taking too long, **check the log file** for a list of errors that occurred during scraping. The log file location is outputted after the scraper finishes, or you can see the location anytime by running `python -c "from cbbpy.utils.cbbpy_utils import log_file; print(log_file)"`.
- Before the 16-17 season, Play-by-Play and Boxscores for women's games on ESPN are pretty sparse.
- If both teams in a game are participating in the first game of conference play, the `is_conference` flag will incorrectly show `False` until the game goes final. This is not an issue after the game is over.


## Functions available in CBBpy
NOTE: game ID, as far as CBBpy is concerned, is a valid **ESPN** game ID

The game-data functions accept a `source` parameter: `"api"` (default, ESPN's JSON API) or `"html"` (page scraping). Both emit an identical output schema, so the choice only matters as a fallback if one transport breaks.

`s.get_game_info(game_id: Union[str, int], source: str = "api")` grabs all the metadata (game date, time, score, teams, referees, etc) for a particular game.

`s.get_game_boxscore(game_id: Union[str, int], source: str = "api")` returns a pandas DataFrame with each player's stats for a particular game.

`s.get_game_pbp(game_id: Union[str, int], source: str = "api")` scrapes the play-by-play tables for a game and returns a pandas DataFrame, with each entry representing a play made during the game.

`s.get_game(game_id: Union[str, int], info: bool = True, box: bool = True, pbp: bool = True, source: str = "api")` gets *all* information about a game (game info, boxscore, PBP) and returns a tuple of results `(game_info, boxscore, pbp)`. `info, box, pbp` are booleans which users can set to `False` if there is any information they wish not to scrape. For example, `box = False` would return an empty DataFrame for the boxscore info, while scraping PBP and metadata info normally.

`s.get_games_season(season: Union[str, int] = None, info: bool = True, box: bool = True, pbp: bool = True, source: str = "api", throttle: float = 0.5, n_jobs: int = None)` scrapes all game information for all finished or in progress games in a particular season (defaults to the current season). As an example, to scrape games for the 2020-21 season, call `get_games_season(2021)`. Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`s.get_games_range(start_date: Union[int, datetime], end_date: Union[int, datetime], info: bool = True, box: bool = True, pbp: bool = True, source: str = "api", throttle: float = 0.5, n_jobs: int = None)` scrapes all game information for all finished or in progress games between `start_date` and `end_date` (inclusive). As an example, to scrape games from November 30, 2022 to December 10, 2022, call `get_games_season('11-30-2022', '12-10-2022')`. Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`s.get_games_team(team: str, season: Union[str, int] = None, info: bool = True, box: bool = True, pbp: bool = True, source: str = "api", throttle: float = 0.5, n_jobs: int = None)` scrapes all game information for all finished or in progress games in a particular season (defaults to the current season) for a given team. As an example, to scrape games for Duke's 2020-21 season, call `get_games_team('duke', 2021)`; for their current season, you can just call `get_games_team('duke')`. If a given team does not have an exact match in the static list of teams scraped from ESPN's site, this function will scrape the games for the closest fuzzy-matched team (e.g. if "valpo" is provided as the team, the function will scrape the games for "Valparaiso"). Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`s.get_games_conference(conference: str, season: Union[str, int] = None, info: bool = True, box: bool = True, pbp: bool = True, source: str = "api", throttle: float = 0.5, n_jobs: int = None)` scrapes all game information for all finished or in progress games in a particular season (defaults to the current season) for all teams in a given conference. As an example, to scrape games for the A10's 2017-18 season, call `get_games_conference('a10', 2018)`; for their current season, you can just call `get_games_conference('a10')`. If a given conference does not have an exact match in the static list of conferences scraped from ESPN's site, this function will scrape the games for the closest fuzzy-matched conference (e.g. if "am east" is provided as the conference, the function will scrape the games for "America East Conference"). Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`s.get_game_ids(date: Union[str, datetime], source: str = "api")` returns a list of all game IDs for a particular date.

`s.get_player_info(player_id: Union[str, int])` returns a DataFrame describing the player's info from ESPN's bio page.

`s.get_teams_from_conference(conference: str, season: Union[str, int] = None)` returns a list of the teams in the given conference for a season (defaults to the current season).

`s.get_team_schedule(team: str, season: Union[str, int] = None)` returns a DataFrame of a team's schedule for a given season (defaults to the current season). If a given team does not have an exact match in the static list of teams scraped from ESPN's site, this function will scrape the schedule for the closest fuzzy-matched team (e.g. if "valpo" is provided as the team, the function will scrape the schedule for "Valparaiso").

`s.get_conference_schedule(conference: str, season: Union[str, int] = None)` returns a DataFrame of the schedules for all teams in a given conference for a given season (defaults to the current season). If a given conference does not have an exact match in the static list of conferences scraped from ESPN's site, this function will scrape the schedules for the closest fuzzy-matched conference (e.g. if "am east" is provided as the conference, the function will scrape the schedules for "America East Conference").

`s.get_team_logos(teams: Union[str, list] = None, season: Union[str, int] = None, dest: str = "team_logos", dark: bool = False, overwrite: bool = False)` downloads team logo PNGs from ESPN's CDN into `dest`, saved as `{team_id}.png` (set `dark=True` for the dark-background variants). With no `teams` argument it downloads a logo for every team in the given season's team map; existing files are skipped unless `overwrite=True`. Returns a DataFrame with one row per team (`team`, `id`, `logo_path`). Intended as a once-a-season utility.

## Responsible use

The bulk scraping functions (`get_games_season`, `get_games_range`, `get_games_team`, `get_games_conference`) fetch games in parallel. To stay polite to ESPN's servers, they accept two knobs:

- `throttle` (default `0.5`): the mean delay, in seconds, each worker waits before scraping a game (jittered ±50% so workers don't fire in lockstep). Set `throttle=0` to disable the delay entirely and restore pre-2.2.0 behavior.
- `n_jobs` (default: CPU count minus 1): the number of parallel workers.

The defaults are a reasonable balance of speed and politeness for occasional bulk scrapes. If you're scraping many seasons back-to-back, consider raising `throttle` and/or lowering `n_jobs`. Every request also carries a 30-second timeout, so a hung connection is retried instead of stalling a worker indefinitely.

## Changelog

Every release, including the columns currently deprecated and the version that removes them, is documented in [CHANGELOG.md](CHANGELOG.md).

## A note on dates and times

As of v2.2.0, every game info and schedule DataFrame includes `game_datetime`: the scheduled tipoff instant as an ISO-8601 UTC string (e.g. `2023-04-04T01:20:00Z`), which you can parse and convert to any timezone. The `game_day` and `game_time` columns (US/Pacific) are deprecated and will be removed in v3.0. Until then, `game_day` continues to reflect the Pacific calendar date of tipoff, which matches both ESPN's own scoreboard day grouping and the game's local calendar date in practice.

## Examples

Function call:

```python
import cbbpy.mens_scraper as s
s.get_game_info('401522202')
```

Returns:
|    |   game_id | game_status   | home_team     |   home_id |   home_rank | home_record   |   home_score | away_team              |   away_id |   away_rank | away_record   |   away_score |   home_point_spread | home_win   |   num_ots | is_conference   | is_neutral   | is_postseason   | tournament                                            | game_datetime        | game_day       | game_time    | game_loc    | arena       |   arena_capacity |   attendance | tv_network   | referee_1   | referee_2     | referee_3    |   over_under |   home_moneyline |   away_moneyline |
|---:|----------:|:--------------|:--------------|----------:|------------:|:--------------|-------------:|:-----------------------|----------:|------------:|:--------------|-------------:|--------------------:|:-----------|----------:|:----------------|:-------------|:----------------|:------------------------------------------------------|:---------------------|:---------------|:-------------|:------------|:------------|-----------------:|-------------:|:-------------|:------------|:--------------|:-------------|-------------:|-----------------:|-----------------:|
|  0 | 401522202 | Final         | UConn Huskies |        41 |           4 | 31-8          |           76 | San Diego State Aztecs |        21 |           5 | 32-7          |           59 |                  -7 | True       |         0 | False           | True         | True            | Men's Basketball Championship - National Championship | 2023-04-04T01:20:00Z | April 03, 2023 | 06:20 PM PDT | Houston, TX | NRG Stadium |              nan |        72423 | CBS          | Ron Groover | Terry Oglesby | Keith Kimble |        130.5 |             -361 |              285 |

Function call:

```python
import cbbpy.womens_scraper as s 
s.get_game_boxscore('401528028')
```

Returns (partially):
|    |   game_id | team       | player      |   player_id | position   | starter   |   min |   fgm |   fga |   2pm |   2pa |   3pm |   3pa |   ftm |   fta |   pts |   reb |   ast |   to |   stl |   blk |   oreb |   dreb |   pf |
|---:|----------:|:-----------|:------------|------------:|:-----------|:----------|------:|------:|------:|------:|------:|------:|------:|------:|------:|------:|------:|------:|-----:|------:|------:|-------:|-------:|-----:|
|  0 | 401528028 | LSU Tigers | A. Reese    |     4433402 | F          | True      |    29 |     5 |    12 |     5 |    12 |     0 |     0 |     5 |     8 |    15 |    10 |     5 |    0 |     3 |     1 |      6 |      4 |    3 |
|  1 | 401528028 | LSU Tigers | L. Williams |     4280886 | F          | True      |    37 |     9 |    16 |     9 |    16 |     0 |     0 |     2 |     2 |    20 |     5 |     0 |    3 |     3 |     0 |      1 |      4 |    4 |
|  2 | 401528028 | LSU Tigers | F. Johnson  |     4698736 | G          | True      |    37 |     4 |    11 |     3 |     7 |     1 |     4 |     1 |     1 |    10 |     7 |     4 |    4 |     1 |     0 |      2 |      5 |    1 |
|  3 | 401528028 | LSU Tigers | K. Poole    |     4433418 | G          | True      |    24 |     2 |     3 |     0 |     1 |     2 |     2 |     0 |     2 |     6 |     3 |     1 |    1 |     0 |     1 |      0 |      3 |    2 |
|  4 | 401528028 | LSU Tigers | A. Morris   |     4281251 | G          | True      |    33 |     8 |    14 |     7 |    11 |     1 |     3 |     4 |     4 |    21 |     2 |     9 |    2 |     1 |     0 |      1 |      1 |    3 |

Function call:

```python
import cbbpy.mens_scraper as s
s.get_game_pbp('401522202')
```

Returns (partially):
|    |                 id |   game_id | home_team     | away_team              | play_desc                                                             |   home_score |   away_score |   period |   secs_left_period |   secs_left_reg | play_team              | play_type          |   play_type_id | shooting_play   | scoring_play   | is_three   | player_name      | is_assisted   | assist_player   | player_id   | assist_player_id   |   shot_x |   shot_y |   home_win_prob |
|---:|-------------------:|----------:|:--------------|:-----------------------|:----------------------------------------------------------------------|-------------:|-------------:|---------:|-------------------:|----------------:|:-----------------------|:-------------------|---------------:|:----------------|:---------------|:-----------|:-----------------|:--------------|:----------------|:------------|:-------------------|---------:|---------:|----------------:|
|  0 | 401522202101799901 | 401522202 | UConn Huskies | San Diego State Aztecs | Jump Ball won by UConn                                                |            0 |            0 |        1 |               1200 |            2400 | UConn Huskies          | Jumpball           |            615 | False           | False          | False      |                  | False         |                 |             |                    |      nan |      nan |           0.752 |
|  1 | 401522202101806501 | 401522202 | UConn Huskies | San Diego State Aztecs | Jordan Hawkins made Jumper. Assisted by Adama Sanogo.                 |            2 |            0 |        1 |               1174 |            2374 | UConn Huskies          | JumpShot           |            558 | True            | True           | False      | Jordan Hawkins   | True          | Adama Sanogo    | 4683750     | 4702023            |       18 |       15 |           0.77  |
|  2 | 401522202101808701 | 401522202 | UConn Huskies | San Diego State Aztecs | Lamont Butler made Three Point Jumper. Assisted by Matt Bradley.      |            2 |            3 |        1 |               1152 |            2352 | San Diego State Aztecs | JumpShot           |            558 | True            | True           | True       | Lamont Butler    | True          | Matt Bradley    | 4433183     | 4397049            |       39 |       22 |           0.729 |
|  3 | 401522202101814901 | 401522202 | UConn Huskies | San Diego State Aztecs | Tristen Newton Turnover.                                              |            2 |            3 |        1 |               1130 |            2330 | UConn Huskies          | Lost Ball Turnover |            598 | False           | False          | False      | Tristen Newton   | False         |                 | 4592965     |                    |      nan |      nan |           0.709 |
|  4 | 401522202101817101 | 401522202 | UConn Huskies | San Diego State Aztecs | Darrion Trammell made Three Point Jumper. Assisted by Keshad Johnson. |            2 |            6 |        1 |               1108 |            2308 | San Diego State Aztecs | JumpShot           |            558 | True            | True           | True       | Darrion Trammell | True          | Keshad Johnson  | 4702011     | 4431786            |        1 |        0 |           0.641 |

`half`/`secs_left_half` (mens, and womens before 2015-16) and `quarter`/`secs_left_qt`
(womens after 2014-15) are also emitted, but are **deprecated in favor of
`period`/`secs_left_period` and will be removed in 3.0**. Only the pair matching a
game's format is present, so concatenating games from either side of the womens
rule change leaves each pair half-empty; `period`/`secs_left_period` are always
populated. `shooter` is likewise emitted but **deprecated in favor of `player_name`
and will be removed in 3.0**.

### Shot coordinates

`shot_x`/`shot_y` are in **feet**, normalized to whichever basket the shooting team
is attacking, so both teams' shots share a single half court. `shot_x` spans the
court's 50 ft width (`25` is the midline) and `shot_y` runs up the court away from
that basket, with `0` sitting at the basket. Plotting `shot_x` rightward and
`shot_y` upward therefore draws the half court as seen from above, basket at the
bottom, with the shooter's right hand side at low `shot_x`. `shot_y` is
occasionally slightly negative — the origin is the backboard plane, a few feet in
from the baseline, so shots released from behind it are recorded below zero.

Two caveats about ESPN's own data:

- **Free throws** are all stamped at `(25, 0)` — the basket — rather than at the
  free-throw line. This is a fixed placeholder, not a measured location, so it will
  skew any shot chart or shot-distance calculation that includes them. Filter on
  `play_type == "MadeFreeThrow"` to handle them however you like.
- **Coordinates are missing for many older games.** ESPN simply did not record shot
  locations for most games before roughly the 2025-26 season, and `shot_x`/`shot_y`
  are `NaN` there. (ESPN's HTML feed fills those games with a placeholder at the
  basket rather than leaving them empty; CBBpy detects that and returns `NaN`, so
  both `source` values agree.)

Function call:

```python
import cbbpy.mens_scraper as s
s.get_player_info('5105865')
```

Returns:
|    |   player_id | first_name   | last_name   |   jersey_number | pos     | status   | team              | experience   | height   | weight   | birthplace   | date_of_birth   |
|---:|------------:|:-------------|:------------|----------------:|:--------|:---------|:------------------|:-------------|:---------|:---------|:-------------|:----------------|
|  0 |     5105865 | Reed         | Bailey      |               1 | Forward | active   | Davidson Wildcats | Junior       | 6' 10"   | 230 lbs  | Harvard, MA  |                 |

Function call:

```python
import cbbpy.womens_scraper as s
s.get_team_schedule('davidson', 2022)
```

Returns (partially):
|    | team     |   team_id |   season |   game_id | game_datetime        | game_day          | game_time    | opponent                                  |   opponent_id | season_type    | game_status   | tv_network   | game_result   |
|---:|:---------|----------:|---------:|----------:|:---------------------|:------------------|:-------------|:------------------------------------------|--------------:|:---------------|:--------------|:-------------|:--------------|
|  0 | Davidson |      2166 |     2022 | 401370995 | 2021-11-10T00:00:00Z | November 09, 2021 | 04:00 PM PST | Delaware Blue Hens                        |            48 | Regular Season | Final         | ESPN+        | W 93-71       |
|  1 | Davidson |      2166 |     2022 | 401370996 | 2021-11-14T01:30:00Z | November 13, 2021 | 05:30 PM PST | San Francisco Dons                        |          2539 | Regular Season | Final         |              | L 60-65       |
|  2 | Davidson |      2166 |     2022 | 401365883 | 2021-11-18T17:00:00Z | November 18, 2021 | 09:00 AM PST | New Mexico State Aggies                   |           166 | Regular Season | Final         | ESPNU        | L 64-75       |
|  3 | Davidson |      2166 |     2022 | 401377036 | 2021-11-19T19:30:00Z | November 19, 2021 | 11:30 AM PST | Pennsylvania Quakers                      |           219 | Regular Season | Final         | ESPNU        | W 72-60       |
|  4 | Davidson |      2166 |     2022 | 401377040 | 2021-11-21T23:00:00Z | November 21, 2021 | 03:00 PM PST | East Carolina Pirates                     |           151 | Regular Season | Final         | ESPNU        | W 76-67       |

Function call:

```python
import cbbpy.mens_scraper as s
s.get_conference_schedule('ovc', 2015)
```

Returns (showing the middle of the output):
|    | team             |   team_id |   season |   game_id | game_datetime        | game_day          | game_time    | opponent                   |   opponent_id | season_type    | game_status   | tv_network   | game_result   |
|---:|:-----------------|----------:|---------:|----------:|:---------------------|:------------------|:-------------|:---------------------------|--------------:|:---------------|:--------------|:-------------|:--------------|
| 30 | Belmont          |      2057 |     2015 | 400766521 | 2015-03-07T03:15:00Z | March 06, 2015    | 07:15 PM PST | Eastern Kentucky Colonels  |          2198 | Regular Season | Final         | ESPNU        | W 53-52       |
| 31 | Belmont          |      2057 |     2015 | 400766705 | 2015-03-08T00:00:00Z | March 07, 2015    | 04:00 PM PST | Murray State Racers        |            93 | Regular Season | Final         | ESPN2        | W 88-87       |
| 32 | Belmont          |      2057 |     2015 | 400785349 | 2015-03-20T19:30:00Z | March 20, 2015    | 12:30 PM PDT | Virginia Cavaliers         |           258 | Postseason     | Final         | truTV        | L 67-79       |
| 33 | Eastern Kentucky |      2198 |     2015 | 400596308 | 2014-11-15T00:00:00Z | November 14, 2014 | 04:00 PM PST | Savannah State Tigers      |          2542 | Regular Season | Final         |              | W 76-53       |
| 34 | Eastern Kentucky |      2198 |     2015 | 400596315 | 2014-11-19T00:00:00Z | November 18, 2014 | 04:00 PM PST | Kentucky Christian Knights |          3077 | Regular Season | Final         |              | W 115-35      |



## Contact
Feel free to reach out to me directly with any questions, requests, or suggestions at <dnlcowan37@gmail.com>.
