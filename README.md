[![PyPi Version](https://img.shields.io/pypi/v/cbbpy.svg)](https://pypi.org/project/cbbpy/) [![Downloads](https://img.shields.io/pypi/dm/cbbpy?color=be94e4
)](https://pypistats.org/packages/cbbpy)

# CBBpy: A Python-based web scraper for NCAA basketball

## Purpose
This package is designed to bridge the gap between data and analysis for NCAA D1 basketball. CBBpy can grab play-by-play, boxscore, and other game metadata for any NCAA D1 men's or women's basketball game. Inspired by the [ncaahoopR package](https://github.com/lbenz730/ncaahoopR) by Luke Benz - check that out if you are an R user!

## Installation and import
CBBpy requires Python >= 3.9 as well as the following packages:
* pandas>=2.0.0
* numpy>=2.0.0
* python-dateutil>=2.4.0
* pytz>=2022.1
* tqdm>=4.63.0
* lxml>=4.9.0
* joblib>=1.0.0
* beautifulsoup4>=4.11.0
* requests>=2.27.0
* rapidfuzz>=3.9.0
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

## Functions available in CBBpy
NOTE: game ID, as far as CBBpy is concerned, is a valid **ESPN** game ID

`s.get_game_info(game_id: Union[str, int])` grabs all the metadata (game date, time, score, teams, referees, etc) for a particular game.

`s.get_game_boxscore(game_id: Union[str, int])` returns a pandas DataFrame with each player's stats for a particular game.

`s.get_game_pbp(game_id: Union[str, int])` scrapes the play-by-play tables for a game and returns a pandas DataFrame, with each entry representing a play made during the game.

`s.get_game(game_id: Union[str, int], info: bool = True, box: bool = True, pbp: bool = True)` gets *all* information about a game (game info, boxscore, PBP) and returns a tuple of results `(game_info, boxscore, pbp)`. `info, box, pbp` are booleans which users can set to `False` if there is any information they wish not to scrape. For example, `box = False` would return an empty DataFrame for the boxscore info, while scraping PBP and metadata info normally.

`s.get_games_season(season: Union[str, int] = None, info: bool = True, box: bool = True, pbp: bool = True)` scrapes all game information for all finished or in progress games in a particular season (defaults to the current season). As an example, to scrape games for the 2020-21 season, call `get_games_season(2021)`. Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`s.get_games_range(start_date: Union[int, datetime], end_date: Union[int, datetime], info: bool = True, box: bool = True, pbp: bool = True)` scrapes all game information for all finished or in progress games between `start_date` and `end_date` (inclusive). As an example, to scrape games from November 30, 2022 to December 10, 2022, call `get_games_season('11-30-2022', '12-10-2022')`. Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`s.get_games_team(team: str, season: Union[str, int] = None, info: bool = True, box: bool = True, pbp: bool = True)` scrapes all game information for all finished or in progress games in a particular season (defaults to the current season) for a given team. As an example, to scrape games for Duke's 2020-21 season, call `get_games_team('duke', 2021)`; for their current season, you can just call `get_games_team('duke')`. If a given team does not have an exact match in the static list of teams scraped from ESPN's site, this function will scrape the games for the closest fuzzy-matched team (e.g. if "valpo" is provided as the team, the function will scrape the games for "Valparaiso"). Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`s.get_games_conference(conference: str, season: Union[str, int] = None, info: bool = True, box: bool = True, pbp: bool = True)` scrapes all game information for all finished or in progress games in a particular season (defaults to the current season) for all teams in a given conference. As an example, to scrape games for the A10's 2017-18 season, call `get_games_conference('a10', 2018)`; for their current season, you can just call `get_games_conference('a10')`. If a given conference does not have an exact match in the static list of conferences scraped from ESPN's site, this function will scrape the games for the closest fuzzy-matched conference (e.g. if "am east" is provided as the conference, the function will scrape the games for "America East Conference"). Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`s.get_game_ids(date: Union[str, datetime])` returns a list of all game IDs for a particular date.

`s.get_player_info(player_id: Union[str, int])` returns a DataFrame describing the player's info from ESPN's bio page.

`s.get_teams_from_conference(conference: str, season: Union[str, int] = None)` returns a list of the teams in the given conference for a season (defaults to the current season).

`s.get_team_schedule(team: str, season: Union[str, int] = None)` returns a DataFrame of a team's schedule for a given season (defaults to the current season). If a given team does not have an exact match in the static list of teams scraped from ESPN's site, this function will scrape the schedule for the closest fuzzy-matched team (e.g. if "valpo" is provided as the team, the function will scrape the schedule for "Valparaiso").

`s.get_conference_schedule(conference: str, season: Union[str, int] = None)` returns a DataFrame of the schedules for all teams in a given conference for a given season (defaults to the current season). If a given conference does not have an exact match in the static list of conferences scraped from ESPN's site, this function will scrape the schedules for the closest fuzzy-matched conference (e.g. if "am east" is provided as the conference, the function will scrape the schedules for "America East Conference").

## Examples

Function call:

```python
import cbbpy.mens_scraper as s
s.get_game_info('401522202')
```

Returns:
|    |   game_id | home_team     |   home_id |   home_rank | home_record   |   home_score | away_team              |   away_id |   away_rank | away_record   |   away_score | home_win   |   num_ots | is_conference   | is_neutral   | is_postseason   | tournament                                            | game_day       | game_time    | game_loc    | arena       |   arena_capacity |   attendance | tv_network   | referee_1   | referee_2     | referee_3    |
|---:|----------:|:--------------|----------:|------------:|:--------------|-------------:|:-----------------------|----------:|------------:|:--------------|-------------:|:-----------|----------:|:----------------|:-------------|:----------------|:------------------------------------------------------|:---------------|:-------------|:------------|:------------|-----------------:|-------------:|:-------------|:------------|:--------------|:-------------|
|  0 | 401522202 | UConn Huskies |        41 |           4 | 31-8          |           76 | San Diego State Aztecs |        21 |           5 | 32-7          |           59 | True       |         0 | False           | True         | True            | Men's Basketball Championship - National Championship | April 03, 2023 | 06:20 PM PDT | Houston, TX | NRG Stadium |                0 |        72423 | CBS          | Ron Groover | Terry Oglesby | Keith Kimble |

Function call:

```python
import cbbpy.womens_scraper as s 
s.get_game_boxscore('401528028')
```

Returns (partially):
|    |   game_id | team       | player      |   player_id | position   | starter   |   min |   fgm |   fga |   2pm |   2pa |   3pm |   3pa |   ftm |   fta |   oreb |   dreb |   reb |   ast |   stl |   blk |   to |   pf |   pts |
|---:|----------:|:-----------|:------------|------------:|:-----------|:----------|------:|------:|------:|------:|------:|------:|------:|------:|------:|-------:|-------:|------:|------:|------:|------:|-----:|-----:|------:|
|  0 | 401528028 | LSU Tigers | A. Reese    |     4433402 | F          | True      |    29 |     5 |    12 |     5 |    12 |     0 |     0 |     5 |     8 |      6 |      4 |    10 |     5 |     3 |     1 |    0 |    3 |    15 |
|  1 | 401528028 | LSU Tigers | L. Williams |     4280886 | F          | True      |    37 |     9 |    16 |     9 |    16 |     0 |     0 |     2 |     2 |      1 |      4 |     5 |     0 |     3 |     0 |    3 |    4 |    20 |
|  2 | 401528028 | LSU Tigers | F. Johnson  |     4698736 | G          | True      |    37 |     4 |    11 |     3 |     7 |     1 |     4 |     1 |     1 |      2 |      5 |     7 |     4 |     1 |     0 |    4 |    1 |    10 |
|  3 | 401528028 | LSU Tigers | K. Poole    |     4433418 | G          | True      |    24 |     2 |     3 |     0 |     1 |     2 |     2 |     0 |     2 |      0 |      3 |     3 |     1 |     0 |     1 |    1 |    2 |     6 |
|  4 | 401528028 | LSU Tigers | A. Morris   |     4281251 | G          | True      |    33 |     8 |    14 |     7 |    11 |     1 |     3 |     4 |     4 |      1 |      1 |     2 |     9 |     1 |     0 |    2 |    3 |    21 |

Function call:

```python
import cbbpy.mens_scraper as s
s.get_game_pbp('401522202')
```

Returns (partially):
|    |   game_id | home_team     | away_team              | play_desc                                                             |   home_score |   away_score |   half |   secs_left_half |   secs_left_reg | play_team              | play_type          | shooting_play   | scoring_play   | is_three   | shooter          | is_assisted   | assist_player   |   shot_x |   shot_y |
|---:|----------:|:--------------|:-----------------------|:----------------------------------------------------------------------|-------------:|-------------:|-------:|-----------------:|----------------:|:-----------------------|:-------------------|:----------------|:---------------|:-----------|:-----------------|:--------------|:----------------|---------:|---------:|
|  0 | 401522202 | UConn Huskies | San Diego State Aztecs | Jump Ball won by UConn                                                |            0 |            0 |      1 |             1200 |            2400 | UConn Huskies          | jump ball          | False           | False          | False      |                  | False         |                 |      nan |      nan |
|  1 | 401522202 | UConn Huskies | San Diego State Aztecs | Jordan Hawkins made Jumper. Assisted by Adama Sanogo.                 |            2 |            0 |      1 |             1174 |            2374 | UConn Huskies          | jumper             | True            | True           | False      | Jordan Hawkins   | True          | Adama Sanogo    |       18 |       15 |
|  2 | 401522202 | UConn Huskies | San Diego State Aztecs | Lamont Butler made Three Point Jumper. Assisted by Matt Bradley.      |            2 |            3 |      1 |             1152 |            2352 | San Diego State Aztecs | three point jumper | True            | True           | True       | Lamont Butler    | True          | Matt Bradley    |       39 |       22 |
|  3 | 401522202 | UConn Huskies | San Diego State Aztecs | Tristen Newton Turnover.                                              |            2 |            3 |      1 |             1130 |            2330 | UConn Huskies          | turnover           | False           | False          | False      |                  | False         |                 |      nan |      nan |
|  4 | 401522202 | UConn Huskies | San Diego State Aztecs | Darrion Trammell made Three Point Jumper. Assisted by Keshad Johnson. |            2 |            6 |      1 |             1108 |            2308 | San Diego State Aztecs | three point jumper | True            | True           | True       | Darrion Trammell | True          | Keshad Johnson  |        1 |        0 |

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
|    | team     |   team_id |   season |   game_id | game_day          | game_time    | opponent                                  |   opponent_id | season_type    | game_status   | tv_network   | game_result   |
|---:|:---------|----------:|---------:|----------:|:------------------|:-------------|:------------------------------------------|--------------:|:---------------|:--------------|:-------------|:--------------|
|  0 | Davidson |      2166 |     2022 | 401370995 | November 09, 2021 | 04:00 PM PST | Delaware Blue Hens                        |            48 | Regular Season | Final         | ESPN+        | W 93-71       |
|  1 | Davidson |      2166 |     2022 | 401370996 | November 13, 2021 | 05:30 PM PST | San Francisco Dons                        |          2539 | Regular Season | Final         |              | L 60-65       |
|  2 | Davidson |      2166 |     2022 | 401365883 | November 18, 2021 | 09:00 AM PST | New Mexico State Aggies                   |           166 | Regular Season | Final         | ESPNU        | L 64-75       |
|  3 | Davidson |      2166 |     2022 | 401377036 | November 19, 2021 | 11:30 AM PST | Pennsylvania Quakers                      |           219 | Regular Season | Final         | ESPNU        | W 72-60       |
|  4 | Davidson |      2166 |     2022 | 401377040 | November 21, 2021 | 03:00 PM PST | East Carolina Pirates                     |           151 | Regular Season | Final         | ESPNU        | W 76-67       |

Function call:

```python
import cbbpy.mens_scraper as s
s.get_conference_schedule('ovc', 2015)
```

Returns (showing the middle of the output):
|    | team             |   team_id |   season |   game_id | game_day          | game_time    | opponent                   |   opponent_id | season_type    | game_status   | tv_network   | game_result   |
|---:|:-----------------|----------:|---------:|----------:|:------------------|:-------------|:---------------------------|--------------:|:---------------|:--------------|:-------------|:--------------|
| 30 | Belmont          |      2057 |     2015 | 400766521 | March 06, 2015    | 07:15 PM PST | Eastern Kentucky Colonels  |          2198 | Regular Season | Final         | ESPNU        | W 53-52       |
| 31 | Belmont          |      2057 |     2015 | 400766705 | March 07, 2015    | 04:00 PM PST | Murray State Racers        |            93 | Regular Season | Final         | ESPN2        | W 88-87       |
| 32 | Belmont          |      2057 |     2015 | 400785349 | March 20, 2015    | 12:30 PM PDT | Virginia Cavaliers         |           258 | Postseason     | Final         | truTV        | L 67-79       |
| 33 | Eastern Kentucky |      2198 |     2015 | 400596308 | November 14, 2014 | 04:00 PM PST | Savannah State Tigers      |          2542 | Regular Season | Final         |              | W 76-53       |
| 34 | Eastern Kentucky |      2198 |     2015 | 400596315 | November 18, 2014 | 04:00 PM PST | Kentucky Christian Knights |          3077 | Regular Season | Final         |              | W 115-35      |



## Contact
Feel free to reach out to me directly with any questions, requests, or suggestions at <dnlcowan37@gmail.com>.
