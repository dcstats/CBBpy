[![PyPi Version](https://img.shields.io/pypi/v/cbbpy.svg)](https://pypi.org/project/cbbpy/) [![Downloads](https://pepy.tech/badge/cbbpy)](https://pepy.tech/project/cbbpy)

# CBBpy: A Python-based web scraper for NCAA basketball

## Purpose
This package is designed to bridge the gap between data and analysis for NCAA D1 basketball. CBBpy can grab play-by-play, boxscore, and other game metadata for any NCAA D1 men's basketball game.

## Installation and import
CBBpy requires Python >= 3.7 as well as the following packages:
* pandas>=1.4.2
* numpy>=1.22.3
* python-dateutil>=2.8.2
* pytz>=2022.1
* tqdm>=4.63.0
* lxml>=4.9.0


Install using pip:
```
pip install cbbpy
```

As of now, CBBpy only offers a men's basketball scraper, which can be imported as such:
```
import cbbpy.mens_scraper as ms
```

## Functions available in CBBpy
NOTE: game ID, as far as CBBpy is concernced, is a valid **ESPN** game ID

`ms.get_game_info(game_id: str)` grabs all the metadata (game date, time, score, teams, referees, etc) for a particular game.

`ms.get_game_boxscore(game_id: str)` returns a pandas DataFrame with each player's stats for a particular game.

`ms.get_game_pbp(game_id: str)` scrapes the play-by-play tables for a game and returns a pandas DataFrame, with each entry representing a play made during the game.

`ms.get_game(game_id: str, info: bool = True, box: bool = True, pbp: bool = True)` gets *all* information about a game (game info, boxscore, PBP) and returns a tuple of results `(game_info, boxscore, pbp)`. `info, box, pbp` are booleans which users can set to `False` if there is any information they wish not to scrape. For example, `box = False` would return an empty DataFrame for the boxscore info, while scraping PBP and metadata info normally.

`ms.get_games_season(season: int, info: bool = True, box: bool = True, pbp: bool = True)` scrapes all game information for all games in a particular season. As an example, to scrape games for the 2020-21 season, call `get_games_season(2021)`. Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`ms.get_games_range(start_date: str, end_date: str, info: bool = True, box: bool = True, pbp: bool = True)` scrapes all game information for all games between `start_date` and `end_date` (inclusive). As an example, to scrape games between November 30, 2022 and December 10, 2022, call `get_games_season('11-30-2022', '12-10-2022')`. Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`ms.get_game_ids(date: str)` returns a list of all game IDs for a particular date.

## Examples

Function call: 

`ms.get_game_info('401408636')`

Returns: 
|    |   game_id | home_team       |   home_id |   home_rank | home_record   |   home_score | away_team                |   away_id |   away_rank | away_record   |   away_score | home_win   |   num_ots | is_conference   | is_neutral   | is_postseason   | tournament                                            | game_day       | game_time    | game_loc        | arena             |   arena_capacity | attendance   | tv_network   | referee_1   | referee_2     | referee_3     |
|---:|----------:|:----------------|----------:|------------:|:--------------|-------------:|:-------------------------|----------:|------------:|:--------------|-------------:|:-----------|----------:|:----------------|:-------------|:----------------|:------------------------------------------------------|:---------------|:-------------|:----------------|:------------------|-----------------:|:-------------|:-------------|:------------|:--------------|:--------------|
|  0 | 401408636 | Kansas Jayhawks |      2305 |           1 | 34-6          |           72 | North Carolina Tar Heels |       153 |           8 | 29-10         |           69 | True       |         0 | False           | True         | True            | Men's Basketball Championship - National Championship | April 04, 2022 | 06:20 PM PDT | New Orleans, LA | Caesars Superdome |              nan | 69,423       | TBS          | Ron Groover | Terry Oglesby | Jeff Anderson |

Function call: 

`ms.get_game_boxscore('401408636')`

Returns (partially): 
|    |   game_id | team            | player       |   player_id | position   | starter   |   min |   fgm |   fga |   2pm |   2pa |   3pm |   3pa |   ftm |   fta |   oreb |   dreb |   reb |   ast |   stl |   blk |   to |   pf |   pts |
|---:|----------:|:----------------|:-------------|------------:|:-----------|:----------|------:|------:|------:|------:|------:|------:|------:|------:|------:|-------:|-------:|------:|------:|------:|------:|-----:|-----:|------:|
|  0 | 401408636 | Kansas Jayhawks | J. Wilson    |     4431714 | F          | True      |    34 |     5 |    13 |     4 |     8 |     1 |     5 |     4 |     4 |      1 |      3 |     4 |     2 |     0 |     1 |    0 |    1 |    15 |
|  1 | 401408636 | Kansas Jayhawks | D. McCormack |     4397019 | F          | True      |    29 |     7 |    15 |     7 |    15 |     0 |     0 |     1 |     2 |      3 |      7 |    10 |     0 |     1 |     1 |    1 |    4 |    15 |
|  2 | 401408636 | Kansas Jayhawks | D. Harris    |     4431983 | G          | True      |    27 |     1 |     5 |     1 |     4 |     0 |     1 |     0 |     0 |      0 |      0 |     0 |     3 |     3 |     1 |    4 |    0 |     2 |
|  3 | 401408636 | Kansas Jayhawks | C. Braun     |     4431767 | G          | True      |    40 |     6 |    14 |     6 |    13 |     0 |     1 |     0 |     0 |      1 |     11 |    12 |     3 |     0 |     0 |    1 |    3 |    12 |
|  4 | 401408636 | Kansas Jayhawks | O. Agbaji    |     4397018 | G          | True      |    37 |     4 |     9 |     3 |     5 |     1 |     4 |     3 |     8 |      1 |      2 |     3 |     1 |     1 |     1 |    2 |    1 |    12 |

Function call: 

`ms.get_game_pbp('401408636')`

Returns (partially): 
|    |   game_id | home_team       | away_team                | play_team                |   home_score |   away_score |   half |   secs_left_half |   secs_left_reg | play_desc                                                          | play_type   | scoring_play   | shooter         | is_assisted   | assist_player    |
|---:|----------:|:----------------|:-------------------------|:-------------------------|-------------:|-------------:|-------:|-----------------:|----------------:|:-------------------------------------------------------------------|:------------|:---------------|:----------------|:--------------|:-----------------|
|  0 | 401408636 | Kansas Jayhawks | North Carolina Tar Heels | Kansas Jayhawks          |            0 |            0 |      1 |             1200 |            2400 | Jump Ball won by Kansas                                            | jump ball   | False          |                 | False         |                  |
|  1 | 401408636 | Kansas Jayhawks | North Carolina Tar Heels | Kansas Jayhawks          |            3 |            0 |      1 |             1179 |            2379 | Ochai Agbaji made Three Point Jumper. Assisted by Christian Braun. | jumper      | True           | Ochai Agbaji    | True          | Christian Braun  |
|  2 | 401408636 | Kansas Jayhawks | North Carolina Tar Heels | North Carolina Tar Heels |            3 |            0 |      1 |             1161 |            2361 | Armando Bacot missed Jumper.                                       | jumper      | False          |                 | False         |                  |
|  3 | 401408636 | Kansas Jayhawks | North Carolina Tar Heels | Kansas Jayhawks          |            3 |            0 |      1 |             1161 |            2361 | Christian Braun Defensive Rebound.                                 | rebound     | False          |                 | False         |                  |
|  4 | 401408636 | Kansas Jayhawks | North Carolina Tar Heels | Kansas Jayhawks          |            5 |            0 |      1 |             1144 |            2344 | David McCormack made Jumper. Assisted by Dajuan Harris Jr..        | jumper      | True           | David McCormack | True          | Dajuan Harris Jr |

## Contact
Feel free to reach out to me directly with any questions, requests, or suggestions at <dnlcowan37@gmail.com>.
