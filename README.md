[![PyPi Version](https://img.shields.io/pypi/v/cbbpy.svg)](https://pypi.org/project/cbbpy/) [![Downloads](https://img.shields.io/pypi/dm/cbbpy?color=be94e4
)](https://pypistats.org/packages/cbbpy)

# CBBpy: A Python-based web scraper for NCAA basketball

## Purpose
This package is designed to bridge the gap between data and analysis for NCAA D1 basketball. CBBpy can grab play-by-play, boxscore, and other game metadata for any NCAA D1 men's or women's basketball game. Inspired by the [ncaahoopR package](https://github.com/lbenz730/ncaahoopR) by Luke Benz - check that out if you are an R user!

## Installation and import
CBBpy requires Python >= 3.9 as well as the following packages:
* pandas>=1.4.2
* numpy>=1.22.3
* python-dateutil>=2.8.2
* pytz>=2022.1
* tqdm>=4.63.0
* lxml>=4.9.0
* joblib>=1.1.0
* beautifulsoup4>=4.11.0
* requests>=2.27.0


Install using pip:
```
pip install cbbpy
```

The men's and women's scrapers can be imported as such:
```
import cbbpy.mens_scraper as s
import cbbpy.womens_scraper as s
```

## Functions available in CBBpy
NOTE: game ID, as far as CBBpy is concernced, is a valid **ESPN** game ID

`s.get_game_info(game_id: str)` grabs all the metadata (game date, time, score, teams, referees, etc) for a particular game.

`s.get_game_boxscore(game_id: str)` returns a pandas DataFrame with each player's stats for a particular game.

`s.get_game_pbp(game_id: str)` scrapes the play-by-play tables for a game and returns a pandas DataFrame, with each entry representing a play made during the game.

`s.get_game(game_id: str, info: bool = True, box: bool = True, pbp: bool = True)` gets *all* information about a game (game info, boxscore, PBP) and returns a tuple of results `(game_info, boxscore, pbp)`. `info, box, pbp` are booleans which users can set to `False` if there is any information they wish not to scrape. For example, `box = False` would return an empty DataFrame for the boxscore info, while scraping PBP and metadata info normally.

`s.get_games_season(season: int, info: bool = True, box: bool = True, pbp: bool = True)` scrapes all game information for all games in a particular season. As an example, to scrape games for the 2020-21 season, call `get_games_season(2021)`. Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`s.get_games_range(start_date: str, end_date: str, info: bool = True, box: bool = True, pbp: bool = True)` scrapes all game information for all games between `start_date` and `end_date` (inclusive). As an example, to scrape games between November 30, 2022 and December 10, 2022, call `get_games_season('11-30-2022', '12-10-2022')`. Returns a tuple of 3 DataFrames, similar to `get_game`. See `get_game` for an explanation of booleans `info, box, pbp`.

`s.get_game_ids(date: Union[str, datetime])` returns a list of all game IDs for a particular date.

## Examples

Function call: 

```
import cbbpy.mens_scraper as s
s.get_game_info('401522202')
```

Returns: 
|    |   game_id | home_team     |   home_id |   home_rank | home_record   |   home_score | away_team              |   away_id |   away_rank | away_record   |   away_score | home_win   |   num_ots | is_conference   | is_neutral   | is_postseason   | tournament                                            | game_day       | game_time    | game_loc    | arena       |   arena_capacity |   attendance | tv_network   | referee_1   | referee_2     | referee_3    |
|---:|----------:|:--------------|----------:|------------:|:--------------|-------------:|:-----------------------|----------:|------------:|:--------------|-------------:|:-----------|----------:|:----------------|:-------------|:----------------|:------------------------------------------------------|:---------------|:-------------|:------------|:------------|-----------------:|-------------:|:-------------|:------------|:--------------|:-------------|
|  0 | 401522202 | UConn Huskies |        41 |           4 | 31-8          |           76 | San Diego State Aztecs |        21 |           5 | 32-7          |           59 | True       |         0 | False           | True         | True            | Men's Basketball Championship - National Championship | April 03, 2023 | 06:20 PM PDT | Houston, TX | NRG Stadium |                0 |        72423 | CBS          | Ron Groover | Terry Oglesby | Keith Kimble |

Function call: 

```
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

```
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

## Contact
Feel free to reach out to me directly with any questions, requests, or suggestions at <dnlcowan37@gmail.com>.
