# CBBpy: A Python-based web scraper for NCAA basketball

## Purpose
This package is designed to bridge the gap between data and analysis for NCAA D1 basketball. CBBpy can grab play-by-play, boxscore, and other game metadata for any NCAA D1 men's basketball game.

## Installation
CBBpy requires Python >= 3.9 as well as the following packages:
* pandas>=1.4.2
* numpy>=1.22.3
* python-dateutil>=2.8.2
* pytz>=2022.1
* tqdm>=4.63.0


Install using pip:
```
pip install cbbpy
```

## Functions available in CBBpy
NOTE: game ID, as far as CBBpy is concernced, is a valid **ESPN** game ID

`get_game_info(game_id: str)` grabs all the metadata (game date, time, score, teams, referees, etc) for a particular game.

`get_game_boxscore(game_id: str)` returns a pandas DataFrame with each player's stats for a particular game.

`get_game_pbp(game_id: str)` scrapes the play-by-play tables for a game and returns a pandas DataFrame, with each entry representing a play made during the game.

`get_game(game_id: str)` gets *all* information about a game (game info, boxscore, PBP) and returns a tuple of results `(game_info, boxscore, pbp)`

`get_games_season(season: int)` scrapes all game information for all games in a particular season. As an example, to scrape games for the 2020-21 season, call `get_games_season(2021)`.

`get_game_ids(date: str)` returns a list of all game IDs for a particular date.
