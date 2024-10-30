"""
A tool to scrape data for NCAA D1 Men's college basketball games.

Author: Daniel Cowan
"""

from datetime import datetime
import pandas as pd
from typing import Union
from utils.cbbpy_utils import (
    _get_game,
    _get_games_range,
    _get_games_season,
    _get_game_ids,
    _get_game_boxscore,
    _get_game_pbp,
    _get_game_info,
    _get_player,
    _get_team_schedule,
    get_current_season,
)


def get_game(
    game_id: str, info: bool = True, box: bool = True, pbp: bool = True
) -> tuple:
    """A function that scrapes all game info (metadata, boxscore, play-by-play).

    Parameters:
        - game_id: a string representing the game's ESPN game ID
        - info: a boolean denoting whether game metadata is to be scraped
        - box: a boolean denoting whether game boxscore is to be scraped
        - pbp: a boolean denoting whether game play-by-play is to be scraped

    Returns
        - (game_info_df, boxscore_df, pbp_df), a tuple consisting of:
            -- game_info_df: a DataFrame of the game's metadata
            -- boxscore_df: a DataFrame of the game's boxscore (both teams combined)
            -- pbp_df: a DataFrame of the game's play-by-play
    """
    return _get_game(game_id, "mens", info, box, pbp)


def get_games_range(
    start_date: str,
    end_date: str,
    info: bool = True,
    box: bool = True,
    pbp: bool = True,
) -> tuple:
    """A function that scrapes a game information between a given range of dates.

    Parameters:
        - start_date: a string representing the first day of games to scrape
        - end_date: a string representing the last day of games to scrape (inclusive)
        - info: a boolean denoting whether game metadata is to be scraped
        - box: a boolean denoting whether game boxscore is to be scraped
        - pbp: a boolean denoting whether game play-by-play is to be scraped

    Returns
        - (game_info_df, boxscore_df, pbp_df), a tuple consisting of:
            -- game_info_df: a DataFrame of the game's metadata
            -- boxscore_df: a DataFrame of the game's boxscore (both teams combined)
            -- pbp_df: a DataFrame of the game's play-by-play
    """
    return _get_games_range(start_date, end_date, "mens", info, box, pbp)


def get_games_season(
    season: int, info: bool = True, box: bool = True, pbp: bool = True
) -> tuple:
    """A function that scrapes all game info (metadata, boxscore, play-by-play) for every game of
    a given season.

    Parameters:
        - season: an integer representing the season to be scraped. NOTE: season is takes the form
        of the four-digit representation of the later year of the season. So, as an example, the
        2021-22 season is referred to by the integer 2022.
        - info: a boolean denoting whether game metadata is to be scraped
        - box: a boolean denoting whether game boxscore is to be scraped
        - pbp: a boolean denoting whether game play-by-play is to be scraped

    Returns
        - (game_info_df, boxscore_df, pbp_df), a tuple consisting of:
            -- game_info_df: a DataFrame of the game's metadata
            -- boxscore_df: a DataFrame of the game's boxscore (both teams combined)
            -- pbp_df: a DataFrame of the game's play-by-play
    """
    return _get_games_season(season, "mens", info, box, pbp)


def get_game_ids(date: Union[str, datetime]) -> list:
    """A function that scrapes all game IDs on a date.

    Parameters:
        - date: a string/datetime object representing the date to be scraped

    Returns
        - a list of ESPN all game IDs for games played on the date given
    """
    return _get_game_ids(date, "mens")


def get_game_boxscore(game_id: str) -> pd.DataFrame:
    """A function that scrapes a game's boxscore.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - the game boxscore as a DataFrame
    """
    return _get_game_boxscore(game_id, "mens")


def get_game_pbp(game_id: str) -> pd.DataFrame:
    """A function that scrapes a game's play-by-play information.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - the game's play-by-play information represented as a DataFrame
    """
    return _get_game_pbp(game_id, "mens")


def get_game_info(game_id: str) -> pd.DataFrame:
    """A function that scrapes game metadata.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - a DataFrame with one row and a column for each piece of metadata
    """
    return _get_game_info(game_id, "mens")


def get_player_info(player_id: str) -> pd.DataFrame:
    """A function that scrapes player info for a given player ID

    Parameters:
        - player_id: a string representing the player's ESPN player ID

    Returns
        - a DataFrame with one row for the player
    """
    return _get_player(player_id, "mens")


def get_team_schedule(team: str, season: int = None) -> pd.DataFrame:
    """A function that scrapes a team's schedule.

    Parameters:
        - team: a string representing the name of the team to be scraped
        - season: a string representing the season to be scraped

    Returns
        - a DataFrame of the team's schedule
    """
    if season is None:
        season = get_current_season()
    return _get_team_schedule(team, season, "mens")