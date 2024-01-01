"""
A tool to scrape data for NCAA D1 Women's college basketball games.

Author: Daniel Cowan
"""

from datetime import datetime
import pandas as pd
from typing import Union
from .cbbpy_utils import (
    _get_game,
    _get_games_range,
    _get_games_season,
    _get_game_ids,
    _get_game_boxscore,
    _get_game_pbp,
    _get_game_info,
)


def get_game(
    game_id: str, info: bool = True, box: bool = True, pbp: bool = True
) -> tuple:
    """A function that scrapes all game info (metadata, boxscore, play-by-play).

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - (game_info_df, boxscore_df, pbp_df), a tuple consisting of:
            -- game_info_df: a DataFrame of the game's metadata
            -- boxscore_df: a DataFrame of the game's boxscore (both teams combined)
            -- pbp_df: a DataFrame of the game's play-by-play
    """
    return _get_game(game_id, "womens", info, box, pbp)


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
    return _get_games_range(start_date, end_date, "womens", info, box, pbp)


def get_games_season(
    season: int, info: bool = True, box: bool = True, pbp: bool = True
) -> tuple:
    """A function that scrapes all game info (metadata, boxscore, play-by-play) for every game of
    a given season.

    Parameters:
        - season: an integer representing the season to be scraped. NOTE: season is takes the form
        of the four-digit representation of the later year of the season. So, as an example, the
        2021-22 season is referred to by the integer 2022.

    Returns
        - (game_info_df, boxscore_df, pbp_df), a tuple consisting of:
            -- game_info_df: a DataFrame of the game's metadata
            -- boxscore_df: a DataFrame of the game's boxscore (both teams combined)
            -- pbp_df: a DataFrame of the game's play-by-play
    """
    return _get_games_season(season, "womens", info, box, pbp)


def get_game_ids(date: Union[str, datetime]) -> list:
    """A function that scrapes all game IDs on a date.

    Parameters:
        - date: a string/datetime object representing the date to be scraped

    Returns
        - a list of ESPN all game IDs for games played on the date given
    """
    return _get_game_ids(date, "womens")


def get_game_boxscore(game_id: str) -> pd.DataFrame:
    """A function that scrapes a game's boxscore.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - the game boxscore as a DataFrame
    """
    return _get_game_boxscore(game_id, "womens")


def get_game_pbp(game_id: str) -> pd.DataFrame:
    """A function that scrapes a game's play-by-play information.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - the game's play-by-play information represented as a DataFrame
    """
    return _get_game_pbp(game_id, "womens")


def get_game_info(game_id: str) -> pd.DataFrame:
    """A function that scrapes game metadata.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - a DataFrame with one row and a column for each piece of metadata
    """
    return _get_game_info(game_id, "womens")
