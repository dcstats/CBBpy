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
    _get_player_info,
    _get_team_schedule,
    get_current_season,
)


def get_game(
    game_id: str, info: bool = True, box: bool = True, pbp: bool = True
) -> tuple:
    """A function that scrapes all game info (metadata, boxscore, play-by-play).

    Parameters:
        game_id (str): The game's ESPN game ID
        info (bool, optional): Whether the game metadata is to be scraped. Defaults to True.
        box (bool, optional): Whether the game boxscore is to be scraped. Defaults to True.
        pbp (bool, optional): Whether the game play-by-play is to be scraped. Defaults to True.

    Returns:
        a tuple containing

        - pd.DataFrame: The game's metadata.\n
        - pd.DataFrame: The game's boxscore (both teams combined).\n
        - pd.DataFrame: The game's play-by-play.
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
        start_date (str): The first day of games to scrape.
        end_date (str): The last day of games to scrape (inclusive).
        info (bool, optional): Whether the game metadata is to be scraped. Defaults to True.
        box (bool, optional): Whether the game boxscore is to be scraped. Defaults to True.
        pbp (bool, optional): Whether the game play-by-play is to be scraped. Defaults to True.

    Returns:
        a tuple containing

        - pd.DataFrame: The game's metadata.\n
        - pd.DataFrame: The game's boxscore (both teams combined).\n
        - pd.DataFrame: The game's play-by-play.
    """
    return _get_games_range(start_date, end_date, "mens", info, box, pbp)


def get_games_season(
    season: int, info: bool = True, box: bool = True, pbp: bool = True
) -> tuple:
    """Scrapes desired game information (metadata, boxscore, play-by-play) for every game of a given season.

    Parameters:
        season (int): The season to be scraped. 
            NOTE: season takes the form of the four-digit representation of the later year of the season. 
            So, as an example, the 2021-22 season is referred to by the integer 2022.
        info (bool, optional): Whether the game metadata is to be scraped. Defaults to True.
        box (bool, optional): Whether the game boxscore is to be scraped. Defaults to True.
        pbp (bool, optional): Whether the game play-by-play is to be scraped. Defaults to True.

    Returns:
        a tuple containing

        - pd.DataFrame: The game's metadata.\n
        - pd.DataFrame: The game's boxscore (both teams combined).\n
        - pd.DataFrame: The game's play-by-play.
    """
    return _get_games_season(season, "mens", info, box, pbp)


def get_game_ids(date: Union[str, datetime]) -> list:
    """Scrapes all game IDs for a given date.

    Parameters:
        date (str | datetime): The date of the games to be scraped.

    Returns:
        list: The ESPN game IDs for each game played on the given date.
    """
    return _get_game_ids(date, "mens")


def get_game_boxscore(game_id: str) -> pd.DataFrame:
    """Scrapes each team's boxscore for a given game.

    Parameters:
        game_id (str): The game's ESPN game ID.

    Returns:
        pd.DataFrame: The boxscores of both teams, combined into one table.
    """
    return _get_game_boxscore(game_id, "mens")


def get_game_pbp(game_id: str) -> pd.DataFrame:
    """Scrapes a game's play-by-play data.

    Parameters:
        game_id (str): The game's ESPN game ID.

    Returns:
        pd.DataFrame: The game's play-by-play information, with a row for each play.
    """
    return _get_game_pbp(game_id, "mens")


def get_game_info(game_id: str) -> pd.DataFrame:
    """Scrapes game metadata from the ESPN game page.

    Args:
        game_id (str): The game's ESPN game ID.

    Returns:
        pd.DataFrame: The game's metadata scraped from the game page.
    """
    return _get_game_info(game_id, "mens")


def get_player_info(player_id: str) -> pd.DataFrame:
    """Scrapes player details from her bio page for a given player ID.

    Args:
        player_id (str): The player's ESPN player ID.

    Returns:
        pd.DataFrame: The given player's details.
    """
    return _get_player_info(player_id, "mens")


def get_team_schedule(team: str, season: int = None) -> pd.DataFrame:
    """Scrapes a given team's schedule for a specified season.

    Args:
        team (str): The name of the team to be scraped.
        season (int, optional): The season to be scraped. Defaults to current season.

    Returns:
        pd.DataFrame: The given team's schedule for the year.
    """
    if season is None:
        season = get_current_season()
    return _get_team_schedule(team, season, "mens")